package hub

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Hub owns all channels and implements channel creation, caps, and sweeping.
type Hub struct {
	mu       sync.Mutex
	channels map[string]*Channel
	cfg      ChannelsConfig
	memory   int64 // current retained bytes across all channels
	stop     chan struct{}
	wg       sync.WaitGroup
}

// NewHub creates a Hub from cfg. Call Start to spawn the sweeper.
func NewHub(cfg ChannelsConfig) *Hub {
	if cfg.MaxChannels <= 0 {
		cfg.MaxChannels = 1000
	}
	return &Hub{
		channels: make(map[string]*Channel),
		cfg:      cfg,
		stop:     make(chan struct{}),
	}
}

// Start launches the hub sweeper.
func (h *Hub) Start() {
	h.wg.Add(1)
	go h.sweep()
}

// Stop closes all channels and the sweeper.
func (h *Hub) Stop() {
	close(h.stop)
	h.wg.Wait()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.channels {
		ch.shutdown()
	}
	h.channels = make(map[string]*Channel)
	h.memory = 0
}

func (h *Hub) sweep() {
	defer h.wg.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-h.stop:
			return
		case <-t.C:
			h.mu.Lock()
			for name, ch := range h.channels {
				if ch.shouldClose() {
					h.memory -= ch.retainedBytes()
					delete(h.channels, name)
					ch.shutdown()
				}
			}
			h.mu.Unlock()
		}
	}
}

// matchRule returns the first rule whose pattern matches name.
func (h *Hub) matchRule(name string) *ChannelRule {
	for _, r := range h.cfg.Rules {
		if patternMatch(r.Pattern, name) {
			return r
		}
	}
	return nil
}

// getOrCreate returns the channel for name, creating it if the name is
// allowed by the declared rules and within caps.
func (h *Hub) getOrCreate(name string) (*Channel, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if ch, ok := h.channels[name]; ok {
		return ch, nil
	}
	rule := h.matchRule(name)
	if rule == nil {
		return nil, fmt.Errorf("hub: channel %q not allowed by any rule", name)
	}
	if len(h.channels) >= h.cfg.MaxChannels {
		return nil, fmt.Errorf("hub: max_channels (%d) reached", h.cfg.MaxChannels)
	}
	ch := newChannel(name, rule, h)
	h.channels[name] = ch
	ch.start()
	return ch, nil
}

// existingChannels returns a copy of channels matching pattern.
func (h *Hub) existingChannels(pattern string) []*Channel {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []*Channel
	for name, ch := range h.channels {
		if patternMatch(pattern, name) {
			out = append(out, ch)
		}
	}
	return out
}

// Subscribe joins name.
func (h *Hub) Subscribe(ctx context.Context, name string, send func([]byte) bool) (*Subscription, error) {
	ch, err := h.getOrCreate(name)
	if err != nil {
		return nil, err
	}
	return ch.subscribe(send)
}

// Publish fans out body to every existing channel matching any pattern.
// Concrete names are created on demand.
func (h *Hub) Publish(ctx context.Context, patterns []string, body []byte, exclude SubID) Result {
	result := make(Result)
	for _, pat := range patterns {
		if isWildcard(pat) {
			matched := h.existingChannels(pat)
			if len(matched) == 0 {
				continue
			}
			for _, ch := range matched {
				result[ch.name] = ch.publish(body, exclude)
			}
		} else {
			ch, err := h.getOrCreate(pat)
			if err != nil {
				result[pat] = false
				continue
			}
			result[pat] = ch.publish(body, exclude)
		}
	}
	return result
}

// patternMatch: "room-*" matches "room-1"; a literal matches itself.
func patternMatch(pattern, name string) bool {
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(name, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == name
}

func isWildcard(p string) bool { return strings.HasSuffix(p, "*") }