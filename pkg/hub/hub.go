package hub

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// DefaultMessages is the ring depth used for a rule that does not set
// `messages`. Without a cap the ring would grow without bound.
const DefaultMessages = 100

// Hub owns all channels and implements channel creation and caps.
type Hub struct {
	mu       sync.Mutex
	channels map[string]*Channel
	cfg      ChannelsConfig

	// log receives operational events; set once, before channels exist.
	log Logger

	// retained is the total bytes held in every channel's ring, maintained by
	// the channel actors. stopped freezes the accounting during teardown.
	retained atomic.Int64
	stopped  atomic.Bool

	// participants counts subscribers across all channels. It is only
	// maintained when MaxParticipants is set.
	participants atomic.Int64
}

// Logger receives hub events an operator should see, such as a subscriber
// falling so far behind that its messages are dropped. It mirrors zap's
// SugaredLogger so the Caddy layer can adapt its own logger directly. A nil
// Logger disables reporting.
type Logger interface {
	Warnw(msg string, kv ...any)
}

// NewHub creates a Hub from cfg, filling in defaults for unset limits.
func NewHub(cfg ChannelsConfig) *Hub {
	if cfg.MaxChannels <= 0 {
		cfg.MaxChannels = 1000
	}
	for _, rule := range cfg.Rules {
		if rule.Messages <= 0 {
			rule.Messages = DefaultMessages
		}
	}
	return &Hub{
		channels: make(map[string]*Channel),
		cfg:      cfg,
	}
}

// SetLogger attaches a logging sink for operational events. Call it before
// Start: channels report through it from their actor goroutines.
func (h *Hub) SetLogger(log Logger) { h.log = log }

// Start is a no-op: each channel actor runs its own idle ticker and reports
// itself back through removeChannel when it closes.
func (h *Hub) Start() {}

// Stop closes every channel. Channels are collected under the lock and shut
// down once it is released, because shutdown must not run while the hub lock
// is held.
func (h *Hub) Stop() {
	h.stopped.Store(true)
	h.mu.Lock()
	chans := make([]*Channel, 0, len(h.channels))
	for _, ch := range h.channels {
		chans = append(chans, ch)
	}
	h.channels = make(map[string]*Channel)
	h.mu.Unlock()
	for _, ch := range chans {
		ch.shutdown()
	}
	h.retained.Store(0)
	h.participants.Store(0)
}

// acquireParticipant reserves a subscriber slot, reporting false when the
// global participant cap is already reached.
func (h *Hub) acquireParticipant() bool {
	if h.cfg.MaxParticipants <= 0 {
		return true
	}
	for {
		cur := h.participants.Load()
		if cur >= int64(h.cfg.MaxParticipants) {
			return false
		}
		if h.participants.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// releaseParticipant gives a subscriber slot back.
func (h *Hub) releaseParticipant() {
	if h.cfg.MaxParticipants > 0 {
		h.participants.Add(-1)
	}
}

// Participants reports the current subscriber count across all channels. It
// stays 0 unless MaxParticipants is configured.
func (h *Hub) Participants() int64 { return h.participants.Load() }

// addRetained adjusts the memory budget. Called by channel actors.
func (h *Hub) addRetained(delta int64) {
	if h.stopped.Load() {
		return
	}
	h.retained.Add(delta)
}

// Retained reports the total bytes currently held in channel rings.
func (h *Hub) Retained() int64 { return h.retained.Load() }

// allChannels returns a snapshot of every live channel.
func (h *Hub) allChannels() []*Channel {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*Channel, 0, len(h.channels))
	for _, ch := range h.channels {
		out = append(out, ch)
	}
	return out
}

// reserve reports whether n more retained bytes fit in the memory budget,
// evicting channels that have no participants to make room. max_memory is a
// soft cap: concurrent publishes can overshoot it.
func (h *Hub) reserve(n int64) bool {
	if h.cfg.MaxMemory <= 0 {
		return true
	}
	if h.retained.Load()+n <= h.cfg.MaxMemory {
		return true
	}
	for _, ch := range h.allChannels() {
		if h.retained.Load()+n <= h.cfg.MaxMemory {
			break
		}
		ch.evictIdle()
	}
	return h.retained.Load()+n <= h.cfg.MaxMemory
}

// removeChannel drops c from the registry if it is still the channel
// registered under its name. Called by the actor once it closes itself.
func (h *Hub) removeChannel(c *Channel) {
	h.mu.Lock()
	if cur, ok := h.channels[c.name]; ok && cur == c {
		delete(h.channels, c.name)
	}
	h.mu.Unlock()
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

// Rule returns the resolved rule for name, or nil when no rule matches. The
// rule set is fixed at construction, so this needs no lock.
func (h *Hub) Rule(name string) *ChannelRule { return h.matchRule(name) }

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
// Concrete names are created on demand. A pattern reports false when it
// could not be published to, including when the message would exceed the
// max_memory budget and no idle channel could be evicted to make room.
func (h *Hub) Publish(ctx context.Context, patterns []string, body []byte, exclude SubID) Result {
	result := make(Result)
	n := int64(len(body))
	for _, pat := range patterns {
		if isWildcard(pat) {
			matched := h.existingChannels(pat)
			if len(matched) == 0 {
				continue
			}
			if !h.reserve(n * int64(len(matched))) {
				result[pat] = false
				continue
			}
			for _, ch := range matched {
				result[ch.name] = ch.publish(body, exclude)
			}
		} else {
			if !h.reserve(n) {
				result[pat] = false
				continue
			}
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
