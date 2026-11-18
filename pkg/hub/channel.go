package hub

import (
	"sync"
	"time"
)

// participant is one subscriber to a channel.
type participant struct {
	id   SubID
	send func([]byte) bool
}

// Channel is an actor: all mutable state is touched only from the actor
// goroutine via inbox closures.
type Channel struct {
	name string
	rule *ChannelRule
	hub  *Hub

	inbox chan func()
	done  chan struct{}

	// actor state (only touched by run)
	ring         []*Message
	participants map[SubID]*participant
	nextID       SubID
	lastMsg      time.Time
	lastPart     time.Time // last time participants count went 0→1 or 1→0

	mu   sync.Mutex // guards ring/retainedBytes for sweep reads from hub
	dead bool
}

func newChannel(name string, rule *ChannelRule, h *Hub) *Channel {
	return &Channel{
		name:         name,
		rule:         rule,
		hub:          h,
		inbox:        make(chan func(), 64),
		done:         make(chan struct{}),
		participants: make(map[SubID]*participant),
	}
}

func (c *Channel) start() {
	go c.run()
}

func (c *Channel) shutdown() {
	c.mu.Lock()
	if c.dead {
		c.mu.Unlock()
		return
	}
	c.dead = true
	c.mu.Unlock()
	close(c.done)
}

func (c *Channel) run() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case fn := <-c.inbox:
			fn()
		case <-ticker.C:
			if c.shouldClose() {
				c.shutdown()
				return
			}
		case <-c.done:
			return
		}
	}
}

// subscribe is called from Hub.Subscribe.
func (c *Channel) subscribe(send func([]byte) bool) (*Subscription, error) {
	type resp struct {
		sub *Subscription
		err error
	}
	out := make(chan resp, 1)
	select {
	case c.inbox <- func() {
		// enforce participants cap
		if c.rule.Participants > 0 && len(c.participants) >= c.rule.Participants {
			out <- resp{err: errParticipantsCap}
			return
		}
		// build byte-capped replay
		replay := c.buildReplay()
		id := c.nextID
		c.nextID++
		c.participants[id] = &participant{id: id, send: send}
		if len(c.participants) == 1 {
			c.lastPart = time.Now()
		}
		cancel := func() {
			select {
			case c.inbox <- func() {
				delete(c.participants, id)
				if len(c.participants) == 0 {
					c.lastPart = time.Now()
				}
			}:
			case <-c.done:
			}
		}
		out <- resp{sub: &Subscription{ID: id, Replay: replay, Cancel: cancel}}
	}:
	case <-c.done:
		return nil, errChannelClosed
	}
	r := <-out
	return r.sub, r.err
}

// buildReplay returns at most rule.Messages messages, stopping early if the
// accumulated byte count would exceed rule.MaxSize (byte-cap).
func (c *Channel) buildReplay() [][]byte {
	if len(c.ring) == 0 {
		return nil
	}
	capBytes := c.rule.MaxSize
	if capBytes <= 0 {
		capBytes = 256 * 1024
	}
	var out [][]byte
	var total int64
	for _, m := range c.ring {
		n := int64(len(m.Body))
		if total+n > capBytes && len(out) > 0 {
			break
		}
		out = append(out, append([]byte(nil), m.Body...))
		total += n
	}
	return out
}

// publish is called from Hub.Publish.
func (c *Channel) publish(body []byte, exclude SubID) bool {
	select {
	case c.inbox <- func() { c.doPublish(body, exclude) }:
		return true
	case <-c.done:
		return false
	}
}

func (c *Channel) doPublish(body []byte, exclude SubID) {
	if c.rule.MaxSize > 0 && int64(len(body)) > c.rule.MaxSize {
		return // oversized; drop
	}
	// append to ring
	msg := &Message{Body: append([]byte(nil), body...), Timestamp: time.Now()}
	c.ring = append(c.ring, msg)
	if c.rule.Messages > 0 && len(c.ring) > c.rule.Messages {
		c.ring = c.ring[len(c.ring)-c.rule.Messages:]
	}
	c.lastMsg = time.Now()

	// fan out
	for id, p := range c.participants {
		if id == exclude {
			continue
		}
		if !p.send(msg.Body) {
			delete(c.participants, id)
		}
	}
	if len(c.participants) == 0 {
		c.lastPart = time.Now()
	}
}

// retainedBytes returns the total bytes held in the ring (for hub accounting).
func (c *Channel) retainedBytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var total int64
	for _, m := range c.ring {
		total += int64(len(m.Body))
	}
	return total
}

// shouldClose reports whether the channel is eligible for sweeping.
func (c *Channel) shouldClose() bool {
	now := time.Now()
	c.mu.Lock()
	dead := c.dead
	c.mu.Unlock()
	if dead {
		return false
	}

	// These reads race with the actor; acceptable for a sweep heuristic.
	noParts := len(c.participants) == 0
	if !noParts {
		return false
	}
	idleFor := now.Sub(c.lastPart)
	if idleFor < c.rule.IdleTTL {
		return false
	}
	if len(c.ring) == 0 {
		return true
	}
	if now.Sub(c.lastMsg) > c.rule.Retain {
		return true
	}
	return false
}