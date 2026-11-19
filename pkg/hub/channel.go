package hub

import (
	"sync"
	"sync/atomic"
	"time"
)

// DefaultQueueSize is the per-participant outbound buffer depth. A subscriber
// whose queue is full has the newest message dropped instead of stalling the
// channel; see participant.enqueue.
const DefaultQueueSize = 64

// dropLogInterval is the minimum time between drop reports for one channel, so
// a stalled subscriber cannot flood the log.
const dropLogInterval = 10 * time.Second

// participant is one subscriber to a channel. id, ip and send are only read
// from the actor goroutine; the queue is drained by a dedicated writer
// goroutine.
type participant struct {
	id    SubID
	ip    string // client host, for max_participants_per_ip; may be empty
	send  func([]byte) bool
	queue chan []byte

	// failed is set by the writer goroutine when send reports that the
	// subscriber is gone. It is only a hint for a publish racing the removal:
	// the writer also asks the actor to drop the participant straight away,
	// so a quiet channel does not keep a dead subscriber's slot.
	failed atomic.Bool
}

// enqueue hands body to the writer goroutine without blocking. A full queue
// means the subscriber is not keeping up, so the newest message is dropped
// and reported through Channel.Dropped.
func (p *participant) enqueue(body []byte) bool {
	select {
	case p.queue <- body:
		return true
	default:
		return false
	}
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
	lastDropLog  time.Time
	dropped      atomic.Uint64

	releaseOnce sync.Once  // makes the memory budget release idempotent
	mu          sync.Mutex // guards dead, which shutdown sets from any goroutine
	dead        bool
}

func newChannel(name string, rule *ChannelRule, h *Hub) *Channel {
	return &Channel{
		name:         name,
		rule:         rule,
		hub:          h,
		inbox:        make(chan func(), 64),
		done:         make(chan struct{}),
		participants: make(map[SubID]*participant),
		// IDs start at 1: 0 means "exclude nobody" in Publish.
		nextID: 1,
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

// Dropped reports how many messages were discarded for this channel: oversize
// bodies that were never retained, plus per-subscriber drops caused by a full
// outbound queue.
func (c *Channel) Dropped() uint64 { return c.dropped.Load() }

// retainedBytes is the total bytes held in the ring. Actor-only.
func (c *Channel) retainedBytes() int64 {
	var total int64
	for _, m := range c.ring {
		total += int64(len(m.Body))
	}
	return total
}

// releaseMemory returns this channel's retained bytes to the hub's memory
// budget. It reads the ring, so it must run on the actor goroutine; the once
// makes a duplicate release harmless.
func (c *Channel) releaseMemory() {
	c.releaseOnce.Do(func() {
		if n := c.retainedBytes(); n > 0 {
			c.hub.addRetained(-n)
		}
	})
}

// evictIdle closes the channel if it has no participants, reporting whether it
// is now closed. The participant check runs in the actor goroutine so it cannot
// race with a concurrent subscribe.
func (c *Channel) evictIdle() bool {
	reply := make(chan bool, 1)
	select {
	case c.inbox <- func() {
		if len(c.participants) > 0 {
			reply <- false
			return
		}
		c.releaseMemory()
		c.shutdown()
		reply <- true
	}:
	case <-c.done:
		c.hub.removeChannel(c)
		return true
	}
	select {
	case evicted := <-reply:
		if !evicted {
			return false
		}
	case <-c.done:
		// The actor exited before replying; the channel is closed either way,
		// so fall through and make sure it is no longer registered.
	}
	c.hub.removeChannel(c)
	return true
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
				c.releaseMemory()
				c.shutdown()
				c.hub.removeChannel(c)
				return
			}
		case <-c.done:
			return
		}
	}
}

// subscribe is called from Hub.Subscribe.
func (c *Channel) subscribe(remoteIP string, send func([]byte) bool) (*Subscription, error) {
	type resp struct {
		sub *Subscription
		err error
	}
	out := make(chan resp, 1)
	select {
	case c.inbox <- func() {
		// enforce per-channel and global participant caps
		if c.rule.Participants > 0 && len(c.participants) >= c.rule.Participants {
			out <- resp{err: errParticipantsCap}
			return
		}
		if err := c.hub.acquireParticipant(remoteIP); err != nil {
			out <- resp{err: err}
			return
		}
		id := c.nextID
		c.nextID++
		p := &participant{
			id:    id,
			ip:    remoteIP,
			send:  send,
			queue: make(chan []byte, DefaultQueueSize),
		}
		c.participants[id] = p
		if len(c.participants) == 1 {
			c.lastPart = time.Now()
		}
		// The writer goroutine replays retained messages first and only
		// then drains the queue, so this subscriber sees retained and live
		// messages in order without the actor ever blocking on a socket.
		go c.writer(p, c.buildReplay())
		cancel := func() {
			select {
			case c.inbox <- func() { c.removeParticipant(id) }:
			case <-c.done:
			}
		}
		out <- resp{sub: &Subscription{ID: id, Cancel: cancel}}
	}:
	case <-c.done:
		return nil, errChannelClosed
	}
	r := <-out
	return r.sub, r.err
}

// writer is the only goroutine that calls send for one participant. It exits
// when the subscription is cancelled (queue closed), the channel shuts down,
// or the subscriber's send reports failure.
func (c *Channel) writer(p *participant, replay [][]byte) {
	for _, body := range replay {
		if !p.send(body) {
			c.failParticipant(p)
			return
		}
	}
	for {
		select {
		case body, ok := <-p.queue:
			if !ok {
				return
			}
			if !p.send(body) {
				c.failParticipant(p)
				return
			}
		case <-c.done:
			return
		}
	}
}

// failParticipant marks p as gone and asks the actor to drop it immediately.
// Without the removal a quiet channel would keep the dead subscriber, and its
// global participant slot, until the next publish or the idle sweep. Removal
// is idempotent, so it is safe against a concurrent Cancel and against the
// fallback check in doPublish.
func (c *Channel) failParticipant(p *participant) {
	p.failed.Store(true)
	select {
	case c.inbox <- func() { c.removeParticipant(p.id) }:
	case <-c.done:
	}
}

// removeParticipant drops a subscriber and closes its queue so the writer
// goroutine exits. Actor-only.
func (c *Channel) removeParticipant(id SubID) {
	p, ok := c.participants[id]
	if !ok {
		return
	}
	delete(c.participants, id)
	c.hub.releaseParticipant(p.ip)
	close(p.queue)
	if len(c.participants) == 0 {
		c.lastPart = time.Now()
	}
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

// publish is called from Hub.Publish with a reservation for len(body) bytes
// held on the hub. It waits for the actor so the returned status describes
// this message rather than an estimate, then hands the reservation back: the
// actor has already moved the message's bytes into retained (minus whatever it
// trimmed), or kept none of them.
func (c *Channel) publish(body []byte, exclude SubID) Status {
	defer c.hub.unreserve(int64(len(body)))
	reply := make(chan Status, 1)
	select {
	case c.inbox <- func() { reply <- c.doPublish(body, exclude) }:
	case <-c.done:
		return Status{Reason: ReasonClosed}
	}
	select {
	case st := <-reply:
		return st
	case <-c.done:
		// The actor may have handled the message just before stopping.
		select {
		case st := <-reply:
			return st
		default:
			return Status{Reason: ReasonClosed}
		}
	}
}

func (c *Channel) doPublish(body []byte, exclude SubID) Status {
	if c.rule.MaxSize > 0 && int64(len(body)) > c.rule.MaxSize {
		c.dropped.Add(1)
		c.reportDrop(ReasonOversize)
		return Status{Reason: ReasonOversize}
	}
	// append to ring
	msg := &Message{Body: append([]byte(nil), body...), Timestamp: time.Now()}
	c.ring = append(c.ring, msg)
	max := c.rule.Messages
	if max <= 0 {
		max = DefaultMessages
	}
	var freed int64
	if len(c.ring) > max {
		for _, m := range c.ring[:len(c.ring)-max] {
			freed += int64(len(m.Body))
		}
		c.ring = c.ring[len(c.ring)-max:]
	}
	c.hub.addRetained(int64(len(msg.Body)) - freed)
	c.lastMsg = time.Now()

	// fan out without blocking: each participant owns a queue drained by
	// its own writer goroutine.
	status := Status{Enqueued: true}
	for id, p := range c.participants {
		if id == exclude {
			continue
		}
		if p.failed.Load() {
			c.removeParticipant(id)
			continue
		}
		if !p.enqueue(msg.Body) {
			c.dropped.Add(1)
			status.Dropped++
			c.reportDrop(ReasonSlowSubscriber)
		}
	}
	if len(c.participants) == 0 {
		c.lastPart = time.Now()
	}
	if status.Dropped > 0 {
		status.Reason = ReasonSlowSubscriber
	}
	return status
}

// reportDrop surfaces dropped messages, at most once per dropLogInterval per
// channel. Actor-only.
func (c *Channel) reportDrop(reason string) {
	log := c.hub.log
	if log == nil {
		return
	}
	now := time.Now()
	if !c.lastDropLog.IsZero() && now.Sub(c.lastDropLog) < dropLogInterval {
		return
	}
	c.lastDropLog = now
	log.Warnw("relay: dropped messages",
		"channel", c.name,
		"reason", reason,
		"dropped", c.dropped.Load())
}

// shouldClose reports whether the channel is eligible for sweeping. It runs
// on the actor goroutine, so it may read actor state directly; dead is set by
// shutdown from any goroutine, hence the lock.
func (c *Channel) shouldClose() bool {
	c.mu.Lock()
	dead := c.dead
	c.mu.Unlock()
	if dead {
		return false
	}
	if len(c.participants) > 0 {
		return false
	}
	if time.Since(c.lastPart) < c.rule.IdleTTL {
		return false
	}
	if len(c.ring) == 0 {
		return true
	}
	return time.Since(c.lastMsg) > c.rule.Retain
}
