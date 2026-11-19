package hub

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// sink is a thread-safe send target for tests; send runs on the writer
// goroutine, so access must be synchronised.
type sink struct {
	mu   sync.Mutex
	msgs []string
}

func (s *sink) send(body []byte) bool {
	s.mu.Lock()
	s.msgs = append(s.msgs, string(body))
	s.mu.Unlock()
	return true
}

// wait blocks until n messages have arrived or d elapses, then returns a copy.
func (s *sink) wait(n int, d time.Duration) []string {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		got := len(s.msgs)
		s.mu.Unlock()
		if got >= n {
			break
		}
		time.Sleep(time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.msgs...)
}

func (s *sink) all() []string { return s.wait(0, 0) }

// snapshot reads actor state from inside the actor goroutine.
func (c *Channel) snapshot() (ring, participants int) {
	out := make(chan [2]int, 1)
	select {
	case c.inbox <- func() { out <- [2]int{len(c.ring), len(c.participants)} }:
	case <-c.done:
		return 0, 0
	}
	r := <-out
	return r[0], r[1]
}

func TestPatternMatching(t *testing.T) {
	h := NewHub(ChannelsConfig{Rules: []*ChannelRule{
		{Pattern: "room-*"},
		{Pattern: "lobby"},
	}})
	ctx := context.Background()
	for _, name := range []string{"room-1", "room-", "lobby"} {
		sub, err := h.Subscribe(ctx, name, func([]byte) bool { return true })
		if err != nil {
			t.Fatalf("Subscribe(%q) = %v, want nil", name, err)
		}
		sub.Cancel()
	}
	for _, name := range []string{"rooms", "other-room", "room1", "lobby-1"} {
		if _, err := h.Subscribe(ctx, name, func([]byte) bool { return true }); err == nil {
			t.Errorf("Subscribe(%q) succeeded, want not-allowed error", name)
		}
	}
}

func TestRingIsBoundedWithoutMessagesRule(t *testing.T) {
	h := NewHub(ChannelsConfig{Rules: []*ChannelRule{{Pattern: "lobby"}}})
	ctx := context.Background()
	if got := h.cfg.Rules[0].Messages; got != DefaultMessages {
		t.Fatalf("Messages = %d, want default %d", got, DefaultMessages)
	}
	for i := 0; i < DefaultMessages+25; i++ {
		h.Publish(ctx, []string{"lobby"}, []byte("m"), 0)
	}
	ch, err := h.getOrCreate("lobby")
	if err != nil {
		t.Fatal(err)
	}
	if ring, _ := ch.snapshot(); ring != DefaultMessages {
		t.Errorf("ring holds %d messages, want %d", ring, DefaultMessages)
	}
}

func TestReplayIsCappedByMessagesRule(t *testing.T) {
	h := NewHub(ChannelsConfig{Rules: []*ChannelRule{{Pattern: "lobby", Messages: 3}}})
	ctx := context.Background()
	for _, m := range []string{"1", "2", "3", "4", "5"} {
		h.Publish(ctx, []string{"lobby"}, []byte(m), 0)
	}
	s := &sink{}
	sub, err := h.Subscribe(ctx, "lobby", s.send)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Cancel()
	got := s.wait(3, 2*time.Second)
	want := []string{"3", "4", "5"}
	if len(got) != len(want) {
		t.Fatalf("replay = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("replay = %v, want %v", got, want)
		}
	}
}

func TestReplayPrecedesLiveMessages(t *testing.T) {
	h := NewHub(ChannelsConfig{Rules: []*ChannelRule{{Pattern: "lobby", Messages: 10}}})
	ctx := context.Background()
	h.Publish(ctx, []string{"lobby"}, []byte("a"), 0)
	h.Publish(ctx, []string{"lobby"}, []byte("b"), 0)

	s := &sink{}
	sub, err := h.Subscribe(ctx, "lobby", s.send)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Cancel()
	h.Publish(ctx, []string{"lobby"}, []byte("c"), 0)

	got := s.wait(3, 2*time.Second)
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestOversizeMessageIsDropped(t *testing.T) {
	h := NewHub(ChannelsConfig{Rules: []*ChannelRule{{Pattern: "lobby", MaxSize: 4}}})
	ctx := context.Background()
	h.Publish(ctx, []string{"lobby"}, []byte("12345"), 0)

	s := &sink{}
	sub, err := h.Subscribe(ctx, "lobby", s.send)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Cancel()
	if got := s.all(); len(got) != 0 {
		t.Errorf("delivered %v, want nothing", got)
	}
	ch, _ := h.getOrCreate("lobby")
	if ring, _ := ch.snapshot(); ring != 0 {
		t.Errorf("ring holds %d messages, want 0", ring)
	}
}

func TestSenderIsExcluded(t *testing.T) {
	h := NewHub(ChannelsConfig{Rules: []*ChannelRule{{Pattern: "lobby", Messages: 10}}})
	ctx := context.Background()
	sa, sb := &sink{}, &sink{}
	subA, err := h.Subscribe(ctx, "lobby", sa.send)
	if err != nil {
		t.Fatal(err)
	}
	defer subA.Cancel()
	subB, err := h.Subscribe(ctx, "lobby", sb.send)
	if err != nil {
		t.Fatal(err)
	}
	defer subB.Cancel()

	h.Publish(ctx, []string{"lobby"}, []byte("hi"), subA.ID)

	if got := sb.wait(1, 2*time.Second); len(got) != 1 || got[0] != "hi" {
		t.Errorf("other subscriber got %v, want [hi]", got)
	}
	if got := sa.all(); len(got) != 0 {
		t.Errorf("sender received its own message: %v", got)
	}
}

func TestParticipantsCap(t *testing.T) {
	h := NewHub(ChannelsConfig{Rules: []*ChannelRule{{Pattern: "lobby", Participants: 1}}})
	ctx := context.Background()
	sub, err := h.Subscribe(ctx, "lobby", func([]byte) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Cancel()
	if _, err := h.Subscribe(ctx, "lobby", func([]byte) bool { return true }); err == nil {
		t.Fatal("second subscriber accepted, want participants cap error")
	}
}

func TestMaxChannels(t *testing.T) {
	h := NewHub(ChannelsConfig{
		MaxChannels: 1,
		Rules:       []*ChannelRule{{Pattern: "room-*"}},
	})
	ctx := context.Background()
	sub, err := h.Subscribe(ctx, "room-1", func([]byte) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Cancel()
	if _, err := h.Subscribe(ctx, "room-2", func([]byte) bool { return true }); err == nil {
		t.Fatal("second channel created, want max_channels error")
	}
}

// A subscriber that stops reading must not stall the channel: publishes have
// to keep returning and the subscriber's queue has to drop.
func TestStalledSubscriberDoesNotBlockPublish(t *testing.T) {
	h := NewHub(ChannelsConfig{Rules: []*ChannelRule{{
		Pattern: "lobby", Messages: 10, IdleTTL: time.Minute,
	}}})
	ctx := context.Background()

	release := make(chan struct{})
	blocked := make(chan struct{}, 1)
	sub, err := h.Subscribe(ctx, "lobby", func([]byte) bool {
		select {
		case blocked <- struct{}{}:
		default:
		}
		<-release
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	h.Publish(ctx, []string{"lobby"}, []byte("first"), 0)
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber never received the first message")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < DefaultQueueSize*4; i++ {
			h.Publish(ctx, []string{"lobby"}, []byte("burst"), 0)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("Publish blocked on a stalled subscriber")
	}

	close(release)
	sub.Cancel()

	ch, err := h.getOrCreate("lobby")
	if err != nil {
		t.Fatal(err)
	}
	if ch.Dropped() == 0 {
		t.Error("Dropped() = 0, want > 0 after overflowing a stalled subscriber")
	}
}

// A publish reports per channel whether the message was enqueued and why it
// was not, so a caller can react without reading the log.
func TestPublishResultReportsStatus(t *testing.T) {
	h := NewHub(ChannelsConfig{Rules: []*ChannelRule{{
		Pattern: "lobby", Messages: 10, MaxSize: 8, IdleTTL: time.Minute,
	}}})
	ctx := context.Background()

	res := h.Publish(ctx, []string{"lobby"}, []byte("ok"), 0)
	if !res.Enqueued("lobby") || !res.OK() || res.Dropped() != 0 {
		t.Fatalf("publish status = %+v, want enqueued without drops", res["lobby"])
	}
	if got := res.String(); got != "" {
		t.Errorf("Result.String() = %q, want empty", got)
	}

	res = h.Publish(ctx, []string{"lobby"}, []byte("far too long"), 0)
	if res.Enqueued("lobby") || res.OK() {
		t.Errorf("oversize publish reported success: %+v", res["lobby"])
	}
	if got := res["lobby"].Reason; got != ReasonOversize {
		t.Errorf("oversize reason = %q, want %q", got, ReasonOversize)
	}

	res = h.Publish(ctx, []string{"other"}, []byte("x"), 0)
	if got := res["other"].Reason; got != ReasonUnavailable {
		t.Errorf("unavailable reason = %q, want %q", got, ReasonUnavailable)
	}
}

// A slow subscriber's drops show up in the publish result, not only in the
// channel's lifetime counter.
func TestPublishResultCountsSlowSubscriberDrops(t *testing.T) {
	h := NewHub(ChannelsConfig{Rules: []*ChannelRule{{
		Pattern: "lobby", Messages: 100, IdleTTL: time.Minute,
	}}})
	ctx := context.Background()

	release := make(chan struct{})
	blocked := make(chan struct{}, 1)
	sub, err := h.Subscribe(ctx, "lobby", func([]byte) bool {
		select {
		case blocked <- struct{}{}:
		default:
		}
		<-release
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { close(release); sub.Cancel() }()

	h.Publish(ctx, []string{"lobby"}, []byte("first"), 0)
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber never received the first message")
	}

	dropped := 0
	for i := 0; i < DefaultQueueSize*2 && dropped == 0; i++ {
		res := h.Publish(ctx, []string{"lobby"}, []byte("burst"), 0)
		if res["lobby"].Reason != "" && res["lobby"].Reason != ReasonSlowSubscriber {
			t.Fatalf("reason = %q, want %q or empty", res["lobby"].Reason, ReasonSlowSubscriber)
		}
		dropped = res.Dropped()
	}
	if dropped == 0 {
		t.Fatal("publish Result never reported a drop for a stalled subscriber")
	}
	if h.Dropped() == 0 {
		t.Error("Hub.Dropped() = 0, want > 0 after a reported drop")
	}
}

func TestIdleChannelIsSweptAndRecreated(t *testing.T) {
	h := NewHub(ChannelsConfig{Rules: []*ChannelRule{{
		Pattern: "lobby", Messages: 5, IdleTTL: time.Millisecond,
	}}})
	ctx := context.Background()
	h.Start()
	defer h.Stop()

	sub, err := h.Subscribe(ctx, "lobby", func([]byte) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	sub.Cancel()

	deadline := time.Now().Add(5 * time.Second)
	for {
		h.mu.Lock()
		n := len(h.channels)
		h.mu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("idle channel was never removed from the hub")
		}
		time.Sleep(25 * time.Millisecond)
	}

	// The name must be usable again: a swept channel cannot be left behind.
	s := &sink{}
	sub2, err := h.Subscribe(ctx, "lobby", s.send)
	if err != nil {
		t.Fatalf("Subscribe after sweep: %v", err)
	}
	defer sub2.Cancel()
	h.Publish(ctx, []string{"lobby"}, []byte("back"), 0)
	if got := s.wait(1, 2*time.Second); len(got) != 1 || got[0] != "back" {
		t.Errorf("after sweep got %v, want [back]", got)
	}
}

func TestStopClosesChannels(t *testing.T) {
	h := NewHub(ChannelsConfig{Rules: []*ChannelRule{{Pattern: "lobby"}}})
	ctx := context.Background()
	s := &sink{}
	sub, err := h.Subscribe(ctx, "lobby", s.send)
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	h.Stop()

	h.mu.Lock()
	n := len(h.channels)
	h.mu.Unlock()
	if n != 0 {
		t.Errorf("hub still tracks %d channels after Stop", n)
	}
	// The stopped subscription's channel is gone, so nothing is delivered to it.
	h.Publish(ctx, []string{"lobby"}, []byte("x"), 0)
	if got := s.wait(1, 200*time.Millisecond); len(got) != 0 {
		t.Errorf("stopped subscriber received %v", got)
	}
	sub.Cancel() // must not block or panic after Stop
}

// waitRetained blocks until the hub reports want bytes retained.
func waitRetained(t *testing.T, h *Hub, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h.Retained() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("hub retained %d bytes, want %d", h.Retained(), want)
}

func TestMaxMemory(t *testing.T) {
	ctx := context.Background()
	body := bytes.Repeat([]byte("x"), 60)
	rule := func() *ChannelRule {
		return &ChannelRule{Pattern: "room-*", Messages: 100, IdleTTL: time.Minute}
	}

	t.Run("refused when every channel has participants", func(t *testing.T) {
		h := NewHub(ChannelsConfig{Rules: []*ChannelRule{rule()}, MaxMemory: 100})
		sub, err := h.Subscribe(ctx, "room-1", func([]byte) bool { return true })
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Cancel()

		if res := h.Publish(ctx, []string{"room-1"}, body, 0); !res.Enqueued("room-1") {
			t.Fatal("first publish was refused")
		}
		waitRetained(t, h, 60)
		if res := h.Publish(ctx, []string{"room-1"}, body, 0); res.Enqueued("room-1") {
			t.Error("publish was accepted over max_memory with a busy channel")
		} else if got := res["room-1"].Reason; got != ReasonMemory {
			t.Errorf("reason = %q, want %q", got, ReasonMemory)
		}
	})

	t.Run("evicts an idle channel to make room", func(t *testing.T) {
		h := NewHub(ChannelsConfig{Rules: []*ChannelRule{rule()}, MaxMemory: 100})
		if res := h.Publish(ctx, []string{"room-a"}, body, 0); !res.Enqueued("room-a") {
			t.Fatal("publish to room-a was refused")
		}
		waitRetained(t, h, 60)

		if res := h.Publish(ctx, []string{"room-b"}, body, 0); !res.Enqueued("room-b") {
			t.Error("publish to room-b was refused instead of evicting an idle channel")
		}
		h.mu.Lock()
		_, exists := h.channels["room-a"]
		h.mu.Unlock()
		if exists {
			t.Error("idle room-a was not evicted to honour max_memory")
		}
	})

	t.Run("trimming the ring frees budget", func(t *testing.T) {
		h := NewHub(ChannelsConfig{
			Rules:     []*ChannelRule{{Pattern: "room-1", Messages: 2, IdleTTL: time.Minute}},
			MaxMemory: 1000,
		})
		for i := 0; i < 3; i++ {
			h.Publish(ctx, []string{"room-1"}, body, 0)
		}
		waitRetained(t, h, 120)
	})

	t.Run("concurrent publishes cannot overshoot", func(t *testing.T) {
		const (
			maxMemory = 5000
			size      = 100
			senders   = 8
			perSender = 25
		)
		h := NewHub(ChannelsConfig{
			Rules:     []*ChannelRule{{Pattern: "room-1", Messages: 1000, IdleTTL: time.Minute}},
			MaxMemory: maxMemory,
		})
		sub, err := h.Subscribe(ctx, "room-1", func([]byte) bool { return true })
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Cancel()

		msg := bytes.Repeat([]byte("x"), size)
		var accepted atomic.Int64
		var wg sync.WaitGroup
		for i := 0; i < senders; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < perSender; j++ {
					if h.Publish(ctx, []string{"room-1"}, msg, 0).Enqueued("room-1") {
						accepted.Add(1)
					}
				}
			}()
		}
		wg.Wait()

		if got := accepted.Load() * size; got > maxMemory {
			t.Errorf("accepted %d bytes over a %d byte budget", got, maxMemory)
		}
		if got := h.Retained(); got > maxMemory {
			t.Errorf("retained %d bytes over a %d byte budget", got, maxMemory)
		}
		if got := h.reserved.Load(); got != 0 {
			t.Errorf("%d bytes still reserved after every publish returned", got)
		}
	})

	t.Run("closing a channel releases its bytes", func(t *testing.T) {
		h := NewHub(ChannelsConfig{
			Rules:     []*ChannelRule{{Pattern: "room-1", Messages: 10, IdleTTL: time.Millisecond}},
			MaxMemory: 1000,
		})
		h.Start()
		defer h.Stop()
		h.Publish(ctx, []string{"room-1"}, body, 0)
		waitRetained(t, h, 60)
		waitRetained(t, h, 0)
	})
}

// recLogger records hub events for assertions.
type recLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (l *recLogger) Warnw(msg string, kv ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.msgs = append(l.msgs, msg+" "+fmt.Sprint(kv...))
}

func (l *recLogger) records() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.msgs...)
}

func (l *recLogger) wait(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if recs := l.records(); len(recs) >= n {
			return recs
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("only %d log records, want %d", len(l.records()), n)
	return nil
}

func TestOversizeDropIsReported(t *testing.T) {
	h := NewHub(ChannelsConfig{Rules: []*ChannelRule{{Pattern: "lobby", MaxSize: 4}}})
	log := &recLogger{}
	h.SetLogger(log)

	h.Publish(context.Background(), []string{"lobby"}, []byte("12345"), 0)
	recs := log.wait(t, 1)
	if !strings.Contains(recs[0], "oversize") {
		t.Errorf("log record = %q, want it to mention oversize", recs[0])
	}
}

func TestSlowSubscriberDropIsReported(t *testing.T) {
	h := NewHub(ChannelsConfig{Rules: []*ChannelRule{{
		Pattern: "lobby", Messages: 10, IdleTTL: time.Minute,
	}}})
	log := &recLogger{}
	h.SetLogger(log)
	ctx := context.Background()

	release := make(chan struct{})
	blocked := make(chan struct{}, 1)
	sub, err := h.Subscribe(ctx, "lobby", func([]byte) bool {
		select {
		case blocked <- struct{}{}:
		default:
		}
		<-release
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { close(release); sub.Cancel() }()

	h.Publish(ctx, []string{"lobby"}, []byte("first"), 0)
	<-blocked
	for i := 0; i < DefaultQueueSize*2; i++ {
		h.Publish(ctx, []string{"lobby"}, []byte("burst"), 0)
	}
	recs := log.wait(t, 1)
	if !strings.Contains(recs[0], "slow subscriber") {
		t.Errorf("log record = %q, want it to mention a slow subscriber", recs[0])
	}
}

func TestMaxParticipants(t *testing.T) {
	h := NewHub(ChannelsConfig{
		MaxParticipants: 1,
		Rules:           []*ChannelRule{{Pattern: "room-*", Participants: 10}},
	})
	ctx := context.Background()

	first, err := h.Subscribe(ctx, "room-1", func([]byte) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if got := h.Participants(); got != 1 {
		t.Fatalf("Participants() = %d, want 1", got)
	}
	// The cap is global: a different channel must not hand out a second slot.
	if _, err := h.Subscribe(ctx, "room-2", func([]byte) bool { return true }); err == nil {
		t.Error("a second subscriber was accepted past max_participants")
	}

	first.Cancel()
	deadline := time.Now().Add(3 * time.Second)
	for h.Participants() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("Participants() = %d after Cancel, want 0", h.Participants())
		}
		time.Sleep(time.Millisecond)
	}
	second, err := h.Subscribe(ctx, "room-2", func([]byte) bool { return true })
	if err != nil {
		t.Fatalf("slot was not released: %v", err)
	}
	second.Cancel()
}

func TestFailedSubscriberIsReapedWithoutNextPublish(t *testing.T) {
	h := NewHub(ChannelsConfig{
		MaxParticipants: 5,
		Rules:           []*ChannelRule{{Pattern: "room-*", Messages: 5}},
	})
	ctx := context.Background()
	// Retain a message so the subscriber's replay call fails as soon as the
	// writer goroutine starts.
	if res := h.Publish(ctx, []string{"room-1"}, []byte("retained"), 0); !res.OK() {
		t.Fatalf("setup publish did not enqueue: %+v", res)
	}
	sub, err := h.Subscribe(ctx, "room-1", func([]byte) bool { return false })
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Cancel()

	deadline := time.Now().Add(3 * time.Second)
	for h.Participants() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("Participants() = %d, want the failed subscriber reaped with no further publish", h.Participants())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestMaxParticipantsReleasedOnStop(t *testing.T) {
	h := NewHub(ChannelsConfig{
		MaxParticipants: 5,
		Rules:           []*ChannelRule{{Pattern: "room-*"}},
	})
	sub, err := h.Subscribe(context.Background(), "room-1", func([]byte) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	h.Stop()
	if got := h.Participants(); got != 0 {
		t.Errorf("Participants() = %d after Stop, want 0", got)
	}
	sub.Cancel()
}
