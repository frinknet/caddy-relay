package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/frinknet/caddy-relay/pkg/hub"
)

// fakeBus stands in for the hub so the transport can be tested on its own.
type fakeBus struct {
	mu        sync.Mutex
	send      func([]byte) bool
	published [][]byte
	intercept func([]byte) ([]byte, bool)
	rule      *hub.ChannelRule
}

func (b *fakeBus) Rule(string) *hub.ChannelRule { return b.rule }

func (b *fakeBus) Subscribe(_ context.Context, _ string, send func([]byte) bool) (*hub.Subscription, error) {
	b.mu.Lock()
	b.send = send
	b.mu.Unlock()
	return &hub.Subscription{ID: 1, Cancel: func() {}}, nil
}

func (b *fakeBus) Publish(_ context.Context, patterns []string, body []byte, _ hub.SubID) hub.Result {
	b.mu.Lock()
	b.published = append(b.published, body)
	b.mu.Unlock()
	return hub.Result{patterns[0]: true}
}

func (b *fakeBus) Intercept(_ context.Context, _ *hub.InterceptSpec, body []byte, _ hub.InterceptInfo) ([]byte, bool) {
	if b.intercept != nil {
		return b.intercept(body)
	}
	return body, true
}

// subscriber waits for the handler to register its send func.
func (b *fakeBus) subscriber(t *testing.T) func([]byte) bool {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		send := b.send
		b.mu.Unlock()
		if send != nil {
			return send
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("handler never subscribed")
	return nil
}

func (b *fakeBus) publishedBodies() [][]byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([][]byte(nil), b.published...)
}

func serveTest(t *testing.T, bus *fakeBus, cfg Config) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		Serve(cfg, bus, w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func dial(t *testing.T, srv *httptest.Server, path string) (context.Context, *websocket.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+path, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", path, err)
	}
	t.Cleanup(func() { conn.Close(websocket.StatusNormalClosure, "") })
	return ctx, conn
}

func TestServeDeliversAndPublishes(t *testing.T) {
	bus := &fakeBus{}
	srv := serveTest(t, bus, Config{
		Subscribe: &hub.SubscribeSpec{Pattern: "room-*"},
		Publish:   &hub.PublishSpec{Pattern: "room-*"},
	})
	ctx, conn := dial(t, srv, "/room-1")

	send := bus.subscriber(t)
	if !send([]byte("hello")) {
		t.Fatal("send reported failure")
	}
	_, msg, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(msg) != "hello" {
		t.Errorf("received %q, want hello", msg)
	}

	// A message from the client is relayed to the publish target.
	if err := conn.Write(ctx, websocket.MessageText, []byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(bus.publishedBodies()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	got := bus.publishedBodies()
	if len(got) != 1 || string(got[0]) != "ping" {
		t.Fatalf("published %q, want [ping]", got)
	}
}

func TestServeRejectsChannelOutsideSubscribePattern(t *testing.T) {
	bus := &fakeBus{}
	srv := serveTest(t, bus, Config{
		Subscribe: &hub.SubscribeSpec{Pattern: "room-*"},
		Publish:   &hub.PublishSpec{Pattern: "room-*"},
	})
	ctx, conn := dial(t, srv, "/lobby")
	if _, _, err := conn.Read(ctx); err == nil {
		t.Fatal("read succeeded for a channel outside the subscribe pattern")
	} else if got := websocket.CloseStatus(err); got != websocket.StatusPolicyViolation {
		t.Fatalf("close status = %v, want %v", got, websocket.StatusPolicyViolation)
	}
}

// A config with no publish target must be refused rather than panic once a
// client sends a message.
func TestServeWithoutPublishTargetIsRefused(t *testing.T) {
	bus := &fakeBus{}
	srv := serveTest(t, bus, Config{Subscribe: &hub.SubscribeSpec{Pattern: "room-*"}})
	ctx, conn := dial(t, srv, "/room-1")
	if _, _, err := conn.Read(ctx); err == nil {
		t.Fatal("read succeeded without a publish target")
	} else if got := websocket.CloseStatus(err); got != websocket.StatusPolicyViolation {
		t.Fatalf("close status = %v, want %v", got, websocket.StatusPolicyViolation)
	}
}

// subscribe is the channel allow-list; without it the transport must refuse
// rather than accept every channel.
func TestServeWithoutSubscribePatternIsRefused(t *testing.T) {
	bus := &fakeBus{}
	srv := serveTest(t, bus, Config{Publish: &hub.PublishSpec{Pattern: "room-*"}})
	ctx, conn := dial(t, srv, "/room-1")
	if _, _, err := conn.Read(ctx); err == nil {
		t.Fatal("read succeeded with no subscribe pattern")
	} else if got := websocket.CloseStatus(err); got != websocket.StatusPolicyViolation {
		t.Fatalf("close status = %v, want %v", got, websocket.StatusPolicyViolation)
	}
}

func TestRateLimitComesFromChannelRule(t *testing.T) {
	bus := &fakeBus{rule: &hub.ChannelRule{Pattern: "room-*", RateLimit: 250 * time.Millisecond}}
	if got := rateLimit(bus, "room-1"); got != 250*time.Millisecond {
		t.Errorf("rateLimit = %v, want 250ms", got)
	}
	bus.rule = &hub.ChannelRule{Pattern: "room-*"}
	if got := rateLimit(bus, "room-1"); got != time.Second {
		t.Errorf("rateLimit = %v, want the 1s default", got)
	}
	bus.rule = nil
	if got := rateLimit(bus, "room-1"); got != time.Second {
		t.Errorf("rateLimit = %v, want the 1s default", got)
	}
}

func TestServeOriginPolicy(t *testing.T) {
	bus := &fakeBus{}
	cfg := Config{
		Subscribe: &hub.SubscribeSpec{Pattern: "room-*"},
		Publish:   &hub.PublishSpec{Pattern: "room-*"},
	}
	foreign := &websocket.DialOptions{HTTPHeader: http.Header{"Origin": {"http://evil.example"}}}

	// Same-origin only by default, so a foreign Origin is refused.
	srv := serveTest(t, bus, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/room-1", foreign)
	if err == nil {
		conn.Close(websocket.StatusNormalClosure, "")
		t.Fatal("dial with a foreign Origin succeeded, want 403")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %v, want 403", resp)
	}

	// Allowlisting the origin pattern admits it.
	cfg.OriginPatterns = []string{"evil.example"}
	srv2 := serveTest(t, bus, cfg)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	conn2, _, err := websocket.Dial(ctx2, "ws"+strings.TrimPrefix(srv2.URL, "http")+"/room-1", foreign)
	if err != nil {
		t.Fatalf("dial with an allowlisted Origin: %v", err)
	}
	conn2.Close(websocket.StatusNormalClosure, "")
}

// A client that keeps sending well past its rate limit is closed instead of
// having its messages dropped forever.
func TestServeClosesClientThatIgnoresRateLimit(t *testing.T) {
	bus := &fakeBus{rule: &hub.ChannelRule{Pattern: "room-*", RateLimit: time.Hour}}
	srv := serveTest(t, bus, Config{
		Subscribe: &hub.SubscribeSpec{Pattern: "room-*"},
		Publish:   &hub.PublishSpec{Pattern: "room-*"},
	})
	ctx, conn := dial(t, srv, "/room-1")

	for i := 0; i < rateLimitBurst+2; i++ {
		if err := conn.Write(ctx, websocket.MessageText, []byte("spam")); err != nil {
			break
		}
	}
	if _, _, err := conn.Read(ctx); err == nil {
		t.Fatal("connection stayed open despite persistent rate-limit violations")
	} else if got := websocket.CloseStatus(err); got != websocket.StatusPolicyViolation {
		t.Fatalf("close status = %v, want %v", got, websocket.StatusPolicyViolation)
	}
}
