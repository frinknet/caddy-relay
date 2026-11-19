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
	ip        string
	send      func([]byte) bool
	published [][]byte
	intercept func([]byte) ([]byte, bool)
	rule      *hub.ChannelRule
}

func (b *fakeBus) Rule(string) *hub.ChannelRule { return b.rule }

func (b *fakeBus) Subscribe(_ context.Context, _, remoteIP string, send func([]byte) bool) (*hub.Subscription, error) {
	b.mu.Lock()
	b.ip = remoteIP
	b.send = send
	b.mu.Unlock()
	return &hub.Subscription{ID: 1, Cancel: func() {}}, nil
}

func (b *fakeBus) Publish(_ context.Context, patterns []string, body []byte, _ hub.SubID) hub.Result {
	b.mu.Lock()
	b.published = append(b.published, body)
	b.mu.Unlock()
	return hub.Result{patterns[0]: hub.Status{Enqueued: true}}
}

func (b *fakeBus) Intercept(_ context.Context, _ *hub.InterceptSpec, body []byte, _ hub.InterceptInfo) ([]byte, bool) {
	if b.intercept != nil {
		return b.intercept(body)
	}
	return body, true
}

// subscriber waits for the handler to register its send func.
// subscribeIP returns the client host the transport passed to Subscribe.
func (b *fakeBus) subscribeIP() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ip
}

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
	// The subscription carries the client host, without its ephemeral port,
	// which is what max_participants_per_ip counts.
	if got := bus.subscribeIP(); got != "127.0.0.1" {
		t.Errorf("Subscribe remote IP = %q, want 127.0.0.1", got)
	}
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

// hideHijacker exposes Unwrap but not http.Hijacker, the way Caddy's
// response writers do. The handshake has to reach the hijack through the
// unwrap chain.
type hideHijacker struct {
	http.ResponseWriter
}

func (w hideHijacker) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func TestServeAcceptsWriterThatHidesHijacker(t *testing.T) {
	bus := &fakeBus{}
	cfg := Config{
		Subscribe: &hub.SubscribeSpec{Pattern: "room-*"},
		Publish:   &hub.PublishSpec{Pattern: "room-*"},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		Serve(cfg, bus, hideHijacker{w}, r)
	}))
	t.Cleanup(srv.Close)

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

func TestCheckOrigin(t *testing.T) {
	req := func(host, origin string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "http://"+host+"/room-1", nil)
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		return r
	}
	tests := []struct {
		name     string
		host     string
		origin   string
		patterns []string
		allowed  bool
	}{
		{"no origin", "relay.example:8080", "", nil, true},
		{"same origin", "relay.example:8080", "http://relay.example:8080", nil, true},
		{"same origin different case", "Relay.Example:8080", "http://relay.example:8080", nil, true},
		{"same host different port", "relay.example:8080", "http://relay.example:9090", nil, false},
		{"foreign origin without patterns", "relay.example:8080", "http://evil.example", nil, false},
		{"port-less pattern admits any port", "relay.example:8080", "https://evil.example:8443", []string{"evil.example"}, true},
		{"port-less pattern admits default port", "relay.example:8080", "https://evil.example", []string{"evil.example"}, true},
		{"wildcard subdomain on a ported origin", "relay.example:8080", "https://a.evil.example:8443", []string{"*.evil.example"}, true},
		{"wildcard subdomain on a nested host", "relay.example:8080", "https://a.b.evil.example:8443", []string{"*.evil.example"}, true},
		{"port-less pattern does not admit a lookalike", "relay.example:8080", "https://evil.example.net:8443", []string{"evil.example"}, false},
		{"explicit port pattern matches that port", "relay.example:8080", "https://evil.example:8443", []string{"evil.example:8443"}, true},
		{"explicit port pattern rejects another port", "relay.example:8080", "https://evil.example:9443", []string{"evil.example:8443"}, false},
		{"ipv6 origin with a port", "relay.example:8080", "http://[::1]:8443", []string{"[::1]"}, true},
		{"ipv6 explicit port", "relay.example:8080", "http://[::1]:9443", []string{"[::1]:8443"}, false},
		{"null origin", "relay.example:8080", "null", []string{"*"}, false},
		{"malformed origin", "relay.example:8080", "http://[::1", []string{"*"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := checkOrigin(req(tc.host, tc.origin), tc.patterns)
			if allowed := err == nil; allowed != tc.allowed {
				t.Fatalf("checkOrigin(%q, %q, %v) = %v, want allowed=%v", tc.host, tc.origin, tc.patterns, err, tc.allowed)
			}
		})
	}
}

// A port-less origin pattern admits the origin on any port, so an operator
// does not have to name the port or fall back to an allow-everything pattern.
func TestServeOriginPatternAdmitsPortedOrigin(t *testing.T) {
	bus := &fakeBus{}
	cfg := Config{
		Subscribe:      &hub.SubscribeSpec{Pattern: "room-*"},
		Publish:        &hub.PublishSpec{Pattern: "room-*"},
		OriginPatterns: []string{"evil.example"},
	}
	srv := serveTest(t, bus, cfg)
	origin := &websocket.DialOptions{HTTPHeader: http.Header{"Origin": {"https://evil.example:8443"}}}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/room-1", origin)
	if err != nil {
		t.Fatalf("dial with a ported allowlisted Origin: %v", err)
	}
	conn.Close(websocket.StatusNormalClosure, "")
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
