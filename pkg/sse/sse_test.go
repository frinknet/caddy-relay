package sse

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frinknet/caddy-relay/pkg/hub"
)

// fakeBus stands in for the hub so the transport can be tested on its own.
type fakeBus struct {
	mu          sync.Mutex
	send        func([]byte) bool
	onSubscribe func(send func([]byte) bool)
	published   [][]byte
}

func (b *fakeBus) Rule(string) *hub.ChannelRule { return nil }

func (b *fakeBus) Subscribe(_ context.Context, _ string, send func([]byte) bool) (*hub.Subscription, error) {
	b.mu.Lock()
	b.send = send
	onSub := b.onSubscribe
	b.mu.Unlock()
	if onSub != nil {
		onSub(send)
	}
	return &hub.Subscription{ID: 1, Cancel: func() {}}, nil
}

func (b *fakeBus) Publish(_ context.Context, patterns []string, body []byte, _ hub.SubID) hub.Result {
	b.mu.Lock()
	b.published = append(b.published, body)
	b.mu.Unlock()
	return hub.Result{patterns[0]: hub.Status{Enqueued: true}}
}

func (b *fakeBus) Intercept(_ context.Context, _ *hub.InterceptSpec, body []byte, _ hub.InterceptInfo) ([]byte, bool) {
	return body, true
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

// nextData reads SSE frames until one carries a data line.
func nextData(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("reading stream: %v", err)
		}
		if data, ok := strings.CutPrefix(line, "data: "); ok {
			return strings.TrimSuffix(data, "\n")
		}
	}
}

func TestGetStreamsRetainedThenLive(t *testing.T) {
	bus := &fakeBus{}
	bus.onSubscribe = func(send func([]byte) bool) { send([]byte("retained")) }
	srv := serveTest(t, bus, Config{Subscribe: &hub.SubscribeSpec{Pattern: "room-*"}})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/room-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	br := bufio.NewReader(resp.Body)

	if got := nextData(t, br); got != "retained" {
		t.Errorf("first frame = %q, want retained", got)
	}

	send := bus.subscriber(t)
	if !send([]byte("live")) {
		t.Fatal("send reported failure")
	}
	if got := nextData(t, br); got != "live" {
		t.Errorf("second frame = %q, want live", got)
	}
}

func TestPostPublishes(t *testing.T) {
	bus := &fakeBus{}
	srv := serveTest(t, bus, Config{
		Subscribe: &hub.SubscribeSpec{Pattern: "room-*"},
		Publish:   &hub.PublishSpec{Pattern: "room-*"},
	})
	resp, err := http.Post(srv.URL+"/room-1", "text/plain", strings.NewReader("hi"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	got := bus.publishedBodies()
	if len(got) != 1 || string(got[0]) != "hi" {
		t.Fatalf("published %q, want [hi]", got)
	}
}

// POST needs a publish target; GET is a legitimate read-only stream.
func TestPostWithoutPublishTargetFails(t *testing.T) {
	bus := &fakeBus{}
	srv := serveTest(t, bus, Config{Subscribe: &hub.SubscribeSpec{Pattern: "room-*"}})
	resp, err := http.Post(srv.URL+"/room-1", "text/plain", strings.NewReader("hi"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
}

func TestPostRejectsChannelOutsideSubscribePattern(t *testing.T) {
	bus := &fakeBus{}
	srv := serveTest(t, bus, Config{
		Subscribe: &hub.SubscribeSpec{Pattern: "room-*"},
		Publish:   &hub.PublishSpec{Pattern: "room-*"},
	})
	resp, err := http.Post(srv.URL+"/lobby", "text/plain", strings.NewReader("hi"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

// Without a subscribe pattern the transport must fail closed instead of
// accepting every channel.
func TestNoSubscribePatternFailsClosed(t *testing.T) {
	bus := &fakeBus{}
	srv := serveTest(t, bus, Config{Publish: &hub.PublishSpec{Pattern: "room-*"}})

	resp, err := http.Post(srv.URL+"/room-1", "text/plain", strings.NewReader("hi"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("POST status = %d, want 403", resp.StatusCode)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/room-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	getResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer getResp.Body.Close()
	// The refusal happens before the stream's status line, so it is a plain
	// 403 rather than a 200 that opens with an error frame.
	if getResp.StatusCode != http.StatusForbidden {
		t.Errorf("GET status = %d, want 403", getResp.StatusCode)
	}
}
