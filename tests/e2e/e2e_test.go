// Package e2e drives the plugin end to end through real clients.
//
// e2e_test.go runs Caddy in-process from a generated Caddyfile and covers the
// adapter, provisioning, listeners and both transports. docker_test.go runs
// the built image in a container with tests/e2e/Caddyfile and covers the same
// ground plus STUN, TURN, TURN over TLS and WebSocket over TLS.
package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/coder/websocket"

	// Register the relay plugin's modules and directives.
	_ "github.com/frinknet/caddy-relay"
)

// globalBlock is the relay app config used by every test: one room-* rule, no
// STUN/TURN listeners, and no admin endpoint (so tests do not fight over :2019).
const globalBlock = `{
	admin off
	relay {
		channels {
			max_channels 100
			room-* {
				messages     10
				participants 10
				idle_ttl     1m
				retain       1m
			}
		}
	}
}
`

// runCaddy adapts and starts a Caddyfile, stopping Caddy when the test ends.
func runCaddy(t *testing.T, caddyfile string) {
	t.Helper()
	adapter := caddyconfig.GetAdapter("caddyfile")
	if adapter == nil {
		t.Fatal("caddyfile adapter is not registered")
	}
	raw, warnings, err := adapter.Adapt([]byte(caddyfile), nil)
	if err != nil {
		t.Fatalf("adapt: %v", err)
	}
	for _, w := range warnings {
		t.Logf("adapt warning: %v", w)
	}
	if err := caddy.Load(raw, false); err != nil {
		t.Fatalf("caddy.Load: %v", err)
	}
	t.Cleanup(func() {
		if err := caddy.Stop(); err != nil {
			t.Errorf("caddy.Stop: %v", err)
		}
	})
}

// wsSite builds a site that serves relay_ws under /ws/* with the given
// intercept, and relay_sse under /events/*.
func wsSite(port int, intercept string) string {
	return fmt.Sprintf(globalBlock+`
http://127.0.0.1:%d {
	handle /ws/* {
		relay_ws {
			subscribe room-*
			publish   room-*
%s
		}
	}
	handle /events/* {
		relay_sse {
			subscribe room-*
			publish   room-*
		}
	}
}
`, port, intercept)
}

func TestWebSocketTemplateIntercept(t *testing.T) {
	port := freePort(t, "tcp")
	runCaddy(t, wsSite(port, "\t\t\tintercept {\n\t\t\t\ttemplate {\n\t\t\t\t\t{{ message }}\n\t\t\t\t}\n\t\t\t}"))

	alice := dialWS(t, wsURL(port, "room-1"), nil)
	bob := dialWS(t, wsURL(port, "room-1"), nil)

	sendWS(t, alice, "hello from alice")
	if got, err := readWS(t, bob); err != nil {
		t.Fatalf("bob read: %v", err)
	} else if got != "hello from alice" {
		t.Errorf("bob received %q, want the rendered body", got)
	}

	// Alice sent it, so the intercept's publish excludes her.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, data, err := alice.Read(ctx); err == nil {
		t.Errorf("sender received its own message: %q", data)
	}
}

// URL intercept mode: the message body is POSTed to an external endpoint and
// whatever it returns becomes the published body.
func TestWebSocketURLIntercept(t *testing.T) {
	seen := make(chan string, 4)
	interceptor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		seen <- string(body)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"rewritten":%s}`, body)
	}))
	defer interceptor.Close()

	port := freePort(t, "tcp")
	runCaddy(t, wsSite(port, "\t\t\tintercept "+interceptor.URL))

	alice := dialWS(t, wsURL(port, "room-1"), nil)
	bob := dialWS(t, wsURL(port, "room-1"), nil)

	sendWS(t, alice, `{"n":1}`)
	if got, err := readWS(t, bob); err != nil {
		t.Fatalf("bob read: %v", err)
	} else if got != `{"rewritten":{"n":1}}` {
		t.Errorf("bob received %q, want the intercepted body", got)
	}
	select {
	case got := <-seen:
		if got != `{"n":1}` {
			t.Errorf("intercept endpoint received %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Error("the intercept endpoint was never called")
	}
}

// SSE: a POST publishes, a live GET stream receives it, and a GET that joins
// afterwards is caught up by the ring buffer.
func TestSSEStreamPostAndReplay(t *testing.T) {
	port := freePort(t, "tcp")
	runCaddy(t, wsSite(port, ""))
	base := fmt.Sprintf("http://127.0.0.1:%d/events/room-1", port)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}
	br := bufio.NewReader(resp.Body)

	post, err := http.Post(base, "text/plain", strings.NewReader("live message"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	post.Body.Close()
	if post.StatusCode != http.StatusAccepted {
		t.Fatalf("POST status = %d, want 202", post.StatusCode)
	}

	if got := nextSSEData(t, br); got != "live message" {
		t.Errorf("stream received %q, want live message", got)
	}

	// A late subscriber is replayed the retained message.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	req2, err := http.NewRequestWithContext(ctx2, http.MethodGet, base, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("second GET: %v", err)
	}
	defer resp2.Body.Close()
	if got := nextSSEData(t, bufio.NewReader(resp2.Body)); got != "live message" {
		t.Errorf("replayed %q, want live message", got)
	}
}

// A channel outside the subscribe pattern is refused, and the refusal happens
// before any stream is opened.
func TestRejectedChannel(t *testing.T) {
	port := freePort(t, "tcp")
	runCaddy(t, wsSite(port, ""))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL(port, "other-1"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	if _, _, err := conn.Read(ctx); err == nil {
		t.Error("a channel outside the subscribe pattern was accepted")
	} else if got := websocket.CloseStatus(err); got != websocket.StatusPolicyViolation {
		t.Errorf("close status = %v, want %v", got, websocket.StatusPolicyViolation)
	}

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/events/other-1", port))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("SSE GET status = %d, want 403", resp.StatusCode)
	}
}

func TestAdaptedConfigIsWellFormed(t *testing.T) {
	raw, _, err := caddyconfig.GetAdapter("caddyfile").Adapt([]byte(wsSite(8080, "")), nil)
	if err != nil {
		t.Fatalf("adapt: %v", err)
	}
	var cfg struct {
		Apps map[string]json.RawMessage `json:"apps"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, app := range []string{"relay", "http"} {
		if _, ok := cfg.Apps[app]; !ok {
			t.Errorf("apps.%s is missing", app)
		}
	}
}
