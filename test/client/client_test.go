// Package client drives the container fixture as a real protocol client.
//
// The shell harness can reach an HTTP handshake and a TLS listener with curl
// and openssl, but WebSocket frames and TURN allocations need a protocol
// client. These checks run against a container started by test/e2e.sh, sharing
// its network namespace, and are skipped everywhere else (including the unit
// layer), so `go test ./...` stays hermetic.
package client_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/pion/stun/v3"
	"github.com/pion/turn/v4"
)

// TestContainer exercises the running fixture. test/e2e.sh sets the
// CADDY_RELAY_E2E_* variables below and runs it in the builder image.
func TestContainer(t *testing.T) {
	base := os.Getenv("CADDY_RELAY_E2E_HTTP")
	if base == "" {
		t.Skip("CADDY_RELAY_E2E_HTTP is not set; run this check through test/e2e.sh")
	}

	t.Run("websocket frames", func(t *testing.T) {
		testWebSocketFrames(t, base)
	})
	t.Run("websocket rejected channel", func(t *testing.T) {
		testWebSocketRejectedChannel(t, base)
	})
	t.Run("turn allocation", func(t *testing.T) {
		testTURNAllocation(t)
	})
}

// testWebSocketFrames uses two browser-like connections: the writer publishes
// and the reader has to receive the message, while the writer must not receive
// its own message back (the hub excludes the sender from its own publish).
// A fresh channel name keeps a previous run's retained messages out of the
// assertion.
func testWebSocketFrames(t *testing.T, base string) {
	channel := fmt.Sprintf("room-e2e-%d", time.Now().UnixNano())
	url := "ws" + strings.TrimPrefix(base, "http") + "/ws/" + channel
	origin := base

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	reader := dial(t, ctx, url, origin)
	defer reader.Close(websocket.StatusNormalClosure, "")
	writer := dial(t, ctx, url, origin)
	defer writer.Close(websocket.StatusNormalClosure, "")

	payload := []byte(fmt.Sprintf(`{"text":"frame-%d"}`, time.Now().UnixNano()))
	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	defer wcancel()
	if err := writer.Write(wctx, websocket.MessageText, payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
	defer rcancel()
	typ, got, err := reader.Read(rctx)
	if err != nil {
		t.Fatalf("read published message: %v", err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("message type = %v, want text", typ)
	}
	if string(got) != string(payload) {
		t.Fatalf("received %q, want %q", got, payload)
	}

	nctx, ncancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer ncancel()
	if _, _, err := writer.Read(nctx); err == nil {
		t.Fatal("the writer received its own message; publish must exclude the sender")
	}
}

// testWebSocketRejectedChannel asserts the subscribe allow-list is enforced
// after the upgrade, where curl cannot see the close code.
func testWebSocketRejectedChannel(t *testing.T, base string) {
	url := "ws" + strings.TrimPrefix(base, "http") + "/ws/outside-1"
	origin := base

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn := dial(t, ctx, url, origin)
	defer conn.Close(websocket.StatusNormalClosure, "")

	_, _, err := conn.Read(ctx)
	if err == nil {
		t.Fatal("a channel outside subscribe was accepted")
	}
	if got := websocket.CloseStatus(err); got != websocket.StatusPolicyViolation {
		t.Fatalf("close status = %v, want %v (error %v)", got, websocket.StatusPolicyViolation, err)
	}
}

// testTURNAllocation allocates a relay with REST credentials and sends a STUN
// binding request through it, which proves both the allocation and the relayed
// data path reach the fixtures' listeners.
func testTURNAllocation(t *testing.T) {
	turnAddr := env(t, "CADDY_RELAY_E2E_TURN")
	stunAddr := env(t, "CADDY_RELAY_E2E_STUN")
	secret := env(t, "CADDY_RELAY_E2E_TURN_SECRET")
	minPort := envInt(t, "CADDY_RELAY_E2E_RELAY_MIN")
	maxPort := envInt(t, "CADDY_RELAY_E2E_RELAY_MAX")

	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("client socket: %v", err)
	}
	defer conn.Close()

	username, password, err := turn.GenerateLongTermTURNRESTCredentials(secret, "e2e", 10*time.Minute)
	if err != nil {
		t.Fatalf("credentials: %v", err)
	}
	c, err := turn.NewClient(&turn.ClientConfig{
		STUNServerAddr: stunAddr,
		TURNServerAddr: turnAddr,
		Conn:           conn,
		Username:       username,
		Password:       password,
	})
	if err != nil {
		t.Fatalf("turn client: %v", err)
	}
	defer c.Close()
	if err := c.Listen(); err != nil {
		t.Fatalf("turn client listen: %v", err)
	}

	relay, err := c.Allocate()
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	defer relay.Close()

	relayed, ok := relay.LocalAddr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("relay address %v is not UDP", relay.LocalAddr())
	}
	if got := relayed.IP.String(); got != "127.0.0.1" {
		t.Errorf("relay address = %s, want the configured external IP 127.0.0.1", got)
	}
	if relayed.Port < minPort || relayed.Port > maxPort {
		t.Errorf("relay port = %d, want %d-%d", relayed.Port, minPort, maxPort)
	}

	dst, err := net.ResolveUDPAddr("udp4", stunAddr)
	if err != nil {
		t.Fatalf("resolve STUN address %q: %v", stunAddr, err)
	}
	req := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	if err := relay.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set relay deadline: %v", err)
	}
	if _, err := relay.WriteTo(req.Raw, dst); err != nil {
		t.Fatalf("write through the relay: %v", err)
	}

	buf := make([]byte, 1500)
	n, from, err := relay.ReadFrom(buf)
	if err != nil {
		t.Fatalf("read through the relay: %v", err)
	}
	var res stun.Message
	res.Raw = buf[:n]
	if err := res.Decode(); err != nil {
		t.Fatalf("decode STUN response from %v: %v", from, err)
	}
	if res.Type != stun.BindingSuccess {
		t.Fatalf("relayed response type = %v, want %v", res.Type, stun.BindingSuccess)
	}
	if _, err := res.Get(stun.AttrXORMappedAddress); err != nil {
		t.Fatalf("relayed response has no XOR-MAPPED-ADDRESS: %v", err)
	}
}

func dial(t *testing.T, ctx context.Context, url, origin string) *websocket.Conn {
	t.Helper()
	conn, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": {origin}},
	})
	if err != nil {
		status := "no response"
		if resp != nil {
			status = resp.Status
		}
		t.Fatalf("dial %s: %v (%s)", url, err, status)
	}
	return conn
}

func env(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Fatalf("%s is not set", key)
	}
	return v
}

func envInt(t *testing.T, key string) int {
	t.Helper()
	v, err := strconv.Atoi(env(t, key))
	if err != nil {
		t.Fatalf("%s is not a port: %v", key, err)
	}
	return v
}
