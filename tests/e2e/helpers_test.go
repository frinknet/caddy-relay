package e2e

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/pion/turn/v4"
)

// Shared helpers for the in-process suite (e2e_test.go) and the container
// suite (docker_test.go).

// freePort returns a port that was free a moment ago, for the given network.
func freePort(t *testing.T, network string) int {
	t.Helper()
	if strings.HasPrefix(network, "udp") {
		conn, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("finding a free udp port: %v", err)
		}
		defer conn.Close()
		return conn.LocalAddr().(*net.UDPAddr).Port
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free tcp port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// dialWS opens a WebSocket, optionally through a prepared HTTP client (used
// for TLS against Caddy's internal CA).
func dialWS(t *testing.T, url string, client *http.Client) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	opts := &websocket.DialOptions{}
	if client != nil {
		opts.HTTPClient = client
	}
	conn, _, err := websocket.Dial(ctx, url, opts)
	if err != nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	t.Cleanup(func() { conn.Close(websocket.StatusNormalClosure, "") })
	return conn
}

func readWS(t *testing.T, conn *websocket.Conn) (string, error) {
	t.Helper()
	return readWSWithin(t, conn, 5*time.Second)
}

func readWSWithin(t *testing.T, conn *websocket.Conn, d time.Duration) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	_, data, err := conn.Read(ctx)
	return string(data), err
}

func sendWS(t *testing.T, conn *websocket.Conn, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, []byte(body)); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// openSSE starts an SSE stream, cancelling it when the test ends.
func openSSE(t *testing.T, url string) *http.Response {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("GET %s: status %d, want 200", url, resp.StatusCode)
	}
	return resp
}

// nextSSEData reads SSE frames until one carries a data line.
func nextSSEData(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("reading the stream: %v", err)
		}
		if data, ok := strings.CutPrefix(line, "data: "); ok {
			return strings.TrimSuffix(data, "\n")
		}
	}
	t.Fatal("no SSE data frame arrived")
	return ""
}

// allocateTURN runs a TURN allocation and returns the relayed address.
func allocateTURN(t *testing.T, addr, username, password string) (net.Addr, error) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	client, err := turn.NewClient(&turn.ClientConfig{
		STUNServerAddr: addr,
		TURNServerAddr: addr,
		Conn:           conn,
		Username:       username,
		Password:       password,
		Realm:          "caddy-relay",
	})
	if err != nil {
		return nil, err
	}
	defer client.Close()
	if err := client.Listen(); err != nil {
		return nil, err
	}
	relay, err := client.Allocate()
	if err != nil {
		return nil, err
	}
	defer relay.Close()
	return relay.LocalAddr(), nil
}

// sseURL builds the stream URL for an SSE channel on the given base.
func sseURL(base, channel string) string {
	return fmt.Sprintf("%s/%s", strings.TrimSuffix(base, "/"), channel)
}

// wsURL builds the WebSocket URL for a channel on a local port.
func wsURL(port int, channel string) string {
	return fmt.Sprintf("ws://127.0.0.1:%d/ws/%s", port, channel)
}
