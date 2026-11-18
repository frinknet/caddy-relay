// This file runs the plugin end to end against the image built by ./build.sh:
// it starts a real container with tests/e2e/Caddyfile mounted, waits for the
// image's own healthcheck, and drives every feature over published ports.
//
// It is skipped when Docker is unavailable or the image has not been built, so
// `go test ./...` stays usable everywhere. Build the image with the repo's own
// entry point first, then run this package:
//
//	./build.sh
//	go test ./tests/e2e/
//
// Set CADDY_RELAY_IMAGE to test a differently tagged image.
package e2e

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	crand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/pion/stun/v3"
	"github.com/pion/turn/v4"
)

// turnSecret is the shared secret in tests/e2e/Caddyfile.
const turnSecret = "e2e-shared-secret"

// ports are chosen at runtime and substituted into the Caddyfile template, so
// the test does not collide with other things listening on the host.
type ports struct {
	http, https, stun, turn, turnTLS int
	relayMin, relayMax               int
}

// freeUDPRange returns a block of n free UDP ports below the Linux ephemeral
// range, so a random outbound connection cannot steal one mid-test.
func freeUDPRange(t *testing.T, n int) (int, int) {
	t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		base := 20000 + rand.IntN(4000)
		conns := make([]net.PacketConn, 0, n)
		free := true
		for i := 0; i < n; i++ {
			conn, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", base+i))
			if err != nil {
				free = false
				break
			}
			conns = append(conns, conn)
		}
		for _, conn := range conns {
			conn.Close()
		}
		if free {
			return base, base + n - 1
		}
	}
	t.Fatal("could not find a free UDP port range")
	return 0, 0
}

func allocatePorts(t *testing.T) ports {
	t.Helper()
	relayMin, relayMax := freeUDPRange(t, 16)
	return ports{
		http:     freePort(t, "tcp"),
		https:    freePort(t, "tcp"),
		stun:     freePort(t, "udp"),
		turn:     freePort(t, "udp"),
		turnTLS:  freePort(t, "tcp"),
		relayMin: relayMin,
		relayMax: relayMax,
	}
}

// renderCaddyfile substitutes the ports into the Caddyfile template. The
// Caddyfile also contains Go template syntax ({{ message }}), so only the
// placeholders this function knows about are substituted and checked.
func renderCaddyfile(t *testing.T, p ports) string {
	t.Helper()
	src, err := os.ReadFile("Caddyfile")
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{
		"{{HTTP_PORT}}":     strconv.Itoa(p.http),
		"{{HTTPS_PORT}}":    strconv.Itoa(p.https),
		"{{STUN_PORT}}":     strconv.Itoa(p.stun),
		"{{TURN_PORT}}":     strconv.Itoa(p.turn),
		"{{TURN_TLS_PORT}}": strconv.Itoa(p.turnTLS),
		"{{RELAY_MIN}}":     strconv.Itoa(p.relayMin),
		"{{RELAY_MAX}}":     strconv.Itoa(p.relayMax),
	}
	rendered := string(src)
	for placeholder, value := range values {
		rendered = strings.ReplaceAll(rendered, placeholder, value)
	}
	for placeholder := range values {
		if strings.Contains(rendered, placeholder) {
			t.Fatalf("placeholder %s was not substituted", placeholder)
		}
	}
	path := filepath.Join(t.TempDir(), "Caddyfile")
	if err := os.WriteFile(path, []byte(rendered), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDockerE2E(t *testing.T) {
	image := os.Getenv("CADDY_RELAY_IMAGE")
	if image == "" {
		image = "caddy-relay:test"
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker is not installed: %v", err)
	}
	if out, err := exec.Command("docker", "image", "inspect", image).CombinedOutput(); err != nil {
		t.Skipf("image %q is not available (%v); run ./build.sh first\n%s", image, err, out)
	}

	cfg := allocatePorts(t)
	caddyfile := renderCaddyfile(t, cfg)
	certFile, keyFile := writeSelfSigned(t)

	name := fmt.Sprintf("caddy-relay-e2e-%d", os.Getpid())
	t.Cleanup(func() {
		if t.Failed() {
			if out, err := exec.Command("docker", "logs", name).CombinedOutput(); err == nil {
				t.Logf("container logs:\n%s", out)
			}
		}
		if out, err := exec.Command("docker", "rm", "-f", name).CombinedOutput(); err != nil {
			t.Logf("docker rm -f %s: %v\n%s", name, err, out)
		}
	})

	// The image's ENTRYPOINT/CMD already run /etc/caddy/Caddyfile, so the
	// mounted config is picked up without extra arguments.
	args := []string{
		"-d", "--name", name,
		"-p", fmt.Sprintf("%d:%d", cfg.http, cfg.http),
		"-p", fmt.Sprintf("%d:%d", cfg.https, cfg.https),
		"-p", fmt.Sprintf("%d:%d/udp", cfg.stun, cfg.stun),
		"-p", fmt.Sprintf("%d:%d/udp", cfg.turn, cfg.turn),
		"-p", fmt.Sprintf("%d:%d", cfg.turnTLS, cfg.turnTLS),
		"-p", fmt.Sprintf("%d-%d:%d-%d/udp", cfg.relayMin, cfg.relayMax, cfg.relayMin, cfg.relayMax),
		"-v", caddyfile + ":/etc/caddy/Caddyfile:ro",
		"-v", certFile + ":/etc/caddy/turn.crt:ro",
		"-v", keyFile + ":/etc/caddy/turn.key:ro",
		image,
	}
	if out, err := exec.Command("docker", append([]string{"run"}, args...)...).CombinedOutput(); err != nil {
		t.Fatalf("docker run: %v\n%s", err, out)
	}

	waitForHTTP(t, fmt.Sprintf("http://127.0.0.1:%d/", cfg.http))
	// The image's HEALTHCHECK polls the admin API, so this also proves the
	// container loaded the config and the admin endpoint is up.
	waitForHealthy(t, name)

	t.Run("websocket template intercept", func(t *testing.T) {
		url := fmt.Sprintf("ws://127.0.0.1:%d/ws/room-1", cfg.http)
		alice := dialWS(t, url, nil)
		bob := dialWS(t, url, nil)

		sendWS(t, alice, "hello container")
		if got, err := readWS(t, bob); err != nil {
			t.Fatalf("bob read: %v", err)
		} else if got != "hello container" {
			t.Errorf("bob received %q, want the rendered body", got)
		}
		if got, err := readWSWithin(t, alice, 300*time.Millisecond); err == nil {
			t.Errorf("sender received its own message: %q", got)
		}
	})

	t.Run("websocket origin policy", func(t *testing.T) {
		url := fmt.Sprintf("ws://127.0.0.1:%d/ws/room-1", cfg.http)
		// A foreign Origin is refused. Matching an allow-listed pattern is
		// covered by pkg/ws, since through published ports the request Host
		// and the Origin host are the same.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{
			HTTPHeader: http.Header{"Origin": {"http://evil.example"}},
		})
		if err == nil {
			conn.Close(websocket.StatusNormalClosure, "")
			t.Fatal("a foreign Origin was accepted")
		}
		if resp == nil || resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %v, want 403", resp)
		}
	})

	t.Run("sse url intercept and replay", func(t *testing.T) {
		base := fmt.Sprintf("http://127.0.0.1:%d/events/room-2", cfg.http)
		stream := openSSE(t, base)
		defer stream.Body.Close()

		// The intercept posts the message back to this same Caddy, so what
		// subscribers see is that endpoint's response.
		post, err := http.Post(base, "text/plain", strings.NewReader("anything"))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		post.Body.Close()
		if post.StatusCode != http.StatusAccepted {
			t.Fatalf("POST status = %d, want 202", post.StatusCode)
		}
		if got := nextSSEData(t, bufio.NewReader(stream.Body)); got != `{"intercepted":true}` {
			t.Errorf("stream received %q, want the intercept response", got)
		}

		// A late subscriber is replayed the retained message.
		late := openSSE(t, base)
		defer late.Body.Close()
		if got := nextSSEData(t, bufio.NewReader(late.Body)); got != `{"intercepted":true}` {
			t.Errorf("replayed %q, want the intercept response", got)
		}
	})

	t.Run("rejected channel", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, _, err := websocket.Dial(ctx, fmt.Sprintf("ws://127.0.0.1:%d/ws/other-1", cfg.http), nil)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		if _, _, err := conn.Read(ctx); err == nil {
			t.Error("a channel outside the subscribe pattern was accepted")
		} else if got := websocket.CloseStatus(err); got != websocket.StatusPolicyViolation {
			t.Errorf("close status = %v, want %v", got, websocket.StatusPolicyViolation)
		}
	})

	t.Run("stun binding", func(t *testing.T) {
		conn, err := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", cfg.stun))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()

		req, err := stun.Build(stun.BindingRequest, stun.NewTransactionIDSetter(stun.NewTransactionID()))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write(req.Raw); err != nil {
			t.Fatal(err)
		}
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 1500)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("reading the binding response: %v", err)
		}
		resp := &stun.Message{Raw: append([]byte(nil), buf[:n]...)}
		if err := resp.Decode(); err != nil {
			t.Fatalf("decoding the response: %v", err)
		}
		if resp.Type != stun.BindingSuccess {
			t.Fatalf("response type = %v, want %v", resp.Type, stun.BindingSuccess)
		}
		var xor stun.XORMappedAddress
		if err := xor.GetFrom(resp); err != nil {
			t.Fatalf("no XOR-MAPPED-ADDRESS: %v", err)
		}
		// The source address the container sees is Docker's NAT, not the host's
		// socket, so only its presence is asserted here.
		if xor.Port == 0 {
			t.Error("XOR-MAPPED-ADDRESS has no port")
		}
	})

	t.Run("turn allocation with rest credentials", func(t *testing.T) {
		addr := fmt.Sprintf("127.0.0.1:%d", cfg.turn)
		username, password, err := turn.GenerateLongTermTURNRESTCredentials(turnSecret, "e2e", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		relayAddr, err := allocateTURN(t, addr, username, password)
		if err != nil {
			t.Fatalf("allocation failed: %v", err)
		}
		udpAddr, ok := relayAddr.(*net.UDPAddr)
		if !ok {
			t.Fatalf("relay address is %T, want *net.UDPAddr", relayAddr)
		}
		if udpAddr.IP.String() != "127.0.0.1" {
			t.Errorf("relay IP = %s, want the configured external IP", udpAddr.IP)
		}
		if udpAddr.Port < cfg.relayMin || udpAddr.Port > cfg.relayMax {
			t.Errorf("relay port = %d, want it inside %d-%d", udpAddr.Port, cfg.relayMin, cfg.relayMax)
		}

		// Credentials are enforced, not just accepted.
		if _, err := allocateTURN(t, addr, username, "wrong-password"); err == nil {
			t.Error("allocation with a wrong password succeeded")
		}
	})

	t.Run("wss over the internal ca", func(t *testing.T) {
		addr := fmt.Sprintf("127.0.0.1:%d", cfg.https)
		// Inspect the served certificate: it must be the internal CA's, for
		// localhost, which is what a browser would have to trust.
		conn, err := tls.Dial("tcp", addr, &tls.Config{
			ServerName:         "localhost",
			InsecureSkipVerify: true, //nolint:gosec // the local CA is not in the host trust store
		})
		if err != nil {
			t.Fatalf("tls dial: %v", err)
		}
		defer conn.Close()
		leaf := conn.ConnectionState().PeerCertificates[0]
		if !strings.Contains(leaf.Issuer.CommonName, "Caddy") {
			t.Errorf("certificate issuer = %q, want Caddy's local CA", leaf.Issuer.CommonName)
		}
		found := false
		for _, dns := range leaf.DNSNames {
			if dns == "localhost" {
				found = true
			}
		}
		if !found {
			t.Errorf("certificate names = %v, want localhost", leaf.DNSNames)
		}

		// And a WebSocket relay works over TLS. The site is for localhost and
		// Caddy matches sites by Host, so dial the name, not 127.0.0.1.
		url := fmt.Sprintf("wss://localhost:%d/ws/room-3", cfg.https)
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
			ServerName:         "localhost",
			InsecureSkipVerify: true, //nolint:gosec // see above
		}}}
		alice := dialWS(t, url, client)
		bob := dialWS(t, url, client)
		sendWS(t, alice, "hello tls")
		if got, err := readWS(t, bob); err != nil {
			t.Fatalf("bob read: %v", err)
		} else if got != "hello tls" {
			t.Errorf("bob received %q, want hello tls", got)
		}
	})
}

// writeSelfSigned creates the certificate and key turn_tls loads.
func writeSelfSigned(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), crand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(crand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, "turn.crt")
	keyFile = filepath.Join(dir, "turn.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

func waitForHTTP(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
			last = fmt.Errorf("status %d", resp.StatusCode)
		} else {
			last = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s never came up: %v", url, last)
}

func waitForHealthy(t *testing.T, name string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		out, err := exec.Command("docker", "inspect", "--format", "{{.State.Health.Status}}", name).Output()
		if err == nil {
			last = strings.TrimSpace(string(out))
			if last == "healthy" {
				return
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("container never reported healthy (last status %q)", last)
}
