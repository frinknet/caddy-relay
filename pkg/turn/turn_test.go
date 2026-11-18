package turn

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pion/turn/v4"
)

func TestListenRequiresAuthSecret(t *testing.T) {
	if _, err := Listen(Config{ListenAddr: "udp/127.0.0.1:0", ExternalIP: "127.0.0.1"}); err == nil {
		t.Fatal("Listen accepted an empty AuthSecret")
	}
}

func TestListenRejectsBadExternalIP(t *testing.T) {
	cfg := Config{ListenAddr: "udp/127.0.0.1:0", ExternalIP: "not-an-ip", AuthSecret: "s"}
	if _, err := Listen(cfg); err == nil {
		t.Fatal("Listen accepted an invalid external IP")
	}
}

func TestListenTLSRequiresCertificate(t *testing.T) {
	cfg := Config{ListenAddr: ":0", ExternalIP: "127.0.0.1", AuthSecret: "s"}
	if _, err := ListenTLS(cfg); err == nil {
		t.Fatal("ListenTLS accepted a config with no certificate")
	}
}

func TestRelayGeneratorSelection(t *testing.T) {
	ip := net.ParseIP("127.0.0.1")
	static, ok := newRelayGenerator(Config{}, ip).(*turn.RelayAddressGeneratorStatic)
	if !ok {
		t.Fatal("no relay ports configured should use the static generator")
	}
	// Relays bind every interface by default, while the advertised address is
	// the external IP.
	if static.Address != "0.0.0.0" {
		t.Errorf("bind address = %q, want 0.0.0.0", static.Address)
	}
	if !static.RelayAddress.Equal(ip) {
		t.Errorf("relay address = %v, want %v", static.RelayAddress, ip)
	}

	if bound, ok := newRelayGenerator(Config{BindAddr: "127.0.0.1"}, ip).(*turn.RelayAddressGeneratorStatic); !ok {
		t.Error("expected the static generator")
	} else if bound.Address != "127.0.0.1" {
		t.Errorf("bind address = %q, want the configured 127.0.0.1", bound.Address)
	}

	ranged, ok := newRelayGenerator(Config{RelayPortMin: 50000, RelayPortMax: 50010}, ip).(*turn.RelayAddressGeneratorPortRange)
	if !ok {
		t.Fatal("a relay port range should use the port-range generator")
	}
	if ranged.MinPort != 50000 || ranged.MaxPort != 50010 {
		t.Errorf("range = %d-%d, want 50000-50010", ranged.MinPort, ranged.MaxPort)
	}
	if ranged.Address != "0.0.0.0" {
		t.Errorf("bind address = %q, want 0.0.0.0", ranged.Address)
	}
}

// A UDP port that is free right now, used to reach a listener we started.
func freeUDPPort(t *testing.T) int {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).Port
}

// startTURN starts a listener on a free port and returns its address.
func startTURN(t *testing.T, cfg Config) string {
	t.Helper()
	closer, err := Listen(cfg)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { closer.Close() })
	_, addr := splitNetworkAddr(cfg.ListenAddr)
	return addr
}

// allocate runs a TURN allocation against addr and returns the relayed address.
func allocate(t *testing.T, addr, username, password, realm string) (net.Addr, error) {
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
		Realm:          realm,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()
	if err := client.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	relay, err := client.Allocate()
	if err != nil {
		return nil, err
	}
	defer relay.Close()
	return relay.LocalAddr(), nil
}

func TestAllocateWithRESTCredentials(t *testing.T) {
	const secret = "test-shared-secret"
	const realm = "test-realm"
	addr := startTURN(t, Config{
		ListenAddr: fmt.Sprintf("udp/127.0.0.1:%d", freeUDPPort(t)),
		ExternalIP: "127.0.0.1",
		Realm:      realm,
		AuthSecret: secret,
	})

	username, password, err := turn.GenerateLongTermTURNRESTCredentials(secret, "alice", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	relayAddr, err := allocate(t, addr, username, password, realm)
	if err != nil {
		t.Fatalf("allocation with valid REST credentials failed: %v", err)
	}
	if relayAddr == nil {
		t.Fatal("allocation returned no relay address")
	}
}

func TestAllocateRejectsBadCredentials(t *testing.T) {
	const secret = "test-shared-secret"
	const realm = "test-realm"
	cfg := Config{
		ListenAddr: fmt.Sprintf("udp/127.0.0.1:%d", freeUDPPort(t)),
		ExternalIP: "127.0.0.1",
		Realm:      realm,
		AuthSecret: secret,
	}
	addr := startTURN(t, cfg)

	username, password, err := turn.GenerateLongTermTURNRESTCredentials(secret, "alice", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// Sanity check: the same flow with the right password must work, so the
	// rejections below are not vacuous.
	if _, err := allocate(t, addr, username, password, realm); err != nil {
		t.Fatalf("valid credentials were rejected: %v", err)
	}
	if _, err := allocate(t, addr, username, "wrong-password", realm); err == nil {
		t.Error("allocation with a wrong password succeeded")
	}

	expired, expiredPassword, err := turn.GenerateLongTermTURNRESTCredentials(secret, "bob", -time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := allocate(t, addr, expired, expiredPassword, realm); err == nil {
		t.Error("allocation with an expired credential succeeded")
	}
}

// A configured relay port range must be the only place relays are allocated.
func TestAllocateHonoursRelayPortRange(t *testing.T) {
	const secret = "test-shared-secret"
	relayPort := freeUDPPort(t)
	addr := startTURN(t, Config{
		ListenAddr:   fmt.Sprintf("udp/127.0.0.1:%d", freeUDPPort(t)),
		ExternalIP:   "127.0.0.1",
		Realm:        "test-realm",
		AuthSecret:   secret,
		RelayPortMin: relayPort,
		RelayPortMax: relayPort,
	})

	username, password, err := turn.GenerateLongTermTURNRESTCredentials(secret, "alice", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	relayAddr, err := allocate(t, addr, username, password, "test-realm")
	if err != nil {
		t.Fatalf("allocation failed: %v", err)
	}
	udpAddr, ok := relayAddr.(*net.UDPAddr)
	if !ok {
		t.Fatalf("relay address is %T, want *net.UDPAddr", relayAddr)
	}
	if udpAddr.Port != relayPort {
		t.Errorf("relay port = %d, want the configured %d", udpAddr.Port, relayPort)
	}
}

// selfSignedCert writes a throwaway certificate/key pair and returns their paths.
func selfSignedCert(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath = filepath.Join(dir, "turn.crt")
	keyPath = filepath.Join(dir, "turn.key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

// The TLS listener must come up with the configured certificate; the TURN
// protocol over TLS itself is not exercised here (see BACKLOG.md).
func TestListenTLSServesTheCertificate(t *testing.T) {
	certPath, keyPath := selfSignedCert(t)
	port := freeUDPPort(t)
	cfg := Config{
		ListenAddr:  fmt.Sprintf(":%d", port),
		ExternalIP:  "127.0.0.1",
		AuthSecret:  "test-shared-secret",
		TLSCertFile: certPath,
		TLSKeyFile:  keyPath,
	}
	closer, err := ListenTLS(cfg)
	if err != nil {
		t.Fatalf("ListenTLS: %v", err)
	}
	defer closer.Close()

	conn, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port), &tls.Config{
		// The certificate is self-signed and freshly generated.
		InsecureSkipVerify: true, //nolint:gosec // test-only
	})
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	defer conn.Close()
	if err := conn.Handshake(); err != nil {
		t.Fatalf("TLS handshake: %v", err)
	}
	if got := conn.ConnectionState().PeerCertificates[0].Subject.CommonName; got != "localhost" {
		t.Errorf("served certificate CN = %q, want localhost", got)
	}
}

func TestListenTLSRejectsMissingKeyPair(t *testing.T) {
	cfg := Config{ListenAddr: ":0", ExternalIP: "127.0.0.1", AuthSecret: "s", TLSCertFile: "cert.pem"}
	if _, err := ListenTLS(cfg); err == nil {
		t.Fatal("ListenTLS accepted a certificate with no key")
	}
}
