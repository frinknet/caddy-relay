package turn

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
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

// A GetCertificate source (the certmagic path) is enough on its own: no
// certificate files are needed.
func TestListenTLSUsesGetCertificate(t *testing.T) {
	certPath, keyPath := selfSignedCert(t)
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	port := freeUDPPort(t)
	closer, err := ListenTLS(Config{
		ListenAddr: fmt.Sprintf(":%d", port),
		ExternalIP: "127.0.0.1",
		AuthSecret: "test-shared-secret",
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return &cert, nil
		},
	})
	if err != nil {
		t.Fatalf("ListenTLS: %v", err)
	}
	defer closer.Close()

	conn, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port), &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // test-only
	})
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	defer conn.Close()
	if got := conn.ConnectionState().PeerCertificates[0].Subject.CommonName; got != "localhost" {
		t.Errorf("served certificate CN = %q, want localhost", got)
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

// A TCP port that is free right now, used to reach a listener we started.
func freeTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
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

// The TLS listener must come up with the configured certificate; no TURN
// allocation is made through it here.
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

// A TURN allocation over the TLS listener: the server wraps the accepted TCP
// connection in STUNConn, so a TURN client can allocate through TLS just as it
// does over UDP.
func TestAllocateOverTLS(t *testing.T) {
	const secret = "test-shared-secret"
	const realm = "test-realm"
	certPath, keyPath := selfSignedCert(t)
	port := freeTCPPort(t)
	closer, err := ListenTLS(Config{
		ListenAddr:  fmt.Sprintf(":%d", port),
		ExternalIP:  "127.0.0.1",
		Realm:       realm,
		AuthSecret:  secret,
		TLSCertFile: certPath,
		TLSKeyFile:  keyPath,
	})
	if err != nil {
		t.Fatalf("ListenTLS: %v", err)
	}
	defer closer.Close()

	username, password, err := turn.GenerateLongTermTURNRESTCredentials(secret, "alice", time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port), &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // test-only
	})
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	defer conn.Close()

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	client, err := turn.NewClient(&turn.ClientConfig{
		STUNServerAddr: addr,
		TURNServerAddr: addr,
		Conn:           turn.NewSTUNConn(conn),
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
		t.Fatalf("allocation over TLS failed: %v", err)
	}
	defer relay.Close()
	if relay.LocalAddr() == nil {
		t.Fatal("allocation over TLS returned no relay address")
	}
}

// openAllocation starts a client and keeps its relay open until the test ends,
// so the allocation stays live while other attempts run.
func openAllocation(t *testing.T, addr, username, password, realm string) error {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	client, err := turn.NewClient(&turn.ClientConfig{
		STUNServerAddr: addr,
		TURNServerAddr: addr,
		Conn:           conn,
		Username:       username,
		Password:       password,
		Realm:          realm,
	})
	if err != nil {
		conn.Close()
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.Listen(); err != nil {
		client.Close()
		conn.Close()
		t.Fatalf("Listen: %v", err)
	}
	relay, err := client.Allocate()
	if err != nil {
		client.Close()
		conn.Close()
		return err
	}
	t.Cleanup(func() {
		relay.Close()
		client.Close()
		conn.Close()
	})
	return nil
}

func TestMaxAllocationsIsEnforced(t *testing.T) {
	const secret = "test-shared-secret"
	const realm = "test-realm"
	addr := startTURN(t, Config{
		ListenAddr:     fmt.Sprintf("udp/127.0.0.1:%d", freeUDPPort(t)),
		ExternalIP:     "127.0.0.1",
		Realm:          realm,
		AuthSecret:     secret,
		MaxAllocations: 1,
	})

	alice, alicePass, err := turn.GenerateLongTermTURNRESTCredentials(secret, "alice", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	bob, bobPass, err := turn.GenerateLongTermTURNRESTCredentials(secret, "bob", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := openAllocation(t, addr, alice, alicePass, realm); err != nil {
		t.Fatalf("first allocation failed: %v", err)
	}
	if err := openAllocation(t, addr, bob, bobPass, realm); err == nil {
		t.Fatal("second allocation succeeded with MaxAllocations 1")
	}
}

// The generator releases a slot when pion closes the relay, so a freed
// allocation does not consume the cap.
func TestCappedGeneratorReleasesSlotOnClose(t *testing.T) {
	gen := newRelayGenerator(Config{MaxAllocations: 1, BindAddr: "127.0.0.1"}, net.ParseIP("127.0.0.1"))
	capped, ok := gen.(*cappedGenerator)
	if !ok {
		t.Fatalf("generator is %T, want *cappedGenerator", gen)
	}
	// pion calls Validate before serving; it installs the net used to bind.
	if err := capped.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	conn, _, err := capped.AllocatePacketConn("udp4", 0)
	if err != nil {
		t.Fatalf("first allocation: %v", err)
	}
	if capped.Live() != 1 {
		t.Errorf("Live() = %d, want 1", capped.Live())
	}
	if _, _, err := capped.AllocatePacketConn("udp4", 0); !errors.Is(err, errAllocationCap) {
		t.Errorf("second allocation error = %v, want errAllocationCap", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if capped.Live() != 0 {
		t.Errorf("Live() = %d after Close, want 0", capped.Live())
	}
	again, _, err := capped.AllocatePacketConn("udp4", 0)
	if err != nil {
		t.Fatalf("allocation after Close: %v", err)
	}
	again.Close()
}

func TestAuthRatePerIP(t *testing.T) {
	limiters := newIPLimiters(2)
	addr := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 7), Port: 40000}
	if !limiters.allow(hostOf(addr)) || !limiters.allow(hostOf(addr)) {
		t.Fatal("the configured burst was refused")
	}
	if limiters.allow(hostOf(addr)) {
		t.Fatal("a third attempt passed a limiter of 2 per second")
	}
	// The port is not part of the key: another connection from the same host
	// shares the limiter.
	other := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 7), Port: 40001}
	if limiters.allow(hostOf(other)) {
		t.Fatal("a different port from the same IP was not limited")
	}
	elsewhere := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 8), Port: 40000}
	if !limiters.allow(hostOf(elsewhere)) {
		t.Fatal("a different IP was limited")
	}
}

func TestAuthRateAllowsNormalAllocation(t *testing.T) {
	const secret = "test-shared-secret"
	const realm = "test-realm"
	addr := startTURN(t, Config{
		ListenAddr:    fmt.Sprintf("udp/127.0.0.1:%d", freeUDPPort(t)),
		ExternalIP:    "127.0.0.1",
		Realm:         realm,
		AuthSecret:    secret,
		AuthRatePerIP: 100,
	})
	username, password, err := turn.GenerateLongTermTURNRESTCredentials(secret, "alice", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	relayAddr, err := allocate(t, addr, username, password, realm)
	if err != nil {
		t.Fatalf("allocation with AuthRatePerIP set failed: %v", err)
	}
	if relayAddr == nil {
		t.Fatal("allocation returned no relay address")
	}
}

func TestHostOf(t *testing.T) {
	cases := map[string]string{
		"192.0.2.7:54321":   "192.0.2.7",
		"[2001:db8::1]:443": "2001:db8::1",
	}
	for in, want := range cases {
		addr, err := net.ResolveUDPAddr("udp", in)
		if err != nil {
			t.Fatalf("ResolveUDPAddr(%q): %v", in, err)
		}
		if got := hostOf(addr); got != want {
			t.Errorf("hostOf(%q) = %q, want %q", in, got, want)
		}
	}
	// An address that carries no port is used as it is.
	if got := hostOf(addrString("192.0.2.7")); got != "192.0.2.7" {
		t.Errorf("hostOf(portless) = %q, want 192.0.2.7", got)
	}
	if got := hostOf(nil); got != "" {
		t.Errorf("hostOf(nil) = %q, want empty", got)
	}
}

// addrString is a net.Addr that returns its value verbatim, used to cover
// addresses without a port.
type addrString string

func (a addrString) Network() string { return "udp" }
func (a addrString) String() string  { return string(a) }
