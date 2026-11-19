// Package turn wraps pion/turn's TURN server.
package turn

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/turn/v4"
	"golang.org/x/time/rate"
)

// Config configures a TURN listener.
type Config struct {
	ListenAddr   string // e.g. "udp/:3478"
	Realm        string // "rtc.example.com"
	ExternalIP   string // public IP advertised in relay addresses
	BindAddr     string // local address relays bind to; empty = 0.0.0.0
	RelayPortMin int    // 0 = pion default (49152)
	RelayPortMax int    // 0 = pion default (65535)
	AuthSecret   string // shared secret for TURN REST credentials; required
	TLSCertFile  string // TLS listener certificate, when ListenAddr is TCP
	TLSKeyFile   string // TLS listener private key, when ListenAddr is TCP

	// MaxAllocations caps concurrent allocations across the listener; 0 =
	// unlimited. One allocation already allows a client to relay arbitrary
	// traffic, so this is the TURN analogue of a connection cap.
	MaxAllocations int
	// AuthRatePerIP limits authenticated requests per second from one client
	// IP; 0 = unlimited. Authentication runs on every request, so this bounds
	// how fast a single source can create allocations, permissions and
	// channel bindings.
	AuthRatePerIP int
}

// Listen starts a TURN server. The returned closer stops it.
//
// Credentials follow the TURN REST scheme: a client obtains a username of
// "expiry-unix:name" and a password of base64(HMAC-SHA1(AuthSecret, username))
// from an application endpoint, and the server recomputes the same key. There
// is no anonymous access, so AuthSecret must be set.
func Listen(cfg Config) (io.Closer, error) {
	if cfg.AuthSecret == "" {
		return nil, fmt.Errorf("turn: AuthSecret is required")
	}
	network, addr := splitNetworkAddr(cfg.ListenAddr)
	udpAddr, err := net.ResolveUDPAddr(network, addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP(network, udpAddr)
	if err != nil {
		return nil, err
	}

	relayIP := net.ParseIP(cfg.ExternalIP)
	if relayIP == nil {
		conn.Close()
		return nil, fmt.Errorf("turn: invalid external IP %q", cfg.ExternalIP)
	}

	srv, err := turn.NewServer(turn.ServerConfig{
		Realm:       cfg.Realm,
		AuthHandler: newAuthHandler(cfg),
		PacketConnConfigs: []turn.PacketConnConfig{
			{
				PacketConn:            conn,
				RelayAddressGenerator: newRelayGenerator(cfg, relayIP),
			},
		},
	})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return srv, nil
}

// ListenTLS starts a TURN server on a TLS (TCP) listener, as used by the
// `turns:` scheme.
func ListenTLS(cfg Config) (io.Closer, error) {
	if cfg.AuthSecret == "" {
		return nil, fmt.Errorf("turn: AuthSecret is required")
	}
	if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" {
		return nil, fmt.Errorf("turn: TLS listener requires a certificate and key")
	}
	cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("turn: loading TLS key pair: %w", err)
	}
	_, addr := splitNetworkAddr(cfg.ListenAddr)
	listener, err := tls.Listen("tcp", addr, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		return nil, err
	}

	relayIP := net.ParseIP(cfg.ExternalIP)
	if relayIP == nil {
		listener.Close()
		return nil, fmt.Errorf("turn: invalid external IP %q", cfg.ExternalIP)
	}

	srv, err := turn.NewServer(turn.ServerConfig{
		Realm:       cfg.Realm,
		AuthHandler: newAuthHandler(cfg),
		ListenerConfigs: []turn.ListenerConfig{
			{
				Listener:              listener,
				RelayAddressGenerator: newRelayGenerator(cfg, relayIP),
			},
		},
	})
	if err != nil {
		listener.Close()
		return nil, err
	}
	return srv, nil
}

// newRelayGenerator returns a relay address generator: one confined to
// RelayPortMin..RelayPortMax when a range is configured, otherwise pion's
// static generator, which allocates any free port. Relays bind BindAddr
// (0.0.0.0 by default) but are advertised as ExternalIP, which is what a
// client outside the host, behind NAT or reaching published container ports,
// has to dial. With MaxAllocations set the generator is wrapped so that only
// that many relays exist at once.
func newRelayGenerator(cfg Config, relayIP net.IP) turn.RelayAddressGenerator {
	bind := cfg.BindAddr
	if bind == "" {
		bind = "0.0.0.0"
	}
	var gen turn.RelayAddressGenerator
	if cfg.RelayPortMin > 0 && cfg.RelayPortMax >= cfg.RelayPortMin {
		gen = &turn.RelayAddressGeneratorPortRange{
			RelayAddress: relayIP,
			Address:      bind,
			MinPort:      uint16(cfg.RelayPortMin),
			MaxPort:      uint16(cfg.RelayPortMax),
		}
	} else {
		gen = &turn.RelayAddressGeneratorStatic{
			RelayAddress: relayIP,
			Address:      bind,
		}
	}
	if cfg.MaxAllocations > 0 {
		return &cappedGenerator{RelayAddressGenerator: gen, max: int64(cfg.MaxAllocations)}
	}
	return gen
}

// errAllocationCap is returned when the configured allocation cap is reached.
// pion reports the failed allocation to the client as an error response.
var errAllocationCap = errors.New("turn: allocation cap reached")

// cappedGenerator bounds the number of live relays. pion asks the generator
// for one socket per allocation and closes that socket when the allocation is
// released (or when a port reservation is abandoned), so counting the sockets
// counts the allocations.
type cappedGenerator struct {
	turn.RelayAddressGenerator
	max  int64
	live atomic.Int64
}

// Live reports how many relays the generator is currently tracking.
func (g *cappedGenerator) Live() int64 { return g.live.Load() }

func (g *cappedGenerator) acquire() bool {
	for {
		cur := g.live.Load()
		if cur >= g.max {
			return false
		}
		if g.live.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (g *cappedGenerator) AllocatePacketConn(network string, requestedPort int) (net.PacketConn, net.Addr, error) {
	if !g.acquire() {
		return nil, nil, errAllocationCap
	}
	conn, addr, err := g.RelayAddressGenerator.AllocatePacketConn(network, requestedPort)
	if err != nil {
		g.live.Add(-1)
		return nil, nil, err
	}
	return &trackedPacketConn{PacketConn: conn, release: func() { g.live.Add(-1) }}, addr, nil
}

func (g *cappedGenerator) AllocateConn(network string, requestedPort int) (net.Conn, net.Addr, error) {
	if !g.acquire() {
		return nil, nil, errAllocationCap
	}
	conn, addr, err := g.RelayAddressGenerator.AllocateConn(network, requestedPort)
	if err != nil {
		g.live.Add(-1)
		return nil, nil, err
	}
	return &trackedConn{Conn: conn, release: func() { g.live.Add(-1) }}, addr, nil
}

// trackedPacketConn returns its slot when closed. pion may close a relay more
// than once, so the release runs once.
type trackedPacketConn struct {
	net.PacketConn
	once    sync.Once
	release func()
}

func (c *trackedPacketConn) Close() error {
	err := c.PacketConn.Close()
	c.once.Do(c.release)
	return err
}

type trackedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

// newAuthHandler returns the TURN REST auth handler, optionally rate limited
// per source IP. A refused attempt fails authentication, which the client sees
// as a rejection rather than a silent drop.
func newAuthHandler(cfg Config) turn.AuthHandler {
	base := turn.LongTermTURNRESTAuthHandler(cfg.AuthSecret, nil)
	if cfg.AuthRatePerIP <= 0 {
		return base
	}
	limiters := newIPLimiters(cfg.AuthRatePerIP)
	return func(username, realm string, srcAddr net.Addr) ([]byte, bool) {
		if !limiters.allow(hostOf(srcAddr)) {
			return nil, false
		}
		return base(username, realm, srcAddr)
	}
}

// authLimiterIdle is how long an IP's limiter is kept after its last attempt.
const authLimiterIdle = 10 * time.Minute

// ipLimiters holds one token bucket per client IP. It is safe for concurrent
// use because pion authenticates on the server goroutines.
type ipLimiters struct {
	mu     sync.Mutex
	limit  rate.Limit
	burst  int
	byIP   map[string]*ipLimiter
	lastGC time.Time
}

type ipLimiter struct {
	lim  *rate.Limiter
	last time.Time
}

func newIPLimiters(perSecond int) *ipLimiters {
	return &ipLimiters{
		limit:  rate.Limit(perSecond),
		burst:  perSecond,
		byIP:   make(map[string]*ipLimiter),
		lastGC: time.Now(),
	}
}

func (l *ipLimiters) allow(ip string) bool {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.lastGC) >= time.Minute {
		l.gc(now)
	}
	e, ok := l.byIP[ip]
	if !ok {
		e = &ipLimiter{lim: rate.NewLimiter(l.limit, l.burst)}
		l.byIP[ip] = e
	}
	e.last = now
	return e.lim.Allow()
}

// gc drops limiters for IPs that have gone quiet, so a scan of many source
// addresses cannot grow the map without bound. Caller holds l.mu.
func (l *ipLimiters) gc(now time.Time) {
	l.lastGC = now
	for ip, e := range l.byIP {
		if now.Sub(e.last) > authLimiterIdle {
			delete(l.byIP, ip)
		}
	}
}

// hostOf strips the port from a client address, so every connection from one
// host shares a limiter.
func hostOf(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	s := addr.String()
	if host, _, err := net.SplitHostPort(s); err == nil {
		return host
	}
	return s
}

func splitNetworkAddr(s string) (string, string) {
	// supports "udp/:3478", "udp/1.2.3.4:3478" and ":3478"
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			return s[:i], s[i+1:]
		}
	}
	return "udp", s
}
