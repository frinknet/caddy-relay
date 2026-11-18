// Package turn wraps pion/turn's TURN server.
package turn

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"

	"github.com/pion/turn/v4"
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
		AuthHandler: turn.LongTermTURNRESTAuthHandler(cfg.AuthSecret, nil),
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
		AuthHandler: turn.LongTermTURNRESTAuthHandler(cfg.AuthSecret, nil),
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
// has to dial.
func newRelayGenerator(cfg Config, relayIP net.IP) turn.RelayAddressGenerator {
	bind := cfg.BindAddr
	if bind == "" {
		bind = "0.0.0.0"
	}
	if cfg.RelayPortMin > 0 && cfg.RelayPortMax >= cfg.RelayPortMin {
		return &turn.RelayAddressGeneratorPortRange{
			RelayAddress: relayIP,
			Address:      bind,
			MinPort:      uint16(cfg.RelayPortMin),
			MaxPort:      uint16(cfg.RelayPortMax),
		}
	}
	return &turn.RelayAddressGeneratorStatic{
		RelayAddress: relayIP,
		Address:      bind,
	}
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
