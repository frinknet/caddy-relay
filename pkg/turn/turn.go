// Package turn wraps pion/turn's TURN server.
package turn

import (
	"fmt"
	"io"
	"net"

	"github.com/pion/turn/v4"
)

// Config configures a TURN listener.
type Config struct {
	ListenAddr    string // e.g. "udp/:3478"
	Realm         string // "rtc.example.com"
	ExternalIP    string // public IP for relay addresses
	RelayPortMin  int    // 0 = pion default (49152)
	RelayPortMax  int    // 0 = pion default (65535)
	AuthSecret    string // shared secret for REST auth; empty = no auth
}

// Listen starts a TURN server. The returned closer stops it.
func Listen(cfg Config) (io.Closer, error) {
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
		Realm: cfg.Realm,
		AuthHandler: func(username, realm string, srcAddr net.Addr) ([]byte, bool) {
			if cfg.AuthSecret == "" {
				return nil, true // no auth configured
			}
			// Simple shared-secret: password == secret. Replace with REST
			// HMAC validation if you need time-limited credentials.
			return []byte(cfg.AuthSecret), true
		},
		PacketConnConfigs: []turn.PacketConnConfig{
			{
				PacketConn: conn,
				RelayAddressGenerator: &turn.RelayAddressGeneratorStatic{
					RelayAddress: relayIP,
					Address:      cfg.ExternalIP,
				},
			},
		},
	})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return srv, nil
}

func splitNetworkAddr(s string) (string, string) {
	// supports "udp/:3478" and "udp/1.2.3.4:3478"
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			return s[:i], s[i+1:]
		}
	}
	return "udp", s
}