// Package stun implements a minimal STUN binding server over UDP.
package stun

import (
	"io"
	"net"

	"github.com/pion/stun/v3"
)

// Listen starts a STUN server on network/addr (e.g. "udp", ":3478").
// Returns a closer that stops the server.
func Listen(network, addr string) (io.Closer, error) {
	udpAddr, err := net.ResolveUDPAddr(network, addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP(network, udpAddr)
	if err != nil {
		return nil, err
	}
	go serve(conn)
	return conn, nil
}

func serve(conn *net.UDPConn) {
	buf := make([]byte, 1500)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		req := &stun.Message{Raw: append([]byte(nil), buf[:n]...)}
		if err := req.Decode(); err != nil {
			continue
		}
		if req.Type != stun.BindingRequest {
			continue
		}
		resp, err := stun.Build(
			stun.BindingSuccess,
			stun.NewTransactionIDSetter(req.TransactionID),
			&stun.XORMappedAddress{IP: addr.IP, Port: addr.Port},
			stun.Fingerprint,
		)
		if err != nil {
			continue
		}
		_, _ = conn.WriteToUDP(resp.Raw, addr)
	}
}