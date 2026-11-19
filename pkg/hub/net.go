package hub

import "net"

// ClientIP reduces a client address in host:port form to its host, which is
// what per-IP limits key on. An address that carries no port, or that cannot
// be split, is returned unchanged; an empty address stays empty, meaning the
// client is unknown and no per-IP cap applies.
func ClientIP(remoteAddr string) string {
	if remoteAddr == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}
