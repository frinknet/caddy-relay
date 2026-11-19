package hub

import (
	"fmt"
	"net"
	"net/http"
	"strings"
)

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

// TrustedProxies is the set of proxy peers whose X-Forwarded-For header is
// believed. An empty set trusts nobody, which is the default: the peer is then
// always the client.
type TrustedProxies []*net.IPNet

// ParseTrustedProxies parses CIDR ranges and bare addresses; a bare address is
// a single host. It returns nil for an empty list.
func ParseTrustedProxies(entries []string) (TrustedProxies, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	trusted := make(TrustedProxies, 0, len(entries))
	for _, entry := range entries {
		if ip := net.ParseIP(entry); ip != nil {
			bits := 8 * len(ip)
			trusted = append(trusted, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
			continue
		}
		_, network, err := net.ParseCIDR(entry)
		if err != nil {
			return nil, fmt.Errorf("%q is not an IP address or CIDR range", entry)
		}
		trusted = append(trusted, network)
	}
	return trusted, nil
}

// Contains reports whether ip is one of the trusted proxies.
func (t TrustedProxies) Contains(ip net.IP) bool {
	for _, network := range t {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

// forwardedFor is where a trusted proxy reports the original client.
const forwardedFor = "X-Forwarded-For"

// ClientIPFrom returns the client's host for r. Without trusted proxies, or
// when r's peer is not one of them, the header is ignored and the peer is the
// client, so a client cannot claim an address for itself. From a trusted
// proxy, the rightmost X-Forwarded-For entry that is not itself a trusted
// proxy is the client: the entries to its left were appended by whoever sent
// them, so a forged one is skipped. An entry that is not an IP address ends the
// walk, leaving the last trusted hop (or the peer) as the client.
func ClientIPFrom(r *http.Request, trusted TrustedProxies) string {
	peer := ClientIP(r.RemoteAddr)
	if len(trusted) == 0 || !trusted.Contains(net.ParseIP(peer)) {
		return peer
	}
	client := peer
	var hops []net.IP
	for _, header := range r.Header.Values(forwardedFor) {
		for _, entry := range strings.Split(header, ",") {
			hops = append(hops, parseHop(entry))
		}
	}
	for i := len(hops) - 1; i >= 0; i-- {
		ip := hops[i]
		if ip == nil {
			return client
		}
		if !trusted.Contains(ip) {
			return ip.String()
		}
		client = ip.String()
	}
	return client
}

// parseHop reads one X-Forwarded-For entry, which some proxies send with a
// port. It returns nil when the entry is not an address.
func parseHop(entry string) net.IP {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return nil
	}
	if host, _, err := net.SplitHostPort(entry); err == nil {
		entry = host
	}
	return net.ParseIP(strings.Trim(entry, "[]"))
}
