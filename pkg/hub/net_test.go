package hub

import (
	"net"
	"net/http"
	"strings"
	"testing"
)

// forwardedRequest builds a request whose peer is remoteAddr and whose
// X-Forwarded-For carries one header per entry of forwarded.
func forwardedRequest(remoteAddr string, forwarded ...string) *http.Request {
	r := &http.Request{RemoteAddr: remoteAddr, Header: http.Header{}}
	for _, value := range forwarded {
		r.Header.Add(forwardedFor, value)
	}
	return r
}

func TestParseTrustedProxies(t *testing.T) {
	trusted, err := ParseTrustedProxies([]string{"10.0.0.0/8", "192.0.2.7", "2001:db8::1"})
	if err != nil {
		t.Fatalf("ParseTrustedProxies: %v", err)
	}
	for _, in := range []string{"10.9.9.9", "192.0.2.7", "2001:db8::1"} {
		if !trusted.Contains(net.ParseIP(in)) {
			t.Errorf("%s is not trusted", in)
		}
	}
	for _, in := range []string{"192.0.2.8", "2001:db8::2"} {
		if trusted.Contains(net.ParseIP(in)) {
			t.Errorf("%s is trusted", in)
		}
	}

	none, err := ParseTrustedProxies(nil)
	if err != nil || none != nil {
		t.Errorf("ParseTrustedProxies(nil) = %v, %v; want nil, nil", none, err)
	}

	_, err = ParseTrustedProxies([]string{"10.0.0.0/8", "nope"})
	if err == nil || !strings.Contains(err.Error(), `"nope"`) {
		t.Errorf("ParseTrustedProxies with a bad entry = %v, want it to name nope", err)
	}
}

// Behind a load balancer every peer is the balancer, so a trusted one reports
// the real client in X-Forwarded-For. The rightmost entry that is not itself a
// trusted proxy wins, and nobody else's header is believed.
func TestClientIPFrom(t *testing.T) {
	trusted, err := ParseTrustedProxies([]string{"10.0.0.0/8", "2001:db8::/32"})
	if err != nil {
		t.Fatalf("ParseTrustedProxies: %v", err)
	}
	cases := []struct {
		name      string
		remote    string
		forwarded []string
		want      string
	}{
		{"untrusted peer ignores the header", "192.0.2.7:5555", []string{"203.0.113.9"}, "192.0.2.7"},
		{"no header", "10.0.0.5:5555", nil, "10.0.0.5"},
		{"trusted peer", "10.0.0.5:5555", []string{"203.0.113.9"}, "203.0.113.9"},
		{"rightmost untrusted hop", "10.0.0.5:5555", []string{"203.0.113.66, 198.51.100.1"}, "198.51.100.1"},
		{"forged entry left of a client", "10.0.0.5:5555", []string{"203.0.113.66, 198.51.100.1, 10.0.0.7"}, "198.51.100.1"},
		{"every hop trusted", "10.0.0.5:5555", []string{"10.0.0.8, 10.0.0.7"}, "10.0.0.8"},
		{"entry with a port", "10.0.0.5:5555", []string{"203.0.113.9:1234"}, "203.0.113.9"},
		{"unparsable entry ends the walk", "10.0.0.5:5555", []string{"203.0.113.9, unknown"}, "10.0.0.5"},
		{"repeated headers", "10.0.0.5:5555", []string{"198.51.100.1", "203.0.113.9"}, "203.0.113.9"},
		{"ipv6 peer", "[2001:db8::5]:5555", []string{"203.0.113.9"}, "203.0.113.9"},
		{"ipv6 client", "10.0.0.5:5555", []string{"2001:db8::99"}, "2001:db8::99"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := forwardedRequest(tc.remote, tc.forwarded...)
			if got := ClientIPFrom(r, trusted); got != tc.want {
				t.Errorf("ClientIPFrom(%q, %q) = %q, want %q", tc.remote, tc.forwarded, got, tc.want)
			}
		})
	}

	r := forwardedRequest("10.0.0.5:5555", "203.0.113.9")
	if got := ClientIPFrom(r, nil); got != "10.0.0.5" {
		t.Errorf("ClientIPFrom with no trusted proxies = %q, want 10.0.0.5", got)
	}
}
