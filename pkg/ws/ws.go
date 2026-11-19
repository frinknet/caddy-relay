// Package ws implements the WebSocket transport for caddy-relay.
package ws

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/time/rate"

	"github.com/frinknet/caddy-relay/pkg/hub"
)

// Config is the resolved, provisioned relay_ws config.
type Config struct {
	Subscribe *hub.SubscribeSpec
	Intercept *hub.InterceptSpec
	Publish   *hub.PublishSpec

	// OriginPatterns lists host patterns allowed in the Origin header, in
	// addition to the request's own Host. Empty means same-origin only.
	OriginPatterns []string
}

// Serve handles an HTTP request for the relay_ws directive.
func Serve(cfg Config, bus hub.Bus, w http.ResponseWriter, r *http.Request) {
	if err := checkOrigin(r, cfg.OriginPatterns); err != nil {
		http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	conn, err := websocket.Accept(upgradeWriter(w), r, &websocket.AcceptOptions{
		// Origin is verified above: the library matches patterns against the
		// origin host including its port, so a port-less pattern such as
		// example.com could never admit example.com:8443.
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusInternalError, "")

	channel := path.Base(r.URL.Path)
	if cfg.Subscribe == nil || !patternMatch(cfg.Subscribe.Pattern, channel) {
		conn.Close(websocket.StatusPolicyViolation, "channel not allowed")
		return
	}
	if cfg.Publish == nil {
		conn.Close(websocket.StatusPolicyViolation, "no publish target")
		return
	}
	// subscribe is the endpoint's allow-list for both reading and writing, so
	// the target must stay inside it. Provisioning rejects such a config, and
	// this keeps a directly built Config from writing outside the allow-list.
	if !hub.PatternWithin(cfg.Publish.Pattern, cfg.Subscribe.Pattern) {
		conn.Close(websocket.StatusPolicyViolation, "publish target not allowed")
		return
	}

	ctx := r.Context()

	var closed atomic.Bool
	send := func(body []byte) bool {
		if closed.Load() {
			return false
		}
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return conn.Write(wctx, websocket.MessageText, body) == nil
	}

	remoteIP := hub.ClientIP(r.RemoteAddr)
	sub, err := bus.Subscribe(ctx, channel, remoteIP, send)
	if err != nil {
		conn.Close(websocket.StatusInternalError, err.Error())
		return
	}
	defer func() { closed.Store(true); sub.Cancel() }()

	limiter := rate.NewLimiter(rate.Every(rateLimit(bus, channel)), 1)
	denied := 0

	for {
		typ, body, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		if !limiter.Allow() {
			// Rather than dropping silently forever, close a client that keeps
			// hammering the channel well past its limit.
			denied++
			if denied > rateLimitBurst {
				conn.Close(websocket.StatusPolicyViolation, "rate limit exceeded")
				return
			}
			continue
		}
		denied = 0

		info := hub.InterceptInfo{
			Channel:  channel,
			RemoteIP: remoteIP,
			Request:  r,
		}
		out, ok := bus.Intercept(ctx, cfg.Intercept, body, info)
		if !ok || len(out) == 0 {
			continue
		}
		bus.Publish(ctx, []string{cfg.Publish.Pattern}, out, sub.ID)
	}
}

// checkOrigin reports whether the request may connect, given the Origin it
// presents. A request without an Origin (a non-browser client) and a
// same-origin request are always allowed. Every other origin must match one of
// the patterns: a pattern that names a port matches that port only, while a
// port-less pattern matches the origin host whatever its port, so
// `example.com` admits `example.com:8443` and `*.example.com` admits
// `a.example.com:8443`.
func checkOrigin(r *http.Request, patterns []string) error {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return nil
	}
	u, err := url.Parse(origin)
	if err != nil {
		return fmt.Errorf("failed to parse Origin header %q: %w", origin, err)
	}
	if strings.EqualFold(r.Host, u.Host) {
		return nil
	}
	if u.Host == "" {
		return fmt.Errorf("request Origin %q is not a valid URL with a host", origin)
	}
	host := strings.ToLower(u.Host)
	hostname := hostWithoutPort(host)
	for _, pattern := range patterns {
		p := strings.ToLower(pattern)
		if matched, err := filepath.Match(p, host); err == nil && matched {
			return nil
		}
		if hostname != host && !patternHasPort(p) {
			if matched, err := filepath.Match(strings.Trim(p, "[]"), hostname); err == nil && matched {
				return nil
			}
		}
	}
	return fmt.Errorf("request Origin %q is not authorized for Host %q", u.Host, r.Host)
}

// hostWithoutPort strips the port and the IPv6 brackets from a host, returning
// the input unchanged when it has no port.
func hostWithoutPort(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		return strings.Trim(h, "[]")
	}
	return strings.Trim(host, "[]")
}

// patternHasPort reports whether an origin pattern pins a port. IPv6 patterns
// are bracketed, so a colon only counts after the closing bracket.
func patternHasPort(pattern string) bool {
	if strings.HasPrefix(pattern, "[") {
		_, rest, ok := strings.Cut(pattern, "]")
		return ok && strings.HasPrefix(rest, ":")
	}
	return strings.Contains(pattern, ":")
}

// rateLimitBurst is how many messages a client may send while over its rate
// limit before the connection is closed with a policy violation.
const rateLimitBurst = 16

// rateLimit returns the publish interval for channel: the channel rule's
// rate_limit when it sets one, otherwise one message per second.
func rateLimit(bus hub.Bus, channel string) time.Duration {
	if rule := bus.Rule(channel); rule != nil && rule.RateLimit > 0 {
		return rule.RateLimit
	}
	return time.Second
}

// upgradeWriter returns the first writer in the Unwrap chain that supports
// http.Hijacker. coder/websocket asserts http.Hijacker directly, while Caddy's
// response writers expose hijacking only through Unwrap.
func upgradeWriter(w http.ResponseWriter) http.ResponseWriter {
	for {
		if _, ok := w.(http.Hijacker); ok {
			return w
		}
		u, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return w
		}
		w = u.Unwrap()
	}
}

func patternMatch(pattern, name string) bool {
	if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
		return len(name) >= len(pattern)-1 && name[:len(pattern)-1] == pattern[:len(pattern)-1]
	}
	return pattern == name
}
