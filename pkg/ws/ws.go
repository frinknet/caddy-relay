// Package ws implements the WebSocket transport for caddy-relay.
package ws

import (
	"context"
	"net/http"
	"path"
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
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: cfg.OriginPatterns,
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

	sub, err := bus.Subscribe(ctx, channel, send)
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
			RemoteIP: r.RemoteAddr,
			Request:  r,
		}
		out, ok := bus.Intercept(ctx, cfg.Intercept, body, info)
		if !ok || len(out) == 0 {
			continue
		}
		bus.Publish(ctx, []string{cfg.Publish.Pattern}, out, sub.ID)
	}
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

func patternMatch(pattern, name string) bool {
	if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
		return len(name) >= len(pattern)-1 && name[:len(pattern)-1] == pattern[:len(pattern)-1]
	}
	return pattern == name
}
