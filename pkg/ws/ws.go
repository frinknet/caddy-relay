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
}

// Serve handles an HTTP request for the relay_ws directive.
func Serve(cfg Config, bus hub.Bus, w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: false,
	})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusInternalError, "")

	channel := path.Base(r.URL.Path)
	if cfg.Subscribe != nil && !patternMatch(cfg.Subscribe.Pattern, channel) {
		conn.Close(websocket.StatusPolicyViolation, "channel not allowed")
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

	for _, body := range sub.Replay {
		if !send(body) {
			return
		}
	}

	limiter := rate.NewLimiter(rate.Every(rateLimit(cfg)), 1)

	for {
		typ, body, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		if !limiter.Allow() {
			continue
		}

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

// rateLimit returns the per-message interval for this transport.
// The rule's RateLimit is not visible here; ws.go uses a fixed 1s default.
// (Per-connection rate limiting is applied at this layer, per spec.)
func rateLimit(cfg Config) time.Duration { return time.Second }

func patternMatch(pattern, name string) bool {
	if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
		return len(name) >= len(pattern)-1 && name[:len(pattern)-1] == pattern[:len(pattern)-1]
	}
	return pattern == name
}