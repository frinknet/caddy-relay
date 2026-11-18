// Package sse implements the Server-Sent Events transport for caddy-relay.
package sse

import (
	"fmt"
	"io"
	"net/http"
	"path"
	"time"

	"github.com/frinknet/caddy-relay/pkg/hub"
)

// Config is the resolved, provisioned relay_sse config.
type Config struct {
	Subscribe *hub.SubscribeSpec
	Intercept *hub.InterceptSpec
	Publish   *hub.PublishSpec
}

// Serve handles an HTTP request for the relay_sse directive.
func Serve(cfg Config, bus hub.Bus, w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		serveGet(cfg, bus, w, r)
	case http.MethodPost:
		servePost(cfg, bus, w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func serveGet(cfg Config, bus hub.Bus, w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Reject before writing the stream's status line, so a refused channel
	// does not look like a healthy 200 stream.
	channel := path.Base(r.URL.Path)
	if cfg.Subscribe == nil || !patternMatch(cfg.Subscribe.Pattern, channel) {
		http.Error(w, "channel not allowed", http.StatusForbidden)
		return
	}

	ctx := r.Context()
	// Only this goroutine writes to w: the hub calls send from its writer
	// goroutine, so messages are handed over through msgs instead of being
	// written to the response directly. send blocks if this handler falls
	// behind, which lets the hub's own queue apply its drop policy.
	msgs := make(chan []byte, 64)
	send := func(body []byte) bool {
		select {
		case msgs <- body:
			return true
		case <-ctx.Done():
			return false
		}
	}

	// Subscribe before opening the stream. The client's response only arrives
	// once the subscription exists, so a publish that follows it cannot fall
	// into the gap, and a subscription failure is a clean error status rather
	// than an error frame after a 200.
	sub, err := bus.Subscribe(ctx, channel, send)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer sub.Cancel()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush() // the stream is live from here on

	// keepalive
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case body := <-msgs:
			if _, err := fmt.Fprintf(w, "data: %s\n\n", body); err != nil {
				return
			}
			flusher.Flush()
		case <-t.C:
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

func servePost(cfg Config, bus hub.Bus, w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	channel := path.Base(r.URL.Path)
	if cfg.Subscribe == nil || !patternMatch(cfg.Subscribe.Pattern, channel) {
		http.Error(w, "channel not allowed", http.StatusForbidden)
		return
	}
	if cfg.Publish == nil {
		http.Error(w, "no publish target", http.StatusInternalServerError)
		return
	}

	info := hub.InterceptInfo{
		Channel:  channel,
		RemoteIP: r.RemoteAddr,
		Request:  r,
	}
	out, ok := bus.Intercept(r.Context(), cfg.Intercept, body, info)
	if !ok || len(out) == 0 {
		http.Error(w, "dropped", http.StatusBadGateway)
		return
	}
	bus.Publish(r.Context(), []string{cfg.Publish.Pattern}, out, 0)
	w.WriteHeader(http.StatusAccepted)
}

func patternMatch(pattern, name string) bool {
	if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
		return len(name) >= len(pattern)-1 && name[:len(pattern)-1] == pattern[:len(pattern)-1]
	}
	return pattern == name
}
