// Package hub provides the shared runtime for caddy-relay: the hub of
// channels, the channel actor, publish/subscribe mechanics, and the
// narrow Bus interface used by transports (ws, sse).
package hub

import (
	"text/template"
	"time"
)

// ChannelRule is one channel pattern declaration from the `channels {}` block.
type ChannelRule struct {
	Pattern      string        // "room-*", "lobby", etc.
	Messages     int           // ring buffer size
	Participants int           // max concurrent subscribers
	MaxSize      int64         // max bytes per message
	RateLimit    time.Duration // per-connection publish rate limit
	IdleTTL      time.Duration // close after no participants this long
	Retain       time.Duration // keep ring this long after last message
}

// ChannelsConfig is the full `channels {}` block.
type ChannelsConfig struct {
	Rules       []*ChannelRule
	MaxChannels int
	MaxMemory   int64 // bytes; 0 = unlimited
}

// SubscribeSpec is the parsed `subscribe <pattern>` sub-directive.
type SubscribeSpec struct {
	Pattern string
}

// PublishSpec is the parsed `publish <pattern>` sub-directive.
type PublishSpec struct {
	Pattern string
}

// InterceptSpec is the parsed `intercept` sub-directive.
// Exactly one of URL or Template is set. Compiled holds the parsed
// template, populated at provision time.
type InterceptSpec struct {
	URL      string
	Template string
	Compiled *template.Template
}

// IsZero reports whether no intercept was configured.
func (s *InterceptSpec) IsZero() bool {
	return s == nil || (s.URL == "" && s.Template == "")
}

// InterceptInfo carries the per-message context passed to intercept.
type InterceptInfo struct {
	Channel  string
	RemoteIP string
	Request  any // *http.Request; typed as any to avoid http import in transports' eyes
}