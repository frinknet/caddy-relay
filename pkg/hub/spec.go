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
	Pattern      string        `json:"pattern"`                // "room-*", "lobby", etc.
	Messages     int           `json:"messages,omitempty"`     // ring buffer size
	Participants int           `json:"participants,omitempty"` // max concurrent subscribers
	MaxSize      int64         `json:"max_size,omitempty"`     // max bytes per message
	RateLimit    time.Duration `json:"rate_limit,omitempty"`   // per-connection publish rate limit
	IdleTTL      time.Duration `json:"idle_ttl,omitempty"`     // close after no participants this long
	Retain       time.Duration `json:"retain,omitempty"`       // keep ring this long after last message
}

// ChannelsConfig is the full `channels {}` block.
type ChannelsConfig struct {
	Rules       []*ChannelRule `json:"rules,omitempty"`
	MaxChannels int            `json:"max_channels,omitempty"`
	// MaxParticipants caps subscribers across all channels; 0 = unlimited.
	MaxParticipants int `json:"max_participants,omitempty"`
	// MaxParticipantsPerIP caps simultaneous subscribers from one client
	// address; 0 = unlimited. Transports pass the client host on subscribe.
	MaxParticipantsPerIP int   `json:"max_participants_per_ip,omitempty"`
	MaxMemory            int64 `json:"max_memory,omitempty"` // bytes; 0 = unlimited
}

// SubscribeSpec is the parsed `subscribe <pattern>` sub-directive.
type SubscribeSpec struct {
	Pattern string `json:"pattern,omitempty"`
}

// PublishSpec is the parsed `publish <pattern>` sub-directive.
type PublishSpec struct {
	Pattern string `json:"pattern,omitempty"`
}

// InterceptSpec is the parsed `intercept` sub-directive.
// Exactly one of URL or Template is set. Compiled holds the parsed
// template, populated at provision time.
type InterceptSpec struct {
	URL      string             `json:"url,omitempty"`
	Template string             `json:"template,omitempty"`
	Compiled *template.Template `json:"-"`
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
