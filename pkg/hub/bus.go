package hub

import "context"

// Bus is the narrow contract transports (ws, sse) use to talk to the hub.
// It is implemented by the root relay package, not by Hub itself — so the
// transports never import the Caddy layer.
type Bus interface {
	// Subscribe joins a channel. remoteIP is the client host (empty when
	// unknown) and is what max_participants_per_ip counts against. send is
	// called, from a goroutine owned by the hub, for each message the
	// subscriber should receive: retained messages first, then live ones, in
	// order. Returning false from send drops the subscriber. send may block
	// without stalling the channel; a subscriber that falls behind has its
	// newest messages dropped. The returned Subscription carries a stable ID
	// and a Cancel func.
	Subscribe(ctx context.Context, channel, remoteIP string, send func([]byte) bool) (*Subscription, error)

	// Publish fans out body to every existing channel matching any pattern.
	// Concrete names create channels on demand. exclude is the subscriber
	// ID to skip (0 = skip nobody; used by transports to exclude the sender
	// for directive publish, and 0 for template publish / SSE POST).
	Publish(ctx context.Context, patterns []string, body []byte, exclude SubID) Result

	// Intercept runs the configured intercept (URL or template) on body.
	// Returns the transformed body and ok=false if the message should be
	// dropped.
	Intercept(ctx context.Context, spec *InterceptSpec, body []byte, info InterceptInfo) ([]byte, bool)

	// Rule returns the resolved rule for a channel name, or nil when no rule
	// matches. Transports use it for limits the hub does not apply itself,
	// such as the per-connection publish rate.
	Rule(channel string) *ChannelRule
}

// SubID is a stable identifier for a subscription within the hub.
type SubID uint64

// Subscription is what Subscribe returns.
type Subscription struct {
	ID     SubID
	Cancel func()
}
