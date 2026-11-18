package hub

import "context"

// Bus is the narrow contract transports (ws, sse) use to talk to the hub.
// It is implemented by the root relay package, not by Hub itself — so the
// transports never import the Caddy layer.
type Bus interface {
	// Subscribe joins a channel. send is called for each message the
	// subscriber should receive (replay is returned separately). Returning
	// false from send drops the subscriber. The returned Subscription
	// carries the replay, a stable ID, and a Cancel func.
	Subscribe(ctx context.Context, channel string, send func([]byte) bool) (*Subscription, error)

	// Publish fans out body to every existing channel matching any pattern.
	// Concrete names create channels on demand. exclude is the subscriber
	// ID to skip (0 = skip nobody; used by transports to exclude the sender
	// for directive publish, and 0 for template publish / SSE POST).
	Publish(ctx context.Context, patterns []string, body []byte, exclude SubID) Result

	// Intercept runs the configured intercept (URL or template) on body.
	// Returns the transformed body and ok=false if the message should be
	// dropped.
	Intercept(ctx context.Context, spec *InterceptSpec, body []byte, info InterceptInfo) ([]byte, bool)
}

// SubID is a stable identifier for a subscription within the hub.
type SubID uint64

// Subscription is what Subscribe returns.
type Subscription struct {
	ID     SubID
	Replay [][]byte // prior messages, oldest first, byte-capped
	Cancel func()
}