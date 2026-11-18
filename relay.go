// Package relay registers the caddy-relay plugin with Caddy. The
// implementation lives in pkg/app; importing this package is what makes
// Caddy aware of the relay app and the relay_ws/relay_sse directives.
package relay

import (
	_ "github.com/frinknet/caddy-relay/pkg/app"
)
