# caddy-relay — AGENTS.md

## Project Overview

caddy-relay is a **Caddy web server plugin** (written in Go) providing real-time messaging relay over WebSocket and Server-Sent Events (SSE), with STUN/TURN for WebRTC NAT traversal. It re-exports a Caddy Docker image with the plugin baked in.

**Module ID**: `caddy.app.relay`  
**Source**: github.com/frinknet/caddy-relay  
**Go version**: 1.22

---

## Directory Layout

| Path | Purpose |
|------|---------|
| `relay.go` | Root package: wires everything into Caddy (init, global option parsing, App module, Bus interface impl, template intercept, Caddyfile directives) |
| `pkg/hub/` | Shared channel runtime: Hub, Channel actor, Bus interface, pub/sub, ring buffer, publish Result type |
| `pkg/stun/` | Minimal STUN binding server (UDP) |
| `pkg/turn/` | TURN relay server wrapper around pion/turn |
| `pkg/ws/` | WebSocket transport (`relay_ws` directive) |
| `pkg/sse/` | SSE transport (`relay_sse` directive, GET subscribe + POST publish) |
| `www/Caddyfile` | Full-featured example Caddyfile showing all directives |
| `entrypoint.sh` | Docker entrypoint (toybox multicall setup, then exec caddy) |
| `Dockerfile` | Multi-stage build: xcaddy → compress with upx → scratch final image |

---

## Architecture & Data Flow

### Control Flow: Caddy Module Lifecycle

1. **init()** — `relay.go` registers 3 modules (`Relay`, `RelayWS`, `RelaySSE`), 2 handler directives (`relay_ws`, `relay_sse`), and 1 global option (`relay`)
2. **Provision** — Caddy provisions `Relay` app first (creates `hub.Hub`), then site-level `RelayWS`/`RelaySSE` look up the app via `ctx.App("relay")`
3. **Start** — `Relay.Start()` starts the hub sweeper goroutine, then optional STUN/TURN listeners
4. **ServeHTTP** — `RelayWS.ServeHTTP` / `RelaySSE.ServeHTTP` delegate to `ws.Serve` / `sse.Serve`
5. **Stop** — `Relay.Stop()` tears down STUN/TURN, then stops hub

### Message Flow: Client → Transport → Hub → Channel → Subscribers

```
Client → WebSocket/SSE → ws.Serve/sse.Serve → bus.Subscribe() → Hub.getOrCreate() → Channel.subscribe()
                                                        ↕
Client → WebSocket Message → ws.Serve read loop → bus.Intercept() → bus.Publish() → Hub.Publish()
                                                                                        ↕
                                                                                  Channel.doPublish() → fan-out to participants
```

- **Subscribe path**: Transport calls `bus.Subscribe(channel, sendCallback)` → Hub creates/finds Channel → Channel actor enqueues subscription → returns replay + cancel func
- **Publish path**: Transport reads message → runs intercept (URL POST or template transformation) → calls `bus.Publish(patterns, body, excludeSubID)` → Hub fans out to matching channels → Channel actor appends to ring buffer and calls each participant's `send` callback

### Architecture Decisions

- **Hub with sweep goroutine**: A background goroutine (`hub.sweep()`) ticks every 1s, closing idle channels (both `IdleTTL` and `Retain` heuristics). Channels are actors with their own inbox goroutine.
- **Channel as actor**: Each `Channel` has an `inbox` channel + `run()` goroutine. All mutable state (ring, participants) is only touched inside closures sent to the inbox. The `mu` mutex only guards `ring`/`dead` for the sweeper's read-only peek into `retainedBytes`/`shouldClose`.
- **Bus interface**: Transports never import Caddy types. They receive a `hub.Bus` interface implemented by `*Relay` in the root package.
- **Intercept model**: Two modes — (a) **URL intercept** POSTs the message body to an external HTTP endpoint that returns transformed JSON, (b) **Template intercept** uses Go `text/template` with custom funcs (`message`, `data`, `channel`, `remote_ip`, `request`, `dict`, `set`, `publish`).

---

## Caddyfile Directives

### Global `relay { ... }`

```
relay {
    stun udp/:3478                 # STUN listener addr or "off"
    turn udp/:3478                 # TURN listener addr or "off"  
    turn_tls udp/:5349             # TURN TLS addr (optional)
    turn_external_ip 203.0.113.1   # Required for TURN relay addresses
    turn_relay_ports 49152-65535   # Optional: port range for TURN relays
    channels { ... }               # Channel rule definitions (see below)
}
```

### `channels { ... }` block

```
channels {
    max_channels 2000     # default 1000
    max_memory 256M       # 0 = unlimited

    room-* {              # pattern with wildcard
        messages        50
        participants    100
        max_size        64K
        rate_limit      100ms
        idle_ttl        5m
        retain          10m
    }
    
    lobby {              # literal pattern
        messages        10
        participants    500
        max_size        16K
        idle_ttl        30s
    }
}
```

### Site-level `relay_ws` / `relay_sse`

```
relay_ws {
    subscribe room-*       # required: channel pattern to allow
    publish room-*         # required: channel pattern to publish to
    intercept /api/filter  # optional: URL or template block
}

relay_sse {
    subscribe room-*
    publish room-*
    intercept {
        template {
            {{ message | publish channel }}
        }
    }
}
```

**SSE details**: GET requests create a long-lived SSE subscription. POST requests publish a message body. The `publish` sub-directive is required for POST, optional for GET (read-only stream).

---

## Key Conventions & Patterns

- **Go style**: This codebase uses Go 1.22 syntax. Module is `github.com/frinknet/caddy-relay`.
- **Channel pattern matching**: Wildcard suffix `*` only (e.g. `room-*` matches `room-1` but not `other-room`). No glob or regex.
- **Ring buffer**: Messages stored as `[]*Message`, capped by `rule.Messages` count and `rule.MaxSize` byte cap on replay. Dropping old messages happens at the slice level (`c.ring[len(c.ring)-c.rule.Messages:]`).
- **Rate limiting**: Applied per-connection in ws.go (fixed 1s). Channel-level `RateLimit` from `ChannelRule` is **not** wired to the transport layer — see plan.md wiring note #6.
- **Error handling**: Caddyfile parsing returns `d.ArgErr()` / `d.Errf(...)`. Runtime errors propagate through Caddy's error handling. Channel publish silently drops oversized messages (checked in `doPublish`). Intercept failures (`InterceptSpec.IsZero()`) pass the body through unchanged.
- **STUN Transaction ID**: The `stun.NewTransactionIDSetter` API varies by pion/stun version. If missing, use `resp.TransactionID = req.TransactionID` after `stun.Build`.

## Non-Obvious Gotchas

1. **pion/stun TransactionID API** — `stun.NewTransactionIDSetter` works with current pinned version but is known to vary across pion/stun releases. If upgrade breaks, fallback: set `resp.TransactionID = req.TransactionID` manually after `stun.Build`.
2. **pion/turn `RelayAddressGeneratorStatic`** — Field names (`Address` expects string, `RelayAddress` expects `net.IP`) verified against current pinned v4.0.0. Verify on upgrade.
3. **Template `Clone()`** — `interceptTemplate` calls `spec.Compiled.Clone()` so the base compiled template stays clean per render. If `Clone` doesn't exist in future versions, parse source per-render instead.
4. **`hubFor()` ordering** — The `relay` App must be provisioned **before** any site-level transport. Caddy provisions apps before handlers, so this works as long as `Relay` is in the module list.
5. **WS rate limit ignores channel rules** — `ws.rateLimit()` returns a fixed 1s interval. The channel's `RateLimit` field is not accessible from ws.go. To make it rule-aware, extend `Bus` with a `Rule(channel)` method or carry it on the subscription.
6. **`publish` template func excludes sender** — For directive publish (ws.go), `sub.ID` is passed as `exclude` so the sender doesn't receive their own message. Template-invoked `publish` passes `0` (no exclusion).
7. **SSE keepalive** — A 15s ticker sends `: keepalive\n\n` comments to keep the connection alive. This is separate from any application-level heartbeat.
8. **Multicall binary** — The Docker image uses `toybox` as a single multicall binary. `entrypoint.sh` creates symlinks for each toybox command on startup. `Dockerfile` also does this during build.

## Build & Deploy

| Command | Via | Notes |
|---------|-----|-------|
| Build | `go build` | Compiles the plugin standalone |
| Docker | `docker build -t caddy-relay .` | Multi-stage: builds caddy with xcaddy, compresses with upx, final image is `FROM scratch` (~few MB) |
| Test | `go test` | No tests written yet |
| Vet | `go vet` | Static analysis (passes clean) |
| Container entry | `/bin/caddy run --config /etc/caddy/Caddyfile --adapter caddyfile` | Set in Dockerfile ENTRYPOINT + CMD |
| Entrypoint | `entrypoint.sh` | Copied as `/bin/entrypoint.sh`, sets up toybox symlinks, then execs caddy |
| Plugin build | `xcaddy build --with github.com/frinknet/caddy-relay` | The Dockerfile Stage 1 does this |

The `Dockerfile` supports `TARGETARCH` (amd64/arm64) for downloading the correct toybox binary.

---

## Dependency Versions

| Dependency | Module | Version | Notes |
|------------|--------|---------|-------|
| Caddy | caddyserver/caddy/v2 | v2.8.4 | Web server host |
| WebSocket | coder/websocket | v1.8.12 | WS handshake & messaging |
| STUN | pion/stun/v3 | v3.0.0 | NAT binding discovery |
| TURN | pion/turn/v4 | v4.0.0 | Media relay server |
| Logging | go.uber.org/zap | v1.27.0 | Structured logging |
| Time | golang.org/x/time | v0.6.0 | Duration parsing, tickers |
| Templates | text/template | std | Intercept template engine |
| JSON | encoding/json | std | JSON marshal/unmarshal for intercept |

## Template Func Reference (Intercept)

Available inside `intercept { template { ... } }` blocks:

| Func | Signature | Description |
|------|-----------|-------------|
| `message` | `() -> string` | Raw message body as string |
| `data` | `() -> map[string]any` | Parsed JSON body (or empty map if not JSON) |
| `channel` | `() -> string` | Channel name from URL path |
| `remote_ip` | `() -> string` | Subscriber's remote IP |
| `request` | `() -> any` | The original `*http.Request` |
| `dict` | `(kv ...any) -> map[string]any` | Build a dict from key-value pairs |
| `set` | `(m map[string]any, k string, v any) -> map[string]any` | Set key in dict (creates new if nil) |
| `publish` | `(patterns ...string, value any) -> Result` | Publish value to channels (Stringer returns "" so bare use emits nothing) |

## Entrypoint & Runtime

- **`entrypoint.sh`**: Creates toybox multicall symlinks, ensures `/var/lib/caddy`, `/var/cache/caddy`, `/tmp` exist, then execs caddy with the Caddyfile config.
- **Environment**: `PATH=/bin:/usr/bin:/usr/local/bin`, `XDG_CONFIG_HOME=/etc/caddy`, `XDG_DATA_HOME=/var/lib/caddy`, `HOME=/tmp`, `TMPDIR=/tmp`.
- **Healthcheck**: Sends raw HTTP GET to admin API (`:2019/config/`) via `printf | nc | grep -q "200 OK"`. Uses proper `Host` header to pass Caddy's host check. Timeout at 3s.
- **Exposed ports**: 80 (HTTP), 443 (HTTPS), 443/udp (QUIC).

## File Inventory

| File | Lines | Est. | Purpose |
|------|-------|------|---------|
| `README.md` | ~40 | Project overview, quick start, status |
| `.gitignore` | ~14 | Go + IDE excludes |
| `go.mod` | 12 | Module declaration + dependency pins |
| `relay.go` | ~710 | Root: Caddy wiring, config parsing, intercept, directive handlers |
| `pkg/hub/spec.go` | ~57 | ChannelRule, ChannelsConfig, SubscribeSpec, PublishSpec, InterceptSpec types |
| `pkg/hub/bus.go` | ~38 | Bus interface + Subscription/SubID types |
| `pkg/hub/message.go` | ~18 | Message struct with Clone |
| `pkg/hub/publish.go` | ~10 | Result type (map[string]bool with empty Stringer) |
| `pkg/hub/hub.go` | ~170 | Hub: channel lifecycle, subscribe, publish, sweep, pattern matching |
| `pkg/hub/channel.go` | ~200 | Channel actor: ring buffer, participants, publish, replay, sweep heuristics |
| `pkg/hub/subscribe.go` | ~10 | Error definitions + replay contract doc |
| `pkg/stun/stun.go` | ~55 | Minimal STUN binding request/response server |
| `pkg/turn/turn.go` | ~80 | TURN server wrapper with shared-secret auth |
| `pkg/ws/ws.go` | ~87 | WebSocket transport: handshake, subscribe loop, rate-limit, publish |
| `pkg/sse/sse.go` | ~127 | SSE transport: GET (stream) + POST (publish) |
| `www/Caddyfile` | ~75 | Full-featured example config |
| `entrypoint.sh` | ~18 | Docker entrypoint |
| `Dockerfile` | ~87 | Multi-stage build to scratch image |