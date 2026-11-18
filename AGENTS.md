# caddy-relay — AGENTS.md

## Project Overview

caddy-relay is a **Caddy web server plugin** (written in Go) providing real-time messaging relay over WebSocket and Server-Sent Events (SSE), with STUN/TURN for WebRTC NAT traversal. It re-exports a Caddy Docker image with the plugin baked in.

**Module ID**: `relay` (Caddy v2 app IDs are bare names — there is no
`caddy.app.` prefix; verified against v2.8.4)  
**Handler IDs**: `http.handlers.relay_ws`, `http.handlers.relay_sse`  
**Source**: github.com/frinknet/caddy-relay  
**Go version**: 1.22

---

## Directory Layout

| Path | Purpose |
|------|---------|
| `relay.go` | Shell package: blank-imports `pkg/app` so importing the plugin registers its modules/directives |
| `pkg/app/` | Caddy wiring: init, global option parsing, App module, Bus interface impl, template intercept, Caddyfile directives |
| `pkg/hub/` | Shared channel runtime: Hub, Channel actor, Bus interface, pub/sub, ring buffer, publish Result type |
| `pkg/stun/` | Minimal STUN binding server (UDP) |
| `pkg/turn/` | TURN relay server wrapper around pion/turn |
| `pkg/ws/` | WebSocket transport (`relay_ws` directive) |
| `pkg/sse/` | SSE transport (`relay_sse` directive, GET subscribe + POST publish) |
| `www/Caddyfile` | Full-featured example Caddyfile showing all directives |
| `Dockerfile` | Multi-stage build: xcaddy → scratch final image (no upx; see Runtime & Image) |
| `build.sh` | Docker build wrapper; derives the image name from `$PWD` |
| `spec.md` | File-by-file implementation spec, maintained by hand |
| `BACKLOG.md` | Open work, fixed pass notes first, then open items |
| `tests/e2e/` | Container end-to-end: `run.sh` orchestrates the built image, `Caddyfile` is its config |
| `.github/workflows/ci.yml` | gofmt, build, vet, `test -race`, image build + container tests |

---

## Architecture & Data Flow

### Control Flow: Caddy Module Lifecycle

1. **init()** — `pkg/app` registers 3 modules (`Relay`, `RelayWS`, `RelaySSE`), 2 handler directives (`relay_ws`, `relay_sse`), 1 global option (`relay`), and directive ordering ahead of `file_server`; the root package blank-imports it
2. **Provision** — the global `relay` option returns an `httpcaddyfile.App`, so Caddy records it as `apps.relay` and provisions `Relay` itself; a site-level `RelayWS`/`RelaySSE` reaches it with `ctx.App("relay")`. With no global option, the app is still created on demand by that call, but with an empty config
3. **Start** — `Relay.Start()` starts the hub, then optional STUN/TURN listeners. `Hub.Start` is a no-op: channel actors manage their own lifetime
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

- **Subscribe path**: Transport calls `bus.Subscribe(channel, sendCallback)` → Hub creates/finds Channel → Channel actor enqueues the subscription, starts a writer goroutine for it, and that goroutine sends the retained replay followed by live messages → returns the subscription ID and cancel func
- **Publish path**: Transport reads message → runs intercept (URL POST or template transformation) → calls `bus.Publish(patterns, body, excludeSubID)` → Hub fans out to matching channels → Channel actor appends to the ring buffer and does a non-blocking enqueue per participant

### Architecture Decisions

- **Self-closing channels**: Each channel actor runs a 1s ticker and closes itself when idle (`IdleTTL`/`Retain` heuristics), then tells the hub via `Hub.removeChannel`. There is no hub-level sweeper, so the decision reads only actor-local state.
- **Channel as actor**: Each `Channel` has an `inbox` channel + `run()` goroutine. All mutable state (ring, participants) is only touched inside closures sent to the inbox. The `mu` mutex guards only `dead`, which `shutdown` may set from any goroutine.
- **Per-participant writer goroutines**: The actor never calls `send` itself. Each participant has a buffered queue drained by one goroutine, so a slow socket cannot stall the channel; a full queue drops the newest message, increments `Channel.Dropped()`, and is counted in the publish `Status.Dropped`. That goroutine is also the only writer for its subscriber, which is why the SSE handler hands messages to its own loop instead of writing from the hub's goroutine.
- **Bus interface**: Transports never import Caddy types. They receive a `hub.Bus` interface implemented by `*Relay` in the root package.
- **Intercept model**: Two modes — (a) **URL intercept** POSTs the message body to an external HTTP endpoint that returns transformed JSON, (b) **Template intercept** uses Go `text/template` with custom funcs (`message`, `data`, `channel`, `remote_ip`, `request`, `dict`, `set`, `publish`).

---

## Caddyfile Directives

### Global `relay { ... }`

```
relay {
    stun udp/:3478                 # STUN listener addr or "off"
    turn udp/:5349                 # TURN listener addr or "off", needs turn_secret
    turn_secret a-long-random-secret # TURN REST shared secret
    turn_tls :5349                 # TURN over TLS listener addr (TCP)
    turn_tls_cert /path/turn.crt   # Certificate for turn_tls
    turn_tls_key /path/turn.key    # Private key for turn_tls
    turn_external_ip 203.0.113.1   # Advertised in TURN relay addresses
    turn_bind 0.0.0.0              # Address relays bind to (default 0.0.0.0)
    turn_relay_ports 49152-65535   # Port range TURN allocates relays from
    channels { ... }               # Channel rule definitions (see below)
}
```

`Relay.Validate` (a `caddy.Validator`) rejects `turn`/`turn_tls` without
`turn_secret`, without `turn_external_ip`, or `turn_tls` without a key pair.

### `channels { ... }` block

```
channels {
    max_channels 2000     # default 1000
    max_participants 10000 # subscribers across all channels, 0 = unlimited
    max_memory 256M       # soft cap on retained bytes, 0 = unlimited

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
    subscribe room-*       # required: channels this endpoint may address
    publish room-*         # required: provision fails without it
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

**SSE details**: GET requests create a long-lived SSE subscription. POST requests publish a message body. The `publish` sub-directive is required for POST, optional for GET (read-only stream). The handler subscribes *before* writing the stream's headers, so a client that has received the response headers is guaranteed not to miss a later publish, and a subscription failure is a plain error status rather than an error frame after a 200.

---

## Key Conventions & Patterns

- **Go style**: This codebase uses Go 1.22 syntax. Module is `github.com/frinknet/caddy-relay`.
- **Channel pattern matching**: Wildcard suffix `*` only (e.g. `room-*` matches `room-1` but not `other-room`). No glob or regex.
- **Ring buffer**: Messages stored as `[]*Message`, capped by `rule.Messages` count and `rule.MaxSize` byte cap on replay. `hub.NewHub` defaults `Messages` to `hub.DefaultMessages` (100) and `doPublish` trims unconditionally (`c.ring[len(c.ring)-max:]`), so the ring is never unbounded. `MaxSize` caps replay bytes in `buildReplay` and drops oversized messages in `doPublish`.
- **Rate limiting**: Per connection in ws.go, using the interval from the channel's resolved rule via `Bus.Rule(channel)` (1s when the rule sets none). A client that keeps sending over the limit is closed with a policy violation.
- **Memory budget**: `Hub.retained` is an atomic total of ring bytes, adjusted by channel actors. `Hub.reserve` claims `len(body)` in `Hub.reserved` with a CAS on `retained+reserved`, so concurrent publishes cannot overshoot; the claim is released by `Channel.publish` (deferred) once the actor has accounted the bytes, and immediately when the target channel cannot be created. `Hub.participants` counts subscribers globally for `max_participants`.
- **Error handling**: Caddyfile parsing returns `d.ArgErr()` / `d.Errf(...)` and wraps parse failures with the sub-directive name. Runtime errors propagate through Caddy's error handling. Channel publish drops oversized messages (checked in `doPublish`) and reports the drop through `hub.Status.Reason` as well as the log. Intercept failures (`InterceptSpec.IsZero()`) pass the body through unchanged.
- **STUN Transaction ID**: `stun.NewTransactionIDSetter` exists in the pinned pion/stun v3.0.0 and is used directly.

## Non-Obvious Gotchas

1. **pion/stun TransactionID API** — `stun.NewTransactionIDSetter` works with the pinned v3.0.0. The API is known to vary across pion/stun releases, so on upgrade fall back to setting `resp.TransactionID = req.TransactionID` after `stun.Build`.
2. **pion/turn relay generators** — Field names (`Address` expects string, `RelayAddress` expects `net.IP`) verified against the pinned v4.0.0. `RelayAddressGeneratorPortRange` is used when `turn_relay_ports` is set, otherwise `RelayAddressGeneratorStatic`. Verify on upgrade.
3. **Template `Clone()`** — `interceptTemplate` calls `spec.Compiled.Clone()` so the base compiled template stays clean per render. If `Clone` doesn't exist in future versions, parse source per-render instead.
4. **`hubFor()` ordering** — Transports get the app via `ctx.App("relay")`. When the global option is present the app is already in `apps.relay` (the option returns an `httpcaddyfile.App`), so it is provisioned and started even without a `relay_ws`/`relay_sse` directive. With no global option the app is created on demand by `ctx.App`, but with an empty config, so no channel rule matches and every subscribe fails.
5. **WS rate limit uses the channel rule** — `ws.rateLimit(bus, channel)` asks the hub for the rule through `Bus.Rule`. Adding a method to `Bus` means every implementation (including test fakes) must provide it.
6. **Directive publish excludes the sender, template `publish` does not** — For directive publish (ws.go), `sub.ID` is passed as `exclude` so the sender doesn't receive their own message. Template-invoked `publish` passes `0` (no exclusion).
7. **SSE keepalive** — A 15s ticker sends `: keepalive\n\n` comments to keep the connection alive. This is separate from any application-level heartbeat.
8. **Multicall binary** — The Docker image uses `toybox` as a single multicall binary. The `Dockerfile` creates the symlinks in a `RUN` step right after copying it; there is no `entrypoint.sh`.
9. **`max_memory` is exact, not soft** — a publish first claims its bytes in `Hub.reserved` and only then asks the actor to retain them, so the budget is checked against `retained+reserved` and concurrent publishes cannot slip past one stale check. `Channel.publish` releases the claim with `defer`, which also covers the oversize and closed-channel paths. A channel only closes when it has no participants, so no release is needed on close.
10. **A subscriber always has an ID ≥ 1** — `Publish` treats `exclude == 0` as "skip nobody", so IDs start at 1 in `newChannel`. Starting at 0 silently excluded the first subscriber of every channel from all `exclude == 0` publishes (SSE `POST`, template `publish`).
11. **One writer per subscriber** — `send` runs on the participant's writer goroutine, never on the actor goroutine and never on the transport's own loop. The SSE handler therefore hands messages to its own loop over a channel rather than writing to the `http.ResponseWriter` from `send`; writing to a `ResponseWriter` off the handler goroutine is a data race even with a mutex.
12. **SSE subscribes before it writes headers** — `sse.serveGet` calls `bus.Subscribe` first, then writes `200` and flushes. The response arriving is therefore proof that the subscription exists; an earlier ordering lost messages published in between and made `tests/e2e` flaky.
13. **`relay_ws` requires `subscribe` and `publish`** — provision fails without either. SSE allows a missing `publish` because a `GET` stream is read-only; only `POST` needs a target, and the transports also fail closed when `Subscribe` is nil.
14. **TURN auth is TURN REST** — `turn.LongTermTURNRESTAuthHandler` with `turn_secret`: the username is `<expiry-unix>:<name>`, the password is `base64(HMAC-SHA1(secret, username))`, and expired usernames are rejected. `turn.Listen` refuses to start without a secret.
15. **Template source is rebuilt, not captured** — inside `intercept { template { ... } }` the tokenizer has dropped the original spacing, so `parseInterceptBlock` joins same-line tokens with one space and breaks lines on `d.Line()` changes. Use a `<<TAG` heredoc when the exact source matters.
16. **Caddy's admin endpoint checks `Host` exactly** — `checkHost` compares `r.Host` to the allowed origins (which include the port), so anything that omits the port gets a 403. This is why the image healthcheck speaks raw HTTP instead of using `toybox wget`.
17. **Relative intercept paths resolve against the receiving server** — `intercept /api/filter` becomes `<scheme>://<request host>/api/filter` via `resolveInterceptURL`, so it posts back to the same Caddy. An absolute URL is used unchanged.
18. **`turn_external_ip` is advertised, `turn_bind` is bound** — relays bind `turn_bind` (0.0.0.0 by default) and hand clients the external IP. Without that split, a container with published ports cannot reach its own relays.
19. **`subscribe` vs `publish` are independent** — the path channel is checked against `subscribe`; `publish` is where the message goes. An endpoint may read `room-*` and write to `lobby`.
20. **`Publish` waits for the actor** — it enqueues `doPublish` and blocks on the reply so `Result` reports this message's outcome, including `Dropped` and `Reason`. That is safe because `doPublish` never blocks (per-participant enqueues are non-blocking); a channel that closes first reports `ReasonClosed`.
21. **E2E host and container ports must match one-to-one** — `tests/e2e/run.sh` publishes the same port on both sides because the relative intercept posts back to the request's own `Host` (gotcha 17). Ephemeral `-p 127.0.0.1::80` mappings break that path. The script picks a random block and retries when Docker refuses a bound port; the TURN relay range is not published because the script does not allocate relays.

## Build & Deploy

| Command | Via | Notes |
|---------|-----|-------|
| Build | `go build` | Compiles the plugin standalone |
| Docker | `./build.sh` | The only supported build path: derives the image name, refreshes old images, then builds `caddy-relay:test` (multi-stage, `FROM scratch`) |
| Test | `go test -race ./...` | Hub, transport, STUN, TURN and Caddyfile adapt/validate tests |
| Container test | `tests/e2e/run.sh` | Runs the built image with `tests/e2e/Caddyfile` and checks it over published ports; needs `caddy-relay:test` (or `$CADDY_RELAY_IMAGE`) |
| Vet | `go vet` | Static analysis (passes clean) |
| Container entry | `/bin/caddy run --config /etc/caddy/Caddyfile --adapter caddyfile` | Set in Dockerfile ENTRYPOINT + CMD; no shell wrapper |
| Plugin build | `xcaddy build --with github.com/frinknet/caddy-relay` | The `build` stage does this with `--replace` pointing at the local tree |

The `Dockerfile` supports `TARGETARCH` (amd64/arm64) for downloading the correct toybox binary.

**Always run the container suite before reporting a change as done**: `go test -race ./...`, then `./build.sh`, then `tests/e2e/run.sh`. Unit tests alone are not enough, because the image, the Dockerfile and the shipped `tests/e2e/Caddyfile` are only exercised there.

**Always put a timeout on shell commands**: pass a bounded `auto_background_after` (and the tool's own timeout where one exists) rather than relying on the default, and wrap potentially hanging steps so a build, test or container run can never stall a session. Nothing here is allowed to run unbounded.

---

## Dependency Versions

| Dependency | Module | Version | Notes |
|------------|--------|---------|-------|
| Caddy | caddyserver/caddy/v2 | v2.8.4 | Web server host. The Dockerfile builder image matches this tag; v2.11.9 needs a Go 1.26 toolchain, see BACKLOG.md |
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

## Runtime & Image

- **Toybox symlinks**: created by a `RUN` step in the `Dockerfile` immediately after `toybox` is copied, which is what makes `sh`, `ls`, `mv`, etc. available in the scratch image. There is no entrypoint script.
- **Config**: `www/Caddyfile` is copied to `/srv/www`, then moved to `/etc/caddy/Caddyfile` during the build.
- **Environment**: `PATH=/bin:/usr/bin:/usr/local/bin`, `XDG_CONFIG_HOME=/etc/caddy`, `XDG_DATA_HOME=/var/lib/caddy`, `XDG_CACHE_HOME=/var/cache/caddy`, `HOME=/tmp`, `TMPDIR=/tmp`.
- **Healthcheck**: speaks HTTP over a raw toybox socket, `printf 'GET /config/ HTTP/1.1\r\nHost: 127.0.0.1:2019\r\nConnection: close\r\n\r\n' | nc | grep -q '200 OK'`, every 30s, 5s timeout, 3 retries. `toybox wget` cannot be used: it sends `Host: 127.0.0.1` with no port, and the admin endpoint requires an exact Host match, so it can only ever get a 403. This assumes the default admin address.
- **No upx**: upx 4.2.4 cannot pack the published toybox binary at any level ("MemBuffer invalid array index 0"), so the step was dropped rather than made non-fatal.
- **Exposed ports**: 80 (HTTP), 443 (HTTPS), 443/udp (QUIC), 3478/udp (STUN), 5349/udp and 5349/tcp (TURN, TURN over TLS). The TURN relay range from `turn_relay_ports` must be published by the caller.
- **CI**: `.github/workflows/ci.yml` runs `gofmt -l`, `go build`, `go vet` and `go test -race`, then a second job runs `./build.sh` and `tests/e2e/run.sh`.

## File Inventory

| File | Lines | Purpose |
|------|-------|---------|
| `README.md` | 275 | Project overview, directives, quick start, doc pointers |
| `AGENTS.md` | 266 | This file |
| `spec.md` | 477 | File-by-file implementation spec, maintained by hand |
| `BACKLOG.md` | 190 | Open work |
| `.gitignore` | 6 | Excludes `AGENTS.md`, `BACKLOG.md`, editor swap/backup files, `.env` |
| `.github/workflows/ci.yml` | 41 | gofmt, build, vet, `test -race`; second job: `./build.sh` + `tests/e2e/run.sh` |
| `.dockerignore` | 8 | Excludes everything dot-prefixed plus `*.md`, `build.sh`, `Dockerfile` |
| `go.mod` | 124 | Module declaration + dependency pins |
| `go.sum` | 481 | Dependency checksums |
| `NOTICE` | 9 | Build metadata consumed by `build.sh` (repo, copyright, license) |
| `LICENSE` | 201 | Apache-2.0 |
| `relay.go` | 11 | Shell: blank-imports `pkg/app` to register the plugin |
| `pkg/app/relay.go` | 887 | Caddy wiring, config parsing, intercept, directive handlers |
| `pkg/app/relay_test.go` | 463 | Caddyfile adapt/validate tests, including `apps.relay` registration |
| `pkg/hub/spec.go` | 60 | ChannelRule, ChannelsConfig, SubscribeSpec, PublishSpec, InterceptSpec types |
| `pkg/hub/bus.go` | 41 | Bus interface + Subscription/SubID types |
| `pkg/hub/message.go` | 17 | Message struct with Clone |
| `pkg/hub/publish.go` | 78 | Result/Status types: per-channel enqueue, drop count and reason |
| `pkg/hub/hub.go` | 305 | Hub: channel registry, caps, memory budget, subscribe, publish, pattern matching |
| `pkg/hub/channel.go` | 398 | Channel actor: ring buffer, participant queues, replay, idle close, eviction |
| `pkg/hub/hub_test.go` | 669 | Hub unit tests (ring, caps, exclusion, drops, sweep, memory) |
| `pkg/hub/subscribe.go` | 9 | Subscription error values |
| `pkg/stun/stun.go` | 51 | Minimal STUN binding request/response server |
| `pkg/stun/stun_test.go` | 91 | STUN binding tests |
| `pkg/turn/turn.go` | 147 | TURN server wrapper: REST auth, UDP and TLS listeners, relay port range |
| `pkg/turn/turn_test.go` | 289 | TURN allocation tests (credentials, port range, TLS listener) |
| `pkg/ws/ws.go` | 121 | WebSocket transport: handshake, origins, subscribe loop, rate limit, publish |
| `pkg/ws/ws_test.go` | 235 | WebSocket transport tests against a fake bus |
| `pkg/sse/sse.go` | 134 | SSE transport: GET (stream) + POST (publish) |
| `pkg/sse/sse_test.go` | 208 | SSE transport tests against a fake bus |
| `tests/e2e/run.sh` | 244 | Container end-to-end: renders the config, runs the image, checks HTTP/SSE/WS/STUN/TLS |
| `tests/e2e/Caddyfile` | 108 | Container config: STUN, TURN, TLS, channels, both transports, intercepts, HTTPS |
| `www/Caddyfile` | 86 | Full-featured example config |
| `Dockerfile` | 115 | Multi-stage build to scratch image |
| `build.sh` | 96 | Image build wrapper |