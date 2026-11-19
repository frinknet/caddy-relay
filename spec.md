# caddy-relay — implementation spec

This document describes the implementation file by file. It is maintained by
hand and deliberately does not reproduce source code; read the files for exact
signatures. Non-Go files (`Dockerfile`, `www/Caddyfile`, `build.sh`) and the
test files are covered alongside their packages.

## Repository map

| Path | Responsibility |
|------|----------------|
| `relay.go` | Shell package: blank-imports `pkg/app` so importing the plugin registers its modules and directives |
| `pkg/app/relay.go` | Caddy module registration, global option and directive parsing, the `Relay` app, intercept execution and template functions |
| `pkg/hub/spec.go` | Configuration types shared by the hub, app and transports |
| `pkg/hub/bus.go` | The narrow `Bus` contract transports use to reach the hub |
| `pkg/hub/message.go` | A retained message and its clone |
| `pkg/hub/publish.go` | The per-channel publish result and its empty Stringer |
| `pkg/hub/hub.go` | Channel registry, caps, memory budget and publish fan-out |
| `pkg/hub/channel.go` | Channel actor: ring buffer, participant queues, replay, idle close and eviction |
| `pkg/hub/subscribe.go` | Subscription error values |
| `pkg/hub/net.go` | `ClientIP`: the client host used by per-IP limits |
| `pkg/stun/stun.go` | Minimal STUN binding server |
| `pkg/turn/turn.go` | TURN server wrapper: REST credentials, UDP, TLS and relay address generation |
| `pkg/ws/ws.go` | WebSocket transport |
| `pkg/sse/sse.go` | Server-Sent Events transport |

## Configuration surface

### Global `relay { ... }`

The global option accepts the following sub-directives:

| Sub-directive | Meaning |
|---------------|---------|
| `stun <addr>` | STUN UDP listen address; `off` disables it |
| `turn <addr>` | TURN UDP listen address; `off` disables it |
| `turn_secret <secret>` | Shared secret for TURN REST credentials; required when TURN is enabled |
| `turn_tls <addr>` | TURN over TLS listen address; `off` disables it |
| `turn_tls_domain <name>` | Serve the TLS listener with the certificate Caddy manages for `<name>`; requires the `tls` app |
| `turn_tls_cert <path>` | Certificate for the TLS listener (takes precedence over `turn_tls_domain`) |
| `turn_tls_key <path>` | Private key for the TLS listener |
| `turn_relay_ports <lo>-<hi>` | Port range TURN allocates relays from |
| `turn_external_ip <ip>` | Address advertised in relay allocations; required when TURN is enabled |
| `turn_bind <addr>` | Local address relays bind to; empty means `0.0.0.0` |
| `turn_max_allocations <n>` | Cap on concurrent relay allocations; `0` means unlimited |
| `turn_auth_rate <n>` | Cap on authenticated requests per second per client IP; `0` means unlimited |
| `channels { ... }` | Channel limits and per-pattern rules |

### `channels { ... }`

The block accepts global limits and any number of channel patterns:

- `max_channels <n>` caps the number of live channels; the hub defaults it to
  `1000`.
- `max_participants <n>` caps subscribers across all channels; `0` means
  unlimited.
- `max_participants_per_ip <n>` caps subscribers from one client address;
  `0` means unlimited.
- `max_memory <size>` sets a soft cap on retained ring bytes; `0` means
  unlimited. Sizes accept `K`, `M` and `G` suffixes (powers of 1024).
- Any other token starts a channel rule block. The first rule whose pattern
  matches a channel name wins. A pattern ending in `*` is a prefix match; any
  other pattern is a literal match.

A channel rule block accepts:

| Sub-directive | Field | Meaning |
|---------------|-------|---------|
| `messages <n>` | `Messages` | Ring depth; the hub default is 100 |
| `participants <n>` | `Participants` | Per-channel subscriber cap |
| `max_size <size>` | `MaxSize` | Maximum bytes per retained message and replay budget |
| `rate_limit <dur>` | `RateLimit` | Per-connection publish interval |
| `idle_ttl <dur>` | `IdleTTL` | Time with no subscribers before the channel may close |
| `retain <dur>` | `Retain` | Time to keep the ring after the last message |

### Site directives

`relay_ws` and `relay_sse` are HTTP handler directives. Both accept
`subscribe <pattern>`, `publish <pattern>` and `intercept ...`. `relay_ws`
additionally accepts `origins <pattern> ...` for WebSocket origin checks. A
pattern without a port admits that host on any port; one with a port pins it.

`relay_ws` requires both `subscribe` and `publish`. `relay_sse` requires
`subscribe`; `publish` is optional because a `GET` stream is read-only, but a
`POST` without `publish` fails at request time. When `publish` is set it must
stay within `subscribe` (`hub.PatternWithin`), so the endpoint's allow-list
bounds writes as well as reads; provisioning rejects a target outside it.

The `intercept` sub-directive accepts either an inline URL or a block:

```caddyfile
intercept /api/filter
```

or

```caddyfile
intercept {
    template {
        {{ message | publish channel }}
    }
}
```

A `template <<TAG ... TAG` form keeps the source verbatim. A braced template
block is rebuilt line by line because the Caddyfile tokenizer has already
dropped the original spacing.

## `pkg/hub/spec.go`

Defines the configuration types that travel between the Caddy layer, the hub
and the transports.

- `ChannelRule` is one channel pattern rule. `Pattern` is the literal or
  prefix pattern; `Messages` is the ring depth; `Participants` is the
  per-channel subscriber cap; `MaxSize` is the message/replay byte limit;
  `RateLimit` is the recommended publish interval; `IdleTTL` is the idle close
  delay; `Retain` is how long the ring survives after the last message. All
  fields have JSON tags so the type can round-trip through the admin API.
- `ChannelsConfig` is the full `channels {}` block: the rule slice plus
  `MaxChannels`, `MaxParticipants`, `MaxParticipantsPerIP` and `MaxMemory`.
- `SubscribeSpec` and `PublishSpec` each carry one channel pattern.
- `InterceptSpec` carries exactly one interception mode: a URL or a template
  source. `Compiled` is the parsed template and is populated at provision time;
  it is excluded from JSON. `IsZero` reports whether neither a URL nor a
  template is configured, which makes the intercept a pass-through.
- `InterceptInfo` is the per-message context handed to intercepts: the channel
  name, the remote host and the originating request. `Request` is typed as
  `any` so the hub package does not import `net/http`.

## `pkg/hub/bus.go`

Defines `Bus`, the contract the WebSocket and SSE transports use. The root
`relay` package implements it; the transports never import Caddy types.

- `Subscribe(ctx, channel, send)` joins a channel and returns a
  `*Subscription`. `send` is called from a goroutine owned by the hub, first
  for retained messages and then for live messages, in order. A `send` that
  returns false drops the subscriber. `send` must not be called from the
  caller's goroutine, and a slow `send` does not stall the channel.
- `Publish(ctx, patterns, body, exclude)` fans a body out to every existing
  channel matching any pattern. A concrete name creates a channel on demand.
  `exclude` is the subscriber ID to skip; `0` means skip nobody.
- `Intercept(ctx, spec, body, info)` runs the configured URL or template
  interception and returns the transformed body plus an `ok` flag. `ok=false`
  means the message should be dropped.
- `Rule(channel)` returns the resolved rule for a channel name, or nil when no
  rule matches. Transports use it for limits the hub does not enforce itself,
  such as the publish rate.
- `SubID` is a stable subscriber identifier. `Subscription` carries that ID and
  a `Cancel` function.

## `pkg/hub/message.go`

`Message` is one retained message: a body and its timestamp. `Clone` returns a
deep copy of the body so a replayed or fanned-out message cannot be mutated by
another subscriber.

## `pkg/hub/publish.go`

`Result` reports, per channel name, a `Status`: whether a publish was enqueued
(`Enqueued`), how many subscribers had to drop it (`Dropped`), and, when
something was lost, a `Reason` (`oversize`, `slow subscriber`, `memory`,
`closed`, `unavailable`). `Status.OK` is true only when the channel took the
message with no drops, `Result.OK` when that holds for every channel, and
`Result.Enqueued`/`Result.Dropped` read one channel or the totals. `String`
returns the empty string so a bare `publish` inside a template emits nothing,
and `Status.String` returns `true`/`false` for templates that index the result.

## `pkg/hub/hub.go`

`Hub` owns the channel registry and the global limits.

- `DefaultMessages` is 100.
- `NewHub` sets `MaxChannels` to 1000 when unset and fills every rule's
  `Messages` with `DefaultMessages` when unset.
- `SetLogger` installs a `Logger` sink. The interface mirrors zap's
  `SugaredLogger.Warnw`; a nil logger disables reporting.
- `Start` is a no-op. Each channel actor runs its own idle ticker and removes
  itself from the hub when it closes.
- `Stop` marks the hub stopped, snapshots the registry under the lock, clears
  the registry and shuts each channel down outside the lock, then resets the
  retained-byte and participant counters.
- `acquireParticipant` and `releaseParticipant` maintain the global
  `MaxParticipants` cap with atomic compare-and-swap. The counter stays zero
  when the cap is unset.
- `Participants`, `Retained` and `Dropped` expose the counters. `Dropped` sums
the live channels' drop counters.
- `addRetained` adjusts the memory budget and ignores updates after `Stop`.
- `reserve(n)` claims `n` bytes of the budget, adding them to `reserved` and
  checking `retained+reserved` under a compare-and-swap loop, so concurrent
  publishes cannot overshoot. When over budget it asks each channel to evict
  itself if idle and retries. `unreserve(n)` gives the claim back once the
  channel actor has moved the message into `retained` (or kept none of it).
- `getOrCreate` returns an existing channel or creates one when a rule matches
  and `MaxChannels` is not reached. A name with no matching rule is an error.
- `matchRule` returns the first matching rule; `Rule` is its lock-free public
  form.
- `Subscribe` gets or creates the channel and forwards to the channel actor.
- `Publish` walks the requested patterns. A wildcard applies to existing
  channels only; a concrete name is created on demand. Before publishing it
  reserves `len(body)` per target before publishing to it and reports
  `ReasonMemory` when the budget cannot be met, `ReasonUnavailable` when the
  channel cannot be created (releasing that reservation). Each target channel
  returns its own `Status`.
- `patternMatch` treats a trailing `*` as a prefix match and everything else as
  a literal; `isWildcard` tests for that suffix. `PatternWithin(inner, outer)`
  reports whether every name `inner` matches is also matched by `outer`, which
  is how transports prove a publish target stays inside the subscribe
  allow-list.

## `pkg/hub/channel.go`

A channel is an actor: mutable state is touched only by closures sent to its
inbox, which are executed by one `run` goroutine.

- `DefaultQueueSize` is 64; `dropLogInterval` is 10 seconds.
- `participant` holds a subscriber ID, its `send` callback, a buffered queue
  and a `failed` atomic flag. `enqueue` never blocks; a full queue drops the
  newest message.
- `Channel` stores the name, resolved rule and hub, an inbox and a done
  channel, the ring, the participant map, the next subscriber ID, idle
  timestamps and the drop counter. The ID starts at 1 because `0` means "skip
  nobody" in a publish. `releaseOnce` makes the memory release idempotent;
  `mu` guards `dead`, which `shutdown` may set from any goroutine.
- `shutdown` marks the channel dead and closes `done`, once.
- `Dropped` exposes the drop counter.
- `retainedBytes` sums the ring; `releaseMemory` subtracts that total from the
  hub budget once.
- `evictIdle` sends a closure that closes the channel only when it has no
  participants, then removes it from the registry. It handles a channel that
  closed before replying.
- `run` selects over the inbox, a one-second ticker and `done`. On each tick it
  calls `shouldClose`; when that is true it releases memory, shuts down and
  removes itself from the hub.
- `subscribe` sends a closure that enforces the per-channel and global
  participant caps, allocates the next ID, creates the participant, starts one
  writer goroutine with a replay snapshot and returns a `Subscription` whose
  `Cancel` enqueues `removeParticipant`.
- `writer` is the only goroutine that calls `send` for a participant. It sends
  the replay first, then drains the queue until the queue closes, the channel
  closes or `send` reports failure. Failure goes through `failParticipant`,
  which sets `failed` and enqueues `removeParticipant`, so a dead subscriber
  loses its slot at once instead of waiting for the next publish or the sweep.
  `failed` remains as a hint for a publish racing that removal, and
  `removeParticipant` is idempotent, so the two paths cannot double-close.
- `removeParticipant` deletes the entry, releases the global slot, closes the
  queue so the writer exits, and records the idle timestamp when the channel
  becomes empty.
- `buildReplay` returns the retained bodies, limited by `Messages` and by
  `MaxSize` (default 256 KiB) when that field is unset.
- `publish` enqueues `doPublish` on the actor and waits for its reply, so the
  returned `Status` describes this message. A channel that closes first reports
  `ReasonClosed`. `doPublish` drops an oversize body and reports it with
  `ReasonOversize`, appends a clone to the ring, trims the ring to `Messages`
  and updates the memory budget, then fans out non-blocking to every
  participant except `exclude`. Failed participants are removed; each full
  queue counts as a drop and is returned in `Status.Dropped` with
  `ReasonSlowSubscriber`.
- `reportDrop` logs at most once per channel per `dropLogInterval`.
- `shouldClose` returns false for a dead channel or one with participants. It
  closes immediately when the ring is empty and `IdleTTL` has elapsed since the
  channel last became empty, or when `Retain` has elapsed since the last
  message. With both limits unset, a channel closes as soon as it is empty.

## `pkg/hub/subscribe.go`

Defines the subscription errors: per-channel participant cap reached, global
`max_participants` reached, `max_participants_per_ip` reached, and channel
closed.

## `pkg/hub/net.go`

`ClientIP` turns a `host:port` client address into its host, which is what
per-IP limits key on. An address without a port is returned unchanged and an
empty address stays empty, meaning the client is unknown and no per-IP cap
applies. Forwarding headers are deliberately not consulted: the address is the
connection's peer.

## `pkg/stun/stun.go`

`Listen(network, addr)` resolves a UDP address, binds it and starts a serving
goroutine, returning the connection as an `io.Closer`.

The server reads datagrams into a 1500-byte buffer, decodes STUN messages,
ignores anything that is not a binding request, and replies with a binding
success carrying the sender's XOR-mapped address and a fingerprint. The reply
reuses the request's transaction ID via `stun.NewTransactionIDSetter`.
Malformed requests and build failures are silently skipped.

## `pkg/turn/turn.go`

`Config` describes a TURN listener: listen address, realm, external IP, bind
address, optional relay port range, the REST authentication secret, the TLS
certificate source (files or a `GetCertificate` func), and the two abuse
limits (`MaxAllocations`, `AuthRatePerIP`).

- `Listen` requires `AuthSecret`, binds a UDP socket, validates
  `ExternalIP`, and creates a pion TURN server with
  `LongTermTURNRESTAuthHandler`. There is no anonymous access. It returns the
  server as an `io.Closer`.
- `ListenTLS` requires a certificate source: it loads `TLSCertFile` and
  `TLSKeyFile` when set, otherwise it installs `GetCertificate`. It binds a
  TLS TCP listener with a minimum of TLS 1.2 and creates the same server with
  a listener config.
- `newRelayGenerator` binds relays to `BindAddr` (default `0.0.0.0`) but
  advertises `ExternalIP`. It uses `RelayAddressGeneratorPortRange` when both
  relay port bounds are set and valid, otherwise the static generator. With
  `MaxAllocations` set the generator is wrapped in `cappedGenerator`.
- `cappedGenerator` bounds live relays. pion asks the generator for one socket
  per allocation and closes it when the allocation is released (or when an
  even-port reservation is abandoned), so the wrapper counts sockets; past the
  cap it returns `errAllocationCap`, which pion reports to the client. The
  returned socket releases its slot once, on `Close`.
- `newAuthHandler` wraps the REST auth handler in `ipLimiters` when
  `AuthRatePerIP` is set: one token bucket per client host, created on first
  use and dropped after ten idle minutes. A refused token fails
  authentication. pion authenticates every request and offers no
  allocation-level or username-aware hook in v4.0.0, so this bounds churn per
  source rather than allocations per user.
- `hostOf` reduces a client `net.Addr` to its host, so every port of one host
  shares a limiter.
- `splitNetworkAddr` splits `udp/:3478`, `udp/1.2.3.4:3478` and `:3478` into a
  network and address, defaulting the network to `udp`.

## `pkg/ws/ws.go`

`Config` holds the provisioned subscribe pattern, optional intercept and
publish pattern, and the allowed origin patterns.

`Serve` accepts the WebSocket handshake, applies the origin patterns, and takes
the channel name from the last path segment. `upgradeWriter` follows the
response writer's `Unwrap` chain to the first writer that implements
`http.Hijacker`, because Caddy's wrappers expose hijacking only that way while
`coder/websocket` asserts the interface directly. It rejects a channel outside
the subscribe pattern, a missing publish target, and a publish target outside
the subscribe pattern with a policy violation.

The send callback writes text messages with a 10-second timeout and returns
false once the connection has been marked closed. The handler subscribes, then
runs a read loop:

- only text messages are considered;
- a `rate.Limiter` with burst 1 enforces the channel's `RateLimit` (or one
  message per second when unset); a client that stays over the limit for more
  than `rateLimitBurst` (16) messages is closed with a policy violation;
- each message is passed through the intercept; dropped or empty results are
  skipped;
- the result is published to the configured publish pattern, excluding the
  sender's own subscription ID.

`rateLimit` resolves the rule through `Bus.Rule`, and `patternMatch` uses the
same trailing-`*` prefix rule as the hub.

## `pkg/sse/sse.go`

`Config` holds the subscribe pattern, optional intercept and publish pattern.

`Serve` dispatches `GET` to `serveGet`, `POST` to `servePost`, and returns 405
for other methods.

`serveGet` verifies that the writer can flush by following its `Unwrap` chain
(covering Caddy's wrappers, which expose `FlushError` rather than
`http.Flusher`), validates the channel against the subscribe pattern before
writing any status, and creates a 64-slot handoff channel. Its send callback
blocks on that channel or gives up when the request context ends. The handler
subscribes *before* writing the 200 status, so a client that sees the response
cannot miss a later publish and a subscription failure is a clean error status.
Only the handler goroutine writes the `http.ResponseWriter`: it sends
`data: <body>` frames and flushes them through `http.ResponseController`, and a
15-second ticker sends `: keepalive` comments.

`servePost` reads at most 1 MiB, validates the channel, requires a publish
target that stays within the subscribe pattern (`hub.PatternWithin`; otherwise
500), runs the intercept (dropping with 502 when it returns false or an empty
body), publishes without excluding anyone, and returns 202.

## `pkg/app/relay.go`

The root `relay.go` is a shell: it only blank-imports this package so Caddy
runs its `init`. Everything below lives in `pkg/app`.

`init` registers the `Relay`, `RelayWS` and `RelaySSE` modules, registers the
`relay_ws` and `relay_sse` handler directives, registers the global `relay`
option, and orders both directives before `file_server`.

### Global option and parsing

`unmarshalRelayOption` parses the block into a `Relay` and returns
`httpcaddyfile.App{Name: "relay"}` with its JSON form. Caddy therefore records
the configuration under `apps.relay`, which makes the app startable from JSON,
visible to the admin API, and started even with no site directive.

`parseRelayBlock` accepts the global sub-directives listed above and rejects
unknown ones. `off` clears the corresponding listener address.
`parseChannelsBlock` handles the global limits and treats every other token as
a channel pattern. `parseChannelRule` reads the per-rule fields and rejects
unknown sub-directives.

### The `Relay` app

`Relay` is the Caddy app. Its exported fields are the JSON configuration:
STUN/TURN listen addresses, the TURN secret and TLS paths, relay port bounds,
external IP, bind address and `Channels`. Its unexported fields hold the hub,
the listener closers and the logger.

- The module ID is `relay`.
- `Provision` creates the hub from `Channels` and installs a zap-backed
  `hub.Logger`. With `turn_tls_domain` and no certificate files it fetches the
  `tls` app, asks it to manage the name, and installs a `GetCertificate` func
  that serves the certificate from `caddytls.AllMatchingCertificates`.
- `Validate` is a no-op when TURN is disabled; otherwise it requires
  `turn_secret` and `turn_external_ip`, and requires either a certificate pair
  or `turn_tls_domain` for `turn_tls`.
- `turnConfig` maps the app fields to `turn.Config` with the fixed realm
  `caddy-relay`, including the certificate source.
- `Start` starts the hub, then the STUN listener, the UDP TURN listener and the
  TLS TURN listener, in that order. A failure aborts startup.
- `Stop` closes the listeners and then the hub.
- `hubFor` fetches the app with `ctx.App("relay")`. With no global option the
  app is created on demand with an empty config, so no rule matches and
  subscriptions fail.

### Bus implementation and intercepts

The `Relay` methods delegate `Subscribe`, `Publish` and `Rule` to the hub.
`Intercept` passes the body through when the spec is zero, uses the URL mode
when set, and otherwise runs the template mode.

URL intercepts share one `http.Client` with a 5-second timeout.
`resolveInterceptURL` leaves absolute URLs unchanged, adds the request scheme
to a scheme-relative URL, and resolves a relative path against the receiving
request's scheme and host, preserving the query. The request posts the body as
JSON and accepts a 2xx response up to 1 MiB; any error, non-2xx status or empty
response drops the message.

Template intercepts parse the body as a JSON object into a map (or use an
empty map), build the function set, clone the compiled template and execute it
with no data. The available functions are `message`, `data`, `channel`,
`remote_ip`, `request`, `dict`, `set` and `publish`. `publish` takes one or
more pattern arguments followed by a value, marshals the value as JSON and
publishes with no exclusion. The result is trimmed of surrounding whitespace;
an execution error is logged and drops the message. `resolve` unwraps
zero-argument function values so template arguments can be passed either way,
and `dict`/`set` build and update maps.

### Site handler modules

`RelayWS` holds the raw subscribe, publish, intercept and origin strings plus
the provisioned `ws.Config`, the app and a logger. Its module ID is
`http.handlers.relay_ws`. `Provision` requires both subscribe and publish,
rejects a publish target outside subscribe, resolves the app, compiles the
intercept and fills the config. `ServeHTTP` delegates to `ws.Serve`.

`RelaySSE` holds the raw subscribe, publish and intercept strings plus the
provisioned `sse.Config`, the app and a logger. Its module ID is
`http.handlers.relay_sse`. `Provision` requires subscribe, rejects a publish
target outside subscribe, resolves the app, compiles the intercept and fills
the config. `ServeHTTP` delegates to `sse.Serve`.

### Caddyfile parsing for the directives

`parseCaddyfileRelayWS` and `parseCaddyfileRelaySSE` delegate to the modules'
`UnmarshalCaddyfile`. Both reject top-level arguments and accept `subscribe`,
`publish` and `intercept`; `relay_ws` also accepts one or more `origins` and
rejects unknown sub-directives.

`parseInterceptBlock` returns an encoded `url:<path>` for the inline form. For
a block, it accepts a `template` sub-directive with either a heredoc argument
(kept verbatim) or a nested block. A nested block is rebuilt by joining tokens
on the same Caddyfile line with one space and breaking on a new line, so a run
of spaces survives only inside a quoted token. An empty
template is an error.

`compileIntercept` decodes the URL form directly. For the template form it
registers stub implementations of every template function, parses with
`missingkey=zero`, and stores both the source and the compiled template in the
`InterceptSpec`.

### Helpers and guards

`atoi` is a plain integer parse. `parseBytes` accepts a `K`, `M` or `G` suffix
(powers of 1024) case-insensitively and rejects a value that would overflow
`int64` once the suffix is applied. `parsePortRange` requires `LO-HI`.
`stripNetworkPrefix` removes a leading `network/` from an address.

Interface guards assert that `Relay` is a Caddy app and that both directives
are middleware handlers.

## Cross-cutting invariants

- Subscriber IDs start at 1; a publish exclusion of 0 skips nobody.
- Each subscriber has exactly one writer goroutine. `send` is never called on
  the channel actor or the transport's read loop. The SSE handler therefore
  hands messages to its own loop instead of writing from the hub goroutine.
- The ring is always bounded by `Messages`; `MaxSize` caps both retained
  messages and replay bytes, and drops oversize publishes.
- Wildcard publishes reach only existing channels; a concrete name creates one
  if a rule allows it. A publish fails when `max_memory` cannot be met after
  evicting idle channels.
- Channels close themselves when idle according to `IdleTTL`/`Retain`, and the
  hub never runs a separate sweeper.
- TURN requires a secret and an external IP; TLS additionally requires either a
  certificate pair or a `turn_tls_domain` the `tls` app manages. Relays bind
  `turn_bind` and advertise `turn_external_ip`.
- Relative intercept URLs resolve against the server that received the
  message; absolute URLs are used unchanged.
- Directive publishes exclude the sender; template and SSE publishes do not.
- The WebSocket rate limit comes from the channel rule and closes a client
  that stays over it.

## Verification

| Check | Command |
|-------|---------|
| Build | `go build ./...` |
| Vet | `go vet ./...` |
| Tests | `go test -race ./...` |
| End to end against the built image | `./build.sh && ./test/e2e.sh` |

All of these run in CI (`.github/workflows/ci.yml`), which also builds the
image. The end-to-end suite is a bash script that orchestrates the container
and checks it with `curl`, `openssl` and a raw STUN datagram; nothing has been
exercised from a real browser or behind a load balancer.

## Open items

Known gaps are tracked in local, untracked working notes rather than in the
repository, so this document describes only what is implemented.
