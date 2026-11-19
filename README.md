# caddy-relay

Real-time messaging relay for Caddy. Adds WebSocket and Server-Sent
Events pub/sub with per-channel limits, plus STUN/TURN listeners for
WebRTC NAT traversal. Ships as a single static binary (`FROM scratch`).

App module ID: `relay` (Caddy's JSON app key is also `relay`). Source:
`github.com/frinknet/caddy-relay`. Handlers: `http.handlers.relay_ws`,
`http.handlers.relay_sse`.

## Build

    sh test/unit.sh                # gofmt, vet, unit and transport tests
    ./build.sh                     # the only supported way to build the image
    ./test/e2e.sh                 # runs the image and drives it over published ports
    docker run --rm -p 80:80 -p 443:443 -p 3478:3478/udp caddy-relay:test

`build.sh` is the supported build entry point: it derives the image name from
the repo directory (here `caddy-relay:test`) and refreshes any old image, so
`docker build` is not needed and not what the tests expect. `--init` is for
scaffolding a brand-new plugin, not for building this one.

The image runs the `Caddyfile` in `/srv/www`, its working directory (mount a
replacement over `/srv/www/Caddyfile` to change the config), and its
`HEALTHCHECK` polls the admin endpoint on the default `localhost:2019`, so a
config that sets `admin off` or moves it will report unhealthy.

## How a request maps to a channel

Both transports take the channel name from the **last path segment**:

| Request | Channel |
|---------|---------|
| `GET /ws/room-1` (upgrade) | `room-1` |
| `GET /events/lobby` | `lobby` |
| `POST /events/lobby` | `lobby` |

A request is only served if its channel both matches the endpoint's
`subscribe` pattern and matches a rule in `channels {}`; anything else is
rejected.

## Global option: `relay { ... }`

    {
        relay {
            stun udp/:3478
            turn udp/:5349
            turn_secret a-long-random-shared-secret
            turn_tls :5349
            # Certificate from Caddy's own TLS automation ...
            turn_tls_domain relay.example.com
            # ... or from a certificate and key file:
            # turn_tls_cert /etc/caddy/turn.crt
            # turn_tls_key  /etc/caddy/turn.key
            turn_external_ip 203.0.113.1
            turn_bind 0.0.0.0
            turn_relay_ports 49152-65535
            turn_max_allocations 1024
            turn_auth_rate 50

            channels {
                max_channels     2000
                max_participants 10000
                max_participants_per_ip 50
                max_memory       256M

                room-* {
                    messages     50
                    participants 100
                    max_size     64K
                    rate_limit   100ms
                    idle_ttl     5m
                    retain       10m
                }

                lobby {
                    messages   10
                    idle_ttl   30s
                }
            }
        }
    }

| Sub-directive | Meaning |
|---|---|
| `stun <addr>` | UDP listen address for the STUN binding server (`off` to disable) |
| `turn <addr>` | UDP listen address for the TURN relay (`off` to disable) |
| `turn_secret <secret>` | Shared secret for TURN REST credentials. **Required** whenever `turn` or `turn_tls` is set |
| `turn_tls <addr>` | TLS (TCP) listen address for `turns:`, e.g. `:5349` |
| `turn_tls_domain <name>` | Serve `turn_tls` with the certificate Caddy manages for `<name>` (requires Caddy's `tls` app, e.g. a site using `tls`) |
| `turn_tls_cert`, `turn_tls_key` | Certificate and key files for `turn_tls`; used instead of `turn_tls_domain` when both are set |
| `turn_external_ip <ip>` | Public IP advertised in TURN relay addresses. Required for `turn`/`turn_tls` |
| `turn_bind <addr>` | Local address relays bind to (default `0.0.0.0`). Separate from the advertised IP so containers and NAT work |
| `turn_relay_ports LO-HI` | Port range TURN allocates relays from (default: any free port) |
| `turn_max_allocations N` | Cap on concurrent TURN relays (default: unlimited). Each relay can carry any amount of traffic, so this is the TURN connection limit |
| `turn_auth_rate N` | Cap on authenticated TURN requests per second from one client IP (default: unlimited). Bounds how fast a source can create allocations, permissions and channel bindings |

### `channels { ... }`

`max_channels` (default 1000) caps live channel count, `max_participants`
caps subscribers across all channels, `max_participants_per_ip` caps them per
client address, and `max_memory` caps total retained bytes (all three default
to unlimited) — see *Limits* below. Every
other entry is a **pattern** with wildcard suffix `*` only — `room-*` matches
`room-1`, not `other-room`. The first matching rule wins.

| Channel field | Meaning |
|---|---|
| `messages` | Ring-buffer depth kept for replay. Default 100; the ring is always capped |
| `participants` | Max concurrent subscribers on this channel |
| `max_size` | Max bytes per message; larger messages are dropped |
| `rate_limit` | Minimum interval between messages a subscriber may publish (default 1s) |
| `idle_ttl` | Close the channel after this long with no participants |
| `retain` | Keep the ring this long after the last message |

Sizes accept `K`/`M`/`G` suffixes (multiples of 1024). Durations are Go
syntax (`100ms`, `5m`). Numbers must be plain integers.

### Limits

- **`max_memory`** is a soft cap on retained bytes. A publish that would
  exceed it first closes channels that have no participants; if that is not
  enough the message is dropped (and reported, see *Dropped messages*).
- **`max_participants`** is exact: once it is reached, further subscriptions
  are rejected with `participants cap reached`.
- **`max_participants_per_ip`** is exact per client host: a subscription over
  the limit for that address is rejected with `max_participants_per_ip
  reached`, while other addresses are unaffected. The address comes from the
  connection, not from a header, so a proxy in front has to be trusted.
- **`rate_limit`** is per connection and comes from the channel's resolved
  rule. A client that sends more than 16 messages while over its limit is
  disconnected with a policy-violation close code.
- One WebSocket or SSE request carries exactly one subscription.

## Site directives

    example.com {
        @ws {
            header Connection "Upgrade"
            header Upgrade    "websocket"
        }
        @sse path /events*

        handle @ws {
            relay_ws {
                subscribe room-*
                publish   room-*
                intercept /api/filter
            }
        }

        handle @sse {
            relay_sse {
                subscribe room-*
                publish   room-*
                intercept {
                    template {
                        {{ message | publish channel }}
                    }
                }
            }
        }

        file_server
    }

| Sub-directive | Meaning |
|---|---|
| `subscribe <pattern>` | Channels this endpoint may address, for reading **and** writing. **Required**; without it Caddy refuses to provision |
| `publish <pattern>` | Target pattern for messages sent from this endpoint. It must stay within `subscribe`; **required** for `relay_ws`, optional for a read-only SSE `GET` |
| `intercept /url` | POST each message to an endpoint and use its response. A path is resolved against the server that received the message |
| `intercept { template { ... } }` | Transform the message with a Go template (or a `<<TAG` heredoc) |
| `origins <pattern>...` | Extra allowed WebSocket `Origin` hosts (`relay_ws` only). A pattern without a port matches that host on any port; a pattern with a port matches that port only. `*` matches any host |

A `template { ... }` block is rebuilt line by line: tokens on one Caddyfile
line are joined with a single space, so a run of spaces survives only inside a
quoted token. Use a heredoc (`template <<TAG ... TAG`) when the template's
literal spacing matters.

The channel in the request path is checked against `subscribe`, and so is the
publish target: `subscribe` is the endpoint's allow-list for both directions,
so `publish` must be a subset of it (`publish room-1` under `subscribe room-*`
is fine; `publish lobby` under `subscribe room-*` is refused at provision
time). Otherwise a client could write into channels it may not even read.

**WebSocket** (`relay_ws`): the connection subscribes to the path channel,
receives any buffered replay, then publishes each inbound text frame.
Binary frames are ignored. The sender is excluded from its own fan-out.
`Origin` is verified against `Host` unless `origins` lists the origin's host.
Patterns follow `filepath.Match` (`*.example.com` matches subdomains) and are
case-insensitive. A pattern that names no port admits the host on every port,
so `origins example.com` accepts `https://example.com:8443`; a pattern such as
`example.com:8443` pins the port.

**SSE** (`relay_sse`): `GET` opens a long-lived `text/event-stream` and
replays the buffer; `POST` publishes the request body to the `publish` target
and returns `202`. A `POST` whose target falls outside `subscribe` is refused
with `500`. A `: keepalive` comment is sent every 15s. Other methods get `405`.
The handler subscribes before it writes the stream headers, so once a client
has the response it cannot miss a later publish.

### Dropped messages

Every subscriber has its own outbound queue. A subscriber that stops reading
never stalls the channel: once its queue is full its newest messages are
dropped, counted, and reported to Caddy's log (`relay: dropped messages`,
with the channel and reason) at most once per channel per 10s. A subscriber
whose connection fails is removed from the channel.

The publish result carries the same information per channel: `Enqueued`,
the number of subscribers that had to drop this message (`Dropped`), and a
`Reason` (`oversize`, `slow subscriber`, `memory`, `closed`, `unavailable`).
A template can react to it, for example
`{{ if not (index (publish channel message) channel).OK }}...{{ end }}`.

## Intercept

**URL mode** — the raw body is POSTed as `application/json` with a 5s
timeout and a 1 MiB response cap. Any non-2xx, transport error, or empty
body drops the message.

**Template mode** — renders to the new body; the result is trimmed. An
empty result drops the message, so `{{ message | publish channel }}` (as in
`www/Caddyfile`) delivers by calling `publish` itself and renders nothing.
The exclusion differs between the two paths: the directive's own publish skips
the sender, a `publish` from a template includes them.

| Func | Returns |
|---|---|
| `message` | Raw message body as a string |
| `data` | Parsed JSON body, or an empty map |
| `channel` | Channel name |
| `remote_ip` | Subscriber's client host, without the port |
| `request` | The original `*http.Request` |
| `dict k v ...` | Map built from key/value pairs |
| `set m k v` | Map with `k` set to `v` |
| `publish pats... v` | Publish `v` to channels; renders to nothing |

## STUN / TURN

`stun` runs a minimal UDP binding server that answers binding requests with
the client's XOR-mapped address.

`turn` wraps pion/turn. Both `turn` and `turn_tls` **require `turn_secret`**,
and there is no anonymous access: clients use TURN REST credentials, where the
username is `<expiry-unix>:<name>` and the password is
`base64(HMAC-SHA1(turn_secret, username))`. Issue those from your own
application so the secret never reaches the browser:

    expiry := time.Now().Add(10 * time.Minute).Unix()
    username := fmt.Sprintf("%d:alice", expiry)
    mac := hmac.New(sha1.New, []byte(secret))
    mac.Write([]byte(username))
    password := base64.StdEncoding.EncodeToString(mac.Sum(nil))

`turn_external_ip` must be the publicly routable IP (it is advertised in relay
addresses), and `turn_relay_ports` confines allocations to a range you can
open in the firewall.

`turn_tls` listens with TLS over TCP. `turn_tls_domain` serves the certificate
Caddy already manages for that name: `Provision` asks Caddy's `tls` app to
manage it, which must therefore be in the config (a site using `tls`, or a
`tls { ... }` global block with a matching automation policy), and handshakes
use the certificate from Caddy's certmagic cache. Issuance is asynchronous, so
the first handshakes can fail until the certificate lands.
`turn_tls_cert`/`turn_tls_key` remain available and take precedence when both
are set, for deployments that manage the files elsewhere.

## Clients

```js
// WebSocket: one connection per channel path segment
const ws = new WebSocket("wss://example.com/ws/room-1")
ws.onmessage = (e) => console.log("message", e.data)
ws.send(JSON.stringify({ text: "hello" }))

// SSE: EventSource cannot POST, so publish with fetch
const es = new EventSource("https://example.com/events/room-1")
es.onmessage = (e) => console.log("message", e.data)
fetch("https://example.com/events/room-1", { method: "POST", body: "hello" })
```

## Docs and status

`sh test/unit.sh` covers the hub, both transports, STUN, TURN and the
Caddyfile adapter. `test/e2e.sh` is the end-to-end suite: it renders
`test/Caddyfile`, runs the built image with Docker, waits for the image's
own healthcheck, and then drives the container over published ports (HTTP and
HTTPS sites, SSE publish/intercept/replay, the channel allow-list, the
WebSocket handshake and origin policy, a STUN binding request, and the
TURN-over-TLS listener). It needs Docker and the image:

    ./build.sh && ./test/e2e.sh

CI runs all of it, including the image build. The plugin has not been run
against real browsers or behind a load balancer.

The global `relay` option registers the app as `apps.relay`, so the hub and
the STUN/TURN listeners start even in a config with no `relay_ws`/`relay_sse`
directive, and the same configuration can be supplied as JSON. The image
exposes 80, 443, 443/udp, 3478/udp (STUN) and 5349/udp + 5349/tcp (TURN), the
ports `www/Caddyfile` uses when its commented-out TURN block is enabled. TURN
is off there by default, and the relay port range must be published by the
caller.

- `spec.md` — file-by-file implementation notes, maintained by hand
