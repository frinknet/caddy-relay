# caddy-relay

Real-time messaging relay for Caddy. Adds WebSocket and Server-Sent
Events pub/sub with per-channel limits, plus STUN/TURN listeners for
WebRTC NAT traversal. Ships as a single static binary (`FROM scratch`).

Module ID: `caddy.app.relay` — `github.com/frinknet/caddy-relay`

## Build

    go vet ./...
    ./build.sh                 # docker build -t caddy-relay:test .
    docker run --rm -p 80:80 -p 443:443 -p 3478:3478/udp caddy-relay:test

`build.sh` derives the image name from the repo directory. `--init` is for
scaffolding a brand-new plugin, not for building this one.

## How a request maps to a channel

Both transports take the channel name from the **last path segment**:

| Request | Channel |
|---------|---------|
| `GET /ws/room-1` (upgrade) | `room-1` |
| `GET /events/lobby` | `lobby` |
| `POST /events/lobby` | `lobby` |

A channel only exists if some rule in `channels {}` matches it. Unmatched
names are rejected.

## Global option: `relay { ... }`

    {
        relay {
            stun udp/:3478
            turn udp/:3478
            turn_tls udp/:5349
            turn_external_ip 203.0.113.1
            turn_relay_ports 49152-65535

            channels {
                max_channels 2000
                max_memory   256M

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
| `turn_tls <addr>` | Reserved; parsed but not yet listened on |
| `turn_external_ip <ip>` | Public IP advertised in TURN relay addresses. Required for `turn` |
| `turn_relay_ports LO-HI` | Reserved; parsed but not yet applied |

### `channels { ... }`

`max_channels` (default 1000) caps live channel count. `max_memory` (0 =
unlimited) caps total retained bytes. Every other entry is a **pattern**
with wildcard suffix `*` only — `room-*` matches `room-1`, not `other-room`.
The first matching rule wins.

| Channel field | Meaning |
|---|---|
| `messages` | Ring-buffer depth kept for replay |
| `participants` | Max concurrent subscribers |
| `max_size` | Max bytes per message |
| `rate_limit` | Per-connection publish interval |
| `idle_ttl` | Close the channel after this long with no participants |
| `retain` | Keep the ring this long after the last message |

Sizes accept `K`/`M`/`G` suffixes. Durations are Go syntax (`100ms`, `5m`).

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
| `subscribe <pattern>` | Pattern this endpoint may join. Without it any channel that matches a rule is allowed |
| `publish <pattern>` | Target pattern for messages sent from this endpoint. **Required** for `relay_ws` and for SSE `POST` |
| `intercept /url` | POST each message to an external endpoint, use its response |
| `intercept { template { ... } }` | Transform the message with a Go template |

**WebSocket** (`relay_ws`): the connection subscribes to the path channel,
receives any buffered replay, then publishes each inbound text frame.
Binary frames are ignored. The sender is excluded from its own fan-out.

**SSE** (`relay_sse`): `GET` opens a long-lived `text/event-stream` and
replays the buffer; `POST` publishes the request body and returns `202`.
A `: keepalive` comment is sent every 15s. Other methods get `405`.

## Intercept

**URL mode** — the raw body is POSTed as `application/json` with a 5s
timeout and a 1 MiB response cap. Any non-2xx, transport error, or empty
body drops the message.

**Template mode** — renders to the new body; the result is trimmed. An
empty result drops the message.

| Func | Returns |
|---|---|
| `message` | Raw message body as a string |
| `data` | Parsed JSON body, or an empty map |
| `channel` | Channel name |
| `remote_ip` | Subscriber's remote address |
| `request` | The original `*http.Request` |
| `dict k v ...` | Map built from key/value pairs |
| `set m k v` | Map with `k` set to `v` |
| `publish pats... v` | Publish `v` to channels; renders to nothing |

## STUN / TURN

`stun` runs a minimal UDP binding server. `turn` wraps pion/turn in static
relay-address mode; `turn_external_ip` must be the publicly routable IP or
`Start` fails. No TURN credentials are configured — the auth handler
accepts any username. Do not expose the TURN port beyond a trusted network
until REST/HMAC credentials are wired in.

## Status

`go vet` passes. No tests yet. Known gaps: channel `rate_limit` is parsed
but the WebSocket transport uses a fixed 1s limiter (`pkg/ws/ws.go:95`);
`turn_tls` and `turn_relay_ports` are parsed but unused; TURN auth is
open. See `AGENTS.md` for the full architecture and `plan.md` for the
design notes.
