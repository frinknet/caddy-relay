#!/usr/bin/env bash
#
# caddy-relay container end-to-end tests.
#
# This is the only end-to-end suite: it orchestrates the image built by
# ./build.sh and drives it the way a client would, so nothing here links the
# plugin or runs Caddy in-process.
#
# It renders test/Caddyfile into a temp directory, publishes the internal
# ports from a random block (host and container ports stay identical), waits
# on the image's own healthcheck, and then checks:
#
#   - the image healthcheck (config loaded, admin endpoint up)
#   - the HTTP and HTTPS sites
#   - SSE: live publish, URL intercept, and replay for a late subscriber
#   - a channel outside the subscribe pattern (403 on SSE and WebSocket)
#   - the WebSocket handshake, including the origin policy
#   - a STUN binding request over UDP
#   - the TURN-over-TLS listener and its certificate
#   - WebSocket frame exchange, a load test and a TURN allocation, driven by
#     the Go client in test/client
#   - a real browser driving WebSocket and EventSource, with
#     CADDY_RELAY_BROWSER=1
#
# The Go client needs a protocol stack, so it runs in the builder image while
# sharing the fixture container's network namespace. The unit layer skips it
# because the CADDY_RELAY_E2E_* variables are unset there.
#
# Usage:
#   ./test/e2e.sh
#   CADDY_RELAY_IMAGE=caddy-relay:my-tag ./test/e2e.sh
#   CADDY_RELAY_BROWSER=1 ./test/e2e.sh   # add the Chromium check
#   KEEP=1 ./test/e2e.sh     # leave the container running for inspection
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

image=${CADDY_RELAY_IMAGE:-caddy-relay:test}
builder=${CADDY_RELAY_BUILDER_IMAGE:-caddy:2.11.9-builder-alpine}
name=caddy-relay-e2e-$$
tmp=$(mktemp -d)
container=
turn_secret=e2e-shared-secret

# The URL intercept posts back to the request's own Host, so the host and
# container ports have to be the same. A free block is picked at runtime and a
# collision (docker refuses to publish a bound port) makes the start retried.
relay_min=49160
relay_max=49175
http_port=
https_port=
stun_port=
turn_port=
turn_tls_port=

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

pass() {
  printf 'ok   %s\n' "$*"
}

cleanup() {
  status=$?
  if [ -n "$container" ]; then
    if [ "$status" -ne 0 ]; then
      printf -- '--- container logs ---\n' >&2
      docker logs "$container" >&2 || true
    fi
    if [ "${KEEP:-0}" = 1 ]; then
      printf 'kept container %s\n' "$container"
    else
      docker rm -f "$container" >/dev/null 2>&1 || true
    fi
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT

need() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 is required"
}

need docker
need openssl
need od
need curl

docker image inspect "$image" >/dev/null 2>&1 ||
  fail "image $image is missing; run ./build.sh or set CADDY_RELAY_IMAGE"

# render_config writes the container config with the ports of the current
# block and the TURN secret. Only the placeholders this script knows about are
# substituted: the Caddyfile also carries Go template syntax for relay_ws.
render_config() {
  sed \
    -e "s/{{STUN_PORT}}/$stun_port/g" \
    -e "s/{{TURN_PORT}}/$turn_port/g" \
    -e "s/{{TURN_TLS_PORT}}/$turn_tls_port/g" \
    -e "s/{{RELAY_MIN}}/$relay_min/g" \
    -e "s/{{RELAY_MAX}}/$relay_max/g" \
    -e "s/{{HTTP_PORT}}/$http_port/g" \
    -e "s/{{HTTPS_PORT}}/$https_port/g" \
    -e "s/{{TURN_SECRET}}/$turn_secret/g" \
    "$here/Caddyfile" >"$tmp/Caddyfile"
  local placeholder
  for placeholder in STUN_PORT TURN_PORT TURN_TLS_PORT RELAY_MIN RELAY_MAX HTTP_PORT HTTPS_PORT TURN_SECRET; do
    grep -q "{{$placeholder}}" "$tmp/Caddyfile" && fail "placeholder $placeholder was not substituted"
  done
}

# TURN over TLS serves the certificate Caddy manages for localhost (the
# HTTPS site below uses `tls internal`), so no certificate files are mounted.

start_container() {
  local base attempt
  for attempt in $(seq 1 20); do
    base=$(((RANDOM % 20000) + 20000))
    http_port=$base
    https_port=$((base + 1))
    stun_port=$((base + 2))
    turn_port=$((base + 3))
    turn_tls_port=$((base + 4))
    render_config
    if container=$(docker run -d --name "$name" \
      -p "127.0.0.1:${http_port}:${http_port}/tcp" \
      -p "127.0.0.1:${https_port}:${https_port}/tcp" \
      -p "127.0.0.1:${stun_port}:${stun_port}/udp" \
      -p "127.0.0.1:${turn_port}:${turn_port}/udp" \
      -p "127.0.0.1:${turn_tls_port}:${turn_tls_port}/tcp" \
      -v "$tmp/Caddyfile:/srv/www/Caddyfile:ro" \
      -v "$here/browser:/srv/browser:ro" \
      "$image" 2>/dev/null); then
      return 0
    fi
    container=
  done
  return 1
}

docker rm -f "$name" >/dev/null 2>&1 || true
start_container || fail "docker run (could not find a free port block)"

# The image's HEALTHCHECK polls the admin API, so healthy means the config
# loaded and the admin endpoint is up.
health=unknown
for _ in $(seq 1 60); do
  health=$(docker inspect -f '{{.State.Health.Status}}' "$container" 2>/dev/null || echo unknown)
  case $health in
  healthy) break ;;
  unhealthy) fail "the container reported unhealthy" ;;
  esac
  sleep 1
done
[ "$health" = healthy ] || fail "the container did not become healthy (state: $health)"
pass "image healthcheck"

http=$http_port
https=$https_port
stun=$stun_port
turn_tls=$turn_tls_port
[ -n "$http" ] || fail "the HTTP port was not published"
[ -n "$https" ] || fail "the HTTPS port was not published"
base="http://127.0.0.1:$http"

status=$(curl -s -o "$tmp/body" -w '%{http_code}' "$base/")
[ "$status" = 200 ] || fail "GET / returned $status, want 200"
grep -q 'caddy-relay e2e' "$tmp/body" || fail "GET / did not serve the readiness probe"
pass "http site"

status=$(curl -sk -o /dev/null -w '%{http_code}' --resolve "localhost:$https:127.0.0.1" "https://localhost:$https/")
[ "$status" = 200 ] || fail "the HTTPS site returned $status, want 200"
pass "https site over the internal CA"

# SSE: POST publishes, the live GET stream receives the intercept response, and
# a late GET is caught up by the retained message.
sse="$base/events/room-2"
: >"$tmp/stream"
curl -sN --max-time 15 "$sse" >"$tmp/stream" 2>/dev/null &
stream_pid=$!
found=0
for _ in $(seq 1 25); do
  status=$(curl -s -o /dev/null -w '%{http_code}' -X POST --data-binary 'anything' "$sse")
  [ "$status" = 202 ] || fail "SSE POST returned $status, want 202"
  if grep -qF 'data: {"intercepted":true}' "$tmp/stream"; then
    found=1
    break
  fi
  sleep 0.2
done
kill "$stream_pid" 2>/dev/null || true
wait "$stream_pid" 2>/dev/null || true
[ "$found" = 1 ] || fail "the SSE stream never received the URL intercept response"
pass "sse live publish through the URL intercept"

curl -sN --max-time 2 "$sse" >"$tmp/replay" 2>/dev/null || true
grep -qF 'data: {"intercepted":true}' "$tmp/replay" ||
  fail "a late SSE subscriber was not replayed the retained message"
pass "sse replay"

status=$(curl -s -o /dev/null -w '%{http_code}' "$base/events/other-1")
[ "$status" = 403 ] || fail "SSE on a rejected channel returned $status, want 403"
pass "sse rejected channel"

# WebSocket: a handshake from the same origin is upgraded, a foreign Origin is
# refused. Frame exchange needs a real client and lives in pkg/ws.
ws_headers=(
  -H 'Connection: Upgrade'
  -H 'Upgrade: websocket'
  -H 'Sec-WebSocket-Version: 13'
  -H 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ=='
)
curl -s -o /dev/null --max-time 3 -w '%{http_code}' \
  "${ws_headers[@]}" -H "Origin: $base" "$base/ws/room-1" >"$tmp/ws_code" || true
status=$(cat "$tmp/ws_code")
[ "$status" = 101 ] || fail "the WebSocket handshake returned $status, want 101"
pass "websocket handshake"

# The fixture allowlists the port-less host 127.0.0.1, which must admit an
# Origin carrying another port instead of demanding an exact match.
curl -s -o /dev/null --max-time 3 -w '%{http_code}' \
  "${ws_headers[@]}" -H 'Origin: http://127.0.0.1:9' "$base/ws/room-1" >"$tmp/ws_code" || true
status=$(cat "$tmp/ws_code")
[ "$status" = 101 ] || fail "a port-less allowlisted Origin returned $status, want 101"
pass "websocket port-less origin pattern"

curl -s -o /dev/null --max-time 3 -w '%{http_code}' \
  "${ws_headers[@]}" -H 'Origin: http://evil.example' "$base/ws/room-1" >"$tmp/ws_code" || true
status=$(cat "$tmp/ws_code")
[ "$status" = 403 ] || fail "a foreign Origin returned $status, want 403"
pass "websocket origin policy"

# STUN: a binding request gets a binding success with XOR-MAPPED-ADDRESS. The
# datagram is sent straight through bash's /dev/udp so no netcat is needed, and
# dd reads the single reply datagram.
printf '\x00\x01\x00\x00\x21\x12\xa4\x42\x61\x62\x63\x64\x65\x66\x67\x68\x69\x6a\x6b\x6c' >"$tmp/stun.req"
exec 3<>"/dev/udp/127.0.0.1/$stun"
cat "$tmp/stun.req" >&3
timeout 5 dd bs=1500 count=1 <&3 >"$tmp/stun.resp" 2>/dev/null || true
exec 3<&- 3>&-
[ -s "$tmp/stun.resp" ] || fail "no STUN binding response"
[ "$(od -An -tx1 -N2 "$tmp/stun.resp" | tr -d ' \n')" = 0101 ] ||
  fail "the STUN response is not a binding success"
od -An -tx1 "$tmp/stun.resp" | tr -d ' \n' | grep -q 0020 ||
  fail "the STUN response carries no XOR-MAPPED-ADDRESS"
pass "stun binding"

# TURN over TLS: the listener completes a handshake and serves the certificate
# Caddy issued for localhost. Issuance is asynchronous, so retry briefly.
# Allocations are covered by test/client below.
served=0
for _ in $(seq 1 30); do
  openssl s_client -connect "127.0.0.1:$turn_tls" -servername localhost </dev/null >"$tmp/turn_tls" 2>&1 || true
  if grep -q 'BEGIN CERTIFICATE' "$tmp/turn_tls"; then
    served=1
    break
  fi
  sleep 0.5
done
[ "$served" = 1 ] || fail "TURN over TLS did not serve a certificate from Caddy's cache"
pass "turn over tls listener from certmagic"

# Protocol clients. WebSocket frames, the load test and TURN allocations need a
# real client, so test/client runs in the builder image, sharing this
# container's network namespace: it reaches every fixture listener on
# 127.0.0.1 without publishing more ports, including the TURN relay range.
# The browser check is part of the same Go test: when CADDY_RELAY_BROWSER=1 the
# container also installs a headless Chromium (about 200 MiB) and the test
# drives the fixture page over the DevTools protocol. CI sets the variable.
repo=$(cd "$here/.." && pwd)
client_prefix=
client_timeout=180s
test_timeout=90s
if [ "${CADDY_RELAY_BROWSER:-0}" = 1 ]; then
  client_prefix='apk add --no-cache chromium-headless-shell >/dev/null && '
  client_timeout=300s
  test_timeout=150s
fi
timeout "$client_timeout" docker run --rm --network "container:$container" \
  -v "$repo":/src \
  -v caddy-sf-mod:/mod \
  -w /src \
  -e GOMODCACHE=/mod -e GOCACHE=/mod/gocache -e CGO_ENABLED=0 \
  -e "CADDY_RELAY_E2E_HTTP=$base" \
  -e "CADDY_RELAY_E2E_TURN=127.0.0.1:$turn_port" \
  -e "CADDY_RELAY_E2E_STUN=127.0.0.1:$stun" \
  -e "CADDY_RELAY_E2E_TURN_SECRET=$turn_secret" \
  -e "CADDY_RELAY_E2E_RELAY_MIN=$relay_min" \
  -e "CADDY_RELAY_E2E_RELAY_MAX=$relay_max" \
  -e "CADDY_RELAY_BROWSER=${CADDY_RELAY_BROWSER:-0}" \
  "$builder" sh -c "${client_prefix}go test -count=1 -timeout=${test_timeout} -run TestContainer ./test/client" ||
  fail "the protocol client checks failed"
pass "websocket frames, load, and turn allocation"
if [ "${CADDY_RELAY_BROWSER:-0}" = 1 ]; then
  pass "browser websocket and eventsource"
fi

printf '\nall container checks passed\n'
