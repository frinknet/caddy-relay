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
#
# Anything that needs a protocol client (WebSocket frames, TURN allocations,
# STUN attribute parsing) is covered by the package tests in pkg/.
#
# Usage:
#   ./test/e2e.sh
#   CADDY_RELAY_IMAGE=caddy-relay:my-tag ./test/e2e.sh
#   KEEP=1 ./test/e2e.sh     # leave the container running for inspection
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

image=${CADDY_RELAY_IMAGE:-caddy-relay:test}
name=caddy-relay-e2e-$$
tmp=$(mktemp -d)
container=

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
# block. Only the placeholders this script knows about are substituted: the
# Caddyfile also carries Go template syntax for relay_ws.
render_config() {
  sed \
    -e "s/{{STUN_PORT}}/$stun_port/g" \
    -e "s/{{TURN_PORT}}/$turn_port/g" \
    -e "s/{{TURN_TLS_PORT}}/$turn_tls_port/g" \
    -e "s/{{RELAY_MIN}}/$relay_min/g" \
    -e "s/{{RELAY_MAX}}/$relay_max/g" \
    -e "s/{{HTTP_PORT}}/$http_port/g" \
    -e "s/{{HTTPS_PORT}}/$https_port/g" \
    "$here/Caddyfile" >"$tmp/Caddyfile"
  local placeholder
  for placeholder in STUN_PORT TURN_PORT TURN_TLS_PORT RELAY_MIN RELAY_MAX HTTP_PORT HTTPS_PORT; do
    grep -q "{{$placeholder}}" "$tmp/Caddyfile" && fail "placeholder $placeholder was not substituted"
  done
}

# The certificate TURN over TLS loads.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout "$tmp/turn.key" -out "$tmp/turn.crt" -days 1 -subj /CN=localhost \
  -addext subjectAltName=IP:127.0.0.1 >/dev/null 2>&1 ||
  fail "generating the turn_tls self-signed certificate"

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
      -v "$tmp/turn.crt:/etc/caddy/turn.crt:ro" \
      -v "$tmp/turn.key:/etc/caddy/turn.key:ro" \
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

# TURN over TLS: the listener completes a handshake and serves the configured
# certificate. Allocations are covered by pkg/turn.
openssl s_client -connect "127.0.0.1:$turn_tls" -servername localhost </dev/null >"$tmp/turn_tls" 2>&1 || true
grep -q 'BEGIN CERTIFICATE' "$tmp/turn_tls" || fail "TURN over TLS did not serve a certificate"
pass "turn over tls listener"

printf '\nall container checks passed\n'
