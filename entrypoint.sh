#!/bin/toybox sh
# caddy-relay entrypoint — sets up toybox multicall symlinks then execs caddy

set -e

# Create toybox symlinks (idempotent at runtime)
cd /bin || exit 1
for cmd in $(./toybox); do
    if [ "$cmd" != "toybox" ] && [ ! -L "$cmd" ]; then
        ln -s toybox "$cmd"
    fi
done

# Ensure standard dirs exist
mkdir -p /var/lib/caddy /var/cache/caddy /tmp
chmod 1777 /tmp

exec /bin/caddy run \
    --config /etc/caddy/Caddyfile \
    --adapter caddyfile \
    "$@"