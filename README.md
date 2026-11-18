# caddy-plugin-template

Skeleton for `github.com/frinknet/caddy-*` plugins. Owns the shared
build/test infrastructure so every plugin stays byte-identical where it
should be — Dockerfile shape, build script, dockerignore, e2e harness —
and plugin-specific only where it needs to be.

## Creating a new plugin

    ./init.sh caddy-foo                     # → ../caddy-foo
    ./init.sh caddy-bar /tmp/caddy-bar      # → /tmp/caddy-bar

    cd ../caddy-foo
    go mod tidy
    ./build.sh --no-replace

The script substitutes `__NAME__`, `__SHORT__`, `__MODULE__` in every
`.tmpl` file and copies the rest verbatim. It fetches Apache 2.0 as
`LICENSE` from apache.org.

## What's shared vs. per-plugin

Shared — do not diverge without a reason:
- `Dockerfile` shape: assets → build → scratch
- `build.sh` (derives image name from `$PWD`)
- `.dockerignore`, `.gitignore`
- `tests/e2e/` scaffold

Per-plugin — edit freely:
- `example.go` → your real module(s)
- `go.mod` — add your dependencies
- `www/Caddyfile` — your plugin's config
- Add `pkg/`, `entrypoint.sh`, `data/` as needed

## Retrofitting an existing plugin

For each `../caddy-*` repo:

    diff <(cat caddy-plugin-template/template/Dockerfile) Dockerfile
    diff <(cat caddy-plugin-template/template/build.sh) build.sh
    diff <(cat caddy-plugin-template/template/.dockerignore) .dockerignore

Port the shared files. Keep the plugin's real code, `www/`, `entrypoint.sh`,
and its working `tests/e2e/` untouched. Where the plugin's Dockerfile
legitimately differs (no `www/`, extra COPY lines), keep the differences —
the goal is "identical where it should be," not "identical everywhere."

## Bumping Caddy

One place:

    caddy-plugin-template/template/go.mod.tmpl

Edit `github.com/caddyserver/caddy/v2` and `go` directive there, then
`diff` against each plugin's `go.mod`. Or, per-plugin:

    docker run --rm -v "$PWD:/src" -w /src caddy:2.11.9-builder-alpine \
        sh -c 'go mod edit -go=1.26 && \
               go get github.com/caddyserver/caddy/v2@v2.11.9 && \
               go mod tidy && go mod verify'
