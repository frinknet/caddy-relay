// Package app wires the caddy-relay module into Caddy: the global `relay`
// option (STUN/TURN listeners + channel declarations), the `relay_ws` and
// `relay_sse` HTTP directives, intercept execution, and the template funcs
// used inside intercept templates.
package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"

	"github.com/frinknet/caddy-relay/pkg/hub"
	"github.com/frinknet/caddy-relay/pkg/sse"
	"github.com/frinknet/caddy-relay/pkg/stun"
	"github.com/frinknet/caddy-relay/pkg/turn"
	"github.com/frinknet/caddy-relay/pkg/ws"
)

func init() {
	caddy.RegisterModule(Relay{})
	caddy.RegisterModule(RelayWS{})
	caddy.RegisterModule(RelaySSE{})
	httpcaddyfile.RegisterHandlerDirective("relay_ws", parseCaddyfileRelayWS)
	httpcaddyfile.RegisterHandlerDirective("relay_sse", parseCaddyfileRelaySSE)
	httpcaddyfile.RegisterGlobalOption("relay", unmarshalRelayOption)
	httpcaddyfile.RegisterDirectiveOrder("relay_ws", httpcaddyfile.Before, "file_server")
	httpcaddyfile.RegisterDirectiveOrder("relay_sse", httpcaddyfile.Before, "file_server")
}

// ============================================================
// Global option: relay { ... }
// ============================================================

// unmarshalRelayOption is the UnmarshalGlobalFunc for the "relay" option. It
// returns an httpcaddyfile.App so Caddy records the parsed config as
// apps.relay. That is what makes the app startable from JSON, visible to the
// admin API, and started even when no relay_ws/relay_sse directive exists.
func unmarshalRelayOption(d *caddyfile.Dispenser, existingVal any) (any, error) {
	r := new(Relay)
	if err := parseRelayBlock(d, r); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return httpcaddyfile.App{Name: "relay", Value: raw}, nil
}

func parseRelayBlock(d *caddyfile.Dispenser, cfg *Relay) error {
	if !d.Next() {
		return d.ArgErr()
	}
	if d.NextArg() {
		return d.ArgErr()
	}
	for d.NextBlock(0) {
		switch d.Val() {
		case "stun":
			if !d.NextArg() {
				return d.ArgErr()
			}
			if d.Val() != "off" {
				cfg.StunAddr = d.Val()
			}
		case "turn":
			if !d.NextArg() {
				return d.ArgErr()
			}
			if d.Val() != "off" {
				cfg.TurnAddr = d.Val()
			}
		case "turn_secret":
			if !d.NextArg() {
				return d.ArgErr()
			}
			cfg.TurnSecret = d.Val()
		case "turn_tls":
			if !d.NextArg() {
				return d.ArgErr()
			}
			if d.Val() != "off" {
				cfg.TurnTLSAddr = d.Val()
			}
		case "turn_tls_cert":
			if !d.NextArg() {
				return d.ArgErr()
			}
			cfg.TurnTLSCert = d.Val()
		case "turn_tls_key":
			if !d.NextArg() {
				return d.ArgErr()
			}
			cfg.TurnTLSKey = d.Val()
		case "turn_relay_ports":
			if !d.NextArg() {
				return d.ArgErr()
			}
			lo, hi, err := parsePortRange(d.Val())
			if err != nil {
				return d.Errf("turn_relay_ports: %v", err)
			}
			cfg.TurnRelayMin, cfg.TurnRelayMax = lo, hi
		case "turn_external_ip":
			if !d.NextArg() {
				return d.ArgErr()
			}
			cfg.TurnExternalIP = d.Val()
		case "turn_bind":
			if !d.NextArg() {
				return d.ArgErr()
			}
			cfg.TurnBind = d.Val()
		case "channels":
			if err := parseChannelsBlock(d, &cfg.Channels); err != nil {
				return err
			}
		default:
			return d.Errf("unknown relay sub-directive %q", d.Val())
		}
	}
	return nil
}

func parseChannelsBlock(d *caddyfile.Dispenser, cfg *hub.ChannelsConfig) error {
	for d.NextBlock(1) {
		switch d.Val() {
		case "max_channels":
			if !d.NextArg() {
				return d.ArgErr()
			}
			n, err := atoi(d.Val())
			if err != nil {
				return d.Errf("max_channels: %v", err)
			}
			cfg.MaxChannels = n
		case "max_memory":
			if !d.NextArg() {
				return d.ArgErr()
			}
			n, err := parseBytes(d.Val())
			if err != nil {
				return d.Errf("max_memory: %v", err)
			}
			cfg.MaxMemory = n
		case "max_participants":
			if !d.NextArg() {
				return d.ArgErr()
			}
			n, err := atoi(d.Val())
			if err != nil {
				return d.Errf("max_participants: %v", err)
			}
			cfg.MaxParticipants = n
		default:
			// channel pattern
			rule := &hub.ChannelRule{Pattern: d.Val()}
			if err := parseChannelRule(d, rule); err != nil {
				return err
			}
			cfg.Rules = append(cfg.Rules, rule)
		}
	}
	return nil
}

func parseChannelRule(d *caddyfile.Dispenser, rule *hub.ChannelRule) error {
	for d.NextBlock(2) {
		directive := d.Val()
		switch directive {
		case "messages":
			if !d.NextArg() {
				return d.ArgErr()
			}
			n, err := atoi(d.Val())
			if err != nil {
				return d.Errf("%s: %v", directive, err)
			}
			rule.Messages = n
		case "participants":
			if !d.NextArg() {
				return d.ArgErr()
			}
			n, err := atoi(d.Val())
			if err != nil {
				return d.Errf("%s: %v", directive, err)
			}
			rule.Participants = n
		case "max_size":
			if !d.NextArg() {
				return d.ArgErr()
			}
			n, err := parseBytes(d.Val())
			if err != nil {
				return d.Errf("%s: %v", directive, err)
			}
			rule.MaxSize = n
		case "rate_limit":
			if !d.NextArg() {
				return d.ArgErr()
			}
			dur, err := time.ParseDuration(d.Val())
			if err != nil {
				return d.Errf("%s: %v", directive, err)
			}
			rule.RateLimit = dur
		case "idle_ttl":
			if !d.NextArg() {
				return d.ArgErr()
			}
			dur, err := time.ParseDuration(d.Val())
			if err != nil {
				return d.Errf("%s: %v", directive, err)
			}
			rule.IdleTTL = dur
		case "retain":
			if !d.NextArg() {
				return d.ArgErr()
			}
			dur, err := time.ParseDuration(d.Val())
			if err != nil {
				return d.Errf("%s: %v", directive, err)
			}
			rule.Retain = dur
		default:
			return d.Errf("unknown channel sub-directive %q", d.Val())
		}
	}
	return nil
}

// ============================================================
// App module: relay
// ============================================================

// turnRealm is the TURN realm advertised to clients. Clients learn it from the
// server's challenge, so it only has to stay stable between restarts.
const turnRealm = "caddy-relay"

// Relay is the Caddy App that owns the hub and the STUN/TURN listeners.
// Its exported fields are its JSON configuration; the unexported fields are
// runtime state and are never serialised.
type Relay struct {
	StunAddr       string             `json:"stun,omitempty"`
	TurnAddr       string             `json:"turn,omitempty"`
	TurnSecret     string             `json:"turn_secret,omitempty"`
	TurnTLSAddr    string             `json:"turn_tls,omitempty"`
	TurnTLSCert    string             `json:"turn_tls_cert,omitempty"`
	TurnTLSKey     string             `json:"turn_tls_key,omitempty"`
	TurnRelayMin   int                `json:"turn_relay_min,omitempty"`
	TurnRelayMax   int                `json:"turn_relay_max,omitempty"`
	TurnExternalIP string             `json:"turn_external_ip,omitempty"`
	TurnBind       string             `json:"turn_bind,omitempty"`
	Channels       hub.ChannelsConfig `json:"channels,omitempty"`

	hub     *hub.Hub
	stun    io.Closer
	turn    io.Closer
	turnTLS io.Closer
	log     *zap.Logger
}

func (Relay) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "relay",
		New: func() caddy.Module { return new(Relay) },
	}
}

func (r *Relay) Provision(ctx caddy.Context) error {
	r.log = ctx.Logger()
	r.hub = hub.NewHub(r.Channels)
	r.hub.SetLogger(zapLog{ctx.Logger().Sugar()})
	return nil
}

// zapLog adapts Caddy's logger to hub.Logger.
type zapLog struct{ log *zap.SugaredLogger }

func (z zapLog) Warnw(msg string, kv ...any) { z.log.Warnw(msg, kv...) }

// Validate rejects TURN configurations that cannot be served safely. Caddy
// runs it after provisioning and before starting.
func (r *Relay) Validate() error {
	if r.TurnAddr == "" && r.TurnTLSAddr == "" {
		return nil
	}
	if r.TurnSecret == "" {
		return fmt.Errorf("relay: turn requires turn_secret, the shared secret for TURN REST credentials")
	}
	if r.TurnExternalIP == "" {
		return fmt.Errorf("relay: turn requires turn_external_ip")
	}
	if r.TurnTLSAddr != "" && (r.TurnTLSCert == "" || r.TurnTLSKey == "") {
		return fmt.Errorf("relay: turn_tls requires turn_tls_cert and turn_tls_key")
	}
	return nil
}

func (r *Relay) turnConfig(addr string) turn.Config {
	return turn.Config{
		ListenAddr:   addr,
		Realm:        turnRealm,
		ExternalIP:   r.TurnExternalIP,
		BindAddr:     r.TurnBind,
		RelayPortMin: r.TurnRelayMin,
		RelayPortMax: r.TurnRelayMax,
		AuthSecret:   r.TurnSecret,
		TLSCertFile:  r.TurnTLSCert,
		TLSKeyFile:   r.TurnTLSKey,
	}
}

func (r *Relay) Start() error {
	r.hub.Start()
	if r.StunAddr != "" {
		c, err := stun.Listen("udp", stripNetworkPrefix(r.StunAddr))
		if err != nil {
			return fmt.Errorf("stun listen: %w", err)
		}
		r.stun = c
		r.log.Info("stun listening", zap.String("addr", r.StunAddr))
	}
	if r.TurnAddr != "" {
		c, err := turn.Listen(r.turnConfig(r.TurnAddr))
		if err != nil {
			return fmt.Errorf("turn listen: %w", err)
		}
		r.turn = c
		r.log.Info("turn listening", zap.String("addr", r.TurnAddr))
	}
	if r.TurnTLSAddr != "" {
		c, err := turn.ListenTLS(r.turnConfig(r.TurnTLSAddr))
		if err != nil {
			return fmt.Errorf("turn tls listen: %w", err)
		}
		r.turnTLS = c
		r.log.Info("turn tls listening", zap.String("addr", r.TurnTLSAddr))
	}
	return nil
}

func (r *Relay) Stop() error {
	if r.stun != nil {
		r.stun.Close()
	}
	if r.turn != nil {
		r.turn.Close()
	}
	if r.turnTLS != nil {
		r.turnTLS.Close()
	}
	r.hub.Stop()
	return nil
}

// hubFor returns the hub for a site-level transport. In this single-process
// design there is exactly one hub; transports reach it via ctx.App("relay").
func hubFor(ctx caddy.Context) (*Relay, error) {
	appIface, err := ctx.App("relay")
	if err != nil {
		return nil, err
	}
	return appIface.(*Relay), nil
}

// ============================================================
// Bus implementation (what transports see)
// ============================================================

func (r *Relay) Subscribe(ctx context.Context, channel string, send func([]byte) bool) (*hub.Subscription, error) {
	return r.hub.Subscribe(ctx, channel, send)
}

func (r *Relay) Publish(ctx context.Context, patterns []string, body []byte, exclude hub.SubID) hub.Result {
	return r.hub.Publish(ctx, patterns, body, exclude)
}

func (r *Relay) Intercept(ctx context.Context, spec *hub.InterceptSpec, body []byte, info hub.InterceptInfo) ([]byte, bool) {
	if spec.IsZero() {
		return body, true
	}
	if spec.URL != "" {
		return r.interceptURL(ctx, spec.URL, body, info)
	}
	return r.interceptTemplate(ctx, spec, body, info)
}

func (r *Relay) Rule(channel string) *hub.ChannelRule { return r.hub.Rule(channel) }

// interceptClient is shared by URL intercepts so they reuse one connection
// pool instead of building a client (and its pool) per message.
var interceptClient = &http.Client{Timeout: 5 * time.Second}

// resolveInterceptURL turns a path like /api/filter into an absolute URL on
// the server that received the message, so an intercept can point at the same
// Caddy instance. Absolute URLs are returned unchanged.
func resolveInterceptURL(rawURL string, info hub.InterceptInfo) (string, bool) {
	u, err := neturl.Parse(rawURL)
	if err != nil {
		return "", false
	}
	if u.IsAbs() {
		return rawURL, true
	}
	req, ok := info.Request.(*http.Request)
	if !ok || req.Host == "" {
		return "", false
	}
	scheme := "http"
	if req.TLS != nil {
		scheme = "https"
	}
	if u.Host != "" {
		// scheme-relative, e.g. //other.example/filter
		return scheme + ":" + rawURL, true
	}
	path := u.Path
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	target := scheme + "://" + req.Host + path
	if u.RawQuery != "" {
		target += "?" + u.RawQuery
	}
	return target, true
}

func (r *Relay) interceptURL(ctx context.Context, rawURL string, body []byte, info hub.InterceptInfo) ([]byte, bool) {
	target, ok := resolveInterceptURL(rawURL, info)
	if !ok {
		return nil, false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return nil, false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := interceptClient.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, false
	}
	out, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || len(out) == 0 {
		return nil, false
	}
	return out, true
}

func (r *Relay) interceptTemplate(ctx context.Context, spec *hub.InterceptSpec, body []byte, info hub.InterceptInfo) ([]byte, bool) {
	if spec.Compiled == nil {
		return nil, false
	}
	data := map[string]any{}
	var parsed any
	if json.Unmarshal(body, &parsed) == nil {
		if m, ok := parsed.(map[string]any); ok {
			data = m
		}
	}

	var buf bytes.Buffer
	funcs := template.FuncMap{
		"message":   func() string { return string(body) },
		"data":      func() map[string]any { return data },
		"channel":   func() string { return info.Channel },
		"remote_ip": func() string { return info.RemoteIP },
		"request":   func() any { return info.Request },
		"dict":      dict,
		"set":       set,
	}
	funcs["publish"] = func(args ...any) hub.Result {
		if len(args) < 2 {
			return hub.Result{}
		}
		val := resolve(args[len(args)-1])
		raw, err := json.Marshal(val)
		if err != nil {
			return hub.Result{}
		}
		pats := make([]string, 0, len(args)-1)
		for _, a := range args[:len(args)-1] {
			if s, ok := resolve(a).(string); ok {
				pats = append(pats, s)
			}
		}
		return r.hub.Publish(ctx, pats, raw, 0) // 0 = include all
	}

	t := spec.Compiled.Funcs(funcs)
	// Re-parse so the funcs are registered; spec.Compiled is the source tree.
	parsed2, err := t.Clone()
	if err != nil {
		return nil, false
	}
	if err := parsed2.Execute(&buf, nil); err != nil {
		r.log.Warn("template execute", zap.Error(err))
		return nil, false
	}
	out := bytes.TrimSpace(buf.Bytes())
	return out, true
}

// resolve calls a template func-arg if Go left it as a func value.
func resolve(v any) any {
	switch f := v.(type) {
	case func() string:
		return f()
	case func() map[string]any:
		return f()
	case func() any:
		return f()
	}
	return v
}

func dict(kv ...any) map[string]any {
	m := make(map[string]any, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		k, _ := kv[i].(string)
		m[k] = kv[i+1]
	}
	return m
}

func set(m map[string]any, k string, v any) map[string]any {
	if m == nil {
		m = map[string]any{}
	}
	m[k] = v
	return m
}

// ============================================================
// Site directive: relay_ws
// ============================================================

// RelayWS is the relay_ws HTTP handler module.
type RelayWS struct {
	SubscribeStr string   `json:"subscribe,omitempty"`
	PublishStr   string   `json:"publish,omitempty"`
	InterceptStr string   `json:"intercept,omitempty"`
	Origins      []string `json:"origins,omitempty"`

	cfg   ws.Config
	relay *Relay
	log   *zap.Logger
}

func (RelayWS) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.relay_ws",
		New: func() caddy.Module { return new(RelayWS) },
	}
}

func (m *RelayWS) Provision(ctx caddy.Context) error {
	m.log = ctx.Logger()
	// subscribe is what stops a client from joining, or publishing to, an
	// arbitrary channel; publish is the read loop's target. A WebSocket
	// endpoint is useless without either, so require both up front. SSE keeps
	// publish optional because a GET stream is legitimately read-only.
	if m.SubscribeStr == "" {
		return fmt.Errorf("relay_ws: subscribe is required")
	}
	if m.PublishStr == "" {
		return fmt.Errorf("relay_ws: publish is required")
	}
	r, err := hubFor(ctx)
	if err != nil {
		return err
	}
	m.relay = r
	m.cfg = ws.Config{OriginPatterns: m.Origins}
	if m.SubscribeStr != "" {
		m.cfg.Subscribe = &hub.SubscribeSpec{Pattern: m.SubscribeStr}
	}
	if m.PublishStr != "" {
		m.cfg.Publish = &hub.PublishSpec{Pattern: m.PublishStr}
	}
	if m.InterceptStr != "" {
		spec, err := compileIntercept(m.InterceptStr)
		if err != nil {
			return fmt.Errorf("intercept: %w", err)
		}
		m.cfg.Intercept = spec
	}
	return nil
}

func (m *RelayWS) ServeHTTP(w http.ResponseWriter, r *http.Request, _ caddyhttp.Handler) error {
	ws.Serve(m.cfg, m.relay, w, r)
	return nil
}

// ============================================================
// Site directive: relay_sse
// ============================================================

type RelaySSE struct {
	SubscribeStr string `json:"subscribe,omitempty"`
	PublishStr   string `json:"publish,omitempty"`
	InterceptStr string `json:"intercept,omitempty"`

	cfg   sse.Config
	relay *Relay
	log   *zap.Logger
}

func (RelaySSE) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.relay_sse",
		New: func() caddy.Module { return new(RelaySSE) },
	}
}

func (m *RelaySSE) Provision(ctx caddy.Context) error {
	m.log = ctx.Logger()
	// subscribe is both the channel allow-list for GET streams and the check
	// applied to POST publishes, so it is required.
	if m.SubscribeStr == "" {
		return fmt.Errorf("relay_sse: subscribe is required")
	}
	r, err := hubFor(ctx)
	if err != nil {
		return err
	}
	m.relay = r
	m.cfg = sse.Config{}
	if m.SubscribeStr != "" {
		m.cfg.Subscribe = &hub.SubscribeSpec{Pattern: m.SubscribeStr}
	}
	if m.PublishStr != "" {
		m.cfg.Publish = &hub.PublishSpec{Pattern: m.PublishStr}
	}
	if m.InterceptStr != "" {
		spec, err := compileIntercept(m.InterceptStr)
		if err != nil {
			return fmt.Errorf("intercept: %w", err)
		}
		m.cfg.Intercept = spec
	}
	return nil
}

func (m *RelaySSE) ServeHTTP(w http.ResponseWriter, r *http.Request, _ caddyhttp.Handler) error {
	sse.Serve(m.cfg, m.relay, w, r)
	return nil
}

// ============================================================
// Caddyfile parsing for relay_ws / relay_sse
// ============================================================

func parseCaddyfileRelayWS(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	m := new(RelayWS)
	err := m.UnmarshalCaddyfile(h.Dispenser)
	return m, err
}

func parseCaddyfileRelaySSE(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	m := new(RelaySSE)
	err := m.UnmarshalCaddyfile(h.Dispenser)
	return m, err
}

func (m *RelayWS) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	if !d.Next() {
		return d.ArgErr()
	}
	if d.NextArg() {
		return d.ArgErr()
	}
	for d.NextBlock(0) {
		switch d.Val() {
		case "subscribe":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.SubscribeStr = d.Val()
		case "publish":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.PublishStr = d.Val()
		case "intercept":
			spec, err := parseInterceptBlock(d)
			if err != nil {
				return err
			}
			m.InterceptStr = spec
		case "origins":
			args := d.RemainingArgs()
			if len(args) == 0 {
				return d.ArgErr()
			}
			m.Origins = append(m.Origins, args...)
		default:
			return d.Errf("unknown relay_ws sub-directive %q", d.Val())
		}
	}
	return nil
}

func (m *RelaySSE) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	if !d.Next() {
		return d.ArgErr()
	}
	if d.NextArg() {
		return d.ArgErr()
	}
	for d.NextBlock(0) {
		switch d.Val() {
		case "subscribe":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.SubscribeStr = d.Val()
		case "publish":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.PublishStr = d.Val()
		case "intercept":
			spec, err := parseInterceptBlock(d)
			if err != nil {
				return err
			}
			m.InterceptStr = spec
		default:
			return d.Errf("unknown relay_sse sub-directive %q", d.Val())
		}
	}
	return nil
}

// parseInterceptBlock handles both `intercept /url` and `intercept { template { ... } }`.
// Returns an encoded string: "url:<path>" or "tmpl:<source>".
func parseInterceptBlock(d *caddyfile.Dispenser) (string, error) {
	if d.NextArg() {
		return "url:" + d.Val(), nil
	}
	var tmpl strings.Builder
	for d.NextBlock(1) {
		if d.Val() != "template" {
			return "", d.Errf("intercept: unknown sub-directive %q", d.Val())
		}
		// `template <<TAG ... TAG` keeps its source verbatim, which is what
		// templates that care about whitespace need.
		if d.NextArg() {
			tmpl.WriteString(d.Val())
			continue
		}
		// Inside a `template { ... }` block the tokenizer has already dropped
		// the original spacing, so rebuild the source a line at a time:
		// tokens on the same line are joined with one space.
		line, first := -1, true
		for d.NextBlock(2) {
			l := d.Line()
			switch {
			case first:
				first = false
			case l == line:
				tmpl.WriteByte(' ')
			default:
				tmpl.WriteByte('\n')
			}
			line = l
			tmpl.WriteString(d.Val())
		}
		tmpl.WriteByte('\n')
	}
	if tmpl.Len() == 0 {
		return "", d.Err("intercept: empty template block")
	}
	return "tmpl:" + tmpl.String(), nil
}

func compileIntercept(encoded string) (*hub.InterceptSpec, error) {
	if strings.HasPrefix(encoded, "url:") {
		return &hub.InterceptSpec{URL: strings.TrimPrefix(encoded, "url:")}, nil
	}
	src := strings.TrimPrefix(encoded, "tmpl:")
	// Register stub funcs so the template parser knows about them.
	// Real funcs are injected per-render in interceptTemplate.
	stubFuncs := template.FuncMap{
		"message":   func() string { return "" },
		"data":      func() map[string]any { return map[string]any{} },
		"channel":   func() string { return "" },
		"remote_ip": func() string { return "" },
		"request":   func() any { return nil },
		"dict":      dict,
		"set":       set,
		"publish":   func(args ...any) hub.Result { return hub.Result{} },
	}
	t, err := template.New("intercept").Funcs(stubFuncs).Option("missingkey=zero").Parse(src)
	if err != nil {
		return nil, err
	}
	return &hub.InterceptSpec{Template: src, Compiled: t}, nil
}

// ============================================================
// Helpers
// ============================================================

// atoi parses a plain integer, rejecting anything with trailing characters.
func atoi(s string) (int, error) {
	return strconv.Atoi(s)
}

// parseBytes parses a byte count with an optional K/M/G suffix (powers of 1024).
func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "k"):
		mult = 1024
		s = s[:len(s)-1]
	case strings.HasSuffix(s, "m"):
		mult = 1024 * 1024
		s = s[:len(s)-1]
	case strings.HasSuffix(s, "g"):
		mult = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return n * mult, nil
}

func parsePortRange(s string) (int, int, error) {
	parts := strings.SplitN(s, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("expected LO-HI, got %q", s)
	}
	lo, err := atoi(parts[0])
	if err != nil {
		return 0, 0, err
	}
	hi, err := atoi(parts[1])
	if err != nil {
		return 0, 0, err
	}
	return lo, hi, nil
}

func stripNetworkPrefix(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			return s[i+1:]
		}
	}
	return s
}

// Interface guards.
var (
	_ caddy.App                   = (*Relay)(nil)
	_ caddy.Module                = (*Relay)(nil)
	_ caddy.Provisioner           = (*Relay)(nil)
	_ caddy.Validator             = (*Relay)(nil)
	_ caddy.Provisioner           = (*RelayWS)(nil)
	_ caddy.Provisioner           = (*RelaySSE)(nil)
	_ caddyhttp.MiddlewareHandler = (*RelayWS)(nil)
	_ caddyhttp.MiddlewareHandler = (*RelaySSE)(nil)
)
