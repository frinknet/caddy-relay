// Package relay wires the caddy-relay module into Caddy: the global `relay`
// option (STUN/TURN listeners + channel declarations), the `relay_ws` and
// `relay_sse` HTTP directives, intercept execution, and the template funcs
// used inside intercept templates.
package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
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

// relayOption is the Caddyfile global option. It parses into a package-level
// Config that Relay.Provision picks up.
// unmarshalRelayOption is the UnmarshalGlobalFunc for the "relay" option.
func unmarshalRelayOption(d *caddyfile.Dispenser, existingVal any) (any, error) {
	cfg := defaultConfig()
	if err := parseRelayBlock(d, cfg); err != nil {
		return nil, err
	}
	globalMu.Lock()
	globalCfg = cfg
	globalMu.Unlock()
	return cfg, nil
}

var (
	globalMu  sync.Mutex
	globalCfg *Config
)

func currentGlobalConfig() *Config {
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalCfg == nil {
		return defaultConfig()
	}
	return globalCfg
}

// Config is the resolved global configuration.
type Config struct {
	StunAddr      string
	TurnAddr      string
	TurnTLSAddr   string
	TurnRelayMin  int
	TurnRelayMax  int
	TurnExternalIP string
	Channels      hub.ChannelsConfig
}

func defaultConfig() *Config {
	return &Config{
		Channels: hub.ChannelsConfig{
			MaxChannels: 1000,
		},
	}
}

func parseRelayBlock(d *caddyfile.Dispenser, cfg *Config) error {
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
		case "turn_tls":
			if !d.NextArg() {
				return d.ArgErr()
			}
			cfg.TurnTLSAddr = d.Val()
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
		switch d.Val() {
		case "messages":
			if !d.NextArg() {
				return d.ArgErr()
			}
			n, err := atoi(d.Val())
			if err != nil {
				return err
			}
			rule.Messages = n
		case "participants":
			if !d.NextArg() {
				return d.ArgErr()
			}
			n, err := atoi(d.Val())
			if err != nil {
				return err
			}
			rule.Participants = n
		case "max_size":
			if !d.NextArg() {
				return d.ArgErr()
			}
			n, err := parseBytes(d.Val())
			if err != nil {
				return err
			}
			rule.MaxSize = n
		case "rate_limit":
			if !d.NextArg() {
				return d.ArgErr()
			}
			dur, err := time.ParseDuration(d.Val())
			if err != nil {
				return err
			}
			rule.RateLimit = dur
		case "idle_ttl":
			if !d.NextArg() {
				return d.ArgErr()
			}
			dur, err := time.ParseDuration(d.Val())
			if err != nil {
				return err
			}
			rule.IdleTTL = dur
		case "retain":
			if !d.NextArg() {
				return d.ArgErr()
			}
			dur, err := time.ParseDuration(d.Val())
			if err != nil {
				return err
			}
			rule.Retain = dur
		default:
			return d.Errf("unknown channel sub-directive %q", d.Val())
		}
	}
	return nil
}

// ============================================================
// App module: caddy.app.relay
// ============================================================

// Relay is the Caddy App that owns the hub and the STUN/TURN listeners.
type Relay struct {
	cfg   *Config
	hub   *hub.Hub
	stun  io.Closer
	turn  io.Closer
	log   *zap.Logger
}

func (Relay) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "relay",
		New: func() caddy.Module { return new(Relay) },
	}
}

func (r *Relay) Provision(ctx caddy.Context) error {
	r.log = ctx.Logger()
	r.cfg = currentGlobalConfig()
	r.hub = hub.NewHub(r.cfg.Channels)
	return nil
}

func (r *Relay) Start() error {
	r.hub.Start()
	if r.cfg.StunAddr != "" {
		c, err := stun.Listen("udp", stripNetworkPrefix(r.cfg.StunAddr))
		if err != nil {
			return fmt.Errorf("stun listen: %w", err)
		}
		r.stun = c
		r.log.Info("stun listening", zap.String("addr", r.cfg.StunAddr))
	}
	if r.cfg.TurnAddr != "" {
		c, err := turn.Listen(turn.Config{
			ListenAddr: r.cfg.TurnAddr,
			Realm:      "caddy-relay",
			ExternalIP: r.cfg.TurnExternalIP,
			AuthSecret: "",
		})
		if err != nil {
			return fmt.Errorf("turn listen: %w", err)
		}
		r.turn = c
		r.log.Info("turn listening", zap.String("addr", r.cfg.TurnAddr))
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
		return r.interceptURL(ctx, spec.URL, body)
	}
	return r.interceptTemplate(ctx, spec, body, info)
}

func (r *Relay) interceptURL(ctx context.Context, url string, body []byte) ([]byte, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, false
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
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
	SubscribeStr string
	InterceptStr string
	PublishStr   string

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
	r, err := hubFor(ctx)
	if err != nil {
		return err
	}
	m.relay = r
	m.cfg = ws.Config{}
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
	SubscribeStr string
	InterceptStr string
	PublishStr   string

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
	var tmpl string
	for d.NextBlock(1) {
		if d.Val() != "template" {
			return "", d.Errf("intercept: unknown sub-directive %q", d.Val())
		}
		// collect all lines inside the template block
		for d.NextBlock(2) {
			tmpl += d.Val() + "\n"
		}
	}
	if tmpl == "" {
		return "", d.Err("intercept: empty template block")
	}
	return "tmpl:" + tmpl, nil
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

func atoi(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

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
	n, err := atoi(s)
	return int64(n) * mult, err
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
	_ caddy.App             = (*Relay)(nil)
	_ caddy.Module          = (*Relay)(nil)
	_ caddy.Provisioner     = (*Relay)(nil)
	_ caddy.Provisioner     = (*RelayWS)(nil)
	_ caddy.Provisioner     = (*RelaySSE)(nil)
	_ caddyhttp.MiddlewareHandler = (*RelayWS)(nil)
	_ caddyhttp.MiddlewareHandler = (*RelaySSE)(nil)
)