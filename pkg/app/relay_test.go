package app

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"

	"github.com/frinknet/caddy-relay/pkg/hub"
)

const relayGlobalBlock = `
{
	relay {
		stun udp/:3478
		turn udp/:5349
		turn_secret test-shared-secret
		turn_external_ip 203.0.113.1
		turn_relay_ports 49152-65535
		channels {
			max_channels 2000
			max_memory 256M
			room-* {
				messages 50
				participants 100
				max_size 64K
				rate_limit 100ms
				idle_ttl 5m
				retain 10m
			}
		}
	}
}
`

// adapt runs the Caddyfile adapter over input and returns the config JSON.
func adapt(t *testing.T, input string) []byte {
	t.Helper()
	adapter := caddyconfig.GetAdapter("caddyfile")
	if adapter == nil {
		t.Fatal("caddyfile adapter is not registered")
	}
	out, warnings, err := adapter.Adapt([]byte(input), nil)
	if err != nil {
		t.Fatalf("adapt: %v", err)
	}
	for _, w := range warnings {
		t.Logf("adapt warning: %v", w)
	}
	return out
}

func appJSON(t *testing.T, raw []byte, name string) json.RawMessage {
	t.Helper()
	var cfg struct {
		Apps map[string]json.RawMessage `json:"apps"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("unmarshal adapted config: %v", err)
	}
	app, ok := cfg.Apps[name]
	if !ok {
		t.Fatalf("apps.%s is missing from the adapted config", name)
	}
	return app
}

// validate provisions the config without starting it.
func validate(t *testing.T, raw []byte) error {
	t.Helper()
	var cfg caddy.Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("unmarshal into caddy.Config: %v", err)
	}
	cfg.Admin = &caddy.AdminConfig{Disabled: true}
	return caddy.Validate(&cfg)
}

// The global `relay` option must land in apps.relay; otherwise the hub and the
// STUN/TURN listeners never start and the config is invisible to the admin API.
func TestRelayOptionRegistersApp(t *testing.T) {
	raw := adapt(t, relayGlobalBlock+":8080 {\n\trelay_ws {\n\t\tsubscribe room-*\n\t\tpublish room-*\n\t}\n}\n")

	var got Relay
	if err := json.Unmarshal(appJSON(t, raw, "relay"), &got); err != nil {
		t.Fatalf("unmarshal apps.relay: %v", err)
	}
	if got.StunAddr != "udp/:3478" {
		t.Errorf("StunAddr = %q", got.StunAddr)
	}
	if got.TurnAddr != "udp/:5349" {
		t.Errorf("TurnAddr = %q", got.TurnAddr)
	}
	if got.TurnExternalIP != "203.0.113.1" {
		t.Errorf("TurnExternalIP = %q", got.TurnExternalIP)
	}
	if got.TurnSecret != "test-shared-secret" {
		t.Errorf("TurnSecret = %q", got.TurnSecret)
	}
	if got.TurnRelayMin != 49152 || got.TurnRelayMax != 65535 {
		t.Errorf("turn relay ports = %d-%d", got.TurnRelayMin, got.TurnRelayMax)
	}
	if got.Channels.MaxChannels != 2000 {
		t.Errorf("MaxChannels = %d", got.Channels.MaxChannels)
	}
	if got.Channels.MaxMemory != 256*1024*1024 {
		t.Errorf("MaxMemory = %d", got.Channels.MaxMemory)
	}
	if len(got.Channels.Rules) != 1 {
		t.Fatalf("rules = %d, want 1", len(got.Channels.Rules))
	}
	rule := got.Channels.Rules[0]
	if rule.Pattern != "room-*" || rule.Messages != 50 || rule.Participants != 100 || rule.MaxSize != 64*1024 {
		t.Errorf("rule = %+v", rule)
	}
	if rule.RateLimit != 100*time.Millisecond || rule.IdleTTL != 5*time.Minute || rule.Retain != 10*time.Minute {
		t.Errorf("rule durations = %+v", rule)
	}
}

// STUN/TURN must be configurable even when no relay_ws/relay_sse directive is
// present, which only works if the option registers the app by itself.
func TestRelayOptionRegistersAppWithoutTransports(t *testing.T) {
	raw := adapt(t, relayGlobalBlock+":8080 {\n\trespond \"ok\"\n}\n")
	_ = appJSON(t, raw, "relay")
}

func TestRelayHandlerJSONKeys(t *testing.T) {
	raw := adapt(t, ":8080 {\n\trelay_ws {\n\t\tsubscribe room-*\n\t\tpublish room-*\n\t}\n}\n")
	app := string(appJSON(t, raw, "http"))
	for _, want := range []string{`"subscribe":"room-*"`, `"publish":"room-*"`} {
		if !strings.Contains(app, want) {
			t.Errorf("http app JSON does not contain %s:\n%s", want, app)
		}
	}
}

func TestValidConfigPassesValidation(t *testing.T) {
	raw := adapt(t, relayGlobalBlock+":8080 {\n\trelay_ws {\n\t\tsubscribe room-*\n\t\tpublish room-*\n\t}\n}\n")
	if err := validate(t, raw); err != nil {
		t.Fatalf("validation failed: %v", err)
	}
}

// relay_ws without a publish target used to panic on the first inbound
// message; it must be rejected at provision time instead.
func TestRelayWSWithoutPublishFailsValidation(t *testing.T) {
	raw := adapt(t, ":8080 {\n\trelay_ws {\n\t\tsubscribe room-*\n\t}\n}\n")
	err := validate(t, raw)
	if err == nil {
		t.Fatal("validation succeeded, want a publish-required error")
	}
	if !strings.Contains(err.Error(), "publish is required") {
		t.Fatalf("error = %v, want it to mention publish", err)
	}
}

// subscribe is the channel allow-list; omitting it must not mean "allow all".
func TestRelayWSWithoutSubscribeFailsValidation(t *testing.T) {
	raw := adapt(t, ":8080 {\n\trelay_ws {\n\t\tpublish room-*\n\t}\n}\n")
	err := validate(t, raw)
	if err == nil {
		t.Fatal("validation succeeded, want a subscribe-required error")
	}
	if !strings.Contains(err.Error(), "subscribe is required") {
		t.Fatalf("error = %v, want it to mention subscribe", err)
	}
}

func TestRelaySSEWithoutSubscribeFailsValidation(t *testing.T) {
	raw := adapt(t, ":8080 {\n\trelay_sse {\n\t\tpublish room-*\n\t}\n}\n")
	err := validate(t, raw)
	if err == nil {
		t.Fatal("validation succeeded, want a subscribe-required error")
	}
	if !strings.Contains(err.Error(), "subscribe is required") {
		t.Fatalf("error = %v, want it to mention subscribe", err)
	}
}

// A GET-only SSE stream is legitimate, so publish stays optional there.
func TestRelaySSEWithoutPublishIsValid(t *testing.T) {
	raw := adapt(t, relayGlobalBlock+":8080 {\n\trelay_sse {\n\t\tsubscribe room-*\n\t}\n}\n")
	if err := validate(t, raw); err != nil {
		t.Fatalf("read-only relay_sse failed validation: %v", err)
	}
}

func TestRelayWSOriginsParse(t *testing.T) {
	raw := adapt(t, relayGlobalBlock+":8080 {\n\trelay_ws {\n\t\tsubscribe room-*\n\t\tpublish room-*\n\t\torigins example.com *.example.com\n\t}\n}\n")
	app := string(appJSON(t, raw, "http"))
	if !strings.Contains(app, `"origins":["example.com","*.example.com"]`) {
		t.Errorf("http app JSON is missing the origins list:\n%s", app)
	}
}

// TURN must not run without credentials, and TURN over TLS needs a key pair.
func TestTurnRequiresSecret(t *testing.T) {
	raw := adapt(t, "{\n\trelay {\n\t\tturn udp/:5349\n\t\tturn_external_ip 203.0.113.1\n\t}\n}\n:8080 {\n\trespond \"ok\"\n}\n")
	err := validate(t, raw)
	if err == nil {
		t.Fatal("validation succeeded, want a turn_secret error")
	}
	if !strings.Contains(err.Error(), "turn_secret") {
		t.Fatalf("error = %v, want it to mention turn_secret", err)
	}
}

func TestTurnRequiresExternalIP(t *testing.T) {
	raw := adapt(t, "{\n\trelay {\n\t\tturn udp/:5349\n\t\tturn_secret s\n\t}\n}\n:8080 {\n\trespond \"ok\"\n}\n")
	err := validate(t, raw)
	if err == nil {
		t.Fatal("validation succeeded, want a turn_external_ip error")
	}
	if !strings.Contains(err.Error(), "turn_external_ip") {
		t.Fatalf("error = %v, want it to mention turn_external_ip", err)
	}
}

func TestTurnTLSRequiresCertAndKey(t *testing.T) {
	raw := adapt(t, "{\n\trelay {\n\t\tturn_tls :5349\n\t\tturn_secret s\n\t\tturn_external_ip 203.0.113.1\n\t}\n}\n:8080 {\n\trespond \"ok\"\n}\n")
	err := validate(t, raw)
	if err == nil {
		t.Fatal("validation succeeded, want a turn_tls_cert error")
	}
	if !strings.Contains(err.Error(), "turn_tls_cert") {
		t.Fatalf("error = %v, want it to mention turn_tls_cert", err)
	}
}

// fileServerBlock matches the file_server directive and its block in the
// example Caddyfile. The test binary does not link Caddy's file_server module
// (doing so would pull its template dependencies into this module), so that
// one unrelated directive is dropped before adapting. Everything relay-related
// is adapted verbatim.
var fileServerBlock = regexp.MustCompile(`(?m)^\tfile_server \{[^}]*\}\n`)

// The shipped example config must adapt and provision, app included.
func TestSampleCaddyfileAdaptsAndValidates(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "..", "www", "Caddyfile"))
	if err != nil {
		t.Fatal(err)
	}
	input := fileServerBlock.ReplaceAllString(string(src), "")
	if strings.Contains(input, "file_server") {
		t.Fatal("file_server block was not stripped from the example Caddyfile")
	}

	raw := adapt(t, input)
	app := appJSON(t, raw, "relay")

	var got Relay
	if err := json.Unmarshal(app, &got); err != nil {
		t.Fatalf("unmarshal apps.relay: %v", err)
	}
	if got.StunAddr != "udp/:3478" {
		t.Errorf("sample config StunAddr = %q, want udp/:3478", got.StunAddr)
	}
	// TURN is commented out in the shipped config, which is also the image's
	// default: enabling it needs a secret and real certificate files.
	if got.TurnAddr != "" || got.TurnTLSAddr != "" || got.TurnSecret != "" {
		t.Errorf("sample config should ship with TURN disabled, got %+v", got)
	}
	if len(got.Channels.Rules) != 2 {
		t.Errorf("sample config declares %d channel rules, want 2", len(got.Channels.Rules))
	}
	if got.Channels.MaxParticipants != 10000 {
		t.Errorf("sample config MaxParticipants = %d, want 10000", got.Channels.MaxParticipants)
	}

	http := string(appJSON(t, raw, "http"))
	for _, want := range []string{`"handler":"relay_ws"`, `"handler":"relay_sse"`} {
		if !strings.Contains(http, want) {
			t.Errorf("http app is missing %s", want)
		}
	}

	if err := validate(t, raw); err != nil {
		t.Fatalf("sample config failed validation: %v", err)
	}
}

// relaySSEIntercept returns the intercept source stored for the relay_sse
// handler in an adapted config.
func relaySSEIntercept(t *testing.T, raw []byte) string {
	t.Helper()
	var cfg struct {
		Servers map[string]struct {
			Routes []struct {
				Handle []struct {
					Handler   string `json:"handler"`
					Intercept string `json:"intercept"`
				} `json:"handle"`
			} `json:"routes"`
		} `json:"servers"`
	}
	app := appJSON(t, raw, "http")
	if err := json.Unmarshal(app, &cfg); err != nil {
		t.Fatalf("unmarshal http app: %v", err)
	}
	for _, srv := range cfg.Servers {
		for _, route := range srv.Routes {
			for _, h := range route.Handle {
				if h.Handler == "relay_sse" {
					return h.Intercept
				}
			}
		}
	}
	t.Fatal("no relay_sse handler in the adapted config")
	return ""
}

// The template block used to be flattened one token per line, which broke any
// template whose text spans a line.
func TestInterceptTemplateKeepsLineStructure(t *testing.T) {
	raw := adapt(t, relayGlobalBlock+":8080 {\n\trelay_sse {\n\t\tsubscribe room-*\n\t\tpublish room-*\n\t\tintercept {\n\t\t\ttemplate {\n\t\t\t\t{{ message | publish channel }}\n\t\t\t}\n\t\t}\n\t}\n}\n")
	got := relaySSEIntercept(t, raw)
	want := "tmpl:{{ message | publish channel }}\n"
	if got != want {
		t.Errorf("intercept source = %q, want %q", got, want)
	}
}

func TestInterceptTemplateHeredoc(t *testing.T) {
	raw := adapt(t, relayGlobalBlock+":8080 {\n\trelay_sse {\n\t\tsubscribe room-*\n\t\tpublish room-*\n\t\tintercept {\n\t\t\ttemplate <<TMPL\n{\"a\":  \"{{ message }}\"}\nTMPL\n\t\t}\n\t}\n}\n")
	got := relaySSEIntercept(t, raw)
	t.Logf("heredoc source = %q", got)
	if !strings.Contains(got, `{"a":  "{{ message }}"}`) {
		t.Errorf("heredoc source = %q, want it to keep the body verbatim", got)
	}
}

// adaptErr returns the adapter's error for an intentionally broken config.
func adaptErr(input string) error {
	adapter := caddyconfig.GetAdapter("caddyfile")
	if adapter == nil {
		return fmt.Errorf("caddyfile adapter is not registered")
	}
	_, _, err := adapter.Adapt([]byte(input), nil)
	return err
}

func TestAdapterErrorPaths(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			"unknown relay sub-directive",
			"{\n\trelay {\n\t\tnope\n\t}\n}\n",
			"unknown relay sub-directive",
		},
		{
			"unknown channels sub-directive",
			"{\n\trelay {\n\t\tchannels {\n\t\t\troom-1 {\n\t\t\t\tnope 1\n\t\t\t}\n\t\t}\n\t}\n}\n",
			"unknown channel sub-directive",
		},
		{
			"unknown relay_ws sub-directive",
			":8080 {\n\trelay_ws {\n\t\tsubscribe room-*\n\t\tpublish room-*\n\t\tnope\n\t}\n}\n",
			"unknown relay_ws sub-directive",
		},
		{
			"unknown relay_sse sub-directive",
			":8080 {\n\trelay_sse {\n\t\tsubscribe room-*\n\t\tnope\n\t}\n}\n",
			"unknown relay_sse sub-directive",
		},
		{
			"bad duration",
			"{\n\trelay {\n\t\tchannels {\n\t\t\troom-1 {\n\t\t\t\tidle_ttl 5x\n\t\t\t}\n\t\t}\n\t}\n}\n",
			"idle_ttl",
		},
		{
			"bad size",
			"{\n\trelay {\n\t\tchannels {\n\t\t\troom-1 {\n\t\t\t\tmax_size 5Q\n\t\t\t}\n\t\t}\n\t}\n}\n",
			"max_size",
		},
		{
			"bad count",
			"{\n\trelay {\n\t\tchannels {\n\t\t\troom-1 {\n\t\t\t\tmessages twelve\n\t\t\t}\n\t\t}\n\t}\n}\n",
			"messages",
		},
		{
			"bad port range",
			"{\n\trelay {\n\t\tturn_relay_ports nonsense\n\t}\n}\n",
			"turn_relay_ports",
		},
		{
			"turn without a secret",
			"{\n\trelay {\n\t\tturn udp/:5349\n\t\tturn_external_ip 203.0.113.1\n\t}\n}\n:8080 {\n\trespond \"ok\"\n}\n",
			"turn_secret",
		},
		{
			"relay_ws without publish",
			":8080 {\n\trelay_ws {\n\t\tsubscribe room-*\n\t}\n}\n",
			"publish is required",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Adapter-stage errors surface from Adapt; provision-stage errors
			// (the last two cases) only surface from Validate.
			if err := adaptErr(tc.input); err != nil {
				if !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("adapt error = %v, want it to mention %q", err, tc.want)
				}
				return
			}
			err := validate(t, adapt(t, tc.input))
			if err == nil {
				t.Fatalf("no error, want one mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// A relative intercept path posts back to the server that received the
// message, which is what the documented `intercept /api/filter` form means.
func TestResolveInterceptURL(t *testing.T) {
	httpsReq := httptest.NewRequest(http.MethodPost, "https://relay.test/events/room-1", nil)
	httpReq := httptest.NewRequest(http.MethodPost, "http://relay.test/events/room-1", nil)

	cases := []struct {
		name string
		raw  string
		info hub.InterceptInfo
		want string
		ok   bool
	}{
		{"absolute", "https://api.test/filter", hub.InterceptInfo{}, "https://api.test/filter", true},
		{"path over https", "/filter", hub.InterceptInfo{Request: httpsReq}, "https://relay.test/filter", true},
		{"path over http", "/filter", hub.InterceptInfo{Request: httpReq}, "http://relay.test/filter", true},
		{"path without a slash", "filter", hub.InterceptInfo{Request: httpsReq}, "https://relay.test/filter", true},
		{"path with a query", "/filter?x=1", hub.InterceptInfo{Request: httpsReq}, "https://relay.test/filter?x=1", true},
		{"scheme relative", "//api.test/filter", hub.InterceptInfo{Request: httpsReq}, "https://api.test/filter", true},
		{"no request to resolve against", "/filter", hub.InterceptInfo{}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := resolveInterceptURL(tc.raw, tc.info)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v (got %q)", ok, tc.ok, got)
			}
			if got != tc.want {
				t.Errorf("resolved %q, want %q", got, tc.want)
			}
		})
	}
}
