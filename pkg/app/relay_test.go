package app

import (
	"bytes"
	"context"
	"crypto/tls"
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
	"github.com/caddyserver/certmagic"
	"go.uber.org/zap"

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
		turn_max_allocations 64
		turn_auth_rate 25
		channels {
			max_channels 2000
			max_memory 256M
			max_participants 5000
			max_participants_per_ip 8
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
		if got.TurnMaxAllocations != 64 {
			t.Errorf("TurnMaxAllocations = %d, want 64", got.TurnMaxAllocations)
		}
		if got.TurnAuthRate != 25 {
			t.Errorf("TurnAuthRate = %d, want 25", got.TurnAuthRate)
		}
		t.Errorf("MaxChannels = %d", got.Channels.MaxChannels)
	}
	if got.Channels.MaxMemory != 256*1024*1024 {
		if got.Channels.MaxParticipants != 5000 {
			t.Errorf("MaxParticipants = %d, want 5000", got.Channels.MaxParticipants)
		}
		if got.Channels.MaxParticipantsPerIP != 8 {
			t.Errorf("MaxParticipantsPerIP = %d, want 8", got.Channels.MaxParticipantsPerIP)
		}
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

// subscribe is the allow-list for both reading and writing, so a publish
// target outside it must be refused rather than let a client write into
// channels it may not read.
func TestRelayPublishOutsideSubscribeFailsValidation(t *testing.T) {
	for _, directive := range []string{"relay_ws", "relay_sse"} {
		t.Run(directive, func(t *testing.T) {
			raw := adapt(t, relayGlobalBlock+":8080 {\n\t"+directive+" {\n\t\tsubscribe room-*\n\t\tpublish lobby\n\t}\n}\n")
			err := validate(t, raw)
			if err == nil {
				t.Fatal("validation succeeded, want a publish-outside-subscribe error")
			}
			if !strings.Contains(err.Error(), "outside subscribe") {
				t.Fatalf("error = %v, want it to mention outside subscribe", err)
			}
		})
	}
}

// A concrete target inside the subscribe wildcard is the normal shape and
// must keep validating.
func TestRelayPublishInsideSubscribeIsValid(t *testing.T) {
	raw := adapt(t, relayGlobalBlock+":8080 {\n\trelay_sse {\n\t\tsubscribe room-*\n\t\tpublish room-1\n\t}\n}\n")
	if err := validate(t, raw); err != nil {
		t.Fatalf("publish inside subscribe failed validation: %v", err)
	}
}

func TestRelayWSOriginsParse(t *testing.T) {
	raw := adapt(t, relayGlobalBlock+":8080 {\n\trelay_ws {\n\t\tsubscribe room-*\n\t\tpublish room-*\n\t\torigins example.com *.example.com\n\t}\n}\n")
	app := string(appJSON(t, raw, "http"))
	if !strings.Contains(app, `"origins":["example.com","*.example.com"]`) {
		t.Errorf("http app JSON is missing the origins list:\n%s", app)
	}
}

// trusted_proxies is a global list, because it decides how every transport
// turns a peer into the client host that per-IP limits key on.
func TestTrustedProxiesParse(t *testing.T) {
	raw := adapt(t, "{\n\trelay {\n\t\ttrusted_proxies 10.0.0.0/8\n\t\ttrusted_proxies 192.0.2.7 2001:db8::/32\n\t}\n}\n:8080 {\n\trespond \"ok\"\n}\n")
	var got Relay
	if err := json.Unmarshal(appJSON(t, raw, "relay"), &got); err != nil {
		t.Fatalf("unmarshal apps.relay: %v", err)
	}
	want := []string{"10.0.0.0/8", "192.0.2.7", "2001:db8::/32"}
	if len(got.TrustedProxies) != len(want) {
		t.Fatalf("TrustedProxies = %q, want %q", got.TrustedProxies, want)
	}
	for i := range want {
		if got.TrustedProxies[i] != want[i] {
			t.Fatalf("TrustedProxies = %q, want %q", got.TrustedProxies, want)
		}
	}
}

func TestTurnTLSDomainParses(t *testing.T) {
	raw := adapt(t, "{\n\trelay {\n\t\tturn_tls :5349\n\t\tturn_tls_domain turn.example.com\n\t}\n}\n:8080 {\n\trespond \"ok\"\n}\n")
	var got Relay
	if err := json.Unmarshal(appJSON(t, raw, "relay"), &got); err != nil {
		t.Fatalf("unmarshal apps.relay: %v", err)
	}
	if got.TurnTLSDomain != "turn.example.com" {
		t.Errorf("TurnTLSDomain = %q, want turn.example.com", got.TurnTLSDomain)
	}
}

// turn_tls takes its certificate from a file pair or from Caddy's certmagic
// cache; the file pair wins when both are configured.
func TestUsesCachedCertificate(t *testing.T) {
	cases := []struct {
		name string
		cfg  Relay
		want bool
	}{
		{"no tls listener", Relay{TurnTLSDomain: "turn.example.com"}, false},
		{"domain only", Relay{TurnTLSAddr: ":5349", TurnTLSDomain: "turn.example.com"}, true},
		{"certificate files", Relay{TurnTLSAddr: ":5349", TurnTLSCert: "cert.pem", TurnTLSKey: "key.pem"}, false},
		{"files and domain", Relay{TurnTLSAddr: ":5349", TurnTLSCert: "cert.pem", TurnTLSKey: "key.pem", TurnTLSDomain: "turn.example.com"}, false},
	}
	for _, tc := range cases {
		if got := tc.cfg.usesCachedCertificate(); got != tc.want {
			t.Errorf("%s: usesCachedCertificate() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestTurnTLSValidationAcceptsEitherCertificateSource(t *testing.T) {
	base := Relay{TurnTLSAddr: ":5349", TurnSecret: "s", TurnExternalIP: "203.0.113.1"}
	filePair := base
	filePair.TurnTLSCert, filePair.TurnTLSKey = "cert.pem", "key.pem"
	domain := base
	domain.TurnTLSDomain = "turn.example.com"
	halfPair := base
	halfPair.TurnTLSCert = "cert.pem"

	for name, cfg := range map[string]Relay{"file pair": filePair, "domain": domain} {
		if err := cfg.Validate(); err != nil {
			t.Errorf("%s failed validation: %v", name, err)
		}
	}
	if err := base.Validate(); err == nil {
		t.Error("turn_tls without a certificate source passed validation")
	}
	if err := halfPair.Validate(); err == nil {
		t.Error("turn_tls with only a certificate file passed validation")
	}
}

// turnCertificate serves the cached certificate for the configured name and
// reports a handshake error until certmagic has one.
func TestTurnCertificateFromCache(t *testing.T) {
	orig := certificateCacheLookup
	t.Cleanup(func() { certificateCacheLookup = orig })

	want := certmagic.Certificate{Certificate: tls.Certificate{Certificate: [][]byte{{1, 2, 3}}}}
	certificateCacheLookup = func(name string) []certmagic.Certificate {
		if name != "turn.example.com" {
			t.Errorf("cache lookup for %q, want turn.example.com", name)
		}
		return []certmagic.Certificate{want}
	}
	got, err := turnCertificate("turn.example.com")(nil)
	if err != nil {
		t.Fatalf("GetCertificate: %v", err)
	}
	if len(got.Certificate) != 1 || len(got.Certificate[0]) != 3 {
		t.Errorf("GetCertificate returned %v, want the cached certificate", got)
	}

	certificateCacheLookup = func(string) []certmagic.Certificate { return nil }
	if _, err := turnCertificate("turn.example.com")(nil); err == nil {
		t.Error("GetCertificate succeeded with an empty cache")
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

// Tokens on one line are joined with a single space, so a run of spaces only
// survives when it is inside a quoted token.
func TestInterceptTemplateQuotedTokenKeepsSpacing(t *testing.T) {
	raw := adapt(t, relayGlobalBlock+":8080 {\n\trelay_sse {\n\t\tsubscribe room-*\n\t\tpublish room-*\n\t\tintercept {\n\t\t\ttemplate {\n\t\t\t\t{{ message }} \"a  b\"\n\t\t\t}\n\t\t}\n\t}\n}\n")
	got := relaySSEIntercept(t, raw)
	want := "tmpl:{{ message }} a  b\n"
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
			"negative allocation cap",
			"{\n\trelay {\n\t\tturn_max_allocations -1\n\t}\n}\n",
			"turn_max_allocations",
		},
		{
			"bad auth rate",
			"{\n\trelay {\n\t\tturn_auth_rate fast\n\t}\n}\n",
			"turn_auth_rate",
		},
		{
			"trusted_proxies without an argument",
			"{\n\trelay {\n\t\ttrusted_proxies\n\t}\n}\n",
			"trusted_proxies",
		},
		{
			"trusted_proxies with a bad entry",
			"{\n\trelay {\n\t\ttrusted_proxies 10.0.0.0/8 nope\n\t}\n}\n:8080 {\n\trespond \"ok\"\n}\n",
			"trusted_proxies",
		},
		{
			"negative per-IP participant cap",
			"{\n\trelay {\n\t\tchannels {\n\t\t\tmax_participants_per_ip -2\n\t\t}\n\t}\n}\n",
			"max_participants_per_ip",
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

// One InterceptSpec serves every message on a channel, so a render must not
// leave its per-message funcs bound to the shared template: concurrent
// renders would then read another message's body, channel or client address.
func TestInterceptTemplateLeavesCompiledSpecUntouched(t *testing.T) {
	spec, err := compileIntercept("tmpl:{{ message }}|{{ channel }}|{{ remote_ip }}")
	if err != nil {
		t.Fatalf("compileIntercept: %v", err)
	}
	r := &Relay{log: zap.NewNop()}
	info := hub.InterceptInfo{Channel: "room-1", RemoteIP: "203.0.113.9"}
	out, ok := r.interceptTemplate(context.Background(), spec, []byte("hello"), info)
	if !ok || string(out) != "hello|room-1|203.0.113.9" {
		t.Fatalf("render = %q, ok = %v", out, ok)
	}

	// The shared tree still holds the provision-time stubs, so rendering it
	// directly gives empty values rather than the message just rendered.
	shared, err := spec.Compiled.Clone()
	if err != nil {
		t.Fatalf("clone compiled spec: %v", err)
	}
	var buf bytes.Buffer
	if err := shared.Execute(&buf, nil); err != nil {
		t.Fatalf("execute compiled spec: %v", err)
	}
	if got := buf.String(); got != "||" {
		t.Errorf("compiled spec rendered %q, want the empty stubs", got)
	}
}

func BenchmarkInterceptTemplate(b *testing.B) {
	spec, err := compileIntercept(`tmpl:{"text":{{ message }}}`)
	if err != nil {
		b.Fatalf("compileIntercept: %v", err)
	}
	r := &Relay{log: zap.NewNop()}
	body := []byte(`{"text":"hello"}`)
	info := hub.InterceptInfo{Channel: "room-1", RemoteIP: "203.0.113.9"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := r.interceptTemplate(context.Background(), spec, body, info); !ok {
			b.Fatal("render failed")
		}
	}
}

func TestParseBytes(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		ok   bool
	}{
		{"0", 0, true},
		{"512", 512, true},
		{" 4k ", 4096, true},
		{"1K", 1024, true},
		{"2M", 2 << 20, true},
		{"3g", 3 << 30, true},
		{"-1k", -1024, true},
		{"9223372036854775807", 9223372036854775807, true},
		{"", 0, false},
		{"k", 0, false},
		{"1.5k", 0, false},
		{"12x", 0, false},
		{"4 k", 0, false},
		{"9223372036854775808", 0, false},
		{"9223372036854775807k", 0, false},
	}
	for _, tc := range cases {
		got, err := parseBytes(tc.in)
		if (err == nil) != tc.ok {
			t.Errorf("parseBytes(%q) error = %v, want ok = %v", tc.in, err, tc.ok)
			continue
		}
		if err == nil && got != tc.want {
			t.Errorf("parseBytes(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestParsePortRange(t *testing.T) {
	cases := []struct {
		in     string
		lo, hi int
		ok     bool
	}{
		{"80-90", 80, 90, true},
		{"0-0", 0, 0, true},
		{"65535-65535", 65535, 65535, true},
		{"80", 0, 0, false},
		{"-80", 0, 0, false},
		{"80-", 0, 0, false},
		{"a-b", 0, 0, false},
		{"80-90-100", 0, 0, false},
		{" 80-90", 0, 0, false},
	}
	for _, tc := range cases {
		lo, hi, err := parsePortRange(tc.in)
		if (err == nil) != tc.ok {
			t.Errorf("parsePortRange(%q) error = %v, want ok = %v", tc.in, err, tc.ok)
			continue
		}
		if err == nil && (lo != tc.lo || hi != tc.hi) {
			t.Errorf("parsePortRange(%q) = %d-%d, want %d-%d", tc.in, lo, hi, tc.lo, tc.hi)
		}
	}
}
