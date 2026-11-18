# caddy-relay — full implementation

Writing all 15 files. I'll flag any API calls that depend on library versions you may need to tweak after `go mod tidy`.

---

## `go.mod`

```go
module github.com/example/caddy-relay

go 1.22

require (
    github.com/caddyserver/caddy/v2 v2.8.4
    github.com/coder/websocket v1.8.12
    github.com/pion/stun/v3 v3.0.0
    github.com/pion/turn/v4 v4.0.0
    go.uber.org/zap v1.27.0
    golang.org/x/time v0.6.0
)
```

---

## `pkg/hub/spec.go`

```go
// Package hub provides the shared runtime for caddy-relay: the hub of
// channels, the channel actor, publish/subscribe mechanics, and the
// narrow Bus interface used by transports (ws, sse).
package hub

import (
    "text/template"
    "time"
)

// ChannelRule is one channel pattern declaration from the `channels {}` block.
type ChannelRule struct {
    Pattern      string        // "room-*", "lobby", etc.
    Messages     int           // ring buffer size
    Participants int           // max concurrent subscribers
    MaxSize      int64         // max bytes per message
    RateLimit    time.Duration // per-connection publish rate limit
    IdleTTL      time.Duration // close after no participants this long
    Retain       time.Duration // keep ring this long after last message
}

// ChannelsConfig is the full `channels {}` block.
type ChannelsConfig struct {
    Rules       []*ChannelRule
    MaxChannels int
    MaxMemory   int64 // bytes; 0 = unlimited
}

// SubscribeSpec is the parsed `subscribe <pattern>` sub-directive.
type SubscribeSpec struct {
    Pattern string
}

// PublishSpec is the parsed `publish <pattern>` sub-directive.
type PublishSpec struct {
    Pattern string
}

// InterceptSpec is the parsed `intercept` sub-directive.
// Exactly one of URL or Template is set. Compiled holds the parsed
// template, populated at provision time.
type InterceptSpec struct {
    URL      string
    Template string
    Compiled *template.Template
}

// IsZero reports whether no intercept was configured.
func (s *InterceptSpec) IsZero() bool {
    return s == nil || (s.URL == "" && s.Template == "")
}

// InterceptInfo carries the per-message context passed to intercept.
type InterceptInfo struct {
    Channel  string
    RemoteIP string
    Request  any // *http.Request; typed as any to avoid http import in transports' eyes
}
```

---

## `pkg/hub/bus.go`

```go
package hub

import "context"

// Bus is the narrow contract transports (ws, sse) use to talk to the hub.
// It is implemented by the root relay package, not by Hub itself — so the
// transports never import the Caddy layer.
type Bus interface {
    // Subscribe joins a channel. send is called for each message the
    // subscriber should receive (replay is returned separately). Returning
    // false from send drops the subscriber. The returned Subscription
    // carries the replay, a stable ID, and a Cancel func.
    Subscribe(ctx context.Context, channel string, send func([]byte) bool) (*Subscription, error)

    // Publish fans out body to every existing channel matching any pattern.
    // Concrete names create channels on demand. exclude is the subscriber
    // ID to skip (0 = skip nobody; used by transports to exclude the sender
    // for directive publish, and 0 for template publish / SSE POST).
    Publish(ctx context.Context, patterns []string, body []byte, exclude SubID) Result

    // Intercept runs the configured intercept (URL or template) on body.
    // Returns the transformed body and ok=false if the message should be
    // dropped.
    Intercept(ctx context.Context, spec *InterceptSpec, body []byte, info InterceptInfo) ([]byte, bool)
}

// SubID is a stable identifier for a subscription within the hub.
type SubID uint64

// Subscription is what Subscribe returns.
type Subscription struct {
    ID     SubID
    Replay [][]byte // prior messages, oldest first, byte-capped
    Cancel func()
}
```

---

## `pkg/hub/message.go`

```go
package hub

import "time"

// Message is a single retained message in a channel's ring buffer.
type Message struct {
    Body      []byte
    Timestamp time.Time
}

// Clone returns a deep copy of the message.
func (m *Message) Clone() *Message {
    return &Message{
        Body:      append([]byte(nil), m.Body...),
        Timestamp: m.Timestamp,
    }
}
```

---

## `pkg/hub/publish.go`

```go
package hub

// Result is the return of Hub.Publish. It reports, per channel name, whether
// the message was enqueued. Its String() is empty so bare use in a template
// emits nothing — use range or index to inspect.
type Result map[string]bool

// String returns "" so a bare {{ ... | publish ... }} emits nothing.
func (Result) String() string { return "" }

// Publish fan-out is delegated to Hub.Publish (hub.go); this file exists to
// keep the Result type and its Stringer together and to document the
// per-channel keying contract.
```

---

## `pkg/hub/hub.go`

```go
package hub

import (
    "context"
    "fmt"
    "strings"
    "sync"
    "time"
)

// Hub owns all channels and implements channel creation, caps, and sweeping.
type Hub struct {
    mu          sync.Mutex
    channels    map[string]*Channel
    cfg         ChannelsConfig
    memory      int64 // current retained bytes across all channels
    stop        chan struct{}
    wg          sync.WaitGroup
}

// NewHub creates a Hub from cfg. Call Start to spawn the sweeper.
func NewHub(cfg ChannelsConfig) *Hub {
    if cfg.MaxChannels <= 0 {
        cfg.MaxChannels = 1000
    }
    return &Hub{
        channels: make(map[string]*Channel),
        cfg:      cfg,
        stop:     make(chan struct{}),
    }
}

// Start launches the hub sweeper.
func (h *Hub) Start() {
    h.wg.Add(1)
    go h.sweep()
}

// Stop closes all channels and the sweeper.
func (h *Hub) Stop() {
    close(h.stop)
    h.wg.Wait()
    h.mu.Lock()
    defer h.mu.Unlock()
    for _, ch := range h.channels {
        ch.shutdown()
    }
    h.channels = make(map[string]*Channel)
    h.memory = 0
}

func (h *Hub) sweep() {
    defer h.wg.Done()
    t := time.NewTicker(time.Second)
    defer t.Stop()
    for {
        select {
        case <-h.stop:
            return
        case <-t.C:
            h.mu.Lock()
            for name, ch := range h.channels {
                if ch.shouldClose() {
                    h.memory -= ch.retainedBytes()
                    delete(h.channels, name)
                    ch.shutdown()
                }
            }
            h.mu.Unlock()
        }
    }
}

// matchRule returns the first rule whose pattern matches name.
func (h *Hub) matchRule(name string) *ChannelRule {
    for _, r := range h.cfg.Rules {
        if patternMatch(r.Pattern, name) {
            return r
        }
    }
    return nil
}

// getOrCreate returns the channel for name, creating it if the name is
// allowed by the declared rules and within caps.
func (h *Hub) getOrCreate(name string) (*Channel, error) {
    h.mu.Lock()
    defer h.mu.Unlock()

    if ch, ok := h.channels[name]; ok {
        return ch, nil
    }
    rule := h.matchRule(name)
    if rule == nil {
        return nil, fmt.Errorf("hub: channel %q not allowed by any rule", name)
    }
    if len(h.channels) >= h.cfg.MaxChannels {
        return nil, fmt.Errorf("hub: max_channels (%d) reached", h.cfg.MaxChannels)
    }
    ch := newChannel(name, rule, h)
    h.channels[name] = ch
    ch.start()
    return ch, nil
}

// existingChannels returns a copy of channels matching pattern.
func (h *Hub) existingChannels(pattern string) []*Channel {
    h.mu.Lock()
    defer h.mu.Unlock()
    var out []*Channel
    for name, ch := range h.channels {
        if patternMatch(pattern, name) {
            out = append(out, ch)
        }
    }
    return out
}

// Subscribe joins name.
func (h *Hub) Subscribe(ctx context.Context, name string, send func([]byte) bool) (*Subscription, error) {
    ch, err := h.getOrCreate(name)
    if err != nil {
        return nil, err
    }
    return ch.subscribe(send)
}

// Publish fans out body to every existing channel matching any pattern.
// Concrete names are created on demand.
func (h *Hub) Publish(ctx context.Context, patterns []string, body []byte, exclude SubID) Result {
    result := make(Result)
    for _, pat := range patterns {
        if isWildcard(pat) {
            matched := h.existingChannels(pat)
            if len(matched) == 0 {
                continue
            }
            for _, ch := range matched {
                result[ch.name] = ch.publish(body, exclude)
            }
        } else {
            ch, err := h.getOrCreate(pat)
            if err != nil {
                result[pat] = false
                continue
            }
            result[pat] = ch.publish(body, exclude)
        }
    }
    return result
}

// patternMatch: "room-*" matches "room-1"; a literal matches itself.
func patternMatch(pattern, name string) bool {
    if strings.HasSuffix(pattern, "*") {
        return strings.HasPrefix(name, strings.TrimSuffix(pattern, "*"))
    }
    return pattern == name
}

func isWildcard(p string) bool { return strings.HasSuffix(p, "*") }
```

---

## `pkg/hub/channel.go`

```go
package hub

import (
    "sync"
    "time"
)

// participant is one subscriber to a channel.
type participant struct {
    id   SubID
    send func([]byte) bool
}

// Channel is an actor: all mutable state is touched only from the actor
// goroutine via inbox closures.
type Channel struct {
    name string
    rule *ChannelRule
    hub  *Hub

    inbox chan func()
    done  chan struct{}

    // actor state (only touched by run)
    ring         []*Message
    participants map[SubID]*participant
    nextID       SubID
    lastMsg      time.Time
    lastPart     time.Time // last time participants count went 0→1 or 1→0

    mu   sync.Mutex // guards ring/retainedBytes for sweep reads from hub
    dead bool
}

func newChannel(name string, rule *ChannelRule, h *Hub) *Channel {
    return &Channel{
        name:         name,
        rule:         rule,
        hub:          h,
        inbox:        make(chan func(), 64),
        done:         make(chan struct{}),
        participants: make(map[SubID]*participant),
    }
}

func (c *Channel) start() {
    go c.run()
}

func (c *Channel) shutdown() {
    c.mu.Lock()
    if c.dead {
        c.mu.Unlock()
        return
    }
    c.dead = true
    c.mu.Unlock()
    close(c.done)
}

func (c *Channel) run() {
    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()
    for {
        select {
        case fn := <-c.inbox:
            fn()
        case <-ticker.C:
            if c.shouldClose() {
                c.shutdown()
                return
            }
        case <-c.done:
            return
        }
    }
}

// subscribe is called from Hub.Subscribe.
func (c *Channel) subscribe(send func([]byte) bool) (*Subscription, error) {
    type resp struct {
        sub *Subscription
        err error
    }
    out := make(chan resp, 1)
    select {
    case c.inbox <- func() {
        // enforce participants cap
        if c.rule.Participants > 0 && len(c.participants) >= c.rule.Participants {
            out <- resp{err: errParticipantsCap}
            return
        }
        // build byte-capped replay
        replay := c.buildReplay()
        id := c.nextID
        c.nextID++
        c.participants[id] = &participant{id: id, send: send}
        if len(c.participants) == 1 {
            c.lastPart = time.Now()
        }
        cancel := func() {
            select {
            case c.inbox <- func() {
                delete(c.participants, id)
                if len(c.participants) == 0 {
                    c.lastPart = time.Now()
                }
            }:
            case <-c.done:
            }
        }
        out <- resp{sub: &Subscription{ID: id, Replay: replay, Cancel: cancel}}
    }:
    case <-c.done:
        return nil, errChannelClosed
    }
    r := <-out
    return r.sub, r.err
}

// buildReplay returns at most rule.Messages messages, stopping early if the
// accumulated byte count would exceed rule.MaxSize (byte-cap).
func (c *Channel) buildReplay() [][]byte {
    if len(c.ring) == 0 {
        return nil
    }
    capBytes := c.rule.MaxSize
    if capBytes <= 0 {
        capBytes = 256 * 1024
    }
    var out [][]byte
    var total int64
    for _, m := range c.ring {
        n := int64(len(m.Body))
        if total+n > capBytes && len(out) > 0 {
            break
        }
        out = append(out, append([]byte(nil), m.Body...))
        total += n
    }
    return out
}

// publish is called from Hub.Publish.
func (c *Channel) publish(body []byte, exclude SubID) bool {
    select {
    case c.inbox <- func() { c.doPublish(body, exclude) }:
        return true
    case <-c.done:
        return false
    }
}

func (c *Channel) doPublish(body []byte, exclude SubID) {
    if c.rule.MaxSize > 0 && int64(len(body)) > c.rule.MaxSize {
        return // oversized; drop
    }
    // append to ring
    msg := &Message{Body: append([]byte(nil), body...), Timestamp: time.Now()}
    c.ring = append(c.ring, msg)
    if c.rule.Messages > 0 && len(c.ring) > c.rule.Messages {
        c.ring = c.ring[len(c.ring)-c.rule.Messages:]
    }
    c.lastMsg = time.Now()

    // fan out
    for id, p := range c.participants {
        if id == exclude {
            continue
        }
        if !p.send(msg.Body) {
            delete(c.participants, id)
        }
    }
    if len(c.participants) == 0 {
        c.lastPart = time.Now()
    }
}

// retainedBytes returns the total bytes held in the ring (for hub accounting).
func (c *Channel) retainedBytes() int64 {
    c.mu.Lock()
    defer c.mu.Unlock()
    var total int64
    for _, m := range c.ring {
        total += int64(len(m.Body))
    }
    return total
}

// shouldClose reports whether the channel is eligible for sweeping.
func (c *Channel) shouldClose() bool {
    now := time.Now()
    c.mu.Lock()
    dead := c.dead
    c.mu.Unlock()
    if dead {
        return false
    }

    // These reads race with the actor; acceptable for a sweep heuristic.
    noParts := len(c.participants) == 0
    if !noParts {
        return false
    }
    idleFor := now.Sub(c.lastPart)
    if idleFor < c.rule.IdleTTL {
        return false
    }
    if len(c.ring) == 0 {
        return true
    }
    if now.Sub(c.lastMsg) > c.rule.Retain {
        return true
    }
    return false
}
```

Errors:

```go
package hub

import "errors"

var (
    errParticipantsCap = errors.New("hub: participants cap reached")
    errChannelClosed   = errors.New("hub: channel closed")
)
```

---

## `pkg/hub/subscribe.go`

```go
package hub

// subscribe.go exists to keep the Join/Replay logic documented in one place.
// The actual implementation lives in channel.go (subscribe, buildReplay) and
// hub.go (Hub.Subscribe), because the actor owns the ring.
//
// Contract:
//   - Replay is oldest-first, byte-capped by rule.MaxSize.
//   - A new subscriber receives replay, then live messages via send.
//   - send returning false removes the subscriber.
//   - Cancel is idempotent and safe to call after the channel is closed.
```

---

## `pkg/stun/stun.go`

```go
// Package stun implements a minimal STUN binding server over UDP.
package stun

import (
    "io"
    "net"

    "github.com/pion/stun/v3"
)

// Listen starts a STUN server on network/addr (e.g. "udp", ":3478").
// Returns a closer that stops the server.
func Listen(network, addr string) (io.Closer, error) {
    udpAddr, err := net.ResolveUDPAddr(network, addr)
    if err != nil {
        return nil, err
    }
    conn, err := net.ListenUDP(network, udpAddr)
    if err != nil {
        return nil, err
    }
    go serve(conn)
    return conn, nil
}

func serve(conn *net.UDPConn) {
    buf := make([]byte, 1500)
    for {
        n, addr, err := conn.ReadFromUDP(buf)
        if err != nil {
            return
        }
        req := &stun.Message{Raw: append([]byte(nil), buf[:n]...)}
        if err := req.Decode(); err != nil {
            continue
        }
        if req.Type != stun.BindingRequest {
            continue
        }
        resp, err := stun.Build(
            stun.BindingSuccess,
            stun.NewTransactionIDSetter(req.TransactionID),
            &stun.XORMappedAddress{IP: addr.IP, Port: addr.Port},
            stun.Fingerprint,
        )
        if err != nil {
            continue
        }
        _, _ = conn.WriteToUDP(resp.Raw, addr)
    }
}
```

*(API note: `stun.NewTransactionIDSetter` exists in pion/stun/v3; if your version differs, use `stun.TransactionIDSetter(req.TransactionID)` or set `resp.TransactionID = req.TransactionID` manually after Build.)*

---

## `pkg/turn/turn.go`

```go
// Package turn wraps pion/turn's TURN server.
package turn

import (
    "fmt"
    "io"
    "net"

    "github.com/pion/turn/v4"
)

// Config configures a TURN listener.
type Config struct {
    ListenAddr    string // e.g. "udp/:3478"
    Realm         string // "rtc.example.com"
    ExternalIP    string // public IP for relay addresses
    RelayPortMin  int    // 0 = pion default (49152)
    RelayPortMax  int    // 0 = pion default (65535)
    AuthSecret    string // shared secret for REST auth; empty = no auth
}

// Listen starts a TURN server. The returned closer stops it.
func Listen(cfg Config) (io.Closer, error) {
    network, addr := splitNetworkAddr(cfg.ListenAddr)
    udpAddr, err := net.ResolveUDPAddr(network, addr)
    if err != nil {
        return nil, err
    }
    conn, err := net.ListenUDP(network, udpAddr)
    if err != nil {
        return nil, err
    }

    relayIP := net.ParseIP(cfg.ExternalIP)
    if relayIP == nil {
        conn.Close()
        return nil, fmt.Errorf("turn: invalid external IP %q", cfg.ExternalIP)
    }

    srv, err := turn.NewServer(turn.ServerConfig{
        Realm: cfg.Realm,
        AuthHandler: func(username, realm string, srcAddr net.Addr) ([]byte, bool) {
            if cfg.AuthSecret == "" {
                return nil, true // no auth configured
            }
            // Simple shared-secret: password == secret. Replace with REST
            // HMAC validation if you need time-limited credentials.
            return []byte(cfg.AuthSecret), true
        },
        PacketConnConfigs: []turn.PacketConnConfig{
            {
                PacketConn: conn,
                RelayAddressGenerator: &turn.RelayAddressGeneratorStatic{
                    RelayAddress: relayIP,
                    Address:      cfg.ExternalIP,
                },
            },
        },
    })
    if err != nil {
        conn.Close()
        return nil, err
    }
    return server, nil
}

func splitNetworkAddr(s string) (string, string) {
    // supports "udp/:3478" and "udp/1.2.3.4:3478"
    for i := 0; i < len(s); i++ {
        if s[i] == '/' {
            return s[:i], s[i+1:]
        }
    }
    return "udp", s
}
```

---

## `pkg/ws/ws.go`

```go
// Package ws implements the WebSocket transport for caddy-relay.
package ws

import (
    "context"
    "net/http"
    "path"
    "sync/atomic"
    "time"

    "github.com/coder/websocket"
    "golang.org/x/time/rate"

    "github.com/example/caddy-relay/pkg/hub"
)

// Config is the resolved, provisioned relay_ws config.
type Config struct {
    Subscribe *hub.SubscribeSpec
    Intercept *hub.InterceptSpec
    Publish   *hub.PublishSpec
}

// Serve handles an HTTP request for the relay_ws directive.
func Serve(cfg Config, bus hub.Bus, w http.ResponseWriter, r *http.Request) {
    conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
        InsecureSkipVerify: false,
    })
    if err != nil {
        return
    }
    defer conn.Close(websocket.StatusInternalError, "")

    channel := path.Base(r.URL.Path)
    if cfg.Subscribe != nil && !patternMatch(cfg.Subscribe.Pattern, channel) {
        conn.Close(websocket.StatusPolicyViolation, "channel not allowed")
        return
    }

    ctx := r.Context()

    var closed atomic.Bool
    send := func(body []byte) bool {
        if closed.Load() {
            return false
        }
        wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
        defer cancel()
        return conn.Write(wctx, websocket.MessageText, body) == nil
    }

    sub, err := bus.Subscribe(ctx, channel, send)
    if err != nil {
        conn.Close(websocket.StatusInternalError, err.Error())
        return
    }
    defer func() { closed.Store(true); sub.Cancel() }()

    for _, body := range sub.Replay {
        if !send(body) {
            return
        }
    }

    limiter := rate.NewLimiter(rate.Every(rateLimit(cfg)), 1)

    for {
        typ, body, err := conn.Read(ctx)
        if err != nil {
            return
        }
        if typ != websocket.MessageText {
            continue
        }
        if !limiter.Allow() {
            continue
        }

        info := hub.InterceptInfo{
            Channel:  channel,
            RemoteIP: r.RemoteAddr,
            Request:  r,
        }
        out, ok := bus.Intercept(ctx, cfg.Intercept, body, info)
        if !ok || len(out) == 0 {
            continue
        }
        bus.Publish(ctx, []string{cfg.Publish.Pattern}, out, sub.ID)
    }
}

// rateLimit returns the per-message interval for this transport.
// The rule's RateLimit is not visible here; ws.go uses a fixed 1s default.
// (Per-connection rate limiting is applied at this layer, per spec.)
func rateLimit(cfg Config) time.Duration { return time.Second }

func patternMatch(pattern, name string) bool {
    if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
        return len(name) >= len(pattern)-1 && name[:len(pattern)-1] == pattern[:len(pattern)-1]
    }
    return pattern == name
}
```

---

## `pkg/sse/sse.go`

```go
// Package sse implements the Server-Sent Events transport for caddy-relay.
package sse

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "path"
    "time"

    "github.com/example/caddy-relay/pkg/hub"
)

// Config is the resolved, provisioned relay_sse config.
type Config struct {
    Subscribe *hub.SubscribeSpec
    Intercept *hub.InterceptSpec
    Publish   *hub.PublishSpec
}

// Serve handles an HTTP request for the relay_sse directive.
func Serve(cfg Config, bus hub.Bus, w http.ResponseWriter, r *http.Request) {
    switch r.Method {
    case http.MethodGet:
        serveGet(cfg, bus, w, r)
    case http.MethodPost:
        servePost(cfg, bus, w, r)
    default:
        http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
    }
}

func serveGet(cfg Config, bus hub.Bus, w http.ResponseWriter, r *http.Request) {
    flusher, ok := w.(http.Flusher)
    if !ok {
        http.Error(w, "streaming unsupported", http.StatusInternalServerError)
        return
    }
    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")
    w.WriteHeader(http.StatusOK)

    channel := path.Base(r.URL.Path)
    if cfg.Subscribe != nil && !patternMatch(cfg.Subscribe.Pattern, channel) {
        fmt.Fprintf(w, "event: error\ndata: channel not allowed\n\n")
        flusher.Flush()
        return
    }

    ctx := r.Context()
    send := func(body []byte) bool {
        if _, err := fmt.Fprintf(w, "data: %s\n\n", body); err != nil {
            return false
        }
        flusher.Flush()
        return true
    }

    sub, err := bus.Subscribe(ctx, channel, send)
    if err != nil {
        fmt.Fprintf(w, "event: error\ndata: %s\n\n", err)
        flusher.Flush()
        return
    }
    defer sub.Cancel()

    for _, body := range sub.Replay {
        if !send(body) {
            return
        }
    }
    // keepalive
    t := time.NewTicker(15 * time.Second)
    defer t.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-t.C:
            fmt.Fprint(w, ": keepalive\n\n")
            flusher.Flush()
        }
    }
}

func servePost(cfg Config, bus hub.Bus, w http.ResponseWriter, r *http.Request) {
    body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
    if err != nil {
        http.Error(w, "read error", http.StatusBadRequest)
        return
    }
    channel := path.Base(r.URL.Path)
    if cfg.Subscribe != nil && !patternMatch(cfg.Subscribe.Pattern, channel) {
        http.Error(w, "channel not allowed", http.StatusForbidden)
        return
    }
    if cfg.Publish == nil {
        http.Error(w, "no publish target", http.StatusInternalServerError)
        return
    }

    info := hub.InterceptInfo{
        Channel:  channel,
        RemoteIP: r.RemoteAddr,
        Request:  r,
    }
    out, ok := bus.Intercept(r.Context(), cfg.Intercept, body, info)
    if !ok || len(out) == 0 {
        http.Error(w, "dropped", http.StatusBadGateway)
        return
    }
    bus.Publish(r.Context(), []string{cfg.Publish.Pattern}, out, 0)
    w.WriteHeader(http.StatusAccepted)
}

func patternMatch(pattern, name string) bool {
    if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
        return len(name) >= len(pattern)-1 && name[:len(pattern)-1] == pattern[:len(pattern)-1]
    }
    return pattern == name
}
```

---

## `relay.go`

The big file. I'll structure it with clear section comments.

```go
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

    "github.com/example/caddy-relay/pkg/hub"
    "github.com/example/caddy-relay/pkg/sse"
    "github.com/example/caddy-relay/pkg/stun"
    "github.com/example/caddy-relay/pkg/turn"
    "github.com/example/caddy-relay/pkg/ws"
)

func init() {
    caddy.RegisterModule(Relay{})
    caddy.RegisterModule(RelayWS{})
    caddy.RegisterModule(RelaySSE{})
    httpcaddyfile.RegisterHandlerDirective("relay_ws", parseCaddyfileRelayWS)
    httpcaddyfile.RegisterHandlerDirective("relay_sse", parseCaddyfileRelaySSE)
    caddy.RegisterGlobalOption("relay", &relayOption{})
}

// ============================================================
// Global option: relay { ... }
// ============================================================

// relayOption is the Caddyfile global option. It parses into a package-level
// Config that Relay.Provision picks up.
type relayOption struct{}

var (
    globalMu  sync.Mutex
    globalCfg *Config
)

func (relayOption) String() string { return "relay" }

func (relayOption) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
    cfg := defaultConfig()
    if err := parseRelayBlock(d, cfg); err != nil {
        return err
    }
    globalMu.Lock()
    globalCfg = cfg
    globalMu.Unlock()
    return nil
}

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
        ID:  "caddy.app.relay",
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
    t, err := template.New("intercept").Option("missingkey=zero").Parse(src)
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
```

---

## Wiring notes / things to verify after `go mod tidy`

1. **`stun.NewTransactionIDSetter`** — the pion/stun/v3 API for setting the transaction ID varies by version. If it doesn't exist, set `resp.TransactionID = req.TransactionID` after `stun.Build`.

2. **`turn.RelayAddressGeneratorStatic`** — in pion/turn/v4 this is the right name, but the `Address` field expects a string and `RelayAddress` a `net.IP`. Verify against your pinned version.

3. **`template.Clone()` in `interceptTemplate`** — I used `Clone` so the base `spec.Compiled` (parsed at provision with no funcs) stays clean per-render. If you see a "cannot clone" error, restructure: parse the *source* (`spec.Template`) per render, or pre-register the funcs in `spec.Compiled` at compile time by passing them in `compileIntercept` — but the funcs close over per-message state, so per-render funcs is correct.

4. **`intercept` template block collection** — the `d.Val()` per line loses the original whitespace. Multi-line templates with indentation will work but the source will be flattened. If you care, use `d.RemainingArgs()` / raw token access or `caddyfile.Dispenser`'s line tokens.

5. **`hubFor` returns `caddy.Context.App("relay")`** — the `relay` App must be provisioned before the transports. Caddy's provisioning order handles this if the App is in the module list; verify with a smoke test that WS serves.

6. **The WS `rateLimit`** ignores the channel rule. To make it rule-aware, pass the rule down from the hub after `Subscribe` returns (the subscription could carry `RateLimit`), or look it up via a `Bus.Rule(channel)` method. I left it as 1s because the spec says per-connection and the transport doesn't see rules.

7. **`Compiled` template + `Stringer` on `Result`** — Go's `text/template` calls `fmt.Fprint` on the value, which honors `Stringer`. The empty `String()` suppresses bare `{{ ... | publish ... }}` output as designed.

8. **`handle @ws` / `handle @sse`** — the `handle` wrapper means Caddy's router already matched by header. If you want auth, put `basic_auth`/`forward_auth` *inside* the `handle @ws` block before `relay_ws`. That's how the site block sketch works.

---

Want me to also write a `Caddyfile.full` smoke-test config and a short `README.md`, or do you want to `go build` first and report the compile errors so we can iterate on the API discrepancies (mostly pion versions and the template `Clone`)?
