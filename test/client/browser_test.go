package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// testBrowser drives the fixture page in a real Chromium. The page runs its
// own WebSocket and EventSource checks and records the verdict in
// data-done; this function waits for it over the DevTools protocol instead of
// guessing how long the page needs.
func testBrowser(t *testing.T, base string) {
	chrome, err := findChrome()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	args := []string{
		"--headless",
		"--no-sandbox",
		"--disable-dev-shm-usage",
		"--disable-gpu",
		"--no-first-run",
		"--no-default-browser-check",
		"--remote-debugging-port=9222",
		"--remote-allow-origins=*",
		"--user-data-dir=" + t.TempDir(),
		"about:blank",
	}
	if !strings.Contains(filepath.Base(chrome), "headless") {
		args[0] = "--headless=new"
	}
	cmd := exec.Command(chrome, args...)
	var output lockedBuffer
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start chromium: %v", err)
	}
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
	}()

	page := waitForPage(ctx, t, &output)
	conn, _, err := websocket.Dial(ctx, page, nil)
	if err != nil {
		t.Fatalf("devtools dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	c := &cdp{conn: conn}
	if err := c.call(ctx, "Page.enable", nil, nil); err != nil {
		t.Fatalf("Page.enable: %v", err)
	}
	if err := c.call(ctx, "Runtime.enable", nil, nil); err != nil {
		t.Fatalf("Runtime.enable: %v", err)
	}
	if err := c.call(ctx, "Page.navigate", map[string]string{"url": base + "/browser/"}, nil); err != nil {
		t.Fatalf("Page.navigate: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		var done struct {
			Result struct{ Value string }
		}
		err := c.call(ctx, "Runtime.evaluate", map[string]any{
			"expression":    `document.documentElement.dataset.done || ""`,
			"returnByValue": true,
		}, &done)
		if err == nil && (done.Result.Value == "1" || done.Result.Value == "0") {
			var text struct {
				Result struct{ Value string }
			}
			_ = c.call(ctx, "Runtime.evaluate", map[string]any{
				"expression":    `document.getElementById("result").textContent`,
				"returnByValue": true,
			}, &text)
			if done.Result.Value != "1" || !strings.HasPrefix(text.Result.Value, "PASS ") {
				t.Fatalf("browser result = %q", text.Result.Value)
			}
			t.Logf("browser: %s", text.Result.Value)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("browser check did not finish: %s", output.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// findChrome locates the headless shell installed by test/e2e.sh, falling back
// to a full Chromium when one is present.
func findChrome() (string, error) {
	for _, name := range []string{
		"chromium-headless-shell",
		"chromium",
		"chromium-browser",
		"chrome-headless-shell",
		"google-chrome",
	} {
		if path, err := exec.LookPath(name); err == nil {
			return path, nil
		}
	}
	for _, path := range []string{
		"/usr/lib/chromium/chrome-headless-shell",
		"/usr/lib/chromium/chromium",
	} {
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
	}
	return "", fmt.Errorf("no chromium binary found")
}

// waitForPage polls the DevTools endpoint until a page target appears.
func waitForPage(ctx context.Context, t *testing.T, output *lockedBuffer) string {
	deadline := time.Now().Add(20 * time.Second)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:9222/json/list", nil)
		if err == nil {
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				var targets []struct {
					Type                 string `json:"type"`
					WebSocketDebuggerURL string `json:"webSocketDebuggerUrl"`
				}
				decodeErr := json.NewDecoder(resp.Body).Decode(&targets)
				resp.Body.Close()
				if decodeErr == nil {
					for _, target := range targets {
						if target.Type == "page" && target.WebSocketDebuggerURL != "" {
							return target.WebSocketDebuggerURL
						}
					}
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("chromium devtools did not expose a page: %s", output.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// lockedBuffer collects Chrome's stderr while the test reads it for a
// diagnostic, so a failed run cannot race the process's writes.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// cdp is the minimal DevTools client used here: one call at a time, ignoring
// events that Chrome interleaves with the responses.
type cdp struct {
	conn *websocket.Conn
	next int
}

func (c *cdp) call(ctx context.Context, method string, params any, out any) error {
	c.next++
	msg := map[string]any{"id": c.next, "method": method}
	if params != nil {
		msg["params"] = params
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if err := c.conn.Write(ctx, websocket.MessageText, data); err != nil {
		return err
	}
	for {
		_, raw, err := c.conn.Read(ctx)
		if err != nil {
			return err
		}
		var resp struct {
			ID     int             `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(raw, &resp) != nil || resp.ID != c.next {
			continue
		}
		if resp.Error != nil {
			return fmt.Errorf("%s: %s", method, resp.Error.Message)
		}
		if out != nil && len(resp.Result) > 0 {
			return json.Unmarshal(resp.Result, out)
		}
		return nil
	}
}
