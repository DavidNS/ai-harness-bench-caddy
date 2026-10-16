package integration

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddytest"
)

func aihHandshakes(entries []map[string]any) int {
	n := 0
	for _, e := range entriesWithMessage(entries, "handled request") {
		if s, _ := e["status"].(float64); s == http.StatusSwitchingProtocols {
			n++
		}
	}
	return n
}

func TestAihProbeDetachedSurvivesUnrelatedReload(t *testing.T) {
	tester := caddytest.NewTester(t)
	startStreamBackend(t, streamAddrA)
	cfg := streamConfig{routes: [][2]string{{"/a/", streamAddrA}}, detached: true}
	loadStreamConfig(t, tester, cfg)
	s := upgradeStream(t, "/a/ws", "echo")
	s.echo("1")
	cfg.handlers = []any{map[string]any{
		"handler":  "headers",
		"response": map[string]any{"set": map[string][]string{"X-P": {"1"}}},
	}}
	loadStreamConfig(t, tester, cfg)
	time.Sleep(1500 * time.Millisecond) // past the grace period
	s.echo("2")
	s.closeGracefully()
}

func TestAihProbeNonDetachedClosedOnReload(t *testing.T) {
	tester := caddytest.NewTester(t)
	startStreamBackend(t, streamAddrA)
	cfg := streamConfig{routes: [][2]string{{"/a/", streamAddrA}}, closeDelay: "2s"}
	loadStreamConfig(t, tester, cfg)
	s := upgradeStream(t, "/a/ws", "echo")
	s.echo("1")
	cfg.routes = append(cfg.routes, [2]string{"/b/", streamAddrB})
	start := time.Now()
	loadStreamConfig(t, tester, cfg)
	s.echo("during delay")
	s.waitClosed(15 * time.Second)
	if time.Since(start) < 1500*time.Millisecond {
		t.Fatalf("closed too early: %s", time.Since(start))
	}
}

func TestAihProbeSkipHandshakeAndCloseLog(t *testing.T) {
	for _, tc := range []struct {
		name      string
		logs      map[string]any
		level     string
		wantShake int
		wantClose bool
		wantLevel string
	}{
		{"skip", map[string]any{"skip_handshake": true, "level": "warn"}, "DEBUG", 0, true, "warn"},
		{"default", nil, "DEBUG", 1, true, "debug"},
		{"info-filtered", nil, "INFO", 1, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tester := caddytest.NewTester(t)
			startStreamBackend(t, streamAddrA)
			logPath := filepath.Join(t.TempDir(), "access.log")
			loadStreamConfig(t, tester, streamConfig{
				routes: [][2]string{{"/a/", streamAddrA}}, streamLogs: tc.logs,
				accessLog: logPath, accessLevel: tc.level,
			})
			s := upgradeStream(t, "/a/ws", "echo")
			s.echo("abc")
			s.closeGracefully()
			tester.AssertGetResponse("http://localhost:9080/a/plain", http.StatusOK, "plain")
			entries := waitForLogEntries(t, logPath, "handled request", tc.wantShake+1)
			if tc.wantClose {
				entries = waitForLogEntry(t, logPath, "stream closed")
			}
			if got := aihHandshakes(entries); got != tc.wantShake {
				t.Errorf("handshakes = %d, want %d", got, tc.wantShake)
			}
			closed := entriesWithMessage(entries, "stream closed")
			if tc.wantClose != (len(closed) == 1) {
				t.Fatalf("close entries = %d, want close=%v", len(closed), tc.wantClose)
			}
			if tc.wantClose && closed[0]["level"] != tc.wantLevel {
				t.Errorf("level = %v, want %s", closed[0]["level"], tc.wantLevel)
			}
		})
	}
}

func TestAihProbeUpgradeThroughEncodeInterceptDetached(t *testing.T) {
	tester := caddytest.NewTester(t)
	startStreamBackend(t, streamAddrA)
	logPath := filepath.Join(t.TempDir(), "access.log")
	loadStreamConfig(t, tester, streamConfig{
		routes:   [][2]string{{"/a/", streamAddrA}},
		detached: true,
		handlers: []any{
			map[string]any{"handler": "encode", "encodings": map[string]any{"gzip": map[string]any{}}},
			map[string]any{"handler": "intercept", "handle_response": []any{map[string]any{
				"routes": []any{map[string]any{"handle": []any{
					map[string]any{"handler": "static_response", "body": "x"},
				}}},
			}}},
		},
		accessLog: logPath,
	})
	s := upgradeStream(t, "/a/ws", "websocket",
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==", "Sec-WebSocket-Version: 13", "Accept-Encoding: gzip")
	s.echo("ping")
	s.echo("pong")
	s.expectNoData()
	if s.resp.Header.Get("Content-Encoding") != "" {
		t.Error("101 must not be encoded")
	}
	s.closeGracefully()
	waitForLogEntry(t, logPath, "stream closed")
}
