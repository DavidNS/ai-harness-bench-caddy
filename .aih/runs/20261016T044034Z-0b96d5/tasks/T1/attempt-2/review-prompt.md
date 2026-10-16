You are reviewing one task of an approved plan, implemented by a different session whose
reasoning you have not seen. The change is already applied in this worktree. You can read the
repository, and you can run tests through `aih check`, the only command available:
`aih check <test files...>` runs those tests, `--race` under the race detector and
`--run <pattern>` only matching tests (when the project supports them), and
`aih check --probe <file>` runs a scratch test you wrote as a NEW file named `aih_probe*` and
deletes it afterwards. You cannot edit the change: every other file is locked for you. The harness
has already verified mechanically that the diff stays within the task's files, that the changed
tests fail without the production change and pass with it, and that the whole suite is green. Do
not re-check those. Your job is judgement, backed by tests you run.

## Original request

Harden upgraded-stream handling in `reverse_proxy` across config reloads (caddyserver/caddy
PR #7649 scope).

Long-lived upgraded connections through the reverse proxy (WebSocket and other `Upgrade`
tunnels, HTTP/2 extended CONNECT) keep the whole handler and config alive across reloads, so a
server that reloads often holds on to old configs, and the current behaviour on reload (close every
upgraded stream, optionally after a delay) is the only option. Middleware and response writers also
misbehave once a connection has been hijacked, and operators cannot see what their streams are
doing.

Required behaviour:

- A new opt-in `reverse_proxy` setting, `stream_detached`: when set, upgraded streams are detached
  from the config lifecycle and survive a reload; on reload only the streams whose upstream was
  removed from the new config are closed. The default stays as today: upgraded streams are closed on
  reload, honouring the existing close-delay behaviour.
- A `stream_logs` block with a `skip_handshake` option to suppress access logging of the upgrade
  handshake (default: logged), with stream close logs available in the access log via
  `logger_name access`, and a configurable log level (default DEBUG).
- Upgraded streams must keep working through the other handlers of a route: `encode`, `intercept`
  with `handle_response`, access logging. Once a connection is hijacked, nothing may write to the
  response again; a 101 is the final response for an upgrade; the proxy copies bytes on the raw
  connection, not through the recording response writer.
- HTTP/2 extended CONNECT (RFC 8441) upgrades keep working, as do HTTP/1.1 upgrades in detached
  mode.
- Stream observability metrics in `reverse_proxy`: active streams, streams closed by result,
  stream duration, bytes to and from the backend, each stream accounted exactly once.
- Caddyfile and JSON config both expose the new options; document them.

Add integration tests in `caddytest/integration` that run a real server and real clients: streams
surviving and being closed across reloads in each mode, extended CONNECT over h2, upgrades through
`encode` and `intercept`, detached streams with access logging. Add unit tests for the pieces you
change. The existing suites must stay green. Do not change unrelated behaviour.

## Plan summary

Single PR: add stream_detached and stream_logs to reverse_proxy, per-upstream stream tracking so reloads close only streams whose upstream was removed, hijack-safe response writers (recorder/encode/intercept), stream metrics and close logs, with unit and integration tests. Docs are the JSON struct-field doc comments and Caddyfile syntax comments, since the repo has no separate docs tree.

## Task T1 (T1)

Order: (1) add the config fields (StreamDetached, StreamLogs) and the Caddyfile and JSON parsing; (2) add the hijack-safety guards in the response recorder, encode, intercept and access logging; (3) add the per-upstream stream registry, detached Cleanup semantics, stream close accounting and the access-logger close log in handleUpgradeResponse; (4) add the metrics; (5) write the unit tests, the adapt test and the integration tests. The default close path (closeConnections, cleanupConnections, stream_close_delay) must not change when stream_detached is unset.

Acceptance criteria:
- ACC1 [behavior]: With stream_detached set, an upgraded stream stays open and keeps echoing after a config reload that keeps its upstream. Test: TestStreamDetachedSurvivesReload.
- ACC2 [behavior]: With stream_detached set, a reload that removes the stream's upstream closes that stream, and streams on retained upstreams stay open. Test: TestStreamDetachedClosesRemovedUpstream.
- ACC3 [behavior]: With stream_detached unset, a reload closes the upgraded stream, honouring stream_close_delay (immediately when 0, after the delay otherwise). Test: TestStreamDefaultClosedOnReload.
- ACC4 [behavior]: stream_logs skip_handshake suppresses the access log entry for the upgrade handshake; the default logs it. Test: TestStreamLogsSkipHandshake.
- ACC5 [behavior]: A stream close entry appears under logger_name access at the configured level, DEBUG by default. Test: TestStreamCloseLogAccessLogger.
- ACC6 [behavior]: After Hijack, the response recorder and encode writers ignore WriteHeader, Write and Flush; the 101 is written once to the raw conn; proxy copying uses the raw conn. Test: TestResponseRecorderNoWriteAfterHijack.
- ACC7 [behavior]: WebSocket-style upgrades succeed and echo through encode, and through intercept with handle_response, with access logging enabled. Test: TestUpgradeThroughEncodeAndIntercept.
- ACC8 [behavior]: An h2 extended CONNECT stream works end to end, and an HTTP/1.1 upgrade works in detached mode. Test: TestExtendedConnectH2 and TestStreamDetachedHTTP11Upgrade.
- ACC9 [behavior]: Metrics record active streams, closed-by-result, duration and bytes to/from the backend, each stream counted exactly once even with concurrent close paths. Test: TestStreamMetricsAccountedOnce.
- ACC10 [behavior]: The Caddyfile parses stream_detached and stream_logs { skip_handshake; level } into the expected JSON; invalid input errors. Test: the reverse_proxy_stream_detached.caddyfiletest adapt test and TestStreamOptionsCaddyfile.
- ACC11 [other]: The new JSON fields and Caddyfile syntax are documented in struct doc comments and the UnmarshalCaddyfile syntax comment.
- ACC12 [other]: Integration tests exist in caddytest/integration using a real server and real clients, covering the scenarios listed in the intent.
- ACC13 [other]: Unit tests accompany each changed package: reverseproxy (registry, metrics, caddyfile) and caddyhttp (hijack guard).

Files declared for the task:
- modules/caddyhttp/reverseproxy/reverseproxy.go (production)
- modules/caddyhttp/reverseproxy/streaming.go (production)
- modules/caddyhttp/reverseproxy/caddyfile.go (production)
- modules/caddyhttp/reverseproxy/metrics.go (production)
- modules/caddyhttp/responsewriter.go (production)
- modules/caddyhttp/server.go (production)
- modules/caddyhttp/logging.go (production)
- modules/caddyhttp/encode/encode.go (production)
- modules/caddyhttp/intercept/intercept.go (production)
- modules/caddyhttp/reverseproxy/streaming_test.go (test)
- modules/caddyhttp/reverseproxy/stream_detached_test.go (test)
- modules/caddyhttp/reverseproxy/metrics_test.go (test)
- modules/caddyhttp/hijack_test.go (test)
- caddytest/integration/stream_reload_test.go (test)
- caddytest/integration/stream_test.go (test)
- caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest (test)

Test files of this task that the change modified (existing tests, declared by the plan):
- (none)

Test results: selected tests green; red proven; suite green

## The diff

```diff
diff --git a/caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest b/caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest
new file mode 100644
index 00000000..36286c58
--- /dev/null
+++ b/caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest
@@ -0,0 +1,60 @@
+https://example.com {
+	reverse_proxy localhost:54321 {
+		stream_detached
+		stream_logs {
+			skip_handshake
+			level info
+		}
+	}
+}
+
+----------
+{
+	"apps": {
+		"http": {
+			"servers": {
+				"srv0": {
+					"listen": [
+						":443"
+					],
+					"routes": [
+						{
+							"match": [
+								{
+									"host": [
+										"example.com"
+									]
+								}
+							],
+							"handle": [
+								{
+									"handler": "subroute",
+									"routes": [
+										{
+											"handle": [
+												{
+													"handler": "reverse_proxy",
+													"stream_detached": true,
+													"stream_logs": {
+														"level": "info",
+														"skip_handshake": true
+													},
+													"upstreams": [
+														{
+															"dial": "localhost:54321"
+														}
+													]
+												}
+											]
+										}
+									]
+								}
+							],
+							"terminal": true
+						}
+					]
+				}
+			}
+		}
+	}
+}
diff --git a/caddytest/integration/stream_reload_test.go b/caddytest/integration/stream_reload_test.go
new file mode 100644
index 00000000..e6118a67
--- /dev/null
+++ b/caddytest/integration/stream_reload_test.go
@@ -0,0 +1,818 @@
+package integration
+
+import (
+	"bufio"
+	"bytes"
+	"crypto/tls"
+	"encoding/json"
+	"errors"
+	"fmt"
+	"io"
+	"net"
+	"net/http"
+	"os"
+	"os/exec"
+	"path/filepath"
+	"strings"
+	"testing"
+	"time"
+
+	"golang.org/x/net/http2"
+	"golang.org/x/net/http2/hpack"
+
+	"github.com/caddyserver/caddy/v2/caddytest"
+)
+
+const (
+	streamAddrA = "localhost:54321"
+	streamAddrB = "localhost:54322"
+
+	// set in the process TestExtendedConnectH2 runs the actual test in
+	extendedConnectChildEnv = "CADDY_TEST_EXTENDED_CONNECT"
+)
+
+// streamBackend is a backend accepting any upgrade (WebSocket or not),
+// that echoes what it receives on the upgraded connection until the client
+// closes it. Other requests get a plain response.
+type streamBackend struct {
+	srv *http.Server
+}
+
+func startStreamBackend(t *testing.T, dial string) *streamBackend {
+	t.Helper()
+
+	ln, err := net.Listen("tcp", strings.Replace(dial, "localhost", "127.0.0.1", 1))
+	if err != nil {
+		t.Fatalf("backend listen: %v", err)
+	}
+	b := &streamBackend{srv: &http.Server{
+		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
+			proto := r.Header.Get("Upgrade")
+			if proto == "" {
+				// chunked, so that a handler replacing the body doesn't have to fix the length
+				_, _ = io.WriteString(w, "plain")
+				_ = http.NewResponseController(w).Flush()
+				return
+			}
+			conn, brw, err := http.NewResponseController(w).Hijack()
+			if err != nil {
+				t.Errorf("backend hijack: %v", err)
+				return
+			}
+			defer conn.Close()
+			_, _ = fmt.Fprintf(brw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", proto)
+			if brw.Flush() != nil {
+				return
+			}
+			_, _ = io.Copy(conn, brw.Reader)
+		}),
+	}}
+	go b.srv.Serve(ln)
+	t.Cleanup(func() { b.srv.Close() })
+	return b
+}
+
+// streamClient is a client holding an upgraded connection open.
+type streamClient struct {
+	t    *testing.T
+	conn net.Conn
+	br   *bufio.Reader
+	resp *http.Response
+}
+
+// upgradeStream performs an upgrade request for proto at path against the
+// HTTP server on port 9080 and returns the client once the 101 is received.
+func upgradeStream(t *testing.T, path, proto string, headers ...string) *streamClient {
+	t.Helper()
+
+	conn, err := net.DialTimeout("tcp", "127.0.0.1:9080", 5*time.Second)
+	if err != nil {
+		t.Fatalf("dial: %v", err)
+	}
+	t.Cleanup(func() { conn.Close() })
+	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
+
+	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: localhost:9080\r\nConnection: Upgrade\r\nUpgrade: %s\r\n", path, proto)
+	for _, h := range headers {
+		req += h + "\r\n"
+	}
+	if _, err := io.WriteString(conn, req+"\r\n"); err != nil {
+		t.Fatalf("writing upgrade request: %v", err)
+	}
+
+	c := &streamClient{t: t, conn: conn, br: bufio.NewReader(conn)}
+	c.resp, err = http.ReadResponse(c.br, &http.Request{Method: http.MethodGet})
+	if err != nil {
+		t.Fatalf("reading upgrade response: %v", err)
+	}
+	if c.resp.StatusCode != http.StatusSwitchingProtocols {
+		t.Fatalf("upgrade status = %d, want 101", c.resp.StatusCode)
+	}
+	return c
+}
+
+// echo sends msg through the stream and expects the same back.
+func (c *streamClient) echo(msg string) {
+	c.t.Helper()
+	_ = c.conn.SetDeadline(time.Now().Add(10 * time.Second))
+	if _, err := io.WriteString(c.conn, msg); err != nil {
+		c.t.Fatalf("writing to the stream: %v", err)
+	}
+	got := make([]byte, len(msg))
+	if _, err := io.ReadFull(c.br, got); err != nil || string(got) != msg {
+		c.t.Fatalf("stream echoed %q (%v), want %q", got, err, msg)
+	}
+}
+
+// expectNoData checks that nothing else is sent on the stream.
+func (c *streamClient) expectNoData() {
+	c.t.Helper()
+	_ = c.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
+	defer c.conn.SetReadDeadline(time.Time{})
+	if b, err := c.br.ReadByte(); err == nil {
+		c.t.Fatalf("unexpected data on the stream, starting with %q", b)
+	}
+}
+
+// waitClosed waits for the server to close the stream.
+func (c *streamClient) waitClosed(timeout time.Duration) {
+	c.t.Helper()
+	_ = c.conn.SetReadDeadline(time.Now().Add(timeout))
+	_, err := io.Copy(io.Discard, c.br)
+	var netErr net.Error
+	if errors.As(err, &netErr) && netErr.Timeout() {
+		c.t.Fatalf("the stream was not closed within %s", timeout)
+	}
+}
+
+// closeGracefully ends the stream from the client side and waits
+// for the server to finish with it.
+func (c *streamClient) closeGracefully() {
+	c.t.Helper()
+	if tcp, ok := c.conn.(*net.TCPConn); ok {
+		_ = tcp.CloseWrite()
+	}
+	c.waitClosed(10 * time.Second)
+}
+
+// streamConfig describes a server proxying upgrades, for the tests.
+type streamConfig struct {
+	routes      [][2]string // path prefix and upstream dial address
+	detached    bool
+	closeDelay  string
+	streamLogs  map[string]any
+	handlers    []any  // handlers in front of the reverse proxy
+	accessLog   string // file the access log is written to, if any
+	accessLevel string // level of the access log, DEBUG if empty
+}
+
+func (c streamConfig) String() string {
+	var routes []any
+	for _, r := range c.routes {
+		proxy := map[string]any{
+			"handler":   "reverse_proxy",
+			"upstreams": []any{map[string]any{"dial": r[1]}},
+		}
+		if c.detached {
+			proxy["stream_detached"] = true
+		}
+		if c.closeDelay != "" {
+			proxy["stream_close_delay"] = c.closeDelay
+		}
+		if c.streamLogs != nil {
+			proxy["stream_logs"] = c.streamLogs
+		}
+		routes = append(routes, map[string]any{
+			"match":  []any{map[string]any{"path": []any{r[0] + "*"}}},
+			"handle": append(append([]any{}, c.handlers...), proxy),
+		})
+	}
+
+	server := map[string]any{
+		"listen":          []any{":9080"},
+		"automatic_https": map[string]any{"disable": true},
+		"routes":          routes,
+	}
+	cfg := map[string]any{
+		"admin": map[string]any{"listen": "localhost:2999"},
+		"apps": map[string]any{
+			"http": map[string]any{
+				"http_port":    9080,
+				"https_port":   9443,
+				"grace_period": 1,
+				"servers":      map[string]any{"srv0": server},
+			},
+		},
+	}
+	if c.accessLog != "" {
+		level := c.accessLevel
+		if level == "" {
+			level = "DEBUG"
+		}
+		server["logs"] = map[string]any{"default_logger_name": "access"}
+		cfg["logging"] = map[string]any{"logs": map[string]any{
+			"default": map[string]any{"exclude": []any{"http.log.access.access"}},
+			"access": map[string]any{
+				"writer":  map[string]any{"output": "file", "filename": c.accessLog},
+				"encoder": map[string]any{"format": "json"},
+				"level":   level,
+				"include": []any{"http.log.access.access"},
+			},
+		}}
+	}
+
+	out, err := json.Marshal(cfg)
+	if err != nil {
+		panic(err)
+	}
+	return string(out)
+}
+
+func loadStreamConfig(t *testing.T, tester *caddytest.Tester, c streamConfig) {
+	t.Helper()
+	tester.InitServer(c.String(), "json")
+}
+
+// TestStreamDetachedSurvivesReload checks that a detached stream stays open
+// and keeps working when a reload keeps its upstream.
+func TestStreamDetachedSurvivesReload(t *testing.T) {
+	tester := caddytest.NewTester(t)
+	startStreamBackend(t, streamAddrA)
+
+	cfg := streamConfig{routes: [][2]string{{"/a/", streamAddrA}}, detached: true}
+	loadStreamConfig(t, tester, cfg)
+
+	stream := upgradeStream(t, "/a/ws", "echo")
+	stream.echo("before the reload")
+
+	// reload with a different config that keeps the upstream
+	cfg.routes = append(cfg.routes, [2]string{"/b/", streamAddrB})
+	loadStreamConfig(t, tester, cfg)
+	stream.echo("after the reload")
+
+	// and once more
+	cfg.closeDelay = "1s"
+	loadStreamConfig(t, tester, cfg)
+	stream.echo("after another reload")
+
+	// new streams keep working, too
+	upgradeStream(t, "/a/ws", "echo").echo("new stream")
+	stream.closeGracefully()
+}
+
+// TestStreamDetachedClosesRemovedUpstream checks that a reload closes only the
+// detached streams whose upstream is gone from the config.
+func TestStreamDetachedClosesRemovedUpstream(t *testing.T) {
+	tester := caddytest.NewTester(t)
+	startStreamBackend(t, streamAddrA)
+	startStreamBackend(t, streamAddrB)
+
+	cfg := streamConfig{
+		routes:   [][2]string{{"/a/", streamAddrA}, {"/b/", streamAddrB}},
+		detached: true,
+	}
+	loadStreamConfig(t, tester, cfg)
+
+	removed := upgradeStream(t, "/a/ws", "websocket", "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==", "Sec-WebSocket-Version: 13")
+	retained := upgradeStream(t, "/b/ws", "echo")
+	removed.echo("a")
+	retained.echo("b")
+
+	// reload without the first upstream
+	cfg.routes = cfg.routes[1:]
+	loadStreamConfig(t, tester, cfg)
+
+	removed.waitClosed(10 * time.Second)
+	retained.echo("b is still there")
+	retained.echo("and again")
+
+	// the removed stream stays closed: the connection is really gone, not just idle
+	if _, err := io.WriteString(removed.conn, "anyone there?"); err == nil {
+		_ = removed.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
+		if n, _ := removed.br.Read(make([]byte, 1)); n != 0 {
+			t.Error("data received on a stream closed by the removal of its upstream")
+		}
+	}
+
+	// a further reload that keeps the retained upstream doesn't close its stream
+	cfg.closeDelay = "1s"
+	loadStreamConfig(t, tester, cfg)
+	retained.echo("b survives the next reload too")
+
+	tester.AssertGetResponse("http://localhost:9080/b/plain", http.StatusOK, "plain")
+	retained.closeGracefully()
+}
+
+// TestStreamDefaultClosedOnReload checks that without stream_detached the
+// stream is closed on reload, even if the upstream is retained, after the
+// stream_close_delay if there is one.
+func TestStreamDefaultClosedOnReload(t *testing.T) {
+	for _, tc := range []struct {
+		name  string
+		delay string
+	}{
+		{name: "no delay"},
+		{name: "delay", delay: "3s"},
+	} {
+		t.Run(tc.name, func(t *testing.T) {
+			tester := caddytest.NewTester(t)
+			startStreamBackend(t, streamAddrA)
+
+			cfg := streamConfig{routes: [][2]string{{"/a/", streamAddrA}}, closeDelay: tc.delay}
+			loadStreamConfig(t, tester, cfg)
+
+			stream := upgradeStream(t, "/a/ws", "echo")
+			stream.echo("before the reload")
+
+			// a reload that keeps everything but a header
+			cfg.handlers = []any{map[string]any{
+				"handler":  "headers",
+				"response": map[string]any{"set": map[string][]string{"X-Reload": {"1"}}},
+			}}
+			start := time.Now()
+			loadStreamConfig(t, tester, cfg)
+
+			if tc.delay == "" {
+				stream.waitClosed(10 * time.Second)
+				return
+			}
+
+			// the stream is still open during the delay, and closed after it
+			stream.echo("during the delay")
+			stream.waitClosed(15 * time.Second)
+			if elapsed := time.Since(start); elapsed < 2*time.Second {
+				t.Errorf("the stream was closed %s after the reload started, want it to be delayed by 3s", elapsed)
+			}
+		})
+	}
+}
+
+// TestStreamDetachedHTTP11Upgrade checks HTTP/1.1 upgrades in detached mode,
+// WebSocket or not, and what the client receives.
+func TestStreamDetachedHTTP11Upgrade(t *testing.T) {
+	tester := caddytest.NewTester(t)
+	startStreamBackend(t, streamAddrA)
+	loadStreamConfig(t, tester, streamConfig{routes: [][2]string{{"/a/", streamAddrA}}, detached: true})
+
+	for _, tc := range []struct {
+		proto   string
+		headers []string
+	}{
+		{proto: "echo"},
+		{proto: "websocket", headers: []string{"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==", "Sec-WebSocket-Version: 13"}},
+	} {
+		stream := upgradeStream(t, "/a/ws", tc.proto, tc.headers...)
+		if got := stream.resp.Header.Get("Upgrade"); !strings.EqualFold(got, tc.proto) {
+			t.Errorf("%s: Upgrade = %q in the 101 response", tc.proto, got)
+		}
+		if got := stream.resp.Header.Get("Connection"); !strings.EqualFold(got, "Upgrade") {
+			t.Errorf("%s: Connection = %q in the 101 response", tc.proto, got)
+		}
+		stream.echo("one")
+		stream.echo("two")
+		stream.expectNoData()
+		stream.closeGracefully()
+	}
+
+	// regular requests are not affected
+	tester.AssertGetResponse("http://localhost:9080/a/plain", http.StatusOK, "plain")
+}
+
+// logEntries returns the entries of a JSON log file.
+func logEntries(t *testing.T, path string) []map[string]any {
+	t.Helper()
+	data, err := os.ReadFile(path)
+	if err != nil {
+		return nil
+	}
+	var entries []map[string]any
+	for line := range bytes.SplitSeq(data, []byte("\n")) {
+		if len(line) == 0 {
+			continue
+		}
+		var entry map[string]any
+		if err := json.Unmarshal(line, &entry); err != nil {
+			continue
+		}
+		entries = append(entries, entry)
+	}
+	return entries
+}
+
+// waitForLogEntries waits for at least n entries with the given message,
+// and returns the entries logged so far.
+func waitForLogEntries(t *testing.T, path, msg string, n int) []map[string]any {
+	t.Helper()
+	deadline := time.Now().Add(15 * time.Second)
+	for {
+		entries := logEntries(t, path)
+		if len(entriesWithMessage(entries, msg)) >= n {
+			return entries
+		}
+		if time.Now().After(deadline) {
+			t.Fatalf("fewer than %d %q entries were logged, got %v", n, msg, entries)
+		}
+		time.Sleep(50 * time.Millisecond)
+	}
+}
+
+// waitForLogEntry waits for an entry with the given message, and returns
+// the entries logged so far.
+func waitForLogEntry(t *testing.T, path, msg string) []map[string]any {
+	t.Helper()
+	return waitForLogEntries(t, path, msg, 1)
+}
+
+func entriesWithMessage(entries []map[string]any, msg string) []map[string]any {
+	var out []map[string]any
+	for _, e := range entries {
+		if e["msg"] == msg {
+			out = append(out, e)
+		}
+	}
+	return out
+}
+
+// TestStreamLogsSkipHandshake checks that the access log entry of the upgrade
+// handshake is logged by default, and left out with skip_handshake.
+func TestStreamLogsSkipHandshake(t *testing.T) {
+	for _, tc := range []struct {
+		name          string
+		streamLogs    map[string]any
+		wantHandshake int
+	}{
+		{name: "default", wantHandshake: 1},
+		{name: "logs configured", streamLogs: map[string]any{"level": "debug"}, wantHandshake: 1},
+		{name: "skip handshake", streamLogs: map[string]any{"skip_handshake": true}, wantHandshake: 0},
+	} {
+		t.Run(tc.name, func(t *testing.T) {
+			tester := caddytest.NewTester(t)
+			startStreamBackend(t, streamAddrA)
+
+			logPath := filepath.Join(t.TempDir(), "access.log")
+			loadStreamConfig(t, tester, streamConfig{
+				routes:     [][2]string{{"/a/", streamAddrA}},
+				streamLogs: tc.streamLogs,
+				accessLog:  logPath,
+			})
+
+			stream := upgradeStream(t, "/a/ws", "echo")
+			stream.echo("hello")
+			stream.closeGracefully()
+			waitForLogEntry(t, logPath, "stream closed")
+
+			// a request made after the stream is done, to know all of its entries are in
+			tester.AssertGetResponse("http://localhost:9080/a/plain", http.StatusOK, "plain")
+			entries := waitForLogEntries(t, logPath, "handled request", tc.wantHandshake+1)
+
+			var handshakes int
+			for _, e := range entriesWithMessage(entries, "handled request") {
+				if status, _ := e["status"].(float64); status == http.StatusSwitchingProtocols {
+					handshakes++
+				}
+			}
+			if handshakes != tc.wantHandshake {
+				t.Errorf("%d handshake entries in the access log, want %d; entries: %v", handshakes, tc.wantHandshake, entries)
+			}
+			if closed := entriesWithMessage(entries, "stream closed"); len(closed) != 1 {
+				t.Errorf("%d stream close entries, want 1", len(closed))
+			}
+		})
+	}
+}
+
+// TestStreamCloseLogAccessLogger checks that the stream close entry is
+// written by the access logger of the request, at the configured level.
+func TestStreamCloseLogAccessLogger(t *testing.T) {
+	for _, tc := range []struct {
+		name        string
+		streamLogs  map[string]any
+		accessLevel string
+		wantLevel   string // empty means no entry is logged
+	}{
+		{name: "default level", accessLevel: "DEBUG", wantLevel: "debug"},
+		{name: "default level is not logged at info", accessLevel: "INFO"},
+		{name: "configured level", streamLogs: map[string]any{"level": "info"}, accessLevel: "INFO", wantLevel: "info"},
+	} {
+		t.Run(tc.name, func(t *testing.T) {
+			tester := caddytest.NewTester(t)
+			startStreamBackend(t, streamAddrA)
+
+			logPath := filepath.Join(t.TempDir(), "access.log")
+			loadStreamConfig(t, tester, streamConfig{
+				routes:      [][2]string{{"/a/", streamAddrA}},
+				streamLogs:  tc.streamLogs,
+				accessLog:   logPath,
+				accessLevel: tc.accessLevel,
+			})
+
+			stream := upgradeStream(t, "/a/ws", "echo")
+			stream.echo("hello")
+			stream.echo("world!")
+			stream.closeGracefully()
+
+			if tc.wantLevel == "" {
+				// the handshake entry comes right after the stream close one
+				waitForLogEntry(t, logPath, "handled request")
+				if closed := entriesWithMessage(logEntries(t, logPath), "stream closed"); len(closed) != 0 {
+					t.Errorf("unexpected stream close entries: %v", closed)
+				}
+				return
+			}
+
+			closed := entriesWithMessage(waitForLogEntry(t, logPath, "stream closed"), "stream closed")
+			if len(closed) != 1 {
+				t.Fatalf("%d stream close entries, want 1", len(closed))
+			}
+			e := closed[0]
+			if e["level"] != tc.wantLevel {
+				t.Errorf("level = %v, want %s", e["level"], tc.wantLevel)
+			}
+			if e["logger"] != "http.log.access.access" {
+				t.Errorf("logger = %v, want http.log.access.access", e["logger"])
+			}
+			if e["upstream"] != streamAddrA {
+				t.Errorf("upstream = %v, want %s", e["upstream"], streamAddrA)
+			}
+			if e["result"] != "closed" {
+				t.Errorf("result = %v, want closed", e["result"])
+			}
+			if e["bytes_to_backend"] != float64(len("helloworld!")) || e["bytes_from_backend"] != float64(len("helloworld!")) {
+				t.Errorf("bytes to/from backend = %v/%v, want %d", e["bytes_to_backend"], e["bytes_from_backend"], len("helloworld!"))
+			}
+			if _, ok := e["duration"]; !ok {
+				t.Errorf("no duration in %v", e)
+			}
+		})
+	}
+}
+
+// TestStreamLogsInvalidLevel checks that an invalid stream log level is refused.
+func TestStreamLogsInvalidLevel(t *testing.T) {
+	caddytest.AssertLoadError(t, streamConfig{
+		routes:     [][2]string{{"/a/", streamAddrA}},
+		streamLogs: map[string]any{"level": "bogus"},
+	}.String(), "json", "invalid level")
+}
+
+// TestUpgradeThroughEncodeAndIntercept checks that upgrades work through the
+// other handlers of a route, with access logging.
+func TestUpgradeThroughEncodeAndIntercept(t *testing.T) {
+	tester := caddytest.NewTester(t)
+	startStreamBackend(t, streamAddrA)
+
+	logPath := filepath.Join(t.TempDir(), "access.log")
+	loadStreamConfig(t, tester, streamConfig{
+		routes: [][2]string{{"/a/", streamAddrA}},
+		handlers: []any{
+			map[string]any{"handler": "encode", "encodings": map[string]any{"gzip": map[string]any{}}},
+			map[string]any{
+				"handler": "intercept",
+				"handle_response": []any{map[string]any{
+					"routes": []any{map[string]any{"handle": []any{
+						map[string]any{"handler": "static_response", "body": "intercepted"},
+					}}},
+				}},
+			},
+		},
+		streamLogs: map[string]any{"level": "debug"},
+		accessLog:  logPath,
+	})
+
+	// the intercept handler is in effect for regular responses
+	tester.AssertGetResponse("http://localhost:9080/a/plain", http.StatusOK, "intercepted")
+
+	for _, tc := range []struct {
+		name    string
+		proto   string
+		headers []string
+	}{
+		{name: "encoded", proto: "echo", headers: []string{"Accept-Encoding: gzip"}},
+		{
+			name:  "websocket",
+			proto: "websocket",
+			headers: []string{
+				"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==", "Sec-WebSocket-Version: 13",
+				"Accept-Encoding: gzip, br",
+			},
+		},
+	} {
+		t.Run(tc.name, func(t *testing.T) {
+			stream := upgradeStream(t, "/a/ws", tc.proto, tc.headers...)
+			if enc := stream.resp.Header.Get("Content-Encoding"); enc != "" {
+				t.Errorf("Content-Encoding = %q on the 101 response", enc)
+			}
+			stream.echo("one")
+			stream.echo("two")
+			stream.expectNoData()
+			stream.closeGracefully()
+		})
+	}
+
+	// the handshakes are access logged, as 101
+	waitForLogEntries(t, logPath, "stream closed", 2)
+	entries := waitForLogEntries(t, logPath, "handled request", 3)
+	var handshakes int
+	for _, e := range entriesWithMessage(entries, "handled request") {
+		if status, _ := e["status"].(float64); status == http.StatusSwitchingProtocols {
+			handshakes++
+		}
+	}
+	if handshakes != 2 {
+		t.Errorf("%d handshake entries in the access log, want 2; entries: %v", handshakes, entries)
+	}
+}
+
+// TestExtendedConnectH2 checks WebSockets over HTTP/2 (extended CONNECT,
+// RFC 8441), proxied to a backend that upgrades with HTTP/1.1.
+func TestExtendedConnectH2(t *testing.T) {
+	// The HTTP/2 server only supports extended CONNECT if GODEBUG has
+	// http2xconnect=1 when the process starts. So the test runs itself in a
+	// process like that, with its own server on other ports than the others.
+	if os.Getenv(extendedConnectChildEnv) == "" {
+		if testing.Short() {
+			t.SkipNow()
+		}
+		godebug := "http2xconnect=1"
+		if existing := os.Getenv("GODEBUG"); existing != "" {
+			godebug = existing + "," + godebug
+		}
+		// (os.Args is not reliable, caddytest overwrites it to start the server)
+		testBinary, err := os.Executable()
+		if err != nil {
+			t.Fatalf("finding the test binary: %v", err)
+		}
+		// the ports of the previous test's servers may take a moment to be
+		// released, so a failure is retried once
+		var out []byte
+		for attempt := 0; attempt < 2; attempt++ {
+			cmd := exec.Command(testBinary, "-test.run=^TestExtendedConnectH2$", "-test.count=1", "-test.v", "-test.timeout=2m") //nolint:gosec
+			cmd.Env = append(os.Environ(), "GODEBUG="+godebug, extendedConnectChildEnv+"=1")
+			if out, err = cmd.CombinedOutput(); err == nil {
+				return
+			}
+			time.Sleep(time.Second)
+		}
+		t.Fatalf("extended CONNECT test failed: %v\n%s", err, out)
+		return
+	}
+
+	tester := caddytest.NewTester(t).WithDefaultOverrides(caddytest.Config{AdminPort: 2998})
+	startStreamBackend(t, streamAddrA)
+
+	// a TLS server, like in the other HTTP/2 tests
+	tester.InitServer(`
+{
+	"admin": {"listen": "localhost:2998"},
+	"apps": {
+		"http": {
+			"http_port": 9082,
+			"https_port": 9444,
+			"grace_period": 1,
+			"servers": {
+				"srv0": {
+					"listen": [":9444"],
+					"automatic_https": {"disable": true},
+					"routes": [{
+						"handle": [{
+							"handler": "reverse_proxy",
+							"upstreams": [{"dial": "`+streamAddrA+`"}]
+						}]
+					}],
+					"tls_connection_policies": [{
+						"certificate_selection": {"any_tag": ["cert0"]},
+						"default_sni": "a.caddy.localhost"
+					}]
+				}
+			}
+		},
+		"tls": {
+			"certificates": {
+				"load_files": [{
+					"certificate": "/a.caddy.localhost.crt",
+					"key": "/a.caddy.localhost.key",
+					"tags": ["cert0"]
+				}]
+			}
+		},
+		"pki": {
+			"certificate_authorities": {
+				"local": {"install_trust": false}
+			}
+		}
+	}
+}`, "json")
+
+	// The extended CONNECT request is written with frames, since the
+	// HTTP/2 clients of the standard libraries don't send the :protocol
+	// pseudo header.
+	conn, err := tls.Dial("tcp", "127.0.0.1:9444",&tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2"}}) //nolint:gosec
+	if err != nil {
+		t.Fatalf("dial: %v", err)
+	}
+	defer conn.Close()
+	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
+	if proto := conn.ConnectionState().NegotiatedProtocol; proto != "h2" {
+		t.Fatalf("negotiated protocol = %q, want h2", proto)
+	}
+
+	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
+		t.Fatalf("writing the client preface: %v", err)
+	}
+	fr := http2.NewFramer(conn, conn)
+	if err := fr.WriteSettings(); err != nil {
+		t.Fatalf("writing settings: %v", err)
+	}
+
+	// the server has to announce it supports extended CONNECT (RFC 8441)
+	const settingEnableConnectProtocol = http2.SettingID(0x8)
+	for announced := false; !announced; {
+		f, err := fr.ReadFrame()
+		if err != nil {
+			t.Fatalf("reading the server settings: %v", err)
+		}
+		sf, ok := f.(*http2.SettingsFrame)
+		if !ok || sf.IsAck() {
+			continue
+		}
+		announced = true
+		if v, ok := sf.Value(settingEnableConnectProtocol); !ok || v != 1 {
+			t.Fatalf("the server doesn't announce support for extended CONNECT: %v", sf)
+		}
+		if err := fr.WriteSettingsAck(); err != nil {
+			t.Fatalf("acknowledging settings: %v", err)
+		}
+	}
+
+	var block bytes.Buffer
+	enc := hpack.NewEncoder(&block)
+	for _, field := range []hpack.HeaderField{
+		{Name: ":method", Value: http.MethodConnect},
+		{Name: ":protocol", Value: "websocket"},
+		{Name: ":scheme", Value: "https"},
+		{Name: ":path", Value: "/ws"},
+		{Name: ":authority", Value: "127.0.0.1:9444"},
+		{Name: "sec-websocket-version", Value: "13"},
+	} {
+		if err := enc.WriteField(field); err != nil {
+			t.Fatalf("encoding %s: %v", field.Name, err)
+		}
+	}
+	const streamID = 1
+	if err := fr.WriteHeaders(http2.HeadersFrameParam{StreamID: streamID, BlockFragment: block.Bytes(), EndHeaders: true}); err != nil {
+		t.Fatalf("writing the CONNECT request: %v", err)
+	}
+
+	var (
+		status string
+		data   bytes.Buffer
+		dec    = hpack.NewDecoder(4096, nil)
+	)
+	// reads until the response status is known and want bytes of data were received
+	readStream := func(want int) {
+		t.Helper()
+		for status == "" || data.Len() < want {
+			f, err := fr.ReadFrame()
+			if err != nil {
+				t.Fatalf("reading from the stream: %v", err)
+			}
+			switch f := f.(type) {
+			case *http2.HeadersFrame:
+				fields, err := dec.DecodeFull(f.HeaderBlockFragment())
+				if err != nil {
+					t.Fatalf("decoding response headers: %v", err)
+				}
+				for _, field := range fields {
+					if field.Name == ":status" {
+						status = field.Value
+					}
+				}
+			case *http2.DataFrame:
+				data.Write(f.Data())
+			case *http2.SettingsFrame:
+				if !f.IsAck() {
+					_ = fr.WriteSettingsAck()
+				}
+			case *http2.RSTStreamFrame:
+				t.Fatalf("stream reset by the server: %v", f.ErrCode)
+			case *http2.GoAwayFrame:
+				t.Fatalf("connection closed by the server: %v", f.ErrCode)
+			}
+		}
+	}
+
+	readStream(0)
+	if status != "200" {
+		t.Fatalf("status = %q, want 200", status)
+	}
+	for _, msg := range []string{"hello over h2", "and again"} {
+		if err := fr.WriteData(streamID, false, []byte(msg)); err != nil {
+			t.Fatalf("writing to the stream: %v", err)
+		}
+		readStream(len(msg))
+		if got := data.String(); got != msg {
+			t.Fatalf("stream echoed %q, want %q", got, msg)
+		}
+		data.Reset()
+	}
+}
diff --git a/modules/caddyhttp/encode/encode.go b/modules/caddyhttp/encode/encode.go
index 46b383ea..29384ad8 100644
--- a/modules/caddyhttp/encode/encode.go
+++ b/modules/caddyhttp/encode/encode.go
@@ -20,10 +20,12 @@
 package encode
 
 import (
+	"bufio"
 	"cmp"
 	"fmt"
 	"io"
 	"math"
+	"net"
 	"net/http"
 	"slices"
 	"strconv"
@@ -289,8 +291,10 @@ func (rw *responseWriter) WriteHeader(status int) {
 
 	// write status immediately when status code is informational
 	// see: https://caddy.community/t/disappear-103-early-hints-response-with-encode-enable-caddy-v2-7-6/23081/5
+	// a 101 is the final response of an upgrade, nothing else is written after it
 	if 100 <= status && status <= 199 {
 		rw.ResponseWriter.WriteHeader(status)
+		rw.wroteHeader = rw.wroteHeader || status == http.StatusSwitchingProtocols
 	}
 
 	// write header immediately for server-sent events responses and for
@@ -499,6 +503,15 @@ func (rw *responseWriter) Close() error {
 	return err
 }
 
+// Hijack hijacks the underlying connection. Afterwards, the response
+// writer underneath drops everything written to it, so neither the
+// encoder nor Close can write to the hijacked connection.
+func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	conn, brw, guarded, err := caddyhttp.HijackResponse(rw.ResponseWriter)
+	rw.ResponseWriter = guarded
+	return conn, brw, err
+}
+
 // Unwrap returns the underlying ResponseWriter.
 func (rw *responseWriter) Unwrap() http.ResponseWriter {
 	return rw.ResponseWriter
diff --git a/modules/caddyhttp/hijack_test.go b/modules/caddyhttp/hijack_test.go
new file mode 100644
index 00000000..a8e05ff2
--- /dev/null
+++ b/modules/caddyhttp/hijack_test.go
@@ -0,0 +1,251 @@
+package caddyhttp
+
+import (
+	"bufio"
+	"bytes"
+	"errors"
+	"io"
+	"net"
+	"net/http"
+	"net/http/httptest"
+	"strings"
+	"testing"
+	"time"
+
+	"go.uber.org/zap"
+	"go.uber.org/zap/zapcore"
+	"go.uber.org/zap/zaptest/observer"
+)
+
+// After a hijack, nothing may be written to the response again: the 101 is
+// written exactly once, and the recorder drops everything that follows.
+func TestResponseRecorderNoWriteAfterHijack(t *testing.T) {
+	type outcome struct {
+		status          int
+		buffered        bool
+		writeErr        error
+		readFromErr     error
+		flushErr        error
+		writeResponse   error
+		headerReadable  bool
+		hijackAgainFail bool
+	}
+	outc := make(chan outcome, 1)
+
+	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
+		// a response that would be buffered, if it were not hijacked
+		rec := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool { return true })
+
+		rec.Header().Set("Upgrade", "echo")
+		rec.Header().Set("Connection", "Upgrade")
+		rec.WriteHeader(http.StatusSwitchingProtocols)
+
+		conn, brw, err := http.NewResponseController(rec).Hijack()
+		if err != nil {
+			t.Errorf("hijack failed: %v", err)
+			return
+		}
+		defer conn.Close()
+
+		// everything below must be ignored
+		rec.WriteHeader(http.StatusOK)
+		rec.WriteHeader(http.StatusInternalServerError)
+		var o outcome
+		_, o.writeErr = rec.Write([]byte("must not be written"))
+		_, o.readFromErr = rec.(io.ReaderFrom).ReadFrom(strings.NewReader("must not be written"))
+		o.flushErr = http.NewResponseController(rec).Flush()
+		o.writeResponse = rec.WriteResponse()
+		o.status = rec.Status()
+		o.buffered = rec.Buffered()
+		o.headerReadable = rec.Header().Get("Upgrade") == "echo"
+		_, _, hijackAgainErr := http.NewResponseController(rec).Hijack()
+		o.hijackAgainFail = hijackAgainErr != nil
+		outc <- o
+
+		// the raw connection is what carries the data from now on
+		_, _ = brw.WriteString("raw")
+		_ = brw.Flush()
+	}))
+	defer srv.Close()
+
+	cli, err := net.Dial("tcp", srv.Listener.Addr().String())
+	if err != nil {
+		t.Fatalf("dial failed: %v", err)
+	}
+	defer cli.Close()
+	_ = cli.SetDeadline(time.Now().Add(10 * time.Second))
+
+	if _, err := io.WriteString(cli, "GET / HTTP/1.1\r\nHost: test\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n"); err != nil {
+		t.Fatalf("writing request failed: %v", err)
+	}
+	br := bufio.NewReader(cli)
+	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
+	if err != nil {
+		t.Fatalf("reading response failed: %v", err)
+	}
+	if resp.StatusCode != http.StatusSwitchingProtocols {
+		t.Fatalf("status = %d, want 101", resp.StatusCode)
+	}
+
+	// the only thing after the 101 is what was written to the raw connection
+	rest, _ := io.ReadAll(br)
+	if string(rest) != "raw" {
+		t.Errorf("data after the 101 = %q, want %q", rest, "raw")
+	}
+
+	select {
+	case o := <-outc:
+		if o.status != http.StatusSwitchingProtocols {
+			t.Errorf("recorded status = %d, want 101", o.status)
+		}
+		if o.buffered {
+			t.Error("a 101 must not be buffered")
+		}
+		for name, err := range map[string]error{
+			"Write":         o.writeErr,
+			"ReadFrom":      o.readFromErr,
+			"Flush":         o.flushErr,
+			"WriteResponse": o.writeResponse,
+		} {
+			if !errors.Is(err, http.ErrHijacked) {
+				t.Errorf("%s error = %v, want %v", name, err, http.ErrHijacked)
+			}
+		}
+		if !o.headerReadable {
+			t.Error("response headers are not readable after the hijack")
+		}
+		if !o.hijackAgainFail {
+			t.Error("hijacking twice must fail")
+		}
+	case <-time.After(5 * time.Second):
+		t.Fatal("handler did not finish")
+	}
+}
+
+// A 101 is the final response: later status codes don't replace it, and it is
+// never buffered even if the buffering function asks for it.
+func TestResponseRecorderSwitchingProtocolsIsFinal(t *testing.T) {
+	w := httptest.NewRecorder()
+	called := false
+	rec := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool {
+		called = true
+		return true
+	})
+
+	rec.WriteHeader(http.StatusSwitchingProtocols)
+	rec.WriteHeader(http.StatusNotFound)
+
+	if rec.Status() != http.StatusSwitchingProtocols {
+		t.Errorf("status = %d, want 101", rec.Status())
+	}
+	if w.Code != http.StatusSwitchingProtocols {
+		t.Errorf("underlying status = %d, want 101", w.Code)
+	}
+	if rec.Buffered() {
+		t.Error("a 101 must not be buffered")
+	}
+	if called {
+		t.Error("the buffering function must not decide about a 101")
+	}
+}
+
+// When the connection can't be hijacked the recorder keeps working normally.
+func TestResponseRecorderHijackFailure(t *testing.T) {
+	w := httptest.NewRecorder()
+	rec := NewResponseRecorder(w, nil, nil)
+
+	if _, _, err := http.NewResponseController(rec).Hijack(); err == nil {
+		t.Fatal("hijacking a recorder over a non-hijackable writer should fail")
+	}
+
+	rec.WriteHeader(http.StatusTeapot)
+	if _, err := rec.Write([]byte("short and stout")); err != nil {
+		t.Fatalf("write failed after a failed hijack: %v", err)
+	}
+	if w.Code != http.StatusTeapot || w.Body.String() != "short and stout" {
+		t.Errorf("got %d %q, want 418 %q", w.Code, w.Body.String(), "short and stout")
+	}
+}
+
+func TestHijackedResponseWriter(t *testing.T) {
+	header := http.Header{"X-Test": []string{"1"}}
+	w := hijackedResponseWriter{header: header}
+
+	if w.Header().Get("X-Test") != "1" {
+		t.Error("header map is not kept")
+	}
+	w.WriteHeader(http.StatusOK)
+	if _, err := w.Write([]byte("x")); !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("Write error = %v, want %v", err, http.ErrHijacked)
+	}
+	if err := w.FlushError(); !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("FlushError error = %v, want %v", err, http.ErrHijacked)
+	}
+	if _, err := w.ReadFrom(strings.NewReader("x")); !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("ReadFrom error = %v, want %v", err, http.ErrHijacked)
+	}
+}
+
+func TestServerAccessLoggers(t *testing.T) {
+	newReq := func(host string) *http.Request {
+		r := httptest.NewRequest(http.MethodGet, "http://"+host+"/", nil)
+		r.Host = host
+		return r
+	}
+
+	core, logs := observer.New(zapcore.DebugLevel)
+	base := zap.New(core).Named("access")
+
+	for i, tc := range []struct {
+		name       string
+		server     *Server
+		host       string
+		wantLogger []string // nil means not logged
+	}{
+		{
+			name:   "no access logger",
+			server: &Server{Logs: &ServerLogConfig{}},
+			host:   "example.com",
+		},
+		{
+			name:   "no log config",
+			server: &Server{accessLogger: base},
+			host:   "example.com",
+		},
+		{
+			name:       "default logger",
+			server:     &Server{accessLogger: base, Logs: &ServerLogConfig{}},
+			host:       "example.com:8080",
+			wantLogger: []string{"access"},
+		},
+		{
+			name: "named loggers",
+			server: &Server{accessLogger: base, Logs: &ServerLogConfig{
+				LoggerNames: map[string]StringArray{"example.com": {"one", "two"}},
+			}},
+			host:       "example.com",
+			wantLogger: []string{"access.one", "access.two"},
+		},
+		{
+			name: "skipped host",
+			server: &Server{accessLogger: base, Logs: &ServerLogConfig{
+				SkipHosts: []string{"example.com"},
+			}},
+			host: "example.com",
+		},
+	} {
+		loggers := tc.server.AccessLoggers(newReq(tc.host))
+		if len(loggers) != len(tc.wantLogger) {
+			t.Errorf("Test %d (%s): got %d loggers, want %d", i, tc.name, len(loggers), len(tc.wantLogger))
+			continue
+		}
+		for j, l := range loggers {
+			logs.TakeAll()
+			l.Info("entry")
+			entries := logs.TakeAll()
+			if len(entries) != 1 || entries[0].LoggerName != tc.wantLogger[j] {
+				t.Errorf("Test %d (%s): logger %d wrote %+v, want logger name %q", i, tc.name, j, entries, tc.wantLogger[j])
+			}
+		}
+	}
+}
diff --git a/modules/caddyhttp/responsewriter.go b/modules/caddyhttp/responsewriter.go
index 904c30c0..2fbc0d3e 100644
--- a/modules/caddyhttp/responsewriter.go
+++ b/modules/caddyhttp/responsewriter.go
@@ -62,6 +62,46 @@ func (rww *ResponseWriterWrapper) Unwrap() http.ResponseWriter {
 // ResponseWriter does not implement the required method.
 var ErrNotImplemented = fmt.Errorf("method not implemented")
 
+// HijackResponse hijacks the connection behind w. On success it also
+// returns a ResponseWriter that must replace w in any wrapper that keeps
+// writing to it: once a connection is hijacked nothing may write to the
+// response anymore, so the returned writer drops headers and status codes,
+// and fails body writes and flushes with http.ErrHijacked. On failure the
+// returned writer is w itself.
+func HijackResponse(w http.ResponseWriter) (net.Conn, *bufio.ReadWriter, http.ResponseWriter, error) {
+	//nolint:bodyclose
+	conn, brw, err := http.NewResponseController(w).Hijack()
+	if err != nil {
+		return nil, nil, w, err
+	}
+	return conn, brw, hijackedResponseWriter{header: w.Header()}, nil
+}
+
+// hijackedResponseWriter stands in for a ResponseWriter whose connection
+// was hijacked. It keeps the header map so that response headers can still
+// be read (e.g. for logging), but never writes anything.
+type hijackedResponseWriter struct {
+	header http.Header
+}
+
+func (h hijackedResponseWriter) Header() http.Header {
+	return h.header
+}
+
+func (hijackedResponseWriter) WriteHeader(int) {}
+
+func (hijackedResponseWriter) Write([]byte) (int, error) {
+	return 0, http.ErrHijacked
+}
+
+func (hijackedResponseWriter) FlushError() error {
+	return http.ErrHijacked
+}
+
+func (hijackedResponseWriter) ReadFrom(io.Reader) (int64, error) {
+	return 0, http.ErrHijacked
+}
+
 type responseRecorder struct {
 	*ResponseWriterWrapper
 	statusCode   int
@@ -70,6 +110,7 @@ type responseRecorder struct {
 	size         int
 	wroteHeader  bool
 	stream       bool
+	hijacked     bool
 
 	readSize *int
 }
@@ -146,7 +187,8 @@ func NewResponseRecorder(w http.ResponseWriter, buf *bytes.Buffer, shouldBuffer
 // ResponseWriter unless the response is to be buffered instead.
 // 1xx responses are never buffered.
 func (rr *responseRecorder) WriteHeader(statusCode int) {
-	if rr.wroteHeader {
+	// nothing may be written once the connection is hijacked
+	if rr.wroteHeader || rr.hijacked {
 		return
 	}
 
@@ -154,15 +196,17 @@ func (rr *responseRecorder) WriteHeader(statusCode int) {
 	// connections by manually setting headers and writing status 101
 	rr.statusCode = statusCode
 
-	// decide whether we should buffer the response
-	if rr.shouldBuffer == nil {
+	// decide whether we should buffer the response; a 101 is never
+	// buffered since the connection is about to be taken over
+	if rr.shouldBuffer == nil || statusCode == http.StatusSwitchingProtocols {
 		rr.stream = true
 	} else {
 		rr.stream = !rr.shouldBuffer(rr.statusCode, rr.ResponseWriterWrapper.Header())
 	}
 
-	// 1xx responses aren't final; just informational
-	if statusCode < 100 || statusCode > 199 {
+	// other 1xx responses aren't final; just informational,
+	// but a 101 is the final response of an upgrade
+	if statusCode < 100 || statusCode > 199 || statusCode == http.StatusSwitchingProtocols {
 		rr.wroteHeader = true
 	}
 
@@ -173,6 +217,9 @@ func (rr *responseRecorder) WriteHeader(statusCode int) {
 }
 
 func (rr *responseRecorder) Write(data []byte) (int, error) {
+	if rr.hijacked {
+		return 0, http.ErrHijacked
+	}
 	rr.WriteHeader(http.StatusOK)
 	var n int
 	var err error
@@ -187,6 +234,9 @@ func (rr *responseRecorder) Write(data []byte) (int, error) {
 }
 
 func (rr *responseRecorder) ReadFrom(r io.Reader) (int64, error) {
+	if rr.hijacked {
+		return 0, http.ErrHijacked
+	}
 	rr.WriteHeader(http.StatusOK)
 	var n int64
 	var err error
@@ -223,6 +273,9 @@ func (rr *responseRecorder) Buffered() bool {
 }
 
 func (rr *responseRecorder) WriteResponse() error {
+	if rr.hijacked {
+		return http.ErrHijacked
+	}
 	if rr.statusCode == 0 {
 		// could happen if no handlers actually wrote anything,
 		// and this prevents a panic; status must be > 0
@@ -239,6 +292,9 @@ func (rr *responseRecorder) WriteResponse() error {
 // FlushError will suppress actual flushing if the response is buffered. See:
 // https://github.com/caddyserver/caddy/issues/6144
 func (rr *responseRecorder) FlushError() error {
+	if rr.hijacked {
+		return http.ErrHijacked
+	}
 	if rr.stream {
 		//nolint:bodyclose
 		return http.NewResponseController(rr.ResponseWriterWrapper).Flush()
@@ -253,11 +309,13 @@ func (rr *responseRecorder) setReadSize(size *int) {
 }
 
 func (rr *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
-	//nolint:bodyclose
-	conn, brw, err := http.NewResponseController(rr.ResponseWriterWrapper).Hijack()
+	conn, brw, guarded, err := HijackResponse(rr.ResponseWriterWrapper)
 	if err != nil {
 		return nil, nil, err
 	}
+	// from now on, everything the recorder passes down is dropped
+	rr.ResponseWriterWrapper.ResponseWriter = guarded
+	rr.hijacked = true
 	// Per http documentation, returned bufio.Writer is empty, but bufio.Read maybe not
 	conn = &hijackedConn{conn, rr}
 	brw.Writer.Reset(conn)
diff --git a/modules/caddyhttp/reverseproxy/caddyfile.go b/modules/caddyhttp/reverseproxy/caddyfile.go
index 12c1177b..ce1a155f 100644
--- a/modules/caddyhttp/reverseproxy/caddyfile.go
+++ b/modules/caddyhttp/reverseproxy/caddyfile.go
@@ -99,6 +99,11 @@ func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error)
 //	    stream_buffer_size <size>
 //	    stream_timeout     <duration>
 //	    stream_close_delay <duration>
+//	    stream_detached
+//	    stream_logs {
+//	        skip_handshake
+//	        level <level>
+//	    }
 //	    verbose_logs
 //
 //	    # reporting
@@ -715,6 +720,44 @@ func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
 				h.StreamCloseDelay = caddy.Duration(dur)
 			}
 
+		case "stream_detached":
+			if d.NextArg() {
+				return d.ArgErr()
+			}
+			h.StreamDetached = true
+
+		case "stream_logs":
+			if d.NextArg() {
+				return d.ArgErr()
+			}
+			if h.StreamLogs == nil {
+				h.StreamLogs = new(StreamLogs)
+			}
+			for nesting := d.Nesting(); d.NextBlock(nesting); {
+				switch d.Val() {
+				case "skip_handshake":
+					if d.NextArg() {
+						return d.ArgErr()
+					}
+					h.StreamLogs.SkipHandshake = true
+
+				case "level":
+					if !d.NextArg() {
+						return d.ArgErr()
+					}
+					h.StreamLogs.Level = d.Val()
+					if _, err := h.StreamLogs.parseLevel(); err != nil {
+						return d.Errf("%v", err)
+					}
+					if d.NextArg() {
+						return d.ArgErr()
+					}
+
+				default:
+					return d.Errf("unrecognized stream_logs subdirective '%s'", d.Val())
+				}
+			}
+
 		case "trusted_proxies":
 			for d.NextArg() {
 				if d.Val() == "private_ranges" {
diff --git a/modules/caddyhttp/reverseproxy/metrics.go b/modules/caddyhttp/reverseproxy/metrics.go
index 24884273..c574269d 100644
--- a/modules/caddyhttp/reverseproxy/metrics.go
+++ b/modules/caddyhttp/reverseproxy/metrics.go
@@ -42,10 +42,82 @@ func initReverseProxyMetrics(handler *Handler, registry *prometheus.Registry) {
 		}) {
 		panic(err)
 	}
+	streamMetrics.register(registry)
 
 	reverseProxyMetrics.logger = handler.logger.Named("reverse_proxy.metrics")
 }
 
+// streamMetrics holds the metrics of upgraded streams. Unlike the other
+// reverse proxy metrics it exists independently of any registry, since
+// detached streams outlive the config (and registry) they were opened in;
+// it is registered whenever a handler is provisioned.
+var streamMetrics = newStreamMetricsSet()
+
+type streamMetricsSet struct {
+	active   *prometheus.GaugeVec
+	closedBy *prometheus.CounterVec
+	duration *prometheus.HistogramVec
+	bytes    *prometheus.CounterVec
+}
+
+func newStreamMetricsSet() *streamMetricsSet {
+	const ns, sub = "caddy", "reverse_proxy"
+
+	return &streamMetricsSet{
+		active: prometheus.NewGaugeVec(prometheus.GaugeOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "streams_active",
+			Help:      "Number of upgraded streams (e.g. WebSockets) currently open.",
+		}, []string{"upstream"}),
+		closedBy: prometheus.NewCounterVec(prometheus.CounterOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "streams_closed_total",
+			Help:      "Number of upgraded streams that were closed, by result (closed, error, timeout, reload, upstream_removed).",
+		}, []string{"upstream", "result"}),
+		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "stream_duration_seconds",
+			Help:      "How long upgraded streams were open.",
+			Buckets:   []float64{0.1, 1, 10, 60, 300, 1800, 3600, 21600, 86400},
+		}, []string{"upstream"}),
+		bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "stream_bytes_total",
+			Help:      "Bytes copied on upgraded streams, by direction (to_backend, from_backend).",
+		}, []string{"upstream", "direction"}),
+	}
+}
+
+// register registers the stream metrics in registry. Registering them
+// again, e.g. by another handler or after a reload, is not an error.
+func (m *streamMetricsSet) register(registry *prometheus.Registry) {
+	for _, c := range []prometheus.Collector{m.active, m.closedBy, m.duration, m.bytes} {
+		if err := registry.Register(c); err != nil && !errors.As(err, &prometheus.AlreadyRegisteredError{}) {
+			panic(err)
+		}
+	}
+}
+
+// opened accounts for a stream to upstream that was just opened.
+func (m *streamMetricsSet) opened(upstream string) {
+	m.active.WithLabelValues(upstream).Inc()
+}
+
+// closed accounts for a stream that ended. It must be called exactly once
+// per opened stream. The closed counter is the last to be updated, so that
+// a stream shown as closed has its other metrics in place.
+func (m *streamMetricsSet) closed(s streamSummary) {
+	m.active.WithLabelValues(s.upstream).Dec()
+	m.duration.WithLabelValues(s.upstream).Observe(s.duration.Seconds())
+	m.bytes.WithLabelValues(s.upstream, "to_backend").Add(float64(s.toBackend))
+	m.bytes.WithLabelValues(s.upstream, "from_backend").Add(float64(s.fromBackend))
+	m.closedBy.WithLabelValues(s.upstream, s.result).Inc()
+}
+
 type metricsUpstreamsHealthyUpdater struct {
 	handler *Handler
 }
diff --git a/modules/caddyhttp/reverseproxy/metrics_test.go b/modules/caddyhttp/reverseproxy/metrics_test.go
new file mode 100644
index 00000000..525e2337
--- /dev/null
+++ b/modules/caddyhttp/reverseproxy/metrics_test.go
@@ -0,0 +1,183 @@
+package reverseproxy
+
+import (
+	"sync"
+	"sync/atomic"
+	"testing"
+	"time"
+
+	"github.com/prometheus/client_golang/prometheus"
+	dto "github.com/prometheus/client_model/go"
+)
+
+func writeMetric(t *testing.T, m prometheus.Metric) *dto.Metric {
+	t.Helper()
+	out := new(dto.Metric)
+	if err := m.Write(out); err != nil {
+		t.Fatalf("reading metric: %v", err)
+	}
+	return out
+}
+
+func activeStreams(t *testing.T, upstream string) float64 {
+	t.Helper()
+	return writeMetric(t, streamMetrics.active.WithLabelValues(upstream)).GetGauge().GetValue()
+}
+
+func closedStreams(t *testing.T, upstream, result string) float64 {
+	t.Helper()
+	return writeMetric(t, streamMetrics.closedBy.WithLabelValues(upstream, result)).GetCounter().GetValue()
+}
+
+func streamBytes(t *testing.T, upstream, direction string) float64 {
+	t.Helper()
+	return writeMetric(t, streamMetrics.bytes.WithLabelValues(upstream, direction)).GetCounter().GetValue()
+}
+
+func observedStreamDurations(t *testing.T, upstream string) uint64 {
+	t.Helper()
+	obs := streamMetrics.duration.WithLabelValues(upstream)
+	return writeMetric(t, obs.(prometheus.Metric)).GetHistogram().GetSampleCount()
+}
+
+// Whichever path ends a stream first (the copiers, a timeout, a reload, an
+// upstream removal), the stream is accounted exactly once.
+func TestStreamMetricsAccountedOnce(t *testing.T) {
+	const upstream = "accounted-once.test:1"
+
+	st := newUpgradedStream(upstream)
+	if got := activeStreams(t, upstream); got != 1 {
+		t.Fatalf("active streams after open = %v, want 1", got)
+	}
+	st.toBackend.Add(100)
+	st.fromBackend.Add(250)
+	time.Sleep(5 * time.Millisecond)
+
+	results := []string{
+		streamResultClosed, streamResultError, streamResultTimeout,
+		streamResultReload, streamResultUpstreamRemoved,
+	}
+
+	var (
+		wg     sync.WaitGroup
+		firsts atomic.Int32
+		start  = make(chan struct{})
+	)
+	for i := range 40 {
+		wg.Add(1)
+		go func() {
+			defer wg.Done()
+			<-start
+			result := results[i%len(results)]
+			if i%2 == 0 {
+				// an outside close, as done on reload or upstream removal
+				st.markResult(result)
+			}
+			if _, first := st.finish(result); first {
+				firsts.Add(1)
+			}
+		}()
+	}
+	close(start)
+	wg.Wait()
+
+	if got := firsts.Load(); got != 1 {
+		t.Fatalf("%d calls accounted the stream, want exactly 1", got)
+	}
+	if got := activeStreams(t, upstream); got != 0 {
+		t.Errorf("active streams after close = %v, want 0", got)
+	}
+
+	var closed float64
+	for _, result := range results {
+		closed += closedStreams(t, upstream, result)
+	}
+	if closed != 1 {
+		t.Errorf("closed streams = %v, want 1", closed)
+	}
+	if got := observedStreamDurations(t, upstream); got != 1 {
+		t.Errorf("observed durations = %d, want 1", got)
+	}
+	if got := streamBytes(t, upstream, "to_backend"); got != 100 {
+		t.Errorf("bytes to backend = %v, want 100", got)
+	}
+	if got := streamBytes(t, upstream, "from_backend"); got != 250 {
+		t.Errorf("bytes from backend = %v, want 250", got)
+	}
+
+	// finishing again changes nothing
+	if _, first := st.finish(streamResultError); first {
+		t.Error("a finished stream was accounted a second time")
+	}
+	if got := activeStreams(t, upstream); got != 0 {
+		t.Errorf("active streams after second finish = %v, want 0", got)
+	}
+}
+
+// The result imposed by an outside close wins over the result the stream
+// ends with, since the latter is only the consequence of the former.
+func TestUpgradedStreamResult(t *testing.T) {
+	const upstream = "result.test:1"
+
+	for _, tc := range []struct {
+		name   string
+		marks  []string
+		finish string
+		want   string
+	}{
+		{name: "natural result", finish: streamResultClosed, want: streamResultClosed},
+		{name: "outside close wins", marks: []string{streamResultReload}, finish: streamResultError, want: streamResultReload},
+		{name: "first outside close wins", marks: []string{streamResultUpstreamRemoved, streamResultReload}, finish: streamResultError, want: streamResultUpstreamRemoved},
+	} {
+		st := newUpgradedStream(upstream)
+		for _, m := range tc.marks {
+			st.markResult(m)
+		}
+		summary, first := st.finish(tc.finish)
+		if !first {
+			t.Errorf("%s: stream not accounted", tc.name)
+		}
+		if summary.result != tc.want {
+			t.Errorf("%s: result = %q, want %q", tc.name, summary.result, tc.want)
+		}
+		if summary.upstream != upstream {
+			t.Errorf("%s: upstream = %q, want %q", tc.name, summary.upstream, upstream)
+		}
+	}
+}
+
+func TestStreamMetricsRegister(t *testing.T) {
+	registry := prometheus.NewRegistry()
+
+	// registering again (another handler, a reload) is fine
+	streamMetrics.register(registry)
+	streamMetrics.register(registry)
+
+	_ = newUpgradedStream("registered.test:1")
+	families, err := registry.Gather()
+	if err != nil {
+		t.Fatalf("gathering metrics: %v", err)
+	}
+	found := make(map[string]bool)
+	for _, f := range families {
+		found[f.GetName()] = true
+	}
+	if !found["caddy_reverse_proxy_streams_active"] {
+		t.Errorf("streams_active is not exposed, got %v", found)
+	}
+
+	// a clash with something that is not our collector is an error
+	clashing := prometheus.NewRegistry()
+	clashing.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{
+		Namespace: "caddy",
+		Subsystem: "reverse_proxy",
+		Name:      "streams_active",
+		Help:      "something else",
+	}))
+	defer func() {
+		if recover() == nil {
+			t.Error("registering over a clashing collector should panic")
+		}
+	}()
+	streamMetrics.register(clashing)
+}
diff --git a/modules/caddyhttp/reverseproxy/reverseproxy.go b/modules/caddyhttp/reverseproxy/reverseproxy.go
index 0838c8be..73968ff5 100644
--- a/modules/caddyhttp/reverseproxy/reverseproxy.go
+++ b/modules/caddyhttp/reverseproxy/reverseproxy.go
@@ -199,6 +199,21 @@ type Handler struct {
 	// by the previous config closing. Default: no delay.
 	StreamCloseDelay caddy.Duration `json:"stream_close_delay,omitempty"`
 
+	// If true, upgraded streams (WebSockets and other Upgrade tunnels)
+	// are detached from the lifecycle of the config that created them:
+	// they survive a config reload instead of being closed with it, and
+	// they no longer keep this handler's connection registry (and
+	// stream_close_delay timer) alive. On reload, only the streams whose
+	// upstream is no longer present in the new config are closed. Streams
+	// to dynamic upstreams are not tracked per upstream and thus are only
+	// closed when they end on their own. Default: false, i.e. upgraded
+	// streams are closed when the config is unloaded, honoring
+	// stream_close_delay.
+	StreamDetached bool `json:"stream_detached,omitempty"`
+
+	// Configures the logs emitted for upgraded streams.
+	StreamLogs *StreamLogs `json:"stream_logs,omitempty"`
+
 	// If configured, rewrites the copy of the upstream request.
 	// Allows changing the request method and URI (path and query).
 	// Since the rewrite is applied to the copy, it does not persist
@@ -263,6 +278,9 @@ type Handler struct {
 	connectionsCloseTimer *time.Timer
 	connectionsMu         *sync.Mutex
 
+	// The level of the log entry emitted when an upgraded stream closes
+	streamLogLevel zapcore.Level
+
 	ctx    caddy.Context
 	logger *zap.Logger
 	events *caddyevents.App
@@ -288,6 +306,11 @@ func (h *Handler) Provision(ctx caddy.Context) error {
 	h.connections = make(map[io.ReadWriteCloser]openConnection)
 	h.connectionsMu = new(sync.Mutex)
 
+	h.streamLogLevel, err = h.StreamLogs.parseLevel()
+	if err != nil {
+		return fmt.Errorf("stream_logs: %v", err)
+	}
+
 	// warn about unsafe buffering config
 	if h.RequestBuffers == -1 || h.ResponseBuffers == -1 {
 		h.logger.Warn("UNLIMITED BUFFERING: buffering is enabled without any cap on buffer size, which can result in OOM crashes")
@@ -460,9 +483,13 @@ func (h *Handler) Provision(ctx caddy.Context) error {
 func (h *Handler) Cleanup() error {
 	err := h.cleanupConnections()
 
-	// remove hosts from our config from the pool
+	// remove hosts from our config from the pool; when the last
+	// reference to an upstream is gone it is no longer in any config,
+	// so the detached streams that still use it must be closed
 	for _, upstream := range h.Upstreams {
-		_, _ = hosts.Delete(upstream.String())
+		if deleted, _ := hosts.Delete(upstream.String()); deleted {
+			detachedStreams.closeUpstream(upstream.String(), streamResultUpstreamRemoved)
+		}
 	}
 
 	return err
diff --git a/modules/caddyhttp/reverseproxy/stream_detached_test.go b/modules/caddyhttp/reverseproxy/stream_detached_test.go
new file mode 100644
index 00000000..870694af
--- /dev/null
+++ b/modules/caddyhttp/reverseproxy/stream_detached_test.go
@@ -0,0 +1,603 @@
+package reverseproxy
+
+import (
+	"bufio"
+	"bytes"
+	"context"
+	"errors"
+	"io"
+	"net"
+	"net/http"
+	"net/http/httptest"
+	"net/url"
+	"reflect"
+	"strings"
+	"sync"
+	"sync/atomic"
+	"testing"
+	"time"
+
+	"github.com/caddyserver/caddy/v2"
+	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
+	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
+)
+
+func TestStreamOptionsCaddyfile(t *testing.T) {
+	block := func(body string) string {
+		return "reverse_proxy localhost:8080 {\n" + body + "\n}"
+	}
+
+	for i, tc := range []struct {
+		input    string
+		wantErr  bool
+		detached bool
+		logs     *StreamLogs
+	}{
+		{input: "reverse_proxy localhost:8080"},
+		{input: block("stream_detached"), detached: true},
+		{input: block("stream_detached extra"), wantErr: true},
+		{input: block("stream_logs {\n}"), logs: &StreamLogs{}},
+		{input: block("stream_logs {\nskip_handshake\n}"), logs: &StreamLogs{SkipHandshake: true}},
+		{input: block("stream_logs {\nlevel info\n}"), logs: &StreamLogs{Level: "info"}},
+		{
+			input:    block("stream_detached\nstream_logs {\nskip_handshake\nlevel WARN\n}"),
+			detached: true,
+			logs:     &StreamLogs{SkipHandshake: true, Level: "WARN"},
+		},
+		{
+			// the block can be repeated, the options add up
+			input: block("stream_logs {\nskip_handshake\n}\nstream_logs {\nlevel error\n}"),
+			logs:  &StreamLogs{SkipHandshake: true, Level: "error"},
+		},
+		{input: block("stream_logs extra {\n}"), wantErr: true},
+		{input: block("stream_logs {\nskip_handshake extra\n}"), wantErr: true},
+		{input: block("stream_logs {\nlevel\n}"), wantErr: true},
+		{input: block("stream_logs {\nlevel bogus\n}"), wantErr: true},
+		{input: block("stream_logs {\nlevel info extra\n}"), wantErr: true},
+		{input: block("stream_logs {\nbogus\n}"), wantErr: true},
+	} {
+		h := new(Handler)
+		err := h.UnmarshalCaddyfile(caddyfile.NewTestDispenser(tc.input))
+		if tc.wantErr {
+			if err == nil {
+				t.Errorf("Test %d: expected an error for %q but got none", i, tc.input)
+			}
+			continue
+		}
+		if err != nil {
+			t.Errorf("Test %d: unexpected error for %q: %v", i, tc.input, err)
+			continue
+		}
+		if h.StreamDetached != tc.detached {
+			t.Errorf("Test %d: StreamDetached = %v, want %v", i, h.StreamDetached, tc.detached)
+		}
+		if !reflect.DeepEqual(h.StreamLogs, tc.logs) {
+			t.Errorf("Test %d: StreamLogs = %+v, want %+v", i, h.StreamLogs, tc.logs)
+		}
+	}
+}
+
+func TestStreamLogsOptions(t *testing.T) {
+	for i, tc := range []struct {
+		logs     *StreamLogs
+		wantErr  bool
+		wantSkip bool
+	}{
+		{logs: nil},
+		{logs: &StreamLogs{}},
+		{logs: &StreamLogs{SkipHandshake: true}, wantSkip: true},
+		{logs: &StreamLogs{Level: "info"}},
+		{logs: &StreamLogs{Level: "bogus"}, wantErr: true},
+	} {
+		level, err := tc.logs.parseLevel()
+		if tc.wantErr {
+			if err == nil {
+				t.Errorf("Test %d: expected an error but got none", i)
+			}
+		} else if err != nil {
+			t.Errorf("Test %d: unexpected error: %v", i, err)
+		}
+		if tc.logs != nil && tc.logs.Level == "info" && level.String() != "info" {
+			t.Errorf("Test %d: level = %s, want info", i, level)
+		}
+		if (tc.logs == nil || tc.logs.Level == "") && level.String() != "debug" {
+			t.Errorf("Test %d: default level = %s, want debug", i, level)
+		}
+		if got := tc.logs.skipHandshake(); got != tc.wantSkip {
+			t.Errorf("Test %d
... (truncated; 30267 more characters; read the files)
```

## Answer these questions, one at a time, in this order

1. Which acceptance criterion of this task is not met, or is met only in appearance (hard-coded, special-cased, or untested path)?
2. Which relevant case (boundary, error, empty input, interaction with existing options or state) is not covered by any test of this task?
3. Which existing test was weakened: an assertion removed or loosened, a case deleted, a skip added? Look at the modified declared test files above.
4. Which part of the diff could be removed or simplified without losing behaviour the task requires (dead code, duplicated branches, unnecessary abstraction)?

For each question, report only concrete findings: `file`, `line` (in the new version; 0 if the
finding is not tied to a line), the question number as `pass`, a `severity` and a one-sentence
`text`. `blocking` means the task must not be committed as is: a criterion unmet, a relevant case
untested, an existing test weakened, or a change that does not belong to the task. `advisory`
means worth a comment but not worth another attempt. No finding is a valid answer to a question.

A `blocking` finding is a claim that some test would fail, so it carries that test as `proof`,
and you have run it with `aih check --probe` and seen it fail for the reason you claim before
reporting it (the harness runs it again; a proof that passes for the harness downgrades the
finding, whatever you saw). The proof is:
`path`, a NEW file in the directory of one of the task's test files, named like a test file of
this repository (same package for Go, `test_*.py` for pytest), and `content`, the whole file
(package clause, imports, one or more tests) asserting exactly what you claim. The harness writes
it, runs it with the task's test command and removes it. If it fails, your finding stands and the
implementer gets the test to make pass. If it passes, or does not compile, or you give none, the
finding is recorded as advisory with that reason, not as blocking. A claim you cannot turn into a
test here (behaviour on another platform, style, taste) is advisory from the start: say it, do not
block on it. The tests already pass, so a proof that merely repeats them will pass too.

## Coverage map and probes

Then fill `coverage` with one entry per acceptance criterion marked `[behavior]` above. For
each, name the single test of this task's test files whose assertion proves the criterion:
`test` as a pytest node id (`tests/path.py::test_name`), `line` of the decisive assertion,
`status` `covered` when that assertion would fail if the criterion were broken, `partial` when
a test touches the behaviour but would not fail (asserts only the happy path, or a weaker
property), `uncovered` when no test exercises it. Read the test body: do not infer coverage
from a test's name. `partial` and `uncovered` are blocking, so when you mark one, say in
`note` what test would prove it.

Each entry also carries a `probe`: a test YOU wrote for that criterion and ran with
`aih check --probe`. Approving is a claim that the behaviour holds, and the task's own tests
were written by the session that wrote the code, so they prove what it believed, not what the
criterion says. Write the probe from the criterion's text, through the surface the request names
(the program's configuration, its public API, a real client), exercising the case the criterion
is about rather than the happy path; do not copy an existing test. Run it until it runs (with
`--run <your test names>` where supported: the rest of the package is not what you are checking;
a run that executed zero tests, `[no tests to run]`, counts as not run);
a probe that passes verifies the criterion, a probe that fails is a blocking finding of pass 1 with
that probe as `proof`. The harness runs every probe again, and runs every passing probe once more
on the base commit without the production change: a probe that passes there too asserts nothing
this change adds and does not count (a test with no assertion, or one that only checks what
already held). A criterion whose probe is missing, does not run, or does not discriminate is
recorded as unprobed, visible in the pull request, and you will be asked for it once.
`probe.path` is a new file in a directory of the task's test files, named like a test file of the
repository; `probe.content` is the whole file.

`verdict` is `request_changes` if and only if there is at least one `blocking` finding or a
criterion that is not `covered`.
