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

One releasable change: opt-in detached upgraded streams in reverse_proxy, with stream logs, hijack-safety fixes in the response-writer chain, stream metrics, config/docs and tests. Docs are limited to the Caddyfile doc comment and JSON struct field comments, as the intents require.

## Task T1 (T1)

Order: (1) add config fields and Caddyfile parsing plus docs; (2) add stream metrics and a stream-logs/access-logger path; (3) add a global detached-stream registry keyed by upstream, used in handleUpgradeResponse and Cleanup/reload logic, leaving the default path unchanged; (4) fix hijack safety in caddyhttp responseRecorder/encode/intercept (no writes after hijack, copy on raw conn); (5) write unit then integration tests, updating existing streaming tests and adapt fixtures if they break.

Acceptance criteria:
- ACC1 [behavior]: With stream_detached set (Caddyfile and JSON), an established WebSocket/Upgrade stream stays open and passes data after a config reload.
- ACC2 [behavior]: In detached mode, after a reload that removes an upstream, streams to that upstream are closed while streams to retained upstreams keep working.
- ACC3 [behavior]: Without stream_detached, upgraded streams are closed on reload, and with stream_close_delay they stay open until the delay elapses.
- ACC4 [behavior]: stream_logs { skip_handshake } suppresses the access log entry for the upgrade handshake; by default it is logged. Caddyfile parses to JSON correctly.
- ACC5 [behavior]: Stream close is logged via the access logger when logger_name access is configured, at a configurable level defaulting to DEBUG.
- ACC6 [behavior]: After hijack, writes and WriteHeader on the recording writer are no-ops or errors and do not reach the connection; 101 is final; the proxy copies on the raw conn, not the hijackedConn wrapper.
- ACC7 [behavior]: WebSocket upgrades work through encode and through intercept with handle_response in the same route, and with access logging.
- ACC8 [behavior]: HTTP/2 extended CONNECT websocket works end to end over h2, and HTTP/1.1 upgrades work in detached mode.
- ACC9 [behavior]: Metrics for active streams, closed streams by result, duration, and bytes to/from the backend are recorded, each stream exactly once, including on timeout, error and reload close.
- ACC10 [other]: stream_detached and stream_logs appear in JSON struct fields and Caddyfile parsing, with doc comments in caddyfile.go and reverseproxy.go.
- ACC11 [behavior]: Integration tests cover reload survival/closure in both modes, h2 extended CONNECT, encode and intercept upgrades, and detached streams with access logging.
- ACC12 [behavior]: Unit tests cover the detached registry, Caddyfile parsing of new options, metrics accounting, and post-hijack writer behaviour.

Files declared for the task:
- modules/caddyhttp/reverseproxy/reverseproxy.go (production)
- modules/caddyhttp/reverseproxy/streaming.go (production)
- modules/caddyhttp/reverseproxy/caddyfile.go (production)
- modules/caddyhttp/reverseproxy/metrics.go (production)
- modules/caddyhttp/responsewriter.go (production)
- modules/caddyhttp/server.go (production)
- modules/caddyhttp/encode/encode.go (production)
- modules/caddyhttp/intercept/intercept.go (production)
- modules/caddyhttp/logging.go (production)
- modules/caddyhttp/reverseproxy/streaming_test.go (test)
- modules/caddyhttp/reverseproxy/stream_detached_test.go (test)
- modules/caddyhttp/responsewriter_test.go (test)
- caddytest/integration/stream_test.go (test)
- caddytest/integration/stream_detached_test.go (test)
- caddytest/integration/intercept_test.go (test)

Test files of this task that the change modified (existing tests, declared by the plan):
- caddytest/integration/intercept_test.go
- modules/caddyhttp/responsewriter_test.go

Test results: selected tests green; red proven; suite green

## The diff

```diff
diff --git a/caddytest/integration/intercept_test.go b/caddytest/integration/intercept_test.go
index c1b33b96..96d2c63a 100644
--- a/caddytest/integration/intercept_test.go
+++ b/caddytest/integration/intercept_test.go
@@ -1,6 +1,7 @@
 package integration
 
 import (
+	"net/http"
 	"testing"
 
 	"github.com/caddyserver/caddy/v2/caddytest"
@@ -39,6 +40,52 @@ func TestIntercept(t *testing.T) {
 	tester.AssertGetResponse("http://localhost:9080/no-intercept", 200, "I'm not a teapot")
 }
 
+// An upgrade is not a response to intercept: a catch-all handle_response must
+// leave a 101 alone (the connection is hijacked, nothing may be written to
+// it afterwards), while it still applies to every other response.
+func TestInterceptUpgrade(t *testing.T) {
+	for name, options := range map[string]string{
+		"default":  "",
+		"detached": "stream_detached",
+	} {
+		t.Run(name, func(t *testing.T) {
+			newUpgradeBackend(t, "127.0.0.1:9081")
+			path := logFile(t)
+			tester := caddytest.NewTester(t)
+			tester.InitServer(streamGlobalOptions+`
+			localhost:9080 {
+				log {
+					output file "`+path+`"
+				}
+				route {
+					intercept {
+						handle_response {
+							respond "intercepted" 203
+						}
+					}
+					reverse_proxy 127.0.0.1:9081 {
+						`+options+`
+					}
+				}
+			}`, "caddyfile")
+
+			tester.AssertGetResponse("http://localhost:9080/plain", 203, "intercepted")
+
+			ws := dialWS(t, "/ws")
+			ws.echo(t, "through intercept")
+			ws.echo(t, "and again")
+			ws.conn.Close()
+
+			eventually(t, "the upgrade to be logged", func() bool {
+				return len(findLog(path, "handled request", "/ws")) == 1
+			})
+			if status := findLog(path, "handled request", "/ws")[0]["status"]; status != float64(http.StatusSwitchingProtocols) {
+				t.Errorf("logged status = %v, want 101", status)
+			}
+		})
+	}
+}
+
 func TestInterceptReplaceStatusWithMatcher(t *testing.T) {
 	tester := caddytest.NewTester(t)
 	tester.InitServer(`{
diff --git a/caddytest/integration/stream_detached_test.go b/caddytest/integration/stream_detached_test.go
new file mode 100644
index 00000000..cb0adc4e
--- /dev/null
+++ b/caddytest/integration/stream_detached_test.go
@@ -0,0 +1,728 @@
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
+	"sync"
+	"testing"
+	"time"
+
+	"golang.org/x/net/http2"
+	"golang.org/x/net/http2/hpack"
+
+	"github.com/caddyserver/caddy/v2/caddytest"
+)
+
+const streamTestTimeout = 5 * time.Second
+
+// http2XConnect is the GODEBUG setting which makes the HTTP/2 server accept
+// extended CONNECT (RFC 8441).
+const http2XConnect = "http2xconnect=1"
+
+// TestMain runs the tests of the package in a child process in which the
+// HTTP/2 server supports extended CONNECT: the server reads the setting once
+// when the process starts, so it cannot be enabled by a test.
+func TestMain(m *testing.M) {
+	debug := os.Getenv("GODEBUG")
+	if strings.Contains(debug, http2XConnect) {
+		os.Exit(m.Run())
+	}
+
+	cmd := exec.Command(os.Args[0], os.Args[1:]...)
+	cmd.Env = append(os.Environ(), "GODEBUG="+strings.TrimPrefix(debug+","+http2XConnect, ","))
+	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
+	err := cmd.Run()
+	if err == nil {
+		os.Exit(0)
+	}
+	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
+		os.Exit(exitErr.ExitCode())
+	}
+	fmt.Fprintf(os.Stderr, "failed to run the tests: %v\n", err)
+	os.Exit(1)
+}
+
+// streamGlobalOptions are the global options of the Caddyfiles of the stream tests.
+const streamGlobalOptions = `{
+	skip_install_trust
+	admin localhost:2999
+	http_port     9080
+	https_port    9443
+	grace_period  1ns
+}
+`
+
+// upgradeBackend is a backend which switches protocols for every upgrade
+// request and then echoes what it receives. Other requests get a plain
+// response.
+type upgradeBackend struct {
+	ln    net.Listener
+	mu    sync.Mutex
+	conns []net.Conn
+}
+
+func newUpgradeBackend(t *testing.T, addr string) *upgradeBackend {
+	t.Helper()
+	ln, err := net.Listen("tcp", addr)
+	if err != nil {
+		t.Fatalf("failed to start the backend on %s: %v", addr, err)
+	}
+	b := &upgradeBackend{ln: ln}
+	t.Cleanup(b.close)
+	go func() {
+		for {
+			c, err := ln.Accept()
+			if err != nil {
+				return
+			}
+			b.mu.Lock()
+			b.conns = append(b.conns, c)
+			b.mu.Unlock()
+			go b.serve(c)
+		}
+	}()
+	return b
+}
+
+func (b *upgradeBackend) close() {
+	_ = b.ln.Close()
+	b.mu.Lock()
+	defer b.mu.Unlock()
+	for _, c := range b.conns {
+		_ = c.Close()
+	}
+}
+
+func (b *upgradeBackend) serve(c net.Conn) {
+	defer c.Close()
+	br := bufio.NewReader(c)
+	req, err := http.ReadRequest(br)
+	if err != nil {
+		return
+	}
+	if !strings.EqualFold(req.Header.Get("Upgrade"), "websocket") {
+		// no Content-Length, so handlers may replace the body
+		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nplain")
+		return
+	}
+	_, err = io.WriteString(c, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: dummy\r\n\r\n")
+	if err != nil {
+		return
+	}
+	// echo, including what was sent along with the handshake
+	_, _ = io.Copy(c, br)
+}
+
+// wsClient is the client of an upgraded stream to 127.0.0.1:9080.
+type wsClient struct {
+	conn net.Conn
+	br   *bufio.Reader
+}
+
+// dialWS performs an HTTP/1.1 upgrade handshake for path.
+func dialWS(t *testing.T, path string, extraHeaders ...string) *wsClient {
+	t.Helper()
+	conn, err := net.DialTimeout("tcp", "127.0.0.1:9080", streamTestTimeout)
+	if err != nil {
+		t.Fatalf("failed to dial: %v", err)
+	}
+	t.Cleanup(func() { conn.Close() })
+
+	req := "GET " + path + " HTTP/1.1\r\n" +
+		"Host: localhost:9080\r\n" +
+		"Connection: Upgrade\r\n" +
+		"Upgrade: websocket\r\n" +
+		"Sec-WebSocket-Version: 13\r\n" +
+		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
+	for _, h := range extraHeaders {
+		req += h + "\r\n"
+	}
+	req += "\r\n"
+	_ = conn.SetDeadline(time.Now().Add(streamTestTimeout))
+	if _, err := io.WriteString(conn, req); err != nil {
+		t.Fatalf("failed to write the upgrade request: %v", err)
+	}
+	br := bufio.NewReader(conn)
+	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
+	if err != nil {
+		t.Fatalf("failed to read the upgrade response: %v", err)
+	}
+	if resp.StatusCode != http.StatusSwitchingProtocols {
+		t.Fatalf("upgrade status = %d, want 101", resp.StatusCode)
+	}
+	return &wsClient{conn: conn, br: br}
+}
+
+// echo checks that data passes through the stream and back.
+func (c *wsClient) echo(t *testing.T, msg string) {
+	t.Helper()
+	_ = c.conn.SetDeadline(time.Now().Add(streamTestTimeout))
+	if _, err := io.WriteString(c.conn, msg); err != nil {
+		t.Fatalf("stream is not writable: %v", err)
+	}
+	got := make([]byte, len(msg))
+	if _, err := io.ReadFull(c.br, got); err != nil {
+		t.Fatalf("stream is not readable: %v", err)
+	}
+	if string(got) != msg {
+		t.Fatalf("echo = %q, want %q", got, msg)
+	}
+}
+
+// requireClosed checks that the server closes the stream within wait.
+func (c *wsClient) requireClosed(t *testing.T, wait time.Duration) {
+	t.Helper()
+	_ = c.conn.SetReadDeadline(time.Now().Add(wait))
+	// a websocket close frame may precede the end of the stream
+	if _, err := io.Copy(io.Discard, c.br); errors.Is(err, os.ErrDeadlineExceeded) {
+		t.Fatalf("stream was not closed within %v", wait)
+	}
+}
+
+func eventually(t *testing.T, what string, cond func() bool) {
+	t.Helper()
+	deadline := time.Now().Add(streamTestTimeout)
+	for !cond() {
+		if time.Now().After(deadline) {
+			t.Fatalf("timed out waiting for %s", what)
+		}
+		time.Sleep(20 * time.Millisecond)
+	}
+}
+
+// readLog returns the JSON entries of the log file.
+func readLog(path string) []map[string]any {
+	data, err := os.ReadFile(path)
+	if err != nil {
+		return nil
+	}
+	var entries []map[string]any
+	for _, line := range bytes.Split(data, []byte("\n")) {
+		var entry map[string]any
+		if json.Unmarshal(line, &entry) == nil {
+			entries = append(entries, entry)
+		}
+	}
+	return entries
+}
+
+// findLog returns the entries with the message msg about the request for uri
+// (an empty uri matches every entry of the message).
+func findLog(path, msg, uri string) []map[string]any {
+	var found []map[string]any
+	for _, e := range readLog(path) {
+		if e["msg"] != msg {
+			continue
+		}
+		if uri != "" {
+			req, _ := e["request"].(map[string]any)
+			if req["uri"] != uri {
+				continue
+			}
+		}
+		found = append(found, e)
+	}
+	return found
+}
+
+func logFile(t *testing.T) string {
+	return filepath.ToSlash(filepath.Join(t.TempDir(), "access.log"))
+}
+
+func TestStreamDetachedSurvivesReload(t *testing.T) {
+	newUpgradeBackend(t, "127.0.0.1:9081")
+	tester := caddytest.NewTester(t)
+	tester.InitServer(streamGlobalOptions+`
+	localhost:9080 {
+		reverse_proxy 127.0.0.1:9081 {
+			stream_detached
+		}
+	}`, "caddyfile")
+
+	ws := dialWS(t, "/ws")
+	ws.echo(t, "before")
+
+	tester.InitServer(streamGlobalOptions+`
+	localhost:9080 {
+		respond /version "v2"
+		reverse_proxy 127.0.0.1:9081 {
+			stream_detached
+		}
+	}`, "caddyfile")
+	tester.AssertGetResponse("http://localhost:9080/version", 200, "v2")
+
+	ws.echo(t, "after the reload")
+	// streams of the new config work too
+	dialWS(t, "/ws").echo(t, "new stream")
+	ws.echo(t, "still open")
+}
+
+func TestStreamDetachedSurvivesReloadJSON(t *testing.T) {
+	newUpgradeBackend(t, "127.0.0.1:9081")
+	tester := caddytest.NewTester(t)
+
+	config := func(extraRoutes string) string {
+		return fmt.Sprintf(`{
+			"admin": {"listen": "localhost:2999"},
+			"apps": {"http": {
+				"http_port": 9080,
+				"https_port": 9443,
+				"grace_period": 1,
+				"servers": {"srv0": {
+					"listen": [":9080"],
+					"routes": [%s{
+						"handle": [{
+							"handler": "reverse_proxy",
+							"stream_detached": true,
+							"upstreams": [{"dial": "127.0.0.1:9081"}]
+						}]
+					}]
+				}}
+			}}
+		}`, extraRoutes)
+	}
+	tester.InitServer(config(""), "json")
+
+	ws := dialWS(t, "/ws")
+	ws.echo(t, "before")
+
+	tester.InitServer(config(`{
+		"match": [{"path": ["/version"]}],
+		"handle": [{"handler": "static_response", "body": "v2"}]
+	},`), "json")
+	tester.AssertGetResponse("http://localhost:9080/version", 200, "v2")
+
+	ws.echo(t, "after the reload")
+}
+
+func TestStreamDetachedClosesRemovedUpstream(t *testing.T) {
+	newUpgradeBackend(t, "127.0.0.1:9081")
+	newUpgradeBackend(t, "127.0.0.1:9082")
+	tester := caddytest.NewTester(t)
+	tester.InitServer(streamGlobalOptions+`
+	localhost:9080 {
+		handle /a {
+			reverse_proxy 127.0.0.1:9081 {
+				stream_detached
+			}
+		}
+		handle /b {
+			reverse_proxy 127.0.0.1:9082 {
+				stream_detached
+			}
+		}
+	}`, "caddyfile")
+
+	a, b := dialWS(t, "/a"), dialWS(t, "/b")
+	a.echo(t, "a")
+	b.echo(t, "b")
+
+	// the second upstream is gone from the new config
+	tester.InitServer(streamGlobalOptions+`
+	localhost:9080 {
+		handle /a {
+			reverse_proxy 127.0.0.1:9081 {
+				stream_detached
+			}
+		}
+	}`, "caddyfile")
+
+	b.requireClosed(t, streamTestTimeout)
+	a.echo(t, "a is retained")
+}
+
+func TestStreamNotDetachedClosedOnReload(t *testing.T) {
+	newUpgradeBackend(t, "127.0.0.1:9081")
+	tester := caddytest.NewTester(t)
+	tester.InitServer(streamGlobalOptions+`
+	localhost:9080 {
+		reverse_proxy 127.0.0.1:9081
+	}`, "caddyfile")
+
+	ws := dialWS(t, "/ws")
+	ws.echo(t, "before")
+
+	tester.InitServer(streamGlobalOptions+`
+	localhost:9080 {
+		respond /version "v2"
+		reverse_proxy 127.0.0.1:9081
+	}`, "caddyfile")
+
+	ws.requireClosed(t, streamTestTimeout)
+	// the new config accepts new streams
+	dialWS(t, "/ws").echo(t, "new stream")
+}
+
+func TestStreamCloseDelayOnReload(t *testing.T) {
+	const delay = 2 * time.Second
+	newUpgradeBackend(t, "127.0.0.1:9081")
+	tester := caddytest.NewTester(t)
+	tester.InitServer(streamGlobalOptions+`
+	localhost:9080 {
+		reverse_proxy 127.0.0.1:9081 {
+			stream_close_delay 2s
+		}
+	}`, "caddyfile")
+
+	ws := dialWS(t, "/ws")
+	ws.echo(t, "before")
+
+	start := time.Now()
+	tester.InitServer(streamGlobalOptions+`
+	localhost:9080 {
+		respond /version "v2"
+		reverse_proxy 127.0.0.1:9081
+	}`, "caddyfile")
+
+	// still open after the reload, until the delay is over
+	ws.echo(t, "after the reload")
+	ws.requireClosed(t, 2*delay)
+	if elapsed := time.Since(start); elapsed < delay {
+		t.Errorf("stream closed %v after the reload started, before the %v delay", elapsed, delay)
+	}
+}
+
+func TestStreamLogsInvalidConfig(t *testing.T) {
+	for name, tc := range map[string]struct {
+		options string
+		want    string
+	}{
+		"logger name": {options: "logger_name other", want: "logger_name"},
+		"level":       {options: "level loud", want: "level"},
+	} {
+		t.Run(name, func(t *testing.T) {
+			caddytest.AssertLoadError(t, streamGlobalOptions+`
+			localhost:9080 {
+				reverse_proxy 127.0.0.1:9081 {
+					stream_logs {
+						`+tc.options+`
+					}
+				}
+			}`, "caddyfile", tc.want)
+		})
+	}
+}
+
+func TestStreamDetachedAccessLogs(t *testing.T) {
+	for name, tc := range map[string]struct {
+		accessLevel string // level of the access log
+		streamLogs  string // contents of the stream_logs block
+		wantHandled bool   // whether the handshake is access logged
+		wantClosed  string // level of the stream close entry in the access log, if any
+	}{
+		"handshake and stream close, default level": {
+			accessLevel: "DEBUG",
+			streamLogs:  "logger_name access",
+			wantHandled: true,
+			wantClosed:  "debug",
+		},
+		"handshake skipped": {
+			accessLevel: "DEBUG",
+			streamLogs:  "skip_handshake\nlogger_name access",
+			wantHandled: false,
+			wantClosed:  "debug",
+		},
+		"configured level": {
+			accessLevel: "INFO",
+			streamLogs:  "logger_name access\nlevel info",
+			wantHandled: true,
+			wantClosed:  "info",
+		},
+		"stream close not sent to the access log by default": {
+			accessLevel: "DEBUG",
+			streamLogs:  "level info",
+			wantHandled: true,
+		},
+	} {
+		t.Run(name, func(t *testing.T) {
+			newUpgradeBackend(t, "127.0.0.1:9081")
+			path := logFile(t)
+			tester := caddytest.NewTester(t)
+			tester.InitServer(streamGlobalOptions+`
+			localhost:9080 {
+				log {
+					output file "`+path+`"
+					level `+tc.accessLevel+`
+				}
+				reverse_proxy 127.0.0.1:9081 {
+					stream_detached
+					stream_logs {
+						`+tc.streamLogs+`
+					}
+				}
+			}`, "caddyfile")
+
+			// proves the access log works
+			tester.AssertGetResponse("http://localhost:9080/plain", 200, "plain")
+			eventually(t, "the plain request to be logged", func() bool {
+				return len(findLog(path, "handled request", "/plain")) == 1
+			})
+
+			ws := dialWS(t, "/ws")
+			ws.echo(t, "hello")
+
+			if tc.wantHandled {
+				// the handler of a detached stream returns right away, so the
+				// handshake is logged while the stream is open
+				eventually(t, "the handshake to be logged", func() bool {
+					return len(findLog(path, "handled request", "/ws")) == 1
+				})
+				if status := findLog(path, "handled request", "/ws")[0]["status"]; status != float64(http.StatusSwitchingProtocols) {
+					t.Errorf("logged handshake status = %v, want 101", status)
+				}
+			}
+
+			ws.conn.Close()
+
+			if tc.wantClosed != "" {
+				eventually(t, "the stream close to be logged", func() bool {
+					return len(findLog(path, "stream closed", "")) == 1
+				})
+				entry := findLog(path, "stream closed", "")[0]
+				if entry["level"] != tc.wantClosed {
+					t.Errorf("stream close logged at %v, want %s", entry["level"], tc.wantClosed)
+				}
+				if entry["bytes_to_backend"] != float64(5) || entry["bytes_from_backend"] != float64(5) {
+					t.Errorf("logged bytes = %v to the backend and %v from it, want 5 and 5", entry["bytes_to_backend"], entry["bytes_from_backend"])
+				}
+			} else {
+				time.Sleep(500 * time.Millisecond)
+				if got := findLog(path, "stream closed", ""); len(got) != 0 {
+					t.Errorf("stream close was access logged: %v", got)
+				}
+			}
+
+			if !tc.wantHandled {
+				if got := findLog(path, "handled request", "/ws"); len(got) != 0 {
+					t.Errorf("the handshake was access logged although skipped: %v", got)
+				}
+			}
+		})
+	}
+}
+
+// Streams which are not detached are access logged when they end, with the
+// status of the handshake, also behind encode.
+func TestStreamThroughEncodeWithAccessLog(t *testing.T) {
+	newUpgradeBackend(t, "127.0.0.1:9081")
+	path := logFile(t)
+	tester := caddytest.NewTester(t)
+	tester.InitServer(streamGlobalOptions+`
+	localhost:9080 {
+		log {
+			output file "`+path+`"
+		}
+		encode gzip
+		reverse_proxy 127.0.0.1:9081
+	}`, "caddyfile")
+
+	ws := dialWS(t, "/ws", "Accept-Encoding: gzip")
+	ws.echo(t, "through encode")
+	ws.conn.Close()
+
+	eventually(t, "the request to be logged", func() bool {
+		return len(findLog(path, "handled request", "/ws")) == 1
+	})
+	if status := findLog(path, "handled request", "/ws")[0]["status"]; status != float64(http.StatusSwitchingProtocols) {
+		t.Errorf("logged status = %v, want 101", status)
+	}
+}
+
+func TestStreamDetachedThroughEncodeWithAccessLog(t *testing.T) {
+	newUpgradeBackend(t, "127.0.0.1:9081")
+	path := logFile(t)
+	tester := caddytest.NewTester(t)
+	tester.InitServer(streamGlobalOptions+`
+	localhost:9080 {
+		log {
+			output file "`+path+`"
+		}
+		encode gzip
+		reverse_proxy 127.0.0.1:9081 {
+			stream_detached
+		}
+	}`, "caddyfile")
+
+	ws := dialWS(t, "/ws", "Accept-Encoding: gzip")
+	ws.echo(t, "through encode")
+
+	// ordinary responses are still encoded
+	tester.AssertGetResponse("http://localhost:9080/plain", 200, "plain")
+	eventually(t, "the handshake to be logged", func() bool {
+		return len(findLog(path, "handled request", "/ws")) == 1
+	})
+	ws.echo(t, "still open")
+}
+
+// HTTP/2 extended CONNECT (RFC 8441) is turned into an upgrade of the
+// backend connection.
+func TestStreamExtendedConnectH2(t *testing.T) {
+	for name, options := range map[string]string{
+		"default":  "",
+		"detached": "stream_detached",
+	} {
+		t.Run(name, func(t *testing.T) {
+			newUpgradeBackend(t, "127.0.0.1:9081")
+			tester := caddytest.NewTester(t)
+			tester.InitServer(streamGlobalOptions+`
+			localhost:9443 {
+				encode gzip
+				reverse_proxy 127.0.0.1:9081 {
+					`+options+`
+				}
+			}`, "caddyfile")
+
+			s := dialH2ExtendedConnect(t, "localhost:9443", "/ws")
+			s.echo(t, "hello over h2")
+			s.echo(t, "and again")
+		})
+	}
+}
+
+// h2Stream is a stream opened with an HTTP/2 extended CONNECT request. The
+// standard clients cannot send the :protocol pseudo-header, so this speaks
+// just enough HTTP/2 to open one stream and exchange DATA on it.
+type h2Stream struct {
+	conn net.Conn
+	fr   *http2.Framer
+	data bytes.Buffer // received and not yet consumed
+}
+
+func dialH2ExtendedConnect(t *testing.T, authority, path string) *h2Stream {
+	t.Helper()
+	conn, err := tls.Dial("tcp", "127.0.0.1:9443", &tls.Config{
+		InsecureSkipVerify: true, //nolint:gosec
+		ServerName:         "localhost",
+		NextProtos:         []string{"h2"},
+	})
+	if err != nil {
+		t.Fatalf("failed to dial: %v", err)
+	}
+	t.Cleanup(func() { conn.Close() })
+	if proto := conn.ConnectionState().NegotiatedProtocol; proto != "h2" {
+		t.Fatalf("negotiated protocol = %q, want h2", proto)
+	}
+
+	_ = conn.SetDeadline(time.Now().Add(streamTestTimeout))
+	s := &h2Stream{conn: conn, fr: http2.NewFramer(conn, conn)}
+	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
+		t.Fatal(err)
+	}
+	if err := s.fr.WriteSettings(); err != nil {
+		t.Fatal(err)
+	}
+
+	// the server has to advertise support for extended CONNECT (RFC 8441)
+	advertised := false
+	for settings := false; !settings; {
+		f, err := s.fr.ReadFrame()
+		if err != nil {
+			t.Fatalf("failed to read the server settings: %v", err)
+		}
+		sf, ok := f.(*http2.SettingsFrame)
+		if !ok || sf.IsAck() {
+			continue
+		}
+		settings = true
+		_ = sf.ForeachSetting(func(setting http2.Setting) error {
+			const settingEnableConnectProtocol http2.SettingID = 0x8
+			advertised = advertised || (setting.ID == settingEnableConnectProtocol && setting.Val == 1)
+			return nil
+		})
+		if err := s.fr.WriteSettingsAck(); err != nil {
+			t.Fatal(err)
+		}
+	}
+	if !advertised {
+		t.Fatal("the server does not advertise support for extended CONNECT")
+	}
+
+	var block bytes.Buffer
+	enc := hpack.NewEncoder(&block)
+	for _, field := range []hpack.HeaderField{
+		{Name: ":method", Value: "CONNECT"},
+		{Name: ":scheme", Value: "https"},
+		{Name: ":authority", Value: authority},
+		{Name: ":path", Value: path},
+		{Name: ":protocol", Value: "websocket"},
+		{Name: "sec-websocket-version", Value: "13"},
+	} {
+		if err := enc.WriteField(field); err != nil {
+			t.Fatal(err)
+		}
+	}
+	if err := s.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block.Bytes(), EndHeaders: true}); err != nil {
+		t.Fatal(err)
+	}
+
+	// wait for the response headers
+	dec := hpack.NewDecoder(4096, nil)
+	for {
+		f, err := s.fr.ReadFrame()
+		if err != nil {
+			t.Fatalf("failed to read the response: %v", err)
+		}
+		switch f := f.(type) {
+		case *http2.HeadersFrame:
+			fields, err := dec.DecodeFull(f.HeaderBlockFragment())
+			if err != nil {
+				t.Fatal(err)
+			}
+			for _, field := range fields {
+				if field.Name == ":status" && field.Value != "200" {
+					t.Fatalf("status = %s, want 200", field.Value)
+				}
+			}
+			return s
+		case *http2.RSTStreamFrame:
+			t.Fatalf("the stream was reset: %v", f.ErrCode)
+		case *http2.GoAwayFrame:
+			t.Fatalf("the connection was closed: %v", f.ErrCode)
+		}
+	}
+}
+
+// echo sends msg on the stream and expects it back.
+func (s *h2Stream) echo(t *testing.T, msg string) {
+	t.Helper()
+	_ = s.conn.SetDeadline(time.Now().Add(streamTestTimeout))
+	if err := s.fr.WriteData(1, false, []byte(msg)); err != nil {
+		t.Fatalf("failed to write to the stream: %v", err)
+	}
+	for s.data.Len() < len(msg) {
+		f, err := s.fr.ReadFrame()
+		if err != nil {
+			t.Fatalf("failed to read from the stream: %v", err)
+		}
+		switch f := f.(type) {
+		case *http2.DataFrame:
+			s.data.Write(f.Data())
+		case *http2.PingFrame:
+			if !f.IsAck() {
+				_ = s.fr.WritePing(true, f.Data)
+			}
+		case *http2.SettingsFrame:
+			if !f.IsAck() {
+				_ = s.fr.WriteSettingsAck()
+			}
+		case *http2.RSTStreamFrame:
+			t.Fatalf("the stream was reset: %v", f.ErrCode)
+		case *http2.GoAwayFrame:
+			t.Fatalf("the connection was closed: %v", f.ErrCode)
+		}
+	}
+	if got := string(s.data.Next(len(msg))); got != msg {
+		t.Fatalf("echo = %q, want %q", got, msg)
+	}
+}
diff --git a/modules/caddyhttp/encode/encode.go b/modules/caddyhttp/encode/encode.go
index 46b383ea..262dea0e 100644
--- a/modules/caddyhttp/encode/encode.go
+++ b/modules/caddyhttp/encode/encode.go
@@ -261,6 +261,16 @@ type responseWriter struct {
 // to actually write the header.
 func (rw *responseWriter) WriteHeader(status int) {
 	rw.statusCode = status
+
+	// 101 Switching Protocols is the final response of an upgrade; the
+	// connection is handed over right after it, so nothing may be
+	// written (or encoded) once it has been sent
+	if status == http.StatusSwitchingProtocols {
+		rw.ResponseWriter.WriteHeader(status)
+		rw.wroteHeader = true
+		return
+	}
+
 	if status == http.StatusPartialContent {
 		rw.disabled = true // partial representations must not be dynamically re-encoded
 	}
diff --git a/modules/caddyhttp/logging.go b/modules/caddyhttp/logging.go
index 17bec7f9..4e1edb2d 100644
--- a/modules/caddyhttp/logging.go
+++ b/modules/caddyhttp/logging.go
@@ -199,6 +199,26 @@ func (sa *StringArray) UnmarshalJSON(b []byte) error {
 	return errors.New("unsupported type")
 }
 
+// accessLog is stored in the request context when the request is
+// access-logged, so handlers can reach the loggers of the request.
+type accessLog struct {
+	server *Server
+	logger *zap.Logger
+}
+
+// AccessLoggers returns the loggers the access log entries of r are
+// emitted to, honoring the server's per-host logger names, or nil if
+// the request is not access-logged. The result does not reference r,
+// so it can be used after the request has been handled.
+func AccessLoggers(r *http.Request) []*zap.Logger {
+	al, ok := r.Context().Value(accessLogCtxKey).(accessLog)
+	if !ok {
+		return nil
+	}
+	// requests are only access logged when the server has log config
+	return al.server.Logs.wrapLogger(al.logger, r)
+}
+
 // errLogValues inspects err and returns the status code
 // to use, the error log message, and any extra fields.
 // If err is a HandlerError, the returned values will
@@ -287,6 +307,9 @@ const (
 	// For adding additional fields to the access logs
 	ExtraLogFieldsCtxKey caddy.CtxKey = "extra_log_fields"
 
+	// Context key holding the access log of the request, if any
+	accessLogCtxKey caddy.CtxKey = "access_log"
+
 	// Variable name used to indicate the logger to be used
 	AccessLoggerNameVarKey string = "access_logger_names"
 )
diff --git a/modules/caddyhttp/responsewriter.go b/modules/caddyhttp/responsewriter.go
index 904c30c0..db6d5013 100644
--- a/modules/caddyhttp/responsewriter.go
+++ b/modules/caddyhttp/responsewriter.go
@@ -70,6 +70,7 @@ type responseRecorder struct {
 	size         int
 	wroteHeader  bool
 	stream       bool
+	hijacked     bool
 
 	readSize *int
 }
@@ -146,7 +147,9 @@ func NewResponseRecorder(w http.ResponseWriter, buf *bytes.Buffer, shouldBuffer
 // ResponseWriter unless the response is to be buffered instead.
 // 1xx responses are never buffered.
 func (rr *responseRecorder) WriteHeader(statusCode int) {
-	if rr.wroteHeader {
+	// once the connection is hijacked the response belongs to the
+	// hijacker; nothing may reach the connection anymore
+	if rr.hijacked || rr.wroteHeader {
 		return
 	}
 
@@ -154,6 +157,15 @@ func (rr *responseRecorder) WriteHeader(statusCode int) {
 	// connections by manually setting headers and writing status 101
 	rr.statusCode = statusCode
 
+	// 101 Switching Protocols is the final response of an upgrade:
+	// it is never buffered and no other status may follow it
+	if statusCode == http.StatusSwitchingProtocols {
+		rr.stream = true
+		rr.wroteHeader = true
+		rr.ResponseWriterWrapper.WriteHeader(statusCode)
+		return
+	}
+
 	// decide whether we should buffer the response
 	if rr.shouldBuffer == nil {
 		rr.stream = true
@@ -173,6 +185,9 @@ func (rr *responseRecorder) WriteHeader(statusCode int) {
 }
 
 func (rr *responseRecorder) Write(data []byte) (int, error) {
+	if rr.hijacked {
+		return 0, http.ErrHijacked
+	}
 	rr.WriteHeader(http.StatusOK)
 	var n int
 	var err error
@@ -187,6 +202,9 @@ func (rr *responseRecorder) Write(data []byte) (int, error) {
 }
 
 func (rr *responseRecorder) ReadFrom(r io.Reader) (int64, error) {
+	if rr.hijacked {
+		return 0, http.ErrHijacked
+	}
 	rr.WriteHeader(http.StatusOK)
 	var n int64
 	var err error
@@ -219,10 +237,13 @@ func (rr *responseRecorder) Buffer() *bytes.Buffer {
 
 // Buffered returns whether rr has decided to buffer the response.
 func (rr *responseRecorder) Buffered() bool {
-	return !rr.stream
+	return !rr.stream && !rr.hijacked
 }
 
 func (rr *responseRecorder) WriteResponse() error {
+	if rr.hijacked {
+		return nil
+	}
 	if rr.statusCode == 0 {
 		// could happen if no handlers actually wrote anything,
 		// and this prevents a panic; status must be > 0
@@ -239,6 +260,9 @@ func (rr *responseRecorder) WriteResponse() error {
 // FlushError will suppress actual flushing if the response is buffered. See:
 // https://github.com/caddyserver/caddy/issues/6144
 func (rr *responseRecorder) FlushError() error {
+	if rr.hijacked {
+		return http.ErrHijacked
+	}
 	if rr.stream {
 		//nolint:bodyclose
 		return http.NewResponseController(rr.ResponseWriterWrapper).Flush()
@@ -258,6 +282,7 @@ func (rr *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
 	if err != nil {
 		return nil, nil, err
 	}
+	rr.hijacked = true
 	// Per http documentation, returned bufio.Writer is empty, but bufio.Read maybe not
 	conn = &hijackedConn{conn, rr}
 	brw.Writer.Reset(conn)
@@ -281,6 +306,14 @@ type hijackedConn struct {
 	rr *responseRecorder
 }
 
+// RawConn returns the connection underneath the size-tracking wrapper,
+// so that long-lived users of the connection can bypass the wrapper
+// and its unsynchronized counters. It deliberately is not named Unwrap:
+// only wrappers created by the response recorder may be stripped off.
+func (hc *hijackedConn) RawConn() net.Conn {
+	return hc.Conn
+}
+
 func (hc *hijackedConn) updateReadSize(n int) {
 	if hc.rr.readSize != nil {
 		*hc.rr.readSize += n
diff --git a/modules/caddyhttp/responsewriter_test.go b/modules/caddyhttp/responsewriter_test.go
index c08ad26a..efe54be1 100644
--- a/modules/caddyhttp/responsewriter_test.go
+++ b/modules/caddyhttp/responsewriter_test.go
@@ -1,13 +1,174 @@
 package caddyhttp
 
 import (
+	"bufio"
 	"bytes"
+	"context"
+	"errors"
 	"io"
+	"net"
 	"net/http"
+	"net/http/httptest"
 	"strings"
 	"testing"
+
+	"go.uber.org/zap"
 )
 
+// hijackableRespWriter is a ResponseWriter mock which records what reaches
+// it and can be hijacked, handing out one end of a pipe.
+type hijackableRespWriter struct {
+	header   http.Header
+	statuses []int
+	body     bytes.Buffer
+	flushes  int
+	conn     net.Conn // the end of the pipe handed out by Hijack
+	peer     net.Conn // the other end of the pipe
+}
+
+func newHijackableRespWriter() *hijackableRespWriter {
+	conn, peer := net.Pipe()
+	return &hijackableRespWriter{header: make(http.Header), conn: conn, peer: peer}
+}
+
+func (w *hijackableRespWriter) Header() http.Header { return w.header }
+func (w *hijackableRespWriter) WriteHeader(statusCode int) {
+	w.statuses = append(w.statuses, statusCode)
+}
+func (w *hijackableRespWriter) Write(p []byte) (int, error) { return w.body.Write(p) }
+func (w *hijackableRespWriter) FlushError() error           { w.flushes++; return nil }
+func (w *hijackableRespWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
+}
+
+// A 101 is the final response of an upgrade: it must not be buffered (even
+// if the buffering decision says so), and no other status may follow it.
+func TestResponseRecorderSwitchingProtocolsIsFinal(t *testing.T) {
+	w := newHijackableRespWriter()
+	asked := false
+	rr := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool {
+		asked = true
+		return true
+	})
+
+	rr.WriteHeader(http.StatusSwitchingProtocols)
+	rr.WriteHeader(http.StatusOK)
+
+	if asked {
+		t.Error("shouldBuffer was consulted for a 101")
+	}
+	if rr.Buffered() {
+		t.Error("a 101 must not be buffered")
+	}
+	if rr.Status() != http.StatusSwitchingProtocols {
+		t.Errorf("Status() = %d, want 101", rr.Status())
+	}
+	if len(w.statuses) != 1 || w.statuses[0] != http.StatusSwitchingProtocols {
+		t.Errorf("statuses written = %v, want [101]", w.statuses)
+	}
+}
+
+// Once the connection is hijacked, nothing may reach the response writer.
+func TestResponseRecorderAfterHijack(t *testing.T) {
+	w := newHijackableRespWriter()
+	defer w.peer.Close()
+	rr := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool { return true })
+
+	conn, _, err := http.NewResponseController(rr).Hijack()
+	if err != nil {
+		t.Fatalf("Hijack() error = %v", err)
+	}
+	defer conn.Close()
+
+	if rr.Buffered() {
+		t.Error("a hijacked recorder must not report a buffered response")
+	}
+
+	rr.WriteHeader(http.StatusInternalServerError)
+	if n, err := rr.Write([]byte("late")); n != 0 || !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("Write() = %d, %v; want 0, %v", n, err, http.ErrHijacked)
+	}
+	if n, err := rr.(io.ReaderFrom).ReadFrom(strings.NewReader("late")); n != 0 || !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("ReadFrom() = %d, %v; want 0, %v", n, err, http.ErrHijacked)
+	}
+	if err := http.NewResponseController(rr).Flush(); !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("Flush() error = %v, want %v", err, http.ErrHijacked)
+	}
+	if err := rr.WriteResponse(); err != nil {
+		t.Errorf("WriteResponse() error = %v, want nil", err)
+	}
+
+	if len(w.statuses) != 0 || w.body.Len() != 0 || w.flushes != 0 {
+		t.Errorf("response writer got statuses %v, body %q, %d flushes after hijack", w.statuses, w.body.String(), w.flushes)
+	}
+}
+
+// Handlers reach the access loggers of a request through its context.
+func TestAccessLoggers(t *testing.T) {
+	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
+	if got := AccessLoggers(req); got != nil {
+		t.Errorf("AccessLoggers() = %v for a request which is not access logged, want nil", got)
+	}
+
+	srv := &Server{Logs: &ServerLogConfig{LoggerNames: map[string]StringArray{"example.com": {"one", "two"}}}}
+	req = req.WithContext(context.WithValue(req.Context(), accessLogCtxKey, accessLog{server: srv, logger: zap.NewNop()}))
+	if got := AccessLoggers(req); len(got) != 2 {
+		t.Errorf("AccessLoggers() returned %d loggers, want the 2 of the host", len(got))
+	}
+}
+
+// A hijack that fails leaves the recorder usable.
+func TestResponseRecorderFailedHijack(t *testing.T) {
+	w := &baseRespWriter{}
+	rr := NewResponseRecorder(w, new(bytes.Buffer), nil)
+
+	if _, _, err := http.NewResponseController(rr).Hijack(); err == nil {
+		t.Fatal("Hijack() on a writer which cannot hijack succeeded")
+	}
+	if _, err := rr.Write([]byte("ok")); err != nil {
+		t.Errorf("Write() after failed hijack error = %v", err)
+	}
+	if got := w.Written(); got != "ok" {
+		t.Errorf("written = %q, want %q", got, "ok")
+	}
+}
+
+// The wrapper around a hijacked connection counts bytes for the access log;
+// RawConn lets long-lived users bypass it.
+func TestHijackedConnRawConn(t *testing.T) {
+	w := newHijackableRespWriter()
+	defer w.peer.Close()
+	rr := NewResponseRecorder(w, nil, nil)
+
+	conn, _, err := http.NewResponseController(rr).Hijack()
+	if err != nil {
+		t.Fatalf("Hijack() error = %v", err)
+	}
+	defer conn.Close()
+
+	raw, ok := conn.(interface{ RawConn() net.Conn })
+	if !ok {
+		t.Fatalf("hijacked conn %T has no RawConn", conn)
+	}
+	if raw.RawConn() != w.conn {
+		t.Fatalf("RawConn() = %v, want the connection which was hijacked", raw.RawConn())
+	}
+
+	go func() { _, _ = io.Copy(io.Discard, w.peer) }()
+	if _, err := conn.Write([]byte("tracked")); err != nil {
+		t.Fatal(err)
+	}
+	if rr.Size() != len("tracked") {
+		t.Errorf("Size() = %d after a write on the wrapper, want %d", rr.Size(), len("tracked"))
+	}
+	if _, err := raw.RawConn().Write([]byte("untracked")); err != nil {
+		t.Fatal(err)
+	}
+	if rr.Size() != len("tracked") {
+		t.Errorf("Size() = %d after a write on the raw conn, want it unchanged at %d", rr.Size(), len("tracked"))
+	}
+}
+
 type responseWriterSpy interface {
 	http.ResponseWriter
 	Written() string
diff --git a/modules/caddyhttp/reverseproxy/caddyfile.go b/modules/caddyhttp/reverseproxy/caddyfile.go
index 12c1177b..3f6115f4 100644
--- a/modules/caddyhttp/reverseproxy/caddyfile.go
+++ b/modules/caddyhttp/reverseproxy/caddyfile.go
@@ -99,6 +99,12 @@ func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error)
 //	    stream_buffer_size <size>
 //	    stream_timeout     <duration>
 //	    stream_close_delay <duration>
+//	    stream_detached
+//	    stream_logs {
+//	        skip_handshake
+//	        logger_name access
+//	        level       <level>
+//	    }
 //	    verbose_logs
 //
 //	    # reporting
@@ -715,6 +721,23 @@ func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
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
+			if err := h.StreamLogs.UnmarshalCaddyfile(d); err != nil {
+				return err
+			}
+
 		case "trusted_proxies":
 			for d.NextArg() {
 				if d.Val() == "private_ranges" {
diff --git a/modules/caddyhttp/reverseproxy/metrics.go b/modules/caddyhttp/reverseproxy/metrics.go
index 24884273..531be856 100644
--- a/modules/caddyhttp/reverseproxy/metrics.go
+++ b/modules/caddyhttp/reverseproxy/metrics.go
@@ -16,9 +16,107 @@ import (
 var reverseProxyMetrics = struct {
 	once             sync.Once
 	upstreamsHealthy *prometheus.GaugeVec
+	streams          *streamMetrics
 	logger           *zap.Logger
 }{}
 
+// Results a stream can be accounted with in the streams_closed_total metric.
+const (
+	streamResultClosed  = "closed"  // both directions ended cleanly
+	streamResultTimeout = "timeout" // stream_timeout elapsed
+	streamResultError   = "error"   // copy error, or a connection was closed under the stream
+	streamResultReload  = "reload"  // closed because of a config reload
+)
+
+// streamMetrics holds the metrics of upgraded streams. The collectors are
+// shared by all handlers and outlive config reloads, because detached
+// streams outlive the config that created them.
+type streamMetrics struct {
+	active   *prometheus.GaugeVec
+	closed   *prometheus.CounterVec
+	duration *prometheus.HistogramVec
+	bytes    *prometheus.CounterVec
+}
+
+func newStreamMetrics() *streamMetrics {
+	const ns, sub = "caddy", "reverse_proxy"
+	return &streamMetrics{
+		active: prometheus.NewGaugeVec(prometheus.GaugeOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "streams_active",
+			Help:      "Number of upgraded streams (e.g. WebSockets) currently open.",
+		}, []string{"upstream"}),
+		closed: prometheus.NewCounterVec(prometheus.CounterOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "streams_closed_total",
+			Help:      "Number of upgraded streams that were closed, by result.",
+		}, []string{"upstream", "result"}),
+		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "stream_duration_seconds",
+			Help:      "How long upgraded streams were open.",
+			Buckets:   []float64{0.1, 1, 10, 60, 300, 1800, 3600, 86400},
+		}, []string{"upstream"}),
+		bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "stream_bytes_total",
+			Help:      "Bytes copied through upgraded streams, by direction relative to the backend.",
+		}, []string{"upstream", "direction"}),
+	}
+}
+
+func (m *streamMetrics) register(registry *prometheus.Registry) {
+	for _, c := range []prometheus.Collector{m.active, m.closed, m.duration, m.bytes} {
+		// duplicate registration happens when multiple handlers are provisioned;
+		// the registered collector is the same one, so the error is harmless
+		if err := registry.Register(c); err != nil {
+			if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
+				panic(err)
+			}
+		}
+	}
+}
+
+// streamAccount accounts a single stream in the stream metrics. Nothing is
+// recorded when the metrics are not initialized (for example in unit tests
+// which build a Handler by hand).
+type streamAccount struct {
+	metrics  *streamMetrics
+	upstream string
+	start    time.Time
+	once     sync.Once
+}
+
+// begin starts accounting a stream to upstream.
+func (m *streamMetrics) begin(upstream string) *streamAccount {
+	if m != nil {
+		m.active.WithLabelValues(upstream).Inc()
+	}
+	return &streamAccount{metrics: m, upstream: upstream, start: time.Now()}
+}
+
+// finish records the end of the stream. Only the first call has an effect,
+// so a stream is accounted exactly once however many paths end it.
+func (a *streamAccount) finish(result string, toBackend, fromBackend int64) (took time.Duration) {
+	a.once.Do(func() {
+		took = time.Since(a.start)
+		m := a.metrics
+		if m == nil {
+			return
+		}
+		m.active.WithLabelValues(a.upstream).Dec()
+		m.closed.WithLabelValues(a.upstream, result).Inc()
+		m.duration.WithLabelValues(a.upstream).Observe(took.Seconds())
+		m.bytes.WithLabelValues(a.upstream, "to_backend").Add(float64(toBackend))
+		m.bytes.WithLabelValues(a.upstream, "from_backend").Add(float64(fromBackend))
+	})
+	return took
+}
+
 func initReverseProxyMetrics(handler *Handler, registry *prometheus.Registry) {
 	const ns, sub = "caddy", "reverse_proxy"
 
@@ -30,6 +128,7 @@ func initReverseProxyMetrics(handler *Handler, registry *prometheus.Registry) {
 			Name:      "upstreams_healthy",
 			Help:      "Health status of reverse proxy upstreams.",
 		}, upstreamsLabels)
+		reverseProxyMetrics.streams = newStreamMetrics()
 	})
 
 	// duplicate registration could happen if multiple sites with reverse proxy are configured; so ignore the error because
@@ -42,6 +141,7 @@ func initReverseProxyMetrics(handler *Handler, registry *prometheus.Registry) {
 		}) {
 		panic(err)
 	}
+	reverseProxyMetrics.streams.register(registry)
 
 	reverseProxyMetrics.logger = handler.logger.Named("reverse_proxy.metrics")
 }
diff --git a/modules/caddyhttp/reverseproxy/reverseproxy.go b/modules/caddyhttp/reverseproxy/reverseproxy.go
index 0838c8be..0a6c3d45 100644
--- a/modules/caddyhttp/reverseproxy/reverseproxy.go
+++ b/modules/caddyhttp/reverseproxy/reverseproxy.go
@@ -199,6 +199,22 @@ type Handler struct {
 	// by the previous config closing. Default: no delay.
 	StreamCloseDelay caddy.Duration `json:"stream_close_delay,omitempty"`
 
+	// If true, upgraded streams (WebSockets and other Upgrade tunnels)
+	// are detached from the config lifecycle: they are not closed when
+	// the proxy config is unloaded (and stream_close_delay does not
+	// apply to them), and they do not keep the handler alive. When a
+	// config reload removes an upstream, only the streams to that
+	// upstream are closed. Streams to dynamic upstreams, which are not
+	// part of any config, are never closed by a reload. HTTP/2 extended
+	// CONNECT streams share the same lifecycle but still occupy their
+	// request. Default: false, i.e. streams are closed on reload.
+	StreamDetached bool `json:"stream_detached,omitempty"`
+
+	// Configures the logs of upgraded streams. By default, the upgrade
+	// handshake is access logged like any other request and the close of
+	// a stream is logged at DEBUG level by the proxy's own logger.
+	StreamLogs *StreamLogs `json:"stream_logs,omitempty"`
+
 	// If configured, rewrites the copy of the upstream request.
 	// Allows changing the request method and URI (path and query).
 	// Since the rewrite is applied to the copy, it does not persist
@@ -263,7 +279,10 @@ type Handler struct {
 	connectionsCloseTimer *time.Timer
 	connectionsMu         *sync.Mutex
 
-	ctx    caddy.Context
+	streamLogLevel zapcore.Level
+	streamMetrics  *streamMetrics
+
+	ctx   caddy.Context
 	logger *zap.Logger
 	events *caddyevents.App
 }
@@ -288,6 +307,13 @@ func (h *Handler) Provision(ctx caddy.Context) error {
 	h.connections = make(map[io.ReadWriteCloser]openConnection)
 	h.connectionsMu = new(sync.Mutex)
 
+	h.streamLogLevel = zapcore.DebugLevel
+	if h.StreamLogs != nil {
+		if err := h.StreamLogs.validate(&h.streamLogLevel); err != nil {
+			return fmt.Errorf("stream_logs: %v", err)
+		}
+	}
+
 	// warn about unsafe buffering config
 	if h.RequestBuffers == -1 || h.ResponseBuffers == -1 {
 		h.logger.Warn("UNLIMITED BUFFERING: buffering is enabled without any cap on buffer size, which can result in OOM crashes")
@@ -452,6 +478,7 @@ func (h *Handler) Provision(ctx caddy.Context) error {
 
 	upstreamHealthyUpdater := newMetricsUpstreamsHealthyUpdater(h, ctx)
 	upstreamHealthyUpdater.init()
+	h.streamMetrics = reverseProxyMetrics.streams
 
 	return nil
 }
@@ -462,7 +489,13 @@ func (h *Handler) Cleanup() error {
 
 	// remove hosts from our config from the pool
 	for _, upstream := range h.Upstreams {
-		_, _ = hosts.Delete(upstream.String())
+		removed, _ := hosts.Delete(upstream.String())
+		if removed {
+			// no config uses the upstream anymore (the new config is provisioned
+			// before the old one is cleaned up), so streams detached from their
+			// config must not outlive it
+			detachedStreams.closeUpstream(upstream.String())
+		}
 	}
 
 	return err
@@ -1283,9 +1316,7 @@ func (h *Handler) finalizeResponse(
 
 	// deal with 101 Switching Protocols responses: (WebSocket, h2c, etc)
 	if res.StatusCode == http.StatusSwitchingProtocols {
-		var wg sync.WaitGroup
-		h.handleUpgradeResponse(logger, &wg, rw, req, res)
-		wg.Wait()
+		h.handleUpgradeResponse(logger, rw, req, res)
 		return nil
 	}
 
diff --git a/modules/caddyhttp/reverseproxy/stream_detached_test.go b/modules/caddyhttp/reverseproxy/stream_detached_test.go
new file mode 100644
index 00000000..736ea253
--- /dev/null
+++ b/modules/caddyhttp/reverseproxy/stream_detached_test.go
@@ -0,0 +1,960 @@
+package reverseproxy
+
+import (
+	"bufio"
+	"encoding/json"
+	"errors"
+	"io"
+	"net"
+	"net/http"
+	"net/http/httptest"
+	"os"
+	"strings"
+	"sync"
+	"sync/atomic"
+	"testing"
+	"time"
+
+	"github.com/prometheus/client_golang/prometheus"
+	"github.com/prometheus/client_golang/prometheus/testutil"
+	dto "github.com/prometheus/client_model/go"
+	"go.uber.org/zap"
+	"go.uber.org/zap/zapcore"
+	"go.uber.org/zap/zaptest/observer"
+
+	"github.com/caddyserver/caddy/v2"
+	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
+	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
+)
+
+const testStreamTimeout = 5 * time.Second
+
+func TestStreamOptionsCaddyfile(t *testing.T) {
+	for i, tc := range []struct {
+		input   string
+		check   func(h *Handler) bool
+		wantErr bool
+	}{
+		{
+			input: `reverse_proxy localhost:8080`,
+			check: func(h *Handler) bool { return !h.StreamDetached && h.StreamLogs == nil },
+		},
+		{
+			input: `reverse_proxy localhost:8080 {
+				stream_detached
+			}`,
+			check: func(h *Handler) bool { return h.StreamDetached && h.StreamLogs == nil },
+		},
+		{
+			input: `reverse_proxy localhost:8080 {
+				stream_logs {
+				}
+			}`,
+			check: func(h *Handler) bool { return h.StreamLogs != nil && *h.StreamLogs == StreamLogs{} },
+		},
+		{
+			input: `reverse_proxy localhost:8080 {
+				stream_detached
+				stream_logs {
+					skip_handshake
+					logger_name access
+					level       info
+				}
+			}`,
+			check: func(h *Handler) bool {
+				return h.StreamDetached && *h.StreamLogs == StreamLogs{SkipHandshake: true, LoggerName: "access", Level: "info"}
+			},
+		},
+		{input: `reverse_proxy localhost:8080 { stream_detached true }`, wantErr: true},
+		{input: `reverse_proxy localhost:8080 { stream_logs true }`, wantErr: true},
+		{input: `reverse_proxy localhost:8080 { stream_logs { skip_handshake true } }`, wantErr: true},
+		{input: `reverse_proxy localhost:8080 { stream_logs { logger_name } }`, wantErr: true},
+		{input: `reverse_proxy localhost:8080 { stream_logs { logger_name access extra } }`, wantErr: true},
+		{input: `reverse_proxy localhost:8080 { stream_logs { level } }`, wantErr: true},
+		{input: `reverse_proxy localhost:8080 { stream_logs { level info extra } }`, wantErr: true},
+		{input: `reverse_proxy localhost:8080 { stream_logs { bogus } }`, wantErr: true},
+	} {
+		h := new(Handler)
+		err := h.UnmarshalCaddyfile(caddyfile.NewTestDispenser(tc.input))
+		if tc.wantErr {
+			if err == nil {
+				t.Errorf("Test %d: expected an error but got none", i)
+			}
+			continue
+		}
+		if err != nil {
+			t.Errorf("Test %d: unexpected error: %v", i, err)
+			continue
+		}
+		if !tc.check(h) {
+			t.Errorf("Test %d: unexpected result: detached=%v logs=%+v", i, h.StreamDetached, h.StreamLogs)
+		}
+	}
+}
+
+func TestStreamOptionsJSON(t *testing.T) {
+	var h Handler
+	err := json.Unmarshal([]byte(`{
+		"stream_detached": true,
+		"stream_logs": {"skip_handshake": true, "logger_name": "access", "level": "warn"}
+	}`), &h)
+	if err != nil {
+		t.Fatal(err)
+	}
+	if !h.StreamDetached || *h.StreamLogs != (StreamLogs{SkipHandshake: true, LoggerName: "access", Level: "warn"}) {
+		t.Fatalf("unexpected result: detached=%v logs=%+v", h.StreamDetached, h.StreamLogs)
+	}
+
+	out, err := json.Marshal(Handler{})
+	if err != nil {
+		t.Fatal(err)
+	}
+	if strings.Contains(string(out), "stream_detached") || strings.Contains(string(out), "stream_logs") {
+		t.Errorf("unset options must be omitted from JSON: %s", out)
+	}
+}
+
+func TestStreamLogsValidate(t *testing.T) {
+	for i, tc := range []struct {
+		logs      StreamLogs
+		wantLevel zapcore.Level
+		wantErr   bool
+	}{
+		{logs: StreamLogs{}, wantLevel: zapcore.DebugLevel},
+		{logs: StreamLogs{LoggerName: "access"}, wantLevel: zapcore.DebugLevel},
+		{logs: StreamLogs{Level: "info"}, wantLevel: zapcore.InfoLevel},
+		{logs: StreamLogs{Level: "WARN", LoggerName: "access"}, wantLevel: zapcore.WarnLevel},
+		{logs: StreamLogs{LoggerName: "other"}, wantErr: true},
+		{logs: StreamLogs{Level: "loud"}, wantErr: true},
+	} {
+		level := zapcore.DebugLevel
+		err := tc.logs.validate(&level)
+		if tc.wantErr {
+			if err == nil {
+				t.Errorf("Test %d: expected an error but got none", i)
+			}
+			continue
+		}
+		if err != nil {
+			t.Errorf("Test %d: unexpected error: %v", i, err)
+		} else if level != tc.wantLevel {
+			t.Errorf("Test %d: level = %v, want %v", i, level, tc.wantLevel)
+		}
+	}
+}
+
+// closeTracker is a connection which records being closed.
+type closeTracker struct {
+	closed      atomic.Bool
+	closeErr    error
+	gracefulErr error
+	graceful    atomic.Bool
+}
+
+func (c *closeTracker) Read([]byte) (int, error)    { return 0, io.EOF }
+func (c *closeTracker) Write(p []byte) (int, error) { return len(p), nil }
+func (c *closeTracker) Close() error {
+	c.closed.Store(true)
+	return c.closeErr
+}
+func (c *closeTracker) gracefulClose() error {
+	c.graceful.Store(true)
+	return c.gracefulErr
+}
+
+func TestStreamRegistry(t *testing.T) {
+	r := &streamRegistry{streams: make(map[string]map[*upgradedStream]struct{})}
+	newStream := func() (*upgradedStream, *closeTracker, *closeTracker) {
+		user, backend := new(closeTracker), new(closeTracker)
+		return &upgradedStream{user: user, backend: backend, gracefulUser: user.gracefulClose}, user, backend
+	}
+
+	a1, a1User, a1Backend := newStream()
+	a2, a2User, _ := newStream()
+	b1, b1User, _ := newStream()
+	removeA1 := r.add("a", a1)
+	removeA2 := r.add("a", a2)
+	removeB1 := r.add("b", b1)
+
+	if got := r.count("a"); got != 2 {
+		t.Fatalf("count(a) = %d, want 2", got)
+	}
+	if got := r.count("none"); got != 0 {
+		t.Fatalf("count(none) = %d, want 0", got)
+	}
+
+	if got := r.closeUpstream("none"); got != 0 {
+		t.Errorf("closeUpstream(none) = %d, want 0", got)
+	}
+	if got := r.closeUpstream("a"); got != 2 {
+		t.Errorf("closeUpstream(a) = %d, want 2", got)
+	}
+	for name, s := range map[string]*closeTracker{"a1 user": a1User, "a1 backend": a1Backend, "a2 user": a2User} {
+		if !s.closed.Load() {
+			t.Errorf("%s was not closed", name)
+		}
+	}
+	if !a1User.graceful.Load() {
+		t.Error("the graceful close of a1 was not attempted")
+	}
+	if !a1.reloaded.Load() || !a2.reloaded.Load() {
+		t.Error("closed streams were not marked as closed by a reload")
+	}
+	if b1User.closed.Load() || b1.reloaded.Load() {
+		t.Error("a stream to another upstream was closed")
+	}
+
+	// streams unregister themselves when they end
+	removeA1()
+	if got := r.count("a"); got != 1 {
+		t.Errorf("count(a) = %d after one stream ended, want 1", got)
+	}
+	removeA2()
+	removeB1()
+	if len(r.streams) != 0 {
+		t.Errorf("registry still has entries after all streams ended: %v", r.streams)
+	}
+}
+
+func TestStreamCloseErrors(t *testing.T) {
+	gracefulErr := errors.New("graceful failed")
+	closeErr := errors.New("close failed")
+
+	for name, tc := range map[string]struct {
+		user, backend *closeTracker
+		want          error
+	}{
+		"no errors":             {user: &closeTracker{}, backend: &closeTracker{}},
+		"graceful error":        {user: &closeTracker{gracefulErr: gracefulErr}, backend: &closeTracker{}, want: gracefulErr},
+		"close error":           {user: &closeTracker{}, backend: &closeTracker{closeErr: closeErr}, want: closeErr},
+		"first error is kept":   {user: &closeTracker{gracefulErr: gracefulErr}, backend: &closeTracker{closeErr: closeErr}, want: gracefulErr},
+		"close after graceful":  {user: &closeTracker{gracefulErr: gracefulErr, closeErr: closeErr}, backend: &closeTracker{}, want: gracefulErr},
+		"backend close errored": {user: &closeTracker{closeErr: closeErr}, backend: &closeTracker{closeErr: errors.New("second")}, want: closeErr},
+	} {
+		t.Run(name, func(t *testing.T) {
+			s := &upgradedStream{user: tc.user, backend: tc.backend, gracefulUser: tc.user.gracefulClose}
+			if err := s.closeForReload(); !errors.Is(err, tc.want) {
+				t.Errorf("closeForReload() error = %v, want %v", err, tc.want)
+			}
+			if !tc.user.closed.Load() || !tc.backend.closed.Load() {
+				t.Error("both connections must be closed whatever the errors")
+			}
+		})
+	}
+}
+
+func histogramCount(t *testing.T, o prometheus.Observer) uint64 {
+	t.Helper()
+	var m dto.Metric
+	if err := o.(prometheus.Metric).Write(&m); err != nil {
+		t.Fatal(err)
+	}
+	return m.GetHistogram().GetSampleCount()
+}
+
+func TestStreamAccountExactlyOnce(t *testing.T) {
+	m := newStreamMetrics()
+	a := m.begin("up")
+
+	if got := testutil.ToFloat64(m.active.WithLabelValues("up")); got != 1 {
+		t.Fatalf("active streams = %v while open, want 1", got)
+	}
+
+	if took := a.finish(streamResultClosed, 5, 7); took <= 0 {
+		t.Errorf("finish() reported a duration of %v for the first call", took)
+	}
+	// every other path that ends the stream must be ignored
+	if took := a.finish(streamResultError, 100, 100); took != 0 {
+		t.Errorf("finish() reported a duration of %v for a repeated call", took)
+	}
+
+	if got := testutil.ToFloat64(m.active.WithLabelValues("up")); got != 0 {
+		t.Errorf("active streams = %v after close, want 0", got)
+	}
+	if got := testutil.ToFloat64(m.closed.WithLabelValues("up", streamResultClosed)); got != 1 {
+		t.Errorf("closed{closed} = %v, want 1", got)
+	}
+	if got := testutil.ToFloat64(m.closed.WithLabelValues("up", streamResultError)); got != 0 {
+		t.Errorf("closed{error} = %v, want 0", got)
+	}
+	if got := testutil.ToFloat64(m.bytes.WithLabelValues("up", "to_backend")); got != 5 {
+		t.Errorf("bytes to backend = %v, want 5", got)
+	}
+	if got := testutil.ToFloat64(m.bytes.WithLabelValues("up", "from_backend")); got != 7 {
+		t.Errorf("bytes from backend = %v, want 7", got)
+	}
+	if got := histogramCount(t, m.duration.WithLabelValues("up")); got != 1 {
+		t.Errorf("duration observations = %d, want 1", got)
+	}
+}
+
+func TestStreamAccountWithoutMetrics(t *testing.T) {
+	var m *streamMetrics
+	a := m.begin("up")
+	if took := a.finish(streamResultClosed, 1, 2); took <= 0 {
+		t.Errorf("finish() reported a duration of %v", took)
+	}
+}
+
+func TestStreamMetricsRegister(t *testing.T) {
+	m := newStreamMetrics()
+	registry := prometheus.NewRegistry()
+	m.register(registry)
+	m.register(registry) // registering the same collectors again is fine
+
+	m.begin("up").finish(streamResultTimeout, 1, 1)
+	families, err := registry.Gather()
+	if err != nil {
+		t.Fatal(err)
+	}
+	names := make(map[string]bool)
+	for _, f := range families {
+		names[f.GetName()] = true
+	}
+	for _, want := range []string{
+		"caddy_reverse_proxy_streams_active",
+		"caddy_reverse_proxy_streams_closed_total",
+		"caddy_reverse_proxy_stream_duration_seconds",
+		"caddy_reverse_proxy_stream_bytes_total",
+	} {
+		if !names[want] {
+			t.Errorf("metric %s is not exposed", want)
+		}
+	}
+
+	// a conflicting registration is a bug, not something to ignore
+	conflicting := prometheus.NewRegistry()
+	conflicting.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{
+		Namespace: "caddy", Subsystem: "reverse_proxy", Name: "streams_active", Help: "conflict",
+	}))
+	defer func() {
+		if recover() == nil {
+			t.Error("expected a panic on a conflicting registration")
+		}
+	}()
+	m.register(conflicting)
+}
+
+// tcpPair returns two ends of a TCP connection.
+func tcpPair(t *testing.T) (a, b *net.TCPConn) {
+	t.Helper()
+	ln, err := net.Listen("tcp", "127.0.0.1:0")
+	if err != nil {
+		t.Fatal(err)
+	}
+	defer ln.Close()
+	accepted := make(chan net.Conn, 1)
+	go func() {
+		c, _ := ln.Accept()
+		accepted <- c
+	}()
+	c1, err := net.Dial("tcp", ln.Addr().String())
+	if err != nil {
+		t.Fatal(err)
+	}
+	c2 := <-accepted
+	if c2 == nil {
+		t.Fatal("accept failed")
+	}
+	t.Cleanup(func() { c1.Close(); c2.Close() })
+	return c1.(*net.TCPConn), c2.(*net.TCPConn)
+}
+
+type failingWriter struct{}
+
+func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }
+
+type failingConn struct {
+	readErr, writeErr error
+}
+
+func (c failingConn) Read([]byte) (int, error) {
+	if c.readErr != nil {
+		return 0, c.readErr
+	}
+	time.Sleep(time.Hour)
+	return 0, io.EOF
+}
+func (c failingConn) Write(p []byte) (int, error) {
+	if c.writeErr != nil {
+		return 0, c.writeErr
+	}
+	return len(p), nil
+}
+func (c failingConn) Close() error { return nil }
+
+// streamHarness runs an upgradedStream between two TCP connections, the
+// ends of which are held by the test.
+type streamHarness struct {
+	s           *upgradedStream
+	clientPeer  *net.TCPConn // the client's end of the connection to the proxy
+	backendPeer *net.TCPConn // the backend's end of the connection from the proxy
+	metrics     *streamMetrics
+	logs        *observer.ObservedLogs
+	done        chan struct{}
+	doneCalls   atomic.Int32
+}
+
+func newStreamHarness(t *testing.T, brw *bufio.ReadWriter) *streamHarness {
+	t.Helper()
+	user, clientPeer := tcpPair(t)
+	backend, backendPeer := tcpPair(t)
+	if brw == nil {
+		brw = bufio.NewReadWriter(bufio.NewReader(strings.NewReader("")), bufio.NewWriter(io.Discard))
+	}
+	core, logs := observer.New(zapcore.DebugLevel)
+	hn := &streamHarness{
+		clientPeer:  clientPeer,
+		backendPeer: backendPeer,
+		metrics:     newStreamMetrics(),
+		logs:        logs,
+		done:        make(chan struct{}),
+	}
+	hn.s = &upgradedStream{
+		user:         user,
+		backend:      backend,
+		brw:          brw,
+		upstream:     "up",
+		logger:       zap.New(core),
+		closeLoggers: []*zap.Logger{zap.New(core)},
+		level:        zapcore.DebugLevel,
+		account:      hn.metrics.begin("up"),
+		onDone:       func() { hn.doneCalls.Add(1) },
+	}
+	return hn
+}
+
+func (hn *streamHarness) start(reqDone <-chan struct{}) {
+	go func() {
+		hn.s.run(reqDone)
+		close(hn.done)
+	}()
+}
+
+func (hn *streamHarness) wait(t *testing.T) {
+	t.Helper()
+	select {
+	case <-hn.done:
+	case <-time.After(testStreamTimeout):
+		t.Fatal("timed out waiting for the stream to end")
+	}
+}
+
+func (hn *streamHarness) closedWith(result string) float64 {
+	return testutil.ToFloat64(hn.metrics.closed.WithLabelValues("up", result))
+}
+
+// assertAccounted checks the stream was accounted exactly once, with result.
+func (hn *streamHarness) assertAccounted(t *testing.T, result string) {
+	t.Helper()
+	for _, r := range []string{streamResultClosed, streamResultTimeout, streamResultError, streamResultReload} {
+		want := 0.0
+		if r == result {
+			want = 1
+		}
+		if got := hn.closedWith(r); got != want {
+			t.Errorf("closed{%s} = %v, want %v", r, got, want)
+		}
+	}
+	if got := testutil.ToFloat64(hn.metrics.active.WithLabelValues("up")); got != 0 {
+		t.Errorf("active streams = %v after the stream ended, want 0", got)
+	}
+	if got := hn.doneCalls.Load(); got != 1 {
+		t.Errorf("onDone called %d times, want 1", got)
+	}
+	entries := hn.logs.FilterMessage("stream closed").All()
+	if len(entries) != 1 {
+		t.Fatalf("got %d stream close logs, want 1", len(entries))
+	}
+	if got := entries[0].ContextMap()["result"]; got != result {
+		t.Errorf("logged result = %v, want %s", got, result)
+	}
+}
+
+func mustReadFull(t *testing.T, c net.Conn, want string) {
+	t.Helper()
+	_ = c.SetReadDeadline(time.Now().Add(testStreamTimeout))
+	buf := make([]byte, len(want))
+	if _, err := io.ReadFull(c, buf); err != nil {
+		t.Fatalf("read failed:
... (truncated; 36483 more characters; read the files)
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

## Coverage map

Then fill `coverage` with one entry per acceptance criterion marked `[behavior]` above. For
each, name the single test of this task's test files whose assertion proves the criterion:
`test` as a pytest node id (`tests/path.py::test_name`), `line` of the decisive assertion,
`status` `covered` when that assertion would fail if the criterion were broken, `partial` when
a test touches the behaviour but would not fail (asserts only the happy path, or a weaker
property), `uncovered` when no test exercises it. Read the test body: do not infer coverage
from a test's name. `partial` and `uncovered` are blocking, so when you mark one, say in
`note` what test would prove it.

`verdict` is `request_changes` if and only if there is at least one `blocking` finding or a
criterion that is not `covered`.
