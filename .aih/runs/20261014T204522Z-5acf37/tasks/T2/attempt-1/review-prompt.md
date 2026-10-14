You are reviewing one task of an approved plan, implemented by a different session whose
reasoning you have not seen. You can read the repository (the change is already applied in this
worktree) but not edit it or run commands. The harness has already verified mechanically that the
diff stays within the task's files, that the changed tests fail without the production change and
pass with it, and that the whole suite is green. Do not re-check those. Your job is judgement.

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



## Task T2 (T1)

Add stream_detached and stream_logs to reverse_proxy with a process-wide registry of detached streams that is pruned on reload by upstream. Add stream metrics with once-only accounting. Make response writers hijack-safe (no writes after hijack, 101 final, raw-conn copy). Caddyfile parsing is tested by a Go unit test in the reverseproxy package; no .caddyfiletest data file is added. Order of work: (1) config fields and Caddyfile parsing; (2) registry and metrics in streaming.go/metrics.go; (3) wire Provision/Cleanup; (4) writer fixes in responsewriter/encode/intercept and logging; (5) unit tests; (6) integration tests.

Acceptance criteria:
- ACC12 [behavior]: With stream_detached set, an established WebSocket stream stays open and keeps echoing after a config reload that keeps its upstream. TestStreamDetachedSurvivesReload.
- ACC13 [behavior]: With stream_detached set, a reload that removes a stream's upstream closes only the streams of that upstream; streams on retained upstreams stay open. TestStreamDetachedClosesRemovedUpstream.
- ACC14 [behavior]: Without stream_detached, a reload closes upgraded streams, immediately or after stream_close_delay. TestStreamDefaultClosedOnReload.
- ACC15 [behavior]: stream_logs parses in the Caddyfile and JSON (skip_handshake, level, default DEBUG). skip_handshake suppresses the handshake access log entry, and the stream close log is emitted with logger_name access. TestStreamLogsConfig and TestDetachedStreamAccessLog.
- ACC16 [behavior]: After Hijack, the response recorder and the encode writer drop any WriteHeader or Write; a 101 is treated as final. TestResponseRecorderNoWriteAfterHijack, TestEncodeUpgradeFinal101.
- ACC17 [behavior]: A WebSocket upgrade works through the encode handler and through intercept with handle_response, and the proxy copies on the raw connection, not through the recorder's hijackedConn. TestUpgradeThroughEncode, TestUpgradeThroughIntercept.
- ACC18 [behavior]: HTTP/2 extended CONNECT WebSocket works over h2 with a real h2 client, and HTTP/1.1 upgrades work in detached mode. TestExtendedConnectH2, TestDetachedHTTP11Upgrade.
- ACC19 [behavior]: Metrics report active streams, closed streams by result, duration and bytes in each direction. Each stream is counted exactly once even when several close paths race. TestStreamMetricsAccountedOnce.
- ACC20 [behavior]: Caddyfile stream_detached and stream_logs { skip_handshake; level } parse into the Handler fields (StreamDetached, StreamLogs) and unknown subdirectives are rejected. TestUnmarshalCaddyfileStreamDetached in the reverseproxy package.
- ACC21 [other]: Doc comments for JSON fields and the Caddyfile syntax block describe stream_detached and stream_logs.
- ACC22 [behavior]: Unit tests cover the detached registry and its upstream-removal pruning, and the stream finish-once accounting. TestDetachedRegistryPrune, TestStreamFinishOnce.

Files declared for the task:
- modules/caddyhttp/reverseproxy/reverseproxy.go (production)
- modules/caddyhttp/reverseproxy/streaming.go (production)
- modules/caddyhttp/reverseproxy/caddyfile.go (production)
- modules/caddyhttp/reverseproxy/metrics.go (production)
- modules/caddyhttp/responsewriter.go (production)
- modules/caddyhttp/encode/encode.go (production)
- modules/caddyhttp/intercept/intercept.go (production)
- modules/caddyhttp/server.go (production)
- modules/caddyhttp/reverseproxy/streaming_test.go (test)
- modules/caddyhttp/reverseproxy/caddyfile_test.go (test)
- modules/caddyhttp/responsewriter_test.go (test)
- modules/caddyhttp/encode/encode_test.go (test)
- caddytest/integration/stream_test.go (test)
- caddytest/integration/intercept_test.go (test)

Test files of this task that the change modified (existing tests, declared by the plan):
- caddytest/integration/intercept_test.go
- caddytest/integration/stream_test.go
- modules/caddyhttp/encode/encode_test.go
- modules/caddyhttp/responsewriter_test.go
- modules/caddyhttp/reverseproxy/streaming_test.go

Test results: selected tests green; red proven; suite green

## The diff

```diff
diff --git a/caddytest/integration/intercept_test.go b/caddytest/integration/intercept_test.go
index c1b33b96..26566c40 100644
--- a/caddytest/integration/intercept_test.go
+++ b/caddytest/integration/intercept_test.go
@@ -1,6 +1,7 @@
 package integration
 
 import (
+	"fmt"
 	"testing"
 
 	"github.com/caddyserver/caddy/v2/caddytest"
@@ -84,6 +85,47 @@ func TestInterceptReplaceStatusWithoutMatcher(t *testing.T) {
 	tester.AssertGetResponse("http://localhost:9080/forbidden", 200, "denied")
 }
 
+// TestUpgradeThroughIntercept checks that upgrades go through intercept with
+// a handle_response that would match any response: the 101 is final, it is not
+// buffered nor handled, and the proxy copies bytes on the raw connection.
+func TestUpgradeThroughIntercept(t *testing.T) {
+	backend := startUpgradeBackend(t)
+	tester := caddytest.NewTester(t)
+
+	for name, proxyOptions := range map[string]string{"default": "", "detached": "stream_detached"} {
+		t.Run(name, func(t *testing.T) {
+			tester.InitServer(streamTestGlobals+fmt.Sprintf(`
+			localhost:9080 {
+				intercept {
+					handle_response {
+						header intercepted yes
+						respond "intercepted" 200
+					}
+				}
+				reverse_proxy %s {
+					%s
+				}
+			}
+			`, backend, proxyOptions), "caddyfile")
+
+			// responses are intercepted...
+			r, _ := tester.AssertGetResponse("http://localhost:9080/plain", 200, "intercepted")
+			if r.Header.Get("intercepted") != "yes" {
+				t.Errorf(`header "intercepted" value is not "yes": %s`, r.Header.Get("intercepted"))
+			}
+
+			// ...but not the switch to a stream
+			client := dialUpgrade(t, "/", nil)
+			if got := client.resp.Header.Get("Intercepted"); got != "" {
+				t.Errorf("the 101 response was handled by intercept: header %q", got)
+			}
+			client.echo(t, "through intercept")
+			client.echo(t, "and again")
+			client.conn.Close()
+		})
+	}
+}
+
 func TestInterceptReplaceStatusNotMatched(t *testing.T) {
 	tester := caddytest.NewTester(t)
 	tester.InitServer(`{
diff --git a/caddytest/integration/stream_test.go b/caddytest/integration/stream_test.go
index 0f960436..6b634937 100644
--- a/caddytest/integration/stream_test.go
+++ b/caddytest/integration/stream_test.go
@@ -1,20 +1,31 @@
 package integration
 
 import (
+	"bufio"
+	"bytes"
 	"compress/gzip"
 	"context"
 	"crypto/rand"
+	"crypto/tls"
+	"encoding/json"
+	"errors"
 	"fmt"
 	"io"
+	"net"
 	"net/http"
+	"net/http/httptest"
 	"net/http/httputil"
 	"net/url"
+	"os"
+	"os/exec"
+	"path/filepath"
 	"strings"
 	"testing"
 	"time"
 
 	"golang.org/x/net/http2"
 	"golang.org/x/net/http2/h2c"
+	"golang.org/x/net/http2/hpack"
 
 	"github.com/caddyserver/caddy/v2/caddytest"
 )
@@ -438,3 +449,668 @@ func GenerateRandomString(n int) (string, error) {
 	}
 	return string(bytes), nil
 }
+
+// startUpgradeBackend starts a backend that switches to an echo stream when it
+// receives a WebSocket upgrade request, and answers other requests with
+// "backend plain". It returns the address of the backend.
+func startUpgradeBackend(t *testing.T) string {
+	t.Helper()
+	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
+		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
+			// flushed so that the response is chunked and has no Content-Length,
+			// which handlers that replace the response would not replace
+			fmt.Fprint(w, "backend plain")
+			_ = http.NewResponseController(w).Flush()
+			return
+		}
+		conn, brw, err := http.NewResponseController(w).Hijack()
+		if err != nil {
+			return
+		}
+		defer conn.Close()
+		if _, err := io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"); err != nil {
+			return
+		}
+		_, _ = io.Copy(conn, brw.Reader)
+	}))
+	t.Cleanup(srv.Close)
+	return srv.Listener.Addr().String()
+}
+
+// streamTestGlobals are the global options shared by the stream tests.
+const streamTestGlobals = `{
+	skip_install_trust
+	admin localhost:2999
+	http_port     9080
+	https_port    9443
+	grace_period  1ns
+}
+`
+
+// upgradeClient is a client of an upgraded stream, speaking to the
+// server over a plain TCP connection.
+type upgradeClient struct {
+	conn net.Conn
+	br   *bufio.Reader
+	resp *http.Response
+}
+
+// dialUpgrade sends an upgrade request for path to the HTTP port of the
+// test server, and returns the client once the response headers are read.
+func dialUpgrade(t *testing.T, path string, header map[string]string) *upgradeClient {
+	t.Helper()
+	conn, err := net.DialTimeout("tcp", "127.0.0.1:9080", 5*time.Second)
+	if err != nil {
+		t.Fatalf("failed to dial the server: %v", err)
+	}
+	t.Cleanup(func() { conn.Close() })
+
+	req, err := http.NewRequest(http.MethodGet, "http://localhost:9080"+path, nil)
+	if err != nil {
+		t.Fatalf("failed to create request: %v", err)
+	}
+	req.Header.Set("Connection", "Upgrade")
+	req.Header.Set("Upgrade", "websocket")
+	for k, v := range header {
+		req.Header.Set(k, v)
+	}
+
+	conn.SetDeadline(time.Now().Add(10 * time.Second))
+	if err := req.Write(conn); err != nil {
+		t.Fatalf("failed to write upgrade request: %v", err)
+	}
+	br := bufio.NewReader(conn)
+	resp, err := http.ReadResponse(br, req)
+	if err != nil {
+		t.Fatalf("failed to read upgrade response: %v", err)
+	}
+	if resp.StatusCode != http.StatusSwitchingProtocols {
+		t.Fatalf("upgrade status = %d, want %d", resp.StatusCode, http.StatusSwitchingProtocols)
+	}
+	return &upgradeClient{conn: conn, br: br, resp: resp}
+}
+
+// echo sends msg on the stream and expects to read it back.
+func (c *upgradeClient) echo(t *testing.T, msg string) {
+	t.Helper()
+	c.conn.SetDeadline(time.Now().Add(10 * time.Second))
+	if _, err := io.WriteString(c.conn, msg); err != nil {
+		t.Fatalf("writing %d bytes to the stream: %v", len(msg), err)
+	}
+	buf := make([]byte, len(msg))
+	if _, err := io.ReadFull(c.br, buf); err != nil {
+		t.Fatalf("reading the echo of %d bytes: %v", len(msg), err)
+	}
+	if string(buf) != msg {
+		t.Fatalf("echo does not match what was sent (%d bytes)", len(msg))
+	}
+}
+
+// expectClosed waits for the server to close the stream.
+func (c *upgradeClient) expectClosed(t *testing.T, within time.Duration) {
+	t.Helper()
+	c.conn.SetReadDeadline(time.Now().Add(within))
+	// the server may send a close message before it closes the connection
+	_, err := io.Copy(io.Discard, c.br)
+	var netErr net.Error
+	if errors.As(err, &netErr) && netErr.Timeout() {
+		t.Fatalf("the stream is still open after %s", within)
+	}
+}
+
+// TestStreamDetachedSurvivesReload checks that, with stream_detached, an
+// established stream keeps working across reloads that keep its upstream.
+func TestStreamDetachedSurvivesReload(t *testing.T) {
+	backend := startUpgradeBackend(t)
+	tester := caddytest.NewTester(t)
+
+	config := func(extra string) string {
+		return streamTestGlobals + fmt.Sprintf(`
+		localhost:9080 {
+			%s
+			reverse_proxy 127.0.0.1:%s {
+				stream_detached
+			}
+		}
+		`, extra, backend[strings.LastIndex(backend, ":")+1:])
+	}
+
+	tester.InitServer(config(""), "caddyfile")
+	client := dialUpgrade(t, "/", nil)
+	client.echo(t, "before the reload")
+
+	for i, extra := range []string{`respond /one "one"`, `respond /two "two"`} {
+		tester.InitServer(config(extra), "caddyfile")
+		client.echo(t, fmt.Sprintf("after reload %d", i+1))
+	}
+
+	// the new config serves new streams as well
+	second := dialUpgrade(t, "/", nil)
+	second.echo(t, "new stream")
+	client.echo(t, "the first stream is still there")
+}
+
+// TestStreamDetachedClosesRemovedUpstream checks that a reload that removes
+// an upstream closes the detached streams of that upstream and only those.
+func TestStreamDetachedClosesRemovedUpstream(t *testing.T) {
+	port := func(addr string) string { return addr[strings.LastIndex(addr, ":")+1:] }
+	backendA, backendB := startUpgradeBackend(t), startUpgradeBackend(t)
+	tester := caddytest.NewTester(t)
+
+	tester.InitServer(streamTestGlobals+fmt.Sprintf(`
+	localhost:9080 {
+		handle /a* {
+			reverse_proxy 127.0.0.1:%s {
+				stream_detached
+			}
+		}
+		handle /b* {
+			reverse_proxy 127.0.0.1:%s {
+				stream_detached
+			}
+		}
+	}
+	`, port(backendA), port(backendB)), "caddyfile")
+
+	streamA, streamB := dialUpgrade(t, "/a", nil), dialUpgrade(t, "/b", nil)
+	streamA.echo(t, "a before")
+	streamB.echo(t, "b before")
+
+	// the new config only has the second upstream
+	tester.InitServer(streamTestGlobals+fmt.Sprintf(`
+	localhost:9080 {
+		handle /b* {
+			reverse_proxy 127.0.0.1:%s {
+				stream_detached
+			}
+		}
+		handle {
+			respond "gone" 404
+		}
+	}
+	`, port(backendB)), "caddyfile")
+
+	streamA.expectClosed(t, 10*time.Second)
+	streamB.echo(t, "b after")
+	dialUpgrade(t, "/b", nil).echo(t, "b new")
+}
+
+// TestStreamDefaultClosedOnReload checks that, by default, a reload closes
+// the upgraded streams: at once, or after stream_close_delay.
+func TestStreamDefaultClosedOnReload(t *testing.T) {
+	backend := startUpgradeBackend(t)
+	port := backend[strings.LastIndex(backend, ":")+1:]
+	tester := caddytest.NewTester(t)
+
+	config := func(proxyOptions, extra string) string {
+		return streamTestGlobals + fmt.Sprintf(`
+		localhost:9080 {
+			%s
+			reverse_proxy 127.0.0.1:%s {
+				%s
+			}
+		}
+		`, extra, port, proxyOptions)
+	}
+
+	t.Run("immediately", func(t *testing.T) {
+		tester.InitServer(config("", ""), "caddyfile")
+		client := dialUpgrade(t, "/", nil)
+		client.echo(t, "before the reload")
+
+		tester.InitServer(config("", `respond /other "other"`), "caddyfile")
+		client.expectClosed(t, 10*time.Second)
+	})
+
+	t.Run("after the close delay", func(t *testing.T) {
+		const delay = 5 * time.Second
+		tester.InitServer(config("stream_close_delay 5s", ""), "caddyfile")
+		client := dialUpgrade(t, "/", nil)
+		client.echo(t, "before the reload")
+
+		tester.InitServer(config("stream_close_delay 5s", `respond /other "other"`), "caddyfile")
+		reloaded := time.Now()
+		// the stream is kept for the delay...
+		client.echo(t, "after the reload")
+		// ...and closed when it is over
+		client.expectClosed(t, 4*delay)
+		if elapsed := time.Since(reloaded); elapsed < delay/2 {
+			t.Errorf("the stream was closed %s after the reload, long before the delay of %s", elapsed, delay)
+		}
+	})
+}
+
+// readAccessLog returns the entries of a JSON log file.
+func readAccessLog(t *testing.T, path string) []map[string]any {
+	t.Helper()
+	data, err := os.ReadFile(path)
+	if err != nil {
+		if errors.Is(err, os.ErrNotExist) {
+			return nil
+		}
+		t.Fatalf("reading log: %v", err)
+	}
+	var entries []map[string]any
+	for _, line := range strings.Split(string(data), "\n") {
+		if strings.TrimSpace(line) == "" {
+			continue
+		}
+		var entry map[string]any
+		if err := json.Unmarshal([]byte(line), &entry); err != nil {
+			t.Fatalf("log line is not JSON: %q: %v", line, err)
+		}
+		entries = append(entries, entry)
+	}
+	return entries
+}
+
+// logEntries returns the entries of the log file with the given message
+// and, if uri is not empty, for a request with that URI.
+func logEntries(t *testing.T, path, msg, uri string) []map[string]any {
+	t.Helper()
+	var found []map[string]any
+	for _, entry := range readAccessLog(t, path) {
+		if entry["msg"] != msg {
+			continue
+		}
+		if req, _ := entry["request"].(map[string]any); uri != "" && req["uri"] != uri {
+			continue
+		}
+		found = append(found, entry)
+	}
+	return found
+}
+
+// waitForLogEntry waits for an entry to show up in the log file.
+func waitForLogEntry(t *testing.T, path, msg, uri string) map[string]any {
+	t.Helper()
+	deadline := time.Now().Add(10 * time.Second)
+	for time.Now().Before(deadline) {
+		if found := logEntries(t, path, msg, uri); len(found) > 0 {
+			return found[0]
+		}
+		time.Sleep(50 * time.Millisecond)
+	}
+	t.Fatalf("no %q log entry (uri %q) after 10s; log: %v", msg, uri, readAccessLog(t, path))
+	return nil
+}
+
+// TestDetachedStreamAccessLog checks the logging of detached streams: the
+// handshake is logged unless skip_handshake is set, and the close of a stream
+// is logged to the access log with the logger name, at the configured level
+// (DEBUG by default).
+func TestDetachedStreamAccessLog(t *testing.T) {
+	backend := startUpgradeBackend(t)
+	port := backend[strings.LastIndex(backend, ":")+1:]
+
+	dir, err := os.MkdirTemp("", "caddy-stream-log")
+	if err != nil {
+		t.Fatalf("creating a log directory: %v", err)
+	}
+	// the log file may still be open when the test is over; do not fail on that
+	t.Cleanup(func() { _ = os.RemoveAll(dir) })
+	logFile := filepath.Join(dir, "access.log")
+
+	tester := caddytest.NewTester(t)
+	config := func(streamLogs string) string {
+		return streamTestGlobals + fmt.Sprintf(`
+		localhost:9080 {
+			log access {
+				output file %s
+			}
+			reverse_proxy 127.0.0.1:%s {
+				stream_detached
+				%s
+			}
+		}
+		`, filepath.ToSlash(logFile), port, streamLogs)
+	}
+
+	// by default: the handshake is logged, the close of the stream is a DEBUG
+	// entry that the access log (INFO) does not keep
+	tester.InitServer(config(""), "caddyfile")
+	first := dialUpgrade(t, "/first", nil)
+	first.echo(t, "first stream")
+
+	// the stream stays open when the config changes to skip the handshake
+	// and to log the close of streams at INFO
+	tester.InitServer(config(`stream_logs {
+		skip_handshake
+		level INFO
+	}`), "caddyfile")
+	first.echo(t, "first stream, after the reload")
+
+	second := dialUpgrade(t, "/second", nil)
+	second.echo(t, "second stream")
+	second.conn.Close()
+
+	closed := waitForLogEntry(t, logFile, "stream closed", "")
+	if closed["logger"] != "http.log.access.access" {
+		t.Errorf("close logged by %v, want http.log.access.access", closed["logger"])
+	}
+	if closed["result"] != "completed" {
+		t.Errorf("close result = %v, want completed", closed["result"])
+	}
+	if closed["upstream"] != backend {
+		t.Errorf("close upstream = %v, want %s", closed["upstream"], backend)
+	}
+	if n, _ := closed["bytes_to_backend"].(float64); n != float64(len("second stream")) {
+		t.Errorf("bytes_to_backend = %v, want %d", closed["bytes_to_backend"], len("second stream"))
+	}
+
+	// the first stream, from the config that logs the handshake
+	first.conn.Close()
+	handled := waitForLogEntry(t, logFile, "handled request", "/first")
+	if status, _ := handled["status"].(float64); status != http.StatusSwitchingProtocols {
+		t.Errorf("handshake logged with status %v, want 101", handled["status"])
+	}
+
+	if got := logEntries(t, logFile, "handled request", "/second"); len(got) != 0 {
+		t.Errorf("handshake of the second stream was logged despite skip_handshake: %v", got)
+	}
+	if got := logEntries(t, logFile, "stream closed", ""); len(got) != 1 {
+		t.Errorf("%d stream close entries, want 1 (the first stream is logged at DEBUG): %v", len(got), got)
+	}
+}
+
+// TestExtendedConnectH2 checks that a WebSocket over HTTP/2 (extended
+// CONNECT, RFC 8441) is proxied to a backend that speaks HTTP/1.1 upgrades,
+// by default and with detached streams.
+func TestExtendedConnectH2(t *testing.T) {
+	// The HTTP/2 server of the standard library only advertises extended
+	// CONNECT when it is started with GODEBUG=http2xconnect=1, so the test runs
+	// in a process of its own that has it, on other ports than the other tests.
+	if os.Getenv(extendedConnectChildEnv) == "" {
+		if testing.Short() {
+			t.Skip("integration tests are skipped in short mode")
+		}
+		exe, err := os.Executable()
+		if err != nil {
+			t.Fatalf("cannot find the test binary: %v", err)
+		}
+		cmd := exec.Command(exe, "-test.run=^TestExtendedConnectH2$", "-test.count=1", "-test.v", "-test.timeout=3m")
+		cmd.Env = append(os.Environ(), "GODEBUG=http2xconnect=1", extendedConnectChildEnv+"=1")
+		out, err := cmd.CombinedOutput()
+		if err != nil {
+			// leave out the logs of the server
+			var report []string
+			for _, line := range strings.Split(string(out), "\n") {
+				if !strings.HasPrefix(line, `{"level"`) {
+					report = append(report, line)
+				}
+			}
+			t.Fatalf("extended CONNECT test failed: %v\n%s", err, strings.Join(report, "\n"))
+		}
+		return
+	}
+
+	backend := startUpgradeBackend(t)
+	port := backend[strings.LastIndex(backend, ":")+1:]
+	tester := caddytest.NewTester(t).WithDefaultOverrides(caddytest.Config{AdminPort: 2998})
+
+	for name, proxyOptions := range map[string]string{"default": "", "detached": "stream_detached"} {
+		t.Run(name, func(t *testing.T) {
+			tester.InitServer(`{
+				skip_install_trust
+				admin localhost:2998
+				http_port     9081
+				https_port    9444
+				grace_period  1ns
+				servers :9444 {
+					protocols h1 h2
+				}
+			}
+			localhost {
+				reverse_proxy 127.0.0.1:`+port+` {
+					`+proxyOptions+`
+				}
+			}
+			`, "caddyfile")
+
+			client := dialH2(t, "127.0.0.1:9444")
+			if !client.extendedConnect {
+				t.Fatalf("the server does not advertise extended CONNECT (SETTINGS_ENABLE_CONNECT_PROTOCOL)")
+			}
+			client.openWebSocket(t, 1, "localhost:9444", "/ws")
+			if client.status != "200" {
+				t.Fatalf(":status = %q, want 200", client.status)
+			}
+
+			for _, msg := range []string{"hello over h2", "and again"} {
+				client.send(t, 1, msg, false)
+				if got := client.receive(t, len(msg)); got != msg {
+					t.Fatalf("echo = %q, want %q", got, msg)
+				}
+			}
+
+			// the client ends the stream, and the server follows
+			client.send(t, 1, "", true)
+			client.awaitEnd(t)
+		})
+	}
+}
+
+// h2Client is a minimal HTTP/2 client speaking frames, for a stream that the
+// Go clients cannot open: a CONNECT with the :protocol pseudo-header.
+type h2Client struct {
+	conn            net.Conn
+	fr              *http2.Framer
+	dec             *hpack.Decoder
+	extendedConnect bool // the server advertised support for extended CONNECT
+	status          string
+	data            []byte
+	ended           bool
+}
+
+// extendedConnectChildEnv marks the process that runs the extended CONNECT test.
+const extendedConnectChildEnv = "CADDY_TEST_EXTENDED_CONNECT_CHILD"
+
+// dialH2 connects to the HTTPS address of the test server with HTTP/2,
+// and exchanges the settings.
+func dialH2(t *testing.T, addr string) *h2Client {
+	t.Helper()
+	conn, err := tls.Dial("tcp", addr, &tls.Config{
+		InsecureSkipVerify: true, //nolint:gosec
+		NextProtos:         []string{"h2"},
+		ServerName:         "localhost",
+	})
+	if err != nil {
+		t.Fatalf("failed to dial the server: %v", err)
+	}
+	t.Cleanup(func() { conn.Close() })
+	if got := conn.ConnectionState().NegotiatedProtocol; got != "h2" {
+		t.Fatalf("negotiated protocol = %q, want h2", got)
+	}
+
+	c := &h2Client{conn: conn, fr: http2.NewFramer(conn, conn), dec: hpack.NewDecoder(4096, nil)}
+	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
+		t.Fatalf("writing the preface: %v", err)
+	}
+	if err := c.fr.WriteSettings(); err != nil {
+		t.Fatalf("writing settings: %v", err)
+	}
+	// the first frame of the server is its settings
+	for !c.extendedConnect {
+		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
+		f, err := c.fr.ReadFrame()
+		if err != nil {
+			t.Fatalf("reading the settings of the server: %v", err)
+		}
+		if sf, ok := f.(*http2.SettingsFrame); ok && !sf.IsAck() {
+			_ = sf.ForeachSetting(func(s http2.Setting) error {
+				// SETTINGS_ENABLE_CONNECT_PROTOCOL, RFC 8441
+				if s.ID == http2.SettingID(0x8) && s.Val == 1 {
+					c.extendedConnect = true
+				}
+				return nil
+			})
+			if err := c.fr.WriteSettingsAck(); err != nil {
+				t.Fatalf("acknowledging settings: %v", err)
+			}
+			break
+		}
+	}
+	return c
+}
+
+// openWebSocket sends the extended CONNECT request of a WebSocket
+// on the stream, and reads the response headers.
+func (c *h2Client) openWebSocket(t *testing.T, stream uint32, authority, path string) {
+	t.Helper()
+	var block bytes.Buffer
+	enc := hpack.NewEncoder(&block)
+	for _, hf := range []hpack.HeaderField{
+		{Name: ":method", Value: "CONNECT"},
+		{Name: ":protocol", Value: "websocket"},
+		{Name: ":scheme", Value: "https"},
+		{Name: ":authority", Value: authority},
+		{Name: ":path", Value: path},
+		{Name: "sec-websocket-version", Value: "13"},
+	} {
+		if err := enc.WriteField(hf); err != nil {
+			t.Fatalf("encoding headers: %v", err)
+		}
+	}
+	if err := c.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: stream, BlockFragment: block.Bytes(), EndHeaders: true}); err != nil {
+		t.Fatalf("writing headers: %v", err)
+	}
+	for c.status == "" {
+		c.readFrame(t)
+	}
+}
+
+// send writes data on the stream, and ends it if asked to.
+func (c *h2Client) send(t *testing.T, stream uint32, data string, end bool) {
+	t.Helper()
+	if err := c.fr.WriteData(stream, end, []byte(data)); err != nil {
+		t.Fatalf("writing data: %v", err)
+	}
+}
+
+// receive reads n bytes of data from the stream.
+func (c *h2Client) receive(t *testing.T, n int) string {
+	t.Helper()
+	for len(c.data) < n {
+		c.readFrame(t)
+	}
+	got := string(c.data[:n])
+	c.data = c.data[n:]
+	return got
+}
+
+// awaitEnd waits for the server to end the stream.
+func (c *h2Client) awaitEnd(t *testing.T) {
+	t.Helper()
+	for !c.ended {
+		c.readFrame(t)
+	}
+}
+
+// readFrame reads a frame of the server, and takes note of what is in it.
+func (c *h2Client) readFrame(t *testing.T) {
+	t.Helper()
+	c.conn.SetReadDeadline(time.Now().Add(15 * time.Second))
+	f, err := c.fr.ReadFrame()
+	if err != nil {
+		t.Fatalf("reading a frame: %v", err)
+	}
+	switch f := f.(type) {
+	case *http2.HeadersFrame:
+		fields, err := c.dec.DecodeFull(f.HeaderBlockFragment())
+		if err != nil {
+			t.Fatalf("decoding headers: %v", err)
+		}
+		for _, field := range fields {
+			if field.Name == ":status" {
+				c.status = field.Value
+			}
+		}
+		c.ended = c.ended || f.StreamEnded()
+	case *http2.DataFrame:
+		c.data = append(c.data, f.Data()...)
+		c.ended = c.ended || f.StreamEnded()
+	case *http2.RSTStreamFrame:
+		// the server resets the stream when it is over, once the backend closed
+		c.ended = true
+	case *http2.PingFrame:
+		if !f.IsAck() {
+			_ = c.fr.WritePing(true, f.Data)
+		}
+	case *http2.GoAwayFrame:
+		t.Fatalf("the server is going away: %v", f.ErrCode)
+	}
+}
+
+// TestDetachedHTTP11Upgrade checks that HTTP/1.1 upgrades work with detached
+// streams: the handshake response is the one of the backend, and a lot of data
+// goes through in both directions on several streams.
+func TestDetachedHTTP11Upgrade(t *testing.T) {
+	backend := startUpgradeBackend(t)
+	tester := caddytest.NewTester(t)
+	tester.InitServer(streamTestGlobals+fmt.Sprintf(`
+	localhost:9080 {
+		reverse_proxy %s {
+			stream_detached
+		}
+	}
+	`, backend), "caddyfile")
+
+	big := strings.Repeat("0123456789abcdef", 16*1024) // 256 KiB
+	for i := range 3 {
+		client := dialUpgrade(t, fmt.Sprintf("/stream%d", i), map[string]string{
+			"Sec-WebSocket-Version": "13",
+			"Sec-WebSocket-Key":     "dGhlIHNhbXBsZSBub25jZQ==",
+		})
+		if got := client.resp.Header.Get("Upgrade"); !strings.EqualFold(got, "websocket") {
+			t.Errorf("Upgrade header = %q, want websocket", got)
+		}
+		if got := client.resp.Header.Get("Connection"); !strings.EqualFold(got, "Upgrade") {
+			t.Errorf("Connection header = %q, want Upgrade", got)
+		}
+		client.echo(t, "small")
+		client.echo(t, big)
+	}
+
+	// ordinary requests are proxied as usual
+	tester.AssertGetResponse("http://localhost:9080/plain", 200, "backend plain")
+}
+
+// TestUpgradeThroughEncode checks that upgrades go through the encode
+// handler, whether or not it handles the response.
+func TestUpgradeThroughEncode(t *testing.T) {
+	backend := startUpgradeBackend(t)
+	tester := caddytest.NewTester(t)
+
+	for name, proxyOptions := range map[string]string{"default": "", "detached": "stream_detached"} {
+		t.Run(name, func(t *testing.T) {
+			tester.InitServer(streamTestGlobals+fmt.Sprintf(`
+			localhost:9080 {
+				encode gzip
+				reverse_proxy %s {
+					%s
+				}
+			}
+			`, backend, proxyOptions), "caddyfile")
+
+			// without a key, encode wraps the response writer; with the key of a
+			// browser it does not (encodings are not offered to WebSockets)
+			for _, header := range []map[string]string{
+				{"Accept-Encoding": "gzip"},
+				{"Accept-Encoding": "gzip", "Sec-WebSocket-Version": "13", "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ=="},
+			} {
+				client := dialUpgrade(t, "/", header)
+				if got := client.resp.Header.Get("Content-Encoding"); got != "" {
+					t.Errorf("Content-Encoding = %q on an upgrade, want none", got)
+				}
+				client.echo(t, "through encode")
+				client.echo(t, strings.Repeat("compressible ", 1000))
+				client.conn.Close()
+			}
+
+			// other requests are still proxied through encode
+			req, _ := http.NewRequest(http.MethodGet, "http://localhost:9080/plain", nil)
+			req.Header.Set("Accept-Encoding", "gzip")
+			tester.AssertResponseCode(req, http.StatusOK)
+		})
+	}
+}
diff --git a/modules/caddyhttp/encode/encode.go b/modules/caddyhttp/encode/encode.go
index 46b383ea..24923a8d 100644
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
@@ -255,12 +257,24 @@ type responseWriter struct {
 	incremental  bool
 	isConnect    bool
 	disabled     bool // disable encoding for this response
+	switched     bool // protocols were switched or the connection hijacked; nothing more may be written
 }
 
 // WriteHeader stores the status to write when the time comes
-// to actually write the header.
+// to actually write the header. A 101 Switching Protocols
+// response is final: it is written immediately and the response
+// is over, so anything written after it is dropped.
 func (rw *responseWriter) WriteHeader(status int) {
+	if rw.switched {
+		return
+	}
 	rw.statusCode = status
+	if status == http.StatusSwitchingProtocols {
+		rw.switched = true
+		rw.wroteHeader = true
+		rw.ResponseWriter.WriteHeader(status)
+		return
+	}
 	if status == http.StatusPartialContent {
 		rw.disabled = true // partial representations must not be dynamically re-encoded
 	}
@@ -365,6 +379,10 @@ func (rw *responseWriter) FlushError() error {
 // it is encoded using the encoder, which is initialized
 // if not done so already.
 func (rw *responseWriter) Write(p []byte) (int, error) {
+	if rw.switched {
+		return 0, http.ErrHijacked
+	}
+
 	// ignore zero data writes, probably head request
 	if len(p) == 0 {
 		return 0, nil
@@ -438,6 +456,9 @@ const sniffLen = 512
 // It's based on the standard library HTTP/1.1 response writer implementation.
 // https://github.com/golang/go/blob/f4e3ec3dbe3b8e04a058d266adf8e048bab563f2/src/net/http/server.go#L586
 func (rw *responseWriter) ReadFrom(r io.Reader) (int64, error) {
+	if rw.switched {
+		return 0, http.ErrHijacked
+	}
 	if rw.incremental || !rw.wroteHeader && caddyhttp.IsIncremental(rw.Header()) {
 		return io.Copy(writerOnly{rw}, r)
 	}
@@ -499,6 +520,20 @@ func (rw *responseWriter) Close() error {
 	return err
 }
 
+// Hijack implements http.Hijacker by hijacking the underlying
+// ResponseWriter. Once hijacked, the response is over and any
+// further write to rw is dropped.
+func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	//nolint:bodyclose
+	conn, brw, err := http.NewResponseController(rw.ResponseWriter).Hijack()
+	if err != nil {
+		return nil, nil, err
+	}
+	rw.switched = true
+	rw.wroteHeader = true
+	return conn, brw, nil
+}
+
 // Unwrap returns the underlying ResponseWriter.
 func (rw *responseWriter) Unwrap() http.ResponseWriter {
 	return rw.ResponseWriter
diff --git a/modules/caddyhttp/encode/encode_test.go b/modules/caddyhttp/encode/encode_test.go
index 8e0037df..9ff1a901 100644
--- a/modules/caddyhttp/encode/encode_test.go
+++ b/modules/caddyhttp/encode/encode_test.go
@@ -1,11 +1,16 @@
 package encode
 
 import (
+	"bufio"
+	"bytes"
 	"context"
+	"errors"
 	"io"
+	"net"
 	"net/http"
 	"net/http/httptest"
 	"slices"
+	"strings"
 	"sync"
 	"testing"
 
@@ -30,6 +35,96 @@ func (w *discardResponseWriter) Header() http.Header         { return w.header }
 func (w *discardResponseWriter) Write(p []byte) (int, error) { return len(p), nil }
 func (w *discardResponseWriter) WriteHeader(int)             {}
 
+// hijackableResponseWriter is a http.ResponseWriter that records what is
+// written to it, and that can be hijacked unless hijackErr is set.
+type hijackableResponseWriter struct {
+	header    http.Header
+	statuses  []int
+	body      bytes.Buffer
+	hijackErr error
+}
+
+func (w *hijackableResponseWriter) Header() http.Header {
+	if w.header == nil {
+		w.header = make(http.Header)
+	}
+	return w.header
+}
+func (w *hijackableResponseWriter) WriteHeader(status int)       { w.statuses = append(w.statuses, status) }
+func (w *hijackableResponseWriter) Write(p []byte) (int, error) { return w.body.Write(p) }
+func (w *hijackableResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	if w.hijackErr != nil {
+		return nil, nil, w.hijackErr
+	}
+	conn, _ := net.Pipe()
+	return conn, bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)), nil
+}
+
+// TestEncodeUpgradeFinal101 checks that a 101 is the final response of the
+// encode writer: it is written at once, and nothing is written after it.
+func TestEncodeUpgradeFinal101(t *testing.T) {
+	w := new(hijackableResponseWriter)
+	rw := new(Encode).openResponseWriter("gzip", w, false)
+
+	rw.WriteHeader(http.StatusSwitchingProtocols)
+	rw.WriteHeader(http.StatusOK)
+	if _, err := rw.Write([]byte("late")); !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("Write() err = %v, want %v", err, http.ErrHijacked)
+	}
+	if _, err := rw.ReadFrom(strings.NewReader("late")); !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("ReadFrom() err = %v, want %v", err, http.ErrHijacked)
+	}
+	if err := rw.Close(); err != nil {
+		t.Errorf("Close() err = %v", err)
+	}
+
+	if !slices.Equal(w.statuses, []int{http.StatusSwitchingProtocols}) {
+		t.Errorf("statuses written = %v, want only the 101", w.statuses)
+	}
+	if w.body.Len() != 0 {
+		t.Errorf("body written after 101: %q", w.body.String())
+	}
+}
+
+// TestEncodeNoWriteAfterHijack checks that nothing is written through the
+// encode writer once the connection has been hijacked, and that a failed
+// hijack does not stop the response.
+func TestEncodeNoWriteAfterHijack(t *testing.T) {
+	w := new(hijackableResponseWriter)
+	rw := new(Encode).openResponseWriter("gzip", w, false)
+	rw.WriteHeader(http.StatusAccepted) // pending, would be written by Close
+
+	conn, _, err := http.NewResponseController(rw).Hijack()
+	if err != nil {
+		t.Fatalf("Hijack() err = %v", err)
+	}
+	defer conn.Close()
+
+	rw.WriteHeader(http.StatusOK)
+	if _, err := rw.Write([]byte("late")); !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("Write() err = %v, want %v", err, http.ErrHijacked)
+	}
+	if err := rw.Close(); err != nil {
+		t.Errorf("Close() err = %v", err)
+	}
+	if len(w.statuses) != 0 || w.body.Len() != 0 {
+		t.Errorf("written after hijack: statuses %v, body %q", w.statuses, w.body.String())
+	}
+
+	failing := &hijackableResponseWriter{hijackErr: errors.New("no way")}
+	rw = new(Encode).openResponseWriter("gzip", failing, false)
+	if _, _, err := http.NewResponseController(rw).Hijack(); err == nil {
+		t.Fatal("Hijack() err = nil, want the error of the underlying writer")
+	}
+	rw.WriteHeader(http.StatusTeapot)
+	if _, err := rw.Write([]byte("short and stout")); err != nil {
+		t.Errorf("Write() err = %v after a failed hijack", err)
+	}
+	if !slices.Equal(failing.statuses, []int{http.StatusTeapot}) || failing.body.String() != "short and stout" {
+		t.Errorf("got %v %q, want the response to be written", failing.statuses, failing.body.String())
+	}
+}
+
 // BenchmarkResponseWriterWriteHeader covers the branches inside WriteHeader:
 // the plain/common case, the SSE Content-Type check (both when it doesn't
 // match and when it does and rw.init() runs), CONNECT 2xx, informational
diff --git a/modules/caddyhttp/responsewriter.go b/modules/caddyhttp/responsewriter.go
index 904c30c0..9d94cdcb 100644
--- a/modules/caddyhttp/responsewriter.go
+++ b/modules/caddyhttp/responsewriter.go
@@ -70,6 +70,7 @@ type responseRecorder struct {
 	size         int
 	wroteHeader  bool
 	stream       bool
+	hijacked     bool
 
 	readSize *int
 }
@@ -144,9 +145,11 @@ func NewResponseRecorder(w http.ResponseWriter, buf *bytes.Buffer, shouldBuffer
 
 // WriteHeader writes the headers with statusCode to the wrapped
 // ResponseWriter unless the response is to be buffered instead.
-// 1xx responses are never buffered.
+// 1xx responses are never buffered. A 101 Switching Protocols
+// response is final and is never buffered either; nothing is
+// written once the connection has been hijacked.
 func (rr *responseRecorder) WriteHeader(statusCode int) {
-	if rr.wroteHeader {
+	if rr.wroteHeader || rr.hijacked {
 		return
 	}
 
@@ -155,14 +158,17 @@ func (rr *responseRecorder) WriteHeader(statusCode int) {
 	rr.statusCode = statusCode
 
 	// decide whether we should buffer the response
-	if rr.shouldBuffer == nil {
+	switch {
+	case statusCode == http.StatusSwitchingProtocols:
 		rr.stream = true
-	} else {
+	case rr.shouldBuffer == nil:
+		rr.stream = true
+	default:
 		rr.stream = !rr.shouldBuffer(rr.statusCode, rr.ResponseWriterWrapper.Header())
 	}
 
-	// 1xx responses aren't final; just informational
-	if statusCode < 100 || statusCode > 199 {
+	// other 1xx responses aren't final; just informational
+	if statusCode < 100 || statusCode > 199 || statusCode == http.StatusSwitchingProtocols {
 		rr.wroteHeader = true
 	}
 
@@ -173,6 +179,9 @@ func (rr *responseRecorder) WriteHeader(statusCode int) {
 }
 
 func (rr *responseRecorder) Write(data []byte) (int, error) {
+	if rr.hijacked {
+		return 0, http.ErrHijacked
+	}
 	rr.WriteHeader(http.StatusOK)
 	var n int
 	var err error
@@ -187,6 +196,9 @@ func (rr *responseRecorder) Write(data []byte) (int, error) {
 }
 
 func (rr *responseRecorder) ReadFrom(r io.Reader) (int64, error) {
+	if rr.hijacked {
+		return 0, http.ErrHijacked
+	}
 	rr.WriteHeader(http.StatusOK)
 	var n int64
 	var err error
@@ -258,6 +270,12 @@ func (rr *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
 	if err != nil {
 		return nil, nil, err
 	}
+	// the connection now belongs to the hijacker: the response is
+	// over, so nothing may be buffered or written to it again
+	rr.hijacked = true
+	rr.stream = true
+	rr.wroteHeader = true
+
 	// Per http documentation, returned bufio.Writer is empty, but bufio.Read maybe not
 	conn = &hijackedConn{conn, rr}
 	brw.Writer.Reset(conn)
@@ -275,6 +293,18 @@ func (rr *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
 	return conn, brw, nil
 }
 
+// RawConn returns the connection underneath conn if conn was returned by
+// the Hijack method of a response recorder, which wraps the connection to
+// track the response size. Callers that copy bytes for a long time (such
+// as a proxy of an upgraded stream) should do so on the raw connection
+// rather than through the recorder. Any other conn is returned as is.
+func RawConn(conn net.Conn) net.Conn {
+	if hc, ok := conn.(*hijackedConn); ok {
+		return hc.Conn
+	}
+	return conn
+}
+
 // used to track the size of hijacked response writers
 type hijackedConn struct {
 	net.Conn
diff --git a/modules/caddyhttp/responsewriter_test.go b/modules/caddyhttp/responsewriter_test.go
index c08ad26a..b399a256 100644
--- a/modules/caddyhttp/responsewriter_test.go
+++ b/modules/caddyhttp/responsewriter_test.go
@@ -1,13 +1,43 @@
 package caddyhttp
 
 import (
+	"bufio"
 	"bytes"
+	"errors"
 	"io"
+	"net"
 	"net/http"
+	"net/http/httptest"
 	"strings"
 	"testing"
 )
 
+// a http.ResponseWriter mock that can be hijacked, and records what is
+// written to it
+type hijackableRespWriter struct {
+	header   http.Header
+	statuses []int
+	body     bytes.Buffer
+	conn     net.Conn
+}
+
+func (w *hijackableRespWriter) Header() http.Header {
+	if w.header == nil {
+		w.header = make(http.Header)
+	}
+	return w.header
+}
+func (w *hijackableRespWriter) WriteHeader(statusCode int) { w.statuses = append(w.statuses, statusCode) }
+func (w *hijackableRespWriter) Write(d []byte) (int, error) {
+	return w.body.Write(d)
+}
+func (w *hijackableRespWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	var peer net.Conn
+	w.conn, peer = net.Pipe()
+	_ = peer
+	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
+}
+
 type responseWriterSpy interface {
 	http.ResponseWriter
 	Written() string
@@ -169,3 +199,121 @@ func TestResponseRecorderReadFrom(t *testing.T) {
 		})
 	}
 }
+
+// TestResponseRecorderNoWriteAfterHijack checks that once the connection is
+// hijacked, the response is over: nothing written to the recorder reaches the
+// underlying writer or the buffer.
+func TestResponseRecorderNoWriteAfterHijack(t *testing.T) {
+	w := new(hijackableRespWriter)
+	var buf bytes.Buffer
+	rec := NewResponseRecorder(w, &buf, func(int, http.Header) bool { return true })
+
+	conn, _, err := http.NewResponseController(rec).Hijack()
+	if err != nil {
+		t.Fatalf("Hijack() err = %v", err)
+	}
+	defer conn.Close()
+
+	if rec.Buffered() {
+		t.Error("a hijacked response must not be buffered")
+	}
+
+	rec.WriteHeader(http.StatusOK)
+	if _, err := rec.Write([]byte("late")); !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("Write() err = %v, want %v", err, http.ErrHijacked)
+	}
+	if _, err := rec.(io.ReaderFrom).ReadFrom(strings.NewReader("late")); !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("ReadFrom() err = %v, want %v", err, http.ErrHijacked)
+	}
+	if err := rec.WriteResponse(); err != nil {
+		t.Errorf("WriteResponse() err = %v", err)
+	}
+
+	if len(w.statuses) != 0 {
+		t.Errorf("statuses written after hijack: %v", w.statuses)
+	}
+	if w.body.Len() != 0 || buf.Len() != 0 {
+		t.Errorf("body written after hijack: %q, buffered %q", w.body.String(), buf.String())
+	}
+	if rec.Status() != 0 || rec.Size() != 0 {
+		t.Errorf("Status() = %d, Size() = %d after hijack, want 0 and 0", rec.Status(), rec.Size())
+	}
+}
+
+// TestResponseRecorderFailedHijack checks that a hijack that fails leaves the
+// recorder usable.
+func TestResponseRecorderFailedHijack(t *testing.T) {
+	w := httptest.NewRecorder() // cannot be hijacked
+	rec := NewResponseRecorder(w, nil, nil)
+
+	if _, _, err := http.NewResponseController(rec).Hijack(); !errors.Is(err, http.ErrNotSupported) {
+		t.Fatalf("Hijack() err = %v, want %v", err, http.ErrNotSupported)
+	}
+	rec.WriteHeader(http.StatusTeapot)
+	if _, err := rec.Write([]byte("short and stout")); err != nil {
+		t.Errorf("Write() err = %v", err)
+	}
+	if w.Code != http.StatusTeapot || w.Body.String() != "short and stout" {
+		t.Errorf("got %d %q, want the response to be written", w.Code, w.Body.String())
+	}
+}
+
+// TestResponseRecorderSwitchingProtocolsFinal checks that a 101 is written
+// at once, never buffered, and is the final response, unlike other 1xx.
+func TestResponseRecorderSwitchingProtocolsFinal(t *testing.T) {
+	for _, tc := range []struct {
+		status       int
+		wantBuffered bool
+		wantStatuses []int
+	}{
+		{status: http.StatusSwitchingProtocols, wantBuffered: false, wantStatuses: []int{101}},
+		{status: http.StatusEarlyHints, wantBuffered: true, wantStatuses: []int{103}},
+	} {
+		w := new(hijackableRespWriter)
+		rec := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool { return true })
+
+		rec.WriteHeader(tc.status)
+		rec.WriteHeader(http.StatusOK) // dropped after a final response; buffered after an informational one
+
+		if rec.Buffered() != tc.wantBuffered {
+			t.Errorf("%d: Buffered() = %t, want %t", tc.status, rec.Buffered(), tc.wantBuffered)
+		}
+		if !equalInts(w.statuses, tc.wantStatuses) {
+			t.Errorf("%d: statuses written = %v, want %v", tc.status, w.statuses, tc.wantStatuses)
+		}
+	}
+}
+
+func equalInts(a, b []int) bool {
+	if len(a) != len(b) {
+		return false
+	}
+	for i := range a {
+		if a[i] != b[i] {
+			return false
+		}
+	}
+	return true
+}
+
+// TestRawConn checks that the connection of a hijacked recorder is unwrapped
+// to the one that was hijacked, and that other connections are left alone.
+func TestRawConn(t *testing.T) {
+	w := new(hijackableRespWriter)
+	rec := NewResponseRecorder(w, nil, nil)
+	conn, _, err := http.NewResponseController(rec).Hijack()
+	if err != nil {
+		t.Fatalf("Hijack() err = %v", err)
+	}
+	defer conn.Close()
+
+	if conn == w.conn {
+		t.Fatal("the recorder is expected to wrap the hijacked connection")
+	}
+	if got := RawConn(conn); got != w.conn {
+		t.Errorf("RawConn(recorder conn) = %v, want the hijacked connection", got)
+	}
+	if got := RawConn(w.conn); got != w.conn {
+		t.Errorf("RawConn(plain conn) = %v, want it unchanged", got)
+	}
+}
diff --git a/modules/caddyhttp/reverseproxy/caddyfile.go b/modules/caddyhttp/reverseproxy/caddyfile.go
index 12c1177b..78388023 100644
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
@@ -715,6 +720,39 @@ func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
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
+				case "level":
+					if !d.NextArg() {
+						return d.ArgErr()
+					}
+					h.StreamLogs.Level = d.Val()
+					if d.NextArg() {
+						return d.ArgErr()
+					}
+				default:
+					return d.Errf("unrecognized stream_logs subdirective: %s", d.Val())
+				}
+			}
+
 		case "trusted_proxies":
 			for d.NextArg() {
 				if d.Val() == "private_ranges" {
diff --git a/modules/caddyhttp/reverseproxy/caddyfile_test.go b/modules/caddyhttp/reverseproxy/caddyfile_test.go
new file mode 100644
index 00000000..d05348fd
--- /dev/null
+++ b/modules/caddyhttp/reverseproxy/caddyfile_test.go
@@ -0,0 +1,192 @@
+package reverseproxy
+
+import (
+	"encoding/json"
+	"reflect"
+	"strings"
+	"testing"
+
+	"go.uber.org/zap/zapcore"
+
+	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
+)
+
+func TestUnmarshalCaddyfileStreamDetached(t *testing.T) {
+	for i, tc := range []struct {
+		input        string
+		wantErr      string
+		wantDetached bool
+		wantLogs     *StreamLogs
+	}{
+		{
+			input: `reverse_proxy localhost:9000`,
+		},
+		{
+			input: `reverse_proxy localhost:9000 {
+				stream_detached
+			}`,
+			wantDetached: true,
+		},
+		{
+			input: `reverse_proxy localhost:9000 {
+				stream_detached yes
+			}`,
+			wantErr: "wrong argument count",
+		},
+		{
+			input: `reverse_proxy localhost:9000 {
+				stream_logs {
+				}
+			}`,
+			wantLogs: &StreamLogs{},
+		},
+		{
+			input: `reverse_proxy localhost:9000 {
+				stream_detached
+				stream_logs {
+					skip_handshake
+					level INFO
+				}
+			}`,
+			wantDetached: true,
+			wantLogs:     &StreamLogs{SkipHandshake: true, Level: "INFO"},
+		},
+		{
+			input: `reverse_proxy localhost:9000 {
+				stream_logs {
+					skip_handshake
+				}
+				stream_logs {
+					level warn
+				}
+			}`,
+			wantLogs: &StreamLogs{SkipHandshake: true, Level: "warn"},
+		},
+		{
+			input: `reverse_proxy localhost:9000 {
+				stream_logs on
+			}`,
+			wantErr: "wrong argument count",
+		},
+		{
+			input: `reverse_proxy localhost:9000 {
+				stream_logs {
+					skip_handshake yes
+				}
+			}`,
+			wantErr: "wrong argument count",
+		},
+		{
+			input: `reverse_proxy localhost:9000 {
+				stream_logs {
+					level
+				}
+			}`,
+			wantErr: "wrong argument count",
+		},
+		{
+			input: `reverse_proxy localhost:9000 {
+				stream_logs {
+					level INFO DEBUG
+				}
+			}`,
+			wantErr: "wrong argument count",
+		},
+		{
+			input: `reverse_proxy localhost:9000 {
+				stream_logs {
+					handshake
+				}
+			}`,
+			wantErr: "unrecognized stream_logs subdirective: handshake",
+		},
+	} {
+		var h Handler
+		err := h.UnmarshalCaddyfile(caddyfile.NewTestDispenser(tc.input))
+		if tc.wantErr != "" {
+			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
+				t.Errorf("Test %d: expected error containing %q, got %v", i, tc.wantErr, err)
+			}
+			continue
+		}
+		if err != nil {
+			t.Errorf("Test %d: unexpected error: %v", i, err)
+			continue
+		}
+		if h.StreamDetached != tc.wantDetached {
+			t.Errorf("Test %d: StreamDetached = %t, want %t", i, h.StreamDetached, tc.wantDetached)
+		}
+		if !reflect.DeepEqual(h.StreamLogs, tc.wantLogs) {
+			t.Errorf("Test %d: StreamLogs = %+v, want %+v", i, h.StreamLogs, tc.wantLogs)
+		}
+	}
+}
+
+// TestStreamLogsConfig checks the stream_logs options, as parsed from the
+// Caddyfile and from JSON, and the level they resolve to.
+func TestStreamLogsConfig(t *testing.T) {
+	var fromCaddyfile Handler
+	err := fromCaddyfile.UnmarshalCaddyfile(caddyfile.NewTestDispenser(`reverse_proxy localhost:9000 {
+		stream_detached
+		stream_logs {
+			skip_handshake
+			level error
+		}
+	}`))
+	if err != nil {
+		t.Fatalf("parsing Caddyfile: %v", err)
+	}
+
+	var fromJSON Handler
+	err = json.Unmarshal([]byte(`{
+		"stream_detached": true,
+		"stream_logs": {"skip_handshake": true, "level": "error"}
+	}`), &fromJSON)
+	if err != nil {
+		t.Fatalf("parsing JSON: %v", err)
+	}
+
+	for name, h := range map[string]Handler{"caddyfile": fromCaddyfile, "json": fromJSON} {
+		if !h.StreamDetached {
+			t.Errorf("%s: StreamDetached not set", name)
+		}
+		want := &StreamLogs{SkipHandshake: true, Level: "error"}
+		if !reflect.DeepEqual(h.StreamLogs, want) {
+			t.Errorf("%s: StreamLogs = %+v, want %+v", name, h.StreamLogs, want)
+		}
+	}
+
+	out, err := json.Marshal(fromCaddyfile.StreamLogs)
+	if err != nil {
+		t.Fatalf("encoding JSON: %v", err)
+	}
+	if got, want := string(out), `{"skip_handshake":true,"level":"error"}`; got != want {
+		t.Errorf("JSON = %s, want %s", got, want)
+	}
+
+	for i, tc := range []struct {
+		logs    *StreamLogs
+		want    zapcore.Level
+		wantErr bool
+	}{
+		{logs: nil, want: zapcore.DebugLevel},
+		{logs: &StreamLogs{}, want: zapcore.DebugLevel},
+		{logs: &StreamLogs{SkipHandshake: true}, want: zapcore.DebugLevel},
+		{logs: &StreamLogs{Level: "INFO"}, want: zapcore.InfoLevel},
+		{logs: &StreamLogs{Level: "error"}, want: zapcore.ErrorLevel},
+		{logs: &StreamLogs{Level: "loud"}, want: zapcore.DebugLevel, wantErr: true},
+	} {
+		got, err := tc.logs.zapLevel()
+		if (err != nil) != tc.wantErr {
+			t.Errorf("Test %d: zapLevel() error = %v, wantErr %t", i, err, tc.wantErr)
+		}
+		if got != tc.want {
+			t.Errorf("Test %d: zapLevel() = %v, want %v", i, got, tc.want)
+		}
+
+		h := Handler{StreamLogs: tc.logs}
+		if err := h.Validate(); (err != nil) != tc.wantErr {
+			t.Errorf("Test %d: Validate() error = %v, wantErr %t", i, err, tc.wantErr)
+		}
+	}
+}
diff --git a/modules/caddyhttp/reverseproxy/metrics.go b/modules/caddyhttp/reverseproxy/metrics.go
index 24884273..69ec8e52 100644
--- a/modules/caddyhttp/reverseproxy/metrics.go
+++ b/modules/caddyhttp/reverseproxy/metrics.go
@@ -43,9 +43,77 @@ func initReverseProxyMetrics(handler *Handler, registry *prometheus.Registry) {
 		panic(err)
 	}
 
+	streamMetrics.register(registry)
+
 	reverseProxyMetrics.logger = handler.logger.Named("reverse_proxy.metrics")
 }
 
+// streamMetricSet holds the collectors that observe upgraded streams
+// (WebSocket and other Upgrade tunnels, extended CONNECT). The collectors
+// are created up front so that streams are always accounted for; they are
+// attached to a registry when a handler is provisioned.
+type streamMetricSet struct {
+	active   *prometheus.GaugeVec
+	closed   *prometheus.CounterVec
+	duration *prometheus.HistogramVec
+	bytes    *prometheus.CounterVec
+}
+
+// Values of the "direction" label of the stream bytes metric.
+const (
+	streamDirectionToBackend   = "to_backend"
+	streamDirectionFromBackend = "from_backend"
+)
+
+func newStreamMetricSet() *streamMetricSet {
+	const ns, sub = "caddy", "reverse_proxy"
+	return &streamMetricSet{
+		active: prometheus.NewGaugeVec(prometheus.GaugeOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "streams_active",
+			Help:      "Number of upgraded streams currently open.",
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
+			Buckets:   []float64{0.1, 1, 10, 60, 300, 900, 3600, 21600, 86400},
+		}, []string{"upstream"}),
+		bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "stream_bytes_total",
+			Help:      "Bytes copied on upgraded streams, by direction relative to the backend.",
+		}, []string{"upstream", "direction"}),
+	}
+}
+
+// register attaches the collectors to registry. Registering collectors
+// that are already registered (another handler, or a reload) is fine. Any
+// other failure is a programming error in the metric definitions and panics,
+// like the other reverse proxy metrics.
+func (m *streamMetricSet) register(registry prometheus.Registerer) {
+	for _, c := range []prometheus.Collector{m.active, m.closed, m.duration, m.bytes} {
+		if err := registry.Register(c); err != nil {
+			var already prometheus.AlreadyRegisteredError
+			if !errors.As(err, &already) {
+				panic(err)
+			}
+		}
+	}
+}
+
+// streamMetrics are the process-wide stream metrics.
+var streamMetrics = newStreamMetricSet()
+
 type metricsUpstreamsHealthyUpdater struct {
 	handler *Handler
 }
diff --git a/modules/caddyhttp/reverseproxy/reverseproxy.go b/modules/caddyhttp/reverseproxy/reverseproxy.go
index 0838c8be..76048786 100644
--- a/modules/caddyhttp/reverseproxy/reverseproxy.go
+++ b/modules/caddyhttp/reverseproxy/reverseproxy.go
@@ -199,6 +199,24 @@ type Handler struct {
 	// by the previous config closing. Default: no delay.
 	StreamCloseDelay caddy.Duration `json:"stream_close_delay,omitempty"`
 
+	// If true, upgraded streams (WebSockets and other Upgrade tunnels)
+	// are detached from the lifecycle of this config: they are not closed
+	// when the config is reloaded, and stay open until they end by
+	// themselves (or stream_timeout elapses). Only when a reload removes
+	// the upstream of a stream is that stream closed, so streams to
+	// upstreams that are kept survive. This makes stream_close_delay
+	// irrelevant. Streams to dynamic upstreams are never closed by a
+	// reload. Default: upgraded streams are closed when the config is
+	// unloaded, honoring stream_close_delay.
+	StreamDetached bool `json:"stream_detached,omitempty"`
+
+	// Configures the logging of upgraded streams: whether the upgrade
+	// handshake is logged in the access log, and at which level the close
+	// of a stream is logged. The close of a stream is logged to the same
+	// logger(s) as the access log (see the `logger_name` of the `log`
+	// directive).
+	StreamLogs *StreamLogs `json:"stream_logs,omitempty"`
+
 	// If configured, rewrites the copy of the upstream request.
 	// Allows changing the request method and URI (path and query).
 	// Since the rewrite is applied to the copy, it does not persist
@@ -263,7 +281,10 @@ type Handler struct {
 	connectionsCloseTimer *time.Timer
 	connectionsMu         *sync.Mutex
 
-	ctx    caddy.Context
+	// level at which closed streams are logged
+	streamLogLevel zapcore.Level
+
+	ctx   caddy.Context
 	logger *zap.Logger
 	events *caddyevents.App
 }
@@ -288,6 +309,9 @@ func (h *Handler) Provision(ctx caddy.Context) error {
 	h.connections = make(map[io.ReadWriteCloser]openConnection)
 	h.connectionsMu = new(sync.Mutex)
 
+	// an invalid level is reported by Validate
+	h.streamLogLevel, _ = h.StreamLogs.zapLevel()
+
 	// warn about unsafe buffering config
 	if h.RequestBuffers == -1 || h.ResponseBuffers == -1 {
 		h.logger.Warn("UNLIMITED BUFFERING: buffering is enabled without any cap on buffer size, which can result in OOM crashes")
@@ -456,13 +480,24 @@ func (h *Handler) Provision(ctx caddy.Context) error {
 	return nil
 }
 
+// Validate ensures that h has a valid configuration.
+func (h *Handler) Validate() error {
+	_, err := h.StreamLogs.zapLevel()
+	return err
+}
+
 // Cleanup cleans up the resources made by h.
 func (h *Handler) Cleanup() error {
 	err := h.cleanupConnections()
 
-	// remove hosts from our config from the pool
+	// remove hosts from our config from the pool; when that was the last
+	// use of an upstream, it is gone from the config for good, and the
+	// detached streams to it must go too (the ones to upstreams that are
+	// still used by the new config are kept)
 	for _, upstream := range h.Upstreams {
-		_, _ = hosts.Delete(upstream.String())
+		if deleted, _ := hosts.Delete(upstream.String()); deleted {
+			detachedStreams.closeUpstream(upstream.String())
+		}
 	}
 
 	return err
@@ -2093,5 +2128,6 @@ var errNoUpstream = fmt.Errorf("no upstreams available")
 var (
 	_ caddy.Provisioner           = (*Handler)(nil)
 	_ caddy.CleanerUpper          = (*Handler)(nil)
+	_ caddy.Validator             = (*Handler)(nil)
 	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
 )
diff --git a/modules/caddyhttp/reverseproxy/streaming.go b/modules/caddyhttp/reverseproxy/streaming.go
index 4a2f39b2..e6b7665f 100644
--- a/modules/caddyhttp/reverseproxy/streaming.go
+++ b/modules/caddyhttp/reverseproxy/streaming.go
@@ -26,8 +26,10 @@ import (
 	"io"
 	weakrand "math/rand/v2"
 	"mime"
+	"net"
 	"net/http"
 	"sync"
+	"sync/atomic"
 	"time"
 	"unsafe"
 
@@ -125,9 +127,12 @@ func (h *Handler) handleUpgradeResponse(logger *zap.Logger, wg *sync.WaitGroup,
 			c.Write(zap.Int("http_version", req.ProtoMajor))
 		}
 
-		var hijackErr error
+		var (
+			hijacked  net.Conn
+			hijackErr error
+		)
 		//nolint:bodyclose
-		conn, brw, hijackErr = http.NewResponseController(rw).Hijack()
+		hijacked, brw, hijackErr = http.NewResponseController(rw).Hijack()
 		if errors.Is(hijackErr, http.ErrNotSupported) {
 			if c := h.logger.Check(zap.ErrorLevel, "can't switch protocols using non-Hijacker ResponseWriter"); c != nil {
 				c.Write(zap.String("type", fmt.Sprintf("%T", rw)))
@@ -141,6 +146,21 @@ func (h *Handler) handleUpgradeResponse(logger *zap.Logger, wg *sync.WaitGroup,
 			}
 			return
 		}
+
+		// copy on the raw connection, not through a recording response
+		// writer that would account and buffer for a response that is over
+		conn = caddyhttp.RawConn(hijacked)
+	}
+
+	stream := newStreamState(upstreamKey(req.Context()), func(sum streamSummary) {
+		if c := logger.Check(zapcore.DebugLevel, "connection closed"); c != nil {
+			c.Write(zap.Duration("duration", sum.duration))
+		}
+		h.logStreamClosed(req, sum)
+	})
+
+	if h.StreamLogs != nil && h.StreamLogs.SkipHandshake {
+		caddyhttp.SetVar(req.Context(), caddyhttp.LogSkipVar, true)
 	}
 
 	// adopted from https://github.com/golang/go/commit/8bcf2834afdf6a1f7937390903a41518715ef6f5
@@ -150,18 +170,22 @@ func (h *Handler) handleUpgradeResponse(logger *zap.Logger, wg *sync.WaitGroup,
 		// See issue https://golang.org/issue/35559.
 		select {
 		case <-req.Context().Done():
+			stream.setReason(streamResultCanceled)
 		case <-backConnCloseCh:
 		}
 		backConn.Close()
 	}()
 	defer close(backConnCloseCh)
 
-	start := time.Now()
+	// result is what the stream is accounted with, unless a close path
+	// that ran first (cancellation, reload) already gave a reason
+	result := streamResultError
 	defer func() {
 		conn.Close()
-		if c := logger.Check(zapcore.DebugLevel, "connection closed"); c != nil {
-			c.Write(zap.Duration("duration", time.Since(start)))
-		}
+		backConn.Close()
+		// wait for the copiers, so the byte counts are final
+		wg.Wait()
+		stream.finish(result)
 	}()
 
 	if err := brw.Flush(); err != nil {
@@ -182,6 +206,7 @@ func (h *Handler) handleUpgradeResponse(logger *zap.Logger, wg *sync.WaitGroup,
 			}
 			return
 		}
+		stream.toBackend.Add(int64(len(data)))
 	}
 
 	// Ensure the hijacked client connection, and the new connection established
@@ -199,16 +224,25 @@ func (h *Handler) handleUpgradeResponse(logger *zap.Logger, wg *sync.WaitGroup,
 		}
 		return nil
 	}
-	deleteFrontConn := h.registerConnection(conn, gracefulClose(conn, false))
-	deleteBackConn := h.registerConnection(backConn, gracefulClose(backConn, true))
-	defer deleteFrontConn()
-	defer deleteBackConn()
+
+	// Detached streams are not tied to this handler (and so to the config it
+	// belongs to): they are tracked process-wide, and only closed when their
+	// upstream is removed from the config.
+	front := openConnection{conn: conn, gracefulClose: gracefulClose(conn, false), stream: stream}
+	back := openConnection{conn: backConn, gracefulClose: gracefulClose(backConn, true), stream: stream}
+	if h.StreamDetached {
+		defer detachedStreams.add(stream.upstream, front, back)()
+	} else {
+		defer h.registerConnection(front)()
+		defer h.registerConnection(back)()
+	}
 
 	spc := switchProtocolCopier{
 		user:       conn,
 		backend:    backConn,
 		wg:         wg,
 		bufferSize: h.StreamBufferSize,
+		stream:     stream,
 	}
 
 	// setup the timeout if requested
@@ -261,9 +295,11 @@ func (h *Handler) handleUpgradeResponse(logger *zap.Logger, wg *sync.WaitGroup,
 			if c := logger.Check(zapcore.DebugLevel, "stream timed out"); c != nil {
 				c.Write(zap.Time("timeout", time))
 			}
+			result = streamResultTimeout
 			return
 		}
 	}
+	result = streamResultCompleted
 }
 
 // flushInterval returns the p.FlushInterval value, conditionally
@@ -414,13 +450,13 @@ func (h Handler) copyBuffer(dst io.Writer, src io.Reader, buf []byte, logger *za
 // connections dialed to backends don't close when server is shut down.
 // The caller should call the returned delete() function when the
 // connection is done to remove it from memory.
-func (h *Handler) registerConnection(conn io.ReadWriteCloser, gracefulClose func() error) (del func()) {
+func (h *Handler) registerConnection(oc openConnection) (del func()) {
 	h.connectionsMu.Lock()
-	h.connections[conn] = openConnection{conn, gracefulClose}
+	h.connections[oc.conn] = oc
 	h.connectionsMu.Unlock()
 	return func() {
 		h.connectionsMu.Lock()
-		delete(h.connections, conn)
+		delete(h.connections, oc.conn)
 		// if there is no connection left before the connections close timer fires
 		if len(h.connections) == 0 && h.connectionsCloseTimer != nil {
 			// we release the timer that holds the reference to Handler
@@ -440,17 +476,11 @@ func (h *Handler) closeConnections() error {
 	defer h.connectionsMu.Unlock()
 
 	for _, oc := range h.connections {
-		if oc.gracefulClose != nil {
-			// this is potentially blocking while we have the lock on the connections
-			// map, but that should be OK since the server has in theory shut down
-			// and we are no longer using the connections map
-			gracefulErr := oc.gracefulClose()
-			if gracefulErr != nil && err == nil {
-				err = gracefulErr
-			}
-		}
-		closeErr := oc.conn.Close()
-		if closeErr != nil && err == nil {
+		oc.stream.setReason(streamResultShutdown)
+		// this is po
... (truncated; 28437 more characters; read the files)
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
