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

One PR: add a process-wide detached-stream registry keyed by upstream, stream_detached and stream_logs options, hijack-safe recorder/encode/intercept, stream metrics, docs, and unit plus integration tests. Docs live in Go doc comments (generated docs), no separate doc files.

## Task T1 (T1)

Order: (1) hijack safety in caddyhttp recorder, encode and intercept; (2) new stream registry (detached.go) plus StreamDetached/StreamLogs fields, Provision/Cleanup and handleUpgradeResponse using raw conn; (3) metrics; (4) Caddyfile parsing and docs; (5) unit tests then integration tests. Default non-detached path keeps closeConnections/StreamCloseDelay unchanged.

Acceptance criteria:
- ACC1 [behavior]: With stream_detached, an established Upgrade stream keeps passing data after a config reload; when the reload removes its upstream that stream is closed while streams to retained upstreams stay open.
- ACC2 [behavior]: Caddyfile and JSON stream_detached both set Handler.StreamDetached and survive Caddyfile-to-JSON adaptation.
- ACC3 [behavior]: Without stream_detached an upgraded stream is closed on reload; with stream_close_delay it stays open until the delay elapses, then closes.
- ACC4 [behavior]: stream_logs with skip_handshake, level and logger_name parse from Caddyfile and JSON; defaults are handshake logged and level DEBUG; skip_handshake suppresses the access log entry for the upgrade request.
- ACC5 [behavior]: With logger_name access, a stream close log entry appears in the access log at the configured level, including duration and byte counts.
- ACC6 [behavior]: WebSocket upgrades through encode and through intercept with handle_response work end to end; the client receives exactly one 101 and no writes happen to the response after hijack.
- ACC7 [behavior]: The proxy copies stream bytes on the raw hijacked connection, so the recorder size is not inflated by stream bytes.
- ACC8 [behavior]: HTTP/2 extended CONNECT WebSocket through reverse_proxy transfers data, and an HTTP/1.1 upgrade works in detached mode.
- ACC9 [behavior]: Active-streams, closed-by-result, duration and bytes-to/from-backend metrics are updated, and each stream is counted exactly once even when both directions end, time out or are closed by cleanup.
- ACC10 [other]: Doc comments on the Handler JSON fields and the Caddyfile syntax block describe stream_detached, stream_logs, skip_handshake and the log level.
- ACC11 [behavior]: Integration tests with a real server and clients cover reload survive/close in both modes, h2 extended CONNECT, upgrades via encode and intercept, and detached streams with access logging.
- ACC12 [behavior]: Unit tests cover the registry, config parsing, metrics, recorder hijack behaviour, and encode and intercept post-hijack behaviour; existing suites stay green.

Files declared for the task:
- modules/caddyhttp/reverseproxy/streaming.go (production)
- modules/caddyhttp/reverseproxy/reverseproxy.go (production)
- modules/caddyhttp/reverseproxy/caddyfile.go (production)
- modules/caddyhttp/reverseproxy/metrics.go (production)
- modules/caddyhttp/reverseproxy/detached.go (production)
- modules/caddyhttp/responsewriter.go (production)
- modules/caddyhttp/encode/encode.go (production)
- modules/caddyhttp/intercept/intercept.go (production)
- modules/caddyhttp/server.go (production)
- modules/caddyhttp/reverseproxy/streaming_test.go (test)
- modules/caddyhttp/reverseproxy/detached_test.go (test)
- modules/caddyhttp/reverseproxy/caddyfile_test.go (test)
- modules/caddyhttp/reverseproxy/metrics_test.go (test)
- modules/caddyhttp/responsewriter_test.go (test)
- modules/caddyhttp/encode/encode_test.go (test)
- caddytest/integration/stream_test.go (test)
- caddytest/integration/stream_reload_test.go (test)
- caddytest/integration/intercept_test.go (test)
- caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest (test)

Test files of this task that the change modified (existing tests, declared by the plan):
- caddytest/integration/intercept_test.go
- caddytest/integration/stream_test.go
- modules/caddyhttp/encode/encode_test.go
- modules/caddyhttp/responsewriter_test.go
- modules/caddyhttp/reverseproxy/streaming_test.go

Test results: selected tests green; red proven; suite green

## The diff

```diff
diff --git a/caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest b/caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest
new file mode 100644
index 00000000..dd99dad1
--- /dev/null
+++ b/caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest
@@ -0,0 +1,62 @@
+https://example.com {
+	reverse_proxy localhost:54321 {
+		stream_detached
+		stream_logs {
+			skip_handshake
+			level info
+			logger_name access
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
+														"logger_name": "access",
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
diff --git a/caddytest/integration/intercept_test.go b/caddytest/integration/intercept_test.go
index c1b33b96..82857e82 100644
--- a/caddytest/integration/intercept_test.go
+++ b/caddytest/integration/intercept_test.go
@@ -1,6 +1,7 @@
 package integration
 
 import (
+	"fmt"
 	"testing"
 
 	"github.com/caddyserver/caddy/v2/caddytest"
@@ -39,6 +40,45 @@ func TestIntercept(t *testing.T) {
 	tester.AssertGetResponse("http://localhost:9080/no-intercept", 200, "I'm not a teapot")
 }
 
+// An upgrade works through an intercept handler that would take any response
+// (handle_response without a matcher): the 101 is final, it is neither buffered
+// nor replaced, and nothing is written to the response once the connection is
+// hijacked.
+func TestInterceptUpgrade(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+
+	tester := caddytest.NewTester(t)
+	tester.InitServer(fmt.Sprintf(`{
+		skip_install_trust
+		admin localhost:2999
+		http_port     9080
+		https_port    9443
+		grace_period  1ns
+	}
+
+	localhost:9080 {
+		route {
+			intercept {
+				handle_response {
+					respond "intercepted" 200
+				}
+			}
+			respond /plain "plain body" 404
+			reverse_proxy %s
+		}
+	}
+	`, streamBackendA), "caddyfile")
+
+	// other responses are intercepted
+	tester.AssertGetResponse("http://localhost:9080/plain", 200, "intercepted")
+
+	for _, proto := range []string{"tunnel", "websocket"} {
+		tn := openTunnel(t, "/ws", proto)
+		tn.echo("through intercept")
+		tn.echo("and again")
+	}
+}
+
 func TestInterceptReplaceStatusWithMatcher(t *testing.T) {
 	tester := caddytest.NewTester(t)
 	tester.InitServer(`{
diff --git a/caddytest/integration/stream_reload_test.go b/caddytest/integration/stream_reload_test.go
new file mode 100644
index 00000000..9d79e503
--- /dev/null
+++ b/caddytest/integration/stream_reload_test.go
@@ -0,0 +1,399 @@
+package integration
+
+import (
+	"bufio"
+	"encoding/json"
+	"fmt"
+	"io"
+	"net"
+	"net/http"
+	"os"
+	"path/filepath"
+	"strings"
+	"sync"
+	"testing"
+	"time"
+
+	"github.com/caddyserver/caddy/v2/caddytest"
+)
+
+const (
+	streamBackendA = "127.0.0.1:54322"
+	streamBackendB = "127.0.0.1:54323"
+
+	streamTestWait = 10 * time.Second
+)
+
+// upgradeBackend is an HTTP/1.1 server which switches every request to the
+// protocol the client asks for, and then echoes what it reads.
+type upgradeBackend struct {
+	server *http.Server
+
+	mu    sync.Mutex
+	conns []net.Conn
+}
+
+func startUpgradeBackend(t *testing.T, addr string) *upgradeBackend {
+	t.Helper()
+
+	ln, err := net.Listen("tcp", addr)
+	if err != nil {
+		t.Fatalf("listening on %s: %v", addr, err)
+	}
+	b := &upgradeBackend{}
+	b.server = &http.Server{Handler: http.HandlerFunc(b.serve)}
+	go b.server.Serve(ln)
+	t.Cleanup(func() {
+		b.server.Close()
+		b.mu.Lock()
+		defer b.mu.Unlock()
+		for _, conn := range b.conns {
+			conn.Close()
+		}
+	})
+	return b
+}
+
+func (b *upgradeBackend) serve(w http.ResponseWriter, r *http.Request) {
+	conn, brw, err := http.NewResponseController(w).Hijack()
+	if err != nil {
+		return
+	}
+	b.mu.Lock()
+	b.conns = append(b.conns, conn)
+	b.mu.Unlock()
+	defer conn.Close()
+
+	fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", r.Header.Get("Upgrade"))
+	io.Copy(conn, brw.Reader)
+}
+
+// tunnel is a client of an upgraded connection.
+type tunnel struct {
+	t    *testing.T
+	conn net.Conn
+	br   *bufio.Reader
+}
+
+// openTunnel requests an upgrade to proto at path of the test server, and
+// expects exactly one 101 response. headers are added to the request.
+func openTunnel(t *testing.T, path, proto string, headers ...string) *tunnel {
+	t.Helper()
+
+	conn, err := net.DialTimeout("tcp", "127.0.0.1:9080", streamTestWait)
+	if err != nil {
+		t.Fatalf("dialing: %v", err)
+	}
+	t.Cleanup(func() { conn.Close() })
+
+	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: localhost:9080\r\nConnection: Upgrade\r\nUpgrade: %s\r\n", path, proto)
+	for _, h := range headers {
+		req += h + "\r\n"
+	}
+	if _, err := io.WriteString(conn, req+"\r\n"); err != nil {
+		t.Fatalf("writing the upgrade request: %v", err)
+	}
+
+	tn := &tunnel{t: t, conn: conn, br: bufio.NewReader(conn)}
+	conn.SetReadDeadline(time.Now().Add(streamTestWait))
+	resp, err := http.ReadResponse(tn.br, nil)
+	if err != nil {
+		t.Fatalf("reading the upgrade response: %v", err)
+	}
+	if resp.StatusCode != http.StatusSwitchingProtocols {
+		t.Fatalf("status = %d, want 101", resp.StatusCode)
+	}
+	return tn
+}
+
+// echo checks that msg makes it to the backend and back. A second response
+// after the 101 would be read here instead of the echo.
+func (tn *tunnel) echo(msg string) {
+	tn.t.Helper()
+	if _, err := io.WriteString(tn.conn, msg); err != nil {
+		tn.t.Fatalf("writing %q: %v", msg, err)
+	}
+	tn.conn.SetReadDeadline(time.Now().Add(streamTestWait))
+	buf := make([]byte, len(msg))
+	if _, err := io.ReadFull(tn.br, buf); err != nil {
+		tn.t.Fatalf("reading the echo of %q: %v", msg, err)
+	}
+	if string(buf) != msg {
+		tn.t.Fatalf("echo = %q, want %q", buf, msg)
+	}
+}
+
+// expectClosed waits for the proxy to close the tunnel.
+func (tn *tunnel) expectClosed() {
+	tn.t.Helper()
+	tn.conn.SetReadDeadline(time.Now().Add(streamTestWait))
+	if _, err := tn.br.ReadByte(); err == nil {
+		tn.t.Fatal("the tunnel is still passing data, want it closed")
+	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
+		tn.t.Fatalf("the tunnel was not closed: %v", err)
+	}
+}
+
+// streamSite returns a Caddyfile serving routes on localhost:9080. The version
+// is answered at /version, so that every config differs and is really reloaded.
+func streamSite(version, routes string) string {
+	return fmt.Sprintf(`{
+		skip_install_trust
+		admin localhost:2999
+		http_port 9080
+		https_port 9443
+		grace_period 1ns
+	}
+
+	localhost:9080 {
+		handle /version {
+			respond "%s"
+		}
+		%s
+	}`, version, routes)
+}
+
+func streamProxy(addr, path, options string) string {
+	return fmt.Sprintf(`handle %s {
+			reverse_proxy %s {
+				%s
+			}
+		}`, path, addr, options)
+}
+
+// With stream_detached, a stream survives a reload and is closed only when its
+// upstream is not part of the config anymore. HTTP/1.1 upgrades work in this mode.
+func TestStreamDetachedReload(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+	startUpgradeBackend(t, streamBackendB)
+
+	tester := caddytest.NewTester(t)
+	tester.InitServer(streamSite("v1",
+		streamProxy(streamBackendA, "/a", "stream_detached")+"\n"+
+			streamProxy(streamBackendB, "/b", "stream_detached")), "caddyfile")
+
+	a := openTunnel(t, "/a", "tunnel")
+	b := openTunnel(t, "/b", "tunnel")
+	a.echo("hello a")
+	b.echo("hello b")
+
+	// the new config keeps B and drops A
+	tester.InitServer(streamSite("v2", streamProxy(streamBackendB, "/b", "stream_detached")), "caddyfile")
+	tester.AssertGetResponse("http://localhost:9080/version", 200, "v2")
+
+	a.expectClosed()
+	b.echo("still there")
+
+	// and a new stream through the retained upstream works
+	openTunnel(t, "/b", "tunnel").echo("new stream")
+}
+
+// Without stream_detached, streams are closed when the config is reloaded.
+func TestStreamDefaultClosedOnReload(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+
+	tester := caddytest.NewTester(t)
+	tester.InitServer(streamSite("v1", streamProxy(streamBackendA, "/a", "")), "caddyfile")
+
+	a := openTunnel(t, "/a", "tunnel")
+	a.echo("hello")
+
+	// even though the new config has the same upstream
+	tester.InitServer(streamSite("v2", streamProxy(streamBackendA, "/a", "")), "caddyfile")
+
+	a.expectClosed()
+}
+
+// With stream_close_delay, streams are closed after the delay once the config is reloaded.
+func TestStreamCloseDelayReload(t *testing.T) {
+	const delay = 4 * time.Second
+	startUpgradeBackend(t, streamBackendA)
+
+	tester := caddytest.NewTester(t)
+	options := fmt.Sprintf("stream_close_delay %s", delay)
+	tester.InitServer(streamSite("v1", streamProxy(streamBackendA, "/a", options)), "caddyfile")
+
+	a := openTunnel(t, "/a", "tunnel")
+	a.echo("hello")
+
+	reloaded := time.Now()
+	tester.InitServer(streamSite("v2", streamProxy(streamBackendA, "/a", options)), "caddyfile")
+
+	a.echo("still open during the delay")
+	if time.Since(reloaded) >= delay {
+		t.Skip("the reload took longer than the delay")
+	}
+
+	a.expectClosed()
+	if elapsed := time.Since(reloaded); elapsed < delay-100*time.Millisecond {
+		t.Errorf("the stream was closed %v after the reload, before the delay of %v", elapsed, delay)
+	}
+}
+
+// readLogLines returns the JSON log entries of the file.
+func readLogLines(t *testing.T, path string) []map[string]any {
+	t.Helper()
+	data, err := os.ReadFile(path)
+	if err != nil {
+		return nil
+	}
+	var entries []map[string]any
+	for _, line := range strings.Split(string(data), "\n") {
+		if strings.TrimSpace(line) == "" {
+			continue
+		}
+		var entry map[string]any
+		if err := json.Unmarshal([]byte(line), &entry); err != nil {
+			t.Fatalf("log line is not JSON: %q", line)
+		}
+		entries = append(entries, entry)
+	}
+	return entries
+}
+
+func findLogEntry(entries []map[string]any, match func(map[string]any) bool) map[string]any {
+	for _, entry := range entries {
+		if match(entry) {
+			return entry
+		}
+	}
+	return nil
+}
+
+func waitForLogEntry(t *testing.T, path, what string, match func(map[string]any) bool) map[string]any {
+	t.Helper()
+	deadline := time.Now().Add(streamTestWait)
+	for time.Now().Before(deadline) {
+		if entry := findLogEntry(readLogLines(t, path), match); entry != nil {
+			return entry
+		}
+		time.Sleep(50 * time.Millisecond)
+	}
+	t.Fatalf("timed out waiting for %s in the access log:\n%v", what, readLogLines(t, path))
+	return nil
+}
+
+func handledRequest(uri string, status float64) func(map[string]any) bool {
+	return func(entry map[string]any) bool {
+		req, _ := entry["request"].(map[string]any)
+		return entry["msg"] == "handled request" && req["uri"] == uri && entry["status"] == status
+	}
+}
+
+func streamClosed(entry map[string]any) bool {
+	return entry["msg"] == "stream closed"
+}
+
+// A detached stream is logged to the access log: the handshake when it is
+// established unless skip_handshake is set, and the close of the stream.
+func TestStreamDetachedAccessLog(t *testing.T) {
+	for _, skipHandshake := range []bool{false, true} {
+		t.Run(fmt.Sprintf("skip_handshake=%v", skipHandshake), func(t *testing.T) {
+			startUpgradeBackend(t, streamBackendA)
+			logPath := filepath.Join(t.TempDir(), "access.log")
+
+			streamLogs := "level info\nlogger_name access"
+			if skipHandshake {
+				streamLogs = "skip_handshake\n" + streamLogs
+			}
+			config := fmt.Sprintf(`{
+				skip_install_trust
+				admin localhost:2999
+				http_port 9080
+				https_port 9443
+				grace_period 1ns
+				log requests {
+					output file %s
+					format json
+					include http.log.access
+				}
+			}
+
+			localhost:9080 {
+				log
+				handle /version {
+					respond "v1"
+				}
+				handle /a {
+					reverse_proxy %s {
+						stream_detached
+						stream_logs {
+							%s
+						}
+					}
+				}
+			}`, logPath, streamBackendA, streamLogs)
+
+			tester := caddytest.NewTester(t)
+			tester.InitServer(config, "caddyfile")
+
+			// the access log works
+			tester.AssertGetResponse("http://localhost:9080/version", 200, "v1")
+			waitForLogEntry(t, logPath, "the entry of /version", handledRequest("/version", 200))
+
+			tn := openTunnel(t, "/a", "tunnel")
+			tn.echo("ping")
+			if !skipHandshake {
+				// the handler returned when the stream was detached, so the handshake is logged already
+				waitForLogEntry(t, logPath, "the entry of the handshake", handledRequest("/a", 101))
+			}
+
+			tn.conn.Close()
+			closed := waitForLogEntry(t, logPath, "the stream close entry", streamClosed)
+			if closed["level"] != "info" {
+				t.Errorf("level = %v, want info", closed["level"])
+			}
+			if closed["logger"] != "http.log.access" {
+				t.Errorf("logger = %v, want http.log.access", closed["logger"])
+			}
+			if closed["upstream"] != streamBackendA {
+				t.Errorf("upstream = %v, want %s", closed["upstream"], streamBackendA)
+			}
+			if closed["bytes_to_backend"] != float64(4) || closed["bytes_from_backend"] != float64(4) {
+				t.Errorf("bytes = %v to, %v from; want 4 each", closed["bytes_to_backend"], closed["bytes_from_backend"])
+			}
+			if _, ok := closed["duration"]; !ok || closed["result"] == nil || closed["result"] == "" {
+				t.Errorf("the entry has no duration or result: %v", closed)
+			}
+
+			handshake := findLogEntry(readLogLines(t, logPath), handledRequest("/a", 101))
+			if skipHandshake && handshake != nil {
+				t.Errorf("the handshake was logged although skip_handshake is set: %v", handshake)
+			}
+			if !skipHandshake && handshake == nil {
+				t.Error("the handshake was not logged")
+			}
+		})
+	}
+}
+
+// An upgrade works through the encode handler, whether the client asks for
+// encoding or not, and the client gets exactly one 101.
+func TestStreamThroughEncode(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+
+	tester := caddytest.NewTester(t)
+	tester.InitServer(streamSite("v1", fmt.Sprintf(`handle /a {
+			encode gzip
+			reverse_proxy %s
+		}`, streamBackendA)), "caddyfile")
+
+	for name, headers := range map[string][]string{
+		"no encoding requested": nil,
+		"gzip requested":        {"Accept-Encoding: gzip"},
+		"websocket handshake":   {"Accept-Encoding: gzip", "Sec-WebSocket-Version: 13", "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ=="},
+	} {
+		t.Run(name, func(t *testing.T) {
+			proto := "tunnel"
+			if strings.Contains(strings.Join(headers, " "), "Sec-WebSocket") {
+				proto = "websocket"
+			}
+			tn := openTunnel(t, "/a", proto, headers...)
+			tn.echo("through encode")
+			tn.echo("and again")
+		})
+	}
+
+	// the other routes keep working
+	tester.AssertGetResponse("http://localhost:9080/version", 200, "v1")
+}
diff --git a/caddytest/integration/stream_test.go b/caddytest/integration/stream_test.go
index 0f960436..f7b86c87 100644
--- a/caddytest/integration/stream_test.go
+++ b/caddytest/integration/stream_test.go
@@ -1,20 +1,25 @@
 package integration
 
 import (
+	"bytes"
 	"compress/gzip"
 	"context"
 	"crypto/rand"
+	"crypto/tls"
 	"fmt"
 	"io"
 	"net/http"
 	"net/http/httputil"
 	"net/url"
+	"os"
+	"os/exec"
 	"strings"
 	"testing"
 	"time"
 
 	"golang.org/x/net/http2"
 	"golang.org/x/net/http2/h2c"
+	"golang.org/x/net/http2/hpack"
 
 	"github.com/caddyserver/caddy/v2/caddytest"
 )
@@ -203,6 +208,173 @@ func testH2ToH2CStreamServeH2C(t *testing.T) *http.Server {
 	return server
 }
 
+// h2ExtendedConnectChildEnv marks the process that runs the test of
+// extended CONNECT, see TestH2ExtendedConnectWebSocket.
+const h2ExtendedConnectChildEnv = "CADDY_TEST_H2_EXTENDED_CONNECT"
+
+// A WebSocket over HTTP/2 (RFC 8441 extended CONNECT) is proxied to an HTTP/1.1
+// backend as an upgrade, and the data goes through both ways.
+func TestH2ExtendedConnectWebSocket(t *testing.T) {
+	// net/http only enables extended CONNECT in its HTTP/2 server when
+	// GODEBUG=http2xconnect=1 is set when the process starts, so run the test
+	// in a process started like that. It uses its own ports and admin
+	// endpoint, since the servers of the tests which ran before this one may
+	// be running in this process still.
+	if os.Getenv(h2ExtendedConnectChildEnv) == "" {
+		if testing.Short() {
+			t.Skip("skipping integration test in short mode")
+		}
+		// not os.Args[0], which the tester replaces to start Caddy
+		self, _ := os.Executable() // if it fails, so does running it
+		cmd := exec.Command(self, "-test.run=^TestH2ExtendedConnectWebSocket$", "-test.count=1", "-test.timeout=2m")
+		cmd.Env = append(os.Environ(), "GODEBUG=http2xconnect=1", h2ExtendedConnectChildEnv+"=1")
+		if out, err := cmd.CombinedOutput(); err != nil {
+			t.Fatalf("the test failed in the process with extended CONNECT enabled: %v\n%s", err, out)
+		}
+		return
+	}
+
+	const (
+		adminPort = 2998
+		httpsPort = 9445
+	)
+
+	startUpgradeBackend(t, streamBackendA)
+
+	tester := caddytest.NewTester(t).WithDefaultOverrides(caddytest.Config{AdminPort: adminPort})
+	tester.InitServer(fmt.Sprintf(`{
+		skip_install_trust
+		admin localhost:%d
+		http_port     9081
+		https_port    %d
+		auto_https    disable_redirects
+		grace_period  1ns
+	}
+
+	localhost:%d {
+		reverse_proxy %s
+	}
+	`, adminPort, httpsPort, httpsPort, streamBackendA), "caddyfile")
+
+	// The standard clients can't send the :protocol pseudo-header, so speak
+	// HTTP/2 by hand.
+	conn, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", httpsPort), &tls.Config{ //nolint:gosec
+		InsecureSkipVerify: true,
+		ServerName:         "localhost",
+		NextProtos:         []string{"h2"},
+	})
+	if err != nil {
+		t.Fatalf("dialing: %v", err)
+	}
+	defer conn.Close()
+	// don't hang if the data doesn't come back
+	conn.SetDeadline(time.Now().Add(streamTestWait))
+
+	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
+		t.Fatal(err)
+	}
+	fr := http2.NewFramer(conn, conn)
+	if err := fr.WriteSettings(); err != nil {
+		t.Fatal(err)
+	}
+
+	// wait for the settings of the server, which has to allow extended CONNECT
+	const settingEnableConnectProtocol = http2.SettingID(0x8)
+	var extendedConnect bool
+	for {
+		f, err := fr.ReadFrame()
+		if err != nil {
+			t.Fatalf("reading the settings of the server: %v", err)
+		}
+		sf, ok := f.(*http2.SettingsFrame)
+		if !ok || sf.IsAck() {
+			continue
+		}
+		if v, ok := sf.Value(settingEnableConnectProtocol); ok && v == 1 {
+			extendedConnect = true
+		}
+		if err := fr.WriteSettingsAck(); err != nil {
+			t.Fatal(err)
+		}
+		break
+	}
+	if !extendedConnect {
+		t.Fatal("the HTTP/2 server doesn't allow extended CONNECT")
+	}
+
+	var block bytes.Buffer
+	enc := hpack.NewEncoder(&block)
+	for _, field := range [][2]string{
+		{":method", "CONNECT"},
+		{":scheme", "https"},
+		{":authority", fmt.Sprintf("localhost:%d", httpsPort)},
+		{":path", "/ws"},
+		{":protocol", "websocket"},
+	} {
+		if err := enc.WriteField(hpack.HeaderField{Name: field[0], Value: field[1]}); err != nil {
+			t.Fatal(err)
+		}
+	}
+	const streamID = 1
+	if err := fr.WriteHeaders(http2.HeadersFrameParam{StreamID: streamID, BlockFragment: block.Bytes(), EndHeaders: true}); err != nil {
+		t.Fatal(err)
+	}
+
+	// readStream returns the next n bytes of data of the stream; the
+	// response headers are checked on the way
+	var (
+		dec      = hpack.NewDecoder(4096, nil)
+		pending  []byte
+		gotHead  bool
+		readData = func(n int) string {
+			for len(pending) < n {
+				f, err := fr.ReadFrame()
+				if err != nil {
+					t.Fatalf("reading the stream: %v", err)
+				}
+				switch f := f.(type) {
+				case *http2.HeadersFrame:
+					fields, err := dec.DecodeFull(f.HeaderBlockFragment())
+					if err != nil {
+						t.Fatalf("decoding the response headers: %v", err)
+					}
+					for _, field := range fields {
+						if field.Name == ":status" && field.Value != "200" {
+							t.Fatalf("status = %s, want 200", field.Value)
+						}
+					}
+					gotHead = true
+				case *http2.DataFrame:
+					pending = append(pending, f.Data()...)
+				case *http2.PingFrame:
+					if !f.IsAck() {
+						fr.WritePing(true, f.Data)
+					}
+				case *http2.RSTStreamFrame:
+					t.Fatalf("the stream was reset: %v", f.ErrCode)
+				case *http2.GoAwayFrame:
+					t.Fatalf("the connection was closed: %v", f.ErrCode)
+				}
+			}
+			got := string(pending[:n])
+			pending = pending[n:]
+			return got
+		}
+	)
+
+	for _, msg := range []string{"hello over h2", "and again"} {
+		if err := fr.WriteData(streamID, false, []byte(msg)); err != nil {
+			t.Fatalf("writing %q: %v", msg, err)
+		}
+		if got := readData(len(msg)); got != msg {
+			t.Fatalf("echo = %q, want %q", got, msg)
+		}
+	}
+	if !gotHead {
+		t.Error("the data came without response headers")
+	}
+}
+
 // (see https://github.com/caddyserver/caddy/issues/3606 for use case)
 func TestH2ToH1ChunkedResponse(t *testing.T) {
 	tester := caddytest.NewTester(t)
diff --git a/modules/caddyhttp/encode/encode.go b/modules/caddyhttp/encode/encode.go
index 46b383ea..408a9037 100644
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
@@ -255,12 +257,27 @@ type responseWriter struct {
 	incremental  bool
 	isConnect    bool
 	disabled     bool // disable encoding for this response
+	hijacked     bool // the connection was taken over, nothing may be written anymore
 }
 
 // WriteHeader stores the status to write when the time comes
-// to actually write the header.
+// to actually write the header. A 101 Switching Protocols status
+// is the final response of an upgrade, so it is written right away
+// and never encoded. Nothing is written once the connection has
+// been hijacked.
 func (rw *responseWriter) WriteHeader(status int) {
+	if rw.hijacked {
+		return
+	}
 	rw.statusCode = status
+
+	if status == http.StatusSwitchingProtocols {
+		rw.disabled = true
+		rw.wroteHeader = true
+		rw.ResponseWriter.WriteHeader(status)
+		return
+	}
+
 	if status == http.StatusPartialContent {
 		rw.disabled = true // partial representations must not be dynamically re-encoded
 	}
@@ -332,6 +349,9 @@ func (enc *Encode) Match(rw *responseWriter) bool {
 // FlushError is an alternative Flush returning an error. It delays the actual Flush of the underlying
 // ResponseWriterWrapper until headers were written.
 func (rw *responseWriter) FlushError() error {
+	if rw.hijacked {
+		return http.ErrHijacked
+	}
 	if !rw.wroteHeader && rw.statusCode == 0 && caddyhttp.IsIncremental(rw.Header()) {
 		rw.WriteHeader(http.StatusOK)
 	}
@@ -365,6 +385,10 @@ func (rw *responseWriter) FlushError() error {
 // it is encoded using the encoder, which is initialized
 // if not done so already.
 func (rw *responseWriter) Write(p []byte) (int, error) {
+	if rw.hijacked {
+		return 0, http.ErrHijacked
+	}
+
 	// ignore zero data writes, probably head request
 	if len(p) == 0 {
 		return 0, nil
@@ -438,6 +462,9 @@ const sniffLen = 512
 // It's based on the standard library HTTP/1.1 response writer implementation.
 // https://github.com/golang/go/blob/f4e3ec3dbe3b8e04a058d266adf8e048bab563f2/src/net/http/server.go#L586
 func (rw *responseWriter) ReadFrom(r io.Reader) (int64, error) {
+	if rw.hijacked {
+		return 0, http.ErrHijacked
+	}
 	if rw.incremental || !rw.wroteHeader && caddyhttp.IsIncremental(rw.Header()) {
 		return io.Copy(writerOnly{rw}, r)
 	}
@@ -475,8 +502,9 @@ func (rw *responseWriter) ReadFrom(r io.Reader) (int64, error) {
 // Close writes any remaining buffered response and
 // deallocates any active resources.
 func (rw *responseWriter) Close() error {
-	// didn't write, probably head request
-	if !rw.wroteHeader {
+	// didn't write, probably head request; a hijacked
+	// connection must not be written to anymore
+	if !rw.wroteHeader && !rw.hijacked {
 		cl, err := strconv.Atoi(rw.Header().Get("Content-Length"))
 		if err == nil && cl > rw.config.MinLength {
 			rw.init()
@@ -504,6 +532,19 @@ func (rw *responseWriter) Unwrap() http.ResponseWriter {
 	return rw.ResponseWriter
 }
 
+// Hijack implements http.Hijacker by hijacking the underlying
+// ResponseWriter. Once it succeeded, rw writes nothing anymore, so
+// that closing the encoder cannot write to the taken-over connection.
+func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	//nolint:bodyclose
+	conn, brw, err := http.NewResponseController(rw.ResponseWriter).Hijack()
+	if err != nil {
+		return nil, nil, err
+	}
+	rw.hijacked = true
+	return conn, brw, nil
+}
+
 // init should be called before we write a response, if rw.buf has contents.
 func (rw *responseWriter) init() {
 	// Don't initialize encoder for responses that must not be encoded.
diff --git a/modules/caddyhttp/encode/encode_test.go b/modules/caddyhttp/encode/encode_test.go
index 8e0037df..a4e838e8 100644
--- a/modules/caddyhttp/encode/encode_test.go
+++ b/modules/caddyhttp/encode/encode_test.go
@@ -1,11 +1,15 @@
 package encode
 
 import (
+	"bufio"
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
 
@@ -388,6 +392,103 @@ func TestIsEncodeAllowed(t *testing.T) {
 	}
 }
 
+// hijackableWriter records what is written to it, and can be hijacked.
+type hijackableWriter struct {
+	header   http.Header
+	statuses []int
+	body     []byte
+	hijacked bool
+	conn     net.Conn
+}
+
+func (w *hijackableWriter) Header() http.Header { return w.header }
+func (w *hijackableWriter) WriteHeader(code int) {
+	w.statuses = append(w.statuses, code)
+}
+func (w *hijackableWriter) Write(p []byte) (int, error) {
+	w.body = append(w.body, p...)
+	return len(p), nil
+}
+
+func (w *hijackableWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	w.hijacked = true
+	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
+}
+
+func newHijackableWriter() *hijackableWriter {
+	conn, _ := net.Pipe()
+	return &hijackableWriter{header: make(http.Header), conn: conn}
+}
+
+// A 101 is the final response of an upgrade: it is written once, right away,
+// and neither a later status nor closing the writer writes another one.
+func TestResponseWriterSwitchingProtocolsIsFinal(t *testing.T) {
+	w := newHijackableWriter()
+	defer w.conn.Close()
+	rw := new(Encode).openResponseWriter("gzip", w, false)
+
+	rw.WriteHeader(http.StatusSwitchingProtocols)
+	rw.WriteHeader(http.StatusOK)
+	if err := rw.Close(); err != nil {
+		t.Fatalf("Close() error = %v", err)
+	}
+
+	if len(w.statuses) != 1 || w.statuses[0] != http.StatusSwitchingProtocols {
+		t.Errorf("statuses written = %v, want exactly one 101", w.statuses)
+	}
+	if rw.w != nil {
+		t.Error("a 101 response must not be encoded")
+	}
+}
+
+// Once the connection is hijacked, nothing may be written to the response.
+func TestResponseWriterHijackStopsWrites(t *testing.T) {
+	w := newHijackableWriter()
+	defer w.conn.Close()
+	rw := new(Encode).openResponseWriter("gzip", w, false)
+
+	// a status is pending when the connection is taken over
+	rw.WriteHeader(http.StatusTeapot)
+
+	conn, _, err := rw.Hijack()
+	if err != nil {
+		t.Fatalf("Hijack() error = %v", err)
+	}
+	if conn != w.conn || !w.hijacked {
+		t.Fatal("the underlying writer was not hijacked")
+	}
+
+	rw.WriteHeader(http.StatusOK)
+	if n, err := rw.Write([]byte("late")); n != 0 || !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("Write() after Hijack() = %d, %v; want 0, %v", n, err, http.ErrHijacked)
+	}
+	if n, err := rw.ReadFrom(strings.NewReader("late")); n != 0 || !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("ReadFrom() after Hijack() = %d, %v; want 0, %v", n, err, http.ErrHijacked)
+	}
+	if err := rw.FlushError(); !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("FlushError() after Hijack() = %v, want %v", err, http.ErrHijacked)
+	}
+	if err := rw.Close(); err != nil {
+		t.Errorf("Close() error = %v", err)
+	}
+	if len(w.statuses) != 0 || len(w.body) != 0 {
+		t.Errorf("the hijacked writer was written to: statuses %v, body %q", w.statuses, w.body)
+	}
+}
+
+func TestResponseWriterHijackNotSupported(t *testing.T) {
+	// a writer that cannot be hijacked
+	w := &discardResponseWriter{header: make(http.Header)}
+	rw := new(Encode).openResponseWriter("gzip", w, false)
+
+	if _, _, err := rw.Hijack(); err == nil {
+		t.Fatal("Hijack() on a writer that is not a Hijacker succeeded")
+	}
+	if n, err := rw.Write([]byte("body")); n != 4 || err != nil {
+		t.Errorf("Write() after failed Hijack() = %d, %v; want 4, nil", n, err)
+	}
+}
+
 type mockEncoder struct{}
 
 func (mockEncoder) Write(p []byte) (n int, err error) { return len(p), nil }
diff --git a/modules/caddyhttp/responsewriter.go b/modules/caddyhttp/responsewriter.go
index 904c30c0..3413d40a 100644
--- a/modules/caddyhttp/responsewriter.go
+++ b/modules/caddyhttp/responsewriter.go
@@ -70,6 +70,7 @@ type responseRecorder struct {
 	size         int
 	wroteHeader  bool
 	stream       bool
+	hijacked     bool
 
 	readSize *int
 }
@@ -144,9 +145,12 @@ func NewResponseRecorder(w http.ResponseWriter, buf *bytes.Buffer, shouldBuffer
 
 // WriteHeader writes the headers with statusCode to the wrapped
 // ResponseWriter unless the response is to be buffered instead.
-// 1xx responses are never buffered.
+// 1xx responses are never buffered. A 101 Switching Protocols
+// response is final: it is never buffered and no other response
+// can follow it. Nothing is written once the connection has been
+// hijacked.
 func (rr *responseRecorder) WriteHeader(statusCode int) {
-	if rr.wroteHeader {
+	if rr.wroteHeader || rr.hijacked {
 		return
 	}
 
@@ -154,6 +158,15 @@ func (rr *responseRecorder) WriteHeader(statusCode int) {
 	// connections by manually setting headers and writing status 101
 	rr.statusCode = statusCode
 
+	// 101 is the final response of an upgrade, the connection is
+	// about to be taken over, so there is nothing left to buffer
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
@@ -173,6 +186,9 @@ func (rr *responseRecorder) WriteHeader(statusCode int) {
 }
 
 func (rr *responseRecorder) Write(data []byte) (int, error) {
+	if rr.hijacked {
+		return 0, http.ErrHijacked
+	}
 	rr.WriteHeader(http.StatusOK)
 	var n int
 	var err error
@@ -187,6 +203,9 @@ func (rr *responseRecorder) Write(data []byte) (int, error) {
 }
 
 func (rr *responseRecorder) ReadFrom(r io.Reader) (int64, error) {
+	if rr.hijacked {
+		return 0, http.ErrHijacked
+	}
 	rr.WriteHeader(http.StatusOK)
 	var n int64
 	var err error
@@ -223,6 +242,9 @@ func (rr *responseRecorder) Buffered() bool {
 }
 
 func (rr *responseRecorder) WriteResponse() error {
+	if rr.hijacked {
+		return nil
+	}
 	if rr.statusCode == 0 {
 		// could happen if no handlers actually wrote anything,
 		// and this prevents a panic; status must be > 0
@@ -239,6 +261,9 @@ func (rr *responseRecorder) WriteResponse() error {
 // FlushError will suppress actual flushing if the response is buffered. See:
 // https://github.com/caddyserver/caddy/issues/6144
 func (rr *responseRecorder) FlushError() error {
+	if rr.hijacked {
+		return http.ErrHijacked
+	}
 	if rr.stream {
 		//nolint:bodyclose
 		return http.NewResponseController(rr.ResponseWriterWrapper).Flush()
@@ -258,6 +283,13 @@ func (rr *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
 	if err != nil {
 		return nil, nil, err
 	}
+
+	// the connection belongs to the hijacker now: nothing may be buffered
+	// or written to the response anymore, see Buffered and WriteResponse
+	rr.hijacked = true
+	rr.stream = true
+	rr.wroteHeader = true
+
 	// Per http documentation, returned bufio.Writer is empty, but bufio.Read maybe not
 	conn = &hijackedConn{conn, rr}
 	brw.Writer.Reset(conn)
@@ -275,6 +307,20 @@ func (rr *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
 	return conn, brw, nil
 }
 
+// UnwrapHijackedConn returns the connection that was hijacked from the
+// underlying ResponseWriter if conn was returned by Hijack on a response
+// recorder, or conn itself otherwise. Code that takes over a connection
+// for a long time, such as a proxied protocol upgrade, uses it to move
+// bytes on the raw connection instead of through the recorder, so that
+// the recorded response size does not include the stream and the recorder
+// is not touched after the handler has returned.
+func UnwrapHijackedConn(conn net.Conn) net.Conn {
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
index c08ad26a..4251a85a 100644
--- a/modules/caddyhttp/responsewriter_test.go
+++ b/modules/caddyhttp/responsewriter_test.go
@@ -1,13 +1,179 @@
 package caddyhttp
 
 import (
+	"bufio"
 	"bytes"
+	"errors"
 	"io"
+	"net"
 	"net/http"
 	"strings"
 	"testing"
 )
 
+// hijackRespWriter is an http.ResponseWriter that can be hijacked and records
+// everything written to it. The connection it hands out is one end of a pipe.
+type hijackRespWriter struct {
+	header   http.Header
+	statuses []int
+	body     bytes.Buffer
+	hijacked bool
+	conn     net.Conn // the end handed out by Hijack
+	peer     net.Conn // the other end
+}
+
+func newHijackRespWriter() *hijackRespWriter {
+	conn, peer := net.Pipe()
+	return &hijackRespWriter{header: make(http.Header), conn: conn, peer: peer}
+}
+
+func (w *hijackRespWriter) Header() http.Header         { return w.header }
+func (w *hijackRespWriter) WriteHeader(statusCode int)  { w.statuses = append(w.statuses, statusCode) }
+func (w *hijackRespWriter) Write(d []byte) (int, error) { return w.body.Write(d) }
+
+func (w *hijackRespWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	w.hijacked = true
+	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
+}
+
+func TestResponseRecorderHijackStopsWrites(t *testing.T) {
+	w := newHijackRespWriter()
+	defer w.conn.Close()
+	defer w.peer.Close()
+
+	// would buffer everything, if the recorder did not know better
+	rec := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool { return true })
+	rr := rec.(*responseRecorder)
+
+	if _, _, err := rr.Hijack(); err != nil {
+		t.Fatalf("Hijack() error = %v", err)
+	}
+	if !w.hijacked {
+		t.Fatal("the underlying writer was not hijacked")
+	}
+	if rec.Buffered() {
+		t.Error("a hijacked response must not be reported as buffered")
+	}
+
+	rec.WriteHeader(http.StatusTeapot)
+	if len(w.statuses) != 0 {
+		t.Errorf("WriteHeader() after Hijack() wrote statuses %v", w.statuses)
+	}
+	if n, err := rec.Write([]byte("late")); n != 0 || !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("Write() after Hijack() = %d, %v; want 0, %v", n, err, http.ErrHijacked)
+	}
+	if n, err := rr.ReadFrom(strings.NewReader("late")); n != 0 || !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("ReadFrom() after Hijack() = %d, %v; want 0, %v", n, err, http.ErrHijacked)
+	}
+	if err := rr.FlushError(); !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("FlushError() after Hijack() = %v, want %v", err, http.ErrHijacked)
+	}
+	if err := rec.WriteResponse(); err != nil {
+		t.Errorf("WriteResponse() after Hijack() = %v, want nil", err)
+	}
+	if len(w.statuses) != 0 || w.body.Len() != 0 {
+		t.Errorf("the hijacked writer was written to: statuses %v, body %q", w.statuses, w.body.String())
+	}
+	if rec.Size() != 0 {
+		t.Errorf("Size() = %d, want 0", rec.Size())
+	}
+}
+
+func TestResponseRecorderHijackFailureKeepsRecording(t *testing.T) {
+	// a writer that cannot be hijacked
+	w := &baseRespWriter{}
+	rec := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool { return true })
+	rr := rec.(*responseRecorder)
+
+	if _, _, err := rr.Hijack(); err == nil {
+		t.Fatal("Hijack() on a writer that is not a Hijacker succeeded")
+	}
+	if _, err := rec.Write([]byte("buffered")); err != nil {
+		t.Fatalf("Write() after failed Hijack() error = %v", err)
+	}
+	if !rec.Buffered() || rec.Buffer().String() != "buffered" {
+		t.Errorf("after a failed Hijack() the recorder must keep recording, got buffered=%v body=%q", rec.Buffered(), rec.Buffer().String())
+	}
+}
+
+func TestResponseRecorderSwitchingProtocolsIsFinal(t *testing.T) {
+	w := newHijackRespWriter()
+	defer w.conn.Close()
+	defer w.peer.Close()
+
+	buffered := false
+	rec := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool {
+		buffered = true
+		return true
+	})
+
+	rec.WriteHeader(http.StatusSwitchingProtocols)
+	rec.WriteHeader(http.StatusOK) // nothing follows a 101
+
+	if buffered {
+		t.Error("shouldBuffer was consulted for a 101 response")
+	}
+	if rec.Buffered() {
+		t.Error("a 101 response must not be buffered")
+	}
+	if rec.Status() != http.StatusSwitchingProtocols {
+		t.Errorf("Status() = %d, want 101", rec.Status())
+	}
+	if len(w.statuses) != 1 || w.statuses[0] != http.StatusSwitchingProtocols {
+		t.Errorf("statuses written = %v, want exactly one 101", w.statuses)
+	}
+}
+
+func TestUnwrapHijackedConn(t *testing.T) {
+	w := newHijackRespWriter()
+	defer w.conn.Close()
+	defer w.peer.Close()
+
+	rec := NewResponseRecorder(w, nil, nil)
+	conn, _, err := rec.(*responseRecorder).Hijack()
+	if err != nil {
+		t.Fatalf("Hijack() error = %v", err)
+	}
+	if conn == w.conn {
+		t.Fatal("the recorder is expected to wrap the hijacked connection")
+	}
+	if got := UnwrapHijackedConn(conn); got != w.conn {
+		t.Errorf("UnwrapHijackedConn() = %v, want the raw connection", got)
+	}
+	if got := UnwrapHijackedConn(w.peer); got != w.peer {
+		t.Errorf("UnwrapHijackedConn() of a plain connection = %v, want it unchanged", got)
+	}
+}
+
+// Stream bytes moved on the raw connection don't count as response size,
+// those written to the wrapper handed out by the recorder do.
+func TestResponseRecorderSizeIgnoresRawConn(t *testing.T) {
+	w := newHijackRespWriter()
+	defer w.conn.Close()
+	defer w.peer.Close()
+
+	rec := NewResponseRecorder(w, nil, nil)
+	conn, _, err := rec.(*responseRecorder).Hijack()
+	if err != nil {
+		t.Fatalf("Hijack() error = %v", err)
+	}
+
+	go io.Copy(io.Discard, w.peer)
+
+	if _, err := UnwrapHijackedConn(conn).Write([]byte("stream bytes")); err != nil {
+		t.Fatalf("raw write error = %v", err)
+	}
+	if rec.Size() != 0 {
+		t.Errorf("Size() after raw write = %d, want 0", rec.Size())
+	}
+	if _, err := conn.Write([]byte("wrapped")); err != nil {
+		t.Fatalf("wrapped write error = %v", err)
+	}
+	if rec.Size() != len("wrapped") {
+		t.Errorf("Size() after wrapped write = %d, want %d", rec.Size(), len("wrapped"))
+	}
+}
+
 type responseWriterSpy interface {
 	http.ResponseWriter
 	Written() string
diff --git a/modules/caddyhttp/reverseproxy/caddyfile.go b/modules/caddyhttp/reverseproxy/caddyfile.go
index 12c1177b..0e892600 100644
--- a/modules/caddyhttp/reverseproxy/caddyfile.go
+++ b/modules/caddyhttp/reverseproxy/caddyfile.go
@@ -99,6 +99,12 @@ func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error)
 //	    stream_buffer_size <size>
 //	    stream_timeout     <duration>
 //	    stream_close_delay <duration>
+//	    stream_detached
+//	    stream_logs {
+//	        skip_handshake
+//	        level       <level>
+//	        logger_name <name>
+//	    }
 //	    verbose_logs
 //
 //	    # reporting
@@ -141,6 +147,16 @@ func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error)
 // and port may be inferred from other parts of the address/URL; if
 // either are missing, defaults to HTTP.
 //
+// By default, upgraded streams (WebSockets and other protocol upgrades)
+// are closed when the config is reloaded, after stream_close_delay if set.
+// With stream_detached, they are detached from the config and survive a
+// reload; only the streams whose upstream is not part of the new config
+// anymore are closed. stream_logs configures the logging of streams:
+// skip_handshake leaves the upgrade request out of the access log (it is
+// logged by default), level is the log level of the entry written when a
+// stream is closed (default DEBUG), and logger_name set to access writes
+// that entry to the access log instead of the log of this handler.
+//
 // The FinalizeUnmarshalCaddyfile method should be called after this
 // to finalize parsing of "handle_response" blocks, if possible.
 func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
@@ -715,6 +731,50 @@ func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
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
+					if d.NextArg() {
+						return d.ArgErr()
+					}
+
+				case "logger_name":
+					if !d.NextArg() {
+						return d.ArgErr()
+					}
+					h.StreamLogs.LoggerName = d.Val()
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
diff --git a/modules/caddyhttp/reverseproxy/caddyfile_test.go b/modules/caddyhttp/reverseproxy/caddyfile_test.go
new file mode 100644
index 00000000..516d9466
--- /dev/null
+++ b/modules/caddyhttp/reverseproxy/caddyfile_test.go
@@ -0,0 +1,125 @@
+package reverseproxy
+
+import (
+	"encoding/json"
+	"reflect"
+	"testing"
+
+	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
+)
+
+func TestStreamOptionsCaddyfile(t *testing.T) {
+	for i, tc := range []struct {
+		input        string
+		wantDetached bool
+		wantLogs     *StreamLogs
+		wantErr      bool
+	}{
+		{input: `reverse_proxy localhost:9080`},
+		{
+			input: `reverse_proxy localhost:9080 {
+				stream_detached
+			}`,
+			wantDetached: true,
+		},
+		{
+			input: `reverse_proxy localhost:9080 {
+				stream_logs {
+					skip_handshake
+					level INFO
+					logger_name access
+				}
+			}`,
+			wantLogs: &StreamLogs{SkipHandshake: true, Level: "INFO", LoggerName: "access"},
+		},
+		{
+			// only the given options are set, the others keep their defaults
+			input: `reverse_proxy localhost:9080 {
+				stream_detached
+				stream_logs {
+					level warn
+				}
+			}`,
+			wantDetached: true,
+			wantLogs:     &StreamLogs{Level: "warn"},
+		},
+		{
+			input: `reverse_proxy localhost:9080 {
+				stream_logs {
+				}
+			}`,
+			wantLogs: &StreamLogs{},
+		},
+		{input: "reverse_proxy localhost:9080 {\n stream_detached yes\n}", wantErr: true},
+		{input: "reverse_proxy localhost:9080 {\n stream_logs inline {\n skip_handshake\n}\n}", wantErr: true},
+		{input: "reverse_proxy localhost:9080 {\n stream_logs {\n skip_handshake yes\n}\n}", wantErr: true},
+		{input: "reverse_proxy localhost:9080 {\n stream_logs {\n level\n}\n}", wantErr: true},
+		{input: "reverse_proxy localhost:9080 {\n stream_logs {\n level INFO DEBUG\n}\n}", wantErr: true},
+		{input: "reverse_proxy localhost:9080 {\n stream_logs {\n logger_name\n}\n}", wantErr: true},
+		{input: "reverse_proxy localhost:9080 {\n stream_logs {\n logger_name access error\n}\n}", wantErr: true},
+		{input: "reverse_proxy localhost:9080 {\n stream_logs {\n unknown\n}\n}", wantErr: true},
+	} {
+		h := &Handler{}
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
+		if h.StreamDetached != tc.wantDetached {
+			t.Errorf("Test %d: StreamDetached = %v, want %v", i, h.StreamDetached, tc.wantDetached)
+		}
+		if !reflect.DeepEqual(h.StreamLogs, tc.wantLogs) {
+			t.Errorf("Test %d: StreamLogs = %+v, want %+v", i, h.StreamLogs, tc.wantLogs)
+		}
+	}
+}
+
+func TestStreamOptionsJSON(t *testing.T) {
+	const in = `{"stream_detached":true,"stream_logs":{"skip_handshake":true,"level":"INFO","logger_name":"access"}}`
+
+	var h Handler
+	if err := json.Unmarshal([]byte(in), &h); err != nil {
+		t.Fatalf("unmarshal: %v", err)
+	}
+	if !h.StreamDetached {
+		t.Error("StreamDetached = false, want true")
+	}
+	want := &StreamLogs{SkipHandshake: true, Level: "INFO", LoggerName: "access"}
+	if !reflect.DeepEqual(h.StreamLogs, want) {
+		t.Errorf("StreamLogs = %+v, want %+v", h.StreamLogs, want)
+	}
+
+	out, err := json.Marshal(h)
+	if err != nil {
+		t.Fatalf("marshal: %v", err)
+	}
+	var back Handler
+	if err := json.Unmarshal(out, &back); err != nil {
+		t.Fatalf("unmarshal of marshaled handler: %v", err)
+	}
+	if !back.StreamDetached || !reflect.DeepEqual(back.StreamLogs, want) {
+		t.Errorf("round trip lost the stream options: detached=%v logs=%+v", back.StreamDetached, back.StreamLogs)
+	}
+
+	// the defaults are not written out
+	out, err = json.Marshal(Handler{})
+	if err != nil {
+		t.Fatalf("marshal defaults: %v", err)
+	}
+	var m map[string]any
+	if err := json.Unmarshal(out, &m); err != nil {
+		t.Fatal(err)
+	}
+	if _, ok := m["stream_detached"]; ok {
+		t.Error("stream_detached is marshaled although unset")
+	}
+	if _, ok := m["stream_logs"]; ok {
+		t.Error("stream_logs is marshaled although unset")
+	}
+}
diff --git a/modules/caddyhttp/reverseproxy/detached.go b/modules/caddyhttp/reverseproxy/detached.go
new file mode 100644
index 00000000..f05ed954
--- /dev/null
+++ b/modules/caddyhttp/reverseproxy/detached.go
@@ -0,0 +1,280 @@
+// Copyright 2015 Matthew Holt and The Caddy Authors
+//
+// Licensed under the Apache License, Version 2.0 (the "License");
+// you may not use this file except in compliance with the License.
+// You may obtain a copy of the License at
+//
+//     http://www.apache.org/licenses/LICENSE-2.0
+//
+// Unless required by applicable law or agreed to in writing, software
+// distributed under the License is distributed on an "AS IS" BASIS,
+// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
+// See the License for the specific language governing permissions and
+// limitations under the License.
+
+package reverseproxy
+
+import (
+	"fmt"
+	"net/http"
+	"sync"
+	"sync/atomic"
+	"time"
+
+	"go.uber.org/zap"
+	"go.uber.org/zap/zapcore"
+
+	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
+)
+
+// StreamLogs configures the logging of upgraded streams (WebSockets and
+// other protocol upgrades, HTTP/2 extended CONNECT).
+type StreamLogs struct {
+	// If true, the upgrade handshake (the request that is answered with
+	// 101 Switching Protocols) is not written to the access log.
+	// Default: the handshake is logged like any other request.
+	SkipHandshake bool `json:"skip_handshake,omitempty"`
+
+	// The level of the log entry written when a stream is closed. It
+	// carries the upstream, the duration of the stream, the number of
+	// bytes sent to and received from the backend, and the result.
+	// Default: DEBUG.
+	Level string `json:"level,omitempty"`
+
+	// The logger to write the stream close log entry to: "access" selects
+	// the access log, where the entry is written next to the access log
+	// entry of the request that established the stream (and is not written
+	// if the request is not logged). Default: the logger of this handler.
+	LoggerName string `json:"logger_name,omitempty"`
+}
+
+// streamLoggerAccess is the name of the logger that stands for the access log.
+const streamLoggerAccess = "access"
+
+// provisionStreamLogs validates the options of StreamLogs.
+func (h *Handler) provisionStreamLogs() error {
+	if h.StreamLogs == nil {
+		return nil
+	}
+	if h.StreamLogs.Level != "" {
+		var lvl zapcore.Level
+		if err := lvl.UnmarshalText([]byte(h.StreamLogs.Level)); err != nil {
+			return fmt.Errorf("invalid stream log level '%s': %v", h.StreamLogs.Level, err)
+		}
+	}
+	if name := h.StreamLogs.LoggerName; name != "" && name != streamLoggerAccess {
+		return fmt.Errorf("invalid stream logger name '%s': must be '%s' or empty", name, streamLoggerAccess)
+	}
+	return nil
+}
+
+// streamLoggers returns the loggers that the close entry of the stream
+// established by req is written to.
+func (h *Handler) streamLoggers(req *http.Request) []*zap.Logger {
+	if h.StreamLogs == nil || h.StreamLogs.LoggerName != streamLoggerAccess {
+		return []*zap.Logger{h.logger}
+	}
+	server, ok := req.Context().Value(caddyhttp.ServerCtxKey).(*caddyhttp.Server)
+	if !ok {
+		return nil
+	}
+	// the access log of the request is decided by the request as the client
+	// sent it, which can differ from the one sent to the backend (for
+	// example in its Host header)
+	if orig, ok := req.Context().Value(caddyhttp.OriginalRequestCtxKey).(http.Request); ok {
+		req = &orig
+	}
+	return server.AccessLoggers(req)
+}
+
+// streamLevel returns the level of the stream close log entry.
+func (h *Handler) streamLevel() zapcore.Level {
+	lvl := zapcore.DebugLevel
+	if h.StreamLogs != nil && h.StreamLogs.Level != "" {
+		_ = lvl.UnmarshalText([]byte(h.StreamLogs.Level)) // validated when provisioning
+	}
+	return lvl
+}
+
+// Results of a stream, as reported by metrics and in the stream close log.
+const (
+	// both directions ended on their own
+	streamResultCompleted = "completed"
+	// a copy failed, or a connection broke
+	streamResultError = "error"
+	// the stream reached stream_timeout
+	streamResultTimeout = "timeout"
+	// the client request was canceled
+	streamResultCanceled = "canceled"
+	// closed because the config that started the stream was unloaded
+	streamResultCleanup = "cleanup"
+	// closed because its upstream is not part of the config anymore
+	streamResultUpstreamRemoved = "upstream_removed"
+)
+
+// streamStats accounts for one stream. It is created when the stream is
+// established and finish must be called when it is over; whatever the number
+// of times and the order in which things end, the stream is counted once.
+type streamStats struct {
+	upstream string
+	start    time.Time
+	loggers  []*zap.Logger // where the stream close entry is written
+	level    zapcore.Level
+
+	toBackend   atomic.Int64
+	fromBackend atomic.Int64
+
+	mu     sync.Mutex
+	result string
+	once   sync.Once
+}
+
+// newStreamStats starts accounting for a stream to upstream. The stream close
+// entry is written to loggers at the given level.
+func newStreamStats(upstream string, loggers []*zap.Logger, level zapcore.Level) *streamStats {
+	streamMetrics.opened(upstream)
+	return &streamStats{upstream: upstream, start: time.Now(), loggers: loggers, level: level}
+}
+
+// setResult records why the stream ended. Only the first result is kept,
+// so whoever causes the end of the stream must call it before the closes
+// it provokes are observed by the stream itself.
+func (s *streamStats) setResult(result string) {
+	s.mu.Lock()
+	defer s.mu.Unlock()
+	if s.result == "" {
+		s.result = result
+	}
+}
+
+// finish completes the accounting of the stream: metrics, and the stream
+// close log entry. Only the first call has an effect.
+func (s *streamStats) finish() {
+	s.once.Do(func() {
+		s.setResult(streamResultCompleted)
+		s.mu.Lock()
+		result := s.result
+		s.mu.Unlock()
+
+		duration := time.Since(s.start)
+		toBackend, fromBackend := s.toBackend.Load(), s.fromBackend.Load()
+		streamMetrics.finished(s.upstream, result, duration, toBackend, fromBackend)
+
+		for _, logger := range s.loggers {
+			if c := logger.Check(s.level, "stream closed"); c != nil {
+				c.Write(
+					zap.String("upstream", s.upstream),
+					zap.String("result", result),
+					zap.Duration("duration", duration),
+					zap.Int64("bytes_to_backend", toBackend),
+					zap.Int64("bytes_from_backend", fromBackend),
+				)
+			}
+		}
+	})
+}
+
+// detachedStream is a stream that is not owned by the Handler which started it.
+type detachedStream struct {
+	conns []openConnection
+	stats *streamStats
+}
+
+// close ends the stream with the given result.
+func (s *detachedStream) close(result string) error {
+	s.stats.setResult(result)
+	return closeOpenConnections(s.conns)
+}
+
+// closeOpenConnections closes conns, politely first if they know how, and
+// returns the first error.
+func closeOpenConnections(conns []openConnection) error {
+	var err error
+	for _, oc := range conns {
+		if oc.gracefulClose != nil {
+			if gracefulErr := oc.gracefulClose(); gracefulErr != nil && err == nil {
+				err = gracefulErr
+			}
+		}
+		if closeErr := oc.conn.Close(); closeErr != nil && err == nil {
+			err = closeErr
+		}
+	}
+	return err
+}
+
+// detachedRegistry keeps the detached streams (see Handler.StreamDetached),
+// by upstream. Detached streams must outlive the config, and thus the
+// Handler, that started them, so the registry is process-wide. It is what
+// lets a later config close the streams whose upstream it removed.
+type detachedRegistry struct {
+	mu      sync.Mutex
+	streams map[string]map[*detachedStream]struct{}
+}
+
+// detachedStreams is the registry of all detached streams of the process.
+var detachedStreams = &detachedRegistry{streams: make(map[string]map[*detachedStream]struct{})}
+
+// add registers s as a stream to upstream. The returned function removes
+// it again; it can be called several times.
+func (r *detachedRegistry) add(upstream string, s *detachedStream) (remove func()) {
+	r.mu.Lock()
+	set := r.streams[upstream]
+	if set == nil {
+		set = make(map[*detachedStream]struct{})
+		r.streams[upstream] = set
+	}
+	set[s] = struct{}{}
+	r.mu.Unlock()
+
+	return func() {
+		r.mu.Lock()
+		defer r.mu.Unlock()
+		if set := r.streams[upstream]; set != nil {
+			delete(set, s)
+			if len(set) == 0 {
+				delete(r.streams, upstream)
+			}
+		}
+	}
+}
+
+// count returns the number of registered streams to upstream.
+func (r *detachedRegistry) count(upstream string) int {
+	r.mu.Lock()
+	defer r.mu.Unlock()
+	return len(r.streams[upstream])
+}
+
+// closeUpstream closes all the registered streams to upstream, which is
+// not part of the configuration anymore, and returns how many it closed
+// and the first error.
+func (r *detachedRegistry) closeUpstream(upstream string) (int, error) {
+	r.mu.Lock()
+	set := r.streams[upstream]
+	delete(r.streams, upstream)
+	r.mu.Unlock()
+
+	var err error
+	for s := range set {
+		if closeErr := s.close(streamResultUpstreamRemoved); closeErr != nil && err == nil {
+			err = closeErr
+		}
+	}
+	return len(set), err
+}
+
+// upstreamKey returns the key the streams of the proxied response res are
+// registered and accounted under: the upstream the request was sent to.
+func upstreamKey(res *http.Response) string {
+	if res.Request == nil {
+		return ""
+	}
+	if di, ok := GetDialInfo(res.Request.Context()); ok {
+		if di.Upstream != nil {
+			return di.Upstream.String()
+		}
+		return di.Address
+	}
+	return ""
+}
diff --git a/modules/caddyhttp/reverseproxy/detached_test.go b/modules/caddyhttp/reverseproxy/detached_test.go
new file mode 100644
index 00000000..22f6a5a1
--- /dev/null
+++ b/modules/caddyhttp/reverseproxy/detached_test.go
@@ -0,0 +1,804 @@
+package reverseproxy
+
+import (
+	"bufio"
+	"context"
+	"errors"
+	"fmt"
+	"io"
+	"net"
+	"net/http"
+	"net/http/httptest"
+	"strings"
+	"sync"
+	"testing"
+	"time"
+
+	"go.uber.org/zap"
+	"go.uber.org/zap/zapcore"
+	"go.uber.org/zap/zaptest/observer"
+
+	"github.com/caddyserver/caddy/v2"
+	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
+)
+
+const streamTestTimeout = 10 * time.Second
+
+// streamBackend is a backend that answers every request with a 101 and hands
+// the hijacked connection to the test.
+type streamBackend struct {
+	server *httptest.Server
+	conns  chan net.Conn
+}
+
+func newStreamBackend(t *testing.T) *streamBackend {
+	t.Helper()
+	b := &streamBackend{conns: make(chan net.Conn, 4)}
+	b.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
+		conn, _, err := http.NewResponseController(w).Hijack()
+		if err != nil {
+			t.Errorf("backend hijack failed: %v", err)
+			return
+		}
+		fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", r.Header.Get("Upgrade"))
+		b.conns <- conn
+	}))
+	t.Cleanup(b.server.Close)
+	return b
+}
+
+func (b *streamBackend) addr() string { return b.server.Listener.Addr().String() }
+
+// upstream returns an upstream for the backend that is part of the pool of
+// hosts, like the upstreams of a provisioned config.
+func (b *streamBackend) upstream() *Upstream {
+	u := &Upstream{Dial: b.addr()}
+	u.fillHost()
+	return u
+}
+
+// streamHandler returns a handler for the upstream, as if provisioned.
+func streamHandler(u *Upstream) *Handler {
+	h := minimalHandler(0, u)
+	h.connections = make(map[io.ReadWriteCloser]openConnection)
+	h.connectionsMu = new(sync.Mutex)
+	return h
+}
+
+// streamServe is what the frontend handler observed once it returned.
+type streamServe struct {
+	err error
+	req *http.Request
+}
+
+// streamPair is an established upgraded stream.
+type streamPair struct {
+	cli    net.Conn
+	br     *bufio.Reader // of cli
+	srv    net.Conn      // the backend side
+	served chan streamServe
+}
+
+// openStream sends an upgrade request for the protocol upgrade through a
+// frontend that serves it with h, and returns the established stream.
+// wrap, if set, wraps the response writer given to h.
+func openStream(t *testing.T, h *Handler, b *streamBackend, upgrade string, wrap func(http.ResponseWriter) http.ResponseWriter) *streamPair {
+	t.Helper()
+
+	p := &streamPair{served: make(chan streamServe, 1)}
+	frontend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
+		r = prepareTestRequest(r)
+		if wrap != nil {
+			w = wrap(w)
+		}
+		err := h.ServeHTTP(w, r, caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error {
+			return nil
+		}))
+		p.served <- streamServe{err: err, req: r}
+	}))
+	t.Cleanup(frontend.Close)
+
+	cli, err := net.Dial("tcp", frontend.Listener.Addr().String())
+	if err != nil {
+		t.Fatalf("failed to dial frontend: %v", err)
+	}
+	t.Cleanup(func() { cli.Close() })
+	p.cli = cli
+
+	req, _ := http.NewRequest(http.MethodGet, frontend.URL, nil)
+	req.Header.Set("Connection", "Upgrade")
+	req.Header.Set("Upgrade", upgrade)
+	if err := req.Write(cli); err != nil {
+		t.Fatalf("failed to write upgrade request: %v", err)
+	}
+	p.br = bufio.NewReader(cli)
+	resp, err := http.ReadResponse(p.br, req)
+	if err != nil {
+		t.Fatalf("failed to read upgrade response: %v", err)
+	}
+	if resp.StatusCode != http.StatusSwitchingProtocols {
+		t.Fatalf("status = %d, want 101", resp.StatusCode)
+	}
+
+	select {
+	case p.srv = <-b.conns:
+		t.Cleanup(func() { p.srv.Close() })
+	case <-time.After(streamTestTimeout):
+		t.Fatal("timed out waiting for the backend connection")
+	}
+	return p
... (truncated; 50557 more characters; read the files)
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
`--run <your test names>` where supported: the rest of the package is not what you are checking);
a probe that passes verifies the criterion, a probe that fails is a blocking finding of pass 1 with
that probe as `proof`. The harness runs every probe again: a criterion whose probe is missing or does
not run is recorded as unprobed, visible in the pull request, and you will be asked for it once.
`probe.path` is a new file in a directory of the task's test files, named like a test file of the
repository; `probe.content` is the whole file.

`verdict` is `request_changes` if and only if there is at least one `blocking` finding or a
criterion that is not `covered`.
