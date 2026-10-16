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

One releasable change: add opt-in `stream_detached` plus a `stream_logs` block (skip_handshake, level) and stream metrics to reverse_proxy. Make the response-writer chain (recorder, encode, intercept) treat a 101 as final and write nothing after hijack. Detached streams are tracked in a package-level registry keyed by upstream and, on handler Cleanup, only those whose upstream is absent from the live config are closed. Docs are the Caddyfile doc comment and JSON field comments, since the repo has no separate docs tree.

## Task T1 (T1)

Order: (1) in caddyhttp, make responseRecorder treat 101 as final/unbuffered and no-op writes after Hijack; add the Server accessor for access loggers; (2) same hijack/101 hardening in encode and intercept; (3) add reverseproxy streams registry (package-level, keyed by upstream dial, live-upstream refcount maintained in Provision/Cleanup), StreamDetached, StreamLogs{SkipHandshake, Level} fields, Caddyfile parsing, and cleanupConnections branching (detached: close only streams whose upstream is no longer live; default path unchanged); (4) rewrite handleUpgradeResponse to account each stream once via a single finalizer (metrics, close log to access loggers) and set LogSkipVar when skip_handshake; (5) add metrics; (6) tests and caddyfile_adapt fixture, then docs in comments.

Acceptance criteria:
- ACC1 [behavior]: With stream_detached set, an upgraded stream stays open after a reload whose config still contains its upstream, and is closed after a reload that removes that upstream.
- ACC2 [behavior]: Caddyfile `stream_detached` and JSON `stream_detached: true` both set Handler.StreamDetached; it defaults to false.
- ACC3 [behavior]: Without stream_detached, an upgraded stream is closed on reload immediately, or after stream_close_delay when configured.
- ACC4 [behavior]: stream_logs { skip_handshake } parses in Caddyfile and JSON; when set the handshake request is not access-logged, by default it is.
- ACC5 [behavior]: A stream close log entry is written to the access logger (selectable with logger_name access) at the configured level, DEBUG by default, including after the config that created it was reloaded in detached mode.
- ACC6 [behavior]: ResponseRecorder, encode and intercept treat a 101 as the final response, never buffer it, and perform no header or body writes after Hijack; the proxy copies on the raw connection.
- ACC7 [behavior]: WebSocket/Upgrade streams work end to end through encode and through intercept with handle_response, with access logging enabled.
- ACC8 [behavior]: An HTTP/2 extended CONNECT websocket through reverse_proxy relays data, and an HTTP/1.1 upgrade works with stream_detached.
- ACC9 [behavior]: Metrics for active streams, closed-by-result, duration, and bytes to/from the backend are updated exactly once per stream, and the active gauge returns to zero.
- ACC10 [other]: Caddyfile syntax comment and JSON field comments document stream_detached, stream_logs, skip_handshake and the log level.
- ACC11 [behavior]: Integration tests exist for reload survive/close in each mode, extended CONNECT over h2, upgrades through encode and intercept, and detached streams with access logging.
- ACC12 [behavior]: Unit tests cover each changed unit (recorder, encode, intercept, registry/cleanup, metrics, caddyfile parsing).

Files declared for the task:
- modules/caddyhttp/responsewriter.go (production)
- modules/caddyhttp/server.go (production)
- modules/caddyhttp/encode/encode.go (production)
- modules/caddyhttp/intercept/intercept.go (production)
- modules/caddyhttp/reverseproxy/reverseproxy.go (production)
- modules/caddyhttp/reverseproxy/caddyfile.go (production)
- modules/caddyhttp/reverseproxy/streaming.go (production)
- modules/caddyhttp/reverseproxy/metrics.go (production)
- modules/caddyhttp/reverseproxy/streams.go (production)
- modules/caddyhttp/responsewriter_test.go (test)
- modules/caddyhttp/encode/encode_test.go (test)
- modules/caddyhttp/intercept/intercept_test.go (test)
- modules/caddyhttp/reverseproxy/streaming_test.go (test)
- modules/caddyhttp/reverseproxy/streams_test.go (test)
- modules/caddyhttp/reverseproxy/metrics_test.go (test)
- modules/caddyhttp/reverseproxy/stream_options_test.go (test)
- caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest (test)
- caddytest/integration/stream_reload_test.go (test)
- caddytest/integration/stream_upgrade_test.go (test)

Test files of this task that the change modified (existing tests, declared by the plan):
- modules/caddyhttp/encode/encode_test.go
- modules/caddyhttp/responsewriter_test.go
- modules/caddyhttp/reverseproxy/streaming_test.go

Test results: selected tests green; red proven; suite green

## The diff

```diff
diff --git a/caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest b/caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest
new file mode 100644
index 00000000..29d54baf
--- /dev/null
+++ b/caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest
@@ -0,0 +1,64 @@
+https://example.com {
+	reverse_proxy https://localhost:54321 {
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
+													"transport": {
+														"protocol": "http",
+														"tls": {}
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
index 00000000..b5261797
--- /dev/null
+++ b/caddytest/integration/stream_reload_test.go
@@ -0,0 +1,227 @@
+package integration
+
+import (
+	"bufio"
+	"errors"
+	"fmt"
+	"io"
+	"net"
+	"net/http"
+	"sync"
+	"testing"
+	"time"
+
+	"github.com/caddyserver/caddy/v2/caddytest"
+)
+
+// streamBackend is an HTTP server that switches protocols on any request
+// that carries an Upgrade header, and echoes the stream afterwards. Other
+// requests get a plain response.
+type streamBackend struct {
+	addr  string
+	mu    sync.Mutex
+	conns []net.Conn
+}
+
+func startStreamBackend(t *testing.T) *streamBackend {
+	t.Helper()
+	ln, err := net.Listen("tcp", "127.0.0.1:0")
+	if err != nil {
+		t.Fatalf("starting the backend: %v", err)
+	}
+	b := &streamBackend{addr: ln.Addr().String()}
+	srv := &http.Server{Handler: http.HandlerFunc(b.serve)}
+	go srv.Serve(ln)
+	t.Cleanup(func() {
+		srv.Close()
+		b.mu.Lock()
+		defer b.mu.Unlock()
+		for _, c := range b.conns {
+			c.Close()
+		}
+	})
+	return b
+}
+
+func (b *streamBackend) serve(w http.ResponseWriter, r *http.Request) {
+	upgrade := r.Header.Get("Upgrade")
+	if upgrade == "" {
+		fmt.Fprint(w, "backend")
+		return
+	}
+	conn, brw, err := http.NewResponseController(w).Hijack()
+	if err != nil {
+		return
+	}
+	b.mu.Lock()
+	b.conns = append(b.conns, conn)
+	b.mu.Unlock()
+	defer conn.Close()
+
+	fmt.Fprintf(brw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", upgrade)
+	if brw.Flush() != nil {
+		return
+	}
+	io.Copy(conn, brw.Reader)
+}
+
+// streamClient is the client side of an upgraded HTTP/1.1 connection.
+type streamClient struct {
+	conn net.Conn
+	br   *bufio.Reader
+	resp *http.Response
+}
+
+// dialStream performs an upgrade handshake against the server listening on
+// port 9080 and fails the test if the response is not a 101.
+func dialStream(t *testing.T, path, upgrade string, header http.Header) *streamClient {
+	t.Helper()
+	conn, err := net.DialTimeout("tcp", "127.0.0.1:9080", 5*time.Second)
+	if err != nil {
+		t.Fatalf("dialing the server: %v", err)
+	}
+	t.Cleanup(func() { conn.Close() })
+
+	req, err := http.NewRequest(http.MethodGet, "http://localhost:9080"+path, nil)
+	if err != nil {
+		t.Fatal(err)
+	}
+	req.Header.Set("Connection", "Upgrade")
+	req.Header.Set("Upgrade", upgrade)
+	for k, v := range header {
+		req.Header[k] = v
+	}
+	if err := req.Write(conn); err != nil {
+		t.Fatalf("writing the upgrade request: %v", err)
+	}
+
+	br := bufio.NewReader(conn)
+	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
+	resp, err := http.ReadResponse(br, req)
+	if err != nil {
+		t.Fatalf("reading the upgrade response: %v", err)
+	}
+	conn.SetReadDeadline(time.Time{})
+	if resp.StatusCode != http.StatusSwitchingProtocols {
+		t.Fatalf("upgrade status = %d, want 101", resp.StatusCode)
+	}
+	return &streamClient{conn: conn, br: br, resp: resp}
+}
+
+// echo sends msg through the stream and expects it back.
+func (c *streamClient) echo(msg string) error {
+	c.conn.SetDeadline(time.Now().Add(5 * time.Second))
+	defer c.conn.SetDeadline(time.Time{})
+	if _, err := io.WriteString(c.conn, msg); err != nil {
+		return err
+	}
+	buf := make([]byte, len(msg))
+	if _, err := io.ReadFull(c.br, buf); err != nil {
+		return err
+	}
+	if string(buf) != msg {
+		return fmt.Errorf("echoed %q, want %q", buf, msg)
+	}
+	return nil
+}
+
+// waitClosed reports whether the server closed the stream within d.
+func (c *streamClient) waitClosed(d time.Duration) bool {
+	c.conn.SetReadDeadline(time.Now().Add(d))
+	defer c.conn.SetReadDeadline(time.Time{})
+	_, err := io.Copy(io.Discard, c.br)
+	var netErr net.Error
+	return !(errors.As(err, &netErr) && netErr.Timeout())
+}
+
+// streamSite returns the config of a server that proxies to upstream, with
+// the given reverse_proxy options and extra site directives. A reload is
+// done by loading the config again with different extra directives.
+func streamSite(upstream, proxyOptions, extra string) string {
+	return fmt.Sprintf(`{
+		skip_install_trust
+		admin localhost:2999
+		http_port     9080
+		https_port    9443
+		grace_period  1ns
+	}
+
+	localhost:9080 {
+		%s
+		reverse_proxy %s {
+			%s
+		}
+	}`, extra, upstream, proxyOptions)
+}
+
+// By default, an upgraded stream is closed when the config is reloaded.
+func TestStreamIsClosedOnReload(t *testing.T) {
+	tester := caddytest.NewTester(t)
+	backend := startStreamBackend(t)
+
+	tester.InitServer(streamSite(backend.addr, "", `respond /version "v1"`), "caddyfile")
+	c := dialStream(t, "/ws", "echo", nil)
+	if err := c.echo("before"); err != nil {
+		t.Fatalf("echo before the reload: %v", err)
+	}
+
+	tester.InitServer(streamSite(backend.addr, "", `respond /version "v2"`), "caddyfile")
+	if !c.waitClosed(5 * time.Second) {
+		t.Fatal("the stream is still open after the reload")
+	}
+	tester.AssertGetResponse("http://localhost:9080/version", 200, "v2")
+}
+
+// With stream_close_delay, the stream outlives the reload until the delay is over.
+func TestStreamIsClosedAfterCloseDelay(t *testing.T) {
+	tester := caddytest.NewTester(t)
+	backend := startStreamBackend(t)
+
+	options := "stream_close_delay 1500ms"
+	tester.InitServer(streamSite(backend.addr, options, `respond /version "v1"`), "caddyfile")
+	c := dialStream(t, "/ws", "echo", nil)
+	if err := c.echo("before"); err != nil {
+		t.Fatalf("echo before the reload: %v", err)
+	}
+
+	reloaded := time.Now()
+	tester.InitServer(streamSite(backend.addr, options, `respond /version "v2"`), "caddyfile")
+	if err := c.echo("during the delay"); err != nil {
+		t.Fatalf("the stream should stay open during the close delay: %v", err)
+	}
+	if !c.waitClosed(5 * time.Second) {
+		t.Fatal("the stream is still open after the close delay")
+	}
+	if elapsed := time.Since(reloaded); elapsed < time.Second {
+		t.Errorf("the stream was closed %v after the reload, want it to wait for the delay", elapsed)
+	}
+}
+
+// A detached stream survives reloads that keep its upstream, and is closed
+// by the reload that removes it.
+func TestDetachedStreamFollowsItsUpstream(t *testing.T) {
+	tester := caddytest.NewTester(t)
+	backend := startStreamBackend(t)
+	otherBackend := startStreamBackend(t)
+	const options = "stream_detached"
+
+	tester.InitServer(streamSite(backend.addr, options, `respond /version "v1"`), "caddyfile")
+	c := dialStream(t, "/ws", "echo", nil)
+	if err := c.echo("before"); err != nil {
+		t.Fatalf("echo before the reload: %v", err)
+	}
+
+	tester.InitServer(streamSite(backend.addr, options, `respond /version "v2"`), "caddyfile")
+	tester.AssertGetResponse("http://localhost:9080/version", 200, "v2")
+	if c.waitClosed(time.Second) {
+		t.Fatal("the detached stream was closed by a reload that keeps its upstream")
+	}
+	if err := c.echo("after"); err != nil {
+		t.Fatalf("echo after the reload: %v", err)
+	}
+
+	tester.InitServer(streamSite(otherBackend.addr, options, `respond /version "v3"`), "caddyfile")
+	if !c.waitClosed(5 * time.Second) {
+		t.Fatal("the detached stream is still open after its upstream was removed")
+	}
+}
diff --git a/caddytest/integration/stream_upgrade_test.go b/caddytest/integration/stream_upgrade_test.go
new file mode 100644
index 00000000..024f6f42
--- /dev/null
+++ b/caddytest/integration/stream_upgrade_test.go
@@ -0,0 +1,389 @@
+package integration
+
+import (
+	"bufio"
+	"bytes"
+	"crypto/tls"
+	"encoding/json"
+	"fmt"
+	"io"
+	"net"
+	"net/http"
+	"os"
+	"os/exec"
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
+// logSink collects the JSON log entries that a server sends to it with the
+// net log writer.
+type logSink struct {
+	addr    string
+	mu      sync.Mutex
+	entries []map[string]any
+}
+
+func startLogSink(t *testing.T) *logSink {
+	t.Helper()
+	ln, err := net.Listen("tcp", "127.0.0.1:0")
+	if err != nil {
+		t.Fatalf("starting the log sink: %v", err)
+	}
+	s := &logSink{addr: ln.Addr().String()}
+	t.Cleanup(func() { ln.Close() })
+	go func() {
+		for {
+			conn, err := ln.Accept()
+			if err != nil {
+				return
+			}
+			go s.read(conn)
+		}
+	}()
+	return s
+}
+
+func (s *logSink) read(conn net.Conn) {
+	defer conn.Close()
+	sc := bufio.NewScanner(conn)
+	sc.Buffer(nil, 1<<20)
+	for sc.Scan() {
+		var entry map[string]any
+		if json.Unmarshal(sc.Bytes(), &entry) != nil {
+			continue
+		}
+		s.mu.Lock()
+		s.entries = append(s.entries, entry)
+		s.mu.Unlock()
+	}
+}
+
+// find returns the first entry that match.
+func (s *logSink) find(match func(map[string]any) bool) map[string]any {
+	s.mu.Lock()
+	defer s.mu.Unlock()
+	for _, e := range s.entries {
+		if match(e) {
+			return e
+		}
+	}
+	return nil
+}
+
+func (s *logSink) waitFor(t *testing.T, what string, match func(map[string]any) bool) map[string]any {
+	t.Helper()
+	deadline := time.Now().Add(5 * time.Second)
+	for time.Now().Before(deadline) {
+		if e := s.find(match); e != nil {
+			return e
+		}
+		time.Sleep(20 * time.Millisecond)
+	}
+	s.mu.Lock()
+	defer s.mu.Unlock()
+	t.Fatalf("timed out waiting for %s; got entries: %v", what, s.entries)
+	return nil
+}
+
+func isStreamClosed(e map[string]any) bool { return e["msg"] == "stream closed" }
+
+func isHandledRequest(uri string) func(map[string]any) bool {
+	return func(e map[string]any) bool {
+		req, _ := e["request"].(map[string]any)
+		return e["msg"] == "handled request" && req != nil && req["uri"] == uri
+	}
+}
+
+// accessLogSite is a server that proxies to upstream and writes its access
+// logs, under the logger name "access", to the sink.
+func accessLogSite(upstream, sink, proxyOptions, extra string) string {
+	return fmt.Sprintf(`{
+		skip_install_trust
+		admin localhost:2999
+		http_port     9080
+		https_port    9443
+		grace_period  1ns
+	}
+
+	localhost:9080 {
+		log access {
+			output net %s
+			format json
+			level DEBUG
+		}
+		%s
+		reverse_proxy %s {
+			%s
+		}
+	}`, sink, extra, upstream, proxyOptions)
+}
+
+// A detached stream still writes its close entry to the access log after
+// the config that created it was reloaded; the handshake is logged by default.
+func TestDetachedStreamCloseLogAfterReload(t *testing.T) {
+	tester := caddytest.NewTester(t)
+	backend := startStreamBackend(t)
+	sink := startLogSink(t)
+	const options = "stream_detached"
+
+	tester.InitServer(accessLogSite(backend.addr, sink.addr, options, `respond /version "v1"`), "caddyfile")
+	c := dialStream(t, "/ws", "echo", nil)
+	if err := c.echo("hello"); err != nil {
+		t.Fatalf("echo before the reload: %v", err)
+	}
+
+	tester.InitServer(accessLogSite(backend.addr, sink.addr, options, `respond /version "v2"`), "caddyfile")
+	tester.AssertGetResponse("http://localhost:9080/version", 200, "v2")
+	if err := c.echo("world"); err != nil {
+		t.Fatalf("echo after the reload: %v", err)
+	}
+	c.conn.Close()
+
+	closed := sink.waitFor(t, "the stream close entry", isStreamClosed)
+	if closed["logger"] != "http.log.access.access" {
+		t.Errorf("logger = %v, want http.log.access.access", closed["logger"])
+	}
+	if closed["level"] != "debug" {
+		t.Errorf("level = %v, want debug", closed["level"])
+	}
+	if closed["result"] != "completed" {
+		t.Errorf("result = %v, want completed", closed["result"])
+	}
+	if toBackend, _ := closed["bytes_to_backend"].(float64); toBackend != float64(len("helloworld")) {
+		t.Errorf("bytes_to_backend = %v, want %d", closed["bytes_to_backend"], len("helloworld"))
+	}
+	if fromBackend, _ := closed["bytes_from_backend"].(float64); fromBackend != float64(len("helloworld")) {
+		t.Errorf("bytes_from_backend = %v, want %d", closed["bytes_from_backend"], len("helloworld"))
+	}
+	if closed["upstream"] != backend.addr {
+		t.Errorf("upstream = %v, want %s", closed["upstream"], backend.addr)
+	}
+
+	// the handshake is logged unless told otherwise
+	sink.waitFor(t, "the handshake request entry", isHandledRequest("/ws"))
+}
+
+// With skip_handshake, the handshake is not access-logged, and the close
+// entry is written at the configured level.
+func TestStreamLogsSkipHandshake(t *testing.T) {
+	tester := caddytest.NewTester(t)
+	backend := startStreamBackend(t)
+	sink := startLogSink(t)
+
+	options := `stream_logs {
+		skip_handshake
+		level warn
+	}`
+	tester.InitServer(accessLogSite(backend.addr, sink.addr, options, ""), "caddyfile")
+	c := dialStream(t, "/ws", "echo", nil)
+	if err := c.echo("hello"); err != nil {
+		t.Fatalf("echo: %v", err)
+	}
+	c.conn.Close()
+
+	closed := sink.waitFor(t, "the stream close entry", isStreamClosed)
+	if closed["level"] != "warn" {
+		t.Errorf("level = %v, want warn", closed["level"])
+	}
+
+	// an ordinary request is logged after the handshake would have been
+	tester.AssertGetResponse("http://localhost:9080/plain", 200, "backend")
+	sink.waitFor(t, "the ordinary request entry", isHandledRequest("/plain"))
+	if e := sink.find(isHandledRequest("/ws")); e != nil {
+		t.Errorf("the handshake was access-logged: %v", e)
+	}
+}
+
+// Upgrades work through the encode handler of the same route, which must
+// leave the 101 alone, with access logging enabled.
+func TestUpgradeThroughEncode(t *testing.T) {
+	tester := caddytest.NewTester(t)
+	backend := startStreamBackend(t)
+	sink := startLogSink(t)
+
+	tester.InitServer(accessLogSite(backend.addr, sink.addr, "stream_detached", "encode gzip zstd"), "caddyfile")
+	c := dialStream(t, "/ws", "echo", http.Header{"Accept-Encoding": {"gzip, zstd"}})
+	if ce := c.resp.Header.Get("Content-Encoding"); ce != "" {
+		t.Errorf("Content-Encoding of the 101 = %q, want none", ce)
+	}
+	if err := c.echo("through encode"); err != nil {
+		t.Fatalf("echo: %v", err)
+	}
+	c.conn.Close()
+	sink.waitFor(t, "the stream close entry", isStreamClosed)
+}
+
+// Upgrades work through an intercept handler whose handle_response
+// matches every response, while ordinary responses are still intercepted.
+func TestUpgradeThroughIntercept(t *testing.T) {
+	tester := caddytest.NewTester(t)
+	backend := startStreamBackend(t)
+	sink := startLogSink(t)
+
+	// the replacement is as long as the backend's response ("backend"),
+	// whose Content-Length is kept by the intercepted response
+	extra := `intercept {
+		handle_response {
+			respond "swapped" 200
+		}
+	}`
+	tester.InitServer(accessLogSite(backend.addr, sink.addr, "", extra), "caddyfile")
+	c := dialStream(t, "/ws", "echo", nil)
+	if err := c.echo("through intercept"); err != nil {
+		t.Fatalf("echo: %v", err)
+	}
+	c.conn.Close()
+	sink.waitFor(t, "the stream close entry", isStreamClosed)
+
+	tester.AssertGetResponse("http://localhost:9080/plain", 200, "swapped")
+}
+
+// A websocket over HTTP/2 (RFC 8441 extended CONNECT) is relayed to a
+// backend that speaks the HTTP/1.1 upgrade.
+//
+// The server only advertises extended CONNECT support when the process runs
+// with GODEBUG=http2xconnect=1, which is read when the process starts. So
+// the test runs itself in a new process with it, with ports and an admin
+// endpoint of its own so that it doesn't meet the server of this process.
+func TestExtendedConnectWebsocketOverH2(t *testing.T) {
+	if testing.Short() {
+		t.Skip("integration test")
+	}
+	if !strings.Contains(os.Getenv("GODEBUG"), "http2xconnect=1") {
+		// os.Args is rewritten when the tests start the server in-process
+		exe, err := os.Executable()
+		if err != nil {
+			t.Fatal(err)
+		}
+		cmd := exec.Command(exe,"-test.run=^TestExtendedConnectWebsocketOverH2$", "-test.count=1", "-test.timeout=2m")
+		cmd.Env = append(os.Environ(), "GODEBUG=http2xconnect=1")
+		if out, err := cmd.CombinedOutput(); err != nil {
+			t.Fatalf("test process failed: %v\n%s", err, out)
+		}
+		return
+	}
+
+	tester := caddytest.NewTester(t).WithDefaultOverrides(caddytest.Config{AdminPort: 2998})
+	backend := startStreamBackend(t)
+
+	config := fmt.Sprintf(`{
+		skip_install_trust
+		admin localhost:2998
+		http_port     9580
+		https_port    9543
+		grace_period  1ns
+	}
+
+	localhost:9543 {
+		reverse_proxy %s
+	}`, backend.addr)
+	tester.InitServer(config, "caddyfile")
+
+	// The HTTP/2 clients of the standard library don't send extended CONNECT
+	// requests, so speak the protocol directly.
+	conn, err := tls.Dial("tcp", "127.0.0.1:9543",&tls.Config{
+		InsecureSkipVerify: true, //nolint:gosec
+		ServerName:         "localhost",
+		NextProtos:         []string{"h2"},
+	})
+	if err != nil {
+		t.Fatalf("dialing the server: %v", err)
+	}
+	defer conn.Close()
+	conn.SetDeadline(time.Now().Add(10 * time.Second))
+
+	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
+		t.Fatal(err)
+	}
+	fr := http2.NewFramer(conn, conn)
+	if err := fr.WriteSettings(); err != nil {
+		t.Fatal(err)
+	}
+
+	var (
+		connectAllowed bool
+		status         string
+		data           []byte
+	)
+	decoder := hpack.NewDecoder(4096, func(f hpack.HeaderField) {
+		if f.Name == ":status" {
+			status = f.Value
+		}
+	})
+	// readFrames handles the frames of the connection until done reports true
+	readFrames := func(done func() bool) {
+		t.Helper()
+		for !done() {
+			frame, err := fr.ReadFrame()
+			if err != nil {
+				t.Fatalf("reading a frame: %v", err)
+			}
+			switch f := frame.(type) {
+			case *http2.SettingsFrame:
+				if f.IsAck() {
+					continue
+				}
+				if v, ok := f.Value(http2.SettingID(0x8)); ok && v == 1 { // SETTINGS_ENABLE_CONNECT_PROTOCOL
+					connectAllowed = true
+				}
+				if err := fr.WriteSettingsAck(); err != nil {
+					t.Fatal(err)
+				}
+			case *http2.HeadersFrame:
+				if _, err := decoder.Write(f.HeaderBlockFragment()); err != nil {
+					t.Fatal(err)
+				}
+			case *http2.DataFrame:
+				data = append(data, f.Data()...)
+			case *http2.RSTStreamFrame:
+				t.Fatalf("the stream was reset: %v", f.ErrCode)
+			case *http2.GoAwayFrame:
+				t.Fatalf("the connection was closed: %v", f.ErrCode)
+			}
+		}
+	}
+
+	readFrames(func() bool { return connectAllowed })
+
+	var block bytes.Buffer
+	encoder := hpack.NewEncoder(&block)
+	for _, f := range [][2]string{
+		{":method", "CONNECT"},
+		{":protocol", "websocket"},
+		{":scheme", "https"},
+		{":path", "/ws"},
+		{":authority", "localhost:9543"},
+		{"sec-websocket-version", "13"},
+	} {
+		if err := encoder.WriteField(hpack.HeaderField{Name: f[0], Value: f[1]}); err != nil {
+			t.Fatal(err)
+		}
+	}
+	err = fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block.Bytes(), EndHeaders: true})
+	if err != nil {
+		t.Fatal(err)
+	}
+
+	readFrames(func() bool { return status != "" })
+	if status != "200" {
+		t.Fatalf("status = %s, want 200", status)
+	}
+
+	for _, msg := range []string{"hello over h2", "and again"} {
+		data = nil
+		if err := fr.WriteData(1, false, []byte(msg)); err != nil {
+			t.Fatalf("writing to the stream: %v", err)
+		}
+		readFrames(func() bool { return len(data) >= len(msg) })
+		if string(data) != msg {
+			t.Errorf("echoed %q, want %q", data, msg)
+		}
+	}
+}
diff --git a/modules/caddyhttp/encode/encode.go b/modules/caddyhttp/encode/encode.go
index 46b383ea..ed83b0a9 100644
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
@@ -255,11 +257,38 @@ type responseWriter struct {
 	incremental  bool
 	isConnect    bool
 	disabled     bool // disable encoding for this response
+	hijacked     bool // the connection was taken over; nothing may be written anymore
+}
+
+// Hijack hijacks the underlying connection. Once it succeeded, no header
+// or body is written by rw anymore.
+func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	//nolint:bodyclose
+	conn, brw, err := http.NewResponseController(rw.ResponseWriter).Hijack()
+	if err != nil {
+		return nil, nil, err
+	}
+	rw.hijacked = true
+	return conn, brw, nil
 }
 
 // WriteHeader stores the status to write when the time comes
 // to actually write the header.
 func (rw *responseWriter) WriteHeader(status int) {
+	if rw.hijacked {
+		return
+	}
+
+	// 101 Switching Protocols is the final response of an upgrade: the
+	// body is never encoded and the connection is taken over right after
+	if status == http.StatusSwitchingProtocols {
+		rw.statusCode = status
+		rw.disabled = true
+		rw.wroteHeader = true
+		rw.ResponseWriter.WriteHeader(status)
+		return
+	}
+
 	rw.statusCode = status
 	if status == http.StatusPartialContent {
 		rw.disabled = true // partial representations must not be dynamically re-encoded
@@ -332,6 +361,9 @@ func (enc *Encode) Match(rw *responseWriter) bool {
 // FlushError is an alternative Flush returning an error. It delays the actual Flush of the underlying
 // ResponseWriterWrapper until headers were written.
 func (rw *responseWriter) FlushError() error {
+	if rw.hijacked {
+		return nil
+	}
 	if !rw.wroteHeader && rw.statusCode == 0 && caddyhttp.IsIncremental(rw.Header()) {
 		rw.WriteHeader(http.StatusOK)
 	}
@@ -365,6 +397,10 @@ func (rw *responseWriter) FlushError() error {
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
@@ -438,6 +474,9 @@ const sniffLen = 512
 // It's based on the standard library HTTP/1.1 response writer implementation.
 // https://github.com/golang/go/blob/f4e3ec3dbe3b8e04a058d266adf8e048bab563f2/src/net/http/server.go#L586
 func (rw *responseWriter) ReadFrom(r io.Reader) (int64, error) {
+	if rw.hijacked {
+		return 0, http.ErrHijacked
+	}
 	if rw.incremental || !rw.wroteHeader && caddyhttp.IsIncremental(rw.Header()) {
 		return io.Copy(writerOnly{rw}, r)
 	}
@@ -476,7 +515,7 @@ func (rw *responseWriter) ReadFrom(r io.Reader) (int64, error) {
 // deallocates any active resources.
 func (rw *responseWriter) Close() error {
 	// didn't write, probably head request
-	if !rw.wroteHeader {
+	if !rw.wroteHeader && !rw.hijacked {
 		cl, err := strconv.Atoi(rw.Header().Get("Content-Length"))
 		if err == nil && cl > rw.config.MinLength {
 			rw.init()
diff --git a/modules/caddyhttp/encode/encode_test.go b/modules/caddyhttp/encode/encode_test.go
index 8e0037df..23f2ad48 100644
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
 
@@ -395,6 +399,144 @@ func (mockEncoder) Close() error                      { return nil }
 func (mockEncoder) Reset(w io.Writer)                 {}
 func (mockEncoder) Flush() error                      { return nil }
 
+// hijackSpy is a http.ResponseWriter that records what is written to it
+// and can be hijacked.
+type hijackSpy struct {
+	header   http.Header
+	statuses []int
+	body     []byte
+	peer     net.Conn
+}
+
+func (s *hijackSpy) Header() http.Header {
+	if s.header == nil {
+		s.header = make(http.Header)
+	}
+	return s.header
+}
+func (s *hijackSpy) WriteHeader(statusCode int) { s.statuses = append(s.statuses, statusCode) }
+func (s *hijackSpy) Write(d []byte) (int, error) {
+	s.body = append(s.body, d...)
+	return len(d), nil
+}
+
+func (s *hijackSpy) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	conn, peer := net.Pipe()
+	s.peer = peer
+	return conn, bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)), nil
+}
+
+func newTestEncode(t *testing.T) *Encode {
+	t.Helper()
+	enc := new(Encode)
+	enc.MinLength = 1 // compress everything
+	enc.writerPools = map[string]*sync.Pool{
+		"gzip": {New: func() any { return mockEncoder{} }},
+	}
+	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
+	t.Cleanup(cancel)
+	if err := enc.Provision(ctx); err != nil {
+		t.Fatalf("Provision failed: %v", err)
+	}
+	return enc
+}
+
+// An upgrade must go through encode untouched: the 101 is final, and once
+// the connection is hijacked nothing may be written to the response again.
+func TestServeHTTPUpgradeNeverWritesAfterHijack(t *testing.T) {
+	enc := newTestEncode(t)
+	r := httptest.NewRequest("GET", "/", nil)
+	r.Header.Set("Accept-Encoding", "gzip")
+	r.Header.Set("Connection", "Upgrade")
+	r.Header.Set("Upgrade", "foo")
+
+	spy := &hijackSpy{}
+	var writeErr, readFromErr error
+	var flushErr error
+	err := enc.ServeHTTP(spy, r, caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
+		w.Header().Set("Content-Type", "text/plain")
+		w.WriteHeader(http.StatusSwitchingProtocols)
+		conn, _, err := http.NewResponseController(w).Hijack()
+		if err != nil {
+			return err
+		}
+		defer conn.Close()
+		defer spy.peer.Close()
+
+		w.WriteHeader(http.StatusInternalServerError)
+		_, writeErr = w.Write([]byte("late body, long enough to be encoded"))
+		_, readFromErr = io.Copy(w, struct{ io.Reader }{strings.NewReader("late")})
+		flushErr = http.NewResponseController(w).Flush()
+		return nil
+	}))
+	if err != nil {
+		t.Fatalf("ServeHTTP error = %v", err)
+	}
+
+	if !errors.Is(writeErr, http.ErrHijacked) {
+		t.Errorf("Write() after hijack error = %v, want ErrHijacked", writeErr)
+	}
+	if !errors.Is(readFromErr, http.ErrHijacked) {
+		t.Errorf("ReadFrom() after hijack error = %v, want ErrHijacked", readFromErr)
+	}
+	if flushErr != nil {
+		t.Errorf("Flush() after hijack error = %v", flushErr)
+	}
+	if len(spy.statuses) != 1 || spy.statuses[0] != http.StatusSwitchingProtocols {
+		t.Errorf("underlying statuses = %v, want [101]", spy.statuses)
+	}
+	if len(spy.body) != 0 {
+		t.Errorf("underlying body = %q, want nothing", spy.body)
+	}
+	if ce := spy.Header().Get("Content-Encoding"); ce != "" {
+		t.Errorf("Content-Encoding = %q, want none for an upgrade", ce)
+	}
+}
+
+// Closing the encoder after a hijack must not write the pending status.
+func TestServeHTTPHijackWithoutStatusWritesNothing(t *testing.T) {
+	enc := newTestEncode(t)
+	r := httptest.NewRequest("GET", "/", nil)
+	r.Header.Set("Accept-Encoding", "gzip")
+
+	spy := &hijackSpy{}
+	err := enc.ServeHTTP(spy, r, caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
+		conn, _, err := http.NewResponseController(w).Hijack()
+		if err != nil {
+			return err
+		}
+		defer conn.Close()
+		defer spy.peer.Close()
+		w.WriteHeader(http.StatusOK)
+		return nil
+	}))
+	if err != nil {
+		t.Fatalf("ServeHTTP error = %v", err)
+	}
+	if len(spy.statuses) != 0 {
+		t.Errorf("underlying statuses = %v, want none after hijack", spy.statuses)
+	}
+}
+
+func TestServeHTTPHijackNotSupported(t *testing.T) {
+	enc := newTestEncode(t)
+	r := httptest.NewRequest("GET", "/", nil)
+	r.Header.Set("Accept-Encoding", "gzip")
+
+	var hijackErr error
+	err := enc.ServeHTTP(httptest.NewRecorder(), r, caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
+		_, _, hijackErr = http.NewResponseController(w).Hijack()
+		_, err := w.Write([]byte("still writable after a failed hijack"))
+		return err
+	}))
+	if err != nil {
+		t.Fatalf("ServeHTTP error = %v", err)
+	}
+	if !errors.Is(hijackErr, http.ErrNotSupported) {
+		t.Errorf("Hijack() error = %v, want ErrNotSupported", hijackErr)
+	}
+}
+
 func TestServeHTTPDefaultEncodingPreference(t *testing.T) {
 	enc := new(Encode)
 	enc.MinLength = 1 // compress everything
diff --git a/modules/caddyhttp/intercept/intercept.go b/modules/caddyhttp/intercept/intercept.go
index fc37be05..ee60246e 100644
--- a/modules/caddyhttp/intercept/intercept.go
+++ b/modules/caddyhttp/intercept/intercept.go
@@ -15,9 +15,11 @@
 package intercept
 
 import (
+	"bufio"
 	"bytes"
 	"fmt"
 	"io"
+	"net"
 	"net/http"
 	"strconv"
 	"strings"
@@ -104,13 +106,28 @@ type interceptedResponseHandler struct {
 	handler      caddyhttp.ResponseHandler
 	handlerIndex int
 	statusCode   int
+	hijacked     bool
 }
 
 // EXPERIMENTAL: Subject to change or removal.
-func (irh interceptedResponseHandler) Unwrap() http.ResponseWriter {
+func (irh *interceptedResponseHandler) Unwrap() http.ResponseWriter {
 	return irh.ResponseRecorder
 }
 
+// Hijack hijacks the underlying connection. Once it succeeded the
+// response is over and nothing may be written to it anymore.
+//
+// EXPERIMENTAL: Subject to change or removal.
+func (irh *interceptedResponseHandler) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	//nolint:bodyclose
+	conn, brw, err := http.NewResponseController(irh.ResponseRecorder).Hijack()
+	if err != nil {
+		return nil, nil, err
+	}
+	irh.hijacked = true
+	return conn, brw, nil
+}
+
 // EXPERIMENTAL: Subject to change or removal.
 func (ir Intercept) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
 	buf := bufPool.Get().(*bytes.Buffer)
@@ -118,7 +135,7 @@ func (ir Intercept) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddy
 	defer bufPool.Put(buf)
 
 	repl := r.Context().Value(caddy.ReplacerCtxKey).(*caddy.Replacer)
-	rec := interceptedResponseHandler{replacer: repl}
+	rec := &interceptedResponseHandler{replacer: repl}
 	rec.ResponseRecorder = caddyhttp.NewResponseRecorder(w, buf, func(status int, header http.Header) bool {
 		// see if any response handler is configured for this original response
 		for i, rh := range ir.HandleResponse {
@@ -150,7 +167,9 @@ func (ir Intercept) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddy
 	if err := next.ServeHTTP(rec, r); err != nil {
 		return err
 	}
-	if !rec.Buffered() {
+	// a hijacked connection (e.g. an upgrade) or a streamed response is
+	// final, there is nothing to intercept or write anymore
+	if rec.hijacked || !rec.Buffered() {
 		return nil
 	}
 
diff --git a/modules/caddyhttp/intercept/intercept_test.go b/modules/caddyhttp/intercept/intercept_test.go
new file mode 100644
index 00000000..f26f97a0
--- /dev/null
+++ b/modules/caddyhttp/intercept/intercept_test.go
@@ -0,0 +1,124 @@
+package intercept
+
+import (
+	"bufio"
+	"context"
+	"errors"
+	"net"
+	"net/http"
+	"net/http/httptest"
+	"testing"
+
+	"go.uber.org/zap"
+
+	"github.com/caddyserver/caddy/v2"
+	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
+)
+
+// hijackSpy is a http.ResponseWriter that records what is written to it
+// and can be hijacked.
+type hijackSpy struct {
+	header   http.Header
+	statuses []int
+	body     []byte
+	peer     net.Conn
+}
+
+func (s *hijackSpy) Header() http.Header {
+	if s.header == nil {
+		s.header = make(http.Header)
+	}
+	return s.header
+}
+func (s *hijackSpy) WriteHeader(statusCode int) { s.statuses = append(s.statuses, statusCode) }
+func (s *hijackSpy) Write(d []byte) (int, error) {
+	s.body = append(s.body, d...)
+	return len(d), nil
+}
+
+func (s *hijackSpy) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	conn, peer := net.Pipe()
+	s.peer = peer
+	return conn, bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)), nil
+}
+
+func newTestIntercept() Intercept {
+	return Intercept{
+		// matches every response
+		HandleResponse: []caddyhttp.ResponseHandler{{Routes: caddyhttp.RouteList{}}},
+		logger:         zap.NewNop(),
+	}
+}
+
+func newTestRequest() *http.Request {
+	r := httptest.NewRequest("GET", "/", nil)
+	return r.WithContext(context.WithValue(r.Context(), caddy.ReplacerCtxKey, caddy.NewReplacer()))
+}
+
+// A 101 is the final response of an upgrade: it must not be buffered even
+// though a handle_response matches every response, and nothing may be
+// written once the connection is hijacked.
+func TestInterceptUpgradeIsNotBufferedNorWrittenAfterHijack(t *testing.T) {
+	spy := &hijackSpy{}
+	var writeErr error
+	err := newTestIntercept().ServeHTTP(spy, newTestRequest(), caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
+		w.WriteHeader(http.StatusSwitchingProtocols)
+		conn, _, err := http.NewResponseController(w).Hijack()
+		if err != nil {
+			return err
+		}
+		defer conn.Close()
+		defer spy.peer.Close()
+
+		_, writeErr = w.Write([]byte("late"))
+		return nil
+	}))
+	if err != nil {
+		t.Fatalf("ServeHTTP error = %v", err)
+	}
+	if !errors.Is(writeErr, http.ErrHijacked) {
+		t.Errorf("Write() after hijack error = %v, want ErrHijacked", writeErr)
+	}
+	if len(spy.statuses) != 1 || spy.statuses[0] != http.StatusSwitchingProtocols {
+		t.Errorf("underlying statuses = %v, want [101]", spy.statuses)
+	}
+	if len(spy.body) != 0 {
+		t.Errorf("underlying body = %q, want nothing", spy.body)
+	}
+}
+
+// A hijacked connection must not be handled as a buffered response, even if
+// no status was written before the hijack.
+func TestInterceptHijackWithoutStatusWritesNothing(t *testing.T) {
+	spy := &hijackSpy{}
+	err := newTestIntercept().ServeHTTP(spy, newTestRequest(), caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
+		conn, _, err := http.NewResponseController(w).Hijack()
+		if err != nil {
+			return err
+		}
+		defer conn.Close()
+		defer spy.peer.Close()
+		return nil
+	}))
+	if err != nil {
+		t.Fatalf("ServeHTTP error = %v", err)
+	}
+	if len(spy.statuses) != 0 || len(spy.body) != 0 {
+		t.Errorf("wrote statuses %v, body %q after hijack; want nothing", spy.statuses, spy.body)
+	}
+}
+
+func TestInterceptHijackNotSupported(t *testing.T) {
+	var hijackErr error
+	err := newTestIntercept().ServeHTTP(httptest.NewRecorder(), newTestRequest(), caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
+		_, _, hijackErr = http.NewResponseController(w).Hijack()
+		w.WriteHeader(http.StatusOK)
+		return nil
+	}))
+	if err != nil {
+		t.Fatalf("ServeHTTP error = %v", err)
+	}
+	if !errors.Is(hijackErr, http.ErrNotSupported) {
+		t.Errorf("Hijack() error = %v, want ErrNotSupported", hijackErr)
+	}
+}
diff --git a/modules/caddyhttp/responsewriter.go b/modules/caddyhttp/responsewriter.go
index 904c30c0..f1669458 100644
--- a/modules/caddyhttp/responsewriter.go
+++ b/modules/caddyhttp/responsewriter.go
@@ -70,6 +70,7 @@ type responseRecorder struct {
 	size         int
 	wroteHeader  bool
 	stream       bool
+	hijacked     bool
 
 	readSize *int
 }
@@ -146,7 +147,7 @@ func NewResponseRecorder(w http.ResponseWriter, buf *bytes.Buffer, shouldBuffer
 // ResponseWriter unless the response is to be buffered instead.
 // 1xx responses are never buffered.
 func (rr *responseRecorder) WriteHeader(statusCode int) {
-	if rr.wroteHeader {
+	if rr.wroteHeader || rr.hijacked {
 		return
 	}
 
@@ -154,6 +155,15 @@ func (rr *responseRecorder) WriteHeader(statusCode int) {
 	// connections by manually setting headers and writing status 101
 	rr.statusCode = statusCode
 
+	// 101 Switching Protocols is the final response of an upgrade: it is
+	// never buffered and the connection is taken over right after it
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
@@ -173,6 +183,9 @@ func (rr *responseRecorder) WriteHeader(statusCode int) {
 }
 
 func (rr *responseRecorder) Write(data []byte) (int, error) {
+	if rr.hijacked {
+		return 0, http.ErrHijacked
+	}
 	rr.WriteHeader(http.StatusOK)
 	var n int
 	var err error
@@ -187,6 +200,9 @@ func (rr *responseRecorder) Write(data []byte) (int, error) {
 }
 
 func (rr *responseRecorder) ReadFrom(r io.Reader) (int64, error) {
+	if rr.hijacked {
+		return 0, http.ErrHijacked
+	}
 	rr.WriteHeader(http.StatusOK)
 	var n int64
 	var err error
@@ -223,6 +239,9 @@ func (rr *responseRecorder) Buffered() bool {
 }
 
 func (rr *responseRecorder) WriteResponse() error {
+	if rr.hijacked {
+		return nil
+	}
 	if rr.statusCode == 0 {
 		// could happen if no handlers actually wrote anything,
 		// and this prevents a panic; status must be > 0
@@ -239,6 +258,9 @@ func (rr *responseRecorder) WriteResponse() error {
 // FlushError will suppress actual flushing if the response is buffered. See:
 // https://github.com/caddyserver/caddy/issues/6144
 func (rr *responseRecorder) FlushError() error {
+	if rr.hijacked {
+		return nil
+	}
 	if rr.stream {
 		//nolint:bodyclose
 		return http.NewResponseController(rr.ResponseWriterWrapper).Flush()
@@ -258,6 +280,7 @@ func (rr *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
 	if err != nil {
 		return nil, nil, err
 	}
+	rr.hijacked = true
 	// Per http documentation, returned bufio.Writer is empty, but bufio.Read maybe not
 	conn = &hijackedConn{conn, rr}
 	brw.Writer.Reset(conn)
diff --git a/modules/caddyhttp/responsewriter_test.go b/modules/caddyhttp/responsewriter_test.go
index c08ad26a..5897701d 100644
--- a/modules/caddyhttp/responsewriter_test.go
+++ b/modules/caddyhttp/responsewriter_test.go
@@ -1,13 +1,165 @@
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
 
+// hijackSpy is a http.ResponseWriter that records what is written to it
+// and can be hijacked.
+type hijackSpy struct {
+	header   http.Header
+	statuses []int
+	body     []byte
+	peer     net.Conn
+}
+
+func (s *hijackSpy) Header() http.Header {
+	if s.header == nil {
+		s.header = make(http.Header)
+	}
+	return s.header
+}
+func (s *hijackSpy) WriteHeader(statusCode int) { s.statuses = append(s.statuses, statusCode) }
+func (s *hijackSpy) Write(d []byte) (int, error) {
+	s.body = append(s.body, d...)
+	return len(d), nil
+}
+
+func (s *hijackSpy) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	conn, peer := net.Pipe()
+	s.peer = peer
+	return conn, bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)), nil
+}
+
+func TestResponseRecorderSwitchingProtocolsIsFinal(t *testing.T) {
+	spy := &hijackSpy{}
+	shouldBufferCalled := false
+	rr := NewResponseRecorder(spy, new(bytes.Buffer), func(int, http.Header) bool {
+		shouldBufferCalled = true
+		return true
+	})
+
+	rr.WriteHeader(http.StatusSwitchingProtocols)
+	// a 101 is the final response: a later status must not replace it
+	rr.WriteHeader(http.StatusOK)
+
+	if shouldBufferCalled {
+		t.Error("shouldBuffer was consulted for a 101")
+	}
+	if rr.Buffered() {
+		t.Error("a 101 must not be buffered")
+	}
+	if rr.Status() != http.StatusSwitchingProtocols {
+		t.Errorf("Status() = %d, want 101", rr.Status())
+	}
+	if len(spy.statuses) != 1 || spy.statuses[0] != http.StatusSwitchingProtocols {
+		t.Errorf("underlying statuses = %v, want [101]", spy.statuses)
+	}
+}
+
+func TestResponseRecorderNoWritesAfterHijack(t *testing.T) {
+	spy := &hijackSpy{}
+	rr := NewResponseRecorder(spy, new(bytes.Buffer), func(int, http.Header) bool { return true })
+	rr.WriteHeader(http.StatusSwitchingProtocols)
+
+	hj, ok := rr.(http.Hijacker)
+	if !ok {
+		t.Fatal("recorder is not a http.Hijacker")
+	}
+	conn, _, err := hj.Hijack()
+	if err != nil {
+		t.Fatalf("Hijack() error = %v", err)
+	}
+	defer conn.Close()
+	defer spy.peer.Close()
+
+	rr.WriteHeader(http.StatusInternalServerError)
+	if n, err := rr.Write([]byte("late")); n != 0 || !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("Write() = %d, %v; want 0, ErrHijacked", n, err)
+	}
+	src := struct{ io.Reader }{strings.NewReader("late")}
+	if n, err := io.Copy(rr, src); n != 0 || !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("ReadFrom() = %d, %v; want 0, ErrHijacked", n, err)
+	}
+	if err := rr.WriteResponse(); err != nil {
+		t.Errorf("WriteResponse() error = %v", err)
+	}
+	if err := http.NewResponseController(rr).Flush(); err != nil {
+		t.Errorf("Flush() error = %v", err)
+	}
+
+	if len(spy.statuses) != 1 {
+		t.Errorf("underlying statuses = %v, want only the 101", spy.statuses)
+	}
+	if len(spy.body) != 0 {
+		t.Errorf("underlying body = %q, want nothing", spy.body)
+	}
+	if rr.Status() != http.StatusSwitchingProtocols {
+		t.Errorf("Status() = %d, want 101", rr.Status())
+	}
+}
+
+func TestResponseRecorderHijackWithoutStatusWritesNoStatus(t *testing.T) {
+	spy := &hijackSpy{}
+	rr := NewResponseRecorder(spy, new(bytes.Buffer), nil)
+
+	conn, _, err := rr.(http.Hijacker).Hijack()
+	if err != nil {
+		t.Fatalf("Hijack() error = %v", err)
+	}
+	defer conn.Close()
+	defer spy.peer.Close()
+
+	rr.WriteHeader(http.StatusOK)
+	if len(spy.statuses) != 0 {
+		t.Errorf("underlying statuses = %v, want none after hijack", spy.statuses)
+	}
+}
+
+func TestServerAccessLoggers(t *testing.T) {
+	r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
+
+	// access logging disabled
+	if got := (&Server{}).AccessLoggers(r); got != nil {
+		t.Errorf("AccessLoggers() without logging = %v, want nil", got)
+	}
+
+	logger := testLogger(func(p []byte) (int, error) { return len(p), nil })
+	s := &Server{accessLogger: logger, Logs: &ServerLogConfig{}}
+	if got := s.AccessLoggers(r); len(got) != 1 || got[0] != logger {
+		t.Errorf("AccessLoggers() = %v, want the access logger", got)
+	}
+
+	s.Logs = &ServerLogConfig{LoggerNames: map[string]StringArray{"example.com": {"custom"}}}
+	got := s.AccessLoggers(r)
+	if len(got) != 1 || got[0].Name() != "custom" {
+		t.Errorf("AccessLoggers() with a logger name = %v, want the logger named custom", got)
+	}
+}
+
+func TestResponseRecorderFailedHijackKeepsWriting(t *testing.T) {
+	spy := &baseRespWriter{}
+	rr := NewResponseRecorder(spy, nil, nil)
+
+	if _, _, err := rr.(http.Hijacker).Hijack(); err == nil {
+		t.Fatal("expected Hijack() to fail on a non-hijackable writer")
+	}
+	if _, err := rr.Write([]byte("ok")); err != nil {
+		t.Errorf("Write() after failed hijack error = %v", err)
+	}
+	if got := spy.Written(); got != "ok" {
+		t.Errorf("written = %q, want %q", got, "ok")
+	}
+}
+
 type responseWriterSpy interface {
 	http.ResponseWriter
 	Written() string
diff --git a/modules/caddyhttp/reverseproxy/caddyfile.go b/modules/caddyhttp/reverseproxy/caddyfile.go
index 12c1177b..1ec31eac 100644
--- a/modules/caddyhttp/reverseproxy/caddyfile.go
+++ b/modules/caddyhttp/reverseproxy/caddyfile.go
@@ -99,8 +99,22 @@ func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error)
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
+// With stream_detached, upgraded streams (e.g. WebSockets) survive config
+// reloads, and a reload only closes the streams whose upstream was removed
+// from the config; by default, they are closed on reload, after
+// stream_close_delay if set. In stream_logs, skip_handshake omits the
+// request that performs the upgrade handshake from the access log (it is
+// logged by default), and level (debug, info, warn or error; default debug)
+// sets the level of the entry written to the access loggers, selectable with
+// the log directive's logger_name, when a stream closes.
+//
 //	    # reporting
 //	    proxy_status_name <name>
 //
@@ -715,6 +729,39 @@ func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
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
diff --git a/modules/caddyhttp/reverseproxy/metrics.go b/modules/caddyhttp/reverseproxy/metrics.go
index 24884273..d8a1bc1f 100644
--- a/modules/caddyhttp/reverseproxy/metrics.go
+++ b/modules/caddyhttp/reverseproxy/metrics.go
@@ -16,36 +16,101 @@ import (
 var reverseProxyMetrics = struct {
 	once             sync.Once
 	upstreamsHealthy *prometheus.GaugeVec
-	logger           *zap.Logger
+
+	// stream metrics; they live as long as the process so that upgraded
+	// streams that outlive a config are still accounted for exactly once
+	streamsActive          prometheus.Gauge
+	streamsClosed          *prometheus.CounterVec
+	streamDuration         prometheus.Histogram
+	streamBytesToBackend   prometheus.Counter
+	streamBytesFromBackend prometheus.Counter
+
+	logger *zap.Logger
 }{}
 
-func initReverseProxyMetrics(handler *Handler, registry *prometheus.Registry) {
+// initReverseProxyMetricCollectors creates the collectors once.
+func initReverseProxyMetricCollectors() {
 	const ns, sub = "caddy", "reverse_proxy"
 
-	upstreamsLabels := []string{"upstream"}
 	reverseProxyMetrics.once.Do(func() {
 		reverseProxyMetrics.upstreamsHealthy = prometheus.NewGaugeVec(prometheus.GaugeOpts{
 			Namespace: ns,
 			Subsystem: sub,
 			Name:      "upstreams_healthy",
 			Help:      "Health status of reverse proxy upstreams.",
-		}, upstreamsLabels)
+		}, []string{"upstream"})
+		reverseProxyMetrics.streamsActive = prometheus.NewGauge(prometheus.GaugeOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "streams_active",
+			Help:      "Number of upgraded streams (e.g. WebSockets) currently open.",
+		})
+		reverseProxyMetrics.streamsClosed = prometheus.NewCounterVec(prometheus.CounterOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "streams_closed_total",
+			Help:      "Number of upgraded streams that were closed, by result.",
+		}, []string{"result"})
+		reverseProxyMetrics.streamDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "stream_duration_seconds",
+			Help:      "How long upgraded streams were open.",
+			Buckets:   []float64{.1, .5, 1, 5, 15, 60, 300, 900, 3600, 14400, 86400},
+		})
+		reverseProxyMetrics.streamBytesToBackend = prometheus.NewCounter(prometheus.CounterOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "stream_bytes_to_backend_total",
+			Help:      "Bytes sent to the backend over upgraded streams.",
+		})
+		reverseProxyMetrics.streamBytesFromBackend = prometheus.NewCounter(prometheus.CounterOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "stream_bytes_from_backend_total",
+			Help:      "Bytes received from the backend over upgraded streams.",
+		})
 	})
+}
+
+func initReverseProxyMetrics(handler *Handler, registry *prometheus.Registry) {
+	initReverseProxyMetricCollectors()
 
 	// duplicate registration could happen if multiple sites with reverse proxy are configured; so ignore the error because
 	// there's no good way to capture having multiple sites with reverse proxy. If this happens, the metrics will be
 	// registered twice, but the second registration will be ignored.
-	if err := registry.Register(reverseProxyMetrics.upstreamsHealthy); err != nil &&
-		!errors.Is(err, prometheus.AlreadyRegisteredError{
-			ExistingCollector: reverseProxyMetrics.upstreamsHealthy,
-			NewCollector:      reverseProxyMetrics.upstreamsHealthy,
-		}) {
-		panic(err)
+	for _, c := range []prometheus.Collector{
+		reverseProxyMetrics.upstreamsHealthy,
+		reverseProxyMetrics.streamsActive,
+		reverseProxyMetrics.streamsClosed,
+		reverseProxyMetrics.streamDuration,
+		reverseProxyMetrics.streamBytesToBackend,
+		reverseProxyMetrics.streamBytesFromBackend,
+	} {
+		if err := registry.Register(c); err != nil && !errors.As(err, &prometheus.AlreadyRegisteredError{}) {
+			panic(err)
+		}
 	}
 
 	reverseProxyMetrics.logger = handler.logger.Named("reverse_proxy.metrics")
 }
 
+// streamStarted accounts for a stream that is now open.
+func streamStarted() {
+	initReverseProxyMetricCollectors()
+	reverseProxyMetrics.streamsActive.Inc()
+}
+
+// streamFinished accounts for a stream that was closed. It must be called
+// exactly once for each call to streamStarted.
+func streamFinished(result string, duration time.Duration, toBackend, fromBackend int64) {
+	reverseProxyMetrics.streamsActive.Dec()
+	reverseProxyMetrics.streamsClosed.WithLabelValues(result).Inc()
+	reverseProxyMetrics.streamDuration.Observe(duration.Seconds())
+	reverseProxyMetrics.streamBytesToBackend.Add(float64(toBackend))
+	reverseProxyMetrics.streamBytesFromBackend.Add(float64(fromBackend))
+}
+
 type metricsUpstreamsHealthyUpdater struct {
 	handler *Handler
 }
diff --git a/modules/caddyhttp/reverseproxy/metrics_test.go b/modules/caddyhttp/reverseproxy/metrics_test.go
new file mode 100644
index 00000000..c80ce161
--- /dev/null
+++ b/modules/caddyhttp/reverseproxy/metrics_test.go
@@ -0,0 +1,96 @@
+package reverseproxy
+
+import (
+	"testing"
+	"time"
+
+	"github.com/prometheus/client_golang/prometheus"
+	"go.uber.org/zap"
+)
+
+// streamMetricsSnapshot is the state of the stream metrics at some point.
+type streamMetricsSnapshot struct {
+	active        float64
+	closed        map[string]float64
+	durationCount uint64
+	toBackend     float64
+	fromBackend   float64
+}
+
+// snapshotStreamMetrics reads the stream metrics through a registry, the way
+// a scrape would.
+func snapshotStreamMetrics(t *testing.T) streamMetricsSnapshot {
+	t.Helper()
+	registry := prometheus.NewRegistry()
+	initReverseProxyMetrics(&Handler{logger: zap.NewNop()}, registry)
+	families, err := registry.Gather()
+	if err != nil {
+		t.Fatalf("gathering metrics: %v", err)
+	}
+
+	snap := streamMetricsSnapshot{closed: make(map[string]float64)}
+	for _, mf := range families {
+		for _, m := range mf.GetMetric() {
+			switch mf.GetName() {
+			case "caddy_reverse_proxy_streams_active":
+				snap.active = m.GetGauge().GetValue()
+			case "caddy_reverse_proxy_streams_closed_total":
+				for _, l := range m.GetLabel() {
+					if l.GetName() == "result" {
+						snap.closed[l.GetValue()] = m.GetCounter().GetValue()
+					}
+				}
+			case "caddy_reverse_proxy_stream_duration_seconds":
+				snap.durationCount = m.GetHistogram().GetSampleCount()
+			case "caddy_reverse_proxy_stream_bytes_to_backend_total":
+				snap.toBackend = m.GetCounter().GetValue()
+			case "caddy_reverse_proxy_stream_bytes_from_backend_total":
+				snap.fromBackend = m.GetCounter().GetValue()
+			}
+		}
+	}
+	return snap
+}
+
+func TestStreamMetricsAccountEachStreamOnce(t *testing.T) {
+	before := snapshotStreamMetrics(t)
+
+	streamStarted()
+	streamStarted()
+	during := snapshotStreamMetrics(t)
+	if got := during.active - before.active; got != 2 {
+		t.Fatalf("active streams went up by %v, want 2", got)
+	}
+
+	streamFinished(streamResultCompleted, 2*time.Second, 10, 20)
+	streamFinished(streamResultTimeout, time.Second, 1, 2)
+	after := snapshotStreamMetrics(t)
+
+	if after.active != before.active {
+		t.Errorf("active gauge = %v, want it back to %v", after.active, before.active)
+	}
+	if got := after.closed[streamResultCompleted] - before.closed[streamResultCompleted]; got != 1 {
+		t.Errorf("completed streams went up by %v, want 1", got)
+	}
+	if got := after.closed[streamResultTimeout] - before.closed[streamResultTimeout]; got != 1 {
+		t.Errorf("timed out streams went up by %v, want 1", got)
+	}
+	if got := after.durationCount - before.durationCount; got != 2 {
+		t.Errorf("duration observations went up by %d, want 2", got)
+	}
+	if got := after.toBackend - before.toBackend; got != 11 {
+		t.Errorf("bytes to backend went up by %v, want 11", got)
+	}
+	if got := after.fromBackend - before.fromBackend; got != 22 {
+		t.Errorf("bytes from backend went up by %v, want 22", got)
+	}
+}
+
+// Registering the metrics on a registry that has them already, as happens
+// with several reverse proxies in one config, must not fail.
+func TestInitReverseProxyMetricsTwiceOnSameRegistry(t *testing.T) {
+	registry := prometheus.NewRegistry()
+	h := &Handler{logger: zap.NewNop()}
+	initReverseProxyMetrics(h, registry)
+	initReverseProxyMetrics(h, registry)
+}
diff --git a/modules/caddyhttp/reverseproxy/reverseproxy.go b/modules/caddyhttp/reverseproxy/reverseproxy.go
index 0838c8be..aef351a2 100644
--- a/modules/caddyhttp/reverseproxy/reverseproxy.go
+++ b/modules/caddyhttp/reverseproxy/reverseproxy.go
@@ -199,6 +199,21 @@ type Handler struct {
 	// by the previous config closing. Default: no delay.
 	StreamCloseDelay caddy.Duration `json:"stream_close_delay,omitempty"`
 
+	// If true, upgraded streams such as WebSockets are detached from the
+	// lifecycle of the config that created them: they survive config
+	// reloads, and a reload only closes the streams whose upstream is not
+	// part of the new config anymore. This lets a server that reloads
+	// often avoid holding on to many old configs. Streams to dynamic
+	// upstreams cannot be matched to the config and are never closed by
+	// a reload. By default, upgraded streams are closed when the config is
+	// unloaded, honoring stream_close_delay. Default: false.
+	StreamDetached bool `json:"stream_detached,omitempty"`
+
+	// Configures the logging of upgraded streams, such as whether the
+	// upgrade handshake is access-logged and at which level the entry
+	// written when a stream closes is emitted.
+	StreamLogs *StreamLogs `json:"stream_logs,omitempty"`
+
 	// If configured, rewrites the copy of the upstream request.
 	// Allows changing the request method and URI (path and query).
 	// Since the rewrite is applied to the copy, it does not persist
@@ -263,7 +278,10 @@ type Handler struct {
 	connectionsCloseTimer *time.Timer
 	connectionsMu         *sync.Mutex
 
-	ctx    caddy.Context
+	// the upstreams this handler registered as live in the stream registry
+	registeredUpstreams []string
+
+	ctx   caddy.Context
 	logger *zap.Logger
 	events *caddyevents.App
 }
@@ -278,6 +296,12 @@ func (Handler) CaddyModule() caddy.ModuleInfo {
 
 // Provision ensures that h is set up properly before use.
 func (h *Handler) Provision(ctx caddy.Context) error {
+	if h.StreamLogs != nil {
+		if _, err := parseStreamLogLevel(h.StreamLogs.Level); err != nil {
+			return err
+		}
+	}
+
 	eventAppIface, err := ctx.App("events")
 	if err != nil {
 		return fmt.Errorf("getting events app: %v", err)
@@ -453,12 +477,25 @@ func (h *Handler) Provision(ctx caddy.Context) error {
 	upstreamHealthyUpdater := newMetricsUpstreamsHealthyUpdater(h, ctx)
 	upstreamHealthyUpdater.init()
 
+	// from here on, nothing can fail anymore, so Cleanup always
+	// releases exactly what was registered
+	h.registeredUpstreams = h.liveUpstreams()
+	detachedStreams.addLive(h.registeredUpstreams)
+
 	return nil
 }
 
 // Cleanup cleans up the resources made by h.
 func (h *Handler) Cleanup() error {
+	// by the time the old config is cleaned up, the new one is provisioned,
+	// so the upstreams it still uses are live and their streams are kept
+	orphaned := detachedStreams.removeLive(h.registeredUpstreams)
+	h.registeredUpstreams = nil
+
 	err := h.cleanupConnections()
+	if closeErr := closeDetachedStreams(orphaned); closeErr != nil && err == nil {
+		err = closeErr
+	}
 
 	// remove hosts from our config from the pool
 	for _, upstream := range h.Upstreams {
diff --git a/modules/caddyhttp/reverseproxy/stream_options_test.go b/modules/caddyhttp/reverseproxy/stream_options_test.go
new file mode 100644
index 00000000..c00b4e44
--- /dev/null
+++ b/modules/caddyhttp/reverseproxy/stream_options_test.go
@@ -0,0 +1,163 @@
+package reverseproxy
+
+import (
+	"encoding/json"
+	"testing"
+
+	"go.uber.org/zap/zapcore"
+
+	"github.com/caddyserver/caddy/v2"
+	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
+)
+
+func TestStreamOptionsCaddyfile(t *testing.T) {
+	for i, tc := range []struct {
+		input      string
+		wantErr    bool
+		detached   bool
+		logs       *StreamLogs
+		wantNoLogs bool
+	}{
+		{input: `reverse_proxy localhost:9000`, wantNoLogs: true},
+		{input: `reverse_proxy localhost:9000 {
+			stream_detached
+		}`, detached: true, wantNoLogs: true},
+		{input: `reverse_proxy localhost:9000 {
+			stream_logs {
+			}
+		}`, logs: &StreamLogs{}},
+		{input: `reverse_proxy localhost:9000 {
+			stream_logs {
+				skip_handshake
+			}
+		}`, logs: &StreamLogs{SkipHandshake: true}},
+		{input: `reverse_proxy localhost:9000 {
+			stream_detached
+			stream_logs {
+				skip_handshake
+				level info
+			}
+		}`, detached: true, logs: &StreamLogs{SkipHandshake: true, Level: "info"}},
+		{input: `reverse_proxy localhost:9000 {
+			stream_detached on
+		}`, wantErr: true},
+		{input: `reverse_proxy localhost:9000 {
+			stream_logs verbose
+		}`, wantErr: true},
+		{input: `reverse_proxy localhost:9000 {
+			stream_logs {
+				skip_handshake yes
+			}
+		}`, wantErr: true},
+		{input: `reverse_proxy localhost:9000 {
+			stream_logs {
+				level
+			}
+		}`, wantErr: true},
+		{input: `reverse_proxy localhost:9000 {
+			stream_logs {
+				level info debug
+			}
+		}`, wantErr: true},
+		{input: `reverse_proxy localhost:9000 {
+			stream_logs {
+				unknown
+			}
+		}`, wantErr: true},
+	} {
+		var h Handler
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
+		if h.StreamDetached != tc.detached {
+			t.Errorf("Test %d: StreamDetached = %v, want %v", i, h.StreamDetached, tc.detached)
+		}
+		if tc.wantNoLogs {
+			if h.StreamLogs != nil {
+				t.Errorf("Test %d: StreamLogs = %+v, want nil", i, h.StreamLogs)
+			}
+			continue
+		}
+		if h.StreamLogs == nil || *h.StreamLogs != *tc.logs {
+			t.Errorf("Test %d: StreamLogs = %+v, want %+v", i, h.StreamLogs, tc.logs)
+		}
+	}
+}
+
+func TestStreamOptionsJSON(t *testing.T) {
+	var defaults Handler
+	if err := json.Unmarshal([]byte(`{}`), &defaults); err != nil {
+		t.Fatal(err)
+	}
+	if defaults.StreamDetached || defaults.StreamLogs != nil {
+		t.Errorf("defaults = %v, %+v; want not detached and no stream logs", defaults.StreamDetached, defaults.StreamLogs)
+	}
+
+	var h Handler
+	err := json.Unmarshal([]byte(`{"stream_detached": true, "stream_logs": {"skip_handshake": true, "level": "warn"}}`), &h)
+	if err != nil {
+		t.Fatal(err)
+	}
+	if !h.StreamDetached {
+		t.Error("StreamDetached = false, want true")
+	}
+	if h.StreamLogs == nil 
... (truncated; 41349 more characters; read the files)
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
that probe as `proof`. The harness runs every probe again: a criterion whose probe is missing or does
not run is recorded as unprobed, visible in the pull request, and you will be asked for it once.
`probe.path` is a new file in a directory of the task's test files, named like a test file of the
repository; `probe.content` is the whole file.

`verdict` is `request_changes` if and only if there is at least one `blocking` finding or a
criterion that is not `covered`.
