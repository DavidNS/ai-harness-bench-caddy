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

The whole request ships as one PR in the reverseproxy module, with small supporting edits in caddyhttp (recorder, server logging), encode and intercept. Docs live in Go doc comments, because the repo has no separate docs files for these options. I did not trace the access-log skip mechanism or how `logRequest` handles hijacked connections, so the implementer should read those first.

## Task T1 (T1)

Implement the whole feature as one PR. Order: (1) add StreamDetached/StreamLogs fields, Caddyfile parsing and docs; (2) add metrics and a once-only per-stream accounting struct; (3) add a global detached-stream registry keyed by upstream, with Cleanup closing only streams whose upstream is gone from the new config, leaving the non-detached path and close-delay untouched; (4) make 101 final and writes after hijack no-ops in the caddyhttp recorder and encode, hijack on the raw connection in handleUpgradeResponse, and send close logs to the access logger with skip_handshake in server logging; (5) fix intercept for upgrades; (6) update existing tests and add unit, caddyfile-adapt and integration tests.

Acceptance criteria:
- ACC1 [behavior]: With stream_detached set, an established upgraded stream keeps passing bytes after a config reload that keeps its upstream.
- ACC2 [behavior]: With stream_detached set, a reload that removes the stream's upstream closes that stream while streams to retained upstreams stay open.
- ACC3 [behavior]: Without stream_detached, upgraded streams are closed on reload, immediately when stream_close_delay is 0 and after the delay otherwise.
- ACC4 [behavior]: stream_logs skip_handshake suppresses the access log entry for the upgrade request; by default the handshake is logged.
- ACC5 [behavior]: A stream close log is written to the access logger (logger_name access) at the configured level, DEBUG by default, including with detached streams.
- ACC6 [behavior]: WebSocket/Upgrade streams work end to end through encode, through intercept with handle_response, and with access logging enabled.
- ACC7 [behavior]: After Hijack, the response recorder and encode writer never write header or body to the underlying writer; 101 is treated as a final status; the proxy copies on the raw connection rather than the recorder's wrapper.
- ACC8 [behavior]: An RFC 8441 extended CONNECT websocket over h2 works through the proxy, and an HTTP/1.1 upgrade works in detached mode.
- ACC9 [behavior]: Stream metrics (active gauge, closed-by-result counter, duration histogram, bytes to/from backend) are updated and each stream is counted exactly once regardless of how many close paths fire.
- ACC10 [behavior]: Caddyfile stream_detached and stream_logs { skip_handshake, level } adapt to the JSON fields stream_detached and stream_logs; invalid input errors; defaults are omitted from JSON.
- ACC11 [other]: New options are documented in the struct field comments and the Caddyfile syntax comment in caddyfile.go.
- ACC12 [behavior]: Integration tests exist for reload survive/close in both modes, h2 extended CONNECT, encode, intercept, and detached with access logging.
- ACC13 [behavior]: Unit tests cover the stream registry, metrics accounting, Caddyfile parsing, recorder/encode hijack safety.

Files declared for the task:
- modules/caddyhttp/reverseproxy/reverseproxy.go (production)
- modules/caddyhttp/reverseproxy/streaming.go (production)
- modules/caddyhttp/reverseproxy/caddyfile.go (production)
- modules/caddyhttp/reverseproxy/metrics.go (production)
- modules/caddyhttp/reverseproxy/streams.go (production)
- modules/caddyhttp/responsewriter.go (production)
- modules/caddyhttp/server.go (production)
- modules/caddyhttp/logging.go (production)
- modules/caddyhttp/encode/encode.go (production)
- modules/caddyhttp/intercept/intercept.go (production)
- modules/caddyhttp/reverseproxy/streaming_test.go (test)
- modules/caddyhttp/reverseproxy/hopheaders_test.go (test)
- modules/caddyhttp/reverseproxy/streams_test.go (test)
- modules/caddyhttp/reverseproxy/metrics_test.go (test)
- modules/caddyhttp/reverseproxy/caddyfile_test.go (test)
- modules/caddyhttp/responsewriter_test.go (test)
- modules/caddyhttp/encode/encode_conformance_test.go (test)
- modules/caddyhttp/intercept/intercept_test.go (test)
- caddytest/integration/stream_reload_test.go (test)
- caddytest/integration/stream_test.go (test)
- caddytest/integration/intercept_test.go (test)
- caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest (test)

Test files of this task that the change modified (existing tests, declared by the plan):
- caddytest/integration/intercept_test.go
- modules/caddyhttp/encode/encode_conformance_test.go
- modules/caddyhttp/responsewriter_test.go
- modules/caddyhttp/reverseproxy/streaming_test.go

Test results: selected tests green; red proven; suite green

## The diff

```diff
diff --git a/caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest b/caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest
new file mode 100644
index 00000000..9c7e7005
--- /dev/null
+++ b/caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest
@@ -0,0 +1,42 @@
+:80 {
+	reverse_proxy localhost:54321 {
+		stream_detached
+		stream_logs {
+			skip_handshake
+			level INFO
+		}
+	}
+}
+----------
+{
+	"apps": {
+		"http": {
+			"servers": {
+				"srv0": {
+					"listen": [
+						":80"
+					],
+					"routes": [
+						{
+							"handle": [
+								{
+									"handler": "reverse_proxy",
+									"stream_detached": true,
+									"stream_logs": {
+										"level": "info",
+										"skip_handshake": true
+									},
+									"upstreams": [
+										{
+											"dial": "localhost:54321"
+										}
+									]
+								}
+							]
+						}
+					]
+				}
+			}
+		}
+	}
+}
diff --git a/caddytest/integration/intercept_test.go b/caddytest/integration/intercept_test.go
index c1b33b96..1d80f083 100644
--- a/caddytest/integration/intercept_test.go
+++ b/caddytest/integration/intercept_test.go
@@ -39,6 +39,46 @@ func TestIntercept(t *testing.T) {
 	tester.AssertGetResponse("http://localhost:9080/no-intercept", 200, "I'm not a teapot")
 }
 
+// An upgrade is the final response: handle_response routes that match every
+// response do not get to intercept it.
+func TestInterceptUpgradeThroughReverseProxy(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+	tester := caddytest.NewTester(t)
+	tester.InitServer(streamCaddyfile(1, `intercept {
+		handle_response {
+			respond "intercepted" 200
+		}
+	}
+	reverse_proxy 127.0.0.1:54331`), "caddyfile")
+
+	tn := openTunnel(t, "/ws", "echo")
+	tn.echo(t, "through intercept")
+	tn.echo(t, "and again")
+
+	// the same with a WebSocket handshake
+	ws := openTunnel(t, "/ws", "websocket",
+		"Sec-WebSocket-Version: 13",
+		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==")
+	ws.echo(t, "websocket through intercept")
+}
+
+// The same with the interception done by the reverse proxy's own handle_response.
+func TestReverseProxyHandleResponseUpgrade(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+	tester := caddytest.NewTester(t)
+	tester.InitServer(streamCaddyfile(1, `reverse_proxy 127.0.0.1:54331 {
+		@upgraded status 101
+		handle_response @upgraded {
+			copy_response
+		}
+		stream_detached
+	}`), "caddyfile")
+
+	tn := openTunnel(t, "/ws", "echo")
+	tn.echo(t, "through handle_response")
+	tester.AssertGetResponse("http://localhost:9080/plain", 200, "plain response")
+}
+
 func TestInterceptReplaceStatusWithMatcher(t *testing.T) {
 	tester := caddytest.NewTester(t)
 	tester.InitServer(`{
diff --git a/caddytest/integration/stream_reload_test.go b/caddytest/integration/stream_reload_test.go
new file mode 100644
index 00000000..4e456992
--- /dev/null
+++ b/caddytest/integration/stream_reload_test.go
@@ -0,0 +1,764 @@
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
+	"path/filepath"
+	"strings"
+	"sync"
+	"syscall"
+	"testing"
+	"time"
+
+	"golang.org/x/net/http2"
+	"golang.org/x/net/http2/hpack"
+
+	"github.com/caddyserver/caddy/v2/caddytest"
+)
+
+// TestMain makes sure net/http's HTTP/2 server supports extended CONNECT
+// (RFC 8441), which it only does if the process starts with this setting.
+func TestMain(m *testing.M) {
+	const setting = "http2xconnect=1"
+	if godebug := os.Getenv("GODEBUG"); !strings.Contains(godebug, setting) {
+		env := []string{"GODEBUG=" + strings.Trim(godebug+","+setting, ",")}
+		for _, kv := range os.Environ() {
+			if !strings.HasPrefix(kv, "GODEBUG=") {
+				env = append(env, kv)
+			}
+		}
+		// replaces the process where that is supported; elsewhere the
+		// tests that need extended CONNECT are skipped
+		_ = syscall.Exec(os.Args[0], os.Args, env)
+	}
+	os.Exit(m.Run())
+}
+
+const (
+	streamBackendA = "127.0.0.1:54331"
+	streamBackendB = "127.0.0.1:54332"
+	streamWait     = 5 * time.Second
+)
+
+// startUpgradeBackend starts a backend that echoes whatever is sent over an
+// upgraded connection (any Upgrade protocol), and answers plain requests with
+// "plain response".
+func startUpgradeBackend(t *testing.T, addr string) {
+	t.Helper()
+	ln, err := net.Listen("tcp", addr)
+	if err != nil {
+		t.Fatalf("listening on %s: %v", addr, err)
+	}
+	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
+		upgrade := r.Header.Get("Upgrade")
+		if upgrade == "" {
+			_, _ = io.WriteString(w, "plain response")
+			return
+		}
+		conn, brw, err := http.NewResponseController(w).Hijack()
+		if err != nil {
+			return
+		}
+		defer conn.Close()
+		_, _ = fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", upgrade)
+		_, _ = io.Copy(conn, brw.Reader)
+	})}
+	go srv.Serve(ln) //nolint:errcheck
+	// hijacked connections are not closed by this: they end when the proxy closes them
+	t.Cleanup(func() { srv.Close() })
+}
+
+// tunnel is the client side of an upgraded connection.
+type tunnel struct {
+	conn net.Conn
+	br   *bufio.Reader
+}
+
+// openTunnel sends an upgrade request for the given protocol to Caddy's
+// HTTP port and expects the 101 response.
+func openTunnel(t *testing.T, path, protocol string, extraHeaders ...string) *tunnel {
+	t.Helper()
+	conn, err := net.DialTimeout("tcp", "127.0.0.1:9080", streamWait)
+	if err != nil {
+		t.Fatalf("dialing caddy: %v", err)
+	}
+	t.Cleanup(func() { conn.Close() })
+
+	var req strings.Builder
+	fmt.Fprintf(&req, "GET %s HTTP/1.1\r\nHost: localhost:9080\r\nConnection: Upgrade\r\nUpgrade: %s\r\n", path, protocol)
+	for _, h := range extraHeaders {
+		req.WriteString(h + "\r\n")
+	}
+	req.WriteString("\r\n")
+	if _, err := io.WriteString(conn, req.String()); err != nil {
+		t.Fatalf("writing upgrade request: %v", err)
+	}
+
+	br := bufio.NewReader(conn)
+	_ = conn.SetReadDeadline(time.Now().Add(streamWait))
+	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
+	if err != nil {
+		t.Fatalf("reading upgrade response: %v", err)
+	}
+	if resp.StatusCode != http.StatusSwitchingProtocols {
+		t.Fatalf("upgrade response status = %d, want 101", resp.StatusCode)
+	}
+	return &tunnel{conn: conn, br: br}
+}
+
+// echo sends msg through the tunnel and expects to get it back.
+func (tn *tunnel) echo(t *testing.T, msg string) {
+	t.Helper()
+	_ = tn.conn.SetDeadline(time.Now().Add(streamWait))
+	if _, err := io.WriteString(tn.conn, msg); err != nil {
+		t.Fatalf("writing %q to the tunnel: %v", msg, err)
+	}
+	got := make([]byte, len(msg))
+	if _, err := io.ReadFull(tn.br, got); err != nil {
+		t.Fatalf("reading %q back from the tunnel: %v", msg, err)
+	}
+	if string(got) != msg {
+		t.Fatalf("tunnel echoed %q, want %q", got, msg)
+	}
+}
+
+// waitClosed waits for the proxy to close the tunnel, and reports how long it took.
+func (tn *tunnel) waitClosed(t *testing.T, within time.Duration) time.Duration {
+	t.Helper()
+	start := time.Now()
+	buf := make([]byte, 64)
+	for {
+		_ = tn.conn.SetReadDeadline(start.Add(within))
+		if _, err := tn.br.Read(buf); err != nil {
+			if ne, ok := err.(net.Error); ok && ne.Timeout() {
+				t.Fatalf("the tunnel was still open after %v", within)
+			}
+			return time.Since(start)
+		}
+	}
+}
+
+// streamCaddyfile returns a Caddyfile with a localhost:9080 site made of body.
+// gen differs between the configs of a test, since an unchanged config
+// is not reloaded.
+func streamCaddyfile(gen int, body string) string {
+	return fmt.Sprintf(`{
+	skip_install_trust
+	admin localhost:2999
+	http_port     9080
+	https_port    9443
+	grace_period  1ns
+}
+
+localhost:9080 {
+	header X-Gen %d
+	%s
+}`, gen, body)
+}
+
+const (
+	routeAdetached = `handle /a* {
+		reverse_proxy 127.0.0.1:54331 {
+			stream_detached
+		}
+	}
+	`
+	routeBdetached = `handle /b* {
+		reverse_proxy 127.0.0.1:54332 {
+			stream_detached
+		}
+	}
+	`
+	routeFallback = `handle {
+		respond "fallback"
+	}`
+)
+
+func TestStreamDetachedSurvivesReload(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+	tester := caddytest.NewTester(t)
+	tester.InitServer(streamCaddyfile(1, routeAdetached+routeFallback), "caddyfile")
+
+	tn := openTunnel(t, "/a", "echo")
+	tn.echo(t, "before reload")
+
+	// reloads that keep the upstream
+	tester.InitServer(streamCaddyfile(2, routeAdetached+routeFallback), "caddyfile")
+	tn.echo(t, "after first reload")
+	tester.InitServer(streamCaddyfile(3, routeAdetached+routeFallback), "caddyfile")
+	tn.echo(t, "after second reload")
+
+	// the upgrade still works for new connections to the reloaded config
+	tn2 := openTunnel(t, "/a", "echo")
+	tn2.echo(t, "new stream")
+
+	// the upstream is removed from the config
+	tester.InitServer(streamCaddyfile(4, routeFallback), "caddyfile")
+	tn.waitClosed(t, streamWait)
+	tn2.waitClosed(t, streamWait)
+}
+
+// An upstream address with a placeholder can't be matched against the new config,
+// but the stream is not closed for that: it is kept while such a handler is live.
+func TestStreamDetachedPlaceholderUpstream(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+	tester := caddytest.NewTester(t)
+	body := `reverse_proxy 127.0.0.1:{http.request.header.X-Port} {
+		stream_detached
+	}`
+	tester.InitServer(streamCaddyfile(1, body), "caddyfile")
+
+	tn := openTunnel(t, "/", "echo", "X-Port: 54331")
+	tn.echo(t, "before reload")
+	tester.InitServer(streamCaddyfile(2, body), "caddyfile")
+	tn.echo(t, "after reload")
+
+	// no such handler in the config anymore
+	tester.InitServer(streamCaddyfile(3, routeFallback), "caddyfile")
+	tn.waitClosed(t, streamWait)
+}
+
+func TestStreamDetachedClosesOnlyRemovedUpstream(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+	startUpgradeBackend(t, streamBackendB)
+	tester := caddytest.NewTester(t)
+	tester.InitServer(streamCaddyfile(1, routeAdetached+routeBdetached+routeFallback), "caddyfile")
+
+	a := openTunnel(t, "/a", "echo")
+	b := openTunnel(t, "/b", "echo")
+	a.echo(t, "to a")
+	b.echo(t, "to b")
+
+	// backend A is removed, backend B is retained
+	tester.InitServer(streamCaddyfile(2, routeBdetached+routeFallback), "caddyfile")
+	a.waitClosed(t, streamWait)
+	b.echo(t, "b is still open")
+
+	// and then B is removed too
+	tester.InitServer(streamCaddyfile(3, routeFallback), "caddyfile")
+	b.waitClosed(t, streamWait)
+}
+
+func TestStreamNotDetachedClosedOnReload(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+	tester := caddytest.NewTester(t)
+	body := `reverse_proxy 127.0.0.1:54331`
+	tester.InitServer(streamCaddyfile(1, body), "caddyfile")
+
+	tn := openTunnel(t, "/", "echo")
+	tn.echo(t, "before reload")
+
+	// even though the upstream is retained, the stream goes with the config
+	tester.InitServer(streamCaddyfile(2, body), "caddyfile")
+	tn.waitClosed(t, streamWait)
+
+	// the new config serves new streams
+	openTunnel(t, "/", "echo").echo(t, "new stream")
+}
+
+func TestStreamCloseDelayKeepsStreamUntilDelay(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+	tester := caddytest.NewTester(t)
+	body := `reverse_proxy 127.0.0.1:54331 {
+		stream_close_delay 3s
+	}`
+	tester.InitServer(streamCaddyfile(1, body), "caddyfile")
+
+	tn := openTunnel(t, "/", "echo")
+	tn.echo(t, "before reload")
+
+	start := time.Now()
+	tester.InitServer(streamCaddyfile(2, body), "caddyfile")
+	tn.echo(t, "after reload, within the delay")
+
+	tn.waitClosed(t, 4*streamWait)
+	if elapsed := time.Since(start); elapsed < 2*time.Second {
+		t.Errorf("the stream was closed %v after the reload started, before stream_close_delay", elapsed)
+	}
+}
+
+// Detached streams are not closed by the delay either, which does not apply to them.
+func TestStreamDetachedIgnoresCloseDelay(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+	tester := caddytest.NewTester(t)
+	body := `reverse_proxy 127.0.0.1:54331 {
+		stream_detached
+		stream_close_delay 1s
+	}`
+	tester.InitServer(streamCaddyfile(1, body), "caddyfile")
+
+	tn := openTunnel(t, "/", "echo")
+	tester.InitServer(streamCaddyfile(2, body), "caddyfile")
+	time.Sleep(2 * time.Second)
+	tn.echo(t, "still open after the delay")
+}
+
+func TestStreamThroughEncode(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+	tester := caddytest.NewTester(t)
+
+	for _, tc := range []struct {
+		name string
+		opts string
+	}{
+		{name: "default"},
+		{name: "detached", opts: "stream_detached"},
+	} {
+		t.Run(tc.name, func(t *testing.T) {
+			tester.InitServer(streamCaddyfile(1, fmt.Sprintf(`encode gzip
+	reverse_proxy 127.0.0.1:54331 {
+		%s
+	}`, tc.opts)), "caddyfile")
+
+			// not a WebSocket handshake, so encode is active for the request
+			tn := openTunnel(t, "/", "echo", "Accept-Encoding: gzip")
+			tn.echo(t, "through encode")
+			tn.echo(t, "and again")
+			_ = tn.conn.Close()
+
+			// responses that are not upgrades are still encoded
+			if tc.name == "default" {
+				req, _ := http.NewRequest(http.MethodGet, "http://localhost:9080/", nil)
+				req.Header.Set("Accept-Encoding", "gzip")
+				tester.AssertResponseCode(req, http.StatusOK)
+			}
+		})
+	}
+}
+
+func TestStreamWebSocketThroughEncode(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+	tester := caddytest.NewTester(t)
+	tester.InitServer(streamCaddyfile(1, `encode gzip
+	reverse_proxy 127.0.0.1:54331`), "caddyfile")
+
+	tn := openTunnel(t, "/", "websocket",
+		"Accept-Encoding: gzip",
+		"Sec-WebSocket-Version: 13",
+		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==")
+	tn.echo(t, "websocket through encode")
+}
+
+// RFC 8441: a WebSocket over an HTTP/2 stream opened with an extended CONNECT.
+func TestStreamExtendedConnectOverH2(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+	tester := caddytest.NewTester(t)
+
+	for _, tc := range []struct {
+		name string
+		opts string
+	}{
+		{name: "default"},
+		{name: "detached", opts: "stream_detached"},
+	} {
+		t.Run(tc.name, func(t *testing.T) {
+			tester.InitServer(fmt.Sprintf(`{
+	skip_install_trust
+	admin localhost:2999
+	http_port     9080
+	https_port    9443
+	grace_period  1ns
+}
+
+localhost:9443 {
+	reverse_proxy 127.0.0.1:54331 {
+		%s
+	}
+}`, tc.opts), "caddyfile")
+
+			c := openExtendedConnect(t, "/ws")
+			c.expectStatus(t, "200")
+			c.echo(t, "hello over h2")
+			c.echo(t, "and again")
+
+			// closing our side ends the stream
+			c.closeSend(t)
+			c.waitEnd(t)
+		})
+	}
+}
+
+// h2Event is something the server sent on the extended CONNECT stream.
+type h2Event struct {
+	noConnect bool // the server does not support extended CONNECT
+	status    string
+	data   []byte
+	end    bool
+	err    error
+}
+
+// h2Stream is a minimal HTTP/2 client with a single extended CONNECT stream
+// (RFC 8441), written with frames since the Go clients do not offer it.
+type h2Stream struct {
+	conn   net.Conn
+	mu     sync.Mutex // guards writes to fr
+	fr     *http2.Framer
+	events chan h2Event
+	buf    []byte // received data not consumed yet
+}
+
+func openExtendedConnect(t *testing.T, path string) *h2Stream {
+	t.Helper()
+	conn, err := tls.Dial("tcp", "127.0.0.1:9443", &tls.Config{ //nolint:gosec
+		InsecureSkipVerify: true,
+		NextProtos:         []string{"h2"},
+		ServerName:         "localhost",
+	})
+	if err != nil {
+		t.Fatalf("dialing caddy over TLS: %v", err)
+	}
+	t.Cleanup(func() { conn.Close() })
+
+	s := &h2Stream{conn: conn, fr: http2.NewFramer(conn, conn), events: make(chan h2Event, 64)}
+	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
+		t.Fatalf("writing the client preface: %v", err)
+	}
+	if err := s.fr.WriteSettings(); err != nil {
+		t.Fatalf("writing settings: %v", err)
+	}
+
+	var block bytes.Buffer
+	enc := hpack.NewEncoder(&block)
+	for _, field := range [][2]string{
+		{":method", "CONNECT"},
+		{":protocol", "websocket"},
+		{":scheme", "https"},
+		{":path", path},
+		{":authority", "localhost:9443"},
+		{"sec-websocket-version", "13"},
+	} {
+		if err := enc.WriteField(hpack.HeaderField{Name: field[0], Value: field[1]}); err != nil {
+			t.Fatalf("encoding header %s: %v", field[0], err)
+		}
+	}
+	if err := s.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block.Bytes(), EndHeaders: true}); err != nil {
+		t.Fatalf("writing the request: %v", err)
+	}
+
+	go s.readLoop()
+	return s
+}
+
+func (s *h2Stream) readLoop() {
+	dec := hpack.NewDecoder(4096, nil)
+	for {
+		frame, err := s.fr.ReadFrame()
+		if err != nil {
+			s.events <- h2Event{err: err}
+			return
+		}
+		switch f := frame.(type) {
+		case *http2.SettingsFrame:
+			if !f.IsAck() {
+				var connectProtocol bool
+				_ = f.ForeachSetting(func(setting http2.Setting) error {
+					if setting.ID == http2.SettingEnableConnectProtocol && setting.Val == 1 {
+						connectProtocol = true
+					}
+					return nil
+				})
+				if !connectProtocol {
+					s.events <- h2Event{noConnect: true}
+				}
+				s.mu.Lock()
+				_ = s.fr.WriteSettingsAck()
+				s.mu.Unlock()
+			}
+		case *http2.PingFrame:
+			if !f.IsAck() {
+				s.mu.Lock()
+				_ = s.fr.WritePing(true, f.Data)
+				s.mu.Unlock()
+			}
+		case *http2.HeadersFrame:
+			fields, err := dec.DecodeFull(f.HeaderBlockFragment())
+			if err != nil {
+				s.events <- h2Event{err: err}
+				return
+			}
+			ev := h2Event{end: f.StreamEnded()}
+			for _, field := range fields {
+				if field.Name == ":status" {
+					ev.status = field.Value
+				}
+			}
+			s.events <- ev
+		case *http2.DataFrame:
+			s.events <- h2Event{data: append([]byte(nil), f.Data()...), end: f.StreamEnded()}
+		case *http2.RSTStreamFrame:
+			s.events <- h2Event{err: fmt.Errorf("stream reset: %v", f.ErrCode)}
+			return
+		case *http2.GoAwayFrame:
+			s.events <- h2Event{err: fmt.Errorf("go away: %v", f.ErrCode)}
+			return
+		}
+	}
+}
+
+func (s *h2Stream) next(t *testing.T) h2Event {
+	t.Helper()
+	for {
+		select {
+		case ev := <-s.events:
+			if ev.noConnect {
+				t.Skip("the HTTP/2 server does not support extended CONNECT; it needs GODEBUG=http2xconnect=1")
+			}
+			if ev.err != nil {
+				t.Fatalf("HTTP/2 connection failed: %v", ev.err)
+			}
+			return ev
+		case <-time.After(streamWait):
+			t.Fatal("timed out waiting for the server")
+			return h2Event{}
+		}
+	}
+}
+
+func (s *h2Stream) expectStatus(t *testing.T, want string) {
+	t.Helper()
+	if ev := s.next(t); ev.status != want {
+		t.Fatalf("response status = %q, want %q", ev.status, want)
+	}
+}
+
+// echo sends msg on the stream and expects to get it back.
+func (s *h2Stream) echo(t *testing.T, msg string) {
+	t.Helper()
+	s.mu.Lock()
+	err := s.fr.WriteData(1, false, []byte(msg))
+	s.mu.Unlock()
+	if err != nil {
+		t.Fatalf("writing %q: %v", msg, err)
+	}
+	for len(s.buf) < len(msg) {
+		s.buf = append(s.buf, s.next(t).data...)
+	}
+	if got := string(s.buf[:len(msg)]); got != msg {
+		t.Fatalf("stream echoed %q, want %q", got, msg)
+	}
+	s.buf = s.buf[len(msg):]
+}
+
+func (s *h2Stream) closeSend(t *testing.T) {
+	t.Helper()
+	s.mu.Lock()
+	err := s.fr.WriteData(1, true, nil)
+	s.mu.Unlock()
+	if err != nil {
+		t.Fatalf("closing the stream: %v", err)
+	}
+}
+
+// waitEnd waits for the server to end the stream.
+func (s *h2Stream) waitEnd(t *testing.T) {
+	t.Helper()
+	for {
+		if ev := s.next(t); ev.end {
+			return
+		}
+	}
+}
+
+func TestStreamLoadRejectsInvalidLogLevel(t *testing.T) {
+	caddytest.AssertLoadError(t, `{
+	"admin": {"listen": "localhost:2999"},
+	"apps": {
+		"http": {
+			"http_port": 9080,
+			"servers": {
+				"srv0": {
+					"listen": [":9080"],
+					"routes": [{
+						"handle": [{
+							"handler": "reverse_proxy",
+							"upstreams": [{"dial": "127.0.0.1:54331"}],
+							"stream_logs": {"level": "loud"}
+						}]
+					}]
+				}
+			}
+		}
+	}
+}`, "json", "invalid stream_logs level")
+}
+
+// logEntries reads the JSON log entries written to path.
+func logEntries(t *testing.T, path string) []map[string]any {
+	t.Helper()
+	data, err := os.ReadFile(path)
+	if err != nil {
+		if os.IsNotExist(err) {
+			return nil
+		}
+		t.Fatalf("reading log: %v", err)
+	}
+	var entries []map[string]any
+	for line := range strings.SplitSeq(string(data), "\n") {
+		if strings.TrimSpace(line) == "" {
+			continue
+		}
+		var entry map[string]any
+		if err := json.Unmarshal([]byte(line), &entry); err != nil {
+			t.Fatalf("log line %q is not JSON: %v", line, err)
+		}
+		entries = append(entries, entry)
+	}
+	return entries
+}
+
+func findLogEntries(entries []map[string]any, msg string) []map[string]any {
+	var found []map[string]any
+	for _, e := range entries {
+		if e["msg"] == msg {
+			found = append(found, e)
+		}
+	}
+	return found
+}
+
+// waitForLog waits for an entry with the message to be written to the log at path.
+func waitForLog(t *testing.T, path, msg string) map[string]any {
+	t.Helper()
+	deadline := time.Now().Add(streamWait)
+	for {
+		if found := findLogEntries(logEntries(t, path), msg); len(found) > 0 {
+			return found[0]
+		}
+		if time.Now().After(deadline) {
+			t.Fatalf("no %q entry in the log: %v", msg, logEntries(t, path))
+		}
+		time.Sleep(50 * time.Millisecond)
+	}
+}
+
+// closeTunnel ends a tunnel from the client side, which also ends the backend's echo.
+func closeTunnel(tn *tunnel) { _ = tn.conn.Close() }
+
+func accessLogCaddyfile(gen int, logPath, streamOpts string) string {
+	return streamCaddyfile(gen, fmt.Sprintf(`log {
+		output file %s
+		format json
+		level DEBUG
+	}
+	reverse_proxy 127.0.0.1:54331 {
+		%s
+	}`, logPath, streamOpts))
+}
+
+func TestStreamLogsDefault(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+	tester := caddytest.NewTester(t)
+	logPath := filepath.ToSlash(filepath.Join(t.TempDir(), "access.log"))
+	tester.InitServer(accessLogCaddyfile(1, logPath, ""), "caddyfile")
+
+	tn := openTunnel(t, "/ws", "echo")
+	tn.echo(t, "ping")
+	closeTunnel(tn)
+
+	closed := waitForLog(t, logPath, "stream closed")
+	if closed["level"] != "debug" {
+		t.Errorf("stream close logged at %v, want the default, debug", closed["level"])
+	}
+	if got, _ := closed["bytes_to_backend"].(float64); got < 4 {
+		t.Errorf("bytes_to_backend = %v, want at least 4", closed["bytes_to_backend"])
+	}
+	if got, _ := closed["bytes_from_backend"].(float64); got < 4 {
+		t.Errorf("bytes_from_backend = %v, want at least 4", closed["bytes_from_backend"])
+	}
+	if closed["upstream"] != streamBackendA {
+		t.Errorf("upstream = %v, want %s", closed["upstream"], streamBackendA)
+	}
+	if closed["result"] == nil {
+		t.Errorf("no result in the stream close log: %v", closed)
+	}
+
+	// by default the handshake is logged
+	handshake := waitForLog(t, logPath, "handled request")
+	if status, _ := handshake["status"].(float64); status != http.StatusSwitchingProtocols {
+		t.Errorf("handshake logged with status %v, want 101", handshake["status"])
+	}
+}
+
+func TestStreamLogsSkipHandshakeAndLevel(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+	tester := caddytest.NewTester(t)
+	logPath := filepath.ToSlash(filepath.Join(t.TempDir(), "access.log"))
+	tester.InitServer(accessLogCaddyfile(1, logPath, `stream_logs {
+			skip_handshake
+			level warn
+		}`), "caddyfile")
+
+	tn := openTunnel(t, "/ws", "echo")
+	tn.echo(t, "ping")
+	closeTunnel(tn)
+
+	closed := waitForLog(t, logPath, "stream closed")
+	if closed["level"] != "warn" {
+		t.Errorf("stream close logged at %v, want warn", closed["level"])
+	}
+
+	// the handshake is written when the handler returns, which is after the stream closes
+	time.Sleep(time.Second)
+	if handshake := findLogEntries(logEntries(t, logPath), "handled request"); len(handshake) > 0 {
+		t.Errorf("the handshake was logged although skip_handshake is set: %v", handshake)
+	}
+
+	// requests that are not upgrades are logged as usual
+	tester.AssertGetResponse("http://localhost:9080/plain", 200, "plain response")
+	waitForLog(t, logPath, "handled request")
+}
+
+func TestStreamDetachedWithAccessLogging(t *testing.T) {
+	startUpgradeBackend(t, streamBackendA)
+	tester := caddytest.NewTester(t)
+	logPath := filepath.ToSlash(filepath.Join(t.TempDir(), "access.log"))
+	opts := `stream_detached
+		stream_logs {
+			level info
+		}`
+	tester.InitServer(accessLogCaddyfile(1, logPath, opts), "caddyfile")
+
+	tn := openTunnel(t, "/ws", "echo")
+	tn.echo(t, "before reload")
+
+	tester.InitServer(accessLogCaddyfile(2, logPath, opts), "caddyfile")
+	tn.echo(t, "after reload")
+	closeTunnel(tn)
+
+	// the stream outlived the config it was started with, and still logs
+	closed := waitForLog(t, logPath, "stream closed")
+	if closed["level"] != "info" {
+		t.Errorf("stream close logged at %v, want info", closed["level"])
+	}
+	if got, _ := closed["bytes_to_backend"].(float64); got < float64(len("before reload")+len("after reload")) {
+		t.Errorf("bytes_to_backend = %v, want the traffic from before and after the reload", closed["bytes_to_backend"])
+	}
+	waitForLog(t, logPath, "handled request")
+
+	// and a stream closed by a reload that removes its upstream is logged as well
+	tn2 := openTunnel(t, "/ws", "echo")
+	tn2.echo(t, "second")
+	tester.InitServer(streamCaddyfile(3, fmt.Sprintf("log {\n\t\toutput file %s\n\t\tformat json\n\t\tlevel DEBUG\n\t}\n\trespond \"gone\"", logPath)), "caddyfile")
+	tn2.waitClosed(t, streamWait)
+	deadline := time.Now().Add(streamWait)
+	for len(findLogEntries(logEntries(t, logPath), "stream closed")) < 2 {
+		if time.Now().After(deadline) {
+			t.Fatalf("the stream closed by the reload was not logged: %v", logEntries(t, logPath))
+		}
+		time.Sleep(50 * time.Millisecond)
+	}
+	var results []any
+	for _, e := range findLogEntries(logEntries(t, logPath), "stream closed") {
+		results = append(results, e["result"])
+	}
+	if results[len(results)-1] != "closed" {
+		t.Errorf("stream closed by the reload has result %v, want closed (all: %v)", results[len(results)-1], results)
+	}
+}
diff --git a/modules/caddyhttp/encode/encode.go b/modules/caddyhttp/encode/encode.go
index 46b383ea..9c997d85 100644
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
@@ -255,12 +257,37 @@ type responseWriter struct {
 	incremental  bool
 	isConnect    bool
 	disabled     bool // disable encoding for this response
+	hijacked     bool // the connection was taken over; nothing may be written anymore
+}
+
+// Hijack implements http.Hijacker. After a successful hijack the
+// connection no longer carries an HTTP response, so the writer
+// stops writing headers and bodies to the underlying writer.
+func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	//nolint:bodyclose
+	conn, brw, err := http.NewResponseController(rw.ResponseWriter).Hijack()
+	if err != nil {
+		return nil, nil, err
+	}
+	rw.hijacked = true
+	rw.wroteHeader = true
+	return conn, brw, nil
 }
 
 // WriteHeader stores the status to write when the time comes
-// to actually write the header.
+// to actually write the header. A 101 (Switching Protocols) is
+// the final response of an upgrade, so it is written right away
+// and never encoded.
 func (rw *responseWriter) WriteHeader(status int) {
+	if rw.hijacked {
+		return
+	}
 	rw.statusCode = status
+	if status == http.StatusSwitchingProtocols {
+		rw.ResponseWriter.WriteHeader(status)
+		rw.wroteHeader = true
+		return
+	}
 	if status == http.StatusPartialContent {
 		rw.disabled = true // partial representations must not be dynamically re-encoded
 	}
@@ -332,6 +359,9 @@ func (enc *Encode) Match(rw *responseWriter) bool {
 // FlushError is an alternative Flush returning an error. It delays the actual Flush of the underlying
 // ResponseWriterWrapper until headers were written.
 func (rw *responseWriter) FlushError() error {
+	if rw.hijacked {
+		return nil
+	}
 	if !rw.wroteHeader && rw.statusCode == 0 && caddyhttp.IsIncremental(rw.Header()) {
 		rw.WriteHeader(http.StatusOK)
 	}
@@ -365,6 +395,10 @@ func (rw *responseWriter) FlushError() error {
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
@@ -438,6 +472,9 @@ const sniffLen = 512
 // It's based on the standard library HTTP/1.1 response writer implementation.
 // https://github.com/golang/go/blob/f4e3ec3dbe3b8e04a058d266adf8e048bab563f2/src/net/http/server.go#L586
 func (rw *responseWriter) ReadFrom(r io.Reader) (int64, error) {
+	if rw.hijacked {
+		return 0, http.ErrHijacked
+	}
 	if rw.incremental || !rw.wroteHeader && caddyhttp.IsIncremental(rw.Header()) {
 		return io.Copy(writerOnly{rw}, r)
 	}
@@ -491,7 +528,10 @@ func (rw *responseWriter) Close() error {
 
 	var err error
 	if rw.w != nil {
-		err = rw.w.Close()
+		// the encoder's trailer cannot go anywhere on a hijacked connection
+		if !rw.hijacked {
+			err = rw.w.Close()
+		}
 		rw.w.Reset(nil)
 		rw.config.writerPools[rw.encodingName].Put(rw.w)
 		rw.w = nil
diff --git a/modules/caddyhttp/encode/encode_conformance_test.go b/modules/caddyhttp/encode/encode_conformance_test.go
index 971d5eb5..c37956e4 100644
--- a/modules/caddyhttp/encode/encode_conformance_test.go
+++ b/modules/caddyhttp/encode/encode_conformance_test.go
@@ -1,8 +1,12 @@
 package encode_test
 
 import (
+	"bufio"
 	"bytes"
+	"errors"
 	"fmt"
+	"io"
+	"net"
 	"net/http"
 	"net/http/httptest"
 	"slices"
@@ -16,6 +20,170 @@ import (
 
 const conformanceContentType = "text/plain"
 
+// hijackableRecorder is a response writer that can be hijacked, and
+// that notes everything written to it after that, which is an error.
+type hijackableRecorder struct {
+	*httptest.ResponseRecorder
+	hijackErr      error
+	hijacked       bool
+	headers        []int // status codes written, in order
+	writesAfterHij int
+	peer           net.Conn
+}
+
+func (h *hijackableRecorder) WriteHeader(code int) {
+	if h.hijacked {
+		h.writesAfterHij++
+	}
+	h.headers = append(h.headers, code)
+	h.ResponseRecorder.WriteHeader(code)
+}
+
+func (h *hijackableRecorder) Write(p []byte) (int, error) {
+	if h.hijacked {
+		h.writesAfterHij++
+	}
+	return h.ResponseRecorder.Write(p)
+}
+
+func (h *hijackableRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	if h.hijackErr != nil {
+		return nil, nil, h.hijackErr
+	}
+	h.hijacked = true
+	conn, peer := net.Pipe()
+	h.peer = peer
+	return conn, bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)), nil
+}
+
+// An upgrade works through encode: the 101 is final and is written as is,
+// and once the connection is hijacked nothing is written to the response.
+func TestEncodeUpgradeAndHijack(t *testing.T) {
+	for _, encCase := range standardEncoderCases(t) {
+		t.Run(encCase.name, func(t *testing.T) {
+			enc := newEncodeHandler(t, encCase, 1)
+
+			w := &hijackableRecorder{ResponseRecorder: httptest.NewRecorder()}
+			r := httptest.NewRequest(http.MethodGet, "/ws", nil)
+			r.Header.Set("Accept-Encoding", encCase.encoding.AcceptEncoding())
+			r.Header.Set("Connection", "Upgrade")
+			r.Header.Set("Upgrade", "websocket")
+
+			var hijackErr error
+			var afterHijack struct {
+				n        int
+				err      error
+				readFrom error
+				flush    error
+			}
+			next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
+				w.Header().Set("Content-Type", "text/plain")
+				w.Header().Set("Upgrade", "websocket")
+				w.Header().Set("Connection", "Upgrade")
+				w.WriteHeader(http.StatusSwitchingProtocols)
+
+				conn, _, err := http.NewResponseController(w).Hijack()
+				hijackErr = err
+				if err != nil {
+					return nil
+				}
+				defer conn.Close()
+
+				// all of this must be refused or ignored
+				w.WriteHeader(http.StatusOK)
+				afterHijack.n, afterHijack.err = w.Write([]byte(strings.Repeat("data ", 100)))
+				_, afterHijack.readFrom = w.(io.ReaderFrom).ReadFrom(strings.NewReader("more"))
+				afterHijack.flush = http.NewResponseController(w).Flush()
+				return nil
+			})
+			if err := enc.ServeHTTP(w, r, next); err != nil {
+				t.Fatal(err)
+			}
+
+			if hijackErr != nil {
+				t.Fatalf("hijack failed: %v", hijackErr)
+			}
+			if !slices.Equal(w.headers, []int{http.StatusSwitchingProtocols}) {
+				t.Errorf("status codes written = %v, want only 101", w.headers)
+			}
+			if got := w.Header().Get("Content-Encoding"); got != "" {
+				t.Errorf("a 101 was encoded: Content-Encoding = %q", got)
+			}
+			if w.writesAfterHij != 0 {
+				t.Errorf("%d writes reached the response after the hijack", w.writesAfterHij)
+			}
+			if w.Body.Len() != 0 {
+				t.Errorf("body = %q, want empty", w.Body.String())
+			}
+			if afterHijack.n != 0 || !errors.Is(afterHijack.err, http.ErrHijacked) {
+				t.Errorf("Write after hijack = %d, %v; want 0, %v", afterHijack.n, afterHijack.err, http.ErrHijacked)
+			}
+			if !errors.Is(afterHijack.readFrom, http.ErrHijacked) {
+				t.Errorf("ReadFrom after hijack = %v, want %v", afterHijack.readFrom, http.ErrHijacked)
+			}
+			if afterHijack.flush != nil {
+				t.Errorf("Flush after hijack = %v, want nil", afterHijack.flush)
+			}
+		})
+	}
+}
+
+// A hijack that fails leaves the writer usable.
+func TestEncodeFailedHijack(t *testing.T) {
+	encCase := standardEncoderCases(t)[0]
+	enc := newEncodeHandler(t, encCase, 1)
+
+	hijackFailure := errors.New("hijack failed")
+	w := &hijackableRecorder{ResponseRecorder: httptest.NewRecorder(), hijackErr: hijackFailure}
+	r := httptest.NewRequest(http.MethodGet, "/", nil)
+	r.Header.Set("Accept-Encoding", encCase.encoding.AcceptEncoding())
+
+	var hijackErr error
+	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
+		_, _, hijackErr = http.NewResponseController(w).Hijack()
+		w.Header().Set("Content-Type", conformanceContentType)
+		_, err := w.Write([]byte("still writable"))
+		return err
+	})
+	if err := enc.ServeHTTP(w, r, next); err != nil {
+		t.Fatal(err)
+	}
+	if !errors.Is(hijackErr, hijackFailure) {
+		t.Fatalf("hijack error = %v, want %v", hijackErr, hijackFailure)
+	}
+	if w.Code != http.StatusOK || w.Body.Len() == 0 {
+		t.Errorf("response after failed hijack: %d, %d body bytes", w.Code, w.Body.Len())
+	}
+}
+
+// A hijack after the response started to be encoded does not try to finish the encoded stream.
+func TestEncodeHijackAfterEncodingStarted(t *testing.T) {
+	encCase := standardEncoderCases(t)[0]
+	enc := newEncodeHandler(t, encCase, 1)
+
+	w := &hijackableRecorder{ResponseRecorder: httptest.NewRecorder()}
+	r := httptest.NewRequest(http.MethodGet, "/", nil)
+	r.Header.Set("Accept-Encoding", encCase.encoding.AcceptEncoding())
+
+	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
+		w.Header().Set("Content-Type", conformanceContentType)
+		if _, err := w.Write([]byte(strings.Repeat("compress me ", 1000))); err != nil {
+			return err
+		}
+		conn, _, err := http.NewResponseController(w).Hijack()
+		if err != nil {
+			return err
+		}
+		return conn.Close()
+	})
+	if err := enc.ServeHTTP(w, r, next); err != nil {
+		t.Fatal(err)
+	}
+	if w.writesAfterHij != 0 {
+		t.Errorf("%d writes reached the response after the hijack", w.writesAfterHij)
+	}
+}
+
 func TestEncodeIfNoneMatch(t *testing.T) {
 	for _, encCase := range standardEncoderCases(t) {
 		t.Run(encCase.name, func(t *testing.T) {
diff --git a/modules/caddyhttp/intercept/intercept_test.go b/modules/caddyhttp/intercept/intercept_test.go
new file mode 100644
index 00000000..7dfc4d68
--- /dev/null
+++ b/modules/caddyhttp/intercept/intercept_test.go
@@ -0,0 +1,138 @@
+package intercept
+
+import (
+	"bufio"
+	"context"
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
+// hijackSpyWriter is a response writer that can be hijacked and notes
+// what is written to it, and whether that was after the hijack.
+type hijackSpyWriter struct {
+	*httptest.ResponseRecorder
+	hijacked    bool
+	statuses    []int
+	afterHijack int
+}
+
+func (h *hijackSpyWriter) WriteHeader(code int) {
+	if h.hijacked {
+		h.afterHijack++
+	}
+	h.statuses = append(h.statuses, code)
+	h.ResponseRecorder.WriteHeader(code)
+}
+
+func (h *hijackSpyWriter) Write(p []byte) (int, error) {
+	if h.hijacked {
+		h.afterHijack++
+	}
+	return h.ResponseRecorder.Write(p)
+}
+
+func (h *hijackSpyWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	h.hijacked = true
+	conn, _ := net.Pipe()
+	return conn, bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)), nil
+}
+
+func newInterceptRequest() *http.Request {
+	r := httptest.NewRequest(http.MethodGet, "/ws", nil)
+	r.Header.Set("Connection", "Upgrade")
+	r.Header.Set("Upgrade", "websocket")
+	return r.WithContext(context.WithValue(r.Context(), caddy.ReplacerCtxKey, caddy.NewReplacer()))
+}
+
+// A response handler that matches every response, like a bare handle_response.
+func newCatchAllIntercept() Intercept {
+	return Intercept{
+		HandleResponse: []caddyhttp.ResponseHandler{{}},
+		logger:         zap.NewNop(),
+	}
+}
+
+// An upgrade is the final response: it is not intercepted, and once
+// the connection is hijacked nothing more is written to the response.
+func TestInterceptUpgradeIsNotIntercepted(t *testing.T) {
+	w := &hijackSpyWriter{ResponseRecorder: httptest.NewRecorder()}
+
+	var hijackErr error
+	err := newCatchAllIntercept().ServeHTTP(w, newInterceptRequest(), caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
+		w.Header().Set("Upgrade", "websocket")
+		w.Header().Set("Connection", "Upgrade")
+		w.WriteHeader(http.StatusSwitchingProtocols)
+		conn, _, err := http.NewResponseController(w).Hijack()
+		hijackErr = err
+		if err == nil {
+			conn.Close()
+		}
+		return nil
+	}))
+	if err != nil {
+		t.Fatalf("ServeHTTP() = %v", err)
+	}
+	if hijackErr != nil {
+		t.Fatalf("hijack through intercept failed: %v", hijackErr)
+	}
+	if len(w.statuses) != 1 || w.statuses[0] != http.StatusSwitchingProtocols {
+		t.Errorf("statuses written = %v, want [101]", w.statuses)
+	}
+	if w.afterHijack != 0 {
+		t.Errorf("%d writes reached the response after the hijack", w.afterHijack)
+	}
+	if w.Body.Len() != 0 {
+		t.Errorf("body = %q, want empty", w.Body.String())
+	}
+}
+
+// A connection hijacked without a response ever being written is not
+// a response to intercept either.
+func TestInterceptHijackWithoutResponse(t *testing.T) {
+	w := &hijackSpyWriter{ResponseRecorder: httptest.NewRecorder()}
+
+	err := newCatchAllIntercept().ServeHTTP(w, newInterceptRequest(), caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
+		conn, _, err := http.NewResponseController(w).Hijack()
+		if err != nil {
+			return err
+		}
+		return conn.Close()
+	}))
+	if err != nil {
+		t.Fatalf("ServeHTTP() = %v", err)
+	}
+	if len(w.statuses) != 0 || w.afterHijack != 0 {
+		t.Errorf("response written after hijack: statuses=%v, writes after hijack=%d", w.statuses, w.afterHijack)
+	}
+}
+
+// Regular responses are still intercepted.
+func TestInterceptStillBuffersRegularResponses(t *testing.T) {
+	ir := Intercept{
+		HandleResponse: []caddyhttp.ResponseHandler{{StatusCode: "418"}},
+		logger:         zap.NewNop(),
+	}
+	w := httptest.NewRecorder()
+
+	err := ir.ServeHTTP(w, newInterceptRequest(), caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
+		w.WriteHeader(http.StatusOK)
+		_, err := w.Write([]byte("original"))
+		return err
+	}))
+	if err != nil {
+		t.Fatalf("ServeHTTP() = %v", err)
+	}
+	if w.Code != http.StatusTeapot {
+		t.Errorf("status = %d, want %d", w.Code, http.StatusTeapot)
+	}
+	if got := w.Body.String(); got != "original" {
+		t.Errorf("body = %q, want %q", got, "original")
+	}
+}
diff --git a/modules/caddyhttp/responsewriter.go b/modules/caddyhttp/responsewriter.go
index 904c30c0..d1e6d83a 100644
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
+// 1xx responses are never buffered, except 101 (Switching Protocols),
+// which is the final response of an upgrade: it is never buffered and
+// nothing else can be written after it. Once the connection was hijacked
+// nothing is written to the underlying writer anymore.
 func (rr *responseRecorder) WriteHeader(statusCode int) {
-	if rr.wroteHeader {
+	if rr.wroteHeader || rr.hijacked {
 		return
 	}
 
@@ -154,6 +158,14 @@ func (rr *responseRecorder) WriteHeader(statusCode int) {
 	// connections by manually setting headers and writing status 101
 	rr.statusCode = statusCode
 
+	// a 101 is final, and the response that follows is not an HTTP one
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
@@ -218,11 +236,15 @@ func (rr *responseRecorder) Buffer() *bytes.Buffer {
 }
 
 // Buffered returns whether rr has decided to buffer the response.
+// A hijacked connection has no response to buffer.
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
@@ -239,6 +261,9 @@ func (rr *responseRecorder) WriteResponse() error {
 // FlushError will suppress actual flushing if the response is buffered. See:
 // https://github.com/caddyserver/caddy/issues/6144
 func (rr *responseRecorder) FlushError() error {
+	if rr.hijacked {
+		return nil
+	}
 	if rr.stream {
 		//nolint:bodyclose
 		return http.NewResponseController(rr.ResponseWriterWrapper).Flush()
@@ -258,6 +283,7 @@ func (rr *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
 	if err != nil {
 		return nil, nil, err
 	}
+	rr.hijacked = true
 	// Per http documentation, returned bufio.Writer is empty, but bufio.Read maybe not
 	conn = &hijackedConn{conn, rr}
 	brw.Writer.Reset(conn)
@@ -281,6 +307,14 @@ type hijackedConn struct {
 	rr *responseRecorder
 }
 
+// UnwrapHijacked returns the underlying connection, which does not
+// account for the traffic in the response recorder. Streams that
+// copy bytes on the raw connection (such as a proxied upgrade)
+// should use it instead of the wrapper.
+func (hc *hijackedConn) UnwrapHijacked() net.Conn {
+	return hc.Conn
+}
+
 func (hc *hijackedConn) updateReadSize(n int) {
 	if hc.rr.readSize != nil {
 		*hc.rr.readSize += n
diff --git a/modules/caddyhttp/responsewriter_test.go b/modules/caddyhttp/responsewriter_test.go
index c08ad26a..4fca980f 100644
--- a/modules/caddyhttp/responsewriter_test.go
+++ b/modules/caddyhttp/responsewriter_test.go
@@ -1,13 +1,247 @@
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
+	"go.uber.org/zap/zapcore"
 )
 
+func TestServerLogStream(t *testing.T) {
+	newReq := func(vars map[string]any) *http.Request {
+		r := httptest.NewRequest(http.MethodGet, "http://example.com/ws", nil)
+		if vars != nil {
+			r = r.WithContext(context.WithValue(r.Context(), VarsCtxKey, vars))
+		}
+		return r
+	}
+
+	t.Run("access logging disabled", func(t *testing.T) {
+		s := &Server{}
+		if s.LogStream(newReq(nil), zapcore.DebugLevel, "stream closed") {
+			t.Error("LogStream() = true on a server without access logs")
+		}
+	})
+
+	t.Run("skipped host", func(t *testing.T) {
+		var buf syncBuffer
+		s := &Server{accessLogger: testLogger(buf.Write), Logs: &ServerLogConfig{SkipHosts: []string{"example.com"}}}
+		if s.LogStream(newReq(nil), zapcore.DebugLevel, "stream closed") {
+			t.Error("LogStream() = true for a host that is skipped")
+		}
+		if buf.String() != "" {
+			t.Errorf("logged: %s", buf.String())
+		}
+	})
+
+	t.Run("logged at the given level", func(t *testing.T) {
+		var buf syncBuffer
+		s := &Server{accessLogger: testLogger(buf.Write), Logs: &ServerLogConfig{}}
+		if !s.LogStream(newReq(nil), zapcore.WarnLevel, "stream closed", zap.String("result", "completed")) {
+			t.Fatal("LogStream() = false")
+		}
+		out := buf.String()
+		for _, want := range []string{`"level":"warn"`, `"msg":"stream closed"`, `"result":"completed"`} {
+			if !strings.Contains(out, want) {
+				t.Errorf("log %s does not contain %s", out, want)
+			}
+		}
+	})
+
+	t.Run("handshake skipped from the access log", func(t *testing.T) {
+		var buf syncBuffer
+		s := &Server{accessLogger: testLogger(buf.Write), Logs: &ServerLogConfig{}}
+		skipped := newReq(map[string]any{LogSkipVar: true})
+		if !s.LogStream(skipped, zapcore.DebugLevel, "stream closed") {
+			t.Fatal("LogStream() = false for a request flagged to skip its access log entry")
+		}
+		if !strings.Contains(buf.String(), `"msg":"stream closed"`) {
+			t.Errorf("stream close not logged: %q", buf.String())
+		}
+	})
+}
+
+// hijackSpyWriter is a response writer that can be hijacked and notes
+// the header and body writes that reach it, and whether they came
+// after the hijack.
+type hijackSpyWriter struct {
+	header      http.Header
+	hijackErr   error
+	hijacked    bool
+	statuses    []int
+	body        bytes.Buffer
+	afterHijack int
+	flushes     int
+	raw, peer   net.Conn
+}
+
+func newHijackSpyWriter() *hijackSpyWriter {
+	return &hijackSpyWriter{header: make(http.Header)}
+}
+
+func (h *hijackSpyWriter) Header() http.Header { return h.header }
+
+func (h *hijackSpyWriter) WriteHeader(code int) {
+	if h.hijacked {
+		h.afterHijack++
+	}
+	h.statuses = append(h.statuses, code)
+}
+
+func (h *hijackSpyWriter) Write(p []byte) (int, error) {
+	if h.hijacked {
+		h.afterHijack++
+	}
+	return h.body.Write(p)
+}
+
+func (h *hijackSpyWriter) Flush() { h.flushes++ }
+
+func (h *hijackSpyWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
+	if h.hijackErr != nil {
+		return nil, nil, h.hijackErr
+	}
+	h.hijacked = true
+	h.raw, h.peer = net.Pipe()
+	return h.raw, bufio.NewReadWriter(bufio.NewReader(h.raw), bufio.NewWriter(h.raw)), nil
+}
+
+func TestResponseRecorderSwitchingProtocolsIsFinal(t *testing.T) {
+	for _, buffering := range []bool{false, true} {
+		w := newHijackSpyWriter()
+		rr := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool { return buffering })
+
+		rr.WriteHeader(http.StatusSwitchingProtocols)
+		// nothing can follow the final response
+		rr.WriteHeader(http.StatusOK)
+
+		if rr.Buffered() {
+			t.Errorf("buffering=%v: a 101 was buffered", buffering)
+		}
+		if got := rr.Status(); got != http.StatusSwitchingProtocols {
+			t.Errorf("buffering=%v: status = %d, want 101", buffering, got)
+		}
+		if len(w.statuses) != 1 || w.statuses[0] != http.StatusSwitchingProtocols {
+			t.Errorf("buffering=%v: statuses written = %v, want [101]", buffering, w.statuses)
+		}
+		if err := rr.WriteResponse(); err != nil {
+			t.Errorf("buffering=%v: WriteResponse() = %v", buffering, err)
+		}
+		if len(w.statuses) != 1 {
+			t.Errorf("buffering=%v: WriteResponse() wrote again: %v", buffering, w.statuses)
+		}
+	}
+}
+
+func TestResponseRecorderNoWritesAfterHijack(t *testing.T) {
+	w := newHijackSpyWriter()
+	buf := new(bytes.Buffer)
+	// asks for buffering, which a hijacked connection can't do
+	rr := NewResponseRecorder(w, buf, func(int, http.Header) bool { return true })
+	var readSize int
+	rr.(*responseRecorder).setReadSize(&readSize)
+
+	conn, brw, err := http.NewResponseController(rr).Hijack()
+	if err != nil {
+		t.Fatalf("Hijack() = %v", err)
+	}
+	if conn == nil || brw == nil {
+		t.Fatal("Hijack() returned no connection")
+	}
+	defer conn.Close()
+
+	if rr.Buffered() {
+		t.Error("a hijacked recorder reports buffering")
+	}
+
+	rr.WriteHeader(http.StatusOK)
+	if n, err := rr.Write([]byte("body")); n != 0 || !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("Write() = %d, %v; want 0, %v", n, err, http.ErrHijacked)
+	}
+	if n, err := rr.(io.ReaderFrom).ReadFrom(strings.NewReader("body")); n != 0 || !errors.Is(err, http.ErrHijacked) {
+		t.Errorf("ReadFrom() = %d, %v; want 0, %v", n, err, http.ErrHijacked)
+	}
+	if err := rr.WriteResponse(); err != nil {
+		t.Errorf("WriteResponse() = %v, want nil", err)
+	}
+	if err := http.NewResponseController(rr).Flush(); err != nil {
+		t.Errorf("Flush() = %v, want nil", err)
+	}
+
+	if w.afterHijack != 0 {
+		t.Errorf("%d header or body writes reached the response after the hijack", w.afterHijack)
+	}
+	if len(w.statuses) != 0 || w.body.Len() != 0 || buf.Len() != 0 {
+		t.Errorf("something was written: statuses=%v body=%q buffer=%q", w.statuses, w.body.String(), buf.String())
+	}
+	if w.flushes != 0 {
+		t.Errorf("%d flushes reached the response after the hijack", w.flushes)
+	}
+	if rr.Size() != 0 {
+		t.Errorf("Size() = %d, want 0", rr.Size())
+	}
+}
+
+func TestResponseRecorderFailedHijack(t *testing.T) {
+	w := newHijackSpyWriter()
+	w.hijackErr = errors.New("cannot hijack")
+	rr := NewResponseRecorder(w, nil, nil)
+
+	if _, _, err := http.NewResponseController(rr).Hijack(); !errors.Is(err, w.hijackErr) {
+		t.Fatalf("Hijack() = %v, want %v", err, w.hijackErr)
+	}
+
+	// the response is still usable
+	if _, err := rr.Write([]byte("body")); err != nil {
+		t.Errorf("Write() after a failed hijack = %v", err)
+	}
+	if w.body.String() != "body" {
+		t.Errorf("body = %q, want %q", w.body.String(), "body")
+	}
+}
+
+// The wrapper that tracks the traffic of a hijacked connection can be
+// bypassed by streams that copy bytes themselves.
+func TestHijackedConnUnwrap(t *testing.T) {
+	w := newHijackSpyWriter()
+	rr := NewResponseRecorder(w, nil, nil)
+
+	conn, _, err := http.NewResponseController(rr).Hijack()
+	if err != nil {
+		t.Fatalf("Hijack() = %v", err)
+	}
+	defer conn.Close()
+
+	unwrapper, ok := conn.(interface{ UnwrapHijacked() net.Conn })
+	if !ok {
+		t.Fatalf("hijacked conn %T has no UnwrapHijacked()", conn)
+	}
+	if got := unwrapper.UnwrapHijacked(); got != w.raw {
+		t.Errorf("UnwrapHijacked() = %v, want the connection that was hijacked", got)
+	}
+
+	// writing on the raw connection is not accounted by the recorder...
+	go func() {
+		buf := make([]byte, 5)
+		_, _ = io.ReadFull(w.peer, buf)
+	}()
+	if _, err := w.raw.Write([]byte("hello")); err != nil {
+		t.Fatalf("raw write: %v", err)
+	}
+	if rr.Size() != 0 {
+		t.Errorf("Size() = %d after a raw write, want 0", rr.Size())
+	}
+}
+
 type responseWriterSpy interface {
 	http.ResponseWriter
 	Written() string
diff --git a/modules/caddyhttp/reverseproxy/caddyfile.go b/modules/caddyhttp/reverseproxy/caddyfile.go
index 12c1177b..25e63bae 100644
--- a/modules/caddyhttp/reverseproxy/caddyfile.go
+++ b/modules/caddyhttp/reverseproxy/caddyfile.go
@@ -23,6 +23,7 @@ import (
 	"strings"
 
 	"github.com/dustin/go-humanize"
+	"go.uber.org/zap/zapcore"
 
 	"github.com/caddyserver/caddy/v2"
 	"github.com/caddyserver/caddy/v2/caddyconfig"
@@ -99,6 +100,11 @@ func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error)
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
@@ -715,6 +721,43 @@ func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
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
+					level := strings.ToLower(d.Val())
+					if err := new(zapcore.Level).UnmarshalText([]byte(level)); err != nil {
+						return d.Errf("invalid stream log level '%s': %v", d.Val(), err)
+					}
+					if d.NextArg() {
+						return d.ArgErr()
+					}
+					h.StreamLogs.Level = level
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
index 00000000..43d78038
--- /dev/null
+++ b/modules/caddyhttp/reverseproxy/caddyfile_test.go
@@ -0,0 +1,165 @@
+package reverseproxy
+
+import (
+	"encoding/json"
+	"testing"
+
+	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
+)
+
+func TestUnmarshalCaddyfileStreamOptions(t *testing.T) {
+	for i, tc := range []struct {
+		input        string
+		wantDetached bool
+		wantLogs     *StreamLogs
+		wantErr      bool
+	}{
+		{input: `reverse_proxy localhost:8080`},
+		{
+			input: `reverse_proxy localhost:8080 {
+				stream_detached
+			}`,
+			wantDetached: true,
+		},
+		{
+			input: `reverse_proxy localhost:8080 {
+				stream_logs {
+					skip_handshake
+				}
+			}`,
+			wantLogs: &StreamLogs{SkipHandshake: true},
+		},
+		{
+			input: `reverse_proxy localhost:8080 {
+				stream_logs {
+					level INFO
+				}
+			}`,
+			wantLogs: &StreamLogs{Level: "info"},
+		},
+		{
+			input: `reverse_proxy localhost:8080 {
+				stream_detached
+				stream_logs {
+					skip_handshake
+					level warn
+				}
+			}`,
+			wantDetached: true,
+			wantLogs:     &StreamLogs{SkipHandshake: true, Level: "warn"},
+		},
+		{
+			// an empty block is accepted and leaves the defaults
+			input: `reverse_proxy localhost:8080 {
+				stream_logs
+			}`,
+			wantLogs: &StreamLogs{},
+		},
+		{
+			input: `reverse_proxy localhost:8080 {
+				stream_detached true
+			}`,
+			wantErr: true,
+		},
+		{
+			input: `reverse_proxy localhost:8080 {
+				stream_logs extra {
+					skip_handshake
+				}
+			}`,
+			wantErr: true,
+		},
+		{
+			input: `reverse_proxy localhost:8080 {
+				stream_logs {
+					skip_handshake true
+				}
+			}`,
+			wantErr: true,
+		},
+		{
+			input: `reverse_proxy localhost:8080 {
+				stream_logs {
+					level
+				}
+			}`,
+			wantErr: true,
+		},
+		{
+			input: `reverse_proxy localhost:8080 {
+				stream_logs {
+					level loud
+				}
+			}`,
+			wantErr: true,
+		},
+		{
+			input: `reverse_proxy localhost:8080 {
+				stream_logs {
+					level info debug
+				}
+			}`,
+			wantErr: true,
+		},
+		{
+			input: `reverse_proxy localhost:8080 {
+				stream_logs {
+					bogus
+				}
+			}`,
+			wantErr: true,
+		},
+	} {
+		var h Handler
+		err := h.UnmarshalCaddyfile(caddyfile.NewTestDispenser(tc.input))
+		if tc.wantErr {
+			if err == nil {
+				t.Errorf("Test %d: expected error but got none", i)
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
+		switch {
+		case tc.wantLogs == nil && h.StreamLogs != nil:
+			t.Errorf("Test %d: StreamLogs = %+v, want nil", i, *h.StreamLogs)
+		case tc.wantLogs != nil && h.StreamLogs == nil:
+			t.Errorf("Test %d: StreamLogs = nil, want %+v", i, *tc.wantLogs)
+		case tc.wantLogs != nil && *h.StreamLogs != *tc.wantLogs:
+			t.Errorf("Test %d: StreamLogs = %+v, want %+v", i, *h.StreamLogs, *tc.wantLogs)
+		}
+	}
+}
+
+func TestStreamOptionsJSONOmitsDefaults(t *testing.T) {
+	b, err := json.Marshal(Handler{})
+	if err != nil {
+		t.Fatal(err)
+	}
+	var got map[string]any
+	if err := json.Unmarshal(b, &got); err != nil {
+		t.Fatal(err)
+	}
+	for _, key := range []string{"stream_detached", "stream_logs"} {
+		if _, ok := got[key]; ok {
+			t.Errorf("default config marshals %q: %s", key, b)
+		}
+	}
+
+	b, err = json.Marshal(Handler{StreamDetached: true, StreamLogs: &StreamLogs{SkipHandshake: true, Level: "info"}})
+	if err != nil {
+		t.Fatal(err)
+	}
+	var h Handler
+	if err := json.Unmarshal(b, &h); err != nil {
+		t.Fatal(err)
+	}
+	if !h.StreamDetached || h.StreamLogs == nil || !h.StreamLogs.SkipHandshake || h.StreamLogs.Level != "info" {
+		t.Errorf("JSON round trip lost the stream options: %s", b)
+	}
+}
diff --git a/modules/caddyhttp/reverseproxy/metrics.go b/modules/caddyhttp/reverseproxy/metrics.go
index 24884273..16507255 100644
--- a/modules/caddyhttp/reverseproxy/metrics.go
+++ b/modules/caddyhttp/reverseproxy/metrics.go
@@ -43,9 +43,66 @@ func initReverseProxyMetrics(handler *Handler, registry *prometheus.Registry) {
 		panic(err)
 	}
 
+	for _, c := range []prometheus.Collector{
+		streamMetrics.active,
+		streamMetrics.closed,
+		streamMetrics.duration,
+		streamMetrics.bytes,
+	} {
+		// the same collectors are registered again on every reload; that is expected
+		if err := registry.Register(c); err != nil {
+			if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
+				panic(err)
+			}
+		}
+	}
+
 	reverseProxyMetrics.logger = handler.logger.Named("reverse_proxy.metrics")
 }
 
+// streamMetrics holds the metrics of upgraded streams. The collectors
+// are created up front, so streams can always be accounted for, and
+// registered with the metrics registry when a handler is provisioned.
+var streamMetrics = newStreamMetricSet()
+
+type streamMetricSet struct {
+	active   prometheus.Gauge
+	closed   *prometheus.CounterVec
+	duration prometheus.Histogram
+	bytes    *prometheus.CounterVec
+}
+
+func newStreamMetricSet() *streamMetricSet {
+	const ns, sub = "caddy", "reverse_proxy"
+	return &streamMetricSet{
+		active: prometheus.NewGauge(prometheus.GaugeOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "streams_active",
+			Help:      "Number of upgraded streams (such as WebSockets) currently open.",
+		}),
+		closed: prometheus.NewCounterVec(prometheus.CounterOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "streams_closed_total",
+			Help:      "Counter of upgraded streams that were closed, by result.",
+		}, []string{"result"}),
+		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "stream_duration_seconds",
+			Help:      "Histogram of the duration of upgraded streams.",
+			Buckets:   []float64{0.1, 1, 10, 60, 300, 1800, 3600, 86400},
+		}),
+		bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
+			Namespace: ns,
+			Subsystem: sub,
+			Name:      "stream_bytes_total",
+			Help:      "Counter of bytes copied through upgraded streams, by direction relative to the backend.",
+		}, []string{"direction"}),
+	}
+}
+
 type metricsUpstreamsHealthyUpdate
... (truncated; 50694 more characters; read the files)
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
