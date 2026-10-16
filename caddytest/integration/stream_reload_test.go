package integration

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

	"github.com/caddyserver/caddy/v2/caddytest"
)

const (
	streamAddrA = "localhost:54321"
	streamAddrB = "localhost:54322"

	// set in the process TestExtendedConnectH2 runs the actual test in
	extendedConnectChildEnv = "CADDY_TEST_EXTENDED_CONNECT"
)

// streamBackend is a backend accepting any upgrade (WebSocket or not),
// that echoes what it receives on the upgraded connection until the client
// closes it. Other requests get a plain response.
type streamBackend struct {
	srv *http.Server
}

func startStreamBackend(t *testing.T, dial string) *streamBackend {
	t.Helper()

	ln, err := net.Listen("tcp", strings.Replace(dial, "localhost", "127.0.0.1", 1))
	if err != nil {
		t.Fatalf("backend listen: %v", err)
	}
	b := &streamBackend{srv: &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			proto := r.Header.Get("Upgrade")
			if proto == "" {
				// chunked, so that a handler replacing the body doesn't have to fix the length
				_, _ = io.WriteString(w, "plain")
				_ = http.NewResponseController(w).Flush()
				return
			}
			conn, brw, err := http.NewResponseController(w).Hijack()
			if err != nil {
				t.Errorf("backend hijack: %v", err)
				return
			}
			defer conn.Close()
			_, _ = fmt.Fprintf(brw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", proto)
			if brw.Flush() != nil {
				return
			}
			_, _ = io.Copy(conn, brw.Reader)
		}),
	}}
	go b.srv.Serve(ln)
	t.Cleanup(func() { b.srv.Close() })
	return b
}

// streamClient is a client holding an upgraded connection open.
type streamClient struct {
	t    *testing.T
	conn net.Conn
	br   *bufio.Reader
	resp *http.Response
}

// upgradeStream performs an upgrade request for proto at path against the
// HTTP server on port 9080 and returns the client once the 101 is received.
func upgradeStream(t *testing.T, path, proto string, headers ...string) *streamClient {
	t.Helper()

	conn, err := net.DialTimeout("tcp", "127.0.0.1:9080", 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: localhost:9080\r\nConnection: Upgrade\r\nUpgrade: %s\r\n", path, proto)
	for _, h := range headers {
		req += h + "\r\n"
	}
	if _, err := io.WriteString(conn, req+"\r\n"); err != nil {
		t.Fatalf("writing upgrade request: %v", err)
	}

	c := &streamClient{t: t, conn: conn, br: bufio.NewReader(conn)}
	c.resp, err = http.ReadResponse(c.br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("reading upgrade response: %v", err)
	}
	if c.resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status = %d, want 101", c.resp.StatusCode)
	}
	return c
}

// echo sends msg through the stream and expects the same back.
func (c *streamClient) echo(msg string) {
	c.t.Helper()
	_ = c.conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.WriteString(c.conn, msg); err != nil {
		c.t.Fatalf("writing to the stream: %v", err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(c.br, got); err != nil || string(got) != msg {
		c.t.Fatalf("stream echoed %q (%v), want %q", got, err, msg)
	}
}

// expectNoData checks that nothing else is sent on the stream.
func (c *streamClient) expectNoData() {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	defer c.conn.SetReadDeadline(time.Time{})
	if b, err := c.br.ReadByte(); err == nil {
		c.t.Fatalf("unexpected data on the stream, starting with %q", b)
	}
}

// waitClosed waits for the server to close the stream.
func (c *streamClient) waitClosed(timeout time.Duration) {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(timeout))
	_, err := io.Copy(io.Discard, c.br)
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		c.t.Fatalf("the stream was not closed within %s", timeout)
	}
}

// closeGracefully ends the stream from the client side and waits
// for the server to finish with it.
func (c *streamClient) closeGracefully() {
	c.t.Helper()
	if tcp, ok := c.conn.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
	}
	c.waitClosed(10 * time.Second)
}

// streamConfig describes a server proxying upgrades, for the tests.
type streamConfig struct {
	routes      [][2]string // path prefix and upstream dial address
	detached    bool
	closeDelay  string
	streamLogs  map[string]any
	handlers    []any  // handlers in front of the reverse proxy
	accessLog   string // file the access log is written to, if any
	accessLevel string // level of the access log, DEBUG if empty
}

func (c streamConfig) String() string {
	var routes []any
	for _, r := range c.routes {
		proxy := map[string]any{
			"handler":   "reverse_proxy",
			"upstreams": []any{map[string]any{"dial": r[1]}},
		}
		if c.detached {
			proxy["stream_detached"] = true
		}
		if c.closeDelay != "" {
			proxy["stream_close_delay"] = c.closeDelay
		}
		if c.streamLogs != nil {
			proxy["stream_logs"] = c.streamLogs
		}
		routes = append(routes, map[string]any{
			"match":  []any{map[string]any{"path": []any{r[0] + "*"}}},
			"handle": append(append([]any{}, c.handlers...), proxy),
		})
	}

	server := map[string]any{
		"listen":          []any{":9080"},
		"automatic_https": map[string]any{"disable": true},
		"routes":          routes,
	}
	cfg := map[string]any{
		"admin": map[string]any{"listen": "localhost:2999"},
		"apps": map[string]any{
			"http": map[string]any{
				"http_port":    9080,
				"https_port":   9443,
				"grace_period": 1,
				"servers":      map[string]any{"srv0": server},
			},
		},
	}
	if c.accessLog != "" {
		level := c.accessLevel
		if level == "" {
			level = "DEBUG"
		}
		server["logs"] = map[string]any{"default_logger_name": "access"}
		cfg["logging"] = map[string]any{"logs": map[string]any{
			"default": map[string]any{"exclude": []any{"http.log.access.access"}},
			"access": map[string]any{
				"writer":  map[string]any{"output": "file", "filename": c.accessLog},
				"encoder": map[string]any{"format": "json"},
				"level":   level,
				"include": []any{"http.log.access.access"},
			},
		}}
	}

	out, err := json.Marshal(cfg)
	if err != nil {
		panic(err)
	}
	return string(out)
}

func loadStreamConfig(t *testing.T, tester *caddytest.Tester, c streamConfig) {
	t.Helper()
	tester.InitServer(c.String(), "json")
}

// TestStreamDetachedSurvivesReload checks that a detached stream stays open
// and keeps working when a reload keeps its upstream.
func TestStreamDetachedSurvivesReload(t *testing.T) {
	tester := caddytest.NewTester(t)
	startStreamBackend(t, streamAddrA)

	cfg := streamConfig{routes: [][2]string{{"/a/", streamAddrA}}, detached: true}
	loadStreamConfig(t, tester, cfg)

	stream := upgradeStream(t, "/a/ws", "echo")
	stream.echo("before the reload")

	// reload with a different config that keeps the upstream
	cfg.routes = append(cfg.routes, [2]string{"/b/", streamAddrB})
	loadStreamConfig(t, tester, cfg)
	stream.echo("after the reload")

	// and once more
	cfg.closeDelay = "1s"
	loadStreamConfig(t, tester, cfg)
	stream.echo("after another reload")

	// new streams keep working, too
	upgradeStream(t, "/a/ws", "echo").echo("new stream")
	stream.closeGracefully()
}

// TestStreamDetachedClosesRemovedUpstream checks that a reload closes only the
// detached streams whose upstream is gone from the config.
func TestStreamDetachedClosesRemovedUpstream(t *testing.T) {
	tester := caddytest.NewTester(t)
	startStreamBackend(t, streamAddrA)
	startStreamBackend(t, streamAddrB)

	cfg := streamConfig{
		routes:   [][2]string{{"/a/", streamAddrA}, {"/b/", streamAddrB}},
		detached: true,
	}
	loadStreamConfig(t, tester, cfg)

	removed := upgradeStream(t, "/a/ws", "websocket", "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==", "Sec-WebSocket-Version: 13")
	retained := upgradeStream(t, "/b/ws", "echo")
	removed.echo("a")
	retained.echo("b")

	// reload without the first upstream
	cfg.routes = cfg.routes[1:]
	loadStreamConfig(t, tester, cfg)

	removed.waitClosed(10 * time.Second)
	retained.echo("b is still there")
	retained.echo("and again")

	// the removed stream stays closed: the connection is really gone, not just idle
	if _, err := io.WriteString(removed.conn, "anyone there?"); err == nil {
		_ = removed.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if n, _ := removed.br.Read(make([]byte, 1)); n != 0 {
			t.Error("data received on a stream closed by the removal of its upstream")
		}
	}

	// a further reload that keeps the retained upstream doesn't close its stream
	cfg.closeDelay = "1s"
	loadStreamConfig(t, tester, cfg)
	retained.echo("b survives the next reload too")

	tester.AssertGetResponse("http://localhost:9080/b/plain", http.StatusOK, "plain")
	retained.closeGracefully()
}

// TestStreamDefaultClosedOnReload checks that without stream_detached the
// stream is closed on reload, even if the upstream is retained, after the
// stream_close_delay if there is one.
func TestStreamDefaultClosedOnReload(t *testing.T) {
	for _, tc := range []struct {
		name  string
		delay string
	}{
		{name: "no delay"},
		{name: "delay", delay: "3s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tester := caddytest.NewTester(t)
			startStreamBackend(t, streamAddrA)

			cfg := streamConfig{routes: [][2]string{{"/a/", streamAddrA}}, closeDelay: tc.delay}
			loadStreamConfig(t, tester, cfg)

			stream := upgradeStream(t, "/a/ws", "echo")
			stream.echo("before the reload")

			// a reload that keeps everything but a header
			cfg.handlers = []any{map[string]any{
				"handler":  "headers",
				"response": map[string]any{"set": map[string][]string{"X-Reload": {"1"}}},
			}}
			start := time.Now()
			loadStreamConfig(t, tester, cfg)

			if tc.delay == "" {
				stream.waitClosed(10 * time.Second)
				return
			}

			// the stream is still open during the delay, and closed after it
			stream.echo("during the delay")
			stream.waitClosed(15 * time.Second)
			if elapsed := time.Since(start); elapsed < 2*time.Second {
				t.Errorf("the stream was closed %s after the reload started, want it to be delayed by 3s", elapsed)
			}
		})
	}
}

// TestStreamDetachedHTTP11Upgrade checks HTTP/1.1 upgrades in detached mode,
// WebSocket or not, and what the client receives.
func TestStreamDetachedHTTP11Upgrade(t *testing.T) {
	tester := caddytest.NewTester(t)
	startStreamBackend(t, streamAddrA)
	loadStreamConfig(t, tester, streamConfig{routes: [][2]string{{"/a/", streamAddrA}}, detached: true})

	for _, tc := range []struct {
		proto   string
		headers []string
	}{
		{proto: "echo"},
		{proto: "websocket", headers: []string{"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==", "Sec-WebSocket-Version: 13"}},
	} {
		stream := upgradeStream(t, "/a/ws", tc.proto, tc.headers...)
		if got := stream.resp.Header.Get("Upgrade"); !strings.EqualFold(got, tc.proto) {
			t.Errorf("%s: Upgrade = %q in the 101 response", tc.proto, got)
		}
		if got := stream.resp.Header.Get("Connection"); !strings.EqualFold(got, "Upgrade") {
			t.Errorf("%s: Connection = %q in the 101 response", tc.proto, got)
		}
		stream.echo("one")
		stream.echo("two")
		stream.expectNoData()
		stream.closeGracefully()
	}

	// regular requests are not affected
	tester.AssertGetResponse("http://localhost:9080/a/plain", http.StatusOK, "plain")
}

// logEntries returns the entries of a JSON log file.
func logEntries(t *testing.T, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var entries []map[string]any
	for line := range bytes.SplitSeq(data, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}
		entries = append(entries, entry)
	}
	return entries
}

// waitForLogEntries waits for at least n entries with the given message,
// and returns the entries logged so far.
func waitForLogEntries(t *testing.T, path, msg string, n int) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		entries := logEntries(t, path)
		if len(entriesWithMessage(entries, msg)) >= n {
			return entries
		}
		if time.Now().After(deadline) {
			t.Fatalf("fewer than %d %q entries were logged, got %v", n, msg, entries)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitForLogEntry waits for an entry with the given message, and returns
// the entries logged so far.
func waitForLogEntry(t *testing.T, path, msg string) []map[string]any {
	t.Helper()
	return waitForLogEntries(t, path, msg, 1)
}

func entriesWithMessage(entries []map[string]any, msg string) []map[string]any {
	var out []map[string]any
	for _, e := range entries {
		if e["msg"] == msg {
			out = append(out, e)
		}
	}
	return out
}

// TestStreamLogsSkipHandshake checks that the access log entry of the upgrade
// handshake is logged by default, and left out with skip_handshake.
func TestStreamLogsSkipHandshake(t *testing.T) {
	for _, tc := range []struct {
		name          string
		streamLogs    map[string]any
		wantHandshake int
	}{
		{name: "default", wantHandshake: 1},
		{name: "logs configured", streamLogs: map[string]any{"level": "debug"}, wantHandshake: 1},
		{name: "skip handshake", streamLogs: map[string]any{"skip_handshake": true}, wantHandshake: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tester := caddytest.NewTester(t)
			startStreamBackend(t, streamAddrA)

			logPath := filepath.Join(t.TempDir(), "access.log")
			loadStreamConfig(t, tester, streamConfig{
				routes:     [][2]string{{"/a/", streamAddrA}},
				streamLogs: tc.streamLogs,
				accessLog:  logPath,
			})

			stream := upgradeStream(t, "/a/ws", "echo")
			stream.echo("hello")
			stream.closeGracefully()
			waitForLogEntry(t, logPath, "stream closed")

			// a request made after the stream is done, to know all of its entries are in
			tester.AssertGetResponse("http://localhost:9080/a/plain", http.StatusOK, "plain")
			entries := waitForLogEntries(t, logPath, "handled request", tc.wantHandshake+1)

			var handshakes int
			for _, e := range entriesWithMessage(entries, "handled request") {
				if status, _ := e["status"].(float64); status == http.StatusSwitchingProtocols {
					handshakes++
				}
			}
			if handshakes != tc.wantHandshake {
				t.Errorf("%d handshake entries in the access log, want %d; entries: %v", handshakes, tc.wantHandshake, entries)
			}
			if closed := entriesWithMessage(entries, "stream closed"); len(closed) != 1 {
				t.Errorf("%d stream close entries, want 1", len(closed))
			}
		})
	}
}

// TestStreamCloseLogAccessLogger checks that the stream close entry is
// written by the access logger of the request, at the configured level.
func TestStreamCloseLogAccessLogger(t *testing.T) {
	for _, tc := range []struct {
		name        string
		streamLogs  map[string]any
		accessLevel string
		wantLevel   string // empty means no entry is logged
	}{
		{name: "default level", accessLevel: "DEBUG", wantLevel: "debug"},
		{name: "default level is not logged at info", accessLevel: "INFO"},
		{name: "configured level", streamLogs: map[string]any{"level": "info"}, accessLevel: "INFO", wantLevel: "info"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tester := caddytest.NewTester(t)
			startStreamBackend(t, streamAddrA)

			logPath := filepath.Join(t.TempDir(), "access.log")
			loadStreamConfig(t, tester, streamConfig{
				routes:      [][2]string{{"/a/", streamAddrA}},
				streamLogs:  tc.streamLogs,
				accessLog:   logPath,
				accessLevel: tc.accessLevel,
			})

			stream := upgradeStream(t, "/a/ws", "echo")
			stream.echo("hello")
			stream.echo("world!")
			stream.closeGracefully()

			if tc.wantLevel == "" {
				// the handshake entry comes right after the stream close one
				waitForLogEntry(t, logPath, "handled request")
				if closed := entriesWithMessage(logEntries(t, logPath), "stream closed"); len(closed) != 0 {
					t.Errorf("unexpected stream close entries: %v", closed)
				}
				return
			}

			closed := entriesWithMessage(waitForLogEntry(t, logPath, "stream closed"), "stream closed")
			if len(closed) != 1 {
				t.Fatalf("%d stream close entries, want 1", len(closed))
			}
			e := closed[0]
			if e["level"] != tc.wantLevel {
				t.Errorf("level = %v, want %s", e["level"], tc.wantLevel)
			}
			if e["logger"] != "http.log.access.access" {
				t.Errorf("logger = %v, want http.log.access.access", e["logger"])
			}
			if e["upstream"] != streamAddrA {
				t.Errorf("upstream = %v, want %s", e["upstream"], streamAddrA)
			}
			if e["result"] != "closed" {
				t.Errorf("result = %v, want closed", e["result"])
			}
			if e["bytes_to_backend"] != float64(len("helloworld!")) || e["bytes_from_backend"] != float64(len("helloworld!")) {
				t.Errorf("bytes to/from backend = %v/%v, want %d", e["bytes_to_backend"], e["bytes_from_backend"], len("helloworld!"))
			}
			if _, ok := e["duration"]; !ok {
				t.Errorf("no duration in %v", e)
			}
		})
	}
}

// TestStreamLogsInvalidLevel checks that an invalid stream log level is refused.
func TestStreamLogsInvalidLevel(t *testing.T) {
	caddytest.AssertLoadError(t, streamConfig{
		routes:     [][2]string{{"/a/", streamAddrA}},
		streamLogs: map[string]any{"level": "bogus"},
	}.String(), "json", "invalid level")
}

// TestUpgradeThroughEncodeAndIntercept checks that upgrades work through the
// other handlers of a route, with access logging.
func TestUpgradeThroughEncodeAndIntercept(t *testing.T) {
	tester := caddytest.NewTester(t)
	startStreamBackend(t, streamAddrA)

	logPath := filepath.Join(t.TempDir(), "access.log")
	loadStreamConfig(t, tester, streamConfig{
		routes: [][2]string{{"/a/", streamAddrA}},
		handlers: []any{
			map[string]any{"handler": "encode", "encodings": map[string]any{"gzip": map[string]any{}}},
			map[string]any{
				"handler": "intercept",
				"handle_response": []any{map[string]any{
					"routes": []any{map[string]any{"handle": []any{
						map[string]any{"handler": "static_response", "body": "intercepted"},
					}}},
				}},
			},
		},
		streamLogs: map[string]any{"level": "debug"},
		accessLog:  logPath,
	})

	// the intercept handler is in effect for regular responses
	tester.AssertGetResponse("http://localhost:9080/a/plain", http.StatusOK, "intercepted")

	for _, tc := range []struct {
		name    string
		proto   string
		headers []string
	}{
		{name: "encoded", proto: "echo", headers: []string{"Accept-Encoding: gzip"}},
		{
			name:  "websocket",
			proto: "websocket",
			headers: []string{
				"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==", "Sec-WebSocket-Version: 13",
				"Accept-Encoding: gzip, br",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := upgradeStream(t, "/a/ws", tc.proto, tc.headers...)
			if enc := stream.resp.Header.Get("Content-Encoding"); enc != "" {
				t.Errorf("Content-Encoding = %q on the 101 response", enc)
			}
			stream.echo("one")
			stream.echo("two")
			stream.expectNoData()
			stream.closeGracefully()
		})
	}

	// the handshakes are access logged, as 101
	waitForLogEntries(t, logPath, "stream closed", 2)
	entries := waitForLogEntries(t, logPath, "handled request", 3)
	var handshakes int
	for _, e := range entriesWithMessage(entries, "handled request") {
		if status, _ := e["status"].(float64); status == http.StatusSwitchingProtocols {
			handshakes++
		}
	}
	if handshakes != 2 {
		t.Errorf("%d handshake entries in the access log, want 2; entries: %v", handshakes, entries)
	}
}

// TestExtendedConnectH2 checks WebSockets over HTTP/2 (extended CONNECT,
// RFC 8441), proxied to a backend that upgrades with HTTP/1.1.
func TestExtendedConnectH2(t *testing.T) {
	// The HTTP/2 server only supports extended CONNECT if GODEBUG has
	// http2xconnect=1 when the process starts. So the test runs itself in a
	// process like that, with its own server on other ports than the others.
	if os.Getenv(extendedConnectChildEnv) == "" {
		if testing.Short() {
			t.SkipNow()
		}
		godebug := "http2xconnect=1"
		if existing := os.Getenv("GODEBUG"); existing != "" {
			godebug = existing + "," + godebug
		}
		// (os.Args is not reliable, caddytest overwrites it to start the server)
		testBinary, err := os.Executable()
		if err != nil {
			t.Fatalf("finding the test binary: %v", err)
		}
		// the ports of the previous test's servers may take a moment to be
		// released, so a failure is retried once
		var out []byte
		for attempt := 0; attempt < 2; attempt++ {
			cmd := exec.Command(testBinary, "-test.run=^TestExtendedConnectH2$", "-test.count=1", "-test.v", "-test.timeout=2m") //nolint:gosec
			cmd.Env = append(os.Environ(), "GODEBUG="+godebug, extendedConnectChildEnv+"=1")
			if out, err = cmd.CombinedOutput(); err == nil {
				return
			}
			time.Sleep(time.Second)
		}
		t.Fatalf("extended CONNECT test failed: %v\n%s", err, out)
		return
	}

	tester := caddytest.NewTester(t).WithDefaultOverrides(caddytest.Config{AdminPort: 2998})
	startStreamBackend(t, streamAddrA)

	// a TLS server, like in the other HTTP/2 tests
	tester.InitServer(`
{
	"admin": {"listen": "localhost:2998"},
	"apps": {
		"http": {
			"http_port": 9082,
			"https_port": 9444,
			"grace_period": 1,
			"servers": {
				"srv0": {
					"listen": [":9444"],
					"automatic_https": {"disable": true},
					"routes": [{
						"handle": [{
							"handler": "reverse_proxy",
							"upstreams": [{"dial": "`+streamAddrA+`"}]
						}]
					}],
					"tls_connection_policies": [{
						"certificate_selection": {"any_tag": ["cert0"]},
						"default_sni": "a.caddy.localhost"
					}]
				}
			}
		},
		"tls": {
			"certificates": {
				"load_files": [{
					"certificate": "/a.caddy.localhost.crt",
					"key": "/a.caddy.localhost.key",
					"tags": ["cert0"]
				}]
			}
		},
		"pki": {
			"certificate_authorities": {
				"local": {"install_trust": false}
			}
		}
	}
}`, "json")

	// The extended CONNECT request is written with frames, since the
	// HTTP/2 clients of the standard libraries don't send the :protocol
	// pseudo header.
	conn, err := tls.Dial("tcp", "127.0.0.1:9444",&tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2"}}) //nolint:gosec
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	if proto := conn.ConnectionState().NegotiatedProtocol; proto != "h2" {
		t.Fatalf("negotiated protocol = %q, want h2", proto)
	}

	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatalf("writing the client preface: %v", err)
	}
	fr := http2.NewFramer(conn, conn)
	if err := fr.WriteSettings(); err != nil {
		t.Fatalf("writing settings: %v", err)
	}

	// the server has to announce it supports extended CONNECT (RFC 8441)
	const settingEnableConnectProtocol = http2.SettingID(0x8)
	for announced := false; !announced; {
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("reading the server settings: %v", err)
		}
		sf, ok := f.(*http2.SettingsFrame)
		if !ok || sf.IsAck() {
			continue
		}
		announced = true
		if v, ok := sf.Value(settingEnableConnectProtocol); !ok || v != 1 {
			t.Fatalf("the server doesn't announce support for extended CONNECT: %v", sf)
		}
		if err := fr.WriteSettingsAck(); err != nil {
			t.Fatalf("acknowledging settings: %v", err)
		}
	}

	var block bytes.Buffer
	enc := hpack.NewEncoder(&block)
	for _, field := range []hpack.HeaderField{
		{Name: ":method", Value: http.MethodConnect},
		{Name: ":protocol", Value: "websocket"},
		{Name: ":scheme", Value: "https"},
		{Name: ":path", Value: "/ws"},
		{Name: ":authority", Value: "127.0.0.1:9444"},
		{Name: "sec-websocket-version", Value: "13"},
	} {
		if err := enc.WriteField(field); err != nil {
			t.Fatalf("encoding %s: %v", field.Name, err)
		}
	}
	const streamID = 1
	if err := fr.WriteHeaders(http2.HeadersFrameParam{StreamID: streamID, BlockFragment: block.Bytes(), EndHeaders: true}); err != nil {
		t.Fatalf("writing the CONNECT request: %v", err)
	}

	var (
		status string
		data   bytes.Buffer
		dec    = hpack.NewDecoder(4096, nil)
	)
	// reads until the response status is known and want bytes of data were received
	readStream := func(want int) {
		t.Helper()
		for status == "" || data.Len() < want {
			f, err := fr.ReadFrame()
			if err != nil {
				t.Fatalf("reading from the stream: %v", err)
			}
			switch f := f.(type) {
			case *http2.HeadersFrame:
				fields, err := dec.DecodeFull(f.HeaderBlockFragment())
				if err != nil {
					t.Fatalf("decoding response headers: %v", err)
				}
				for _, field := range fields {
					if field.Name == ":status" {
						status = field.Value
					}
				}
			case *http2.DataFrame:
				data.Write(f.Data())
			case *http2.SettingsFrame:
				if !f.IsAck() {
					_ = fr.WriteSettingsAck()
				}
			case *http2.RSTStreamFrame:
				t.Fatalf("stream reset by the server: %v", f.ErrCode)
			case *http2.GoAwayFrame:
				t.Fatalf("connection closed by the server: %v", f.ErrCode)
			}
		}
	}

	readStream(0)
	if status != "200" {
		t.Fatalf("status = %q, want 200", status)
	}
	for _, msg := range []string{"hello over h2", "and again"} {
		if err := fr.WriteData(streamID, false, []byte(msg)); err != nil {
			t.Fatalf("writing to the stream: %v", err)
		}
		readStream(len(msg))
		if got := data.String(); got != msg {
			t.Fatalf("stream echoed %q, want %q", got, msg)
		}
		data.Reset()
	}
}
