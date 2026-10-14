package integration

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

	"github.com/caddyserver/caddy/v2/caddytest"
)

// TestMain makes sure net/http's HTTP/2 server supports extended CONNECT
// (RFC 8441), which it only does if the process starts with this setting.
func TestMain(m *testing.M) {
	const setting = "http2xconnect=1"
	if godebug := os.Getenv("GODEBUG"); !strings.Contains(godebug, setting) {
		env := []string{"GODEBUG=" + strings.Trim(godebug+","+setting, ",")}
		for _, kv := range os.Environ() {
			if !strings.HasPrefix(kv, "GODEBUG=") {
				env = append(env, kv)
			}
		}
		// replaces the process where that is supported; elsewhere the
		// tests that need extended CONNECT are skipped
		_ = syscall.Exec(os.Args[0], os.Args, env)
	}
	os.Exit(m.Run())
}

const (
	streamBackendA = "127.0.0.1:54331"
	streamBackendB = "127.0.0.1:54332"
	streamWait     = 5 * time.Second
)

// startUpgradeBackend starts a backend that echoes whatever is sent over an
// upgraded connection (any Upgrade protocol), and answers plain requests with
// "plain response".
func startUpgradeBackend(t *testing.T, addr string) {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listening on %s: %v", addr, err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrade := r.Header.Get("Upgrade")
		if upgrade == "" {
			_, _ = io.WriteString(w, "plain response")
			return
		}
		conn, brw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", upgrade)
		_, _ = io.Copy(conn, brw.Reader)
	})}
	go srv.Serve(ln) //nolint:errcheck
	// hijacked connections are not closed by this: they end when the proxy closes them
	t.Cleanup(func() { srv.Close() })
}

// tunnel is the client side of an upgraded connection.
type tunnel struct {
	conn net.Conn
	br   *bufio.Reader
}

// openTunnel sends an upgrade request for the given protocol to Caddy's
// HTTP port and expects the 101 response.
func openTunnel(t *testing.T, path, protocol string, extraHeaders ...string) *tunnel {
	t.Helper()
	conn, err := net.DialTimeout("tcp", "127.0.0.1:9080", streamWait)
	if err != nil {
		t.Fatalf("dialing caddy: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	var req strings.Builder
	fmt.Fprintf(&req, "GET %s HTTP/1.1\r\nHost: localhost:9080\r\nConnection: Upgrade\r\nUpgrade: %s\r\n", path, protocol)
	for _, h := range extraHeaders {
		req.WriteString(h + "\r\n")
	}
	req.WriteString("\r\n")
	if _, err := io.WriteString(conn, req.String()); err != nil {
		t.Fatalf("writing upgrade request: %v", err)
	}

	br := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(streamWait))
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("reading upgrade response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade response status = %d, want 101", resp.StatusCode)
	}
	return &tunnel{conn: conn, br: br}
}

// echo sends msg through the tunnel and expects to get it back.
func (tn *tunnel) echo(t *testing.T, msg string) {
	t.Helper()
	_ = tn.conn.SetDeadline(time.Now().Add(streamWait))
	if _, err := io.WriteString(tn.conn, msg); err != nil {
		t.Fatalf("writing %q to the tunnel: %v", msg, err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(tn.br, got); err != nil {
		t.Fatalf("reading %q back from the tunnel: %v", msg, err)
	}
	if string(got) != msg {
		t.Fatalf("tunnel echoed %q, want %q", got, msg)
	}
}

// waitClosed waits for the proxy to close the tunnel, and reports how long it took.
func (tn *tunnel) waitClosed(t *testing.T, within time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	buf := make([]byte, 64)
	for {
		_ = tn.conn.SetReadDeadline(start.Add(within))
		if _, err := tn.br.Read(buf); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatalf("the tunnel was still open after %v", within)
			}
			return time.Since(start)
		}
	}
}

// streamCaddyfile returns a Caddyfile with a localhost:9080 site made of body.
// gen differs between the configs of a test, since an unchanged config
// is not reloaded.
func streamCaddyfile(gen int, body string) string {
	return fmt.Sprintf(`{
	skip_install_trust
	admin localhost:2999
	http_port     9080
	https_port    9443
	grace_period  1ns
}

localhost:9080 {
	header X-Gen %d
	%s
}`, gen, body)
}

const (
	routeAdetached = `handle /a* {
		reverse_proxy 127.0.0.1:54331 {
			stream_detached
		}
	}
	`
	routeBdetached = `handle /b* {
		reverse_proxy 127.0.0.1:54332 {
			stream_detached
		}
	}
	`
	routeFallback = `handle {
		respond "fallback"
	}`
)

func TestStreamDetachedSurvivesReload(t *testing.T) {
	startUpgradeBackend(t, streamBackendA)
	tester := caddytest.NewTester(t)
	tester.InitServer(streamCaddyfile(1, routeAdetached+routeFallback), "caddyfile")

	tn := openTunnel(t, "/a", "echo")
	tn.echo(t, "before reload")

	// reloads that keep the upstream
	tester.InitServer(streamCaddyfile(2, routeAdetached+routeFallback), "caddyfile")
	tn.echo(t, "after first reload")
	tester.InitServer(streamCaddyfile(3, routeAdetached+routeFallback), "caddyfile")
	tn.echo(t, "after second reload")

	// the upgrade still works for new connections to the reloaded config
	tn2 := openTunnel(t, "/a", "echo")
	tn2.echo(t, "new stream")

	// the upstream is removed from the config
	tester.InitServer(streamCaddyfile(4, routeFallback), "caddyfile")
	tn.waitClosed(t, streamWait)
	tn2.waitClosed(t, streamWait)
}

// An upstream address with a placeholder can't be matched against the new config,
// but the stream is not closed for that: it is kept while such a handler is live.
func TestStreamDetachedPlaceholderUpstream(t *testing.T) {
	startUpgradeBackend(t, streamBackendA)
	tester := caddytest.NewTester(t)
	body := `reverse_proxy 127.0.0.1:{http.request.header.X-Port} {
		stream_detached
	}`
	tester.InitServer(streamCaddyfile(1, body), "caddyfile")

	tn := openTunnel(t, "/", "echo", "X-Port: 54331")
	tn.echo(t, "before reload")
	tester.InitServer(streamCaddyfile(2, body), "caddyfile")
	tn.echo(t, "after reload")

	// no such handler in the config anymore
	tester.InitServer(streamCaddyfile(3, routeFallback), "caddyfile")
	tn.waitClosed(t, streamWait)
}

func TestStreamDetachedClosesOnlyRemovedUpstream(t *testing.T) {
	startUpgradeBackend(t, streamBackendA)
	startUpgradeBackend(t, streamBackendB)
	tester := caddytest.NewTester(t)
	tester.InitServer(streamCaddyfile(1, routeAdetached+routeBdetached+routeFallback), "caddyfile")

	a := openTunnel(t, "/a", "echo")
	b := openTunnel(t, "/b", "echo")
	a.echo(t, "to a")
	b.echo(t, "to b")

	// backend A is removed, backend B is retained
	tester.InitServer(streamCaddyfile(2, routeBdetached+routeFallback), "caddyfile")
	a.waitClosed(t, streamWait)
	b.echo(t, "b is still open")

	// and then B is removed too
	tester.InitServer(streamCaddyfile(3, routeFallback), "caddyfile")
	b.waitClosed(t, streamWait)
}

func TestStreamNotDetachedClosedOnReload(t *testing.T) {
	startUpgradeBackend(t, streamBackendA)
	tester := caddytest.NewTester(t)
	body := `reverse_proxy 127.0.0.1:54331`
	tester.InitServer(streamCaddyfile(1, body), "caddyfile")

	tn := openTunnel(t, "/", "echo")
	tn.echo(t, "before reload")

	// even though the upstream is retained, the stream goes with the config
	tester.InitServer(streamCaddyfile(2, body), "caddyfile")
	tn.waitClosed(t, streamWait)

	// the new config serves new streams
	openTunnel(t, "/", "echo").echo(t, "new stream")
}

func TestStreamCloseDelayKeepsStreamUntilDelay(t *testing.T) {
	startUpgradeBackend(t, streamBackendA)
	tester := caddytest.NewTester(t)
	body := `reverse_proxy 127.0.0.1:54331 {
		stream_close_delay 3s
	}`
	tester.InitServer(streamCaddyfile(1, body), "caddyfile")

	tn := openTunnel(t, "/", "echo")
	tn.echo(t, "before reload")

	start := time.Now()
	tester.InitServer(streamCaddyfile(2, body), "caddyfile")
	tn.echo(t, "after reload, within the delay")

	tn.waitClosed(t, 4*streamWait)
	if elapsed := time.Since(start); elapsed < 2*time.Second {
		t.Errorf("the stream was closed %v after the reload started, before stream_close_delay", elapsed)
	}
}

// Detached streams are not closed by the delay either, which does not apply to them.
func TestStreamDetachedIgnoresCloseDelay(t *testing.T) {
	startUpgradeBackend(t, streamBackendA)
	tester := caddytest.NewTester(t)
	body := `reverse_proxy 127.0.0.1:54331 {
		stream_detached
		stream_close_delay 1s
	}`
	tester.InitServer(streamCaddyfile(1, body), "caddyfile")

	tn := openTunnel(t, "/", "echo")
	tester.InitServer(streamCaddyfile(2, body), "caddyfile")
	time.Sleep(2 * time.Second)
	tn.echo(t, "still open after the delay")
}

func TestStreamThroughEncode(t *testing.T) {
	startUpgradeBackend(t, streamBackendA)
	tester := caddytest.NewTester(t)

	for _, tc := range []struct {
		name string
		opts string
	}{
		{name: "default"},
		{name: "detached", opts: "stream_detached"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tester.InitServer(streamCaddyfile(1, fmt.Sprintf(`encode gzip
	reverse_proxy 127.0.0.1:54331 {
		%s
	}`, tc.opts)), "caddyfile")

			// not a WebSocket handshake, so encode is active for the request
			tn := openTunnel(t, "/", "echo", "Accept-Encoding: gzip")
			tn.echo(t, "through encode")
			tn.echo(t, "and again")
			_ = tn.conn.Close()

			// responses that are not upgrades are still encoded
			if tc.name == "default" {
				req, _ := http.NewRequest(http.MethodGet, "http://localhost:9080/", nil)
				req.Header.Set("Accept-Encoding", "gzip")
				tester.AssertResponseCode(req, http.StatusOK)
			}
		})
	}
}

func TestStreamWebSocketThroughEncode(t *testing.T) {
	startUpgradeBackend(t, streamBackendA)
	tester := caddytest.NewTester(t)
	tester.InitServer(streamCaddyfile(1, `encode gzip
	reverse_proxy 127.0.0.1:54331`), "caddyfile")

	tn := openTunnel(t, "/", "websocket",
		"Accept-Encoding: gzip",
		"Sec-WebSocket-Version: 13",
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==")
	tn.echo(t, "websocket through encode")
}

// RFC 8441: a WebSocket over an HTTP/2 stream opened with an extended CONNECT.
func TestStreamExtendedConnectOverH2(t *testing.T) {
	startUpgradeBackend(t, streamBackendA)
	tester := caddytest.NewTester(t)

	for _, tc := range []struct {
		name string
		opts string
	}{
		{name: "default"},
		{name: "detached", opts: "stream_detached"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tester.InitServer(fmt.Sprintf(`{
	skip_install_trust
	admin localhost:2999
	http_port     9080
	https_port    9443
	grace_period  1ns
}

localhost:9443 {
	reverse_proxy 127.0.0.1:54331 {
		%s
	}
}`, tc.opts), "caddyfile")

			c := openExtendedConnect(t, "/ws")
			c.expectStatus(t, "200")
			c.echo(t, "hello over h2")
			c.echo(t, "and again")

			// closing our side ends the stream
			c.closeSend(t)
			c.waitEnd(t)
		})
	}
}

// h2Event is something the server sent on the extended CONNECT stream.
type h2Event struct {
	noConnect bool // the server does not support extended CONNECT
	status    string
	data   []byte
	end    bool
	err    error
}

// h2Stream is a minimal HTTP/2 client with a single extended CONNECT stream
// (RFC 8441), written with frames since the Go clients do not offer it.
type h2Stream struct {
	conn   net.Conn
	mu     sync.Mutex // guards writes to fr
	fr     *http2.Framer
	events chan h2Event
	buf    []byte // received data not consumed yet
}

func openExtendedConnect(t *testing.T, path string) *h2Stream {
	t.Helper()
	conn, err := tls.Dial("tcp", "127.0.0.1:9443", &tls.Config{ //nolint:gosec
		InsecureSkipVerify: true,
		NextProtos:         []string{"h2"},
		ServerName:         "localhost",
	})
	if err != nil {
		t.Fatalf("dialing caddy over TLS: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	s := &h2Stream{conn: conn, fr: http2.NewFramer(conn, conn), events: make(chan h2Event, 64)}
	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatalf("writing the client preface: %v", err)
	}
	if err := s.fr.WriteSettings(); err != nil {
		t.Fatalf("writing settings: %v", err)
	}

	var block bytes.Buffer
	enc := hpack.NewEncoder(&block)
	for _, field := range [][2]string{
		{":method", "CONNECT"},
		{":protocol", "websocket"},
		{":scheme", "https"},
		{":path", path},
		{":authority", "localhost:9443"},
		{"sec-websocket-version", "13"},
	} {
		if err := enc.WriteField(hpack.HeaderField{Name: field[0], Value: field[1]}); err != nil {
			t.Fatalf("encoding header %s: %v", field[0], err)
		}
	}
	if err := s.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block.Bytes(), EndHeaders: true}); err != nil {
		t.Fatalf("writing the request: %v", err)
	}

	go s.readLoop()
	return s
}

func (s *h2Stream) readLoop() {
	dec := hpack.NewDecoder(4096, nil)
	for {
		frame, err := s.fr.ReadFrame()
		if err != nil {
			s.events <- h2Event{err: err}
			return
		}
		switch f := frame.(type) {
		case *http2.SettingsFrame:
			if !f.IsAck() {
				var connectProtocol bool
				_ = f.ForeachSetting(func(setting http2.Setting) error {
					if setting.ID == http2.SettingEnableConnectProtocol && setting.Val == 1 {
						connectProtocol = true
					}
					return nil
				})
				if !connectProtocol {
					s.events <- h2Event{noConnect: true}
				}
				s.mu.Lock()
				_ = s.fr.WriteSettingsAck()
				s.mu.Unlock()
			}
		case *http2.PingFrame:
			if !f.IsAck() {
				s.mu.Lock()
				_ = s.fr.WritePing(true, f.Data)
				s.mu.Unlock()
			}
		case *http2.HeadersFrame:
			fields, err := dec.DecodeFull(f.HeaderBlockFragment())
			if err != nil {
				s.events <- h2Event{err: err}
				return
			}
			ev := h2Event{end: f.StreamEnded()}
			for _, field := range fields {
				if field.Name == ":status" {
					ev.status = field.Value
				}
			}
			s.events <- ev
		case *http2.DataFrame:
			s.events <- h2Event{data: append([]byte(nil), f.Data()...), end: f.StreamEnded()}
		case *http2.RSTStreamFrame:
			s.events <- h2Event{err: fmt.Errorf("stream reset: %v", f.ErrCode)}
			return
		case *http2.GoAwayFrame:
			s.events <- h2Event{err: fmt.Errorf("go away: %v", f.ErrCode)}
			return
		}
	}
}

func (s *h2Stream) next(t *testing.T) h2Event {
	t.Helper()
	for {
		select {
		case ev := <-s.events:
			if ev.noConnect {
				t.Skip("the HTTP/2 server does not support extended CONNECT; it needs GODEBUG=http2xconnect=1")
			}
			if ev.err != nil {
				t.Fatalf("HTTP/2 connection failed: %v", ev.err)
			}
			return ev
		case <-time.After(streamWait):
			t.Fatal("timed out waiting for the server")
			return h2Event{}
		}
	}
}

func (s *h2Stream) expectStatus(t *testing.T, want string) {
	t.Helper()
	if ev := s.next(t); ev.status != want {
		t.Fatalf("response status = %q, want %q", ev.status, want)
	}
}

// echo sends msg on the stream and expects to get it back.
func (s *h2Stream) echo(t *testing.T, msg string) {
	t.Helper()
	s.mu.Lock()
	err := s.fr.WriteData(1, false, []byte(msg))
	s.mu.Unlock()
	if err != nil {
		t.Fatalf("writing %q: %v", msg, err)
	}
	for len(s.buf) < len(msg) {
		s.buf = append(s.buf, s.next(t).data...)
	}
	if got := string(s.buf[:len(msg)]); got != msg {
		t.Fatalf("stream echoed %q, want %q", got, msg)
	}
	s.buf = s.buf[len(msg):]
}

func (s *h2Stream) closeSend(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	err := s.fr.WriteData(1, true, nil)
	s.mu.Unlock()
	if err != nil {
		t.Fatalf("closing the stream: %v", err)
	}
}

// waitEnd waits for the server to end the stream.
func (s *h2Stream) waitEnd(t *testing.T) {
	t.Helper()
	for {
		if ev := s.next(t); ev.end {
			return
		}
	}
}

func TestStreamLoadRejectsInvalidLogLevel(t *testing.T) {
	caddytest.AssertLoadError(t, `{
	"admin": {"listen": "localhost:2999"},
	"apps": {
		"http": {
			"http_port": 9080,
			"servers": {
				"srv0": {
					"listen": [":9080"],
					"routes": [{
						"handle": [{
							"handler": "reverse_proxy",
							"upstreams": [{"dial": "127.0.0.1:54331"}],
							"stream_logs": {"level": "loud"}
						}]
					}]
				}
			}
		}
	}
}`, "json", "invalid stream_logs level")
}

// logEntries reads the JSON log entries written to path.
func logEntries(t *testing.T, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("reading log: %v", err)
	}
	var entries []map[string]any
	for line := range strings.SplitSeq(string(data), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line %q is not JSON: %v", line, err)
		}
		entries = append(entries, entry)
	}
	return entries
}

func findLogEntries(entries []map[string]any, msg string) []map[string]any {
	var found []map[string]any
	for _, e := range entries {
		if e["msg"] == msg {
			found = append(found, e)
		}
	}
	return found
}

// waitForLog waits for an entry with the message to be written to the log at path.
func waitForLog(t *testing.T, path, msg string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(streamWait)
	for {
		if found := findLogEntries(logEntries(t, path), msg); len(found) > 0 {
			return found[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %q entry in the log: %v", msg, logEntries(t, path))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// closeTunnel ends a tunnel from the client side, which also ends the backend's echo.
func closeTunnel(tn *tunnel) { _ = tn.conn.Close() }

func accessLogCaddyfile(gen int, logPath, streamOpts string) string {
	return streamCaddyfile(gen, fmt.Sprintf(`log {
		output file %s
		format json
		level DEBUG
	}
	reverse_proxy 127.0.0.1:54331 {
		%s
	}`, logPath, streamOpts))
}

func TestStreamLogsDefault(t *testing.T) {
	startUpgradeBackend(t, streamBackendA)
	tester := caddytest.NewTester(t)
	logPath := filepath.ToSlash(filepath.Join(t.TempDir(), "access.log"))
	tester.InitServer(accessLogCaddyfile(1, logPath, ""), "caddyfile")

	tn := openTunnel(t, "/ws", "echo")
	tn.echo(t, "ping")
	closeTunnel(tn)

	closed := waitForLog(t, logPath, "stream closed")
	if closed["level"] != "debug" {
		t.Errorf("stream close logged at %v, want the default, debug", closed["level"])
	}
	if got, _ := closed["bytes_to_backend"].(float64); got < 4 {
		t.Errorf("bytes_to_backend = %v, want at least 4", closed["bytes_to_backend"])
	}
	if got, _ := closed["bytes_from_backend"].(float64); got < 4 {
		t.Errorf("bytes_from_backend = %v, want at least 4", closed["bytes_from_backend"])
	}
	if closed["upstream"] != streamBackendA {
		t.Errorf("upstream = %v, want %s", closed["upstream"], streamBackendA)
	}
	if closed["result"] == nil {
		t.Errorf("no result in the stream close log: %v", closed)
	}

	// by default the handshake is logged
	handshake := waitForLog(t, logPath, "handled request")
	if status, _ := handshake["status"].(float64); status != http.StatusSwitchingProtocols {
		t.Errorf("handshake logged with status %v, want 101", handshake["status"])
	}
}

func TestStreamLogsSkipHandshakeAndLevel(t *testing.T) {
	startUpgradeBackend(t, streamBackendA)
	tester := caddytest.NewTester(t)
	logPath := filepath.ToSlash(filepath.Join(t.TempDir(), "access.log"))
	tester.InitServer(accessLogCaddyfile(1, logPath, `stream_logs {
			skip_handshake
			level warn
		}`), "caddyfile")

	tn := openTunnel(t, "/ws", "echo")
	tn.echo(t, "ping")
	closeTunnel(tn)

	closed := waitForLog(t, logPath, "stream closed")
	if closed["level"] != "warn" {
		t.Errorf("stream close logged at %v, want warn", closed["level"])
	}

	// the handshake is written when the handler returns, which is after the stream closes
	time.Sleep(time.Second)
	if handshake := findLogEntries(logEntries(t, logPath), "handled request"); len(handshake) > 0 {
		t.Errorf("the handshake was logged although skip_handshake is set: %v", handshake)
	}

	// requests that are not upgrades are logged as usual
	tester.AssertGetResponse("http://localhost:9080/plain", 200, "plain response")
	waitForLog(t, logPath, "handled request")
}

func TestStreamDetachedWithAccessLogging(t *testing.T) {
	startUpgradeBackend(t, streamBackendA)
	tester := caddytest.NewTester(t)
	logPath := filepath.ToSlash(filepath.Join(t.TempDir(), "access.log"))
	opts := `stream_detached
		stream_logs {
			level info
		}`
	tester.InitServer(accessLogCaddyfile(1, logPath, opts), "caddyfile")

	tn := openTunnel(t, "/ws", "echo")
	tn.echo(t, "before reload")

	tester.InitServer(accessLogCaddyfile(2, logPath, opts), "caddyfile")
	tn.echo(t, "after reload")
	closeTunnel(tn)

	// the stream outlived the config it was started with, and still logs
	closed := waitForLog(t, logPath, "stream closed")
	if closed["level"] != "info" {
		t.Errorf("stream close logged at %v, want info", closed["level"])
	}
	if got, _ := closed["bytes_to_backend"].(float64); got < float64(len("before reload")+len("after reload")) {
		t.Errorf("bytes_to_backend = %v, want the traffic from before and after the reload", closed["bytes_to_backend"])
	}
	waitForLog(t, logPath, "handled request")

	// and a stream closed by a reload that removes its upstream is logged as well
	tn2 := openTunnel(t, "/ws", "echo")
	tn2.echo(t, "second")
	tester.InitServer(streamCaddyfile(3, fmt.Sprintf("log {\n\t\toutput file %s\n\t\tformat json\n\t\tlevel DEBUG\n\t}\n\trespond \"gone\"", logPath)), "caddyfile")
	tn2.waitClosed(t, streamWait)
	deadline := time.Now().Add(streamWait)
	for len(findLogEntries(logEntries(t, logPath), "stream closed")) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("the stream closed by the reload was not logged: %v", logEntries(t, logPath))
		}
		time.Sleep(50 * time.Millisecond)
	}
	var results []any
	for _, e := range findLogEntries(logEntries(t, logPath), "stream closed") {
		results = append(results, e["result"])
	}
	if results[len(results)-1] != "closed" {
		t.Errorf("stream closed by the reload has result %v, want closed (all: %v)", results[len(results)-1], results)
	}
}
