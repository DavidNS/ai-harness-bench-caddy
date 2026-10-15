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
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

	"github.com/caddyserver/caddy/v2/caddytest"
)

const streamTestTimeout = 5 * time.Second

// http2XConnect is the GODEBUG setting which makes the HTTP/2 server accept
// extended CONNECT (RFC 8441).
const http2XConnect = "http2xconnect=1"

// TestMain runs the tests of the package in a child process in which the
// HTTP/2 server supports extended CONNECT: the server reads the setting once
// when the process starts, so it cannot be enabled by a test.
func TestMain(m *testing.M) {
	debug := os.Getenv("GODEBUG")
	if strings.Contains(debug, http2XConnect) {
		os.Exit(m.Run())
	}

	cmd := exec.Command(os.Args[0], os.Args[1:]...)
	cmd.Env = append(os.Environ(), "GODEBUG="+strings.TrimPrefix(debug+","+http2XConnect, ","))
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	err := cmd.Run()
	if err == nil {
		os.Exit(0)
	}
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		os.Exit(exitErr.ExitCode())
	}
	fmt.Fprintf(os.Stderr, "failed to run the tests: %v\n", err)
	os.Exit(1)
}

// streamGlobalOptions are the global options of the Caddyfiles of the stream tests.
const streamGlobalOptions = `{
	skip_install_trust
	admin localhost:2999
	http_port     9080
	https_port    9443
	grace_period  1ns
}
`

// upgradeBackend is a backend which switches protocols for every upgrade
// request and then echoes what it receives. Other requests get a plain
// response.
type upgradeBackend struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func newUpgradeBackend(t *testing.T, addr string) *upgradeBackend {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("failed to start the backend on %s: %v", addr, err)
	}
	b := &upgradeBackend{ln: ln}
	t.Cleanup(b.close)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			b.mu.Lock()
			b.conns = append(b.conns, c)
			b.mu.Unlock()
			go b.serve(c)
		}
	}()
	return b
}

func (b *upgradeBackend) close() {
	_ = b.ln.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range b.conns {
		_ = c.Close()
	}
}

func (b *upgradeBackend) serve(c net.Conn) {
	defer c.Close()
	br := bufio.NewReader(c)
	req, err := http.ReadRequest(br)
	if err != nil {
		return
	}
	if !strings.EqualFold(req.Header.Get("Upgrade"), "websocket") {
		// no Content-Length, so handlers may replace the body
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nplain")
		return
	}
	_, err = io.WriteString(c, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: dummy\r\n\r\n")
	if err != nil {
		return
	}
	// echo, including what was sent along with the handshake
	_, _ = io.Copy(c, br)
}

// wsClient is the client of an upgraded stream to 127.0.0.1:9080.
type wsClient struct {
	conn net.Conn
	br   *bufio.Reader
}

// dialWS performs an HTTP/1.1 upgrade handshake for path.
func dialWS(t *testing.T, path string, extraHeaders ...string) *wsClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", "127.0.0.1:9080", streamTestTimeout)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	req := "GET " + path + " HTTP/1.1\r\n" +
		"Host: localhost:9080\r\n" +
		"Connection: Upgrade\r\n" +
		"Upgrade: websocket\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
	for _, h := range extraHeaders {
		req += h + "\r\n"
	}
	req += "\r\n"
	_ = conn.SetDeadline(time.Now().Add(streamTestTimeout))
	if _, err := io.WriteString(conn, req); err != nil {
		t.Fatalf("failed to write the upgrade request: %v", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("failed to read the upgrade response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status = %d, want 101", resp.StatusCode)
	}
	return &wsClient{conn: conn, br: br}
}

// echo checks that data passes through the stream and back.
func (c *wsClient) echo(t *testing.T, msg string) {
	t.Helper()
	_ = c.conn.SetDeadline(time.Now().Add(streamTestTimeout))
	if _, err := io.WriteString(c.conn, msg); err != nil {
		t.Fatalf("stream is not writable: %v", err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(c.br, got); err != nil {
		t.Fatalf("stream is not readable: %v", err)
	}
	if string(got) != msg {
		t.Fatalf("echo = %q, want %q", got, msg)
	}
}

// requireClosed checks that the server closes the stream within wait.
func (c *wsClient) requireClosed(t *testing.T, wait time.Duration) {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(wait))
	// a websocket close frame may precede the end of the stream
	if _, err := io.Copy(io.Discard, c.br); errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("stream was not closed within %v", wait)
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(streamTestTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// readLog returns the JSON entries of the log file.
func readLog(path string) []map[string]any {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var entries []map[string]any
	for _, line := range bytes.Split(data, []byte("\n")) {
		var entry map[string]any
		if json.Unmarshal(line, &entry) == nil {
			entries = append(entries, entry)
		}
	}
	return entries
}

// findLog returns the entries with the message msg about the request for uri
// (an empty uri matches every entry of the message).
func findLog(path, msg, uri string) []map[string]any {
	var found []map[string]any
	for _, e := range readLog(path) {
		if e["msg"] != msg {
			continue
		}
		if uri != "" {
			req, _ := e["request"].(map[string]any)
			if req["uri"] != uri {
				continue
			}
		}
		found = append(found, e)
	}
	return found
}

func logFile(t *testing.T) string {
	return filepath.ToSlash(filepath.Join(t.TempDir(), "access.log"))
}

func TestStreamDetachedSurvivesReload(t *testing.T) {
	newUpgradeBackend(t, "127.0.0.1:9081")
	tester := caddytest.NewTester(t)
	tester.InitServer(streamGlobalOptions+`
	localhost:9080 {
		reverse_proxy 127.0.0.1:9081 {
			stream_detached
		}
	}`, "caddyfile")

	ws := dialWS(t, "/ws")
	ws.echo(t, "before")

	tester.InitServer(streamGlobalOptions+`
	localhost:9080 {
		respond /version "v2"
		reverse_proxy 127.0.0.1:9081 {
			stream_detached
		}
	}`, "caddyfile")
	tester.AssertGetResponse("http://localhost:9080/version", 200, "v2")

	ws.echo(t, "after the reload")
	// streams of the new config work too
	dialWS(t, "/ws").echo(t, "new stream")
	ws.echo(t, "still open")
}

func TestStreamDetachedSurvivesReloadJSON(t *testing.T) {
	newUpgradeBackend(t, "127.0.0.1:9081")
	tester := caddytest.NewTester(t)

	config := func(extraRoutes string) string {
		return fmt.Sprintf(`{
			"admin": {"listen": "localhost:2999"},
			"apps": {"http": {
				"http_port": 9080,
				"https_port": 9443,
				"grace_period": 1,
				"servers": {"srv0": {
					"listen": [":9080"],
					"routes": [%s{
						"handle": [{
							"handler": "reverse_proxy",
							"stream_detached": true,
							"upstreams": [{"dial": "127.0.0.1:9081"}]
						}]
					}]
				}}
			}}
		}`, extraRoutes)
	}
	tester.InitServer(config(""), "json")

	ws := dialWS(t, "/ws")
	ws.echo(t, "before")

	tester.InitServer(config(`{
		"match": [{"path": ["/version"]}],
		"handle": [{"handler": "static_response", "body": "v2"}]
	},`), "json")
	tester.AssertGetResponse("http://localhost:9080/version", 200, "v2")

	ws.echo(t, "after the reload")
}

func TestStreamDetachedClosesRemovedUpstream(t *testing.T) {
	newUpgradeBackend(t, "127.0.0.1:9081")
	newUpgradeBackend(t, "127.0.0.1:9082")
	tester := caddytest.NewTester(t)
	tester.InitServer(streamGlobalOptions+`
	localhost:9080 {
		handle /a {
			reverse_proxy 127.0.0.1:9081 {
				stream_detached
			}
		}
		handle /b {
			reverse_proxy 127.0.0.1:9082 {
				stream_detached
			}
		}
	}`, "caddyfile")

	a, b := dialWS(t, "/a"), dialWS(t, "/b")
	a.echo(t, "a")
	b.echo(t, "b")

	// the second upstream is gone from the new config
	tester.InitServer(streamGlobalOptions+`
	localhost:9080 {
		handle /a {
			reverse_proxy 127.0.0.1:9081 {
				stream_detached
			}
		}
	}`, "caddyfile")

	b.requireClosed(t, streamTestTimeout)
	a.echo(t, "a is retained")
}

func TestStreamNotDetachedClosedOnReload(t *testing.T) {
	newUpgradeBackend(t, "127.0.0.1:9081")
	tester := caddytest.NewTester(t)
	tester.InitServer(streamGlobalOptions+`
	localhost:9080 {
		reverse_proxy 127.0.0.1:9081
	}`, "caddyfile")

	ws := dialWS(t, "/ws")
	ws.echo(t, "before")

	tester.InitServer(streamGlobalOptions+`
	localhost:9080 {
		respond /version "v2"
		reverse_proxy 127.0.0.1:9081
	}`, "caddyfile")

	ws.requireClosed(t, streamTestTimeout)
	// the new config accepts new streams
	dialWS(t, "/ws").echo(t, "new stream")
}

func TestStreamCloseDelayOnReload(t *testing.T) {
	const delay = 2 * time.Second
	newUpgradeBackend(t, "127.0.0.1:9081")
	tester := caddytest.NewTester(t)
	tester.InitServer(streamGlobalOptions+`
	localhost:9080 {
		reverse_proxy 127.0.0.1:9081 {
			stream_close_delay 2s
		}
	}`, "caddyfile")

	ws := dialWS(t, "/ws")
	ws.echo(t, "before")

	start := time.Now()
	tester.InitServer(streamGlobalOptions+`
	localhost:9080 {
		respond /version "v2"
		reverse_proxy 127.0.0.1:9081
	}`, "caddyfile")

	// still open after the reload, until the delay is over
	ws.echo(t, "after the reload")
	ws.requireClosed(t, 2*delay)
	if elapsed := time.Since(start); elapsed < delay {
		t.Errorf("stream closed %v after the reload started, before the %v delay", elapsed, delay)
	}
}

func TestStreamLogsInvalidConfig(t *testing.T) {
	for name, tc := range map[string]struct {
		options string
		want    string
	}{
		"logger name": {options: "logger_name other", want: "logger_name"},
		"level":       {options: "level loud", want: "level"},
	} {
		t.Run(name, func(t *testing.T) {
			caddytest.AssertLoadError(t, streamGlobalOptions+`
			localhost:9080 {
				reverse_proxy 127.0.0.1:9081 {
					stream_logs {
						`+tc.options+`
					}
				}
			}`, "caddyfile", tc.want)
		})
	}
}

func TestStreamDetachedAccessLogs(t *testing.T) {
	for name, tc := range map[string]struct {
		accessLevel string // level of the access log
		streamLogs  string // contents of the stream_logs block
		wantHandled bool   // whether the handshake is access logged
		wantClosed  string // level of the stream close entry in the access log, if any
	}{
		"handshake and stream close, default level": {
			accessLevel: "DEBUG",
			streamLogs:  "logger_name access",
			wantHandled: true,
			wantClosed:  "debug",
		},
		"handshake skipped": {
			accessLevel: "DEBUG",
			streamLogs:  "skip_handshake\nlogger_name access",
			wantHandled: false,
			wantClosed:  "debug",
		},
		"configured level": {
			accessLevel: "INFO",
			streamLogs:  "logger_name access\nlevel info",
			wantHandled: true,
			wantClosed:  "info",
		},
		"stream close not sent to the access log by default": {
			accessLevel: "DEBUG",
			streamLogs:  "level info",
			wantHandled: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			newUpgradeBackend(t, "127.0.0.1:9081")
			path := logFile(t)
			tester := caddytest.NewTester(t)
			tester.InitServer(streamGlobalOptions+`
			localhost:9080 {
				log {
					output file "`+path+`"
					level `+tc.accessLevel+`
				}
				reverse_proxy 127.0.0.1:9081 {
					stream_detached
					stream_logs {
						`+tc.streamLogs+`
					}
				}
			}`, "caddyfile")

			// proves the access log works
			tester.AssertGetResponse("http://localhost:9080/plain", 200, "plain")
			eventually(t, "the plain request to be logged", func() bool {
				return len(findLog(path, "handled request", "/plain")) == 1
			})

			ws := dialWS(t, "/ws")
			ws.echo(t, "hello")

			if tc.wantHandled {
				// the handler of a detached stream returns right away, so the
				// handshake is logged while the stream is open
				eventually(t, "the handshake to be logged", func() bool {
					return len(findLog(path, "handled request", "/ws")) == 1
				})
				if status := findLog(path, "handled request", "/ws")[0]["status"]; status != float64(http.StatusSwitchingProtocols) {
					t.Errorf("logged handshake status = %v, want 101", status)
				}
			}

			ws.conn.Close()

			if tc.wantClosed != "" {
				eventually(t, "the stream close to be logged", func() bool {
					return len(findLog(path, "stream closed", "")) == 1
				})
				entry := findLog(path, "stream closed", "")[0]
				if entry["level"] != tc.wantClosed {
					t.Errorf("stream close logged at %v, want %s", entry["level"], tc.wantClosed)
				}
				if entry["bytes_to_backend"] != float64(5) || entry["bytes_from_backend"] != float64(5) {
					t.Errorf("logged bytes = %v to the backend and %v from it, want 5 and 5", entry["bytes_to_backend"], entry["bytes_from_backend"])
				}
			} else {
				time.Sleep(500 * time.Millisecond)
				if got := findLog(path, "stream closed", ""); len(got) != 0 {
					t.Errorf("stream close was access logged: %v", got)
				}
			}

			if !tc.wantHandled {
				if got := findLog(path, "handled request", "/ws"); len(got) != 0 {
					t.Errorf("the handshake was access logged although skipped: %v", got)
				}
			}
		})
	}
}

// Streams which are not detached are access logged when they end, with the
// status of the handshake, also behind encode.
func TestStreamThroughEncodeWithAccessLog(t *testing.T) {
	newUpgradeBackend(t, "127.0.0.1:9081")
	path := logFile(t)
	tester := caddytest.NewTester(t)
	tester.InitServer(streamGlobalOptions+`
	localhost:9080 {
		log {
			output file "`+path+`"
		}
		encode gzip
		reverse_proxy 127.0.0.1:9081
	}`, "caddyfile")

	ws := dialWS(t, "/ws", "Accept-Encoding: gzip")
	ws.echo(t, "through encode")
	ws.conn.Close()

	eventually(t, "the request to be logged", func() bool {
		return len(findLog(path, "handled request", "/ws")) == 1
	})
	if status := findLog(path, "handled request", "/ws")[0]["status"]; status != float64(http.StatusSwitchingProtocols) {
		t.Errorf("logged status = %v, want 101", status)
	}
}

func TestStreamDetachedThroughEncodeWithAccessLog(t *testing.T) {
	newUpgradeBackend(t, "127.0.0.1:9081")
	path := logFile(t)
	tester := caddytest.NewTester(t)
	tester.InitServer(streamGlobalOptions+`
	localhost:9080 {
		log {
			output file "`+path+`"
		}
		encode gzip
		reverse_proxy 127.0.0.1:9081 {
			stream_detached
		}
	}`, "caddyfile")

	ws := dialWS(t, "/ws", "Accept-Encoding: gzip")
	ws.echo(t, "through encode")

	// ordinary responses are still encoded
	tester.AssertGetResponse("http://localhost:9080/plain", 200, "plain")
	eventually(t, "the handshake to be logged", func() bool {
		return len(findLog(path, "handled request", "/ws")) == 1
	})
	ws.echo(t, "still open")
}

// HTTP/2 extended CONNECT (RFC 8441) is turned into an upgrade of the
// backend connection.
func TestStreamExtendedConnectH2(t *testing.T) {
	for name, options := range map[string]string{
		"default":  "",
		"detached": "stream_detached",
	} {
		t.Run(name, func(t *testing.T) {
			newUpgradeBackend(t, "127.0.0.1:9081")
			tester := caddytest.NewTester(t)
			tester.InitServer(streamGlobalOptions+`
			localhost:9443 {
				encode gzip
				reverse_proxy 127.0.0.1:9081 {
					`+options+`
				}
			}`, "caddyfile")

			s := dialH2ExtendedConnect(t, "localhost:9443", "/ws")
			s.echo(t, "hello over h2")
			s.echo(t, "and again")
		})
	}
}

// h2Stream is a stream opened with an HTTP/2 extended CONNECT request. The
// standard clients cannot send the :protocol pseudo-header, so this speaks
// just enough HTTP/2 to open one stream and exchange DATA on it.
type h2Stream struct {
	conn net.Conn
	fr   *http2.Framer
	data bytes.Buffer // received and not yet consumed
}

func dialH2ExtendedConnect(t *testing.T, authority, path string) *h2Stream {
	t.Helper()
	conn, err := tls.Dial("tcp", "127.0.0.1:9443", &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec
		ServerName:         "localhost",
		NextProtos:         []string{"h2"},
	})
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	if proto := conn.ConnectionState().NegotiatedProtocol; proto != "h2" {
		t.Fatalf("negotiated protocol = %q, want h2", proto)
	}

	_ = conn.SetDeadline(time.Now().Add(streamTestTimeout))
	s := &h2Stream{conn: conn, fr: http2.NewFramer(conn, conn)}
	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatal(err)
	}
	if err := s.fr.WriteSettings(); err != nil {
		t.Fatal(err)
	}

	// the server has to advertise support for extended CONNECT (RFC 8441)
	advertised := false
	for settings := false; !settings; {
		f, err := s.fr.ReadFrame()
		if err != nil {
			t.Fatalf("failed to read the server settings: %v", err)
		}
		sf, ok := f.(*http2.SettingsFrame)
		if !ok || sf.IsAck() {
			continue
		}
		settings = true
		_ = sf.ForeachSetting(func(setting http2.Setting) error {
			const settingEnableConnectProtocol http2.SettingID = 0x8
			advertised = advertised || (setting.ID == settingEnableConnectProtocol && setting.Val == 1)
			return nil
		})
		if err := s.fr.WriteSettingsAck(); err != nil {
			t.Fatal(err)
		}
	}
	if !advertised {
		t.Fatal("the server does not advertise support for extended CONNECT")
	}

	var block bytes.Buffer
	enc := hpack.NewEncoder(&block)
	for _, field := range []hpack.HeaderField{
		{Name: ":method", Value: "CONNECT"},
		{Name: ":scheme", Value: "https"},
		{Name: ":authority", Value: authority},
		{Name: ":path", Value: path},
		{Name: ":protocol", Value: "websocket"},
		{Name: "sec-websocket-version", Value: "13"},
	} {
		if err := enc.WriteField(field); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block.Bytes(), EndHeaders: true}); err != nil {
		t.Fatal(err)
	}

	// wait for the response headers
	dec := hpack.NewDecoder(4096, nil)
	for {
		f, err := s.fr.ReadFrame()
		if err != nil {
			t.Fatalf("failed to read the response: %v", err)
		}
		switch f := f.(type) {
		case *http2.HeadersFrame:
			fields, err := dec.DecodeFull(f.HeaderBlockFragment())
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range fields {
				if field.Name == ":status" && field.Value != "200" {
					t.Fatalf("status = %s, want 200", field.Value)
				}
			}
			return s
		case *http2.RSTStreamFrame:
			t.Fatalf("the stream was reset: %v", f.ErrCode)
		case *http2.GoAwayFrame:
			t.Fatalf("the connection was closed: %v", f.ErrCode)
		}
	}
}

// echo sends msg on the stream and expects it back.
func (s *h2Stream) echo(t *testing.T, msg string) {
	t.Helper()
	_ = s.conn.SetDeadline(time.Now().Add(streamTestTimeout))
	if err := s.fr.WriteData(1, false, []byte(msg)); err != nil {
		t.Fatalf("failed to write to the stream: %v", err)
	}
	for s.data.Len() < len(msg) {
		f, err := s.fr.ReadFrame()
		if err != nil {
			t.Fatalf("failed to read from the stream: %v", err)
		}
		switch f := f.(type) {
		case *http2.DataFrame:
			s.data.Write(f.Data())
		case *http2.PingFrame:
			if !f.IsAck() {
				_ = s.fr.WritePing(true, f.Data)
			}
		case *http2.SettingsFrame:
			if !f.IsAck() {
				_ = s.fr.WriteSettingsAck()
			}
		case *http2.RSTStreamFrame:
			t.Fatalf("the stream was reset: %v", f.ErrCode)
		case *http2.GoAwayFrame:
			t.Fatalf("the connection was closed: %v", f.ErrCode)
		}
	}
	if got := string(s.data.Next(len(msg))); got != msg {
		t.Fatalf("echo = %q, want %q", got, msg)
	}
}
