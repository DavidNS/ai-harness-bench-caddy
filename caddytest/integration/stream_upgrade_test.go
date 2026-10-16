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
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

	"github.com/caddyserver/caddy/v2/caddytest"
)

// logSink collects the JSON log entries that a server sends to it with the
// net log writer.
type logSink struct {
	addr    string
	mu      sync.Mutex
	entries []map[string]any
}

func startLogSink(t *testing.T) *logSink {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("starting the log sink: %v", err)
	}
	s := &logSink{addr: ln.Addr().String()}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go s.read(conn)
		}
	}()
	return s
}

func (s *logSink) read(conn net.Conn) {
	defer conn.Close()
	sc := bufio.NewScanner(conn)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var entry map[string]any
		if json.Unmarshal(sc.Bytes(), &entry) != nil {
			continue
		}
		s.mu.Lock()
		s.entries = append(s.entries, entry)
		s.mu.Unlock()
	}
}

// find returns the first entry that match.
func (s *logSink) find(match func(map[string]any) bool) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.entries {
		if match(e) {
			return e
		}
	}
	return nil
}

func (s *logSink) waitFor(t *testing.T, what string, match func(map[string]any) bool) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if e := s.find(match); e != nil {
			return e
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t.Fatalf("timed out waiting for %s; got entries: %v", what, s.entries)
	return nil
}

func isStreamClosed(e map[string]any) bool { return e["msg"] == "stream closed" }

func isHandledRequest(uri string) func(map[string]any) bool {
	return func(e map[string]any) bool {
		req, _ := e["request"].(map[string]any)
		return e["msg"] == "handled request" && req != nil && req["uri"] == uri
	}
}

// accessLogSite is a server that proxies to upstream and writes its access
// logs, under the logger name "access", to the sink.
func accessLogSite(upstream, sink, proxyOptions, extra string) string {
	return fmt.Sprintf(`{
		skip_install_trust
		admin localhost:2999
		http_port     9080
		https_port    9443
		grace_period  1ns
	}

	localhost:9080 {
		log access {
			output net %s
			format json
			level DEBUG
		}
		%s
		reverse_proxy %s {
			%s
		}
	}`, sink, extra, upstream, proxyOptions)
}

// A detached stream still writes its close entry to the access log after
// the config that created it was reloaded; the handshake is logged by default.
func TestDetachedStreamCloseLogAfterReload(t *testing.T) {
	tester := caddytest.NewTester(t)
	backend := startStreamBackend(t)
	sink := startLogSink(t)
	const options = "stream_detached"

	tester.InitServer(accessLogSite(backend.addr, sink.addr, options, `respond /version "v1"`), "caddyfile")
	c := dialStream(t, "/ws", "echo", nil)
	if err := c.echo("hello"); err != nil {
		t.Fatalf("echo before the reload: %v", err)
	}

	tester.InitServer(accessLogSite(backend.addr, sink.addr, options, `respond /version "v2"`), "caddyfile")
	tester.AssertGetResponse("http://localhost:9080/version", 200, "v2")
	if err := c.echo("world"); err != nil {
		t.Fatalf("echo after the reload: %v", err)
	}
	c.conn.Close()

	closed := sink.waitFor(t, "the stream close entry", isStreamClosed)
	if closed["logger"] != "http.log.access.access" {
		t.Errorf("logger = %v, want http.log.access.access", closed["logger"])
	}
	if closed["level"] != "debug" {
		t.Errorf("level = %v, want debug", closed["level"])
	}
	if closed["result"] != "completed" {
		t.Errorf("result = %v, want completed", closed["result"])
	}
	if toBackend, _ := closed["bytes_to_backend"].(float64); toBackend != float64(len("helloworld")) {
		t.Errorf("bytes_to_backend = %v, want %d", closed["bytes_to_backend"], len("helloworld"))
	}
	if fromBackend, _ := closed["bytes_from_backend"].(float64); fromBackend != float64(len("helloworld")) {
		t.Errorf("bytes_from_backend = %v, want %d", closed["bytes_from_backend"], len("helloworld"))
	}
	if closed["upstream"] != backend.addr {
		t.Errorf("upstream = %v, want %s", closed["upstream"], backend.addr)
	}

	// the handshake is logged unless told otherwise
	sink.waitFor(t, "the handshake request entry", isHandledRequest("/ws"))
}

// With skip_handshake, the handshake is not access-logged, and the close
// entry is written at the configured level.
func TestStreamLogsSkipHandshake(t *testing.T) {
	tester := caddytest.NewTester(t)
	backend := startStreamBackend(t)
	sink := startLogSink(t)

	options := `stream_logs {
		skip_handshake
		level warn
	}`
	tester.InitServer(accessLogSite(backend.addr, sink.addr, options, ""), "caddyfile")
	c := dialStream(t, "/ws", "echo", nil)
	if err := c.echo("hello"); err != nil {
		t.Fatalf("echo: %v", err)
	}
	c.conn.Close()

	closed := sink.waitFor(t, "the stream close entry", isStreamClosed)
	if closed["level"] != "warn" {
		t.Errorf("level = %v, want warn", closed["level"])
	}

	// an ordinary request is logged after the handshake would have been
	tester.AssertGetResponse("http://localhost:9080/plain", 200, "backend")
	sink.waitFor(t, "the ordinary request entry", isHandledRequest("/plain"))
	if e := sink.find(isHandledRequest("/ws")); e != nil {
		t.Errorf("the handshake was access-logged: %v", e)
	}
}

// Upgrades work through the encode handler of the same route, which must
// leave the 101 alone, with access logging enabled.
func TestUpgradeThroughEncode(t *testing.T) {
	tester := caddytest.NewTester(t)
	backend := startStreamBackend(t)
	sink := startLogSink(t)

	tester.InitServer(accessLogSite(backend.addr, sink.addr, "stream_detached", "encode gzip zstd"), "caddyfile")
	c := dialStream(t, "/ws", "echo", http.Header{"Accept-Encoding": {"gzip, zstd"}})
	if ce := c.resp.Header.Get("Content-Encoding"); ce != "" {
		t.Errorf("Content-Encoding of the 101 = %q, want none", ce)
	}
	if err := c.echo("through encode"); err != nil {
		t.Fatalf("echo: %v", err)
	}
	c.conn.Close()
	sink.waitFor(t, "the stream close entry", isStreamClosed)
}

// Upgrades work through an intercept handler whose handle_response
// matches every response, while ordinary responses are still intercepted.
func TestUpgradeThroughIntercept(t *testing.T) {
	tester := caddytest.NewTester(t)
	backend := startStreamBackend(t)
	sink := startLogSink(t)

	// the replacement is as long as the backend's response ("backend"),
	// whose Content-Length is kept by the intercepted response
	extra := `intercept {
		handle_response {
			respond "swapped" 200
		}
	}`
	tester.InitServer(accessLogSite(backend.addr, sink.addr, "", extra), "caddyfile")
	c := dialStream(t, "/ws", "echo", nil)
	if err := c.echo("through intercept"); err != nil {
		t.Fatalf("echo: %v", err)
	}
	c.conn.Close()
	sink.waitFor(t, "the stream close entry", isStreamClosed)

	tester.AssertGetResponse("http://localhost:9080/plain", 200, "swapped")
}

// A websocket over HTTP/2 (RFC 8441 extended CONNECT) is relayed to a
// backend that speaks the HTTP/1.1 upgrade.
//
// The server only advertises extended CONNECT support when the process runs
// with GODEBUG=http2xconnect=1, which is read when the process starts. So
// the test runs itself in a new process with it, with ports and an admin
// endpoint of its own so that it doesn't meet the server of this process.
func TestExtendedConnectWebsocketOverH2(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	if !strings.Contains(os.Getenv("GODEBUG"), "http2xconnect=1") {
		// os.Args is rewritten when the tests start the server in-process
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(exe,"-test.run=^TestExtendedConnectWebsocketOverH2$", "-test.count=1", "-test.timeout=2m")
		cmd.Env = append(os.Environ(), "GODEBUG=http2xconnect=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("test process failed: %v\n%s", err, out)
		}
		return
	}

	tester := caddytest.NewTester(t).WithDefaultOverrides(caddytest.Config{AdminPort: 2998})
	backend := startStreamBackend(t)

	config := fmt.Sprintf(`{
		skip_install_trust
		admin localhost:2998
		http_port     9580
		https_port    9543
		grace_period  1ns
	}

	localhost:9543 {
		reverse_proxy %s
	}`, backend.addr)
	tester.InitServer(config, "caddyfile")

	// The HTTP/2 clients of the standard library don't send extended CONNECT
	// requests, so speak the protocol directly.
	conn, err := tls.Dial("tcp", "127.0.0.1:9543",&tls.Config{
		InsecureSkipVerify: true, //nolint:gosec
		ServerName:         "localhost",
		NextProtos:         []string{"h2"},
	})
	if err != nil {
		t.Fatalf("dialing the server: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatal(err)
	}
	fr := http2.NewFramer(conn, conn)
	if err := fr.WriteSettings(); err != nil {
		t.Fatal(err)
	}

	var (
		connectAllowed bool
		status         string
		data           []byte
	)
	decoder := hpack.NewDecoder(4096, func(f hpack.HeaderField) {
		if f.Name == ":status" {
			status = f.Value
		}
	})
	// readFrames handles the frames of the connection until done reports true
	readFrames := func(done func() bool) {
		t.Helper()
		for !done() {
			frame, err := fr.ReadFrame()
			if err != nil {
				t.Fatalf("reading a frame: %v", err)
			}
			switch f := frame.(type) {
			case *http2.SettingsFrame:
				if f.IsAck() {
					continue
				}
				if v, ok := f.Value(http2.SettingID(0x8)); ok && v == 1 { // SETTINGS_ENABLE_CONNECT_PROTOCOL
					connectAllowed = true
				}
				if err := fr.WriteSettingsAck(); err != nil {
					t.Fatal(err)
				}
			case *http2.HeadersFrame:
				if _, err := decoder.Write(f.HeaderBlockFragment()); err != nil {
					t.Fatal(err)
				}
			case *http2.DataFrame:
				data = append(data, f.Data()...)
			case *http2.RSTStreamFrame:
				t.Fatalf("the stream was reset: %v", f.ErrCode)
			case *http2.GoAwayFrame:
				t.Fatalf("the connection was closed: %v", f.ErrCode)
			}
		}
	}

	readFrames(func() bool { return connectAllowed })

	var block bytes.Buffer
	encoder := hpack.NewEncoder(&block)
	for _, f := range [][2]string{
		{":method", "CONNECT"},
		{":protocol", "websocket"},
		{":scheme", "https"},
		{":path", "/ws"},
		{":authority", "localhost:9543"},
		{"sec-websocket-version", "13"},
	} {
		if err := encoder.WriteField(hpack.HeaderField{Name: f[0], Value: f[1]}); err != nil {
			t.Fatal(err)
		}
	}
	err = fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block.Bytes(), EndHeaders: true})
	if err != nil {
		t.Fatal(err)
	}

	readFrames(func() bool { return status != "" })
	if status != "200" {
		t.Fatalf("status = %s, want 200", status)
	}

	for _, msg := range []string{"hello over h2", "and again"} {
		data = nil
		if err := fr.WriteData(1, false, []byte(msg)); err != nil {
			t.Fatalf("writing to the stream: %v", err)
		}
		readFrames(func() bool { return len(data) >= len(msg) })
		if string(data) != msg {
			t.Errorf("echoed %q, want %q", data, msg)
		}
	}
}
