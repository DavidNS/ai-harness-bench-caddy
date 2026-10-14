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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

	"github.com/caddyserver/caddy/v2/caddytest"
)

// upgradeBackend is a backend that echoes whatever is sent through a
// connection upgraded with the Upgrade header of the request.
type upgradeBackend struct {
	ln       net.Listener
	upgrades atomic.Int32
	closed   atomic.Int32
}

func newUpgradeBackend(t *testing.T) *upgradeBackend {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	b := &upgradeBackend{ln: ln}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrade := r.Header.Get("Upgrade")
		if upgrade == "" {
			_, _ = io.WriteString(w, "plain response")
			return
		}
		conn, brw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("backend hijack failed: %v", err)
			return
		}
		defer conn.Close()
		_, _ = fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", upgrade)
		b.upgrades.Add(1)
		defer b.closed.Add(1)
		_, _ = io.Copy(conn, brw.Reader)
	})}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return b
}

func (b *upgradeBackend) addr() string { return b.ln.Addr().String() }

// waitFor polls cond until it is true, failing the test if it
// doesn't become true in time.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// streamClient is a client of an upgraded connection.
type streamClient struct {
	conn net.Conn
	br   *bufio.Reader
}

// dialStream upgrades a connection to caddy's HTTP port with the given
// protocol, and returns it once the 101 response has been read.
func dialStream(t *testing.T, path, protocol string) *streamClient {
	t.Helper()

	conn, err := net.DialTimeout("tcp", "127.0.0.1:9080", 5*time.Second)
	if err != nil {
		t.Fatalf("dialing: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	var req bytes.Buffer
	fmt.Fprintf(&req, "GET %s HTTP/1.1\r\nHost: localhost:9080\r\nConnection: Upgrade\r\nUpgrade: %s\r\n", path, protocol)
	if protocol == "websocket" {
		req.WriteString("Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n")
	}
	req.WriteString("\r\n")
	if _, err := conn.Write(req.Bytes()); err != nil {
		t.Fatalf("writing upgrade request: %v", err)
	}

	c := &streamClient{conn: conn, br: bufio.NewReader(conn)}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(c.br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("reading upgrade response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 101; body: %s", resp.StatusCode, body)
	}
	_ = conn.SetReadDeadline(time.Time{})
	return c
}

// echo sends msg through the stream and checks it comes back.
func (c *streamClient) echo(t *testing.T, msg string) {
	t.Helper()

	_ = c.conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer c.conn.SetDeadline(time.Time{})
	if _, err := io.WriteString(c.conn, msg); err != nil {
		t.Fatalf("writing %q to the stream: %v", msg, err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(c.br, got); err != nil {
		t.Fatalf("reading echo of %q from the stream: %v", msg, err)
	}
	if string(got) != msg {
		t.Fatalf("echo = %q, want %q", got, msg)
	}
}

// waitClosed waits for the stream to be closed by the other side. Data
// received meanwhile, such as the WebSocket close frame, is discarded.
func (c *streamClient) waitClosed(t *testing.T, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	buf := make([]byte, 512)
	for {
		_ = c.conn.SetReadDeadline(deadline)
		if _, err := c.br.Read(buf); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatalf("the stream was not closed within %s", timeout)
			}
			return
		}
	}
}

// streamSite returns a Caddyfile with a site proxying to the given
// upstreams, which differs for each marker so that loading it
// reloads the config.
func streamSite(marker, proxyBody string, upstreams ...string) string {
	return fmt.Sprintf(`
	{
		admin localhost:2999
		http_port 9080
		https_port 9443
		grace_period 1ns
	}
	http://localhost:9080 {
		header X-Config %s
		reverse_proxy %s {
			%s
		}
	}`, marker, strings.Join(upstreams, " "), proxyBody)
}

func TestStreamReloadClosesStreamsByDefault(t *testing.T) {
	backend := newUpgradeBackend(t)
	tester := caddytest.NewTester(t)
	tester.InitServer(streamSite("one", "", backend.addr()), "caddyfile")

	client := dialStream(t, "/ws", "websocket")
	client.echo(t, "before reload")

	tester.InitServer(streamSite("two", "", backend.addr()), "caddyfile")

	client.waitClosed(t, 5*time.Second)
	waitFor(t, "the backend to see the stream closed", func() bool { return backend.closed.Load() == 1 })
}

func TestStreamReloadHonorsCloseDelay(t *testing.T) {
	backend := newUpgradeBackend(t)
	tester := caddytest.NewTester(t)
	tester.InitServer(streamSite("one", "stream_close_delay 2s", backend.addr()), "caddyfile")

	client := dialStream(t, "/ws", "tcp-tunnel")
	client.echo(t, "before reload")

	reloaded := time.Now()
	tester.InitServer(streamSite("two", "stream_close_delay 2s", backend.addr()), "caddyfile")

	// still open during the delay
	client.echo(t, "during the delay")

	client.waitClosed(t, 10*time.Second)
	if elapsed := time.Since(reloaded); elapsed < 1500*time.Millisecond {
		t.Errorf("the stream was closed %s after the reload, want after the 2s delay", elapsed)
	}
}

func TestStreamDetachedSurvivesReload(t *testing.T) {
	for _, protocol := range []string{"websocket", "tcp-tunnel"} {
		t.Run(protocol, func(t *testing.T) {
			backend := newUpgradeBackend(t)
			tester := caddytest.NewTester(t)
			tester.InitServer(streamSite("one", "stream_detached", backend.addr()), "caddyfile")

			client := dialStream(t, "/ws", protocol)
			client.echo(t, "before reload")

			for _, marker := range []string{"two", "three"} {
				tester.InitServer(streamSite(marker, "stream_detached", backend.addr()), "caddyfile")
				client.echo(t, "after reload "+marker)
			}

			// new streams work too, on the new config
			other := dialStream(t, "/ws", protocol)
			other.echo(t, "new stream")

			// the stream is still a regular one: it ends when the client leaves
			client.conn.Close()
			waitFor(t, "the backend to see the stream closed", func() bool { return backend.closed.Load() == 1 })
			other.echo(t, "still open")
		})
	}
}

// A detached stream going through an upstream that is not in the new
// config is closed, those through upstreams that remain are not.
func TestStreamDetachedClosedWhenUpstreamRemoved(t *testing.T) {
	removed, kept := newUpgradeBackend(t), newUpgradeBackend(t)

	config := func(marker string, upstreams ...*upgradeBackend) string {
		var routes strings.Builder
		for i, u := range upstreams {
			fmt.Fprintf(&routes, "handle /u%d* {\n reverse_proxy %s {\n stream_detached\n }\n}\n", i, u.addr())
		}
		return fmt.Sprintf(`
		{
			admin localhost:2999
			http_port 9080
			https_port 9443
			grace_period 1ns
		}
		http://localhost:9080 {
			header X-Config %s
			%s
		}`, marker, routes.String())
	}

	tester := caddytest.NewTester(t)
	tester.InitServer(config("one", removed, kept), "caddyfile")

	removedClient := dialStream(t, "/u0", "websocket")
	keptClient := dialStream(t, "/u1", "websocket")
	removedClient.echo(t, "hello")
	keptClient.echo(t, "hello")

	// the kept upstream is now the first and only one
	tester.InitServer(strings.ReplaceAll(config("two", kept), "/u0*", "/u1*"), "caddyfile")

	removedClient.waitClosed(t, 5*time.Second)
	waitFor(t, "the removed backend to see its stream closed", func() bool { return removed.closed.Load() == 1 })
	keptClient.echo(t, "still here")
	if kept.closed.Load() != 0 {
		t.Error("the stream of the upstream that is still in the config was closed")
	}
}

func TestStreamDetachedHonorsCloseDelayWhenUpstreamRemoved(t *testing.T) {
	removed, kept := newUpgradeBackend(t), newUpgradeBackend(t)
	tester := caddytest.NewTester(t)
	tester.InitServer(streamSite("one", "stream_detached\nstream_close_delay 2s", removed.addr()), "caddyfile")

	client := dialStream(t, "/ws", "websocket")
	client.echo(t, "hello")

	reloaded := time.Now()
	tester.InitServer(streamSite("two", "stream_detached\nstream_close_delay 2s", kept.addr()), "caddyfile")

	client.echo(t, "during the delay")
	client.waitClosed(t, 10*time.Second)
	if elapsed := time.Since(reloaded); elapsed < 1500*time.Millisecond {
		t.Errorf("the stream was closed %s after the reload, want after the 2s delay", elapsed)
	}
}

// Upgrades go through the other handlers of the route.
func TestStreamThroughEncodeAndIntercept(t *testing.T) {
	for _, detached := range []bool{false, true} {
		t.Run(fmt.Sprintf("detached=%t", detached), func(t *testing.T) {
			backend := newUpgradeBackend(t)
			tester := caddytest.NewTester(t)

			body := ""
			if detached {
				body = "stream_detached"
			}
			tester.InitServer(fmt.Sprintf(`
			{
				admin localhost:2999
				http_port 9080
				https_port 9443
				grace_period 1ns
			}
			http://localhost:9080 {
				encode gzip zstd
				intercept {
					# matches every response, including the 101 and the 2xx of CONNECT
					handle_response {
						respond "intercepted" 418
					}
				}
				reverse_proxy %s {
					%s
				}
			}`, backend.addr(), body), "caddyfile")

			client := dialStream(t, "/ws", "websocket")
			client.echo(t, "through encode and intercept")
			client.echo(t, "twice")

			client.conn.Close()
			waitFor(t, "the backend to see the stream closed", func() bool { return backend.closed.Load() == 1 })
		})
	}
}

// A request for a protocol upgrade that the backend doesn't grant is a
// regular response.
func TestStreamNotUpgraded(t *testing.T) {
	backend := newUpgradeBackend(t)
	tester := caddytest.NewTester(t)
	tester.InitServer(streamSite("one", "stream_detached", backend.addr()), "caddyfile")

	tester.AssertGetResponse("http://localhost:9080/plain", http.StatusOK, "plain response")
	if backend.upgrades.Load() != 0 {
		t.Error("unexpected upgrade")
	}
}

// accessLogEntries reads the JSON log entries of the file.
func accessLogEntries(t *testing.T, path string) []map[string]any {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading log: %v", err)
	}
	var entries []map[string]any
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("parsing log line %q: %v", line, err)
		}
		entries = append(entries, entry)
	}
	return entries
}

func countEntries(entries []map[string]any, msg string) int {
	n := 0
	for _, entry := range entries {
		if entry["msg"] == msg {
			n++
		}
	}
	return n
}

func TestStreamDetachedAccessLogs(t *testing.T) {
	for _, tc := range []struct {
		name          string
		streamLogs    string
		wantHandshake bool
	}{
		{name: "handshake logged by default", streamLogs: "stream_logs {\n logger_name access\n level info\n }", wantHandshake: true},
		{name: "handshake skipped", streamLogs: "stream_logs {\n skip_handshake\n logger_name access\n level info\n }", wantHandshake: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := newUpgradeBackend(t)
			tester := caddytest.NewTester(t)

			f, err := os.CreateTemp("", "caddy-stream-access-*.log")
			if err != nil {
				t.Fatal(err)
			}
			f.Close()
			logPath := f.Name()
			t.Cleanup(func() { os.Remove(logPath) })

			tester.InitServer(fmt.Sprintf(`
			{
				admin localhost:2999
				http_port 9080
				https_port 9443
				grace_period 1ns
			}
			http://localhost:9080 {
				log {
					output file %s
					format json
				}
				reverse_proxy %s {
					stream_detached
					%s
				}
			}`, filepath.ToSlash(logPath), backend.addr(), tc.streamLogs), "caddyfile")

			client := dialStream(t, "/ws", "websocket")
			client.echo(t, "hello")
			client.echo(t, "world")

			// detached, the handler is done with the request once the
			// stream is set up, so the handshake is logged while it's open
			if tc.wantHandshake {
				waitFor(t, "the handshake to be logged", func() bool {
					return countEntries(accessLogEntries(t, logPath), "handled request") == 1
				})
				for _, entry := range accessLogEntries(t, logPath) {
					if entry["msg"] == "handled request" && entry["status"] != float64(http.StatusSwitchingProtocols) {
						t.Errorf("handshake logged with status %v, want 101", entry["status"])
					}
				}
			} else {
				time.Sleep(500 * time.Millisecond)
			}
			if got := countEntries(accessLogEntries(t, logPath), "stream closed"); got != 0 {
				t.Fatalf("%d stream closed entries while the stream is open", got)
			}

			client.conn.Close()
			waitFor(t, "the closing of the stream to be logged", func() bool {
				return countEntries(accessLogEntries(t, logPath), "stream closed") == 1
			})

			entries := accessLogEntries(t, logPath)
			if got := countEntries(entries, "handled request"); tc.wantHandshake && got != 1 || !tc.wantHandshake && got != 0 {
				t.Errorf("%d handled request entries, want handshake logged: %t", got, tc.wantHandshake)
			}
			for _, entry := range entries {
				if entry["msg"] != "stream closed" {
					continue
				}
				if entry["level"] != "info" {
					t.Errorf("stream closed logged at %v, want info", entry["level"])
				}
				if entry["logger"] != "http.log.access.log0" {
					t.Errorf("stream closed logged by %v, want the access logger", entry["logger"])
				}
				if entry["bytes_to_backend"] != float64(len("helloworld")) || entry["bytes_from_backend"] != float64(len("helloworld")) {
					t.Errorf("bytes to/from the backend = %v/%v, want %d", entry["bytes_to_backend"], entry["bytes_from_backend"], len("helloworld"))
				}
				if entry["detached"] != true {
					t.Errorf("detached = %v, want true", entry["detached"])
				}
			}
		})
	}
}

// The 101 is the final response of an upgrade: handlers that would replace
// or encode a response don't get to write another one afterwards, which would
// show in the access log as a different status.
func TestStreamUpgradeIsFinalResponse(t *testing.T) {
	for _, detached := range []bool{false, true} {
		t.Run(fmt.Sprintf("detached=%t", detached), func(t *testing.T) {
			backend := newUpgradeBackend(t)
			tester := caddytest.NewTester(t)

			f, err := os.CreateTemp("", "caddy-stream-access-*.log")
			if err != nil {
				t.Fatal(err)
			}
			f.Close()
			logPath := f.Name()
			t.Cleanup(func() { os.Remove(logPath) })

			body := ""
			if detached {
				body = "stream_detached"
			}
			tester.InitServer(fmt.Sprintf(`
			{
				admin localhost:2999
				http_port 9080
				https_port 9443
				grace_period 1ns
			}
			http://localhost:9080 {
				log {
					output file %s
					format json
				}
				encode gzip
				intercept {
					handle_response {
						respond "intercepted" 418
					}
				}
				reverse_proxy %s {
					%s
				}
			}`, filepath.ToSlash(logPath), backend.addr(), body), "caddyfile")

			client := dialStream(t, "/ws", "websocket")
			client.echo(t, "hello")
			client.conn.Close()
			waitFor(t, "the backend to see the stream closed", func() bool { return backend.closed.Load() == 1 })
			waitFor(t, "the access log entry", func() bool {
				return countEntries(accessLogEntries(t, logPath), "handled request") == 1
			})

			for _, entry := range accessLogEntries(t, logPath) {
				if entry["msg"] == "handled request" && entry["status"] != float64(http.StatusSwitchingProtocols) {
					t.Errorf("logged status = %v, want 101", entry["status"])
				}
			}
		})
	}
}

// A stream logged at the default level doesn't show in an info log.
func TestStreamLogsDefaultLevelIsDebug(t *testing.T) {
	backend := newUpgradeBackend(t)
	tester := caddytest.NewTester(t)

	f, err := os.CreateTemp("", "caddy-stream-access-*.log")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	logPath := f.Name()
	t.Cleanup(func() { os.Remove(logPath) })

	tester.InitServer(fmt.Sprintf(`
	{
		admin localhost:2999
		http_port 9080
		https_port 9443
		grace_period 1ns
	}
	http://localhost:9080 {
		log {
			output file %s
			format json
			level info
		}
		reverse_proxy %s {
			stream_logs {
				logger_name access
			}
		}
	}`, filepath.ToSlash(logPath), backend.addr()), "caddyfile")

	client := dialStream(t, "/ws", "websocket")
	client.echo(t, "hello")
	client.conn.Close()
	waitFor(t, "the backend to see the stream closed", func() bool { return backend.closed.Load() == 1 })
	waitFor(t, "the access log entry", func() bool {
		return countEntries(accessLogEntries(t, logPath), "handled request") == 1
	})
	if got := countEntries(accessLogEntries(t, logPath), "stream closed"); got != 0 {
		t.Errorf("%d stream closed entries at the default level, want none in an info log", got)
	}
}

// metricValue returns the value of the sample of the metric with the
// given labels in the metrics of the admin endpoint, or -1 if absent.
func metricValue(t *testing.T, name string, labels ...string) float64 {
	t.Helper()

	resp, err := http.Get("http://localhost:2999/metrics")
	if err != nil {
		t.Fatalf("getting metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

next:
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, name+"{") && !strings.HasPrefix(line, name+" ") {
			continue
		}
		for _, l := range labels {
			if !strings.Contains(line, l) {
				continue next
			}
		}
		v, err := strconv.ParseFloat(line[strings.LastIndex(line, " ")+1:], 64)
		if err != nil {
			t.Fatalf("parsing metric line %q: %v", line, err)
		}
		return v
	}
	return -1
}

func TestStreamMetrics(t *testing.T) {
	backend := newUpgradeBackend(t)
	tester := caddytest.NewTester(t)
	tester.InitServer(streamSite("one", "stream_detached", backend.addr()), "caddyfile")

	upstream := fmt.Sprintf(`upstream="%s"`, backend.addr())
	baseline := func(name string, labels ...string) float64 {
		if v := metricValue(t, name, append([]string{upstream}, labels...)...); v > 0 {
			return v
		}
		return 0
	}
	closedBefore := baseline("caddy_reverse_proxy_streams_closed_total", `result="completed"`)
	toBefore := baseline("caddy_reverse_proxy_stream_bytes_total", `direction="to_backend"`)
	fromBefore := baseline("caddy_reverse_proxy_stream_bytes_total", `direction="from_backend"`)

	client := dialStream(t, "/ws", "websocket")
	client.echo(t, "12345")
	client.echo(t, "678")
	if got := metricValue(t, "caddy_reverse_proxy_streams_active", upstream); got != 1 {
		t.Errorf("active streams = %v, want 1", got)
	}

	// the active gauge and the counters are kept across reloads
	tester.InitServer(streamSite("two", "stream_detached", backend.addr()), "caddyfile")
	client.echo(t, "9")
	if got := metricValue(t, "caddy_reverse_proxy_streams_active", upstream); got != 1 {
		t.Errorf("active streams after reload = %v, want 1", got)
	}

	// closing the write side of the client ends the stream cleanly
	client.conn.(*net.TCPConn).CloseWrite()
	client.waitClosed(t, 5*time.Second)
	waitFor(t, "the stream to be accounted", func() bool {
		return metricValue(t, "caddy_reverse_proxy_streams_active", upstream) == 0
	})

	if got := metricValue(t, "caddy_reverse_proxy_streams_closed_total", upstream, `result="completed"`); got != closedBefore+1 {
		t.Errorf("completed streams = %v, want %v", got, closedBefore+1)
	}
	if got := metricValue(t, "caddy_reverse_proxy_stream_bytes_total", upstream, `direction="to_backend"`); got != toBefore+9 {
		t.Errorf("bytes to backend = %v, want %v", got, toBefore+9)
	}
	if got := metricValue(t, "caddy_reverse_proxy_stream_bytes_total", upstream, `direction="from_backend"`); got != fromBefore+9 {
		t.Errorf("bytes from backend = %v, want %v", got, fromBefore+9)
	}
	if got := metricValue(t, "caddy_reverse_proxy_stream_duration_seconds_count", upstream); got < 1 {
		t.Errorf("stream duration observations = %v, want at least 1", got)
	}
}

func TestStreamMetricsClosedByReload(t *testing.T) {
	backend := newUpgradeBackend(t)
	tester := caddytest.NewTester(t)
	tester.InitServer(streamSite("one", "", backend.addr()), "caddyfile")

	upstream := fmt.Sprintf(`upstream="%s"`, backend.addr())
	client := dialStream(t, "/ws", "websocket")
	client.echo(t, "hello")

	tester.InitServer(streamSite("two", "", backend.addr()), "caddyfile")
	client.waitClosed(t, 5*time.Second)

	waitFor(t, "the stream to be accounted", func() bool {
		return metricValue(t, "caddy_reverse_proxy_streams_closed_total", upstream, `result="closed"`) >= 1
	})
	if got := metricValue(t, "caddy_reverse_proxy_streams_active", upstream); got != 0 {
		t.Errorf("active streams = %v, want 0", got)
	}
}

// h2Tunnel is a client of an HTTP/2 extended CONNECT stream (RFC 8441). The
// HTTP/2 client of the standard library and x/net can't send the :protocol
// pseudo-header, so frames are exchanged by hand.
type h2Tunnel struct {
	conn   net.Conn
	fr     *http2.Framer
	mu     sync.Mutex // serializes writes to fr
	pr     *io.PipeReader
	status chan string
}

func (h *h2Tunnel) writeData(t *testing.T, data string, endStream bool) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.fr.WriteData(1, endStream, []byte(data)); err != nil {
		t.Fatalf("writing to the stream: %v", err)
	}
}

// read reads n bytes from the stream.
func (h *h2Tunnel) read(t *testing.T, n int) string {
	t.Helper()

	buf := make([]byte, n)
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(h.pr, buf)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("reading from the stream: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out reading from the stream")
	}
	return string(buf)
}

// h2ExtendedConnect opens a WebSocket over HTTP/2 through caddy with an
// extended CONNECT request, and returns once the response has been received.
func h2ExtendedConnect(t *testing.T, path string) *h2Tunnel {
	t.Helper()

	conn, err := tls.Dial("tcp", "127.0.0.1:9443", &tls.Config{InsecureSkipVerify: true, ServerName: "localhost", NextProtos: []string{"h2"}}) //nolint:gosec
	if err != nil {
		t.Fatalf("dialing: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatal(err)
	}
	fr := http2.NewFramer(conn, conn)
	fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	if err := fr.WriteSettings(); err != nil {
		t.Fatal(err)
	}

	// wait for the settings of the server, which has to allow extended CONNECT
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("reading the settings of the server: %v", err)
		}
		settings, ok := f.(*http2.SettingsFrame)
		if !ok || settings.IsAck() {
			continue
		}
		if v, ok := settings.Value(http2.SettingEnableConnectProtocol); !ok || v != 1 {
			var all []string
			_ = settings.ForeachSetting(func(s http2.Setting) error {
				all = append(all, s.String())
				return nil
			})
			t.Fatalf("the server doesn't support extended CONNECT; its settings: %v", all)
		}
		if err := fr.WriteSettingsAck(); err != nil {
			t.Fatal(err)
		}
		break
	}
	_ = conn.SetReadDeadline(time.Time{})

	var block bytes.Buffer
	enc := hpack.NewEncoder(&block)
	for _, field := range [][2]string{
		{":method", "CONNECT"},
		{":scheme", "https"},
		{":authority", "localhost:9443"},
		{":path", path},
		{":protocol", "websocket"},
		{"sec-websocket-version", "13"},
	} {
		if err := enc.WriteField(hpack.HeaderField{Name: field[0], Value: field[1]}); err != nil {
			t.Fatal(err)
		}
	}
	if err := fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block.Bytes(), EndHeaders: true}); err != nil {
		t.Fatal(err)
	}

	pr, pw := io.Pipe()
	h := &h2Tunnel{conn: conn, fr: fr, pr: pr, status: make(chan string, 1)}
	go func() {
		for {
			f, err := fr.ReadFrame()
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			switch f := f.(type) {
			case *http2.MetaHeadersFrame:
				select {
				case h.status <- f.PseudoValue("status"):
				default:
				}
				if f.StreamEnded() {
					pw.Close()
					return
				}
			case *http2.DataFrame:
				if _, err := pw.Write(f.Data()); err != nil {
					return
				}
				if f.StreamEnded() {
					pw.Close()
					return
				}
			case *http2.SettingsFrame:
				if !f.IsAck() {
					h.mu.Lock()
					_ = fr.WriteSettingsAck()
					h.mu.Unlock()
				}
			case *http2.PingFrame:
				if !f.IsAck() {
					h.mu.Lock()
					_ = fr.WritePing(true, f.Data)
					h.mu.Unlock()
				}
			case *http2.RSTStreamFrame:
				pw.CloseWithError(fmt.Errorf("stream reset: %v", f.ErrCode))
				return
			case *http2.GoAwayFrame:
				pw.CloseWithError(fmt.Errorf("go away: %v", f.ErrCode))
				return
			}
		}
	}()

	select {
	case status := <-h.status:
		if status != "200" {
			t.Fatalf("status = %s, want 200", status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the response to the extended CONNECT")
	}
	return h
}

func TestStreamExtendedConnectOverH2(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	// The HTTP/2 server only advertises extended CONNECT if asked to with
	// this setting, which it reads once, when the process starts. So the test
	// runs again in a new process that has it.
	const setting = "http2xconnect=1"
	if !strings.Contains(os.Getenv("GODEBUG"), setting) {
		// (os.Args is overwritten once the first test has started caddy)
		self, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		// the config of caddy running in this process, if any, would keep
		// the ports the new process needs; it has its own admin endpoint
		if req, err := http.NewRequest(http.MethodDelete, "http://localhost:2999/config/", nil); err == nil {
			if resp, err := http.DefaultClient.Do(req); err == nil {
				resp.Body.Close()
			}
		}
		cmd := exec.Command(self, "-test.run=^TestStreamExtendedConnectOverH2$", "-test.v")
		cmd.Env = append(os.Environ(), "GODEBUG="+setting)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("running the test with %s: %v\n%s", setting, err, out)
		}
		return
	}

	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "default"},
		{name: "detached", body: "stream_detached"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := newUpgradeBackend(t)
			tester := caddytest.NewTester(t).WithDefaultOverrides(caddytest.Config{AdminPort: 2998})
			tester.InitServer(fmt.Sprintf(`
			{
				admin localhost:2998
				http_port 9080
				https_port 9443
				grace_period 1ns
				skip_install_trust
				local_certs
			}
			localhost:9443 {
				encode gzip
				intercept {
					# matches every response, including the 200 of the CONNECT
					handle_response {
						respond "intercepted" 418
					}
				}
				reverse_proxy %s {
					%s
				}
			}`, backend.addr(), tc.body), "caddyfile")

			tunnel := h2ExtendedConnect(t, "/ws")
			tunnel.writeData(t, "over h2", false)
			if got := tunnel.read(t, len("over h2")); got != "over h2" {
				t.Fatalf("echo = %q", got)
			}
			tunnel.writeData(t, "again", false)
			if got := tunnel.read(t, len("again")); got != "again" {
				t.Fatalf("echo = %q", got)
			}

			// ending the stream ends the tunnel to the backend
			tunnel.writeData(t, "", true)
			waitFor(t, "the backend to see the stream closed", func() bool { return backend.closed.Load() == 1 })
		})
	}
}

// HTTP/1.1 upgrades keep working when caddy serves HTTP/2 too, detached or not.
func TestStreamH1UpgradeOverTLS(t *testing.T) {
	for _, body := range []string{"", "stream_detached"} {
		t.Run(body, func(t *testing.T) {
			backend := newUpgradeBackend(t)
			tester := caddytest.NewTester(t)
			tester.InitServer(fmt.Sprintf(`
			{
				admin localhost:2999
				http_port 9080
				https_port 9443
				grace_period 1ns
				skip_install_trust
				local_certs
			}
			localhost:9443 {
				encode gzip
				reverse_proxy %s {
					%s
				}
			}`, backend.addr(), body), "caddyfile")

			conn, err := tls.Dial("tcp", "127.0.0.1:9443", &tls.Config{InsecureSkipVerify: true, ServerName: "localhost", NextProtos: []string{"http/1.1"}}) //nolint:gosec
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_, err = io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: localhost:9443\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nAccept-Encoding: gzip\r\n\r\n")
			if err != nil {
				t.Fatal(err)
			}
			br := bufio.NewReader(conn)
			resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusSwitchingProtocols {
				t.Fatalf("status = %d, want 101", resp.StatusCode)
			}
			if resp.Header.Get("Content-Encoding") != "" {
				t.Errorf("the upgrade response is encoded with %s", resp.Header.Get("Content-Encoding"))
			}
			client := &streamClient{conn: conn, br: br}
			client.echo(t, "over tls")
		})
	}
}
