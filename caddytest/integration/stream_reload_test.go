package integration

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddytest"
)

const (
	streamBackendA = "127.0.0.1:54322"
	streamBackendB = "127.0.0.1:54323"

	streamTestWait = 10 * time.Second
)

// upgradeBackend is an HTTP/1.1 server which switches every request to the
// protocol the client asks for, and then echoes what it reads.
type upgradeBackend struct {
	server *http.Server

	mu    sync.Mutex
	conns []net.Conn
}

func startUpgradeBackend(t *testing.T, addr string) *upgradeBackend {
	t.Helper()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listening on %s: %v", addr, err)
	}
	b := &upgradeBackend{}
	b.server = &http.Server{Handler: http.HandlerFunc(b.serve)}
	go b.server.Serve(ln)
	t.Cleanup(func() {
		b.server.Close()
		b.mu.Lock()
		defer b.mu.Unlock()
		for _, conn := range b.conns {
			conn.Close()
		}
	})
	return b
}

func (b *upgradeBackend) serve(w http.ResponseWriter, r *http.Request) {
	conn, brw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return
	}
	b.mu.Lock()
	b.conns = append(b.conns, conn)
	b.mu.Unlock()
	defer conn.Close()

	fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", r.Header.Get("Upgrade"))
	io.Copy(conn, brw.Reader)
}

// tunnel is a client of an upgraded connection.
type tunnel struct {
	t    *testing.T
	conn net.Conn
	br   *bufio.Reader
}

// openTunnel requests an upgrade to proto at path of the test server, and
// expects exactly one 101 response. headers are added to the request.
func openTunnel(t *testing.T, path, proto string, headers ...string) *tunnel {
	t.Helper()

	conn, err := net.DialTimeout("tcp", "127.0.0.1:9080", streamTestWait)
	if err != nil {
		t.Fatalf("dialing: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: localhost:9080\r\nConnection: Upgrade\r\nUpgrade: %s\r\n", path, proto)
	for _, h := range headers {
		req += h + "\r\n"
	}
	if _, err := io.WriteString(conn, req+"\r\n"); err != nil {
		t.Fatalf("writing the upgrade request: %v", err)
	}

	tn := &tunnel{t: t, conn: conn, br: bufio.NewReader(conn)}
	conn.SetReadDeadline(time.Now().Add(streamTestWait))
	resp, err := http.ReadResponse(tn.br, nil)
	if err != nil {
		t.Fatalf("reading the upgrade response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	return tn
}

// echo checks that msg makes it to the backend and back. A second response
// after the 101 would be read here instead of the echo.
func (tn *tunnel) echo(msg string) {
	tn.t.Helper()
	if _, err := io.WriteString(tn.conn, msg); err != nil {
		tn.t.Fatalf("writing %q: %v", msg, err)
	}
	tn.conn.SetReadDeadline(time.Now().Add(streamTestWait))
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(tn.br, buf); err != nil {
		tn.t.Fatalf("reading the echo of %q: %v", msg, err)
	}
	if string(buf) != msg {
		tn.t.Fatalf("echo = %q, want %q", buf, msg)
	}
}

// expectClosed waits for the proxy to close the tunnel.
func (tn *tunnel) expectClosed() {
	tn.t.Helper()
	tn.conn.SetReadDeadline(time.Now().Add(streamTestWait))
	if _, err := tn.br.ReadByte(); err == nil {
		tn.t.Fatal("the tunnel is still passing data, want it closed")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		tn.t.Fatalf("the tunnel was not closed: %v", err)
	}
}

// streamSite returns a Caddyfile serving routes on localhost:9080. The version
// is answered at /version, so that every config differs and is really reloaded.
func streamSite(version, routes string) string {
	return fmt.Sprintf(`{
		skip_install_trust
		admin localhost:2999
		http_port 9080
		https_port 9443
		grace_period 1ns
	}

	localhost:9080 {
		handle /version {
			respond "%s"
		}
		%s
	}`, version, routes)
}

func streamProxy(addr, path, options string) string {
	return fmt.Sprintf(`handle %s {
			reverse_proxy %s {
				%s
			}
		}`, path, addr, options)
}

// With stream_detached, a stream survives a reload and is closed only when its
// upstream is not part of the config anymore. HTTP/1.1 upgrades work in this mode.
func TestStreamDetachedReload(t *testing.T) {
	startUpgradeBackend(t, streamBackendA)
	startUpgradeBackend(t, streamBackendB)

	tester := caddytest.NewTester(t)
	tester.InitServer(streamSite("v1",
		streamProxy(streamBackendA, "/a", "stream_detached")+"\n"+
			streamProxy(streamBackendB, "/b", "stream_detached")), "caddyfile")

	a := openTunnel(t, "/a", "tunnel")
	b := openTunnel(t, "/b", "tunnel")
	a.echo("hello a")
	b.echo("hello b")

	// the new config keeps B and drops A
	tester.InitServer(streamSite("v2", streamProxy(streamBackendB, "/b", "stream_detached")), "caddyfile")
	tester.AssertGetResponse("http://localhost:9080/version", 200, "v2")

	a.expectClosed()
	b.echo("still there")

	// and a new stream through the retained upstream works
	openTunnel(t, "/b", "tunnel").echo("new stream")
}

// Without stream_detached, streams are closed when the config is reloaded.
func TestStreamDefaultClosedOnReload(t *testing.T) {
	startUpgradeBackend(t, streamBackendA)

	tester := caddytest.NewTester(t)
	tester.InitServer(streamSite("v1", streamProxy(streamBackendA, "/a", "")), "caddyfile")

	a := openTunnel(t, "/a", "tunnel")
	a.echo("hello")

	// even though the new config has the same upstream
	tester.InitServer(streamSite("v2", streamProxy(streamBackendA, "/a", "")), "caddyfile")

	a.expectClosed()
}

// With stream_close_delay, streams are closed after the delay once the config is reloaded.
func TestStreamCloseDelayReload(t *testing.T) {
	const delay = 4 * time.Second
	startUpgradeBackend(t, streamBackendA)

	tester := caddytest.NewTester(t)
	options := fmt.Sprintf("stream_close_delay %s", delay)
	tester.InitServer(streamSite("v1", streamProxy(streamBackendA, "/a", options)), "caddyfile")

	a := openTunnel(t, "/a", "tunnel")
	a.echo("hello")

	reloaded := time.Now()
	tester.InitServer(streamSite("v2", streamProxy(streamBackendA, "/a", options)), "caddyfile")

	a.echo("still open during the delay")
	if time.Since(reloaded) >= delay {
		t.Skip("the reload took longer than the delay")
	}

	a.expectClosed()
	if elapsed := time.Since(reloaded); elapsed < delay-100*time.Millisecond {
		t.Errorf("the stream was closed %v after the reload, before the delay of %v", elapsed, delay)
	}
}

// readLogLines returns the JSON log entries of the file.
func readLogLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var entries []map[string]any
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		entries = append(entries, entry)
	}
	return entries
}

func findLogEntry(entries []map[string]any, match func(map[string]any) bool) map[string]any {
	for _, entry := range entries {
		if match(entry) {
			return entry
		}
	}
	return nil
}

func waitForLogEntry(t *testing.T, path, what string, match func(map[string]any) bool) map[string]any {
	t.Helper()
	deadline := time.Now().Add(streamTestWait)
	for time.Now().Before(deadline) {
		if entry := findLogEntry(readLogLines(t, path), match); entry != nil {
			return entry
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s in the access log:\n%v", what, readLogLines(t, path))
	return nil
}

func handledRequest(uri string, status float64) func(map[string]any) bool {
	return func(entry map[string]any) bool {
		req, _ := entry["request"].(map[string]any)
		return entry["msg"] == "handled request" && req["uri"] == uri && entry["status"] == status
	}
}

func streamClosed(entry map[string]any) bool {
	return entry["msg"] == "stream closed"
}

// A detached stream is logged to the access log: the handshake when it is
// established unless skip_handshake is set, and the close of the stream.
func TestStreamDetachedAccessLog(t *testing.T) {
	for _, skipHandshake := range []bool{false, true} {
		t.Run(fmt.Sprintf("skip_handshake=%v", skipHandshake), func(t *testing.T) {
			startUpgradeBackend(t, streamBackendA)
			logPath := filepath.Join(t.TempDir(), "access.log")

			streamLogs := "level info\nlogger_name access"
			if skipHandshake {
				streamLogs = "skip_handshake\n" + streamLogs
			}
			config := fmt.Sprintf(`{
				skip_install_trust
				admin localhost:2999
				http_port 9080
				https_port 9443
				grace_period 1ns
				log requests {
					output file %s
					format json
					include http.log.access
				}
			}

			localhost:9080 {
				log
				handle /version {
					respond "v1"
				}
				handle /a {
					reverse_proxy %s {
						stream_detached
						stream_logs {
							%s
						}
					}
				}
			}`, logPath, streamBackendA, streamLogs)

			tester := caddytest.NewTester(t)
			tester.InitServer(config, "caddyfile")

			// the access log works
			tester.AssertGetResponse("http://localhost:9080/version", 200, "v1")
			waitForLogEntry(t, logPath, "the entry of /version", handledRequest("/version", 200))

			tn := openTunnel(t, "/a", "tunnel")
			tn.echo("ping")
			if !skipHandshake {
				// the handler returned when the stream was detached, so the handshake is logged already
				waitForLogEntry(t, logPath, "the entry of the handshake", handledRequest("/a", 101))
			}

			tn.conn.Close()
			closed := waitForLogEntry(t, logPath, "the stream close entry", streamClosed)
			if closed["level"] != "info" {
				t.Errorf("level = %v, want info", closed["level"])
			}
			if closed["logger"] != "http.log.access" {
				t.Errorf("logger = %v, want http.log.access", closed["logger"])
			}
			if closed["upstream"] != streamBackendA {
				t.Errorf("upstream = %v, want %s", closed["upstream"], streamBackendA)
			}
			if closed["bytes_to_backend"] != float64(4) || closed["bytes_from_backend"] != float64(4) {
				t.Errorf("bytes = %v to, %v from; want 4 each", closed["bytes_to_backend"], closed["bytes_from_backend"])
			}
			if _, ok := closed["duration"]; !ok || closed["result"] == nil || closed["result"] == "" {
				t.Errorf("the entry has no duration or result: %v", closed)
			}

			handshake := findLogEntry(readLogLines(t, logPath), handledRequest("/a", 101))
			if skipHandshake && handshake != nil {
				t.Errorf("the handshake was logged although skip_handshake is set: %v", handshake)
			}
			if !skipHandshake && handshake == nil {
				t.Error("the handshake was not logged")
			}
		})
	}
}

// An upgrade works through the encode handler, whether the client asks for
// encoding or not, and the client gets exactly one 101.
func TestStreamThroughEncode(t *testing.T) {
	startUpgradeBackend(t, streamBackendA)

	tester := caddytest.NewTester(t)
	tester.InitServer(streamSite("v1", fmt.Sprintf(`handle /a {
			encode gzip
			reverse_proxy %s
		}`, streamBackendA)), "caddyfile")

	for name, headers := range map[string][]string{
		"no encoding requested": nil,
		"gzip requested":        {"Accept-Encoding: gzip"},
		"websocket handshake":   {"Accept-Encoding: gzip", "Sec-WebSocket-Version: 13", "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ=="},
	} {
		t.Run(name, func(t *testing.T) {
			proto := "tunnel"
			if strings.Contains(strings.Join(headers, " "), "Sec-WebSocket") {
				proto = "websocket"
			}
			tn := openTunnel(t, "/a", proto, headers...)
			tn.echo("through encode")
			tn.echo("and again")
		})
	}

	// the other routes keep working
	tester.AssertGetResponse("http://localhost:9080/version", 200, "v1")
}
