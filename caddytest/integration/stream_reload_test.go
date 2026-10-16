package integration

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddytest"
)

// streamBackend is an HTTP server that switches protocols on any request
// that carries an Upgrade header, and echoes the stream afterwards. Other
// requests get a plain response.
type streamBackend struct {
	addr  string
	mu    sync.Mutex
	conns []net.Conn
}

func startStreamBackend(t *testing.T) *streamBackend {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("starting the backend: %v", err)
	}
	b := &streamBackend{addr: ln.Addr().String()}
	srv := &http.Server{Handler: http.HandlerFunc(b.serve)}
	go srv.Serve(ln)
	t.Cleanup(func() {
		srv.Close()
		b.mu.Lock()
		defer b.mu.Unlock()
		for _, c := range b.conns {
			c.Close()
		}
	})
	return b
}

func (b *streamBackend) serve(w http.ResponseWriter, r *http.Request) {
	upgrade := r.Header.Get("Upgrade")
	if upgrade == "" {
		fmt.Fprint(w, "backend")
		return
	}
	conn, brw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return
	}
	b.mu.Lock()
	b.conns = append(b.conns, conn)
	b.mu.Unlock()
	defer conn.Close()

	fmt.Fprintf(brw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", upgrade)
	if brw.Flush() != nil {
		return
	}
	io.Copy(conn, brw.Reader)
}

// streamClient is the client side of an upgraded HTTP/1.1 connection.
type streamClient struct {
	conn net.Conn
	br   *bufio.Reader
	resp *http.Response
}

// dialStream performs an upgrade handshake against the server listening on
// port 9080 and fails the test if the response is not a 101.
func dialStream(t *testing.T, path, upgrade string, header http.Header) *streamClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", "127.0.0.1:9080", 5*time.Second)
	if err != nil {
		t.Fatalf("dialing the server: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	req, err := http.NewRequest(http.MethodGet, "http://localhost:9080"+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", upgrade)
	for k, v := range header {
		req.Header[k] = v
	}
	if err := req.Write(conn); err != nil {
		t.Fatalf("writing the upgrade request: %v", err)
	}

	br := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		t.Fatalf("reading the upgrade response: %v", err)
	}
	conn.SetReadDeadline(time.Time{})
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status = %d, want 101", resp.StatusCode)
	}
	return &streamClient{conn: conn, br: br, resp: resp}
}

// echo sends msg through the stream and expects it back.
func (c *streamClient) echo(msg string) error {
	c.conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer c.conn.SetDeadline(time.Time{})
	if _, err := io.WriteString(c.conn, msg); err != nil {
		return err
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(c.br, buf); err != nil {
		return err
	}
	if string(buf) != msg {
		return fmt.Errorf("echoed %q, want %q", buf, msg)
	}
	return nil
}

// waitClosed reports whether the server closed the stream within d.
func (c *streamClient) waitClosed(d time.Duration) bool {
	c.conn.SetReadDeadline(time.Now().Add(d))
	defer c.conn.SetReadDeadline(time.Time{})
	_, err := io.Copy(io.Discard, c.br)
	var netErr net.Error
	return !(errors.As(err, &netErr) && netErr.Timeout())
}

// streamSite returns the config of a server that proxies to upstream, with
// the given reverse_proxy options and extra site directives. A reload is
// done by loading the config again with different extra directives.
func streamSite(upstream, proxyOptions, extra string) string {
	return fmt.Sprintf(`{
		skip_install_trust
		admin localhost:2999
		http_port     9080
		https_port    9443
		grace_period  1ns
	}

	localhost:9080 {
		%s
		reverse_proxy %s {
			%s
		}
	}`, extra, upstream, proxyOptions)
}

// By default, an upgraded stream is closed when the config is reloaded.
func TestStreamIsClosedOnReload(t *testing.T) {
	tester := caddytest.NewTester(t)
	backend := startStreamBackend(t)

	tester.InitServer(streamSite(backend.addr, "", `respond /version "v1"`), "caddyfile")
	c := dialStream(t, "/ws", "echo", nil)
	if err := c.echo("before"); err != nil {
		t.Fatalf("echo before the reload: %v", err)
	}

	tester.InitServer(streamSite(backend.addr, "", `respond /version "v2"`), "caddyfile")
	if !c.waitClosed(5 * time.Second) {
		t.Fatal("the stream is still open after the reload")
	}
	tester.AssertGetResponse("http://localhost:9080/version", 200, "v2")
}

// With stream_close_delay, the stream outlives the reload until the delay is over.
func TestStreamIsClosedAfterCloseDelay(t *testing.T) {
	tester := caddytest.NewTester(t)
	backend := startStreamBackend(t)

	options := "stream_close_delay 1500ms"
	tester.InitServer(streamSite(backend.addr, options, `respond /version "v1"`), "caddyfile")
	c := dialStream(t, "/ws", "echo", nil)
	if err := c.echo("before"); err != nil {
		t.Fatalf("echo before the reload: %v", err)
	}

	reloaded := time.Now()
	tester.InitServer(streamSite(backend.addr, options, `respond /version "v2"`), "caddyfile")
	if err := c.echo("during the delay"); err != nil {
		t.Fatalf("the stream should stay open during the close delay: %v", err)
	}
	if !c.waitClosed(5 * time.Second) {
		t.Fatal("the stream is still open after the close delay")
	}
	if elapsed := time.Since(reloaded); elapsed < time.Second {
		t.Errorf("the stream was closed %v after the reload, want it to wait for the delay", elapsed)
	}
}

// A detached stream survives reloads that keep its upstream, and is closed
// by the reload that removes it.
func TestDetachedStreamFollowsItsUpstream(t *testing.T) {
	tester := caddytest.NewTester(t)
	backend := startStreamBackend(t)
	otherBackend := startStreamBackend(t)
	const options = "stream_detached"

	tester.InitServer(streamSite(backend.addr, options, `respond /version "v1"`), "caddyfile")
	c := dialStream(t, "/ws", "echo", nil)
	if err := c.echo("before"); err != nil {
		t.Fatalf("echo before the reload: %v", err)
	}

	tester.InitServer(streamSite(backend.addr, options, `respond /version "v2"`), "caddyfile")
	tester.AssertGetResponse("http://localhost:9080/version", 200, "v2")
	if c.waitClosed(time.Second) {
		t.Fatal("the detached stream was closed by a reload that keeps its upstream")
	}
	if err := c.echo("after"); err != nil {
		t.Fatalf("echo after the reload: %v", err)
	}

	tester.InitServer(streamSite(otherBackend.addr, options, `respond /version "v3"`), "caddyfile")
	if !c.waitClosed(5 * time.Second) {
		t.Fatal("the detached stream is still open after its upstream was removed")
	}
}
