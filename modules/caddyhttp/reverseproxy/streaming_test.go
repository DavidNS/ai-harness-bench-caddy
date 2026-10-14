package reverseproxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

func TestHandlerCopyResponse(t *testing.T) {
	h := Handler{}
	testdata := []string{
		"",
		strings.Repeat("a", defaultBufferSize),
		strings.Repeat("123456789 123456789 123456789 12", 3000),
	}

	dst := bytes.NewBuffer(nil)
	recorder := httptest.NewRecorder()
	recorder.Body = dst

	for _, d := range testdata {
		src := bytes.NewBuffer([]byte(d))
		dst.Reset()
		err := h.copyResponse(recorder, src, 0, caddy.Log())
		if err != nil {
			t.Errorf("failed with error: %v", err)
		}
		out := dst.String()
		if out != d {
			t.Errorf("bad read: got %q", out)
		}
	}
}

func TestSwitchProtocolCopierBufferSize(t *testing.T) {
	var wg sync.WaitGroup
	var errc = make(chan error, 1)
	var dst bytes.Buffer

	copier := switchProtocolCopier{
		user:       nopReadWriteCloser{Reader: strings.NewReader("hello")},
		backend:    nopReadWriteCloser{Writer: &dst},
		wg:         &wg,
		bufferSize: 7,
	}

	buf := copier.buffer()
	if got := len(buf); got != 7 {
		t.Fatalf("buffer len = %d, want 7", got)
	}

	wg.Add(1)
	go copier.copyToBackend(errc)
	wg.Wait()

	// the destination cannot be half-closed, so a clean copy reports errCopyDone
	if err := <-errc; !errors.Is(err, errCopyDone) {
		t.Fatalf("copyToBackend() error = %v, want %v", err, errCopyDone)
	}
	if got := dst.String(); got != "hello" {
		t.Fatalf("copied data = %q, want %q", got, "hello")
	}
}

// A clean EOF in one direction must be propagated to the destination as a
// write-half close instead of tearing the whole tunnel down.
// See https://github.com/caddyserver/caddy/issues/8026.
func TestSwitchProtocolCopierHalfClose(t *testing.T) {
	closeWriteErr := errors.New("no half-close")

	for _, tc := range []struct {
		name    string
		dst     io.ReadWriteCloser
		wantErr error // nil means the half-close was propagated
	}{
		{
			name:    "destination supports CloseWrite",
			dst:     &closeWriteRecorder{},
			wantErr: nil,
		},
		{
			name:    "destination does not support CloseWrite",
			dst:     nopReadWriteCloser{Writer: io.Discard},
			wantErr: errCopyDone,
		},
		{
			name:    "CloseWrite fails",
			dst:     &closeWriteRecorder{err: closeWriteErr},
			wantErr: closeWriteErr,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var wg sync.WaitGroup
			errc := make(chan error, 1)
			copier := switchProtocolCopier{
				user:    nopReadWriteCloser{Reader: strings.NewReader("hello")},
				backend: tc.dst,
				wg:      &wg,
			}

			wg.Add(1)
			go copier.copyToBackend(errc)
			wg.Wait()

			if err := <-errc; !errors.Is(err, tc.wantErr) {
				t.Fatalf("copyToBackend() error = %v, want %v", err, tc.wantErr)
			}
			if rec, ok := tc.dst.(*closeWriteRecorder); ok && !rec.called {
				t.Fatal("CloseWrite() was not called on a destination that supports it")
			}
		})
	}
}

// A copy error must be reported as-is and must not be mistaken for a clean
// half-close, so the tunnel is still torn down immediately.
func TestSwitchProtocolCopierErrorIsNotHalfClose(t *testing.T) {
	var wg sync.WaitGroup
	errc := make(chan error, 1)
	wantErr := errors.New("write failed")
	dst := &closeWriteRecorder{writeErr: wantErr}

	copier := switchProtocolCopier{
		user:    nopReadWriteCloser{Reader: strings.NewReader("hello")},
		backend: dst,
		wg:      &wg,
	}

	wg.Add(1)
	go copier.copyToBackend(errc)
	wg.Wait()

	if err := <-errc; !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if dst.called {
		t.Fatal("CloseWrite() was called after a copy error")
	}
}

func TestSwitchProtocolCopierDefaultBufferSize(t *testing.T) {
	copier := switchProtocolCopier{}
	buf := copier.buffer()
	if got := len(buf); got != defaultBufferSize {
		t.Fatalf("buffer len = %d, want %d", got, defaultBufferSize)
	}
}

type nopReadWriteCloser struct {
	io.Reader
	io.Writer
}

func (nopReadWriteCloser) Close() error { return nil }

// closeWriteRecorder is a destination that supports closing only its write
// half, and records whether that happened.
type closeWriteRecorder struct {
	called   bool
	err      error
	writeErr error
}

func (c *closeWriteRecorder) Read([]byte) (int, error) { return 0, io.EOF }

func (c *closeWriteRecorder) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return len(p), nil
}

func (c *closeWriteRecorder) Close() error { return nil }

func (c *closeWriteRecorder) CloseWrite() error {
	c.called = true
	return c.err
}

// TestHandlerUpgradedStreamHalfClose drives a real upgraded stream through the
// handler and checks that closing one direction is propagated to the other side
// instead of tearing the whole tunnel down.
//
// It mirrors net/http/httputil's TestReverseProxyWebSocketHalfTCP, which covers
// the same class upstream (https://go.dev/issue/35892). Before the fix, the
// "close write" cases failed: the handler returned as soon as the first
// direction reported, and the deferred closes killed the other direction before
// its pending bytes could be delivered.
//
// See https://github.com/caddyserver/caddy/issues/8026.
func TestHandlerUpgradedStreamHalfClose(t *testing.T) {
	switch runtime.GOOS {
	case "plan9", "js", "wasip1":
		t.Skipf("not supported on %s", runtime.GOOS)
	}

	// Reads carry a deadline so a regression fails the test promptly instead of
	// blocking until the whole package times out.
	const readTimeout = 10 * time.Second

	mustRead := func(t *testing.T, conn *net.TCPConn, msg string) {
		t.Helper()
		if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			t.Fatalf("failed to set read deadline: %v", err)
		}
		b := make([]byte, len(msg))
		if _, err := io.ReadFull(conn, b); err != nil {
			t.Fatalf("failed to read: %v", err)
		}
		if got, want := string(b), msg; got != want {
			t.Fatalf("got %#q, want %#q", got, want)
		}
	}

	mustReadEOF := func(t *testing.T, conn *net.TCPConn) {
		t.Helper()
		if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			t.Fatalf("failed to set read deadline: %v", err)
		}
		b := make([]byte, 1)
		if _, err := conn.Read(b); !errors.Is(err, io.EOF) {
			t.Fatalf("read after peer half-close: got %v, want EOF", err)
		}
	}

	mustWrite := func(t *testing.T, conn *net.TCPConn, msg string) {
		t.Helper()
		if _, err := conn.Write([]byte(msg)); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}

	mustCloseRead := func(t *testing.T, conn *net.TCPConn) {
		t.Helper()
		if err := conn.CloseRead(); err != nil {
			t.Fatalf("failed to CloseRead: %v", err)
		}
	}

	mustCloseWrite := func(t *testing.T, conn *net.TCPConn) {
		t.Helper()
		if err := conn.CloseWrite(); err != nil {
			t.Fatalf("failed to CloseWrite: %v", err)
		}
	}

	tests := map[string]func(t *testing.T, cli, srv *net.TCPConn){
		"backend close read": func(t *testing.T, cli, srv *net.TCPConn) {
			mustCloseRead(t, srv)
			mustWrite(t, srv, "backend sends")
			mustRead(t, cli, "backend sends")
		},
		"backend close write": func(t *testing.T, cli, srv *net.TCPConn) {
			mustCloseWrite(t, srv)
			mustReadEOF(t, cli)
			mustWrite(t, cli, "client sends")
			mustRead(t, srv, "client sends")
		},
		"client close read": func(t *testing.T, cli, srv *net.TCPConn) {
			mustCloseRead(t, cli)
			mustWrite(t, cli, "client sends")
			mustRead(t, srv, "client sends")
		},
		"client close write": func(t *testing.T, cli, srv *net.TCPConn) {
			mustCloseWrite(t, cli)
			mustReadEOF(t, srv)
			mustWrite(t, srv, "backend sends")
			mustRead(t, cli, "backend sends")
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			srvc := make(chan *net.TCPConn, 1)

			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, _, err := http.NewResponseController(w).Hijack()
				if err != nil {
					t.Errorf("backend hijack failed: %v", err)
					return
				}
				tcp, ok := conn.(*net.TCPConn)
				if !ok {
					conn.Close()
					t.Errorf("backend conn is %T, want *net.TCPConn", conn)
					return
				}
				if _, err := io.WriteString(tcp, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n"); err != nil {
					tcp.Close()
					t.Errorf("backend upgrade write failed: %v", err)
					return
				}
				srvc <- tcp
			}))
			defer backend.Close()

			h := minimalHandler(0, &Upstream{
				Host: new(Host),
				Dial: backend.Listener.Addr().String(),
			})
			h.connections = make(map[io.ReadWriteCloser]openConnection)
			h.connectionsMu = new(sync.Mutex)

			frontend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Isolate half-close propagation from request cancellation which has
				// its own backend-close path.
				r = r.WithContext(context.WithoutCancel(r.Context()))
				_ = h.ServeHTTP(w, prepareTestRequest(r), caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error {
					return nil
				}))
			}))
			defer frontend.Close()

			frontendURL, err := url.Parse(frontend.URL)
			if err != nil {
				t.Fatalf("failed to parse frontend URL: %v", err)
			}
			addr, err := net.ResolveTCPAddr("tcp", frontendURL.Host)
			if err != nil {
				t.Fatalf("failed to resolve TCP address: %v", err)
			}
			cli, err := net.DialTCP("tcp", nil, addr)
			if err != nil {
				t.Fatalf("failed to dial frontend: %v", err)
			}
			defer cli.Close()

			req, _ := http.NewRequest(http.MethodGet, frontend.URL, nil)
			req.Header.Set("Connection", "Upgrade")
			req.Header.Set("Upgrade", "websocket")
			if err := req.Write(cli); err != nil {
				t.Fatalf("failed to write upgrade request: %v", err)
			}

			resp, err := http.ReadResponse(bufio.NewReader(cli), &http.Request{Method: http.MethodGet})
			if err != nil {
				t.Fatalf("failed to read upgrade response: %v", err)
			}
			if resp.StatusCode != http.StatusSwitchingProtocols {
				t.Fatalf("status = %d, want 101", resp.StatusCode)
			}

			var srv *net.TCPConn
			select {
			case srv = <-srvc:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for the backend connection")
			}
			defer srv.Close()

			test(t, cli, srv)
		})
	}
}

// upgradedStream is an upgraded stream through a Handler, established
// between a real client connection and a real backend connection.
type upgradedStream struct {
	h        *Handler
	upstream string
	cli      *net.TCPConn
	srv      *net.TCPConn

	// br reads what the client receives
	br *bufio.Reader
	// done is closed when the handler returned
	done chan struct{}
	// req is the request as the handler saw it
	req *http.Request
	rec caddyhttp.ResponseRecorder
}

const upgradedStreamTimeout = 10 * time.Second

// startUpgradedStream establishes a stream through a Handler. configure
// is called with the handler and the address of the backend before the
// client connects. If recorded is true, the handler is given a response
// recorder like the one the server uses for access logging.
func startUpgradedStream(t *testing.T, recorded bool, configure func(h *Handler, upstream string)) *upgradedStream {
	t.Helper()
	s := &upgradedStream{done: make(chan struct{})}

	srvc := make(chan *net.TCPConn, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("backend hijack failed: %v", err)
			return
		}
		tcp := conn.(*net.TCPConn)
		if _, err := io.WriteString(tcp, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n"); err != nil {
			tcp.Close()
			t.Errorf("backend upgrade write failed: %v", err)
			return
		}
		srvc <- tcp
	}))
	t.Cleanup(backend.Close)

	s.upstream = backend.Listener.Addr().String()
	s.h = minimalHandler(0, &Upstream{Host: new(Host), Dial: s.upstream})
	s.h.connections = make(map[io.ReadWriteCloser]openConnection)
	s.h.connectionsMu = new(sync.Mutex)
	if configure != nil {
		configure(s.h, s.upstream)
	}

	frontend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(s.done)
		r = r.WithContext(context.WithoutCancel(r.Context()))
		s.req = prepareTestRequest(r)
		if recorded {
			s.rec = caddyhttp.NewResponseRecorder(w, nil, nil)
			w = s.rec
		}
		_ = s.h.ServeHTTP(w, s.req, caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error {
			return nil
		}))
	}))
	t.Cleanup(frontend.Close)

	frontendURL, err := url.Parse(frontend.URL)
	if err != nil {
		t.Fatalf("failed to parse frontend URL: %v", err)
	}
	addr, err := net.ResolveTCPAddr("tcp", frontendURL.Host)
	if err != nil {
		t.Fatalf("failed to resolve TCP address: %v", err)
	}
	s.cli, err = net.DialTCP("tcp", nil, addr)
	if err != nil {
		t.Fatalf("failed to dial frontend: %v", err)
	}
	t.Cleanup(func() { s.cli.Close() })

	req, _ := http.NewRequest(http.MethodGet, frontend.URL, nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	if err := req.Write(s.cli); err != nil {
		t.Fatalf("failed to write upgrade request: %v", err)
	}
	s.br = bufio.NewReader(s.cli)
	resp, err := http.ReadResponse(s.br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("failed to read upgrade response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}

	select {
	case s.srv = <-srvc:
	case <-time.After(upgradedStreamTimeout):
		t.Fatal("timed out waiting for the backend connection")
	}
	t.Cleanup(func() { s.srv.Close() })
	return s
}

// roundTrip passes a message in each direction.
func (s *upgradedStream) roundTrip(t *testing.T, toBackend, fromBackend string) {
	t.Helper()
	read := func(r io.Reader, want string) {
		t.Helper()
		s.cli.SetReadDeadline(time.Now().Add(upgradedStreamTimeout))
		s.srv.SetReadDeadline(time.Now().Add(upgradedStreamTimeout))
		b := make([]byte, len(want))
		if _, err := io.ReadFull(r, b); err != nil {
			t.Fatalf("failed to read %q: %v", want, err)
		}
		if string(b) != want {
			t.Fatalf("read %q, want %q", b, want)
		}
	}
	if _, err := s.cli.Write([]byte(toBackend)); err != nil {
		t.Fatalf("client write: %v", err)
	}
	read(s.srv, toBackend)
	if _, err := s.srv.Write([]byte(fromBackend)); err != nil {
		t.Fatalf("backend write: %v", err)
	}
	read(s.br, fromBackend)
}

func (s *upgradedStream) waitDone(t *testing.T) {
	t.Helper()
	select {
	case <-s.done:
	case <-time.After(upgradedStreamTimeout):
		t.Fatal("timed out waiting for the handler to return")
	}
}

func (s *upgradedStream) assertStillOpen(t *testing.T) {
	t.Helper()
	select {
	case <-s.done:
		t.Fatal("the handler returned: the stream was closed")
	default:
	}
	s.roundTrip(t, "again", "still here")
}

// A stream that ends cleanly is accounted once, with its bytes in each direction.
func TestUpgradedStreamAccountsCompletedStream(t *testing.T) {
	settleStreamMetrics(t)
	before := snapshotStreamMetrics(t, streamResultCompleted)

	s := startUpgradedStream(t, false, nil)
	// the client sees the 101 before the stream is accounted for
	deadline := time.Now().Add(upgradedStreamTimeout)
	for snapshotStreamMetrics(t, streamResultCompleted).active != before.active+1 {
		if time.Now().After(deadline) {
			t.Fatalf("active streams = %v, want %v", snapshotStreamMetrics(t, streamResultCompleted).active, before.active+1)
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.roundTrip(t, "ping", "pong!!")

	s.cli.Close()
	s.srv.Close()
	s.waitDone(t)

	after := snapshotStreamMetrics(t, streamResultCompleted)
	if after.active != before.active {
		t.Errorf("active streams = %v, want %v", after.active, before.active)
	}
	if after.closed != before.closed+1 {
		t.Errorf("closed{completed} = %v, want %v", after.closed, before.closed+1)
	}
	if after.durationCount != before.durationCount+1 {
		t.Errorf("duration samples = %d, want %d", after.durationCount, before.durationCount+1)
	}
	if after.toBackend != before.toBackend+4 {
		t.Errorf("bytes to backend = %v, want %v", after.toBackend, before.toBackend+4)
	}
	if after.fromBackend != before.fromBackend+6 {
		t.Errorf("bytes from backend = %v, want %v", after.fromBackend, before.fromBackend+6)
	}
	if got := len(s.h.connections); got != 0 {
		t.Errorf("%d connections still registered after the stream ended", got)
	}
}

// A connection reset ends the stream as an error.
func TestUpgradedStreamAccountsErrorStream(t *testing.T) {
	settleStreamMetrics(t)
	before := snapshotStreamMetrics(t, streamResultError)

	s := startUpgradedStream(t, false, nil)
	s.roundTrip(t, "ping", "pong!!")

	s.cli.SetLinger(0) // closing sends a reset
	s.cli.Close()
	s.waitDone(t)

	after := snapshotStreamMetrics(t, streamResultError)
	if after.closed != before.closed+1 {
		t.Errorf("closed{error} = %v, want %v", after.closed, before.closed+1)
	}
	if after.active != before.active {
		t.Errorf("active streams = %v, want %v", after.active, before.active)
	}
}

func TestUpgradedStreamAccountsTimeout(t *testing.T) {
	settleStreamMetrics(t)
	before := snapshotStreamMetrics(t, streamResultTimeout)

	s := startUpgradedStream(t, false, func(h *Handler, _ string) {
		h.StreamTimeout = 200_000_000 // 200ms
	})
	s.waitDone(t)

	after := snapshotStreamMetrics(t, streamResultTimeout)
	if after.closed != before.closed+1 {
		t.Errorf("closed{timeout} = %v, want %v", after.closed, before.closed+1)
	}
}

// Without stream_detached, closing the handler's connections (what Cleanup
// does on reload) ends the stream, and that is what it is accounted as.
func TestUpgradedStreamClosedWithConfig(t *testing.T) {
	settleStreamMetrics(t)
	before := snapshotStreamMetrics(t, streamResultClosed)

	s := startUpgradedStream(t, false, nil)
	s.roundTrip(t, "ping", "pong!!")
	if got := len(s.h.connections); got != 2 {
		t.Fatalf("registered connections = %d, want 2", got)
	}

	// the config is unloaded
	if err := s.h.Cleanup(); err != nil {
		t.Logf("Cleanup: %v", err)
	}
	s.waitDone(t)

	after := snapshotStreamMetrics(t, streamResultClosed)
	if after.closed != before.closed+1 {
		t.Errorf("closed{closed} = %v, want %v", after.closed, before.closed+1)
	}
}

// With stream_close_delay the stream outlives the config, until the delay is over.
func TestUpgradedStreamClosedAfterDelay(t *testing.T) {
	s := startUpgradedStream(t, false, func(h *Handler, _ string) {
		h.StreamCloseDelay = caddy.Duration(500 * time.Millisecond)
	})
	s.roundTrip(t, "ping", "pong!!")

	start := time.Now()
	if err := s.h.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	s.assertStillOpen(t)
	s.waitDone(t)
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Errorf("the stream was closed after %v, before the delay", elapsed)
	}
}

// Detached streams are not registered with the handler, and a reload
// that keeps their upstream leaves them open; only removing the upstream
// closes them.
func TestUpgradedStreamDetached(t *testing.T) {
	settleStreamMetrics(t)
	before := snapshotStreamMetrics(t, streamResultClosed)

	s := startUpgradedStream(t, false, func(h *Handler, upstream string) {
		h.StreamDetached = true
		// what Provision does
		h.registeredUpstreams = []string{upstream}
		detachedStreams.addLive(h.registeredUpstreams)
		t.Cleanup(func() { detachedStreams.removeLive([]string{upstream}) })
	})
	s.roundTrip(t, "ping", "pong!!")

	if got := len(s.h.connections); got != 0 {
		t.Errorf("a detached stream registered %d connections with the handler", got)
	}
	if got := detachedStreams.count(); got < 1 {
		t.Fatalf("registry count = %d, want the stream to be registered", got)
	}

	// reload: the new config has a handler for the same upstream, then the old one is cleaned up
	detachedStreams.addLive([]string{s.upstream})
	if err := s.h.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	s.assertStillOpen(t)

	// reload again: the upstream is gone from the new config
	detachedStreams.removeLive([]string{s.upstream})
	if err := detachedStreams.closeOrphaned(); err != nil {
		t.Logf("closeOrphaned: %v", err)
	}
	s.waitDone(t)

	after := snapshotStreamMetrics(t, streamResultClosed)
	if after.closed != before.closed+1 {
		t.Errorf("closed{closed} = %v, want %v", after.closed, before.closed+1)
	}
}

// With dynamic upstreams the address of the upstream is never in the config,
// yet a detached stream must not be closed for that, and survives a reload.
func TestUpgradedStreamDetachedDynamicUpstreams(t *testing.T) {
	s := startUpgradedStream(t, false, func(h *Handler, _ string) {
		h.StreamDetached = true
		h.dynamicStreams = true
		// what Provision does for a handler with dynamic upstreams
		h.registeredUpstreams = []string{dynamicLiveKey}
		detachedStreams.addLive(h.registeredUpstreams)
		t.Cleanup(func() { detachedStreams.removeLive([]string{dynamicLiveKey}) })
	})
	s.roundTrip(t, "ping", "pong!!")

	// reload: the new config has a dynamic handler too
	detachedStreams.addLive([]string{dynamicLiveKey})
	if err := s.h.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	s.assertStillOpen(t)

	// and then there is none
	detachedStreams.removeLive([]string{dynamicLiveKey})
	_ = detachedStreams.closeOrphaned()
	s.waitDone(t)
}

// A stream to an upstream that left the config while it was being established is closed.
func TestUpgradedStreamDetachedUpstreamNotLive(t *testing.T) {
	settleStreamMetrics(t)
	before := snapshotStreamMetrics(t, streamResultClosed)

	s := startUpgradedStream(t, false, func(h *Handler, _ string) {
		h.StreamDetached = true
	})
	s.waitDone(t)

	after := snapshotStreamMetrics(t, streamResultClosed)
	if after.closed != before.closed+1 {
		t.Errorf("closed{closed} = %v, want %v", after.closed, before.closed+1)
	}
	if after.active != before.active {
		t.Errorf("active streams = %v, want %v", after.active, before.active)
	}
}

// The proxy copies on the raw connection: the traffic of the stream is not
// counted by the response recorder (which would be wrong for the access log
// size), and the recorder neither buffers nor writes after the hijack.
func TestUpgradedStreamBypassesRecorder(t *testing.T) {
	s := startUpgradedStream(t, true, nil)
	s.roundTrip(t, "ping", "pong!!")

	if got := s.rec.Size(); got != 0 {
		t.Errorf("recorder size = %d, want 0: the stream was copied through the recorder", got)
	}
	if got := s.rec.Status(); got != http.StatusSwitchingProtocols {
		t.Errorf("recorder status = %d, want 101", got)
	}
	if s.rec.Buffered() {
		t.Error("the recorder buffers a hijacked response")
	}

	s.cli.Close()
	s.srv.Close()
	s.waitDone(t)
}

func TestUpgradedStreamSkipHandshake(t *testing.T) {
	for _, tc := range []struct {
		name string
		logs *StreamLogs
		want bool
	}{
		{name: "default", logs: nil, want: false},
		{name: "logs without skip", logs: &StreamLogs{Level: "info"}, want: false},
		{name: "skip handshake", logs: &StreamLogs{SkipHandshake: true}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := startUpgradedStream(t, false, func(h *Handler, _ string) {
				h.StreamLogs = tc.logs
			})
			s.roundTrip(t, "ping", "pong!!")
			s.cli.Close()
			s.srv.Close()
			s.waitDone(t)

			skipped, _ := caddyhttp.GetVar(s.req.Context(), caddyhttp.LogSkipVar).(bool)
			if skipped != tc.want {
				t.Errorf("access log skipped = %v, want %v", skipped, tc.want)
			}
		})
	}
}

type unwrappableConn struct {
	nopReadWriteCloser
	raw net.Conn
}

func (u unwrappableConn) UnwrapHijacked() net.Conn { return u.raw }

func TestUnwrapHijacked(t *testing.T) {
	plain := nopReadWriteCloser{}
	if got := unwrapHijacked(plain); got != io.ReadWriteCloser(plain) {
		t.Errorf("unwrapHijacked(plain) = %v, want the same connection", got)
	}

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	if got := unwrapHijacked(unwrappableConn{raw: c1}); got != io.ReadWriteCloser(c1) {
		t.Errorf("unwrapHijacked(wrapped) = %v, want the raw connection", got)
	}
}

func TestStreamUpstream(t *testing.T) {
	if got := streamUpstream(&http.Response{}); got != "" {
		t.Errorf("no request: got %q, want empty", got)
	}

	noDialInfo, _ := http.NewRequest(http.MethodGet, "http://example.com/", nil)
	if got := streamUpstream(&http.Response{Request: noDialInfo}); got != "" {
		t.Errorf("no dial info: got %q, want empty", got)
	}

	withNil := noDialInfo.WithContext(context.WithValue(noDialInfo.Context(), dialInfoCtxKey, DialInfo{}))
	if got := streamUpstream(&http.Response{Request: withNil}); got != "" {
		t.Errorf("dial info without upstream: got %q, want empty", got)
	}

	with := noDialInfo.WithContext(context.WithValue(noDialInfo.Context(), dialInfoCtxKey, DialInfo{Upstream: &Upstream{Dial: "backend:80"}}))
	if got := streamUpstream(&http.Response{Request: with}); got != "backend:80" {
		t.Errorf("got %q, want backend:80", got)
	}
}

func TestStreamLogRequest(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "http://rewritten.example/", nil)
	if got := streamLogRequest(req); got != req {
		t.Error("without an original request, the request itself should be used")
	}

	orig := &http.Request{Method: http.MethodGet, Host: "original.example"}
	req = req.WithContext(context.WithValue(req.Context(), streamLogRequestCtxKey, orig))
	if got := streamLogRequest(req); got != orig {
		t.Errorf("host = %q, want original.example", got.Host)
	}
}

func TestLogStreamCloseWithoutAccessLog(t *testing.T) {
	h := &Handler{streamLogLevel: -1}
	acct := newStreamAccounting()
	defer acct.finish(streamResultCompleted)

	// no server in the context: nothing to log to, and no panic
	req, _ := http.NewRequest(http.MethodGet, "http://example.com/", nil)
	h.logStreamClose(req, "backend:80", streamResultCompleted, acct)

	// a server without access logging
	h.logStreamClose(prepareTestRequest(req), "backend:80", streamResultCompleted, acct)
}

// A response carrying the Incremental header field (RFC 10036) must be
// forwarded without buffering, whatever its Content-Type and Content-Length.
func TestFlushIntervalIncremental(t *testing.T) {
	h := Handler{FlushInterval: caddy.Duration(time.Second)}
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	for _, tc := range []struct {
		name                string
		incremental         string
		receivedIncremental bool
		want                time.Duration
	}{
		{name: "incremental", incremental: "?1", want: -1},
		{name: "not incremental", incremental: "?0", want: time.Second},
		{name: "absent", want: time.Second},
		{name: "removed by header operations", receivedIncremental: true, want: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := &http.Response{
				Header:        http.Header{"Content-Type": []string{"application/json"}},
				ContentLength: 42,
			}
			if tc.incremental != "" {
				res.Header.Set("Incremental", tc.incremental)
			}
			if got := h.flushInterval(req, res, tc.receivedIncremental); got != tc.want {
				t.Errorf("flushInterval() = %v, want %v", got, tc.want)
			}
		})
	}
}
