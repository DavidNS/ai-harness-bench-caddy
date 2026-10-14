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
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

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

// metricValue returns the current value of the stream metric name for the
// given labels: the value of a counter or gauge, or the sample count of a
// histogram. It is 0 if there is no such series.
func metricValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	streamMetrics.register(reg)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
	metrics:
		for _, m := range mf.GetMetric() {
			got := make(map[string]string)
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			for k, v := range labels {
				if got[k] != v {
					continue metrics
				}
			}
			if h := m.GetHistogram(); h != nil {
				return float64(h.GetSampleCount())
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
			return m.GetGauge().GetValue()
		}
	}
	return 0
}

// closeTracker is a connection that records being closed.
type closeTracker struct {
	closed atomic.Bool
	err    error
}

func (c *closeTracker) Read([]byte) (int, error)    { return 0, io.EOF }
func (c *closeTracker) Write(p []byte) (int, error) { return len(p), nil }
func (c *closeTracker) Close() error {
	c.closed.Store(true)
	return c.err
}

// TestStreamFinishOnce makes several close paths race to end the same
// stream and checks that it is accounted exactly once.
func TestStreamFinishOnce(t *testing.T) {
	results := []string{streamResultCompleted, streamResultError, streamResultTimeout, streamResultCanceled, streamResultShutdown}

	t.Run("racing close paths", func(t *testing.T) {
		const upstream = "finish-once.test:1"
		var calls atomic.Int32
		s := newStreamState(upstream, func(streamSummary) { calls.Add(1) })
		if got := metricValue(t, "caddy_reverse_proxy_streams_active", map[string]string{"upstream": upstream}); got != 1 {
			t.Fatalf("active streams = %v, want 1", got)
		}
		s.toBackend.Add(7)
		s.fromBackend.Add(11)

		sums := make([]streamSummary, 16)
		var wg sync.WaitGroup
		for i := range sums {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sums[i] = s.finish(results[i%len(results)])
			}()
		}
		wg.Wait()

		if got := calls.Load(); got != 1 {
			t.Errorf("onFinish called %d times, want 1", got)
		}
		for i, sum := range sums {
			if sum != sums[0] {
				t.Errorf("summary %d = %+v, want the one of the first finish %+v", i, sum, sums[0])
			}
		}
		var closed float64
		for _, result := range results {
			closed += metricValue(t, "caddy_reverse_proxy_streams_closed_total", map[string]string{"upstream": upstream, "result": result})
		}
		if closed != 1 {
			t.Errorf("closed streams = %v, want 1", closed)
		}
		if got := metricValue(t, "caddy_reverse_proxy_streams_closed_total", map[string]string{"upstream": upstream, "result": sums[0].result}); got != 1 {
			t.Errorf("closed streams with result %q = %v, want 1", sums[0].result, got)
		}
		if got := metricValue(t, "caddy_reverse_proxy_streams_active", map[string]string{"upstream": upstream}); got != 0 {
			t.Errorf("active streams = %v, want 0", got)
		}
		if got := metricValue(t, "caddy_reverse_proxy_stream_duration_seconds", map[string]string{"upstream": upstream}); got != 1 {
			t.Errorf("duration observations = %v, want 1", got)
		}
		if got := metricValue(t, "caddy_reverse_proxy_stream_bytes_total", map[string]string{"upstream": upstream, "direction": streamDirectionToBackend}); got != 7 {
			t.Errorf("bytes to backend = %v, want 7", got)
		}
		if got := metricValue(t, "caddy_reverse_proxy_stream_bytes_total", map[string]string{"upstream": upstream, "direction": streamDirectionFromBackend}); got != 11 {
			t.Errorf("bytes from backend = %v, want 11", got)
		}
		if sums[0].toBackend != 7 || sums[0].fromBackend != 11 || sums[0].upstream != upstream {
			t.Errorf("unexpected summary %+v", sums[0])
		}
	})

	t.Run("first reason wins", func(t *testing.T) {
		const upstream = "finish-reason.test:1"
		s := newStreamState(upstream, func(streamSummary) {})
		s.setReason(streamResultShutdown)
		s.setReason(streamResultCanceled)
		if sum := s.finish(streamResultCompleted); sum.result != streamResultShutdown {
			t.Errorf("result = %q, want %q", sum.result, streamResultShutdown)
		}
		if got := metricValue(t, "caddy_reverse_proxy_streams_closed_total", map[string]string{"upstream": upstream, "result": streamResultShutdown}); got != 1 {
			t.Errorf("closed streams = %v, want 1", got)
		}
	})
}

func TestRegisterStreamMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	streamMetrics.register(reg)
	// registering again, as another handler or a reload does, is fine
	streamMetrics.register(reg)

	// a clashing definition is a bug, not something to ignore
	clash := prometheus.NewRegistry()
	clash.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "caddy",
		Subsystem: "reverse_proxy",
		Name:      "streams_active",
		Help:      "A clashing metric.",
	}))
	defer func() {
		if recover() == nil {
			t.Error("expected a panic registering clashing stream metrics")
		}
	}()
	streamMetrics.register(clash)
}

func TestUpstreamKey(t *testing.T) {
	base := context.Background()
	for i, tc := range []struct {
		ctx  context.Context
		want string
	}{
		{ctx: base, want: ""},
		{ctx: context.WithValue(base, dialInfoCtxKey, DialInfo{}), want: ""},
		{ctx: context.WithValue(base, dialInfoCtxKey, DialInfo{Upstream: &Upstream{Dial: "backend:80"}}), want: "backend:80"},
	} {
		if got := upstreamKey(tc.ctx); got != tc.want {
			t.Errorf("Test %d: upstreamKey() = %q, want %q", i, got, tc.want)
		}
	}
}

func TestOpenConnectionClose(t *testing.T) {
	gracefulErr := errors.New("graceful failed")
	closeErr := errors.New("close failed")

	for i, tc := range []struct {
		graceful func() error
		closeErr error
		want     error
	}{
		{graceful: nil, want: nil},
		{graceful: func() error { return nil }, want: nil},
		{graceful: func() error { return gracefulErr }, want: gracefulErr},
		{graceful: nil, closeErr: closeErr, want: closeErr},
		{graceful: func() error { return gracefulErr }, closeErr: closeErr, want: gracefulErr},
	} {
		conn := &closeTracker{err: tc.closeErr}
		oc := openConnection{conn: conn, gracefulClose: tc.graceful}
		if got := oc.close(); got != tc.want {
			t.Errorf("Test %d: close() = %v, want %v", i, got, tc.want)
		}
		if !conn.closed.Load() {
			t.Errorf("Test %d: connection was not closed", i)
		}
	}
}

// TestDetachedRegistryPrune checks that closing the streams of an upstream
// leaves the streams of other upstreams alone, and forgets the closed ones.
func TestDetachedRegistryPrune(t *testing.T) {
	r := &detachedRegistry{streams: make(map[*detachedStream]struct{})}

	type entry struct {
		front, back *closeTracker
		stream      *streamState
		remove      func()
		graceful    atomic.Int32
	}
	add := func(upstream string) *entry {
		e := &entry{front: new(closeTracker), back: new(closeTracker)}
		e.stream = newStreamState(upstream, func(streamSummary) {})
		graceful := func() error { e.graceful.Add(1); return nil }
		e.remove = r.add(upstream,
			openConnection{conn: e.front, gracefulClose: graceful, stream: e.stream},
			openConnection{conn: e.back, stream: e.stream},
		)
		return e
	}

	removed1, removed2, kept := add("prune-removed.test:1"), add("prune-removed.test:1"), add("prune-kept.test:1")
	if got := r.count(); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}

	if got := r.closeUpstream("prune-removed.test:1"); got != 2 {
		t.Errorf("closeUpstream returned %d, want 2", got)
	}
	for i, e := range []*entry{removed1, removed2} {
		if !e.front.closed.Load() || !e.back.closed.Load() {
			t.Errorf("stream %d of the removed upstream was not closed", i)
		}
		if e.graceful.Load() != 1 {
			t.Errorf("stream %d of the removed upstream was closed gracefully %d times, want 1", i, e.graceful.Load())
		}
		if sum := e.stream.finish(streamResultError); sum.result != streamResultRemoved {
			t.Errorf("stream %d result = %q, want %q", i, sum.result, streamResultRemoved)
		}
	}
	if kept.front.closed.Load() || kept.back.closed.Load() {
		t.Error("stream of the kept upstream was closed")
	}
	if got := r.count(); got != 1 {
		t.Errorf("count after prune = %d, want 1", got)
	}
	if got := r.closeUpstream("prune-removed.test:1"); got != 0 {
		t.Errorf("closing an upstream again closed %d streams, want 0", got)
	}

	// a stream that ends by itself leaves the registry
	kept.remove()
	if got := r.count(); got != 0 {
		t.Errorf("count after remove = %d, want 0", got)
	}
	removed1.remove() // removing twice is harmless
}

// TestHandlerCleanupPrunesRemovedUpstreams checks that unloading a handler
// closes the detached streams of the upstreams that no config uses any more,
// and only those.
func TestHandlerCleanupPrunesRemovedUpstreams(t *testing.T) {
	const gone, shared = "cleanup-gone.test:1", "cleanup-shared.test:1"

	// the handler being unloaded uses both upstreams; another one, which
	// stands for the new config, still uses the shared upstream
	hosts.LoadOrStore(gone, new(Host))
	hosts.LoadOrStore(shared, new(Host))
	hosts.LoadOrStore(shared, new(Host))
	defer hosts.Delete(shared)

	h := &Handler{
		logger:         zap.NewNop(),
		Upstreams:      UpstreamPool{{Dial: gone}, {Dial: shared}},
		connections:    make(map[io.ReadWriteCloser]openConnection),
		connectionsMu:  new(sync.Mutex),
		StreamDetached: true,
	}

	goneConn, sharedConn := new(closeTracker), new(closeTracker)
	defer detachedStreams.add(gone, openConnection{conn: goneConn, stream: newStreamState(gone, func(streamSummary) {})})()
	defer detachedStreams.add(shared, openConnection{conn: sharedConn, stream: newStreamState(shared, func(streamSummary) {})})()

	if err := h.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if !goneConn.closed.Load() {
		t.Error("stream of the removed upstream was not closed")
	}
	if sharedConn.closed.Load() {
		t.Error("stream of the upstream that is still used was closed")
	}
}

// TestHandlerCloseConnectionsMarksShutdown checks that streams that are not
// detached are closed when the handler is unloaded, with the shutdown result.
func TestHandlerCloseConnectionsMarksShutdown(t *testing.T) {
	h := &Handler{
		logger:        zap.NewNop(),
		connections:   make(map[io.ReadWriteCloser]openConnection),
		connectionsMu: new(sync.Mutex),
	}
	conn := new(closeTracker)
	stream := newStreamState("shutdown.test:1", func(streamSummary) {})
	del := h.registerConnection(openConnection{conn: conn, stream: stream})

	if err := h.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if !conn.closed.Load() {
		t.Error("connection was not closed")
	}
	if sum := stream.finish(streamResultError); sum.result != streamResultShutdown {
		t.Errorf("result = %q, want %q", sum.result, streamResultShutdown)
	}
	del()
	if len(h.connections) != 0 {
		t.Errorf("%d connections left registered", len(h.connections))
	}
}

func TestSwitchProtocolCopierCountsBytes(t *testing.T) {
	var wg sync.WaitGroup
	stream := newStreamState("copier-bytes.test:1", func(streamSummary) {})
	toBackend, fromBackend := new(bytes.Buffer), new(bytes.Buffer)
	spc := switchProtocolCopier{
		user:    nopReadWriteCloser{strings.NewReader("from the client"), fromBackend},
		backend: nopReadWriteCloser{strings.NewReader("from the backend, longer"), toBackend},
		wg:      &wg,
		stream:  stream,
	}
	errc := make(chan error, 2)
	wg.Add(2)
	go spc.copyToBackend(errc)
	go spc.copyFromBackend(errc)
	wg.Wait()

	if got, want := stream.toBackend.Load(), int64(len("from the client")); got != want {
		t.Errorf("bytes to backend = %d, want %d", got, want)
	}
	if got, want := stream.fromBackend.Load(), int64(len("from the backend, longer")); got != want {
		t.Errorf("bytes from backend = %d, want %d", got, want)
	}
}

func TestLogStreamClosedWithoutServer(t *testing.T) {
	h := &Handler{logger: zap.NewNop()}
	// a request that did not go through the server has nowhere to log to
	h.logStreamClosed(httptest.NewRequest(http.MethodGet, "/", nil), streamSummary{})
}

// TestHandlerUpgradedStreamDetached drives a real upgraded stream through a
// detached handler: it is registered process-wide instead of with the
// handler, it is accounted, and removing its upstream closes it.
func TestHandlerUpgradedStreamDetached(t *testing.T) {
	const readTimeout = 10 * time.Second

	for name, skipHandshake := range map[string]bool{"handshake logged": false, "handshake skipped": true} {
		t.Run(name, func(t *testing.T) {
			srvc := make(chan net.Conn, 1)
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, _, err := http.NewResponseController(w).Hijack()
				if err != nil {
					t.Errorf("backend hijack failed: %v", err)
					return
				}
				if _, err := io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n"); err != nil {
					conn.Close()
					t.Errorf("backend upgrade write failed: %v", err)
					return
				}
				srvc <- conn
			}))
			defer backend.Close()
			upstream := backend.Listener.Addr().String()

			h := minimalHandler(0, &Upstream{Host: new(Host), Dial: upstream})
			h.StreamDetached = true
			if skipHandshake {
				h.StreamLogs = &StreamLogs{SkipHandshake: true}
			}

			handled := make(chan *http.Request, 1)
			frontend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r = prepareTestRequest(r.WithContext(context.WithoutCancel(r.Context())))
				_ = h.ServeHTTP(w, r, caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error {
					return nil
				}))
				handled <- r
			}))
			defer frontend.Close()

			cli, err := net.Dial("tcp", strings.TrimPrefix(frontend.URL, "http://"))
			if err != nil {
				t.Fatalf("failed to dial frontend: %v", err)
			}
			defer cli.Close()
			cli.SetDeadline(time.Now().Add(readTimeout))

			req, _ := http.NewRequest(http.MethodGet, frontend.URL, nil)
			req.Header.Set("Connection", "Upgrade")
			req.Header.Set("Upgrade", "websocket")
			if err := req.Write(cli); err != nil {
				t.Fatalf("failed to write upgrade request: %v", err)
			}
			br := bufio.NewReader(cli)
			resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
			if err != nil {
				t.Fatalf("failed to read upgrade response: %v", err)
			}
			if resp.StatusCode != http.StatusSwitchingProtocols {
				t.Fatalf("status = %d, want 101", resp.StatusCode)
			}

			var srv net.Conn
			select {
			case srv = <-srvc:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for the backend connection")
			}
			defer srv.Close()
			srv.SetDeadline(time.Now().Add(readTimeout))

			// bytes flow in both directions; by now the stream is registered
			buf := make([]byte, 4)
			if _, err := cli.Write([]byte("ping")); err != nil {
				t.Fatalf("client write: %v", err)
			}
			if _, err := io.ReadFull(srv, buf); err != nil || string(buf) != "ping" {
				t.Fatalf("backend read %q, %v; want ping", buf, err)
			}
			if _, err := srv.Write([]byte("pong")); err != nil {
				t.Fatalf("backend write: %v", err)
			}
			if _, err := io.ReadFull(br, buf); err != nil || string(buf) != "pong" {
				t.Fatalf("client read %q, %v; want pong", buf, err)
			}

			if len(h.connections) != 0 {
				t.Errorf("detached stream registered %d connections with the handler", len(h.connections))
			}
			if got := metricValue(t, "caddy_reverse_proxy_streams_active", map[string]string{"upstream": upstream}); got != 1 {
				t.Errorf("active streams = %v, want 1", got)
			}

			if got := detachedStreams.closeUpstream(upstream); got != 1 {
				t.Fatalf("closeUpstream closed %d streams, want 1", got)
			}
			// the client is told, and the connection ends
			if _, err := io.Copy(io.Discard, br); err != nil {
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					t.Fatalf("the client connection was not closed: %v", err)
				}
			}

			var handledReq *http.Request
			select {
			case handledReq = <-handled:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for the handler to return")
			}
			skipped, _ := caddyhttp.GetVar(handledReq.Context(), caddyhttp.LogSkipVar).(bool)
			if skipped != skipHandshake {
				t.Errorf("handshake log skipped = %t, want %t", skipped, skipHandshake)
			}

			if got := metricValue(t, "caddy_reverse_proxy_streams_active", map[string]string{"upstream": upstream}); got != 0 {
				t.Errorf("active streams = %v, want 0", got)
			}
			if got := metricValue(t, "caddy_reverse_proxy_streams_closed_total", map[string]string{"upstream": upstream, "result": streamResultRemoved}); got != 1 {
				t.Errorf("streams closed as removed = %v, want 1", got)
			}
			if got := metricValue(t, "caddy_reverse_proxy_stream_bytes_total", map[string]string{"upstream": upstream, "direction": streamDirectionToBackend}); got != 4 {
				t.Errorf("bytes to backend = %v, want 4", got)
			}
			if got := metricValue(t, "caddy_reverse_proxy_stream_bytes_total", map[string]string{"upstream": upstream, "direction": streamDirectionFromBackend}); got != 4 {
				t.Errorf("bytes from backend = %v, want 4", got)
			}
		})
	}
}
