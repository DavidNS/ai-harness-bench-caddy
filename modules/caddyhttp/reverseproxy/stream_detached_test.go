package reverseproxy

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

const testStreamTimeout = 5 * time.Second

func TestStreamOptionsCaddyfile(t *testing.T) {
	for i, tc := range []struct {
		input   string
		check   func(h *Handler) bool
		wantErr bool
	}{
		{
			input: `reverse_proxy localhost:8080`,
			check: func(h *Handler) bool { return !h.StreamDetached && h.StreamLogs == nil },
		},
		{
			input: `reverse_proxy localhost:8080 {
				stream_detached
			}`,
			check: func(h *Handler) bool { return h.StreamDetached && h.StreamLogs == nil },
		},
		{
			input: `reverse_proxy localhost:8080 {
				stream_logs {
				}
			}`,
			check: func(h *Handler) bool { return h.StreamLogs != nil && *h.StreamLogs == StreamLogs{} },
		},
		{
			input: `reverse_proxy localhost:8080 {
				stream_detached
				stream_logs {
					skip_handshake
					logger_name access
					level       info
				}
			}`,
			check: func(h *Handler) bool {
				return h.StreamDetached && *h.StreamLogs == StreamLogs{SkipHandshake: true, LoggerName: "access", Level: "info"}
			},
		},
		{input: `reverse_proxy localhost:8080 { stream_detached true }`, wantErr: true},
		{input: `reverse_proxy localhost:8080 { stream_logs true }`, wantErr: true},
		{input: `reverse_proxy localhost:8080 { stream_logs { skip_handshake true } }`, wantErr: true},
		{input: `reverse_proxy localhost:8080 { stream_logs { logger_name } }`, wantErr: true},
		{input: `reverse_proxy localhost:8080 { stream_logs { logger_name access extra } }`, wantErr: true},
		{input: `reverse_proxy localhost:8080 { stream_logs { level } }`, wantErr: true},
		{input: `reverse_proxy localhost:8080 { stream_logs { level info extra } }`, wantErr: true},
		{input: `reverse_proxy localhost:8080 { stream_logs { bogus } }`, wantErr: true},
	} {
		h := new(Handler)
		err := h.UnmarshalCaddyfile(caddyfile.NewTestDispenser(tc.input))
		if tc.wantErr {
			if err == nil {
				t.Errorf("Test %d: expected an error but got none", i)
			}
			continue
		}
		if err != nil {
			t.Errorf("Test %d: unexpected error: %v", i, err)
			continue
		}
		if !tc.check(h) {
			t.Errorf("Test %d: unexpected result: detached=%v logs=%+v", i, h.StreamDetached, h.StreamLogs)
		}
	}
}

func TestStreamOptionsJSON(t *testing.T) {
	var h Handler
	err := json.Unmarshal([]byte(`{
		"stream_detached": true,
		"stream_logs": {"skip_handshake": true, "logger_name": "access", "level": "warn"}
	}`), &h)
	if err != nil {
		t.Fatal(err)
	}
	if !h.StreamDetached || *h.StreamLogs != (StreamLogs{SkipHandshake: true, LoggerName: "access", Level: "warn"}) {
		t.Fatalf("unexpected result: detached=%v logs=%+v", h.StreamDetached, h.StreamLogs)
	}

	out, err := json.Marshal(Handler{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "stream_detached") || strings.Contains(string(out), "stream_logs") {
		t.Errorf("unset options must be omitted from JSON: %s", out)
	}
}

func TestStreamLogsValidate(t *testing.T) {
	for i, tc := range []struct {
		logs      StreamLogs
		wantLevel zapcore.Level
		wantErr   bool
	}{
		{logs: StreamLogs{}, wantLevel: zapcore.DebugLevel},
		{logs: StreamLogs{LoggerName: "access"}, wantLevel: zapcore.DebugLevel},
		{logs: StreamLogs{Level: "info"}, wantLevel: zapcore.InfoLevel},
		{logs: StreamLogs{Level: "WARN", LoggerName: "access"}, wantLevel: zapcore.WarnLevel},
		{logs: StreamLogs{LoggerName: "other"}, wantErr: true},
		{logs: StreamLogs{Level: "loud"}, wantErr: true},
	} {
		level := zapcore.DebugLevel
		err := tc.logs.validate(&level)
		if tc.wantErr {
			if err == nil {
				t.Errorf("Test %d: expected an error but got none", i)
			}
			continue
		}
		if err != nil {
			t.Errorf("Test %d: unexpected error: %v", i, err)
		} else if level != tc.wantLevel {
			t.Errorf("Test %d: level = %v, want %v", i, level, tc.wantLevel)
		}
	}
}

// closeTracker is a connection which records being closed.
type closeTracker struct {
	closed      atomic.Bool
	closeErr    error
	gracefulErr error
	graceful    atomic.Bool
}

func (c *closeTracker) Read([]byte) (int, error)    { return 0, io.EOF }
func (c *closeTracker) Write(p []byte) (int, error) { return len(p), nil }
func (c *closeTracker) Close() error {
	c.closed.Store(true)
	return c.closeErr
}
func (c *closeTracker) gracefulClose() error {
	c.graceful.Store(true)
	return c.gracefulErr
}

func TestStreamRegistry(t *testing.T) {
	r := &streamRegistry{streams: make(map[string]map[*upgradedStream]struct{})}
	newStream := func() (*upgradedStream, *closeTracker, *closeTracker) {
		user, backend := new(closeTracker), new(closeTracker)
		return &upgradedStream{user: user, backend: backend, gracefulUser: user.gracefulClose}, user, backend
	}

	a1, a1User, a1Backend := newStream()
	a2, a2User, _ := newStream()
	b1, b1User, _ := newStream()
	removeA1 := r.add("a", a1)
	removeA2 := r.add("a", a2)
	removeB1 := r.add("b", b1)

	if got := r.count("a"); got != 2 {
		t.Fatalf("count(a) = %d, want 2", got)
	}
	if got := r.count("none"); got != 0 {
		t.Fatalf("count(none) = %d, want 0", got)
	}

	if got := r.closeUpstream("none"); got != 0 {
		t.Errorf("closeUpstream(none) = %d, want 0", got)
	}
	if got := r.closeUpstream("a"); got != 2 {
		t.Errorf("closeUpstream(a) = %d, want 2", got)
	}
	for name, s := range map[string]*closeTracker{"a1 user": a1User, "a1 backend": a1Backend, "a2 user": a2User} {
		if !s.closed.Load() {
			t.Errorf("%s was not closed", name)
		}
	}
	if !a1User.graceful.Load() {
		t.Error("the graceful close of a1 was not attempted")
	}
	if !a1.reloaded.Load() || !a2.reloaded.Load() {
		t.Error("closed streams were not marked as closed by a reload")
	}
	if b1User.closed.Load() || b1.reloaded.Load() {
		t.Error("a stream to another upstream was closed")
	}

	// streams unregister themselves when they end
	removeA1()
	if got := r.count("a"); got != 1 {
		t.Errorf("count(a) = %d after one stream ended, want 1", got)
	}
	removeA2()
	removeB1()
	if len(r.streams) != 0 {
		t.Errorf("registry still has entries after all streams ended: %v", r.streams)
	}
}

func TestStreamCloseErrors(t *testing.T) {
	gracefulErr := errors.New("graceful failed")
	closeErr := errors.New("close failed")

	for name, tc := range map[string]struct {
		user, backend *closeTracker
		want          error
	}{
		"no errors":             {user: &closeTracker{}, backend: &closeTracker{}},
		"graceful error":        {user: &closeTracker{gracefulErr: gracefulErr}, backend: &closeTracker{}, want: gracefulErr},
		"close error":           {user: &closeTracker{}, backend: &closeTracker{closeErr: closeErr}, want: closeErr},
		"first error is kept":   {user: &closeTracker{gracefulErr: gracefulErr}, backend: &closeTracker{closeErr: closeErr}, want: gracefulErr},
		"close after graceful":  {user: &closeTracker{gracefulErr: gracefulErr, closeErr: closeErr}, backend: &closeTracker{}, want: gracefulErr},
		"backend close errored": {user: &closeTracker{closeErr: closeErr}, backend: &closeTracker{closeErr: errors.New("second")}, want: closeErr},
	} {
		t.Run(name, func(t *testing.T) {
			s := &upgradedStream{user: tc.user, backend: tc.backend, gracefulUser: tc.user.gracefulClose}
			if err := s.closeForReload(); !errors.Is(err, tc.want) {
				t.Errorf("closeForReload() error = %v, want %v", err, tc.want)
			}
			if !tc.user.closed.Load() || !tc.backend.closed.Load() {
				t.Error("both connections must be closed whatever the errors")
			}
		})
	}
}

func histogramCount(t *testing.T, o prometheus.Observer) uint64 {
	t.Helper()
	var m dto.Metric
	if err := o.(prometheus.Metric).Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram().GetSampleCount()
}

func TestStreamAccountExactlyOnce(t *testing.T) {
	m := newStreamMetrics()
	a := m.begin("up")

	if got := testutil.ToFloat64(m.active.WithLabelValues("up")); got != 1 {
		t.Fatalf("active streams = %v while open, want 1", got)
	}

	if took := a.finish(streamResultClosed, 5, 7); took <= 0 {
		t.Errorf("finish() reported a duration of %v for the first call", took)
	}
	// every other path that ends the stream must be ignored
	if took := a.finish(streamResultError, 100, 100); took != 0 {
		t.Errorf("finish() reported a duration of %v for a repeated call", took)
	}

	if got := testutil.ToFloat64(m.active.WithLabelValues("up")); got != 0 {
		t.Errorf("active streams = %v after close, want 0", got)
	}
	if got := testutil.ToFloat64(m.closed.WithLabelValues("up", streamResultClosed)); got != 1 {
		t.Errorf("closed{closed} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.closed.WithLabelValues("up", streamResultError)); got != 0 {
		t.Errorf("closed{error} = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.bytes.WithLabelValues("up", "to_backend")); got != 5 {
		t.Errorf("bytes to backend = %v, want 5", got)
	}
	if got := testutil.ToFloat64(m.bytes.WithLabelValues("up", "from_backend")); got != 7 {
		t.Errorf("bytes from backend = %v, want 7", got)
	}
	if got := histogramCount(t, m.duration.WithLabelValues("up")); got != 1 {
		t.Errorf("duration observations = %d, want 1", got)
	}
}

func TestStreamAccountWithoutMetrics(t *testing.T) {
	var m *streamMetrics
	a := m.begin("up")
	if took := a.finish(streamResultClosed, 1, 2); took <= 0 {
		t.Errorf("finish() reported a duration of %v", took)
	}
}

func TestStreamMetricsRegister(t *testing.T) {
	m := newStreamMetrics()
	registry := prometheus.NewRegistry()
	m.register(registry)
	m.register(registry) // registering the same collectors again is fine

	m.begin("up").finish(streamResultTimeout, 1, 1)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := make(map[string]bool)
	for _, f := range families {
		names[f.GetName()] = true
	}
	for _, want := range []string{
		"caddy_reverse_proxy_streams_active",
		"caddy_reverse_proxy_streams_closed_total",
		"caddy_reverse_proxy_stream_duration_seconds",
		"caddy_reverse_proxy_stream_bytes_total",
	} {
		if !names[want] {
			t.Errorf("metric %s is not exposed", want)
		}
	}

	// a conflicting registration is a bug, not something to ignore
	conflicting := prometheus.NewRegistry()
	conflicting.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "caddy", Subsystem: "reverse_proxy", Name: "streams_active", Help: "conflict",
	}))
	defer func() {
		if recover() == nil {
			t.Error("expected a panic on a conflicting registration")
		}
	}()
	m.register(conflicting)
}

// tcpPair returns two ends of a TCP connection.
func tcpPair(t *testing.T) (a, b *net.TCPConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()
	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c2 := <-accepted
	if c2 == nil {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() { c1.Close(); c2.Close() })
	return c1.(*net.TCPConn), c2.(*net.TCPConn)
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

type failingConn struct {
	readErr, writeErr error
}

func (c failingConn) Read([]byte) (int, error) {
	if c.readErr != nil {
		return 0, c.readErr
	}
	time.Sleep(time.Hour)
	return 0, io.EOF
}
func (c failingConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return len(p), nil
}
func (c failingConn) Close() error { return nil }

// streamHarness runs an upgradedStream between two TCP connections, the
// ends of which are held by the test.
type streamHarness struct {
	s           *upgradedStream
	clientPeer  *net.TCPConn // the client's end of the connection to the proxy
	backendPeer *net.TCPConn // the backend's end of the connection from the proxy
	metrics     *streamMetrics
	logs        *observer.ObservedLogs
	done        chan struct{}
	doneCalls   atomic.Int32
}

func newStreamHarness(t *testing.T, brw *bufio.ReadWriter) *streamHarness {
	t.Helper()
	user, clientPeer := tcpPair(t)
	backend, backendPeer := tcpPair(t)
	if brw == nil {
		brw = bufio.NewReadWriter(bufio.NewReader(strings.NewReader("")), bufio.NewWriter(io.Discard))
	}
	core, logs := observer.New(zapcore.DebugLevel)
	hn := &streamHarness{
		clientPeer:  clientPeer,
		backendPeer: backendPeer,
		metrics:     newStreamMetrics(),
		logs:        logs,
		done:        make(chan struct{}),
	}
	hn.s = &upgradedStream{
		user:         user,
		backend:      backend,
		brw:          brw,
		upstream:     "up",
		logger:       zap.New(core),
		closeLoggers: []*zap.Logger{zap.New(core)},
		level:        zapcore.DebugLevel,
		account:      hn.metrics.begin("up"),
		onDone:       func() { hn.doneCalls.Add(1) },
	}
	return hn
}

func (hn *streamHarness) start(reqDone <-chan struct{}) {
	go func() {
		hn.s.run(reqDone)
		close(hn.done)
	}()
}

func (hn *streamHarness) wait(t *testing.T) {
	t.Helper()
	select {
	case <-hn.done:
	case <-time.After(testStreamTimeout):
		t.Fatal("timed out waiting for the stream to end")
	}
}

func (hn *streamHarness) closedWith(result string) float64 {
	return testutil.ToFloat64(hn.metrics.closed.WithLabelValues("up", result))
}

// assertAccounted checks the stream was accounted exactly once, with result.
func (hn *streamHarness) assertAccounted(t *testing.T, result string) {
	t.Helper()
	for _, r := range []string{streamResultClosed, streamResultTimeout, streamResultError, streamResultReload} {
		want := 0.0
		if r == result {
			want = 1
		}
		if got := hn.closedWith(r); got != want {
			t.Errorf("closed{%s} = %v, want %v", r, got, want)
		}
	}
	if got := testutil.ToFloat64(hn.metrics.active.WithLabelValues("up")); got != 0 {
		t.Errorf("active streams = %v after the stream ended, want 0", got)
	}
	if got := hn.doneCalls.Load(); got != 1 {
		t.Errorf("onDone called %d times, want 1", got)
	}
	entries := hn.logs.FilterMessage("stream closed").All()
	if len(entries) != 1 {
		t.Fatalf("got %d stream close logs, want 1", len(entries))
	}
	if got := entries[0].ContextMap()["result"]; got != result {
		t.Errorf("logged result = %v, want %s", got, result)
	}
}

func mustReadFull(t *testing.T, c net.Conn, want string) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(testStreamTimeout))
	buf := make([]byte, len(want))
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(buf) != want {
		t.Fatalf("read %q, want %q", buf, want)
	}
}

func mustWrite(t *testing.T, c net.Conn, s string) {
	t.Helper()
	if _, err := c.Write([]byte(s)); err != nil {
		t.Fatalf("write failed: %v", err)
	}
}

func TestUpgradedStreamRunClosed(t *testing.T) {
	hn := newStreamHarness(t, nil)
	hn.start(nil)

	mustWrite(t, hn.clientPeer, "hello")
	mustReadFull(t, hn.backendPeer, "hello")
	mustWrite(t, hn.backendPeer, "worlds")
	mustReadFull(t, hn.clientPeer, "worlds")

	// both sides half-close: a clean end of the stream
	_ = hn.clientPeer.CloseWrite()
	_ = hn.backendPeer.CloseWrite()
	hn.wait(t)

	hn.assertAccounted(t, streamResultClosed)
	fields := hn.logs.FilterMessage("stream closed").All()[0].ContextMap()
	if fields["bytes_to_backend"] != int64(5) || fields["bytes_from_backend"] != int64(6) {
		t.Errorf("logged bytes = %v to the backend, %v from it; want 5 and 6", fields["bytes_to_backend"], fields["bytes_from_backend"])
	}
	if got := testutil.ToFloat64(hn.metrics.bytes.WithLabelValues("up", "to_backend")); got != 5 {
		t.Errorf("bytes to backend = %v, want 5", got)
	}
	if got := testutil.ToFloat64(hn.metrics.bytes.WithLabelValues("up", "from_backend")); got != 6 {
		t.Errorf("bytes from backend = %v, want 6", got)
	}
}

func TestUpgradedStreamRunTimeout(t *testing.T) {
	hn := newStreamHarness(t, nil)
	hn.s.timeout = 50 * time.Millisecond
	hn.start(nil)

	mustWrite(t, hn.clientPeer, "hi")
	mustReadFull(t, hn.backendPeer, "hi")
	hn.wait(t)

	hn.assertAccounted(t, streamResultTimeout)
	if got := testutil.ToFloat64(hn.metrics.bytes.WithLabelValues("up", "to_backend")); got != 2 {
		t.Errorf("bytes to backend = %v, want 2", got)
	}
}

func TestUpgradedStreamRunReload(t *testing.T) {
	hn := newStreamHarness(t, nil)
	hn.start(nil)

	mustWrite(t, hn.clientPeer, "hi")
	mustReadFull(t, hn.backendPeer, "hi")
	// closing the user connection ends the stream, which may close the
	// backend connection first: an error here is expected sometimes
	_ = hn.s.closeForReload()
	hn.wait(t)

	hn.assertAccounted(t, streamResultReload)
}

func TestUpgradedStreamRunCopyError(t *testing.T) {
	hn := newStreamHarness(t, nil)
	hn.s.backend = failingConn{readErr: errors.New("backend broke")}
	hn.start(nil)
	hn.wait(t)

	hn.assertAccounted(t, streamResultError)
}

func TestUpgradedStreamRunRequestDone(t *testing.T) {
	hn := newStreamHarness(t, nil)
	reqDone := make(chan struct{})
	hn.start(reqDone)

	mustWrite(t, hn.clientPeer, "hi")
	mustReadFull(t, hn.backendPeer, "hi")
	close(reqDone)
	hn.wait(t)

	// the backend was closed: its peer sees the end of the connection
	_ = hn.backendPeer.SetReadDeadline(time.Now().Add(testStreamTimeout))
	if _, err := hn.backendPeer.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("backend peer read error = %v, want the connection to be closed", err)
	}
	hn.assertAccounted(t, streamResultError)
}

func TestUpgradedStreamRunFlushError(t *testing.T) {
	brw := bufio.NewReadWriter(bufio.NewReader(strings.NewReader("")), bufio.NewWriter(failingWriter{}))
	_, _ = brw.WriteString("HTTP/1.1 101 Switching Protocols\r\n\r\n")
	hn := newStreamHarness(t, brw)
	hn.start(nil)
	hn.wait(t)

	hn.assertAccounted(t, streamResultError)
	if hn.logs.FilterMessage("response flush").Len() != 1 {
		t.Error("the flush failure was not logged")
	}
}

func TestUpgradedStreamRunBufferedData(t *testing.T) {
	// data the client sent along with the handshake sits in the buffered reader
	r := bufio.NewReader(strings.NewReader("early"))
	if _, err := r.Peek(1); err != nil {
		t.Fatal(err)
	}
	hn := newStreamHarness(t, bufio.NewReadWriter(r, bufio.NewWriter(io.Discard)))
	hn.start(nil)

	mustReadFull(t, hn.backendPeer, "early")
	_ = hn.s.closeForReload()
	hn.wait(t)

	if got := testutil.ToFloat64(hn.metrics.bytes.WithLabelValues("up", "to_backend")); got != 5 {
		t.Errorf("bytes to backend = %v, want 5 (the buffered data)", got)
	}
}

func TestUpgradedStreamRunBufferedDataWriteError(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("early"))
	if _, err := r.Peek(1); err != nil {
		t.Fatal(err)
	}
	hn := newStreamHarness(t, bufio.NewReadWriter(r, bufio.NewWriter(io.Discard)))
	hn.s.backend = failingConn{writeErr: errors.New("backend gone")}
	hn.start(nil)
	hn.wait(t)

	hn.assertAccounted(t, streamResultError)
	if hn.logs.FilterMessage("backConn write failed").Len() != 1 {
		t.Error("the write failure was not logged")
	}
}

func TestSwitchProtocolCopierCountsBytes(t *testing.T) {
	var sent, received atomic.Int64

	copier := switchProtocolCopier{
		user:     nopReadWriteCloser{Reader: strings.NewReader("abc"), Writer: io.Discard},
		backend:  nopReadWriteCloser{Reader: strings.NewReader("defgh"), Writer: io.Discard},
		wg:       new(sync.WaitGroup),
		sent:     &sent,
		received: &received,
	}
	errc := make(chan error, 2)
	copier.wg.Add(2)
	go copier.copyToBackend(errc)
	go copier.copyFromBackend(errc)
	copier.wg.Wait()

	if sent.Load() != 3 || received.Load() != 5 {
		t.Errorf("counted %d bytes to the backend and %d from it, want 3 and 5", sent.Load(), received.Load())
	}
}

// upgradeBackend is a backend which accepts every upgrade request and
// hands out its side of the upgraded connection.
func upgradeBackend(t *testing.T) (addr string, conns chan *net.TCPConn) {
	t.Helper()
	conns = make(chan *net.TCPConn, 4)
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
		conns <- tcp
	}))
	t.Cleanup(backend.Close)
	return backend.Listener.Addr().String(), conns
}

// upgrade performs an upgrade handshake against the server at url and
// returns the client connection with the backend's connection.
func upgrade(t *testing.T, url string, backendConns chan *net.TCPConn) (cli *net.TCPConn, br *bufio.Reader, srv *net.TCPConn) {
	t.Helper()
	addr, err := net.ResolveTCPAddr("tcp", strings.TrimPrefix(url, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	cli, err = net.DialTCP("tcp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cli.Close() })

	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	if err := req.Write(cli); err != nil {
		t.Fatal(err)
	}
	_ = cli.SetReadDeadline(time.Now().Add(testStreamTimeout))
	br = bufio.NewReader(cli)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		t.Fatalf("failed to read the upgrade response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	select {
	case srv = <-backendConns:
	case <-time.After(testStreamTimeout):
		t.Fatal("timed out waiting for the backend connection")
	}
	if srv != nil {
		t.Cleanup(func() { srv.Close() })
	}
	return cli, br, srv
}

func mustReadFullFrom(t *testing.T, r io.Reader, want string) {
	t.Helper()
	buf := make([]byte, len(want))
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(buf) != want {
		t.Fatalf("read %q, want %q", buf, want)
	}
}

// mustBeClosed checks the stream is closed from the client's point of view.
func mustBeClosed(t *testing.T, cli *net.TCPConn, br *bufio.Reader) {
	t.Helper()
	_ = cli.SetReadDeadline(time.Now().Add(testStreamTimeout))
	// a websocket close frame may precede the end of the connection
	if _, err := io.Copy(io.Discard, br); errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal("the stream was not closed")
	}
}

// mustStayOpen checks data still flows through the stream, in both directions.
func mustStayOpen(t *testing.T, cli *net.TCPConn, br *bufio.Reader, srv *net.TCPConn) {
	t.Helper()
	_ = cli.SetReadDeadline(time.Now().Add(testStreamTimeout))
	mustWrite(t, cli, "ping")
	mustReadFull(t, srv, "ping")
	mustWrite(t, srv, "pong")
	mustReadFullFrom(t, br, "pong")
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testStreamTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

var noopNext = caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error { return nil })

// The reload hook of a connection is optional.
func TestCloseConnectionsReloadHook(t *testing.T) {
	h := minimalHandler(0)
	h.connections = make(map[io.ReadWriteCloser]openConnection)
	h.connectionsMu = new(sync.Mutex)

	plain, hooked := new(closeTracker), new(closeTracker)
	var reloads atomic.Int32
	h.registerConnection(plain, nil, nil)
	h.registerConnection(hooked, nil, func() { reloads.Add(1) })

	if err := h.closeConnections(); err != nil {
		t.Fatal(err)
	}
	if !plain.closed.Load() || !hooked.closed.Load() {
		t.Error("connections were not closed")
	}
	if got := reloads.Load(); got != 1 {
		t.Errorf("reload hook called %d times, want 1", got)
	}
}

// A stream whose request carries no dial info (the proxy always sets it, so
// only a direct call can lack it) is accounted without an upstream, and
// stream_logs asking for the access logger falls back to the proxy's logger
// when the request is not access logged.
func TestHandleUpgradeResponseWithoutDialInfo(t *testing.T) {
	h := minimalHandler(0)
	h.streamMetrics = newStreamMetrics()
	h.StreamLogs = &StreamLogs{LoggerName: streamLogsAccessLogger}
	h.connections = make(map[io.ReadWriteCloser]openConnection)
	h.connectionsMu = new(sync.Mutex)

	backend, backendPeer := net.Pipe()
	defer backendPeer.Close()
	go func() { _, _ = io.Copy(io.Discard, backendPeer) }()

	done := make(chan struct{})
	frontend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		res := &http.Response{
			StatusCode: http.StatusSwitchingProtocols,
			Header:     http.Header{"Connection": []string{"Upgrade"}, "Upgrade": []string{"websocket"}},
			Body:       backend,
		}
		h.handleUpgradeResponse(zap.NewNop(), w, prepareTestRequest(r), res)
	}))
	t.Cleanup(frontend.Close)

	cli, _, _ := upgrade(t, frontend.URL, func() chan *net.TCPConn {
		c := make(chan *net.TCPConn, 1)
		c <- nil // there is no backend connection to wait for
		return c
	}())
	_ = cli.Close()

	select {
	case <-done:
	case <-time.After(testStreamTimeout):
		t.Fatal("the stream did not end")
	}
	// the pipe cannot be half closed, so the end of the client is an error
	if got := testutil.ToFloat64(h.streamMetrics.closed.WithLabelValues("", streamResultError)); got != 1 {
		t.Errorf("closed{error} = %v, want 1", got)
	}
}

// A detached HTTP/1.1 stream leaves the handler right after the upgrade,
// survives the request being done and the handler being cleaned up, and
// is closed only when its upstream is removed.
func TestHandlerDetachedStream(t *testing.T) {
	backendAddr, backendConns := upgradeBackend(t)

	h := minimalHandler(0, &Upstream{Host: new(Host), Dial: backendAddr})
	h.StreamDetached = true
	h.StreamLogs = &StreamLogs{SkipHandshake: true}
	h.streamMetrics = newStreamMetrics()
	h.connections = make(map[io.ReadWriteCloser]openConnection)
	h.connectionsMu = new(sync.Mutex)

	type result struct {
		skipped any
		status  int
		err     error
	}
	returned := make(chan result, 1)
	frontend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := caddyhttp.NewResponseRecorder(w, nil, nil)
		r = prepareTestRequest(r)
		err := h.ServeHTTP(rec, r, noopNext)
		// the handshake is the response the access log sees
		returned <- result{caddyhttp.GetVar(r.Context(), caddyhttp.LogSkipVar), rec.Status(), err}
	}))
	t.Cleanup(frontend.Close)

	cli, br, srv := upgrade(t, frontend.URL, backendConns)

	select {
	case res := <-returned:
		if res.err != nil {
			t.Errorf("ServeHTTP() error = %v", res.err)
		}
		if res.skipped != true {
			t.Errorf("the handshake was not flagged to be skipped by the access log: %v", res.skipped)
		}
		if res.status != http.StatusSwitchingProtocols {
			t.Errorf("recorded status = %d, want 101", res.status)
		}
	case <-time.After(testStreamTimeout):
		t.Fatal("the handler of a detached stream did not return")
	}

	if got := detachedStreams.count(backendAddr); got != 1 {
		t.Fatalf("detached streams to the upstream = %d, want 1", got)
	}
	mustStayOpen(t, cli, br, srv)

	// the config is unloaded: a detached stream is not closed with it
	if err := h.cleanupConnections(); err != nil {
		t.Fatal(err)
	}
	mustStayOpen(t, cli, br, srv)

	// the upstream is removed
	if got := detachedStreams.closeUpstream(backendAddr); got != 1 {
		t.Fatalf("closeUpstream() closed %d streams, want 1", got)
	}
	mustBeClosed(t, cli, br)

	waitFor(t, "the stream to be unregistered", func() bool { return detachedStreams.count(backendAddr) == 0 })
	waitFor(t, "the stream to be accounted", func() bool {
		return testutil.ToFloat64(h.streamMetrics.closed.WithLabelValues(backendAddr, streamResultReload)) == 1
	})
	if got := testutil.ToFloat64(h.streamMetrics.active.WithLabelValues(backendAddr)); got != 0 {
		t.Errorf("active streams = %v, want 0", got)
	}
}

// Without stream_detached the stream is tied to the handler: it is closed
// when the handler is cleaned up, after stream_close_delay if configured.
// Bytes are copied on the raw connection, not through the recorder wrapper.
func TestHandlerNotDetachedStreamClosedOnCleanup(t *testing.T) {
	for name, delay := range map[string]time.Duration{
		"immediately":    0,
		"after the delay": 400 * time.Millisecond,
	} {
		t.Run(name, func(t *testing.T) {
			backendAddr, backendConns := upgradeBackend(t)

			h := minimalHandler(0, &Upstream{Host: new(Host), Dial: backendAddr})
			h.StreamCloseDelay = caddy.Duration(delay)
			h.streamMetrics = newStreamMetrics()
			h.connections = make(map[io.ReadWriteCloser]openConnection)
			h.connectionsMu = new(sync.Mutex)

			type result struct {
				recordedSize int
				err          error
			}
			returned := make(chan result, 1)
			frontend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				rec := caddyhttp.NewResponseRecorder(w, nil, nil)
				err := h.ServeHTTP(rec, prepareTestRequest(r), noopNext)
				returned <- result{rec.Size(), err}
			}))
			t.Cleanup(frontend.Close)

			cli, br, srv := upgrade(t, frontend.URL, backendConns)
			if got := detachedStreams.count(backendAddr); got != 0 {
				t.Fatalf("a stream which is not detached was registered as detached: %d", got)
			}

			// a lot of data in each direction
			payload := strings.Repeat("x", 64<<10)
			go func() { _, _ = srv.Write([]byte(payload)) }()
			go func() { _, _ = cli.Write([]byte(payload)) }()
			mustReadFull(t, srv, payload)
			mustReadFullFrom(t, br, payload)

			start := time.Now()
			// closing one connection ends the stream, which may close the
			// other one first: an error here is expected sometimes
			_ = h.cleanupConnections()
			if delay > 0 {
				// the stream still works until the delay elapsed
				mustStayOpen(t, cli, br, srv)
			}
			mustBeClosed(t, cli, br)
			if elapsed := time.Since(start); elapsed < delay {
				t.Errorf("stream closed after %v, before the %v delay", elapsed, delay)
			}

			select {
			case res := <-returned:
				if res.err != nil {
					t.Errorf("ServeHTTP() error = %v", res.err)
				}
				// only the handshake went through the recorder
				if res.recordedSize >= len(payload) {
					t.Errorf("recorded size = %d; the stream must be copied on the raw connection, not through the recorder", res.recordedSize)
				}
			case <-time.After(testStreamTimeout):
				t.Fatal("the handler did not return after the stream was closed")
			}
			waitFor(t, "the stream to be accounted", func() bool {
				return testutil.ToFloat64(h.streamMetrics.closed.WithLabelValues(backendAddr, streamResultReload)) == 1
			})
		})
	}
}
