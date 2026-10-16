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

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

// upgradeRW is a response writer for an upgrade whose hijacked connection is
// a pipe, so that tests drive both ends of a stream deterministically.
type upgradeRW struct {
	header   http.Header
	conn     net.Conn
	brw      *bufio.ReadWriter
	statuses []int
}

func (rw *upgradeRW) Header() http.Header   { return rw.header }
func (rw *upgradeRW) WriteHeader(code int)  { rw.statuses = append(rw.statuses, code) }
func (rw *upgradeRW) Write([]byte) (int, error) {
	return 0, errors.New("body written to an upgrade response")
}
func (rw *upgradeRW) Hijack() (net.Conn, *bufio.ReadWriter, error) { return rw.conn, rw.brw, nil }

type upgradeHarness struct {
	h           *Handler
	rw          *upgradeRW
	backConn    net.Conn
	clientPeer  net.Conn
	backendPeer net.Conn
	done        chan struct{}
}

func newStreamTestHandler(logger *zap.Logger) *Handler {
	return &Handler{
		logger:        logger,
		connections:   make(map[io.ReadWriteCloser]openConnection),
		connectionsMu: new(sync.Mutex),
	}
}

func newUpgradeHarness(t *testing.T, h *Handler) *upgradeHarness {
	t.Helper()
	conn, clientPeer := net.Pipe()
	backConn, backendPeer := net.Pipe()
	t.Cleanup(func() {
		clientPeer.Close()
		backendPeer.Close()
	})
	return &upgradeHarness{
		h: h,
		rw: &upgradeRW{
			header: make(http.Header),
			conn:   conn,
			brw:    bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
		},
		backConn:    backConn,
		clientPeer:  clientPeer,
		backendPeer: backendPeer,
		done:        make(chan struct{}),
	}
}

// start runs the upgrade in the background; done is closed when it returns.
func (u *upgradeHarness) start(ctx context.Context) {
	req := httptest.NewRequest(http.MethodGet, "/stream", nil).WithContext(ctx)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "foo")
	res := &http.Response{
		StatusCode: http.StatusSwitchingProtocols,
		Header:     http.Header{"Upgrade": {"foo"}, "Connection": {"Upgrade"}},
		Body:       u.backConn,
	}
	go func() {
		var wg sync.WaitGroup
		u.h.handleUpgradeResponse(zap.NewNop(), &wg, u.rw, req, res)
		wg.Wait()
		close(u.done)
	}()
}

func (u *upgradeHarness) waitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-u.done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the stream to close")
	}
}

func (u *upgradeHarness) assertOpen(t *testing.T) {
	t.Helper()
	select {
	case <-u.done:
		t.Fatal("the stream was closed, want it to stay open")
	case <-time.After(150 * time.Millisecond):
	}
}

// assertRelays checks that data flows from the client to the backend.
func (u *upgradeHarness) assertRelays(t *testing.T) {
	t.Helper()
	go u.clientPeer.Write([]byte("ping"))
	buf := make([]byte, 4)
	if err := u.backendPeer.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(u.backendPeer, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("backend read %q, %v; want ping", buf, err)
	}
}

func waitForStreamCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func openConnectionCount(h *Handler) int {
	h.connectionsMu.Lock()
	defer h.connectionsMu.Unlock()
	return len(h.connections)
}

func detachedStreamCount(upstream string) int {
	detachedStreams.mu.Lock()
	defer detachedStreams.mu.Unlock()
	return len(detachedStreams.open[upstream])
}

func closedDelta(before, after streamMetricsSnapshot, result string) float64 {
	return after.closed[result] - before.closed[result]
}

func TestUpgradedStreamIsAccountedOnce(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	h := newStreamTestHandler(zap.New(core))
	u := newUpgradeHarness(t, h)

	// data already buffered when the connection is hijacked goes to the backend
	early := bufio.NewReader(strings.NewReader("early"))
	_, _ = early.Peek(1)
	u.rw.brw = bufio.NewReadWriter(early, bufio.NewWriter(u.rw.conn))

	before := snapshotStreamMetrics(t)
	u.start(context.Background())

	got := make(chan string, 1)
	go func() {
		buf := make([]byte, 10)
		if _, err := io.ReadFull(u.backendPeer, buf); err != nil {
			got <- err.Error()
			return
		}
		got <- string(buf)
	}()
	if _, err := u.clientPeer.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-got:
		if s != "earlyhello" {
			t.Fatalf("backend received %q, want %q", s, "earlyhello")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the backend to receive the data")
	}

	go u.backendPeer.Write([]byte("world!"))
	buf := make([]byte, 6)
	if _, err := io.ReadFull(u.clientPeer, buf); err != nil || string(buf) != "world!" {
		t.Fatalf("client read %q, %v; want world!", buf, err)
	}

	u.clientPeer.Close()
	u.waitClosed(t)

	if len(u.rw.statuses) != 1 || u.rw.statuses[0] != http.StatusSwitchingProtocols {
		t.Errorf("statuses written = %v, want only the 101", u.rw.statuses)
	}

	after := snapshotStreamMetrics(t)
	if after.active != before.active {
		t.Errorf("active streams = %v, want %v", after.active, before.active)
	}
	if got := closedDelta(before, after, streamResultCompleted); got != 1 {
		t.Errorf("completed streams went up by %v, want 1", got)
	}
	if got := after.durationCount - before.durationCount; got != 1 {
		t.Errorf("duration observations went up by %d, want 1", got)
	}
	if got := after.toBackend - before.toBackend; got != 10 {
		t.Errorf("bytes to backend went up by %v, want 10", got)
	}
	if got := after.fromBackend - before.fromBackend; got != 6 {
		t.Errorf("bytes from backend went up by %v, want 6", got)
	}
	if n := openConnectionCount(h); n != 0 {
		t.Errorf("%d connections still registered, want 0", n)
	}

	entries := logs.FilterMessage("stream closed").All()
	if len(entries) != 1 {
		t.Fatalf("got %d stream close entries, want 1", len(entries))
	}
	if entries[0].Level != zapcore.DebugLevel {
		t.Errorf("close entry level = %v, want debug", entries[0].Level)
	}
	fields := entries[0].ContextMap()
	if fields["result"] != streamResultCompleted ||
		fields["bytes_to_backend"] != int64(10) ||
		fields["bytes_from_backend"] != int64(6) {
		t.Errorf("close entry fields = %v", fields)
	}
}

func TestStreamClosedLogLevel(t *testing.T) {
	for _, tc := range []struct {
		level string
		want  zapcore.Level
	}{
		{level: "", want: zapcore.DebugLevel},
		{level: "warn", want: zapcore.WarnLevel},
	} {
		core, logs := observer.New(zapcore.DebugLevel)
		h := newStreamTestHandler(zap.New(core))
		if tc.level != "" {
			h.StreamLogs = &StreamLogs{Level: tc.level}
		}
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		h.logStreamClosed(req, &upgradedStream{upstream: "u"}, streamResultCompleted, time.Second, 1, 2)

		entries := logs.FilterMessage("stream closed").All()
		if len(entries) != 1 || entries[0].Level != tc.want {
			t.Errorf("level %q: entries = %v, want one at %v", tc.level, entries, tc.want)
		}
	}

	// entries below the level of the logger are dropped
	core, logs := observer.New(zapcore.InfoLevel)
	h := newStreamTestHandler(zap.New(core))
	h.logStreamClosed(httptest.NewRequest(http.MethodGet, "/", nil), &upgradedStream{}, streamResultCompleted, 0, 0, 0)
	if logs.Len() != 0 {
		t.Errorf("a debug entry was written to an info logger: %v", logs.All())
	}
}

func TestUpgradedStreamTimeout(t *testing.T) {
	h := newStreamTestHandler(zap.NewNop())
	h.StreamTimeout = caddy.Duration(30 * time.Millisecond)
	u := newUpgradeHarness(t, h)

	before := snapshotStreamMetrics(t)
	u.start(context.Background())
	u.waitClosed(t)

	after := snapshotStreamMetrics(t)
	if got := closedDelta(before, after, streamResultTimeout); got != 1 {
		t.Errorf("timed out streams went up by %v, want 1", got)
	}
	if after.active != before.active {
		t.Errorf("active streams = %v, want %v", after.active, before.active)
	}
}

func TestUpgradedStreamCanceled(t *testing.T) {
	h := newStreamTestHandler(zap.NewNop())
	u := newUpgradeHarness(t, h)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	before := snapshotStreamMetrics(t)
	u.start(ctx)
	waitForStreamCond(t, "the stream to be registered", func() bool { return openConnectionCount(h) == 2 })
	cancel()
	u.waitClosed(t)

	after := snapshotStreamMetrics(t)
	if got := closedDelta(before, after, streamResultCanceled); got != 1 {
		t.Errorf("canceled streams went up by %v, want 1", got)
	}
}

// failingConn is a connection that can't be written to.
type failingConn struct{ net.Conn }

func (failingConn) Write([]byte) (int, error) { return 0, errors.New("write failed") }
func (failingConn) Close() error              { return nil }

func TestUpgradedStreamFlushFailure(t *testing.T) {
	h := newStreamTestHandler(zap.NewNop())
	u := newUpgradeHarness(t, h)
	w := bufio.NewWriter(failingConn{})
	_, _ = w.WriteString("pending")
	u.rw.brw = bufio.NewReadWriter(bufio.NewReader(u.rw.conn), w)

	before := snapshotStreamMetrics(t)
	u.start(context.Background())
	u.waitClosed(t)

	after := snapshotStreamMetrics(t)
	if got := closedDelta(before, after, streamResultError); got != 1 {
		t.Errorf("failed streams went up by %v, want 1", got)
	}
	if after.active != before.active {
		t.Errorf("active streams = %v, want %v", after.active, before.active)
	}
}

func TestUpgradedStreamBufferedWriteFailure(t *testing.T) {
	h := newStreamTestHandler(zap.NewNop())
	u := newUpgradeHarness(t, h)
	early := bufio.NewReader(strings.NewReader("early"))
	_, _ = early.Peek(1)
	u.rw.brw = bufio.NewReadWriter(early, bufio.NewWriter(u.rw.conn))
	u.backendPeer.Close()

	before := snapshotStreamMetrics(t)
	u.start(context.Background())
	u.waitClosed(t)

	after := snapshotStreamMetrics(t)
	if got := closedDelta(before, after, streamResultError); got != 1 {
		t.Errorf("failed streams went up by %v, want 1", got)
	}
	if got := after.toBackend - before.toBackend; got != 0 {
		t.Errorf("bytes to backend went up by %v, want 0", got)
	}
}

func TestUpgradedStreamSkipHandshakeLog(t *testing.T) {
	for _, skip := range []bool{false, true} {
		h := newStreamTestHandler(zap.NewNop())
		if skip {
			h.StreamLogs = &StreamLogs{SkipHandshake: true}
		}
		u := newUpgradeHarness(t, h)
		vars := make(map[string]any)
		ctx := context.WithValue(context.Background(), caddyhttp.VarsCtxKey, vars)
		u.start(ctx)
		u.clientPeer.Close()
		u.waitClosed(t)

		got, _ := vars[caddyhttp.LogSkipVar].(bool)
		if got != skip {
			t.Errorf("skip_handshake=%v: %s = %v, want %v", skip, caddyhttp.LogSkipVar, got, skip)
		}
	}
}

func TestDefaultModeClosesStreamsOnCleanup(t *testing.T) {
	h := newStreamTestHandler(zap.NewNop())
	u := newUpgradeHarness(t, h)

	before := snapshotStreamMetrics(t)
	u.start(context.Background())
	waitForStreamCond(t, "the stream to be registered", func() bool { return openConnectionCount(h) == 2 })
	u.assertRelays(t)

	if err := h.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	u.waitClosed(t)

	after := snapshotStreamMetrics(t)
	if got := closedDelta(before, after, streamResultReload); got != 1 {
		t.Errorf("streams closed by reload went up by %v, want 1", got)
	}
	if after.active != before.active {
		t.Errorf("active streams = %v, want %v", after.active, before.active)
	}
}

func TestDefaultModeClosesStreamsAfterCloseDelay(t *testing.T) {
	h := newStreamTestHandler(zap.NewNop())
	h.StreamCloseDelay = caddy.Duration(300 * time.Millisecond)
	u := newUpgradeHarness(t, h)

	u.start(context.Background())
	waitForStreamCond(t, "the stream to be registered", func() bool { return openConnectionCount(h) == 2 })

	start := time.Now()
	if err := h.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	u.assertOpen(t)
	u.assertRelays(t)
	u.waitClosed(t)
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
		t.Errorf("stream closed after %v, want it to wait for the delay", elapsed)
	}
}

func detachedTestHandler(upstreams ...string) *Handler {
	h := newStreamTestHandler(zap.NewNop())
	h.StreamDetached = true
	for _, dial := range upstreams {
		h.Upstreams = append(h.Upstreams, &Upstream{Dial: dial})
	}
	h.registeredUpstreams = h.liveUpstreams()
	detachedStreams.addLive(h.registeredUpstreams)
	return h
}

func startDetachedStream(t *testing.T, h *Handler, upstream string) *upgradeHarness {
	t.Helper()
	u := newUpgradeHarness(t, h)
	ctx := context.WithValue(context.Background(), dialInfoCtxKey, DialInfo{Upstream: &Upstream{Dial: upstream}})
	u.start(ctx)
	waitForStreamCond(t, "the stream to be registered", func() bool { return detachedStreamCount(upstream) == 1 })
	return u
}

// A detached stream survives a reload as long as its upstream is part of the
// new config, and is closed once a reload removes that upstream.
func TestDetachedStreamsFollowTheirUpstream(t *testing.T) {
	const upA, upB = "detached-a.test:80", "detached-b.test:80"
	oldConfig := detachedTestHandler(upA, upB)
	newConfig := detachedTestHandler(upB)

	streamA := startDetachedStream(t, oldConfig, upA)
	streamB := startDetachedStream(t, oldConfig, upB)
	if n := openConnectionCount(oldConfig); n != 0 {
		t.Fatalf("%d connections registered on the handler, want detached streams to be tracked by upstream", n)
	}

	before := snapshotStreamMetrics(t)

	// the reload: the new config was provisioned, the old one is cleaned up
	if err := oldConfig.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	streamA.waitClosed(t)
	streamB.assertOpen(t)
	streamB.assertRelays(t)

	after := snapshotStreamMetrics(t)
	if got := closedDelta(before, after, streamResultReload); got != 1 {
		t.Errorf("streams closed by reload went up by %v, want 1", got)
	}

	// the next reload removes the last user of the upstream
	if err := newConfig.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	streamB.waitClosed(t)
	if n := detachedStreamCount(upB); n != 0 {
		t.Errorf("%d detached streams still tracked, want 0", n)
	}
}

// Detached streams ignore the close delay: they are not on a timer, they
// are closed with their upstream.
func TestDetachedStreamIgnoresCloseDelay(t *testing.T) {
	const up = "detached-delay.test:80"
	h := detachedTestHandler(up)
	h.StreamCloseDelay = caddy.Duration(time.Hour)
	u := startDetachedStream(t, h, up)

	if err := h.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	u.waitClosed(t)
}

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
