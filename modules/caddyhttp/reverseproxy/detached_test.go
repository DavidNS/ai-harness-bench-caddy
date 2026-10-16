package reverseproxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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

const streamTestTimeout = 10 * time.Second

// streamBackend is a backend that answers every request with a 101 and hands
// the hijacked connection to the test.
type streamBackend struct {
	server *httptest.Server
	conns  chan net.Conn
}

func newStreamBackend(t *testing.T) *streamBackend {
	t.Helper()
	b := &streamBackend{conns: make(chan net.Conn, 4)}
	b.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("backend hijack failed: %v", err)
			return
		}
		fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", r.Header.Get("Upgrade"))
		b.conns <- conn
	}))
	t.Cleanup(b.server.Close)
	return b
}

func (b *streamBackend) addr() string { return b.server.Listener.Addr().String() }

// upstream returns an upstream for the backend that is part of the pool of
// hosts, like the upstreams of a provisioned config.
func (b *streamBackend) upstream() *Upstream {
	u := &Upstream{Dial: b.addr()}
	u.fillHost()
	return u
}

// streamHandler returns a handler for the upstream, as if provisioned.
func streamHandler(u *Upstream) *Handler {
	h := minimalHandler(0, u)
	h.connections = make(map[io.ReadWriteCloser]openConnection)
	h.connectionsMu = new(sync.Mutex)
	return h
}

// streamServe is what the frontend handler observed once it returned.
type streamServe struct {
	err error
	req *http.Request
}

// streamPair is an established upgraded stream.
type streamPair struct {
	cli    net.Conn
	br     *bufio.Reader // of cli
	srv    net.Conn      // the backend side
	served chan streamServe
}

// openStream sends an upgrade request for the protocol upgrade through a
// frontend that serves it with h, and returns the established stream.
// wrap, if set, wraps the response writer given to h.
func openStream(t *testing.T, h *Handler, b *streamBackend, upgrade string, wrap func(http.ResponseWriter) http.ResponseWriter) *streamPair {
	t.Helper()

	p := &streamPair{served: make(chan streamServe, 1)}
	frontend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = prepareTestRequest(r)
		if wrap != nil {
			w = wrap(w)
		}
		err := h.ServeHTTP(w, r, caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error {
			return nil
		}))
		p.served <- streamServe{err: err, req: r}
	}))
	t.Cleanup(frontend.Close)

	cli, err := net.Dial("tcp", frontend.Listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial frontend: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	p.cli = cli

	req, _ := http.NewRequest(http.MethodGet, frontend.URL, nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", upgrade)
	if err := req.Write(cli); err != nil {
		t.Fatalf("failed to write upgrade request: %v", err)
	}
	p.br = bufio.NewReader(cli)
	resp, err := http.ReadResponse(p.br, req)
	if err != nil {
		t.Fatalf("failed to read upgrade response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}

	select {
	case p.srv = <-b.conns:
		t.Cleanup(func() { p.srv.Close() })
	case <-time.After(streamTestTimeout):
		t.Fatal("timed out waiting for the backend connection")
	}
	return p
}

func (p *streamPair) send(t *testing.T, conn net.Conn, msg string) {
	t.Helper()
	if _, err := io.WriteString(conn, msg); err != nil {
		t.Fatalf("write %q: %v", msg, err)
	}
}

func (p *streamPair) expect(t *testing.T, conn net.Conn, r io.Reader, msg string) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(streamTestTimeout))
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("read %q: %v", msg, err)
	}
	if string(buf) != msg {
		t.Fatalf("got %q, want %q", buf, msg)
	}
}

// roundTrip checks that data goes both ways through the stream.
func (p *streamPair) roundTrip(t *testing.T) {
	t.Helper()
	p.send(t, p.cli, "ping")
	p.expect(t, p.srv, p.srv, "ping")
	p.send(t, p.srv, "pong")
	p.expect(t, p.cli, p.br, "pong")
}

// expectClosed checks that the proxy closed the client side of the stream.
func (p *streamPair) expectClosed(t *testing.T) {
	t.Helper()
	p.cli.SetReadDeadline(time.Now().Add(streamTestTimeout))
	_, err := p.br.ReadByte()
	if err == nil {
		t.Fatal("the stream is still passing data, want it closed")
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatalf("the stream was not closed: %v", err)
	}
}

// expectOpen checks that the stream is still working.
func (p *streamPair) expectOpen(t *testing.T) {
	t.Helper()
	p.roundTrip(t)
}

func (p *streamPair) waitServed(t *testing.T) streamServe {
	t.Helper()
	select {
	case s := <-p.served:
		return s
	case <-time.After(streamTestTimeout):
		t.Fatal("timed out waiting for the handler to return")
		return streamServe{}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(streamTestTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDetachedRegistry(t *testing.T) {
	r := &detachedRegistry{streams: make(map[string]map[*detachedStream]struct{})}

	closed := map[string]int{}
	newStream := func(upstream string, closeErr error) *detachedStream {
		return &detachedStream{
			stats: &streamStats{upstream: upstream},
			conns: []openConnection{
				{conn: closeCounter{func() error { closed[upstream]++; return closeErr }}, gracefulClose: func() error { return nil }},
			},
		}
	}

	a1, a2, b1 := newStream("a", nil), newStream("a", errors.New("close failed")), newStream("b", nil)
	removeA1 := r.add("a", a1)
	r.add("a", a2)
	r.add("b", b1)
	if got := r.count("a"); got != 2 {
		t.Fatalf("count(a) = %d, want 2", got)
	}

	// removing is idempotent and keeps the other streams of the upstream
	removeA1()
	removeA1()
	if got := r.count("a"); got != 1 {
		t.Fatalf("count(a) after remove = %d, want 1", got)
	}

	// only the streams of the upstream are closed, with their error reported
	n, err := r.closeUpstream("a")
	if n != 1 || err == nil || err.Error() != "close failed" {
		t.Fatalf("closeUpstream(a) = %d, %v; want 1, close failed", n, err)
	}
	if closed["a"] != 1 || closed["b"] != 0 {
		t.Errorf("closed = %v, want only a closed once", closed)
	}
	if got := a2.stats.result; got != streamResultUpstreamRemoved {
		t.Errorf("result = %q, want %q", got, streamResultUpstreamRemoved)
	}
	if r.count("a") != 0 || r.count("b") != 1 {
		t.Errorf("count(a) = %d, count(b) = %d; want 0, 1", r.count("a"), r.count("b"))
	}

	// an upstream without streams, and a stream removing itself after the
	// upstream was closed, are fine
	if n, err := r.closeUpstream("a"); n != 0 || err != nil {
		t.Errorf("closeUpstream(a) again = %d, %v; want 0, nil", n, err)
	}
	removeA1()
}

type closeCounter struct{ closeFn func() error }

func (c closeCounter) Read([]byte) (int, error)    { return 0, io.EOF }
func (c closeCounter) Write(p []byte) (int, error) { return len(p), nil }
func (c closeCounter) Close() error                { return c.closeFn() }

func TestCloseOpenConnectionsReportsFirstError(t *testing.T) {
	first, second := errors.New("graceful"), errors.New("close")
	var order []string
	conns := []openConnection{
		{
			conn:          closeCounter{func() error { order = append(order, "close1"); return second }},
			gracefulClose: func() error { order = append(order, "graceful1"); return first },
		},
		{
			conn:          closeCounter{func() error { order = append(order, "close2"); return nil }},
			gracefulClose: func() error { order = append(order, "graceful2"); return second },
		},
	}
	if err := closeOpenConnections(conns); err != first {
		t.Errorf("error = %v, want %v", err, first)
	}
	if got := strings.Join(order, ","); got != "graceful1,close1,graceful2,close2" {
		t.Errorf("order = %s", got)
	}

	// a connection without graceful close, failing to close
	if err := closeOpenConnections([]openConnection{{conn: closeCounter{func() error { return second }}}}); err != second {
		t.Errorf("error = %v, want %v", err, second)
	}
}

func TestProvisionStreamLogs(t *testing.T) {
	for i, tc := range []struct {
		logs      *StreamLogs
		wantLevel zapcore.Level
		wantErr   bool
	}{
		{logs: nil, wantLevel: zapcore.DebugLevel},
		{logs: &StreamLogs{SkipHandshake: true}, wantLevel: zapcore.DebugLevel},
		{logs: &StreamLogs{Level: "warn"}, wantLevel: zapcore.WarnLevel},
		{logs: &StreamLogs{Level: "INFO", LoggerName: "access"}, wantLevel: zapcore.InfoLevel},
		{logs: &StreamLogs{Level: "loud"}, wantErr: true},
		{logs: &StreamLogs{LoggerName: "nope"}, wantErr: true},
	} {
		h := &Handler{StreamLogs: tc.logs}
		err := h.provisionStreamLogs()
		if tc.wantErr {
			if err == nil {
				t.Errorf("Test %d: expected an error", i)
			}
			continue
		}
		if err != nil {
			t.Errorf("Test %d: unexpected error: %v", i, err)
			continue
		}
		if got := h.streamLevel(); got != tc.wantLevel {
			t.Errorf("Test %d: level = %v, want %v", i, got, tc.wantLevel)
		}
	}
}

func TestStreamLoggers(t *testing.T) {
	base := zap.NewNop()
	req := prepareTestRequest(httptest.NewRequest(http.MethodGet, "/", nil))

	// by default, the logger of the handler
	for _, logs := range []*StreamLogs{nil, {Level: "info"}} {
		h := &Handler{logger: base, StreamLogs: logs}
		if got := h.streamLoggers(req); len(got) != 1 || got[0] != base {
			t.Errorf("loggers = %v, want the logger of the handler", got)
		}
	}

	// the access log: nothing for a request that is not access logged,
	// and for one that doesn't belong to a server
	h := &Handler{logger: base, StreamLogs: &StreamLogs{LoggerName: "access"}}
	if got := h.streamLoggers(req); len(got) != 0 {
		t.Errorf("loggers = %v, want none for a request that is not logged", got)
	}
	if got := h.streamLoggers(httptest.NewRequest(http.MethodGet, "/", nil)); len(got) != 0 {
		t.Errorf("loggers = %v, want none without a server", got)
	}

	// with and without the original request, which decides what is logged
	for _, withOriginal := range []bool{false, true} {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		ctx := context.WithValue(r.Context(), caddyhttp.ServerCtxKey, &caddyhttp.Server{})
		if withOriginal {
			ctx = context.WithValue(ctx, caddyhttp.OriginalRequestCtxKey, *r)
		}
		if got := h.streamLoggers(r.WithContext(ctx)); len(got) != 0 {
			t.Errorf("original request %v: loggers = %v, want none for a request that is not logged", withOriginal, got)
		}
	}
}

func TestUpstreamKey(t *testing.T) {
	newRes := func(ctx context.Context) *http.Response {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/", nil)
		return &http.Response{Request: req}
	}

	if got := upstreamKey(&http.Response{}); got != "" {
		t.Errorf("without request: %q, want empty", got)
	}
	if got := upstreamKey(newRes(context.Background())); got != "" {
		t.Errorf("without dial info: %q, want empty", got)
	}
	withUpstream := context.WithValue(context.Background(), dialInfoCtxKey, DialInfo{Upstream: &Upstream{Dial: "up:80"}, Address: "10.0.0.1:80"})
	if got := upstreamKey(newRes(withUpstream)); got != "up:80" {
		t.Errorf("with upstream: %q, want up:80", got)
	}
	withAddress := context.WithValue(context.Background(), dialInfoCtxKey, DialInfo{Address: "10.0.0.1:80"})
	if got := upstreamKey(newRes(withAddress)); got != "10.0.0.1:80" {
		t.Errorf("with address only: %q, want 10.0.0.1:80", got)
	}
}

// A detached stream is established through the handler, which returns right
// away; the stream then survives the cleanup of the handler as long as another
// config still uses its upstream, and is closed once none does.
func TestDetachedStreamSurvivesReload(t *testing.T) {
	b := newStreamBackend(t)
	hOld := streamHandler(b.upstream())
	hOld.StreamDetached = true
	// the reloaded config keeps the upstream
	hNew := streamHandler(b.upstream())

	p := openStream(t, hOld, b, "foo", nil)
	served := p.waitServed(t)
	if served.err != nil {
		t.Fatalf("ServeHTTP returned %v", served.err)
	}
	key := b.addr()
	if got := detachedStreams.count(key); got != 1 {
		t.Fatalf("detached streams of the upstream = %d, want 1", got)
	}
	p.expectOpen(t)

	// the old config is unloaded after the new one was loaded
	if err := hOld.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	p.expectOpen(t)
	if got := detachedStreams.count(key); got != 1 {
		t.Fatalf("detached streams of the upstream after reload = %d, want 1", got)
	}

	// the upstream is gone with the next config
	before := counterValue(streamMetrics.closed, key, streamResultUpstreamRemoved)
	if err := hNew.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	p.expectClosed(t)
	waitFor(t, "the stream to be forgotten", func() bool { return detachedStreams.count(key) == 0 })
	waitFor(t, "the stream to be accounted for", func() bool {
		return counterValue(streamMetrics.closed, key, streamResultUpstreamRemoved) == before+1
	})
	if got := gaugeValue(streamMetrics.active, key); got != 0 {
		t.Errorf("active streams = %v, want 0", got)
	}
}

// Only the streams whose upstream was removed from the new config are closed.
func TestDetachedStreamClosedWhenUpstreamRemoved(t *testing.T) {
	removed, kept := newStreamBackend(t), newStreamBackend(t)
	hRemoved := streamHandler(removed.upstream())
	hRemoved.StreamDetached = true
	hKept := streamHandler(kept.upstream())
	hKept.StreamDetached = true
	hNew := streamHandler(kept.upstream()) // the new config only has the kept upstream
	defer hNew.Cleanup()

	pRemoved := openStream(t, hRemoved, removed, "foo", nil)
	pKept := openStream(t, hKept, kept, "foo", nil)
	pRemoved.expectOpen(t)
	pKept.expectOpen(t)

	if err := hRemoved.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if err := hKept.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	pRemoved.expectClosed(t)
	pKept.expectOpen(t)
}

func TestHandlerCleanupWithoutStreams(t *testing.T) {
	b := newStreamBackend(t)
	h := streamHandler(b.upstream())
	if err := h.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if got := detachedStreams.count(b.addr()); got != 0 {
		t.Errorf("detached streams = %d, want 0", got)
	}
}

// Without stream_detached the stream is closed on reload (the handler is
// cleaned up), and counted as closed by cleanup.
func TestAttachedStreamClosedOnCleanup(t *testing.T) {
	b := newStreamBackend(t)
	h := streamHandler(b.upstream())

	p := openStream(t, h, b, "foo", nil)
	p.expectOpen(t)

	key := b.addr()
	before := counterValue(streamMetrics.closed, key, streamResultCleanup)
	if got := gaugeValue(streamMetrics.active, key); got != 1 {
		t.Errorf("active streams = %v, want 1", got)
	}
	if got := detachedStreams.count(key); got != 0 {
		t.Errorf("the stream of a handler that is not detached is registered as detached")
	}

	if err := h.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	p.expectClosed(t)

	if s := p.waitServed(t); s.err != nil {
		t.Errorf("ServeHTTP returned %v", s.err)
	}
	waitFor(t, "the stream to be accounted for", func() bool {
		return counterValue(streamMetrics.closed, key, streamResultCleanup) == before+1
	})
	if got := gaugeValue(streamMetrics.active, key); got != 0 {
		t.Errorf("active streams = %v, want 0", got)
	}
}

// With stream_close_delay the stream stays open until the delay elapsed.
func TestAttachedStreamCloseDelay(t *testing.T) {
	b := newStreamBackend(t)
	h := streamHandler(b.upstream())
	h.StreamCloseDelay = caddy.Duration(500 * time.Millisecond)

	p := openStream(t, h, b, "foo", nil)
	p.expectOpen(t)

	start := time.Now()
	if err := h.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	p.expectOpen(t)
	if time.Since(start) >= 500*time.Millisecond {
		t.Skip("the machine is too slow to tell that the stream is open before the delay")
	}

	p.expectClosed(t)
	if elapsed := time.Since(start); elapsed < 450*time.Millisecond {
		t.Errorf("the stream was closed after %v, before the delay", elapsed)
	}
}

// A closing WebSocket gets a close frame first.
func TestStreamCleanupGracefullyClosesWebSocket(t *testing.T) {
	b := newStreamBackend(t)
	h := streamHandler(b.upstream())

	p := openStream(t, h, b, "websocket", nil)
	p.expectOpen(t)

	if err := h.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	// close frame, 1001 going away, from the proxy acting as server
	p.expect(t, p.cli, p.br, "\x88\x02\x03\xe9")
	p.expectClosed(t)
}

func TestStreamTimeout(t *testing.T) {
	b := newStreamBackend(t)
	h := streamHandler(b.upstream())
	h.StreamTimeout = caddy.Duration(200 * time.Millisecond)

	key := b.addr()
	before := counterValue(streamMetrics.closed, key, streamResultTimeout)

	p := openStream(t, h, b, "foo", nil)
	p.expectOpen(t)
	p.expectClosed(t)
	waitFor(t, "the stream to be accounted for", func() bool {
		return counterValue(streamMetrics.closed, key, streamResultTimeout) == before+1
	})
}

func TestStreamSkipHandshake(t *testing.T) {
	for _, tc := range []struct {
		name string
		logs *StreamLogs
		want bool
	}{
		{name: "default"},
		{name: "logs without skip", logs: &StreamLogs{Level: "info"}},
		{name: "skip", logs: &StreamLogs{SkipHandshake: true}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newStreamBackend(t)
			h := streamHandler(b.upstream())
			h.StreamDetached = true // so that the handler returns
			h.StreamLogs = tc.logs

			p := openStream(t, h, b, "foo", nil)
			served := p.waitServed(t)
			skip, _ := caddyhttp.GetVar(served.req.Context(), caddyhttp.LogSkipVar).(bool)
			if skip != tc.want {
				t.Errorf("access log skipped = %v, want %v", skip, tc.want)
			}
			h.Cleanup()
		})
	}
}

// The stream close entry has the duration, the bytes, the result and the
// level that was configured.
func TestStreamCloseLog(t *testing.T) {
	b := newStreamBackend(t)
	h := streamHandler(b.upstream())
	h.StreamLogs = &StreamLogs{Level: "warn"}
	core, logs := observer.New(zapcore.DebugLevel)
	h.logger = zap.New(core)

	p := openStream(t, h, b, "foo", nil)
	p.roundTrip(t)
	if err := h.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	p.expectClosed(t)

	waitFor(t, "the stream close entry", func() bool { return logs.FilterMessage("stream closed").Len() == 1 })
	entry := logs.FilterMessage("stream closed").All()[0]
	if entry.Level != zapcore.WarnLevel {
		t.Errorf("level = %v, want WARN", entry.Level)
	}
	fields := entry.ContextMap()
	if fields["upstream"] != b.addr() {
		t.Errorf("upstream = %v, want %s", fields["upstream"], b.addr())
	}
	if fields["result"] != streamResultCleanup {
		t.Errorf("result = %v, want %s", fields["result"], streamResultCleanup)
	}
	if fields["bytes_to_backend"] != int64(4) || fields["bytes_from_backend"] != int64(4) {
		t.Errorf("bytes = %v to, %v from; want 4 each", fields["bytes_to_backend"], fields["bytes_from_backend"])
	}
	if _, ok := fields["duration"]; !ok {
		t.Error("the entry has no duration")
	}
}

// The proxy moves the stream on the raw connection: it is not counted in the
// size of the recorded response, and the recorder is not written to.
func TestStreamBypassesResponseRecorder(t *testing.T) {
	for _, detached := range []bool{false, true} {
		t.Run(fmt.Sprintf("detached=%v", detached), func(t *testing.T) {
			b := newStreamBackend(t)
			h := streamHandler(b.upstream())
			h.StreamDetached = detached

			var rec caddyhttp.ResponseRecorder
			p := openStream(t, h, b, "foo", func(w http.ResponseWriter) http.ResponseWriter {
				rec = caddyhttp.NewResponseRecorder(w, nil, nil)
				return rec
			})
			p.roundTrip(t)
			p.send(t, p.cli, strings.Repeat("x", 1000))
			p.expect(t, p.srv, p.srv, strings.Repeat("x", 1000))

			if err := h.Cleanup(); err != nil {
				t.Fatalf("Cleanup() error = %v", err)
			}
			p.expectClosed(t)
			p.waitServed(t)

			if rec.Status() != http.StatusSwitchingProtocols {
				t.Errorf("status = %d, want 101", rec.Status())
			}
			if rec.Size() != 0 {
				t.Errorf("size = %d, want 0: stream bytes went through the recorder", rec.Size())
			}
		})
	}
}

// newTestStream returns an upgradedStream between two pipes. The test
// controls the far ends.
func newTestStream(t *testing.T, buffered string) (s *upgradedStream, client, backend net.Conn) {
	t.Helper()
	front, client := net.Pipe()
	back, backend := net.Pipe()
	t.Cleanup(func() {
		front.Close()
		client.Close()
		back.Close()
		backend.Close()
	})
	brw := bufio.NewReadWriter(bufio.NewReader(io.MultiReader(strings.NewReader(buffered), front)), bufio.NewWriter(io.Discard))
	if buffered != "" {
		brw.Peek(len(buffered))
	}
	s = &upgradedStream{
		logger: zap.NewNop(),
		stats:  newStreamStats(t.Name(), nil, zapcore.DebugLevel),
		front:  front,
		back:   back,
		brw:    brw,
		wg:     new(sync.WaitGroup),
		track:  func(...openConnection) func() { return func() {} },
	}
	return s, client, backend
}

func TestUpgradedStreamBufferedClientBytes(t *testing.T) {
	s, client, backend := newTestStream(t, "early")
	done := make(chan struct{})
	go func() { s.run(); close(done) }()

	buf := make([]byte, 5)
	backend.SetReadDeadline(time.Now().Add(streamTestTimeout))
	if _, err := io.ReadFull(backend, buf); err != nil || string(buf) != "early" {
		t.Fatalf("backend got %q, %v; want the bytes the client sent along with the handshake", buf, err)
	}

	// both ends going away is a regular end
	client.Close()
	backend.Close()
	<-done
	if got := s.stats.toBackend.Load(); got < 5 {
		t.Errorf("bytes to backend = %d, want at least 5", got)
	}
}

func TestUpgradedStreamBufferedWriteFailure(t *testing.T) {
	s, _, backend := newTestStream(t, "early")
	backend.Close() // writes to the backend fail

	s.run()
	if s.stats.result != streamResultError {
		t.Errorf("result = %q, want %q", s.stats.result, streamResultError)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func TestUpgradedStreamFlushFailure(t *testing.T) {
	s, _, _ := newTestStream(t, "")
	bw := bufio.NewWriter(failingWriter{})
	bw.WriteString("headers")
	s.brw = bufio.NewReadWriter(s.brw.Reader, bw)

	s.run()
	if s.stats.result != streamResultError {
		t.Errorf("result = %q, want %q", s.stats.result, streamResultError)
	}
}

func TestUpgradedStreamCanceled(t *testing.T) {
	s, _, _ := newTestStream(t, "")
	canceled := make(chan struct{})
	s.canceled = canceled
	done := make(chan struct{})
	go func() { s.run(); close(done) }()

	close(canceled)
	select {
	case <-done:
	case <-time.After(streamTestTimeout):
		t.Fatal("the stream did not end when the request was canceled")
	}
	if s.stats.result != streamResultCanceled {
		t.Errorf("result = %q, want %q", s.stats.result, streamResultCanceled)
	}
}

// brokenConn blocks reads until it is closed and fails all writes.
type brokenConn struct {
	closed chan struct{}
	once   sync.Once
}

func (c *brokenConn) Read([]byte) (int, error)  { <-c.closed; return 0, io.ErrClosedPipe }
func (c *brokenConn) Write([]byte) (int, error) { return 0, errors.New("broken") }
func (c *brokenConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func TestUpgradedStreamBackendError(t *testing.T) {
	s, client, _ := newTestStream(t, "")
	s.back = &brokenConn{closed: make(chan struct{})}
	done := make(chan struct{})
	go func() { s.run(); close(done) }()

	// what the client sends can't be written to the backend
	client.Write([]byte("data"))
	select {
	case <-done:
	case <-time.After(streamTestTimeout):
		t.Fatal("the stream did not end")
	}
	if s.stats.result != streamResultError {
		t.Errorf("result = %q, want %q", s.stats.result, streamResultError)
	}
}

// Whichever way a stream ends, it is accounted for once.
func TestStreamAccountedOnce(t *testing.T) {
	s, client, backend := newTestStream(t, "")
	key := s.stats.upstream
	closedBefore := counterValue(streamMetrics.closed, key, streamResultCleanup)

	var untracked int
	s.track = func(conns ...openConnection) func() {
		return func() { untracked++ }
	}
	s.timeout = time.Hour // armed, and never fires
	done := make(chan struct{})
	go func() { s.run(); close(done) }()

	if got := gaugeValue(streamMetrics.active, key); got != 1 {
		t.Fatalf("active = %v, want 1", got)
	}

	// a cleanup, then both directions ending and the stream being
	// finished once more by whoever is last
	s.stats.setResult(streamResultCleanup)
	client.Close()
	backend.Close()
	<-done
	s.stats.finish()

	if got := counterValue(streamMetrics.closed, key, streamResultCleanup); got != closedBefore+1 {
		t.Errorf("closed by cleanup = %v, want %v", got, closedBefore+1)
	}
	if got := counterValue(streamMetrics.closed, key, streamResultError); got != 0 {
		t.Errorf("closed by error = %v, want 0", got)
	}
	if got := gaugeValue(streamMetrics.active, key); got != 0 {
		t.Errorf("active = %v, want 0", got)
	}
	if untracked != 1 {
		t.Errorf("untracked %d times, want once", untracked)
	}
}
