package reverseproxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"go.uber.org/zap"
)

// closeSpy is a connection that records being closed.
type closeSpy struct {
	io.ReadWriteCloser
	closed      int
	closeErr    error
	gracefulHit int
}

func (c *closeSpy) Close() error {
	c.closed++
	return c.closeErr
}

func (c *closeSpy) open(gracefulErr error, graceful bool) openConnection {
	oc := openConnection{conn: c}
	if graceful {
		oc.gracefulClose = func() error {
			c.gracefulHit++
			return gracefulErr
		}
	}
	return oc
}

func TestStreamRegistryOrphans(t *testing.T) {
	r := newStreamRegistry()
	r.addLive([]string{"a", "b"})
	r.addLive([]string{"b"})

	sa := &detachedStream{upstream: "a"}
	sb := &detachedStream{upstream: "b"}
	removeA := r.add(sa)
	removeB := r.add(sb)

	// "a" is only used by the first handler, "b" is also used by another
	orphaned := r.removeLive([]string{"a", "b"})
	if len(orphaned) != 1 || orphaned[0] != sa {
		t.Fatalf("orphaned = %v, want only the stream of a", orphaned)
	}
	if _, ok := r.live["a"]; ok {
		t.Error("a is still live")
	}
	if r.live["b"] != 1 {
		t.Errorf("live count of b = %d, want 1", r.live["b"])
	}
	if len(r.open["b"]) != 1 {
		t.Errorf("streams of b = %d, want 1", len(r.open["b"]))
	}

	// removing a stream that was already forgotten must be harmless
	removeA()

	removeB()
	if len(r.open["b"]) != 0 {
		t.Errorf("streams of b = %d after the stream ended, want 0", len(r.open["b"]))
	}
	if orphaned := r.removeLive([]string{"b"}); len(orphaned) != 0 {
		t.Errorf("orphaned = %v, want none since the stream ended", orphaned)
	}
	if len(r.live) != 0 || len(r.open) != 0 {
		t.Errorf("registry not empty: live=%v open=%v", r.live, r.open)
	}
}

func TestDetachedStreamClose(t *testing.T) {
	front, back := &closeSpy{}, &closeSpy{closeErr: errors.New("close failed")}
	gracefulErr := errors.New("graceful failed")
	s := &detachedStream{conns: []openConnection{
		front.open(gracefulErr, true),
		back.open(nil, false),
	}}

	// the first error is reported, but everything is closed
	if err := s.close(); !errors.Is(err, gracefulErr) {
		t.Errorf("close() error = %v, want %v", err, gracefulErr)
	}
	if front.closed != 1 || back.closed != 1 || front.gracefulHit != 1 {
		t.Errorf("closed=%d,%d graceful=%d; want both closed once, graceful once",
			front.closed, back.closed, front.gracefulHit)
	}

	// a close error is reported if the graceful close succeeded
	conn := &closeSpy{closeErr: errors.New("close failed")}
	if err := closeOpenConnection(conn.open(nil, true)); err == nil {
		t.Error("expected the close error")
	}

	if err := closeDetachedStreams([]*detachedStream{s, {}}); err == nil {
		t.Error("closeDetachedStreams() = nil, want the first error")
	}
	if err := closeDetachedStreams(nil); err != nil {
		t.Errorf("closeDetachedStreams(nil) = %v", err)
	}
}

func TestHandlerLiveUpstreamsAreDistinct(t *testing.T) {
	h := &Handler{Upstreams: UpstreamPool{{Dial: "a:1"}, {Dial: "b:1"}, {Dial: "a:1"}}}
	if got := h.liveUpstreams(); !slices.Equal(got, []string{"a:1", "b:1"}) {
		t.Errorf("liveUpstreams() = %v", got)
	}
	if got := (&Handler{}).liveUpstreams(); got != nil {
		t.Errorf("liveUpstreams() without upstreams = %v, want nil", got)
	}
}

// Cleaning up a handler that never registered, such as one whose provisioning
// failed early, must not disturb the streams of others.
func TestCleanupOfUnregisteredHandlerKeepsLiveCounts(t *testing.T) {
	const up = "unregistered.test:80"
	live := detachedTestHandler(up)
	unregistered := newStreamTestHandler(zap.NewNop())
	unregistered.Upstreams = UpstreamPool{{Dial: up}}

	if err := unregistered.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	detachedStreams.mu.Lock()
	count := detachedStreams.live[up]
	detachedStreams.mu.Unlock()
	if count != 1 {
		t.Errorf("live count = %d, want 1", count)
	}
	if err := live.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

// A default-mode handler going away also closes the detached streams of
// other handlers when it was the last one using their upstream.
func TestCleanupClosesDetachedStreamsOfRemovedUpstream(t *testing.T) {
	const up = "removed-by-default.test:80"
	detached := detachedTestHandler(up)
	u := startDetachedStream(t, detached, up)

	plain := newStreamTestHandler(zap.NewNop())
	plain.Upstreams = UpstreamPool{{Dial: up}}
	plain.registeredUpstreams = plain.liveUpstreams()
	detachedStreams.addLive(plain.registeredUpstreams)

	// the detached handler goes away, the plain one still uses the upstream
	if err := detached.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	u.assertOpen(t)

	if err := plain.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	u.waitClosed(t)
}

func TestCleanupReportsErrorClosingDetachedStreams(t *testing.T) {
	const up = "close-error.test:80"
	h := detachedTestHandler(up)
	conn := &closeSpy{closeErr: errors.New("close failed")}
	detachedStreams.add(&detachedStream{upstream: up, conns: []openConnection{conn.open(nil, false)}})

	if err := h.Cleanup(); err == nil {
		t.Error("Cleanup() = nil, want the error of closing the stream")
	}
	if conn.closed != 1 {
		t.Errorf("connection closed %d times, want 1", conn.closed)
	}
}

func TestStreamUpstream(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if got := streamUpstream(req); got != "" {
		t.Errorf("without dial info = %q, want empty", got)
	}

	withUpstream := req.WithContext(context.WithValue(req.Context(), dialInfoCtxKey,
		DialInfo{Upstream: &Upstream{Dial: "{http.request.host}:80"}, Network: "tcp", Host: "example.com", Port: "80"}))
	if got := streamUpstream(withUpstream); got != "{http.request.host}:80" {
		t.Errorf("with upstream = %q, want the configured dial", got)
	}

	withoutUpstream := req.WithContext(context.WithValue(req.Context(), dialInfoCtxKey,
		DialInfo{Network: "tcp", Host: "example.com", Port: "80"}))
	if got := streamUpstream(withoutUpstream); got != "tcp/example.com:80" {
		t.Errorf("without upstream = %q, want the dialed address", got)
	}
}

func TestStreamLoggersFallBackToHandlerLogger(t *testing.T) {
	logger := zap.NewNop()
	h := newStreamTestHandler(logger)

	// no server in the context
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if got := h.streamLoggers(req); len(got) != 1 || got[0] != logger {
		t.Errorf("without server = %v, want the handler logger", got)
	}

	// a server that does not log access
	req = prepareTestRequest(req)
	if got := h.streamLoggers(req); len(got) != 1 || got[0] != logger {
		t.Errorf("without access logging = %v, want the handler logger", got)
	}
}

func TestCleanupConnMarksStream(t *testing.T) {
	stream := &upgradedStream{}
	conn := &closeSpy{}
	c := &cleanupConn{ReadWriteCloser: conn, stream: stream}
	if stream.closedByCleanup.Load() {
		t.Fatal("stream marked before being closed")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !stream.closedByCleanup.Load() || conn.closed != 1 {
		t.Errorf("marked=%v closed=%d; want marked and closed once", stream.closedByCleanup.Load(), conn.closed)
	}
}
