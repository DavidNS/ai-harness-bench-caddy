package reverseproxy

import (
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestStreamLogsProvision(t *testing.T) {
	for i, tc := range []struct {
		level   string
		want    zapcore.Level
		wantErr bool
	}{
		{level: "", want: zapcore.DebugLevel},
		{level: "debug", want: zapcore.DebugLevel},
		{level: "INFO", want: zapcore.InfoLevel},
		{level: "warn", want: zapcore.WarnLevel},
		{level: "error", want: zapcore.ErrorLevel},
		{level: "loud", wantErr: true},
	} {
		sl := &StreamLogs{Level: tc.level}
		err := sl.provision()
		if tc.wantErr {
			if err == nil {
				t.Errorf("Test %d (%q): expected an error", i, tc.level)
			}
			continue
		}
		if err != nil {
			t.Errorf("Test %d (%q): unexpected error: %v", i, tc.level, err)
			continue
		}
		if sl.logLevel() != tc.want {
			t.Errorf("Test %d (%q): level = %v, want %v", i, tc.level, sl.logLevel(), tc.want)
		}
	}

	// without any configuration, the handshake is logged and the level is debug
	var none *StreamLogs
	if none.skipHandshake() {
		t.Error("the handshake is skipped by default")
	}
	if none.logLevel() != zapcore.DebugLevel {
		t.Errorf("default level = %v, want debug", none.logLevel())
	}
}

func TestStreamLoggerNames(t *testing.T) {
	for i, tc := range []struct {
		name string
		want []string
	}{
		{name: "", want: []string{"http.handlers.reverse_proxy"}},
		{name: "stream", want: []string{"http.log.stream"}},
		// there is no server to tell where the access log goes
		{name: "access", want: []string{"http.log.access"}},
	} {
		got := streamLoggerNames(tc.name, nil, nil)
		if len(got) != len(tc.want) || got[0] != tc.want[0] {
			t.Errorf("Test %d (%q): names = %q, want %q", i, tc.name, got, tc.want)
		}
	}
}

func TestUnmarshalCaddyfileStreamOptions(t *testing.T) {
	for i, tc := range []struct {
		input        string
		wantDetached bool
		wantLogs     *StreamLogs
		wantErr      bool
	}{
		{input: `reverse_proxy localhost:8080`},
		{input: `reverse_proxy localhost:8080 {
			stream_detached
		}`, wantDetached: true},
		{input: `reverse_proxy localhost:8080 {
			stream_logs {
				skip_handshake
				level info
				logger_name access
			}
		}`, wantLogs: &StreamLogs{SkipHandshake: true, Level: "info", LoggerName: "access"}},
		{input: `reverse_proxy localhost:8080 {
			stream_logs {
			}
		}`, wantLogs: &StreamLogs{}},
		{input: `reverse_proxy localhost:8080 {
			stream_detached
			stream_logs {
				level warn
			}
		}`, wantDetached: true, wantLogs: &StreamLogs{Level: "warn"}},
		{input: `reverse_proxy localhost:8080 {
			stream_detached yes
		}`, wantErr: true},
		{input: `reverse_proxy localhost:8080 {
			stream_detached
			stream_detached
		}`, wantErr: true},
		{input: `reverse_proxy localhost:8080 {
			stream_logs {
				level
			}
		}`, wantErr: true},
		{input: `reverse_proxy localhost:8080 {
			stream_logs {
				skip_handshake now
			}
		}`, wantErr: true},
		{input: `reverse_proxy localhost:8080 {
			stream_logs {
				colour red
			}
		}`, wantErr: true},
		{input: `reverse_proxy localhost:8080 {
			stream_logs {
				level info
			}
			stream_logs {
				level warn
			}
		}`, wantErr: true},
	} {
		h := new(Handler)
		err := h.UnmarshalCaddyfile(caddyfile.NewTestDispenser(tc.input))
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
		if h.StreamDetached != tc.wantDetached {
			t.Errorf("Test %d: StreamDetached = %t, want %t", i, h.StreamDetached, tc.wantDetached)
		}
		switch {
		case tc.wantLogs == nil && h.StreamLogs != nil:
			t.Errorf("Test %d: unexpected stream logs %+v", i, h.StreamLogs)
		case tc.wantLogs != nil && h.StreamLogs == nil:
			t.Errorf("Test %d: expected stream logs %+v", i, tc.wantLogs)
		case tc.wantLogs != nil && *h.StreamLogs != *tc.wantLogs:
			t.Errorf("Test %d: stream logs = %+v, want %+v", i, *h.StreamLogs, *tc.wantLogs)
		}
	}
}

// tcpPair returns the two ends of a TCP connection.
func tcpPair(t *testing.T) (a, b *net.TCPConn) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan *net.TCPConn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			accepted <- nil
			return
		}
		accepted <- c.(*net.TCPConn)
	}()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	b = <-accepted
	if b == nil {
		t.Fatal("accepting")
	}
	t.Cleanup(func() { c.Close(); b.Close() })
	return c.(*net.TCPConn), b
}

// streamHarness is a stream between a client and a backend, with the
// proxy in the middle, running in its own goroutine.
type streamHarness struct {
	client, backend *net.TCPConn
	stream          *upgradedStream
	done            chan struct{}
}

func newStreamHarness(t *testing.T, opts streamOptions, buffered []byte, reqDone <-chan struct{}) *streamHarness {
	t.Helper()

	client, proxyUser := tcpPair(t)
	proxyBackend, backend := tcpPair(t)
	h := &streamHarness{
		client:  client,
		backend: backend,
		stream:  newUpgradedStream(opts, proxyUser, proxyBackend),
		done:    make(chan struct{}),
	}
	go func() {
		defer close(h.done)
		h.stream.run(zap.NewNop(), new(sync.WaitGroup), reqDone, buffered)
	}()
	return h
}

func (h *streamHarness) waitDone(t *testing.T) {
	t.Helper()
	select {
	case <-h.done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the stream to end")
	}
}

func readAll(t *testing.T, r io.Reader) string {
	t.Helper()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("reading: %v", err)
	}
	return string(data)
}

func closedCount(upstream, result string) float64 {
	return testutil.ToFloat64(streamMetrics.closed.WithLabelValues(upstream, result))
}

func bytesCount(upstream, direction string) float64 {
	return testutil.ToFloat64(streamMetrics.bytes.WithLabelValues(upstream, direction))
}

func activeCount(upstream string) float64 {
	return testutil.ToFloat64(streamMetrics.active.WithLabelValues(upstream))
}

func TestUpgradedStreamAccounting(t *testing.T) {
	t.Run("completed", func(t *testing.T) {
		const upstream = "test-completed"
		h := newStreamHarness(t, streamOptions{upstream: upstream}, []byte("buffered"), nil)
		if got := activeCount(upstream); got != 1 {
			t.Errorf("active streams = %v, want 1", got)
		}

		// both sides finish what they send and half-close; what the client
		// sent before the stream was set up is delivered first
		if _, err := h.client.Write([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		h.client.CloseWrite()
		if got := readAll(t, h.backend); got != "bufferedhello" {
			t.Errorf("backend received %q, want %q", got, "bufferedhello")
		}
		if _, err := h.backend.Write([]byte("goodbye!")); err != nil {
			t.Fatal(err)
		}
		h.backend.CloseWrite()
		if got := readAll(t, h.client); got != "goodbye!" {
			t.Errorf("client received %q, want %q", got, "goodbye!")
		}
		h.waitDone(t)

		if got := closedCount(upstream, streamResultCompleted); got != 1 {
			t.Errorf("completed streams = %v, want 1", got)
		}
		if got := activeCount(upstream); got != 0 {
			t.Errorf("active streams = %v, want 0", got)
		}
		if got := bytesCount(upstream, "to_backend"); got != float64(len("bufferedhello")) {
			t.Errorf("bytes to backend = %v, want %d", got, len("bufferedhello"))
		}
		if got := bytesCount(upstream, "from_backend"); got != 8 {
			t.Errorf("bytes from backend = %v, want 8", got)
		}
		if got := testutil.CollectAndCount(streamMetrics.duration); got == 0 {
			t.Error("the duration was not observed")
		}
	})

	t.Run("error", func(t *testing.T) {
		const upstream = "test-error"
		h := newStreamHarness(t, streamOptions{upstream: upstream}, nil, nil)

		// the connection to the client breaks
		h.stream.user.Close()
		h.waitDone(t)

		if got := closedCount(upstream, streamResultError); got != 1 {
			t.Errorf("failed streams = %v, want 1", got)
		}
		if got := activeCount(upstream); got != 0 {
			t.Errorf("active streams = %v, want 0", got)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		const upstream = "test-timeout"
		h := newStreamHarness(t, streamOptions{upstream: upstream, timeout: 50 * time.Millisecond}, nil, nil)
		h.waitDone(t)

		if got := closedCount(upstream, streamResultTimeout); got != 1 {
			t.Errorf("timed out streams = %v, want 1", got)
		}
		// the connections are closed with the stream
		_ = h.client.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := h.client.Read(make([]byte, 1)); err != io.EOF {
			t.Errorf("client read error = %v, want EOF", err)
		}
	})

	t.Run("canceled", func(t *testing.T) {
		const upstream = "test-canceled"
		reqDone := make(chan struct{})
		h := newStreamHarness(t, streamOptions{upstream: upstream}, nil, reqDone)
		close(reqDone)
		h.waitDone(t)

		if got := closedCount(upstream, streamResultCanceled); got != 1 {
			t.Errorf("canceled streams = %v, want 1", got)
		}
	})

	t.Run("closed by the proxy", func(t *testing.T) {
		const upstream = "test-closed"
		h := newStreamHarness(t, streamOptions{upstream: upstream}, nil, nil)
		if err := h.stream.closeByProxy(); err != nil {
			t.Errorf("closeByProxy: %v", err)
		}
		h.waitDone(t)

		if got := closedCount(upstream, streamResultClosed); got != 1 {
			t.Errorf("closed streams = %v, want 1", got)
		}
		if got := activeCount(upstream); got != 0 {
			t.Errorf("active streams = %v, want 0", got)
		}
	})

	t.Run("accounted once", func(t *testing.T) {
		const upstream = "test-once"
		client, proxyUser := tcpPair(t)
		_ = client
		proxyBackend, _ := tcpPair(t)
		s := newUpgradedStream(streamOptions{upstream: upstream}, proxyUser, proxyBackend)

		s.finish(streamResultError, nil)
		s.finish(streamResultCompleted, nil)
		s.finish(streamResultClosed, nil)

		if got := closedCount(upstream, streamResultError); got != 1 {
			t.Errorf("error streams = %v, want 1", got)
		}
		if got := closedCount(upstream, streamResultCompleted) + closedCount(upstream, streamResultClosed); got != 0 {
			t.Errorf("a stream was accounted for again: %v", got)
		}
		if got := activeCount(upstream); got != 0 {
			t.Errorf("active streams = %v, want 0", got)
		}
	})
}

func TestCloseDetachedStreams(t *testing.T) {
	newDetached := func(t *testing.T, key string) (*streamHarness, func()) {
		h := newStreamHarness(t, streamOptions{upstream: "test-detached-" + key, upstreamKey: key, detached: true}, nil, nil)
		remove := registerDetached(h.stream)
		t.Cleanup(remove)
		return h, remove
	}

	t.Run("closes the streams of the upstream", func(t *testing.T) {
		const key = "detached-closed.test:80"
		gone, _ := newDetached(t, key)
		other, _ := newDetached(t, "detached-other.test:80")

		closeDetachedStreams(key, 0, nil)

		gone.waitDone(t)
		if got := closedCount(gone.stream.upstream, streamResultClosed); got != 1 {
			t.Errorf("closed streams = %v, want 1", got)
		}
		select {
		case <-other.done:
			t.Error("the stream of another upstream was closed")
		case <-time.After(100 * time.Millisecond):
		}
		other.stream.closeByProxy()
		other.waitDone(t)
	})

	t.Run("keeps the streams of an upstream that is in use", func(t *testing.T) {
		const key = "detached-inuse.test:80"
		hosts.LoadOrStore(key, new(Host))
		defer hosts.Delete(key)

		h, _ := newDetached(t, key)
		closeDetachedStreams(key, 0, nil)

		select {
		case <-h.done:
			t.Error("the stream of an upstream that is still in use was closed")
		case <-time.After(100 * time.Millisecond):
		}
		h.stream.closeByProxy()
		h.waitDone(t)
	})

	t.Run("waits for the delay, and keeps the streams if the upstream is back by then", func(t *testing.T) {
		const key = "detached-delayed.test:80"
		h, _ := newDetached(t, key)
		closeDetachedStreams(key, 200*time.Millisecond, nil)

		select {
		case <-h.done:
			t.Fatal("the stream was closed before the delay")
		case <-time.After(50 * time.Millisecond):
		}
		hosts.LoadOrStore(key, new(Host))
		defer hosts.Delete(key)

		time.Sleep(300 * time.Millisecond)
		select {
		case <-h.done:
			t.Error("the stream was closed although the upstream is back")
		default:
		}
		h.stream.closeByProxy()
		h.waitDone(t)
	})

	t.Run("closes the streams after the delay", func(t *testing.T) {
		const key = "detached-late.test:80"
		h, _ := newDetached(t, key)
		start := time.Now()
		closeDetachedStreams(key, 100*time.Millisecond, nil)
		h.waitDone(t)
		if elapsed := time.Since(start); elapsed < 90*time.Millisecond {
			t.Errorf("closed after %s, want after the delay", elapsed)
		}
	})

	t.Run("streams without an upstream key are not closed", func(t *testing.T) {
		h, _ := newDetached(t, "")
		closeDetachedStreams("", 0, nil)
		select {
		case <-h.done:
			t.Error("a stream that is not tied to an upstream was closed")
		case <-time.After(100 * time.Millisecond):
		}
		h.stream.closeByProxy()
		h.waitDone(t)
	})
}

// Cleanup of a handler closes the detached streams only through upstreams
// that no config uses anymore, as happens when another config replaced it.
func TestHandlerCleanupDetachedStreams(t *testing.T) {
	newHandler := func(key string, detached bool) *Handler {
		h := minimalHandler(0, &Upstream{Dial: key})
		h.StreamDetached = detached
		h.connections = make(map[io.ReadWriteCloser]openConnection)
		h.connectionsMu = new(sync.Mutex)
		return h
	}

	t.Run("upstream removed", func(t *testing.T) {
		const key = "cleanup-removed.test:80"
		old := newHandler(key, true)
		hosts.LoadOrStore(key, new(Host)) // the old config

		h := newStreamHarness(t, streamOptions{upstream: key, upstreamKey: key, detached: true}, nil, nil)
		t.Cleanup(registerDetached(h.stream))

		if err := old.Cleanup(); err != nil {
			t.Fatal(err)
		}
		h.waitDone(t)
		if !h.stream.closedByProxy.Load() {
			t.Error("the stream was not closed by the proxy")
		}
	})

	t.Run("upstream kept by the new config", func(t *testing.T) {
		const key = "cleanup-kept.test:80"
		old := newHandler(key, true)
		hosts.LoadOrStore(key, new(Host)) // the old config
		hosts.LoadOrStore(key, new(Host)) // the new config
		defer hosts.Delete(key)

		h := newStreamHarness(t, streamOptions{upstream: key, upstreamKey: key, detached: true}, nil, nil)
		t.Cleanup(registerDetached(h.stream))

		if err := old.Cleanup(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-h.done:
			t.Error("the stream was closed although its upstream is in the new config")
		case <-time.After(100 * time.Millisecond):
		}
		h.stream.closeByProxy()
		h.waitDone(t)
	})

	t.Run("attached streams are closed by the handler", func(t *testing.T) {
		const key = "cleanup-attached.test:80"
		old := newHandler(key, false)
		hosts.LoadOrStore(key, new(Host))

		h := newStreamHarness(t, streamOptions{upstream: key}, nil, nil)
		old.registerStreamConnection(h.stream, h.stream.user, nil)
		old.registerStreamConnection(h.stream, h.stream.backend, nil)

		if err := old.Cleanup(); err != nil {
			t.Fatal(err)
		}
		h.waitDone(t)
		if got := closedCount(key, streamResultClosed); got != 1 {
			t.Errorf("closed streams = %v, want 1", got)
		}
	})
}

// unhashableWriter is a response writer that can't be used as a map key, as
// those of handlers that have slices in them.
type unhashableWriter struct {
	http.ResponseWriter
	parts []string
}

// The connection of an extended CONNECT stream is a map key of the handler's
// open connections, whatever response writer it writes to.
func TestRegisterConnectionWithUnhashableResponseWriter(t *testing.T) {
	h := minimalHandler(0)
	h.connections = make(map[io.ReadWriteCloser]openConnection)
	h.connectionsMu = new(sync.Mutex)

	conn := &h2ReadWriteCloser{ReadCloser: io.NopCloser(nil), ResponseWriter: unhashableWriter{parts: []string{"x"}}}
	del := h.registerConnection(conn, nil)
	if len(h.connections) != 1 {
		t.Fatalf("connections = %d, want 1", len(h.connections))
	}
	del()
	if len(h.connections) != 0 {
		t.Fatalf("connections = %d after delete, want 0", len(h.connections))
	}
}
