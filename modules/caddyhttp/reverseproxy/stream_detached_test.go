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
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

func TestStreamOptionsCaddyfile(t *testing.T) {
	block := func(body string) string {
		return "reverse_proxy localhost:8080 {\n" + body + "\n}"
	}

	for i, tc := range []struct {
		input    string
		wantErr  bool
		detached bool
		logs     *StreamLogs
	}{
		{input: "reverse_proxy localhost:8080"},
		{input: block("stream_detached"), detached: true},
		{input: block("stream_detached extra"), wantErr: true},
		{input: block("stream_logs {\n}"), logs: &StreamLogs{}},
		{input: block("stream_logs {\nskip_handshake\n}"), logs: &StreamLogs{SkipHandshake: true}},
		{input: block("stream_logs {\nlevel info\n}"), logs: &StreamLogs{Level: "info"}},
		{
			input:    block("stream_detached\nstream_logs {\nskip_handshake\nlevel WARN\n}"),
			detached: true,
			logs:     &StreamLogs{SkipHandshake: true, Level: "WARN"},
		},
		{
			// the block can be repeated, the options add up
			input: block("stream_logs {\nskip_handshake\n}\nstream_logs {\nlevel error\n}"),
			logs:  &StreamLogs{SkipHandshake: true, Level: "error"},
		},
		{input: block("stream_logs extra {\n}"), wantErr: true},
		{input: block("stream_logs {\nskip_handshake extra\n}"), wantErr: true},
		{input: block("stream_logs {\nlevel\n}"), wantErr: true},
		{input: block("stream_logs {\nlevel bogus\n}"), wantErr: true},
		{input: block("stream_logs {\nlevel info extra\n}"), wantErr: true},
		{input: block("stream_logs {\nbogus\n}"), wantErr: true},
	} {
		h := new(Handler)
		err := h.UnmarshalCaddyfile(caddyfile.NewTestDispenser(tc.input))
		if tc.wantErr {
			if err == nil {
				t.Errorf("Test %d: expected an error for %q but got none", i, tc.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("Test %d: unexpected error for %q: %v", i, tc.input, err)
			continue
		}
		if h.StreamDetached != tc.detached {
			t.Errorf("Test %d: StreamDetached = %v, want %v", i, h.StreamDetached, tc.detached)
		}
		if !reflect.DeepEqual(h.StreamLogs, tc.logs) {
			t.Errorf("Test %d: StreamLogs = %+v, want %+v", i, h.StreamLogs, tc.logs)
		}
	}
}

func TestStreamLogsOptions(t *testing.T) {
	for i, tc := range []struct {
		logs     *StreamLogs
		wantErr  bool
		wantSkip bool
	}{
		{logs: nil},
		{logs: &StreamLogs{}},
		{logs: &StreamLogs{SkipHandshake: true}, wantSkip: true},
		{logs: &StreamLogs{Level: "info"}},
		{logs: &StreamLogs{Level: "bogus"}, wantErr: true},
	} {
		level, err := tc.logs.parseLevel()
		if tc.wantErr {
			if err == nil {
				t.Errorf("Test %d: expected an error but got none", i)
			}
		} else if err != nil {
			t.Errorf("Test %d: unexpected error: %v", i, err)
		}
		if tc.logs != nil && tc.logs.Level == "info" && level.String() != "info" {
			t.Errorf("Test %d: level = %s, want info", i, level)
		}
		if (tc.logs == nil || tc.logs.Level == "") && level.String() != "debug" {
			t.Errorf("Test %d: default level = %s, want debug", i, level)
		}
		if got := tc.logs.skipHandshake(); got != tc.wantSkip {
			t.Errorf("Test %d: skipHandshake = %v, want %v", i, got, tc.wantSkip)
		}
	}
}

func TestStreamRegistry(t *testing.T) {
	r := &streamRegistry{streams: make(map[string]map[*upgradedStream]struct{})}

	newStream := func(upstream string, closed *int) *upgradedStream {
		st := newUpgradedStream(upstream)
		st.closeConns = func() { *closed++ }
		return st
	}
	var closedA1, closedA2, closedB int
	a1, a2, b := newStream("a.test:1", &closedA1), newStream("a.test:1", &closedA2), newStream("b.test:1", &closedB)

	removeA1, removeA2, removeB := r.add(a1), r.add(a2), r.add(b)
	if len(r.streams) != 2 || len(r.streams["a.test:1"]) != 2 {
		t.Fatalf("registry = %v, want two streams for a and one for b", r.streams)
	}

	// a stream that ended on its own is not closed anymore
	removeA1()
	if len(r.streams["a.test:1"]) != 1 {
		t.Fatalf("registry after removal = %v, want one stream for a", r.streams)
	}

	if got := r.closeUpstream("unknown.test:1", streamResultUpstreamRemoved); got != 0 {
		t.Errorf("closing an unknown upstream closed %d streams, want 0", got)
	}
	if got := r.closeUpstream("a.test:1", streamResultUpstreamRemoved); got != 1 {
		t.Errorf("closing a closed %d streams, want 1", got)
	}
	if closedA1 != 0 || closedA2 != 1 || closedB != 0 {
		t.Errorf("closed (a1, a2, b) = (%d, %d, %d), want (0, 1, 0)", closedA1, closedA2, closedB)
	}

	// the result is that of the outside close
	if summary, _ := a2.finish(streamResultError); summary.result != streamResultUpstreamRemoved {
		t.Errorf("result = %q, want %q", summary.result, streamResultUpstreamRemoved)
	}

	removeA2()
	removeB()
	if len(r.streams) != 0 {
		t.Errorf("registry = %v, want it empty once all streams are gone", r.streams)
	}
	a1.finish(streamResultClosed)
	b.finish(streamResultClosed)
}

// Cleanup closes the detached streams of an upstream only once no config
// uses the upstream anymore.
func TestHandlerCleanupClosesDetachedStreamsOfRemovedUpstream(t *testing.T) {
	const dial = "cleanup-removed.test:1"

	newHandler := func() *Handler {
		u := &Upstream{Dial: dial}
		h := minimalHandler(0, u)
		h.connections = make(map[io.ReadWriteCloser]openConnection)
		h.connectionsMu = new(sync.Mutex)
		h.provisionUpstream(u, false)
		return h
	}
	oldConfig, newConfig := newHandler(), newHandler()

	closed := make(chan struct{}, 1)
	st := newUpgradedStream(dial)
	st.closeConns = func() { closed <- struct{}{} }
	defer detachedStreams.add(st)()

	// reload keeping the upstream: the new config still uses it
	if err := oldConfig.Cleanup(); err != nil {
		t.Fatalf("cleanup of the old config: %v", err)
	}
	select {
	case <-closed:
		t.Fatal("a stream was closed although its upstream is still in the config")
	default:
	}

	// the upstream is gone for good
	if err := newConfig.Cleanup(); err != nil {
		t.Fatalf("cleanup of the new config: %v", err)
	}
	select {
	case <-closed:
	default:
		t.Fatal("the stream of a removed upstream was not closed")
	}
	if summary, _ := st.finish(streamResultError); summary.result != streamResultUpstreamRemoved {
		t.Errorf("result = %q, want %q", summary.result, streamResultUpstreamRemoved)
	}
}

func TestStreamUpstream(t *testing.T) {
	ctx := context.Background()
	if got := streamUpstream(ctx); got != "" {
		t.Errorf("upstream without dial info = %q, want none", got)
	}

	withoutUpstream := context.WithValue(ctx, dialInfoCtxKey, DialInfo{Address: "somewhere:1"})
	if got := streamUpstream(withoutUpstream); got != "" {
		t.Errorf("upstream of dial info without upstream = %q, want none", got)
	}

	withUpstream := context.WithValue(ctx, dialInfoCtxKey, DialInfo{Upstream: &Upstream{Dial: "backend.test:1"}})
	if got := streamUpstream(withUpstream); got != "backend.test:1" {
		t.Errorf("upstream = %q, want backend.test:1", got)
	}
}

func TestSwitchProtocolCopierCountsBytes(t *testing.T) {
	var (
		wg                     sync.WaitGroup
		toBackend, fromBackend atomic.Int64
		toBackendData          bytes.Buffer
		fromBackendData        bytes.Buffer
	)
	errc := make(chan error, 4)

	counting := switchProtocolCopier{
		user:        nopReadWriteCloser{Reader: strings.NewReader("hello"), Writer: &fromBackendData},
		backend:     nopReadWriteCloser{Reader: strings.NewReader("world!!"), Writer: &toBackendData},
		wg:          &wg,
		toBackend:   &toBackend,
		fromBackend: &fromBackend,
	}
	wg.Add(2)
	go counting.copyToBackend(errc)
	go counting.copyFromBackend(errc)
	wg.Wait()

	if got := toBackend.Load(); got != int64(len("hello")) {
		t.Errorf("bytes to backend = %d, want %d", got, len("hello"))
	}
	if got := fromBackend.Load(); got != int64(len("world!!")) {
		t.Errorf("bytes from backend = %d, want %d", got, len("world!!"))
	}

	// the counters are optional
	plain := switchProtocolCopier{
		user:    nopReadWriteCloser{Reader: strings.NewReader("hello"), Writer: io.Discard},
		backend: nopReadWriteCloser{Reader: strings.NewReader("world!!"), Writer: io.Discard},
		wg:      &wg,
	}
	wg.Add(2)
	go plain.copyToBackend(errc)
	go plain.copyFromBackend(errc)
	wg.Wait()
}

func TestLogStreamClosedWithoutAccessLog(t *testing.T) {
	h := new(Handler)

	// no server in the request context
	h.logStreamClosed(httptest.NewRequest(http.MethodGet, "/", nil), streamSummary{})

	// a server that doesn't log requests
	h.logStreamClosed(prepareTestRequest(httptest.NewRequest(http.MethodGet, "/", nil)), streamSummary{})
}

// upgradeProxy is a handler proxying an upgrade to a backend, with real
// connections on both sides: cli is the client, srv the backend's side.
type upgradeProxy struct {
	t        *testing.T
	h        *Handler
	upstream string
	cli      *net.TCPConn
	br       *bufio.Reader // reads from cli
	srv      *net.TCPConn

	mu  sync.Mutex
	req *http.Request // the request as seen by the handler
}

const streamTestTimeout = 10 * time.Second

// newUpgradeProxy sets up a proxy, performs an upgrade to proto through it,
// and returns once the stream is up (data went through it).
func newUpgradeProxy(t *testing.T, proto string, configure func(*Handler)) *upgradeProxy {
	t.Helper()

	srvc := make(chan *net.TCPConn, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("backend hijack failed: %v", err)
			return
		}
		tcp := conn.(*net.TCPConn)
		if _, err := io.WriteString(tcp, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: "+proto+"\r\n\r\n"); err != nil {
			tcp.Close()
			t.Errorf("backend upgrade write failed: %v", err)
			return
		}
		srvc <- tcp
	}))
	t.Cleanup(backend.Close)

	p := &upgradeProxy{t: t, upstream: backend.Listener.Addr().String()}
	p.h = minimalHandler(0, &Upstream{Host: new(Host), Dial: p.upstream})
	p.h.connections = make(map[io.ReadWriteCloser]openConnection)
	p.h.connectionsMu = new(sync.Mutex)
	configure(p.h)

	frontend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// isolate the tests from request cancellation, which has its own backend-close path
		r = prepareTestRequest(r.WithContext(context.WithoutCancel(r.Context())))
		p.mu.Lock()
		p.req = r
		p.mu.Unlock()
		_ = p.h.ServeHTTP(w, r, caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error {
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
	p.cli, err = net.DialTCP("tcp", nil, addr)
	if err != nil {
		t.Fatalf("failed to dial frontend: %v", err)
	}
	t.Cleanup(func() { p.cli.Close() })
	_ = p.cli.SetDeadline(time.Now().Add(streamTestTimeout))

	req, _ := http.NewRequest(http.MethodGet, frontend.URL, nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", proto)
	if err := req.Write(p.cli); err != nil {
		t.Fatalf("failed to write upgrade request: %v", err)
	}
	p.br = bufio.NewReader(p.cli)
	resp, err := http.ReadResponse(p.br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("failed to read upgrade response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}

	select {
	case p.srv = <-srvc:
	case <-time.After(streamTestTimeout):
		t.Fatal("timed out waiting for the backend connection")
	}
	t.Cleanup(func() { p.srv.Close() })
	_ = p.srv.SetDeadline(time.Now().Add(streamTestTimeout))

	// the stream is registered before any data is copied, so once
	// data went through, the stream is fully set up
	p.roundTrip("hi")
	return p
}

// roundTrip sends msg to the backend, and a reply back to the client.
func (p *upgradeProxy) roundTrip(msg string) {
	p.t.Helper()
	if _, err := p.cli.Write([]byte(msg)); err != nil {
		p.t.Fatalf("client write failed: %v", err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(p.srv, got); err != nil || string(got) != msg {
		p.t.Fatalf("backend read %q (%v), want %q", got, err, msg)
	}
	reply := "re:" + msg
	if _, err := p.srv.Write([]byte(reply)); err != nil {
		p.t.Fatalf("backend write failed: %v", err)
	}
	got = make([]byte, len(reply))
	if _, err := io.ReadFull(p.br, got); err != nil || string(got) != reply {
		p.t.Fatalf("client read %q (%v), want %q", got, err, reply)
	}
}

// expectClosed waits for the client side of the stream to be closed,
// and returns what was still received before that.
func (p *upgradeProxy) expectClosed() []byte {
	p.t.Helper()
	var rest bytes.Buffer
	_, err := io.Copy(&rest, p.br)
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		p.t.Fatal("timed out waiting for the stream to be closed")
	}
	return rest.Bytes()
}

func (p *upgradeProxy) openConnections() int {
	p.h.connectionsMu.Lock()
	defer p.h.connectionsMu.Unlock()
	return len(p.h.connections)
}

func (p *upgradeProxy) registered() int {
	detachedStreams.mu.Lock()
	defer detachedStreams.mu.Unlock()
	return len(detachedStreams.streams[p.upstream])
}

func (p *upgradeProxy) request() *http.Request {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.req
}

// waitForClosed waits until a stream with the given result is accounted.
func (p *upgradeProxy) waitForClosed(result string) {
	p.t.Helper()
	deadline := time.Now().Add(streamTestTimeout)
	for closedStreams(p.t, p.upstream, result) != 1 {
		if time.Now().After(deadline) {
			p.t.Fatalf("timed out waiting for a stream closed with result %q", result)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A detached stream is not owned by its handler: it survives the handler's
// cleanup and is only closed when its upstream is removed.
func TestHandlerDetachedStream(t *testing.T) {
	p := newUpgradeProxy(t, "echo", func(h *Handler) { h.StreamDetached = true })

	if got := p.openConnections(); got != 0 {
		t.Errorf("a detached stream registered %d connections with its handler, want 0", got)
	}
	if got := p.registered(); got != 1 {
		t.Errorf("registered detached streams = %d, want 1", got)
	}
	if got := activeStreams(t, p.upstream); got != 1 {
		t.Errorf("active streams = %v, want 1", got)
	}

	// the handler's config is unloaded
	if err := p.h.cleanupConnections(); err != nil {
		t.Fatalf("cleanupConnections: %v", err)
	}
	p.roundTrip("still alive")

	// the upstream is removed from the config
	if got := detachedStreams.closeUpstream(p.upstream, streamResultUpstreamRemoved); got != 1 {
		t.Fatalf("closed %d streams, want 1", got)
	}
	if rest := p.expectClosed(); len(rest) != 0 {
		t.Errorf("received %q when the stream was closed, want nothing for a non-websocket", rest)
	}
	p.waitForClosed(streamResultUpstreamRemoved)

	if got := activeStreams(t, p.upstream); got != 0 {
		t.Errorf("active streams = %v, want 0", got)
	}
	if got := streamBytes(t, p.upstream, "to_backend"); got != float64(len("hi")+len("still alive")) {
		t.Errorf("bytes to backend = %v", got)
	}
	if got := streamBytes(t, p.upstream, "from_backend"); got != float64(len("re:hi")+len("re:still alive")) {
		t.Errorf("bytes from backend = %v", got)
	}
	if got := p.registered(); got != 0 {
		t.Errorf("registered detached streams after close = %d, want 0", got)
	}
}

// Closing the streams of an upstream leaves the streams of others alone.
func TestHandlerDetachedStreamsPerUpstream(t *testing.T) {
	configure := func(h *Handler) { h.StreamDetached = true }
	removed := newUpgradeProxy(t, "echo", configure)
	retained := newUpgradeProxy(t, "echo", configure)

	detachedStreams.closeUpstream(removed.upstream, streamResultUpstreamRemoved)
	removed.expectClosed()
	removed.waitForClosed(streamResultUpstreamRemoved)

	retained.roundTrip("unaffected")
	if got := closedStreams(t, retained.upstream, streamResultUpstreamRemoved); got != 0 {
		t.Errorf("the stream of the retained upstream was accounted as closed")
	}
}

// Detached WebSocket streams are closed gracefully, with a close frame.
func TestHandlerDetachedWebsocketGracefulClose(t *testing.T) {
	p := newUpgradeProxy(t, "websocket", func(h *Handler) { h.StreamDetached = true })

	detachedStreams.closeUpstream(p.upstream, streamResultUpstreamRemoved)

	// close frame, status 1001 (going away)
	if rest := p.expectClosed(); !bytes.Equal(rest, []byte{0x88, 0x02, 0x03, 0xe9}) {
		t.Errorf("received %x when the stream was closed, want a close frame", rest)
	}
	p.waitForClosed(streamResultUpstreamRemoved)
}

// Without stream_detached, the stream belongs to the handler and is closed
// right away when the handler is cleaned up.
func TestHandlerDefaultStreamClosedOnCleanup(t *testing.T) {
	p := newUpgradeProxy(t, "echo", func(*Handler) {})

	if got := p.openConnections(); got != 2 {
		t.Errorf("registered connections = %d, want 2", got)
	}
	if got := p.registered(); got != 0 {
		t.Errorf("registered detached streams = %d, want 0", got)
	}

	if err := p.h.cleanupConnections(); err != nil {
		t.Fatalf("cleanupConnections: %v", err)
	}
	p.expectClosed()
	p.waitForClosed(streamResultReload)
}

// With stream_close_delay, the stream is closed once the delay is over.
func TestHandlerDefaultStreamClosedAfterDelay(t *testing.T) {
	const delay = 300 * time.Millisecond
	p := newUpgradeProxy(t, "echo", func(h *Handler) { h.StreamCloseDelay = caddy.Duration(delay) })

	start := time.Now()
	if err := p.h.cleanupConnections(); err != nil {
		t.Fatalf("cleanupConnections: %v", err)
	}
	p.roundTrip("during the delay")

	p.expectClosed()
	if elapsed := time.Since(start); elapsed < delay {
		t.Errorf("the stream was closed after %s, want at least %s", elapsed, delay)
	}
	p.waitForClosed(streamResultReload)
}

func TestHandlerStreamTimeout(t *testing.T) {
	p := newUpgradeProxy(t, "echo", func(h *Handler) { h.StreamTimeout = caddy.Duration(200 * time.Millisecond) })

	p.expectClosed()
	p.waitForClosed(streamResultTimeout)
}

// A stream that both sides close cleanly is accounted as closed.
func TestHandlerStreamClosedCleanly(t *testing.T) {
	p := newUpgradeProxy(t, "echo", func(*Handler) {})

	if err := p.cli.CloseWrite(); err != nil {
		t.Fatalf("client CloseWrite: %v", err)
	}
	if rest, err := io.ReadAll(p.srv); err != nil || len(rest) != 0 {
		t.Fatalf("backend read %q (%v) after the client closed, want EOF", rest, err)
	}
	if err := p.srv.CloseWrite(); err != nil {
		t.Fatalf("backend CloseWrite: %v", err)
	}
	p.expectClosed()
	p.waitForClosed(streamResultClosed)

	if got := streamBytes(t, p.upstream, "to_backend"); got != float64(len("hi")) {
		t.Errorf("bytes to backend = %v, want %d", got, len("hi"))
	}
}

// A stream broken by a connection error is accounted as an error.
func TestHandlerStreamError(t *testing.T) {
	p := newUpgradeProxy(t, "echo", func(*Handler) {})

	// reset the backend connection instead of closing it
	if err := p.srv.SetLinger(0); err != nil {
		t.Fatalf("SetLinger: %v", err)
	}
	p.srv.Close()

	p.expectClosed()
	p.waitForClosed(streamResultError)
}

// The handshake is left out of the access log only if asked to.
func TestHandlerStreamLogsSkipHandshake(t *testing.T) {
	for _, tc := range []struct {
		name string
		logs *StreamLogs
		want bool
	}{
		{name: "default", logs: nil, want: false},
		{name: "logs without skip", logs: &StreamLogs{Level: "info"}, want: false},
		{name: "skip", logs: &StreamLogs{SkipHandshake: true}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newUpgradeProxy(t, "echo", func(h *Handler) { h.StreamLogs = tc.logs })
			skipped, _ := caddyhttp.GetVar(p.request().Context(), caddyhttp.LogSkipVar).(bool)
			if skipped != tc.want {
				t.Errorf("handshake skipped = %v, want %v", skipped, tc.want)
			}
		})
	}
}
