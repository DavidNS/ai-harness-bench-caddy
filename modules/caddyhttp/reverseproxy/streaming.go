// Copyright 2015 Matthew Holt and The Caddy Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Most of the code in this file was initially borrowed from the Go
// standard library and modified; It had this copyright notice:
// Copyright 2011 The Go Authors

package reverseproxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	weakrand "math/rand/v2"
	"mime"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/net/http/httpguts"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

type h2ReadWriteCloser struct {
	io.ReadCloser
	http.ResponseWriter
}

func (rwc h2ReadWriteCloser) Write(p []byte) (n int, err error) {
	n, err = rwc.ResponseWriter.Write(p)
	if err != nil {
		return 0, err
	}

	//nolint:bodyclose
	err = http.NewResponseController(rwc.ResponseWriter).Flush()
	if err != nil {
		return 0, err
	}
	return n, nil
}

// handleUpgradeResponse switches protocols and proxies the upgraded stream.
// Unless the stream is detached from the request, it returns once the
// stream has ended.
func (h *Handler) handleUpgradeResponse(logger *zap.Logger, rw http.ResponseWriter, req *http.Request, res *http.Response) {
	reqUpType := upgradeType(req.Header)
	resUpType := upgradeType(res.Header)

	// Taken from https://github.com/golang/go/commit/5c489514bc5e61ad9b5b07bd7d8ec65d66a0512a
	// We know reqUpType is ASCII, it's checked by the caller.
	if !asciiIsPrint(resUpType) {
		if c := logger.Check(zapcore.DebugLevel, "backend tried to switch to invalid protocol"); c != nil {
			c.Write(zap.String("backend_upgrade", resUpType))
		}
		return
	}
	if !asciiEqualFold(reqUpType, resUpType) {
		if c := logger.Check(zapcore.DebugLevel, "backend tried to switch to unexpected protocol via Upgrade header"); c != nil {
			c.Write(
				zap.String("backend_upgrade", resUpType),
				zap.String("requested_upgrade", reqUpType),
			)
		}
		return
	}

	backConn, ok := res.Body.(io.ReadWriteCloser)
	if !ok {
		logger.Error("internal error: 101 switching protocols response with non-writable body")
		return
	}

	// write header first, response headers should not be counted in size
	// like the rest of handler chain.
	copyHeader(rw.Header(), res.Header)
	normalizeWebsocketHeaders(rw.Header())

	var (
		conn io.ReadWriteCloser
		brw  *bufio.ReadWriter
		isH2 bool
	)
	// websocket over http2 or http3 if extended connect is enabled, assuming backend doesn't support this, the request will be modified to http1.1 upgrade
	// TODO: once we can reliably detect backend support this, it can be removed for those backends
	if body, ok := caddyhttp.GetVar(req.Context(), "extended_connect_websocket_body").(io.ReadCloser); ok {
		req.Body = body
		rw.Header().Del("Upgrade")
		rw.Header().Del("Connection")
		delete(rw.Header(), "Sec-WebSocket-Accept")
		rw.WriteHeader(http.StatusOK)

		if c := logger.Check(zap.DebugLevel, "upgrading connection"); c != nil {
			c.Write(zap.Int("http_version", 2))
		}

		//nolint:bodyclose
		flushErr := http.NewResponseController(rw).Flush()
		if flushErr != nil {
			if c := h.logger.Check(zap.ErrorLevel, "failed to flush http2 websocket response"); c != nil {
				c.Write(zap.Error(flushErr))
			}
			return
		}
		conn = h2ReadWriteCloser{req.Body, rw}
		isH2 = true
		// bufio is not needed, use minimal buffer
		brw = bufio.NewReadWriter(bufio.NewReaderSize(conn, 1), bufio.NewWriterSize(conn, 1))
	} else {
		rw.WriteHeader(res.StatusCode)

		if c := logger.Check(zap.DebugLevel, "upgrading connection"); c != nil {
			c.Write(zap.Int("http_version", req.ProtoMajor))
		}

		var hijackErr error
		//nolint:bodyclose
		conn, brw, hijackErr = http.NewResponseController(rw).Hijack()
		if errors.Is(hijackErr, http.ErrNotSupported) {
			if c := h.logger.Check(zap.ErrorLevel, "can't switch protocols using non-Hijacker ResponseWriter"); c != nil {
				c.Write(zap.String("type", fmt.Sprintf("%T", rw)))
			}
			return
		}

		if hijackErr != nil {
			if c := h.logger.Check(zap.ErrorLevel, "hijack failed on protocol switch"); c != nil {
				c.Write(zap.Error(hijackErr))
			}
			return
		}
	}

	// the hijacked connection is wrapped by response recorders which track its
	// size for the access log without synchronization; the stream may outlive
	// the request, so copy bytes on the raw connection
	if !isH2 {
		conn = rawConn(conn)
	}

	if h.StreamLogs != nil && h.StreamLogs.SkipHandshake {
		caddyhttp.SetVar(req.Context(), caddyhttp.LogSkipVar, true)
	}

	var upstream string
	if di, ok := GetDialInfo(req.Context()); ok {
		upstream = di.Upstream.String()
	}

	// a detached stream outlives the config and the request: its HTTP/1.1
	// handler returns right after the hijack, so the stream must not hold on
	// to the handler or the request. HTTP/2 extended CONNECT streams are part
	// of their request and keep the handler until they end.
	async := h.StreamDetached && !isH2

	s := &upgradedStream{
		user:       conn,
		backend:    backConn,
		bufferSize: h.StreamBufferSize,
		timeout:    time.Duration(h.StreamTimeout),
		upstream:   upstream,
		logger:     logger,
		account:    h.streamMetrics.begin(upstream),
		level:      h.streamLogLevel,
		brw:        brw,
	}
	s.closeLoggers = []*zap.Logger{h.logger}
	if h.StreamLogs != nil && h.StreamLogs.LoggerName == streamLogsAccessLogger {
		if loggers := caddyhttp.AccessLoggers(req); len(loggers) > 0 {
			s.closeLoggers = loggers
		}
	}
	if async {
		s.logger = h.logger.With(zap.String("upstream", upstream))
	}

	// Ensure the hijacked client connection, and the new connection established
	// with the backend, are both closed in the event of a server shutdown. This
	// is done by registering them. We also try to gracefully close connections
	// we recognize as websockets.
	// We need to make sure the client connection messages (i.e. to upstream)
	// are masked, so we need to know whether the connection is considered the
	// server or the client side of the proxy.
	isWS := isWebsocket(req)
	gracefulClose := func(conn io.ReadWriteCloser, isClient bool) func() error {
		if isWS {
			return func() error {
				return writeCloseControl(conn, isClient)
			}
		}
		return nil
	}
	if h.StreamDetached {
		// detached streams are closed when their upstream is removed from the
		// config, not when the handler is cleaned up
		s.gracefulUser, s.gracefulBackend = gracefulClose(conn, false), gracefulClose(backConn, true)
		s.onDone = detachedStreams.add(upstream, s)
	} else {
		deleteFrontConn := h.registerConnection(conn, gracefulClose(conn, false), s.markReloaded)
		deleteBackConn := h.registerConnection(backConn, gracefulClose(backConn, true), s.markReloaded)
		s.onDone = func() {
			deleteFrontConn()
			deleteBackConn()
		}
	}

	if async {
		go s.run(nil)
		return
	}
	s.run(req.Context().Done())
}

// StreamLogs configures the logs of upgraded streams.
type StreamLogs struct {
	// If true, the upgrade handshake is not access logged. Unless
	// the stream is detached, the access log entry of a request is
	// written when the request is done, which for an upgrade is when
	// the stream ends. Default: the handshake is logged.
	SkipHandshake bool `json:"skip_handshake,omitempty"`

	// The logger the close of a stream is logged to. Empty (the default)
	// logs to the logger of the proxy. The only other supported value
	// is "access", which logs to the access logs of the request, just
	// like the access log entry of the handshake, if access logging is
	// enabled for it.
	LoggerName string `json:"logger_name,omitempty"`

	// The level the close of a stream is logged at. Default: DEBUG.
	Level string `json:"level,omitempty"`
}

// streamLogsAccessLogger is the logger name which sends the stream logs
// to the access logs.
const streamLogsAccessLogger = "access"

// validate checks s and stores the level to log at in level.
func (s *StreamLogs) validate(level *zapcore.Level) error {
	if s.LoggerName != "" && s.LoggerName != streamLogsAccessLogger {
		return fmt.Errorf("unsupported logger_name %q: must be %q or empty", s.LoggerName, streamLogsAccessLogger)
	}
	if s.Level != "" {
		if err := level.UnmarshalText([]byte(s.Level)); err != nil {
			return fmt.Errorf("invalid level %q: %v", s.Level, err)
		}
	}
	return nil
}

// UnmarshalCaddyfile sets up the stream logs from Caddyfile tokens. Syntax:
//
//	stream_logs {
//	    skip_handshake
//	    logger_name access
//	    level       <level>
//	}
func (s *StreamLogs) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch d.Val() {
		case "skip_handshake":
			if d.NextArg() {
				return d.ArgErr()
			}
			s.SkipHandshake = true

		case "logger_name":
			if !d.NextArg() {
				return d.ArgErr()
			}
			s.LoggerName = d.Val()
			if d.NextArg() {
				return d.ArgErr()
			}

		case "level":
			if !d.NextArg() {
				return d.ArgErr()
			}
			s.Level = d.Val()
			if d.NextArg() {
				return d.ArgErr()
			}

		default:
			return d.Errf("unrecognized subdirective: %s", d.Val())
		}
	}
	return nil
}

// rawConn strips the connection wrappers of response recorders off c.
func rawConn(c io.ReadWriteCloser) io.ReadWriteCloser {
	for {
		u, ok := c.(interface{ RawConn() net.Conn })
		if !ok {
			return c
		}
		c = u.RawConn()
	}
}

// upgradedStream is an upgraded connection (WebSocket, extended CONNECT,
// any other Upgrade) being proxied between a client and a backend.
type upgradedStream struct {
	user, backend io.ReadWriteCloser
	brw           *bufio.ReadWriter
	bufferSize    int
	timeout       time.Duration
	upstream      string

	logger       *zap.Logger
	closeLoggers []*zap.Logger
	level        zapcore.Level

	account *streamAccount
	wg      sync.WaitGroup

	// bytes copied in each direction
	toBackend, fromBackend atomic.Int64

	// set when the stream was closed because of a config reload
	reloaded atomic.Bool

	// how to close the connections gracefully, for detached streams
	gracefulUser, gracefulBackend func() error

	// called when the stream is done, to unregister it
	onDone func()
}

// markReloaded notes that the stream is being closed because of a reload.
func (s *upgradedStream) markReloaded() {
	s.reloaded.Store(true)
}

// closeForReload closes the connections of a stream which is detached
// from its config because its upstream was removed from the config.
func (s *upgradedStream) closeForReload() error {
	s.markReloaded()
	var err error
	for _, oc := range []openConnection{
		{conn: s.user, gracefulClose: s.gracefulUser},
		{conn: s.backend, gracefulClose: s.gracefulBackend},
	} {
		if closeErr := oc.close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

// run proxies the stream until it ends and accounts it. If reqDone is not
// nil, the backend is closed when it is closed.
func (s *upgradedStream) run(reqDone <-chan struct{}) {
	result := streamResultClosed
	defer func() {
		s.user.Close()
		s.backend.Close()
		s.wg.Wait()
		s.onDone()
		if s.reloaded.Load() {
			result = streamResultReload
		}
		s.finish(result)
	}()

	if reqDone != nil {
		// adopted from https://github.com/golang/go/commit/8bcf2834afdf6a1f7937390903a41518715ef6f5
		// Ensure that the cancellation of a request closes the backend.
		// See issue https://golang.org/issue/35559.
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			select {
			case <-reqDone:
				s.backend.Close()
			case <-stop:
			}
		}()
	}

	if err := s.brw.Flush(); err != nil {
		if c := s.logger.Check(zapcore.DebugLevel, "response flush"); c != nil {
			c.Write(zap.Error(err))
		}
		result = streamResultError
		return
	}

	// There may be buffered data in the *bufio.Reader
	// see: https://github.com/caddyserver/caddy/issues/6273
	if buffered := s.brw.Reader.Buffered(); buffered > 0 {
		data, _ := s.brw.Peek(buffered)
		n, err := s.backend.Write(data)
		s.toBackend.Add(int64(n))
		if err != nil {
			if c := s.logger.Check(zapcore.DebugLevel, "backConn write failed"); c != nil {
				c.Write(zap.Error(err))
			}
			result = streamResultError
			return
		}
	}

	spc := switchProtocolCopier{
		user:       s.user,
		backend:    s.backend,
		wg:         &s.wg,
		bufferSize: s.bufferSize,
		sent:       &s.toBackend,
		received:   &s.fromBackend,
	}

	// setup the timeout if requested
	var timeoutc <-chan time.Time
	if s.timeout > 0 {
		timer := time.NewTimer(s.timeout)
		defer timer.Stop()
		timeoutc = timer.C
	}

	// when a stream timeout is encountered, no error will be read from errc
	// a buffer size of 2 will allow both the read and write goroutines to send the error and exit
	// see: https://github.com/caddyserver/caddy/issues/7418
	errc := make(chan error, 2)
	s.wg.Add(2)
	go spc.copyToBackend(errc)
	go spc.copyFromBackend(errc)

	// Wait for both directions to finish. A clean EOF in one direction is a
	// half-close of that direction only, not the end of the tunnel: the copier
	// propagates it to the destination with CloseWrite and reports nil, so the
	// other direction is left open and pending bytes can still drain. Anything
	// else ends the tunnel: a copy error, a failed CloseWrite, or errCopyDone
	// when the destination has no write half to close.
	//
	// net/http/httputil expresses the same wait as:
	//
	//	err := <-errc
	//	if err == nil {
	//		err = <-errc
	//	}
	//
	// which cannot be used verbatim here because this handler also selects on the
	// stream timeout, so the wait is repeated inside that select instead. Both
	// accept the same sequences; the loop only additionally lets the timeout win
	// at either wait.
	//
	// See https://github.com/caddyserver/caddy/issues/8026, and the same class
	// fixed upstream in net/http/httputil (https://go.dev/issue/35892).
	for range 2 {
		select {
		case err := <-errc:
			if err != nil {
				if c := s.logger.Check(zapcore.DebugLevel, "streaming error"); c != nil {
					c.Write(zap.Error(err))
				}
				result = streamResultError
				return
			}
		case time := <-timeoutc:
			if c := s.logger.Check(zapcore.DebugLevel, "stream timed out"); c != nil {
				c.Write(zap.Time("timeout", time))
			}
			result = streamResultTimeout
			return
		}
	}
}

// finish accounts the stream in the metrics and logs that it closed.
func (s *upgradedStream) finish(result string) {
	toBackend, fromBackend := s.toBackend.Load(), s.fromBackend.Load()
	took := s.account.finish(result, toBackend, fromBackend)
	for _, l := range s.closeLoggers {
		if c := l.Check(s.level, "stream closed"); c != nil {
			c.Write(
				zap.String("upstream", s.upstream),
				zap.String("result", result),
				zap.Duration("duration", took),
				zap.Int64("bytes_to_backend", toBackend),
				zap.Int64("bytes_from_backend", fromBackend),
			)
		}
	}
}

// streamRegistry tracks the streams that are detached from the config
// that created them, by upstream. It is global because it must outlive
// the handlers, which are replaced when the config is reloaded.
type streamRegistry struct {
	mu      sync.Mutex
	streams map[string]map[*upgradedStream]struct{}
}

var detachedStreams = &streamRegistry{streams: make(map[string]map[*upgradedStream]struct{})}

// add registers s as a stream to upstream. The returned function
// unregisters it.
func (r *streamRegistry) add(upstream string, s *upgradedStream) (remove func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.streams[upstream] == nil {
		r.streams[upstream] = make(map[*upgradedStream]struct{})
	}
	r.streams[upstream][s] = struct{}{}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.streams[upstream], s)
		if len(r.streams[upstream]) == 0 {
			delete(r.streams, upstream)
		}
	}
}

// count returns the number of registered streams to upstream.
func (r *streamRegistry) count(upstream string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.streams[upstream])
}

// closeUpstream closes the registered streams to upstream, and returns
// how many there were. Streams unregister themselves once they ended.
func (r *streamRegistry) closeUpstream(upstream string) int {
	r.mu.Lock()
	closing := make([]*upgradedStream, 0, len(r.streams[upstream]))
	for s := range r.streams[upstream] {
		closing = append(closing, s)
	}
	r.mu.Unlock()

	// closing may block (graceful close writes to the connection)
	for _, s := range closing {
		_ = s.closeForReload()
	}
	return len(closing)
}

// flushInterval returns the p.FlushInterval value, conditionally
// overriding its value for a specific request/response.
func (h Handler) flushInterval(req *http.Request, res *http.Response, incremental bool) time.Duration {
	// The upstream asked for the response to be forwarded incrementally.
	// See RFC 10036: https://www.rfc-editor.org/rfc/rfc10036.html
	if incremental || caddyhttp.IsIncremental(res.Header) {
		return -1 // negative means immediately
	}

	resCTHeader := res.Header.Get("Content-Type")
	resCT, _, err := mime.ParseMediaType(resCTHeader)

	// For Server-Sent Events responses, flush immediately.
	// The MIME type is defined in https://www.w3.org/TR/eventsource/#text-event-stream
	if err == nil && resCT == "text/event-stream" {
		return -1
	}

	// We might have the case of streaming for which Content-Length might be unset.
	if res.ContentLength == -1 {
		return -1
	}

	// for h2 and h2c upstream streaming data to client (issues #3556 and #3606)
	if h.isBidirectionalStream(req, res) {
		return -1
	}

	return time.Duration(h.FlushInterval)
}

// isBidirectionalStream returns whether we should work in bi-directional stream mode.
//
// See https://github.com/caddyserver/caddy/pull/3620 for discussion of nuances.
func (h Handler) isBidirectionalStream(req *http.Request, res *http.Response) bool {
	// We have to check the encoding here; only flush headers with identity encoding.
	// Non-identity encoding might combine with "encode" directive, and in that case,
	// if body size larger than enc.MinLength, upper level encode handle might have
	// Content-Encoding header to write.
	// (see https://github.com/caddyserver/caddy/issues/3606 for use case)
	ae := req.Header.Get("Accept-Encoding")

	return req.ProtoMajor == 2 &&
		res.ProtoMajor == 2 &&
		res.ContentLength == -1 &&
		(ae == "identity" || ae == "")
}

func (h Handler) copyResponse(dst http.ResponseWriter, src io.Reader, flushInterval time.Duration, logger *zap.Logger) error {
	var w io.Writer = dst

	if flushInterval != 0 {
		var mlwLogger *zap.Logger
		if h.VerboseLogs {
			mlwLogger = logger.Named("max_latency_writer")
		} else {
			mlwLogger = zap.NewNop()
		}
		mlw := &maxLatencyWriter{
			dst: dst,
			//nolint:bodyclose
			flush:   http.NewResponseController(dst).Flush,
			latency: flushInterval,
			logger:  mlwLogger,
		}
		defer mlw.stop()

		// set up initial timer so headers get flushed even if body writes are delayed
		mlw.flushPending = true
		mlw.t = time.AfterFunc(flushInterval, mlw.delayedFlush)

		w = mlw
	}

	buf := streamingBufPool.Get().(*[]byte)
	defer streamingBufPool.Put(buf)

	var copyLogger *zap.Logger
	if h.VerboseLogs {
		copyLogger = logger
	} else {
		copyLogger = zap.NewNop()
	}

	_, err := h.copyBuffer(w, src, *buf, copyLogger)
	return err
}

// copyBuffer returns any write errors or non-EOF read errors, and the amount
// of bytes written.
func (h Handler) copyBuffer(dst io.Writer, src io.Reader, buf []byte, logger *zap.Logger) (int64, error) {
	if len(buf) == 0 {
		buf = make([]byte, defaultBufferSize)
	}
	var written int64
	for {
		logger.Debug("waiting to read from upstream")
		nr, rerr := src.Read(buf)
		logger := logger.With(zap.Int("read", nr))
		if c := logger.Check(zapcore.DebugLevel, "read from upstream"); c != nil {
			c.Write(zap.Error(rerr))
		}
		if rerr != nil && rerr != io.EOF && rerr != context.Canceled {
			// TODO: this could be useful to know (indeed, it revealed an error in our
			// fastcgi PoC earlier; but it's this single error report here that necessitates
			// a function separate from io.CopyBuffer, since io.CopyBuffer does not distinguish
			// between read or write errors; in a reverse proxy situation, write errors are not
			// something we need to report to the client, but read errors are a problem on our
			// end for sure. so we need to decide what we want.)
			// p.logf("copyBuffer: ReverseProxy read error during body copy: %v", rerr)
			if c := logger.Check(zapcore.ErrorLevel, "reading from backend"); c != nil {
				c.Write(zap.Error(rerr))
			}
		}
		if nr > 0 {
			logger.Debug("writing to downstream")
			nw, werr := dst.Write(buf[:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if c := logger.Check(zapcore.DebugLevel, "wrote to downstream"); c != nil {
				c.Write(
					zap.Int("written", nw),
					zap.Int64("written_total", written),
					zap.Error(werr),
				)
			}
			if werr != nil {
				return written, fmt.Errorf("writing: %w", werr)
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				return written, nil
			}
			return written, fmt.Errorf("reading: %w", rerr)
		}
	}
}

// registerConnection holds onto conn so it can be closed in the event
// of a server shutdown. This is useful because hijacked connections or
// connections dialed to backends don't close when server is shut down.
// The caller should call the returned delete() function when the
// connection is done to remove it from memory.
// onReload, if not nil, is called before the connection is closed because
// of a config reload.
func (h *Handler) registerConnection(conn io.ReadWriteCloser, gracefulClose func() error, onReload func()) (del func()) {
	h.connectionsMu.Lock()
	h.connections[conn] = openConnection{conn, gracefulClose, onReload}
	h.connectionsMu.Unlock()
	return func() {
		h.connectionsMu.Lock()
		delete(h.connections, conn)
		// if there is no connection left before the connections close timer fires
		if len(h.connections) == 0 && h.connectionsCloseTimer != nil {
			// we release the timer that holds the reference to Handler
			if (*h.connectionsCloseTimer).Stop() {
				h.logger.Debug("stopped streaming connections close timer - all connections are already closed")
			}
			h.connectionsCloseTimer = nil
		}
		h.connectionsMu.Unlock()
	}
}

// closeConnections immediately closes all hijacked connections (both to client and backend).
func (h *Handler) closeConnections() error {
	var err error
	h.connectionsMu.Lock()
	defer h.connectionsMu.Unlock()

	for _, oc := range h.connections {
		if oc.onReload != nil {
			oc.onReload()
		}
		// this is potentially blocking while we have the lock on the connections
		// map, but that should be OK since the server has in theory shut down
		// and we are no longer using the connections map
		if closeErr := oc.close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

// cleanupConnections closes hijacked connections.
// Depending on the value of StreamCloseDelay it does that either immediately
// or sets up a timer that will do that later.
func (h *Handler) cleanupConnections() error {
	if h.StreamCloseDelay == 0 {
		return h.closeConnections()
	}

	h.connectionsMu.Lock()
	defer h.connectionsMu.Unlock()
	// the handler is shut down, no new connection can appear,
	// so we can skip setting up the timer when there are no connections
	if len(h.connections) > 0 {
		delay := time.Duration(h.StreamCloseDelay)
		h.connectionsCloseTimer = time.AfterFunc(delay, func() {
			if c := h.logger.Check(zapcore.DebugLevel, "closing streaming connections after delay"); c != nil {
				c.Write(zap.Duration("delay", delay))
			}
			err := h.closeConnections()
			if err != nil {
				if c := h.logger.Check(zapcore.ErrorLevel, "failed to closed connections after delay"); c != nil {
					c.Write(
						zap.Error(err),
						zap.Duration("delay", delay),
					)
				}
			}
		})
	}
	return nil
}

// writeCloseControl sends a best-effort Close control message to the given
// WebSocket connection. Thanks to @pascaldekloe who provided inspiration
// from his simple implementation of this I was able to learn from at:
// github.com/pascaldekloe/websocket. Further work for handling masking
// taken from github.com/gorilla/websocket.
func writeCloseControl(conn io.Writer, isClient bool) error {
	// Sources:
	// https://github.com/pascaldekloe/websocket/blob/32050af67a5d/websocket.go#L119
	// https://github.com/gorilla/websocket/blob/v1.5.0/conn.go#L413

	// For now, we're not using a reason. We might later, though.
	// The code handling the reason is left in
	var reason string // max 123 bytes (control frame payload limit is 125; status code takes 2)

	const closeMessage = 8
	const finalBit = 1 << 7 // Frame header byte 0 bits from Section 5.2 of RFC 6455
	const maskBit = 1 << 7  // Frame header byte 1 bits from Section 5.2 of RFC 6455
	const goingAwayUpper uint8 = 1001 >> 8
	const goingAwayLower uint8 = 1001 & 0xff

	b0 := byte(closeMessage) | finalBit
	b1 := byte(len(reason) + 2)
	if isClient {
		b1 |= maskBit
	}

	buf := make([]byte, 0, 127)
	buf = append(buf, b0, b1)
	msgLength := 4 + len(reason)

	// Both branches below append the "going away" code and reason
	appendMessage := func(buf []byte) []byte {
		buf = append(buf, goingAwayUpper, goingAwayLower)
		buf = append(buf, []byte(reason)...)
		return buf
	}

	// When we're the client, we need to mask the message as per
	// https://www.rfc-editor.org/rfc/rfc6455#section-5.3
	if isClient {
		key := newMaskKey()
		buf = append(buf, key[:]...)
		msgLength += len(key)
		buf = appendMessage(buf)
		maskBytes(key, 0, buf[2+len(key):])
	} else {
		buf = appendMessage(buf)
	}

	// simply best-effort, but return error for logging purposes
	// TODO: we might need to ensure we are the exclusive writer by this point (io.Copy is stopped)?
	_, err := conn.Write(buf[:msgLength])
	return err
}

// Copied from https://github.com/gorilla/websocket/blob/v1.5.0/mask.go
func maskBytes(key [4]byte, pos int, b []byte) int {
	// Mask one byte at a time for small buffers.
	if len(b) < 2*wordSize {
		for i := range b {
			b[i] ^= key[pos&3]
			pos++
		}
		return pos & 3
	}

	// Mask one byte at a time to word boundary.
	if n := int(uintptr(unsafe.Pointer(&b[0]))) % wordSize; n != 0 {
		n = wordSize - n
		for i := range b[:n] {
			b[i] ^= key[pos&3]
			pos++
		}
		b = b[n:]
	}

	// Create aligned word size key.
	var k [wordSize]byte
	for i := range k {
		k[i] = key[(pos+i)&3] // nolint:gosec // false positive, impossible to be out of bounds; see: https://github.com/securego/gosec/issues/1525
	}
	kw := *(*uintptr)(unsafe.Pointer(&k))

	// Mask one word at a time.
	n := (len(b) / wordSize) * wordSize
	for i := 0; i < n; i += wordSize {
		*(*uintptr)(unsafe.Add(unsafe.Pointer(&b[0]), i)) ^= kw
	}

	// Mask one byte at a time for remaining bytes.
	b = b[n:]
	for i := range b {
		b[i] ^= key[pos&3]
		pos++
	}

	return pos & 3
}

// Copied from https://github.com/gorilla/websocket/blob/v1.5.0/conn.go#L184
func newMaskKey() [4]byte {
	n := weakrand.Uint32()
	return [4]byte{byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24)}
}

// isWebsocket returns true if r looks to be an upgrade request for WebSockets.
// It is a fairly naive check.
func isWebsocket(r *http.Request) bool {
	return httpguts.HeaderValuesContainsToken(r.Header["Connection"], "upgrade") &&
		httpguts.HeaderValuesContainsToken(r.Header["Upgrade"], "websocket")
}

// openConnection maps an open connection to
// an optional function for graceful close.
type openConnection struct {
	conn          io.ReadWriteCloser
	gracefulClose func() error
	onReload      func()
}

// close closes the connection, gracefully first if it can be.
func (oc openConnection) close() error {
	var err error
	if oc.gracefulClose != nil {
		err = oc.gracefulClose()
	}
	if closeErr := oc.conn.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	return err
}

type maxLatencyWriter struct {
	dst     io.Writer
	flush   func() error
	latency time.Duration // non-zero; negative means to flush immediately

	mu           sync.Mutex // protects t, flushPending, and dst.Flush
	t            *time.Timer
	flushPending bool
	logger       *zap.Logger
}

func (m *maxLatencyWriter) Write(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, err = m.dst.Write(p)
	if c := m.logger.Check(zapcore.DebugLevel, "wrote bytes"); c != nil {
		c.Write(zap.Int("n", n), zap.Error(err))
	}
	if m.latency < 0 {
		m.logger.Debug("flushing immediately")
		//nolint:errcheck
		m.flush()
		return n, err
	}
	if m.flushPending {
		m.logger.Debug("delayed flush already pending")
		return n, err
	}
	if m.t == nil {
		m.t = time.AfterFunc(m.latency, m.delayedFlush)
	} else {
		m.t.Reset(m.latency)
	}
	if c := m.logger.Check(zapcore.DebugLevel, "timer set for delayed flush"); c != nil {
		c.Write(zap.Duration("duration", m.latency))
	}
	m.flushPending = true
	return n, err
}

func (m *maxLatencyWriter) delayedFlush() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.flushPending { // if stop was called but AfterFunc already started this goroutine
		m.logger.Debug("delayed flush is not pending")
		return
	}
	m.logger.Debug("delayed flush")
	//nolint:errcheck
	m.flush()
	m.flushPending = false
}

func (m *maxLatencyWriter) stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushPending = false
	if m.t != nil {
		m.t.Stop()
	}
}

// switchProtocolCopier exists so goroutines proxying data back and
// forth have nice names in stacks.
type switchProtocolCopier struct {
	user, backend io.ReadWriteCloser
	wg            *sync.WaitGroup
	bufferSize    int

	// count the bytes copied to the backend and from the backend, if not nil
	sent, received *atomic.Int64
}

// errCopyDone is reported by a direction that ended at a clean EOF on a
// connection whose write half cannot be closed, so the end of that direction
// could not be propagated to the peer.
var errCopyDone = errors.New("hijacked connection copy complete")

func (c switchProtocolCopier) copyFromBackend(errc chan<- error) {
	defer c.wg.Done()
	n, err := io.CopyBuffer(c.user, c.backend, c.buffer())
	if c.received != nil {
		c.received.Add(n)
	}
	if err != nil {
		errc <- err
		return
	}

	// backend conn has reached EOF so propagate close write to user conn
	if wc, ok := c.user.(interface{ CloseWrite() error }); ok {
		errc <- wc.CloseWrite()
		return
	}

	errc <- errCopyDone
}

func (c switchProtocolCopier) copyToBackend(errc chan<- error) {
	defer c.wg.Done()
	n, err := io.CopyBuffer(c.backend, c.user, c.buffer())
	if c.sent != nil {
		c.sent.Add(n)
	}
	if err != nil {
		errc <- err
		return
	}

	// user conn has reached EOF so propagate close write to backend conn
	if wc, ok := c.backend.(interface{ CloseWrite() error }); ok {
		errc <- wc.CloseWrite()
		return
	}

	errc <- errCopyDone
}

func (c switchProtocolCopier) buffer() []byte {
	size := c.bufferSize
	if size <= 0 {
		size = defaultBufferSize
	}
	return make([]byte, size)
}

var streamingBufPool = sync.Pool{
	New: func() any {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation
		// - (from the package docs)
		b := make([]byte, defaultBufferSize)
		return &b
	},
}

const (
	defaultBufferSize = 32 * 1024
	wordSize          = int(unsafe.Sizeof(uintptr(0)))
)
