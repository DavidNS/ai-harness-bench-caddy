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

func (h *Handler) handleUpgradeResponse(logger *zap.Logger, wg *sync.WaitGroup, rw http.ResponseWriter, req *http.Request, res *http.Response) {
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

	if h.StreamLogs != nil && h.StreamLogs.SkipHandshake {
		caddyhttp.SetVar(req.Context(), caddyhttp.LogSkipVar, true)
	}

	var (
		conn     io.ReadWriteCloser
		brw      *bufio.ReadWriter
		hijacked bool
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
		// bufio is not needed, use minimal buffer
		brw = bufio.NewReadWriter(bufio.NewReaderSize(conn, 1), bufio.NewWriterSize(conn, 1))
	} else {
		rw.WriteHeader(res.StatusCode)

		if c := logger.Check(zap.DebugLevel, "upgrading connection"); c != nil {
			c.Write(zap.Int("http_version", req.ProtoMajor))
		}

		var (
			netConn   net.Conn
			hijackErr error
		)
		//nolint:bodyclose
		netConn, brw, hijackErr = http.NewResponseController(rw).Hijack()
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

		// move the bytes on the raw connection, not through the response
		// recorder of the access log, which must not be written to once
		// the connection is hijacked and would count the stream as response
		conn = caddyhttp.UnwrapHijackedConn(netConn)
		hijacked = true
	}

	// A detached stream outlives this handler call and the config, so what
	// runs it must not depend on the request (whose context is canceled
	// when the handler returns) or on the handler. Only a connection that
	// was hijacked can outlive the handler: the HTTP/2 stream of an extended
	// CONNECT request is over when the handler returns.
	detached := h.StreamDetached && hijacked
	s := &upgradedStream{
		logger:     logger,
		stats:      newStreamStats(upstreamKey(res), h.streamLoggers(req), h.streamLevel()),
		front:      conn,
		back:       backConn,
		brw:        brw,
		websocket:  isWebsocket(req),
		timeout:    time.Duration(h.StreamTimeout),
		bufferSize: h.StreamBufferSize,
		wg:         wg,
	}
	if detached {
		s.wg = new(sync.WaitGroup)
		s.track = func(oc ...openConnection) func() {
			return detachedStreams.add(s.stats.upstream, &detachedStream{conns: oc, stats: s.stats})
		}
		// return only once the stream is known to the registry, so that a
		// config unloaded right after cannot miss it
		s.ready = make(chan struct{})
		go s.run()
		<-s.ready
		return
	}
	s.canceled = req.Context().Done()
	s.track = func(oc ...openConnection) func() {
		var dels []func()
		for _, c := range oc {
			dels = append(dels, h.registerConnection(c.conn, c.gracefulClose, func() { s.stats.setResult(streamResultCleanup) }))
		}
		return func() {
			for _, del := range dels {
				del()
			}
		}
	}
	s.run()
}

// upgradedStream proxies an upgraded connection between the client (front)
// and the backend (back) once the handshake is done. It deliberately holds no
// reference to the Handler or to the request, so that a detached stream does
// not keep the config it was started by alive.
type upgradedStream struct {
	logger     *zap.Logger
	stats      *streamStats
	front      io.ReadWriteCloser
	back       io.ReadWriteCloser
	brw        *bufio.ReadWriter // of the client connection, may hold client bytes
	websocket  bool
	timeout    time.Duration
	bufferSize int
	wg         *sync.WaitGroup

	// canceled is closed when the client request is canceled, which closes
	// the backend. It is nil when the stream is detached from the request.
	canceled <-chan struct{}

	// track makes the connections known so that they get closed when the
	// stream has to end; it returns the function to forget them again.
	track func(conns ...openConnection) (untrack func())

	// ready, if not nil, is closed once the connections are tracked, or
	// the stream failed before they could be.
	ready     chan struct{}
	readyOnce sync.Once
}

func (s *upgradedStream) setReady() {
	if s.ready != nil {
		s.readyOnce.Do(func() { close(s.ready) })
	}
}

// run proxies the stream until it is over, then closes both connections
// and accounts for the stream.
func (s *upgradedStream) run() {
	defer s.setReady()

	// adopted from https://github.com/golang/go/commit/8bcf2834afdf6a1f7937390903a41518715ef6f5
	backConnCloseCh := make(chan struct{})
	go func() {
		// Ensure that the cancellation of a request closes the backend.
		// See issue https://golang.org/issue/35559.
		select {
		case <-s.canceled:
			s.stats.setResult(streamResultCanceled)
		case <-backConnCloseCh:
		}
		s.back.Close()
	}()

	start := time.Now()
	spc := switchProtocolCopier{
		user:        s.front,
		backend:     s.back,
		wg:          s.wg,
		bufferSize:  s.bufferSize,
		toBackend:   &s.stats.toBackend,
		fromBackend: &s.stats.fromBackend,
	}

	// the copiers have finished once the connections are closed, which is
	// when the byte counts are final and the stream can be accounted for
	defer func() {
		close(backConnCloseCh)
		s.front.Close()
		s.back.Close()
		s.wg.Wait()
		if c := s.logger.Check(zapcore.DebugLevel, "connection closed"); c != nil {
			c.Write(zap.Duration("duration", time.Since(start)))
		}
		s.stats.finish()
	}()

	if err := s.brw.Flush(); err != nil {
		s.stats.setResult(streamResultError)
		if c := s.logger.Check(zapcore.DebugLevel, "response flush"); c != nil {
			c.Write(zap.Error(err))
		}
		return
	}

	// There may be buffered data in the *bufio.Reader
	// see: https://github.com/caddyserver/caddy/issues/6273
	if buffered := s.brw.Reader.Buffered(); buffered > 0 {
		data, _ := s.brw.Peek(buffered)
		n, err := s.back.Write(data)
		s.stats.toBackend.Add(int64(n))
		if err != nil {
			s.stats.setResult(streamResultError)
			if c := s.logger.Check(zapcore.DebugLevel, "backConn write failed"); c != nil {
				c.Write(zap.Error(err))
			}
			return
		}
	}

	// Ensure the hijacked client connection, and the new connection established
	// with the backend, are both closed in the event of a server shutdown. This
	// is done by registering them. We also try to gracefully close connections
	// we recognize as websockets.
	// We need to make sure the client connection messages (i.e. to upstream)
	// are masked, so we need to know whether the connection is considered the
	// server or the client side of the proxy.
	gracefulClose := func(conn io.ReadWriteCloser, isClient bool) func() error {
		if s.websocket {
			return func() error {
				return writeCloseControl(conn, isClient)
			}
		}
		return nil
	}
	untrack := s.track(
		openConnection{conn: s.front, gracefulClose: gracefulClose(s.front, false)},
		openConnection{conn: s.back, gracefulClose: gracefulClose(s.back, true)},
	)
	defer untrack()
	s.setReady()

	// setup the timeout if requested
	var timeoutc <-chan time.Time
	if s.timeout > 0 {
		timer := time.NewTimer(s.timeout)
		defer timer.Stop()
		timeoutc = timer.C
	}
	logger := s.logger
	wg := s.wg

	// when a stream timeout is encountered, no error will be read from errc
	// a buffer size of 2 will allow both the read and write goroutines to send the error and exit
	// see: https://github.com/caddyserver/caddy/issues/7418
	errc := make(chan error, 2)
	wg.Add(2)
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
				// errCopyDone is the regular end of a connection that can't be half-closed
				if !errors.Is(err, errCopyDone) {
					s.stats.setResult(streamResultError)
				}
				if c := logger.Check(zapcore.DebugLevel, "streaming error"); c != nil {
					c.Write(zap.Error(err))
				}
				return
			}
		case time := <-timeoutc:
			s.stats.setResult(streamResultTimeout)
			if c := logger.Check(zapcore.DebugLevel, "stream timed out"); c != nil {
				c.Write(zap.Time("timeout", time))
			}
			return
		}
	}
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
// onClose, if not nil, is called right before the connection is closed
// because the handler is shutting down.
func (h *Handler) registerConnection(conn io.ReadWriteCloser, gracefulClose func() error, onClose func()) (del func()) {
	h.connectionsMu.Lock()
	h.connections[conn] = openConnection{conn: conn, gracefulClose: gracefulClose, onClose: onClose}
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
		if oc.onClose != nil {
			oc.onClose()
		}
		if oc.gracefulClose != nil {
			// this is potentially blocking while we have the lock on the connections
			// map, but that should be OK since the server has in theory shut down
			// and we are no longer using the connections map
			gracefulErr := oc.gracefulClose()
			if gracefulErr != nil && err == nil {
				err = gracefulErr
			}
		}
		closeErr := oc.conn.Close()
		if closeErr != nil && err == nil {
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
	onClose       func() // called before closing because of a shutdown
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

	// the number of bytes copied in each direction is added to these, if set
	toBackend, fromBackend *atomic.Int64
}

// errCopyDone is reported by a direction that ended at a clean EOF on a
// connection whose write half cannot be closed, so the end of that direction
// could not be propagated to the peer.
var errCopyDone = errors.New("hijacked connection copy complete")

func (c switchProtocolCopier) copyFromBackend(errc chan<- error) {
	defer c.wg.Done()
	n, err := io.CopyBuffer(c.user, c.backend, c.buffer())
	if c.fromBackend != nil {
		c.fromBackend.Add(n)
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
	if c.toBackend != nil {
		c.toBackend.Add(n)
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
