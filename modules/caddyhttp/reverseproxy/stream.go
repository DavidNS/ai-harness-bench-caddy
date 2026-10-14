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

package reverseproxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

// StreamLogs configures the logging of upgraded streams such as
// WebSockets.
type StreamLogs struct {
	// If true, the handshake of the upgrade is not written to the
	// access log. By default, it is logged like any other request.
	SkipHandshake bool `json:"skip_handshake,omitempty"`

	// The level at which the closing of a stream is logged, one of
	// debug, info, warn or error. Default: debug.
	Level string `json:"level,omitempty"`

	// The name of the logger that logs the closing of a stream, appended
	// to "http.log.". The special name "access" makes the closing of a
	// stream get logged wherever the access log of its request goes, so
	// that it appears next to the access log entries; any other name,
	// like "stream", is the logger "http.log.stream". By default, the
	// logger of the reverse_proxy handler is used.
	LoggerName string `json:"logger_name,omitempty"`

	level zapcore.Level
}

// provision validates sl and parses its level.
func (sl *StreamLogs) provision() error {
	sl.level = zapcore.DebugLevel
	if sl.Level == "" {
		return nil
	}
	if err := sl.level.UnmarshalText([]byte(sl.Level)); err != nil {
		return fmt.Errorf("invalid stream_logs level '%s': %v", sl.Level, err)
	}
	return nil
}

// logLevel returns the level at which stream logs are written.
func (sl *StreamLogs) logLevel() zapcore.Level {
	if sl == nil {
		return zapcore.DebugLevel
	}
	return sl.level
}

// skipHandshake reports whether the handshake is kept out of the access log.
func (sl *StreamLogs) skipHandshake() bool {
	return sl != nil && sl.SkipHandshake
}

// streamLogModule lets a logger be created under an arbitrary name,
// since Caddy names (and routes) loggers after a module ID.
type streamLogModule string

func (m streamLogModule) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{ID: caddy.ModuleID(m)}
}

// streamOptions are the settings of a stream. They are copied from
// the handler so that a detached stream doesn't reference it, and
// with it the config that was loaded when the stream started.
type streamOptions struct {
	// upstream is the address of the backend, used to label metrics.
	upstream string

	// upstreamKey identifies the configured upstream the stream is going
	// through in the pool of hosts. It is empty if the stream is not tied
	// to an upstream of the config, e.g. for dynamic upstreams.
	upstreamKey string

	bufferSize int
	timeout    time.Duration

	// detached streams are not closed when the config they were started
	// under is unloaded, only when their upstream is removed from the config.
	detached bool

	logs StreamLogs

	// logNames are the names of the loggers the closing of the stream is
	// logged to, see StreamLogs.LoggerName.
	logNames []string

	// req is a copy of the request that holds no reference to
	// the config, kept to describe the stream when it is closed.
	req        *http.Request
	logCreds   bool
	websocket  bool
	logger     *zap.Logger // used while the config is known to be alive
	verboseLog bool
}

// upgradedStream is a connection that was upgraded (WebSocket, HTTP/2
// extended CONNECT, etc.) and is being copied between a client and a backend.
type upgradedStream struct {
	streamOptions

	user, backend io.ReadWriteCloser
	userGraceful  func() error
	backGraceful  func() error

	start time.Time

	toBackend   atomic.Int64
	fromBackend atomic.Int64

	// closedByProxy is set when the proxy closes the stream because its
	// config went away or its upstream was removed, before the connections
	// are closed, to tell that apart from the connections failing.
	closedByProxy atomic.Bool
	// canceled is set when the request is canceled, which closes the backend.
	canceled atomic.Bool

	finishOnce sync.Once
}

func newUpgradedStream(opts streamOptions, user, backend io.ReadWriteCloser) *upgradedStream {
	s := &upgradedStream{
		streamOptions: opts,
		user:          user,
		backend:       backend,
		start:         time.Now(),
	}
	if opts.websocket {
		s.userGraceful = func() error { return writeCloseControl(user, false) }
		s.backGraceful = func() error { return writeCloseControl(backend, true) }
	}
	streamMetrics.active.WithLabelValues(opts.upstream).Inc()
	return s
}

// closeByProxy closes both ends of the stream, trying to close WebSockets
// gracefully, on behalf of the proxy rather than because of the peers.
func (s *upgradedStream) closeByProxy() error {
	s.closedByProxy.Store(true)

	var err error
	for _, c := range []struct {
		conn     io.Closer
		graceful func() error
	}{{s.user, s.userGraceful}, {s.backend, s.backGraceful}} {
		if c.graceful != nil {
			if gerr := c.graceful(); gerr != nil && err == nil {
				err = gerr
			}
		}
		if cerr := c.conn.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

// finish accounts for the end of the stream, which it must do exactly once
// per stream however the stream ended, and logs it.
func (s *upgradedStream) finish(result string, streamErr error) {
	s.finishOnce.Do(func() {
		duration := time.Since(s.start)
		toBackend, fromBackend := s.toBackend.Load(), s.fromBackend.Load()

		streamMetrics.active.WithLabelValues(s.upstream).Dec()
		streamMetrics.closed.With(prometheus.Labels{"upstream": s.upstream, "result": result}).Inc()
		streamMetrics.duration.With(prometheus.Labels{"upstream": s.upstream, "result": result}).Observe(duration.Seconds())
		streamMetrics.bytes.WithLabelValues(s.upstream, "to_backend").Add(float64(toBackend))
		streamMetrics.bytes.WithLabelValues(s.upstream, "from_backend").Add(float64(fromBackend))

		loggers := s.closeLoggers()
		var fields []zapcore.Field
		for _, logger := range loggers {
			c := logger.Check(s.logs.logLevel(), "stream closed")
			if c == nil {
				continue
			}
			if fields == nil {
				fields = s.closeFields(result, duration, streamErr)
			}
			c.Write(fields...)
		}
	})
}

// closeFields returns the fields describing the end of the stream.
func (s *upgradedStream) closeFields(result string, duration time.Duration, streamErr error) []zapcore.Field {
	fields := []zapcore.Field{
		zap.String("upstream", s.upstream),
		zap.String("result", result),
		zap.Bool("detached", s.detached),
		zap.Duration("duration", duration),
		zap.Int64("bytes_to_backend", s.toBackend.Load()),
		zap.Int64("bytes_from_backend", s.fromBackend.Load()),
	}
	if streamErr != nil {
		fields = append(fields, zap.Error(streamErr))
	}
	if s.req != nil {
		fields = append(fields, zap.Object("request", caddyhttp.LoggableHTTPRequest{
			Request:              s.req,
			ShouldLogCredentials: s.logCreds,
		}))
	}
	return fields
}

// defaultStreamLoggerID is the ID of the logger that logs the closing of
// streams unless configured otherwise: the one of the handler.
const defaultStreamLoggerID = "http.handlers.reverse_proxy"

// closeLoggers returns the loggers for the log of the closing of the stream.
// They are looked up in the config active now, since the config the stream
// was started under may be gone, together with its log destinations.
func (s *upgradedStream) closeLoggers() []*zap.Logger {
	active := caddy.ActiveContext()
	if active.Context == nil {
		// no config is running, as in tests
		if s.logger != nil {
			return []*zap.Logger{s.logger}
		}
		return nil
	}
	names := s.logNames
	if len(names) == 0 {
		names = []string{defaultStreamLoggerID}
	}
	loggers := make([]*zap.Logger, 0, len(names))
	for _, name := range names {
		loggers = append(loggers, active.Logger(streamLogModule(name)))
	}
	return loggers
}

// streamLoggerNames returns the IDs of the loggers named by name, see
// StreamLogs.LoggerName, for the request req.
func streamLoggerNames(name string, server *caddyhttp.Server, req *http.Request) []string {
	switch {
	case name == "":
		return []string{defaultStreamLoggerID}
	case name == "access" && server != nil && server.Logs != nil:
		var ids []string
		for _, accessName := range server.Logs.AccessLoggerNames(req) {
			if accessName == "" {
				ids = append(ids, "http.log.access")
				continue
			}
			ids = append(ids, "http.log.access."+accessName)
		}
		return ids
	default:
		return []string{"http.log." + name}
	}
}

// run proxies the stream until it ends, then closes it. If reqDone is not nil,
// it ends the stream when it is closed. The caller must not use the connections
// afterwards.
func (s *upgradedStream) run(logger *zap.Logger, wg *sync.WaitGroup, reqDone <-chan struct{}, brwBuffered []byte) {
	result, streamErr := streamResultError, error(nil)

	backConnCloseCh := make(chan struct{})
	if reqDone != nil {
		go func() {
			// Ensure that the cancellation of a request closes the backend.
			// See issue https://golang.org/issue/35559.
			select {
			case <-reqDone:
				s.canceled.Store(true)
			case <-backConnCloseCh:
			}
			s.backend.Close()
		}()
	}

	defer func() {
		close(backConnCloseCh)
		s.user.Close()
		s.backend.Close()
		// let the copiers end so that all the bytes are accounted for
		wg.Wait()
		if c := logger.Check(zapcore.DebugLevel, "connection closed"); c != nil {
			c.Write(zap.Duration("duration", time.Since(s.start)))
		}
		s.finish(result, streamErr)
	}()

	// There may be buffered data in the *bufio.Reader
	// see: https://github.com/caddyserver/caddy/issues/6273
	if len(brwBuffered) > 0 {
		n, err := s.backend.Write(brwBuffered)
		s.toBackend.Add(int64(n))
		if err != nil {
			if c := logger.Check(zapcore.DebugLevel, "backConn write failed"); c != nil {
				c.Write(zap.Error(err))
			}
			streamErr = err
			return
		}
	}

	spc := switchProtocolCopier{
		user:        s.user,
		backend:     s.backend,
		wg:          wg,
		bufferSize:  s.bufferSize,
		toBackend:   &s.toBackend,
		fromBackend: &s.fromBackend,
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
				if c := logger.Check(zapcore.DebugLevel, "streaming error"); c != nil {
					c.Write(zap.Error(err))
				}
				streamErr = err
				switch {
				case s.closedByProxy.Load():
					result = streamResultClosed
				case s.canceled.Load():
					result = streamResultCanceled
				}
				return
			}
		case t := <-timeoutc:
			if c := logger.Check(zapcore.DebugLevel, "stream timed out"); c != nil {
				c.Write(zap.Time("timeout", t))
			}
			result = streamResultTimeout
			return
		}
	}
	result = streamResultCompleted
}

// detachedStreams holds the streams started by handlers with stream_detached
// set. It is global because the streams outlive the handler, and the config,
// that created them.
var detachedStreams = struct {
	sync.Mutex
	streams map[*upgradedStream]struct{}
}{streams: make(map[*upgradedStream]struct{})}

// registerDetached adds s to the detached streams and returns a
// function that removes it again.
func registerDetached(s *upgradedStream) (remove func()) {
	detachedStreams.Lock()
	detachedStreams.streams[s] = struct{}{}
	detachedStreams.Unlock()

	return func() {
		detachedStreams.Lock()
		delete(detachedStreams.streams, s)
		detachedStreams.Unlock()
	}
}

// closeDetachedStreams closes the detached streams going through the upstream
// with the given key, after delay if it is not zero, unless the upstream is
// used by a config again by then.
func closeDetachedStreams(key string, delay time.Duration, logger *zap.Logger) {
	if key == "" {
		return
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	closeNow := func() {
		if _, ok := hosts.References(key); ok {
			return
		}

		detachedStreams.Lock()
		var toClose []*upgradedStream
		for s := range detachedStreams.streams {
			if s.upstreamKey == key {
				toClose = append(toClose, s)
			}
		}
		detachedStreams.Unlock()

		for _, s := range toClose {
			if err := s.closeByProxy(); err != nil {
				if c := logger.Check(zapcore.DebugLevel, "closing detached stream"); c != nil {
					c.Write(zap.String("upstream", key), zap.Error(err))
				}
			}
		}
	}
	if delay <= 0 {
		closeNow()
		return
	}
	time.AfterFunc(delay, closeNow)
}

// streamRequestCopy returns a copy of req for describing a stream in logs
// that references nothing of the config, for it to be kept by streams that
// outlive it.
func streamRequestCopy(req *http.Request) *http.Request {
	cp := req.Clone(context.Background())
	cp.Body = nil
	if cp.RequestURI == "" && cp.URL != nil {
		cp.RequestURI = cp.URL.RequestURI()
	}
	return cp
}
