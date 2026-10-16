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
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

// StreamLogs configures the logging of upgraded streams
// such as WebSockets.
type StreamLogs struct {
	// If true, the request that performs the upgrade handshake is not
	// written to the access log. By default, it is logged.
	SkipHandshake bool `json:"skip_handshake,omitempty"`

	// The level at which the log entry written when a stream closes is
	// emitted: debug, info, warn or error. The entry goes to the access
	// loggers of the request (see the `logger_name` option of the
	// Caddyfile `log` directive), and survives the config that created
	// the stream being reloaded. Default: debug.
	Level string `json:"level,omitempty"`
}

// parseStreamLogLevel returns the level named by s, or DEBUG if s is empty.
func parseStreamLogLevel(s string) (zapcore.Level, error) {
	if s == "" {
		return zapcore.DebugLevel, nil
	}
	var level zapcore.Level
	if err := level.UnmarshalText([]byte(s)); err != nil {
		return zapcore.DebugLevel, fmt.Errorf("invalid stream log level %q: %v", s, err)
	}
	return level, nil
}

// streamLogLevel returns the level of the stream close log entry.
func (h *Handler) streamLogLevel() zapcore.Level {
	if h.StreamLogs == nil {
		return zapcore.DebugLevel
	}
	// the level was validated at provisioning
	level, _ := parseStreamLogLevel(h.StreamLogs.Level)
	return level
}

// detachedStream is an upgraded stream that is not bound to the
// lifecycle of the handler that proxies it, only to its upstream.
type detachedStream struct {
	upstream string
	conns    []openConnection
}

// close closes the connections of the stream.
func (s *detachedStream) close() error {
	var err error
	for _, oc := range s.conns {
		if closeErr := closeOpenConnection(oc); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

// closeOpenConnection closes oc, gracefully if possible.
func closeOpenConnection(oc openConnection) error {
	var err error
	if oc.gracefulClose != nil {
		err = oc.gracefulClose()
	}
	if closeErr := oc.conn.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	return err
}

// streamRegistry keeps track of the upstreams that are part of the loaded
// configs and of the detached streams going to them, so that a stream can
// outlive the handler that created it and still be closed once nothing
// in the live config can reach its upstream anymore.
type streamRegistry struct {
	mu   sync.Mutex
	live map[string]int
	open map[string]map[*detachedStream]struct{}
}

func newStreamRegistry() *streamRegistry {
	return &streamRegistry{
		live: make(map[string]int),
		open: make(map[string]map[*detachedStream]struct{}),
	}
}

// detachedStreams is shared by all handlers, across config reloads.
var detachedStreams = newStreamRegistry()

// addLive records that a handler of the live config uses upstreams.
func (r *streamRegistry) addLive(upstreams []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, u := range upstreams {
		r.live[u]++
	}
}

// removeLive records that a handler stopped using upstreams. It returns, and
// forgets, the detached streams whose upstream is not used by any handler
// anymore; closing them is up to the caller.
func (r *streamRegistry) removeLive(upstreams []string) []*detachedStream {
	r.mu.Lock()
	defer r.mu.Unlock()
	var orphaned []*detachedStream
	for _, u := range upstreams {
		r.live[u]--
		if r.live[u] > 0 {
			continue
		}
		delete(r.live, u)
		for s := range r.open[u] {
			orphaned = append(orphaned, s)
		}
		delete(r.open, u)
	}
	return orphaned
}

// add tracks s until the returned function is called.
func (r *streamRegistry) add(s *detachedStream) (remove func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.open[s.upstream] == nil {
		r.open[s.upstream] = make(map[*detachedStream]struct{})
	}
	r.open[s.upstream][s] = struct{}{}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.open[s.upstream], s)
	}
}

// liveUpstreams returns the distinct upstream keys of h.
func (h *Handler) liveUpstreams() []string {
	var keys []string
	seen := make(map[string]struct{}, len(h.Upstreams))
	for _, u := range h.Upstreams {
		if _, ok := seen[u.Dial]; ok {
			continue
		}
		seen[u.Dial] = struct{}{}
		keys = append(keys, u.Dial)
	}
	return keys
}

// closeDetachedStreams closes streams and returns the first error.
func closeDetachedStreams(streams []*detachedStream) error {
	var err error
	for _, s := range streams {
		if closeErr := s.close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

// streamUpstream returns the key of the upstream that serves req.
func streamUpstream(req *http.Request) string {
	di, ok := GetDialInfo(req.Context())
	if !ok {
		return ""
	}
	if di.Upstream != nil {
		return di.Upstream.Dial
	}
	return di.String()
}

// Possible values of the result label of the closed streams metric.
const (
	streamResultCompleted = "completed"
	streamResultError     = "error"
	streamResultTimeout   = "timeout"
	streamResultCanceled  = "canceled"
	streamResultReload    = "reload"
)

// upgradedStream is the accounting state of one upgraded stream.
type upgradedStream struct {
	upstream string
	// closedByCleanup is set when the stream was closed
	// by the cleanup of the config instead of ending by itself
	closedByCleanup atomic.Bool
}

// cleanupConn marks the stream as closed by cleanup when closed.
type cleanupConn struct {
	io.ReadWriteCloser
	stream *upgradedStream
}

func (c *cleanupConn) Close() error {
	c.stream.closedByCleanup.Store(true)
	return c.ReadWriteCloser.Close()
}

// logStreamClosed writes the entry that reports a closed stream.
func (h *Handler) logStreamClosed(req *http.Request, stream *upgradedStream, result string, duration time.Duration, toBackend, fromBackend int64) {
	level := h.streamLogLevel()
	var fields []zapcore.Field
	for _, logger := range h.streamLoggers(req) {
		c := logger.Check(level, "stream closed")
		if c == nil {
			continue
		}
		if fields == nil {
			fields = []zapcore.Field{
				zap.Object("request", caddyhttp.LoggableHTTPRequest{Request: req}),
				zap.String("upstream", stream.upstream),
				zap.String("result", result),
				zap.Duration("duration", duration),
				zap.Int64("bytes_to_backend", toBackend),
				zap.Int64("bytes_from_backend", fromBackend),
			}
		}
		c.Write(fields...)
	}
}

// streamLoggers returns the loggers a stream close entry for req goes to:
// the access loggers if access logging is enabled for req, the logger of
// the handler otherwise.
func (h *Handler) streamLoggers(req *http.Request) []*zap.Logger {
	if srv, ok := req.Context().Value(caddyhttp.ServerCtxKey).(*caddyhttp.Server); ok {
		if loggers := srv.AccessLoggers(req); len(loggers) > 0 {
			return loggers
		}
	}
	return []*zap.Logger{h.logger}
}
