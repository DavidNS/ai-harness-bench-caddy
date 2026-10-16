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
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

// StreamLogs configures the logging of upgraded streams (WebSockets and
// other protocol upgrades, HTTP/2 extended CONNECT).
type StreamLogs struct {
	// If true, the upgrade handshake (the request that is answered with
	// 101 Switching Protocols) is not written to the access log.
	// Default: the handshake is logged like any other request.
	SkipHandshake bool `json:"skip_handshake,omitempty"`

	// The level of the log entry written when a stream is closed. It
	// carries the upstream, the duration of the stream, the number of
	// bytes sent to and received from the backend, and the result.
	// Default: DEBUG.
	Level string `json:"level,omitempty"`

	// The logger to write the stream close log entry to: "access" selects
	// the access log, where the entry is written next to the access log
	// entry of the request that established the stream (and is not written
	// if the request is not logged). Default: the logger of this handler.
	LoggerName string `json:"logger_name,omitempty"`
}

// streamLoggerAccess is the name of the logger that stands for the access log.
const streamLoggerAccess = "access"

// provisionStreamLogs validates the options of StreamLogs.
func (h *Handler) provisionStreamLogs() error {
	if h.StreamLogs == nil {
		return nil
	}
	if h.StreamLogs.Level != "" {
		var lvl zapcore.Level
		if err := lvl.UnmarshalText([]byte(h.StreamLogs.Level)); err != nil {
			return fmt.Errorf("invalid stream log level '%s': %v", h.StreamLogs.Level, err)
		}
	}
	if name := h.StreamLogs.LoggerName; name != "" && name != streamLoggerAccess {
		return fmt.Errorf("invalid stream logger name '%s': must be '%s' or empty", name, streamLoggerAccess)
	}
	return nil
}

// streamLoggers returns the loggers that the close entry of the stream
// established by req is written to.
func (h *Handler) streamLoggers(req *http.Request) []*zap.Logger {
	if h.StreamLogs == nil || h.StreamLogs.LoggerName != streamLoggerAccess {
		return []*zap.Logger{h.logger}
	}
	server, ok := req.Context().Value(caddyhttp.ServerCtxKey).(*caddyhttp.Server)
	if !ok {
		return nil
	}
	// the access log of the request is decided by the request as the client
	// sent it, which can differ from the one sent to the backend (for
	// example in its Host header)
	if orig, ok := req.Context().Value(caddyhttp.OriginalRequestCtxKey).(http.Request); ok {
		req = &orig
	}
	return server.AccessLoggers(req)
}

// streamLevel returns the level of the stream close log entry.
func (h *Handler) streamLevel() zapcore.Level {
	lvl := zapcore.DebugLevel
	if h.StreamLogs != nil && h.StreamLogs.Level != "" {
		_ = lvl.UnmarshalText([]byte(h.StreamLogs.Level)) // validated when provisioning
	}
	return lvl
}

// Results of a stream, as reported by metrics and in the stream close log.
const (
	// both directions ended on their own
	streamResultCompleted = "completed"
	// a copy failed, or a connection broke
	streamResultError = "error"
	// the stream reached stream_timeout
	streamResultTimeout = "timeout"
	// the client request was canceled
	streamResultCanceled = "canceled"
	// closed because the config that started the stream was unloaded
	streamResultCleanup = "cleanup"
	// closed because its upstream is not part of the config anymore
	streamResultUpstreamRemoved = "upstream_removed"
)

// streamStats accounts for one stream. It is created when the stream is
// established and finish must be called when it is over; whatever the number
// of times and the order in which things end, the stream is counted once.
type streamStats struct {
	upstream string
	start    time.Time
	loggers  []*zap.Logger // where the stream close entry is written
	level    zapcore.Level

	toBackend   atomic.Int64
	fromBackend atomic.Int64

	mu     sync.Mutex
	result string
	once   sync.Once
}

// newStreamStats starts accounting for a stream to upstream. The stream close
// entry is written to loggers at the given level.
func newStreamStats(upstream string, loggers []*zap.Logger, level zapcore.Level) *streamStats {
	streamMetrics.opened(upstream)
	return &streamStats{upstream: upstream, start: time.Now(), loggers: loggers, level: level}
}

// setResult records why the stream ended. Only the first result is kept,
// so whoever causes the end of the stream must call it before the closes
// it provokes are observed by the stream itself.
func (s *streamStats) setResult(result string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.result == "" {
		s.result = result
	}
}

// finish completes the accounting of the stream: metrics, and the stream
// close log entry. Only the first call has an effect.
func (s *streamStats) finish() {
	s.once.Do(func() {
		s.setResult(streamResultCompleted)
		s.mu.Lock()
		result := s.result
		s.mu.Unlock()

		duration := time.Since(s.start)
		toBackend, fromBackend := s.toBackend.Load(), s.fromBackend.Load()
		streamMetrics.finished(s.upstream, result, duration, toBackend, fromBackend)

		for _, logger := range s.loggers {
			if c := logger.Check(s.level, "stream closed"); c != nil {
				c.Write(
					zap.String("upstream", s.upstream),
					zap.String("result", result),
					zap.Duration("duration", duration),
					zap.Int64("bytes_to_backend", toBackend),
					zap.Int64("bytes_from_backend", fromBackend),
				)
			}
		}
	})
}

// detachedStream is a stream that is not owned by the Handler which started it.
type detachedStream struct {
	conns []openConnection
	stats *streamStats
}

// close ends the stream with the given result.
func (s *detachedStream) close(result string) error {
	s.stats.setResult(result)
	return closeOpenConnections(s.conns)
}

// closeOpenConnections closes conns, politely first if they know how, and
// returns the first error.
func closeOpenConnections(conns []openConnection) error {
	var err error
	for _, oc := range conns {
		if oc.gracefulClose != nil {
			if gracefulErr := oc.gracefulClose(); gracefulErr != nil && err == nil {
				err = gracefulErr
			}
		}
		if closeErr := oc.conn.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

// detachedRegistry keeps the detached streams (see Handler.StreamDetached),
// by upstream. Detached streams must outlive the config, and thus the
// Handler, that started them, so the registry is process-wide. It is what
// lets a later config close the streams whose upstream it removed.
type detachedRegistry struct {
	mu      sync.Mutex
	streams map[string]map[*detachedStream]struct{}
}

// detachedStreams is the registry of all detached streams of the process.
var detachedStreams = &detachedRegistry{streams: make(map[string]map[*detachedStream]struct{})}

// add registers s as a stream to upstream. The returned function removes
// it again; it can be called several times.
func (r *detachedRegistry) add(upstream string, s *detachedStream) (remove func()) {
	r.mu.Lock()
	set := r.streams[upstream]
	if set == nil {
		set = make(map[*detachedStream]struct{})
		r.streams[upstream] = set
	}
	set[s] = struct{}{}
	r.mu.Unlock()

	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if set := r.streams[upstream]; set != nil {
			delete(set, s)
			if len(set) == 0 {
				delete(r.streams, upstream)
			}
		}
	}
}

// count returns the number of registered streams to upstream.
func (r *detachedRegistry) count(upstream string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.streams[upstream])
}

// closeUpstream closes all the registered streams to upstream, which is
// not part of the configuration anymore, and returns how many it closed
// and the first error.
func (r *detachedRegistry) closeUpstream(upstream string) (int, error) {
	r.mu.Lock()
	set := r.streams[upstream]
	delete(r.streams, upstream)
	r.mu.Unlock()

	var err error
	for s := range set {
		if closeErr := s.close(streamResultUpstreamRemoved); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return len(set), err
}

// upstreamKey returns the key the streams of the proxied response res are
// registered and accounted under: the upstream the request was sent to.
func upstreamKey(res *http.Response) string {
	if res.Request == nil {
		return ""
	}
	if di, ok := GetDialInfo(res.Request.Context()); ok {
		if di.Upstream != nil {
			return di.Upstream.String()
		}
		return di.Address
	}
	return ""
}
