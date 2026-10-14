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
	"sync"
	"sync/atomic"
	"time"
)

// StreamLogs configures the logging of upgraded streams, such as
// WebSockets. When the server has access logging enabled, a log entry
// is written to the access logger when a stream is closed.
type StreamLogs struct {
	// If true, the access log entry of the request that upgraded to
	// a stream (the handshake) is not written. Stream close logs are
	// not affected. Default: the handshake is logged.
	SkipHandshake bool `json:"skip_handshake,omitempty"`

	// The level of the stream close log entries: debug, info, warn
	// or error. Default: debug.
	Level string `json:"level,omitempty"`
}

// Results with which a stream can end.
const (
	streamResultCompleted = "completed" // both directions ended cleanly
	streamResultError     = "error"     // a copy failed, e.g. a peer went away
	streamResultTimeout   = "timeout"   // stream_timeout elapsed
	streamResultClosed    = "closed"    // closed by a config reload or shutdown
)

// streamAccounting accounts for a single upgraded stream in the stream
// metrics. However many paths try to end the stream, it is counted once.
type streamAccounting struct {
	start        time.Time
	toBackend    atomic.Int64
	fromBackend  atomic.Int64
	mu           sync.Mutex
	result       string
	finishedOnce sync.Once
}

// newStreamAccounting starts accounting for a stream that was just established.
func newStreamAccounting() *streamAccounting {
	streamMetrics.active.Inc()
	return &streamAccounting{start: time.Now()}
}

// setResult records why the stream is ending. Only the first
// result is kept, so the cause of the end wins over its effects.
func (s *streamAccounting) setResult(result string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.result == "" {
		s.result = result
	}
}

// finish ends the accounting of the stream, using result unless an earlier
// one was recorded with setResult. Only the first call has an effect: it
// updates the metrics and reports true along with the final result.
func (s *streamAccounting) finish(result string) (string, bool) {
	var first bool
	s.finishedOnce.Do(func() {
		first = true
		s.setResult(result)
		s.mu.Lock()
		result = s.result
		s.mu.Unlock()

		streamMetrics.active.Dec()
		streamMetrics.closed.WithLabelValues(result).Inc()
		streamMetrics.duration.Observe(time.Since(s.start).Seconds())
		streamMetrics.bytes.WithLabelValues("to_backend").Add(float64(s.toBackend.Load()))
		streamMetrics.bytes.WithLabelValues("from_backend").Add(float64(s.fromBackend.Load()))
	})
	if !first {
		return "", false
	}
	return result, true
}

// streamRegistry tracks the upgraded streams that are detached from the
// lifecycle of the config that created them (stream_detached), as well as
// the upstreams of the handlers that are currently live. A detached stream
// stays open until its upstream is no longer live.
type streamRegistry struct {
	mu      sync.Mutex
	live    map[string]int // upstream -> number of live handlers using it
	streams map[*detachedStream]struct{}
}

type detachedStream struct {
	upstream string
	dynamic  bool
	close    func() error
}

// dynamicLiveKey marks a live handler whose upstreams are only known when
// requests are served (dynamic upstreams, or placeholders in a dial address).
// Streams started by such handlers stay open while any such handler is live.
const dynamicLiveKey = "\x00dynamic"

// detachedStreams is shared by all handlers, since the handlers of an old
// config are cleaned up while the streams they started are still running.
var detachedStreams = newStreamRegistry()

func newStreamRegistry() *streamRegistry {
	return &streamRegistry{
		live:    make(map[string]int),
		streams: make(map[*detachedStream]struct{}),
	}
}

// addLive marks the upstreams as used by a live handler.
func (r *streamRegistry) addLive(upstreams []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, u := range upstreams {
		r.live[u]++
	}
}

// removeLive marks the upstreams as no longer used by a handler.
func (r *streamRegistry) removeLive(upstreams []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, u := range upstreams {
		if r.live[u] <= 1 {
			delete(r.live, u)
			continue
		}
		r.live[u]--
	}
}

// add registers a stream to upstream that is closed with closeFn when the
// upstream is no longer live. It returns a function that unregisters the
// stream, or false if the upstream is not live anymore, in which case the
// stream was not registered and must be closed by the caller.
func (r *streamRegistry) add(upstream string, closeFn func() error) (remove func(), ok bool) {
	return r.addStream(upstream, false, closeFn)
}

// addStream is like add. A dynamic stream, started by a handler whose upstreams
// are not known up front, is also kept while a dynamic handler is live.
func (r *streamRegistry) addStream(upstream string, dynamic bool, closeFn func() error) (remove func(), ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.isLive(upstream, dynamic) {
		return nil, false
	}
	ds := &detachedStream{upstream: upstream, dynamic: dynamic, close: closeFn}
	r.streams[ds] = struct{}{}
	return func() {
		r.mu.Lock()
		delete(r.streams, ds)
		r.mu.Unlock()
	}, true
}

// closeOrphaned closes the streams whose upstream is not live anymore,
// and returns the first error encountered. The streams to retained
// upstreams are left alone.
func (r *streamRegistry) closeOrphaned() error {
	r.mu.Lock()
	var orphaned []*detachedStream
	for ds := range r.streams {
		if !r.isLive(ds.upstream, ds.dynamic) {
			orphaned = append(orphaned, ds)
			delete(r.streams, ds)
		}
	}
	r.mu.Unlock()

	// closing may block (graceful close writes to the connections), so don't hold the lock
	var err error
	for _, ds := range orphaned {
		if closeErr := ds.close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

// isLive reports whether a stream to upstream may stay open. The lock must be held.
func (r *streamRegistry) isLive(upstream string, dynamic bool) bool {
	return r.live[upstream] > 0 || (dynamic && r.live[dynamicLiveKey] > 0)
}

// count returns the number of registered streams.
func (r *streamRegistry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.streams)
}
