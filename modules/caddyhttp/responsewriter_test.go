package caddyhttp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// hijackSpy is a http.ResponseWriter that records what is written to it
// and can be hijacked.
type hijackSpy struct {
	header   http.Header
	statuses []int
	body     []byte
	peer     net.Conn
}

func (s *hijackSpy) Header() http.Header {
	if s.header == nil {
		s.header = make(http.Header)
	}
	return s.header
}
func (s *hijackSpy) WriteHeader(statusCode int) { s.statuses = append(s.statuses, statusCode) }
func (s *hijackSpy) Write(d []byte) (int, error) {
	s.body = append(s.body, d...)
	return len(d), nil
}

func (s *hijackSpy) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, peer := net.Pipe()
	s.peer = peer
	return conn, bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)), nil
}

func TestResponseRecorderSwitchingProtocolsIsFinal(t *testing.T) {
	spy := &hijackSpy{}
	shouldBufferCalled := false
	rr := NewResponseRecorder(spy, new(bytes.Buffer), func(int, http.Header) bool {
		shouldBufferCalled = true
		return true
	})

	rr.WriteHeader(http.StatusSwitchingProtocols)
	// a 101 is the final response: a later status must not replace it
	rr.WriteHeader(http.StatusOK)

	if shouldBufferCalled {
		t.Error("shouldBuffer was consulted for a 101")
	}
	if rr.Buffered() {
		t.Error("a 101 must not be buffered")
	}
	if rr.Status() != http.StatusSwitchingProtocols {
		t.Errorf("Status() = %d, want 101", rr.Status())
	}
	if len(spy.statuses) != 1 || spy.statuses[0] != http.StatusSwitchingProtocols {
		t.Errorf("underlying statuses = %v, want [101]", spy.statuses)
	}
}

func TestResponseRecorderNoWritesAfterHijack(t *testing.T) {
	spy := &hijackSpy{}
	rr := NewResponseRecorder(spy, new(bytes.Buffer), func(int, http.Header) bool { return true })
	rr.WriteHeader(http.StatusSwitchingProtocols)

	hj, ok := rr.(http.Hijacker)
	if !ok {
		t.Fatal("recorder is not a http.Hijacker")
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		t.Fatalf("Hijack() error = %v", err)
	}
	defer conn.Close()
	defer spy.peer.Close()

	rr.WriteHeader(http.StatusInternalServerError)
	if n, err := rr.Write([]byte("late")); n != 0 || !errors.Is(err, http.ErrHijacked) {
		t.Errorf("Write() = %d, %v; want 0, ErrHijacked", n, err)
	}
	src := struct{ io.Reader }{strings.NewReader("late")}
	if n, err := io.Copy(rr, src); n != 0 || !errors.Is(err, http.ErrHijacked) {
		t.Errorf("ReadFrom() = %d, %v; want 0, ErrHijacked", n, err)
	}
	if err := rr.WriteResponse(); err != nil {
		t.Errorf("WriteResponse() error = %v", err)
	}
	if err := http.NewResponseController(rr).Flush(); err != nil {
		t.Errorf("Flush() error = %v", err)
	}

	if len(spy.statuses) != 1 {
		t.Errorf("underlying statuses = %v, want only the 101", spy.statuses)
	}
	if len(spy.body) != 0 {
		t.Errorf("underlying body = %q, want nothing", spy.body)
	}
	if rr.Status() != http.StatusSwitchingProtocols {
		t.Errorf("Status() = %d, want 101", rr.Status())
	}
}

func TestResponseRecorderHijackWithoutStatusWritesNoStatus(t *testing.T) {
	spy := &hijackSpy{}
	rr := NewResponseRecorder(spy, new(bytes.Buffer), nil)

	conn, _, err := rr.(http.Hijacker).Hijack()
	if err != nil {
		t.Fatalf("Hijack() error = %v", err)
	}
	defer conn.Close()
	defer spy.peer.Close()

	rr.WriteHeader(http.StatusOK)
	if len(spy.statuses) != 0 {
		t.Errorf("underlying statuses = %v, want none after hijack", spy.statuses)
	}
}

func TestServerAccessLoggers(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)

	// access logging disabled
	if got := (&Server{}).AccessLoggers(r); got != nil {
		t.Errorf("AccessLoggers() without logging = %v, want nil", got)
	}

	logger := testLogger(func(p []byte) (int, error) { return len(p), nil })
	s := &Server{accessLogger: logger, Logs: &ServerLogConfig{}}
	if got := s.AccessLoggers(r); len(got) != 1 || got[0] != logger {
		t.Errorf("AccessLoggers() = %v, want the access logger", got)
	}

	s.Logs = &ServerLogConfig{LoggerNames: map[string]StringArray{"example.com": {"custom"}}}
	got := s.AccessLoggers(r)
	if len(got) != 1 || got[0].Name() != "custom" {
		t.Errorf("AccessLoggers() with a logger name = %v, want the logger named custom", got)
	}
}

func TestResponseRecorderFailedHijackKeepsWriting(t *testing.T) {
	spy := &baseRespWriter{}
	rr := NewResponseRecorder(spy, nil, nil)

	if _, _, err := rr.(http.Hijacker).Hijack(); err == nil {
		t.Fatal("expected Hijack() to fail on a non-hijackable writer")
	}
	if _, err := rr.Write([]byte("ok")); err != nil {
		t.Errorf("Write() after failed hijack error = %v", err)
	}
	if got := spy.Written(); got != "ok" {
		t.Errorf("written = %q, want %q", got, "ok")
	}
}

type responseWriterSpy interface {
	http.ResponseWriter
	Written() string
	CalledReadFrom() bool
}

var (
	_ responseWriterSpy = (*baseRespWriter)(nil)
	_ responseWriterSpy = (*readFromRespWriter)(nil)
)

// a barebones http.ResponseWriter mock
type baseRespWriter []byte

func (brw *baseRespWriter) Write(d []byte) (int, error) {
	*brw = append(*brw, d...)
	return len(d), nil
}
func (brw *baseRespWriter) Header() http.Header        { return nil }
func (brw *baseRespWriter) WriteHeader(statusCode int) {}
func (brw *baseRespWriter) Written() string            { return string(*brw) }
func (brw *baseRespWriter) CalledReadFrom() bool       { return false }

// an http.ResponseWriter mock that supports ReadFrom
type readFromRespWriter struct {
	baseRespWriter
	called bool
}

func (rf *readFromRespWriter) ReadFrom(r io.Reader) (int64, error) {
	rf.called = true
	return io.Copy(&rf.baseRespWriter, r)
}

func (rf *readFromRespWriter) CalledReadFrom() bool { return rf.called }

func TestResponseWriterWrapperReadFrom(t *testing.T) {
	tests := map[string]struct {
		responseWriter responseWriterSpy
		wantReadFrom   bool
	}{
		"no ReadFrom": {
			responseWriter: &baseRespWriter{},
			wantReadFrom:   false,
		},
		"has ReadFrom": {
			responseWriter: &readFromRespWriter{},
			wantReadFrom:   true,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			// what we expect middlewares to do:
			type myWrapper struct {
				*ResponseWriterWrapper
			}

			wrapped := myWrapper{
				ResponseWriterWrapper: &ResponseWriterWrapper{ResponseWriter: tt.responseWriter},
			}

			const srcData = "boo!"
			// hides everything but Read, since strings.Reader implements WriteTo it would
			// take precedence over our ReadFrom.
			src := struct{ io.Reader }{strings.NewReader(srcData)}

			if _, err := io.Copy(wrapped, src); err != nil {
				t.Errorf("%s: Copy() err = %v", name, err)
			}

			if got := tt.responseWriter.Written(); got != srcData {
				t.Errorf("%s: data = %q, want %q", name, got, srcData)
			}

			if tt.responseWriter.CalledReadFrom() != tt.wantReadFrom {
				if tt.wantReadFrom {
					t.Errorf("%s: ReadFrom() should have been called", name)
				} else {
					t.Errorf("%s: ReadFrom() should not have been called", name)
				}
			}
		})
	}
}

func TestResponseWriterWrapperUnwrap(t *testing.T) {
	w := &ResponseWriterWrapper{&baseRespWriter{}}

	if _, ok := w.Unwrap().(*baseRespWriter); !ok {
		t.Errorf("Unwrap() doesn't return the underlying ResponseWriter")
	}
}

func TestResponseRecorderReadFrom(t *testing.T) {
	tests := map[string]struct {
		responseWriter responseWriterSpy
		shouldBuffer   bool
		wantReadFrom   bool
	}{
		"buffered plain": {
			responseWriter: &baseRespWriter{},
			shouldBuffer:   true,
			wantReadFrom:   false,
		},
		"streamed plain": {
			responseWriter: &baseRespWriter{},
			shouldBuffer:   false,
			wantReadFrom:   false,
		},
		"buffered ReadFrom": {
			responseWriter: &readFromRespWriter{},
			shouldBuffer:   true,
			wantReadFrom:   false,
		},
		"streamed ReadFrom": {
			responseWriter: &readFromRespWriter{},
			shouldBuffer:   false,
			wantReadFrom:   true,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer

			rr := NewResponseRecorder(tt.responseWriter, &buf, func(status int, header http.Header) bool {
				return tt.shouldBuffer
			})

			const srcData = "boo!"
			// hides everything but Read, since strings.Reader implements WriteTo it would
			// take precedence over our ReadFrom.
			src := struct{ io.Reader }{strings.NewReader(srcData)}

			if _, err := io.Copy(rr, src); err != nil {
				t.Errorf("Copy() err = %v", err)
			}

			wantStreamed := srcData
			wantBuffered := ""
			if tt.shouldBuffer {
				wantStreamed = ""
				wantBuffered = srcData
			}

			if got := tt.responseWriter.Written(); got != wantStreamed {
				t.Errorf("streamed data = %q, want %q", got, wantStreamed)
			}
			if got := buf.String(); got != wantBuffered {
				t.Errorf("buffered data = %q, want %q", got, wantBuffered)
			}

			if tt.responseWriter.CalledReadFrom() != tt.wantReadFrom {
				if tt.wantReadFrom {
					t.Errorf("ReadFrom() should have been called")
				} else {
					t.Errorf("ReadFrom() should not have been called")
				}
			}
		})
	}
}
