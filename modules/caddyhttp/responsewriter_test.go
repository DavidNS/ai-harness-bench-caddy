package caddyhttp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"
)

// hijackableRespWriter is a ResponseWriter mock which records what reaches
// it and can be hijacked, handing out one end of a pipe.
type hijackableRespWriter struct {
	header   http.Header
	statuses []int
	body     bytes.Buffer
	flushes  int
	conn     net.Conn // the end of the pipe handed out by Hijack
	peer     net.Conn // the other end of the pipe
}

func newHijackableRespWriter() *hijackableRespWriter {
	conn, peer := net.Pipe()
	return &hijackableRespWriter{header: make(http.Header), conn: conn, peer: peer}
}

func (w *hijackableRespWriter) Header() http.Header { return w.header }
func (w *hijackableRespWriter) WriteHeader(statusCode int) {
	w.statuses = append(w.statuses, statusCode)
}
func (w *hijackableRespWriter) Write(p []byte) (int, error) { return w.body.Write(p) }
func (w *hijackableRespWriter) FlushError() error           { w.flushes++; return nil }
func (w *hijackableRespWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
}

// A 101 is the final response of an upgrade: it must not be buffered (even
// if the buffering decision says so), and no other status may follow it.
func TestResponseRecorderSwitchingProtocolsIsFinal(t *testing.T) {
	w := newHijackableRespWriter()
	asked := false
	rr := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool {
		asked = true
		return true
	})

	rr.WriteHeader(http.StatusSwitchingProtocols)
	rr.WriteHeader(http.StatusOK)

	if asked {
		t.Error("shouldBuffer was consulted for a 101")
	}
	if rr.Buffered() {
		t.Error("a 101 must not be buffered")
	}
	if rr.Status() != http.StatusSwitchingProtocols {
		t.Errorf("Status() = %d, want 101", rr.Status())
	}
	if len(w.statuses) != 1 || w.statuses[0] != http.StatusSwitchingProtocols {
		t.Errorf("statuses written = %v, want [101]", w.statuses)
	}
}

// Once the connection is hijacked, nothing may reach the response writer.
func TestResponseRecorderAfterHijack(t *testing.T) {
	w := newHijackableRespWriter()
	defer w.peer.Close()
	rr := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool { return true })

	conn, _, err := http.NewResponseController(rr).Hijack()
	if err != nil {
		t.Fatalf("Hijack() error = %v", err)
	}
	defer conn.Close()

	if rr.Buffered() {
		t.Error("a hijacked recorder must not report a buffered response")
	}

	rr.WriteHeader(http.StatusInternalServerError)
	if n, err := rr.Write([]byte("late")); n != 0 || !errors.Is(err, http.ErrHijacked) {
		t.Errorf("Write() = %d, %v; want 0, %v", n, err, http.ErrHijacked)
	}
	if n, err := rr.(io.ReaderFrom).ReadFrom(strings.NewReader("late")); n != 0 || !errors.Is(err, http.ErrHijacked) {
		t.Errorf("ReadFrom() = %d, %v; want 0, %v", n, err, http.ErrHijacked)
	}
	if err := http.NewResponseController(rr).Flush(); !errors.Is(err, http.ErrHijacked) {
		t.Errorf("Flush() error = %v, want %v", err, http.ErrHijacked)
	}
	if err := rr.WriteResponse(); err != nil {
		t.Errorf("WriteResponse() error = %v, want nil", err)
	}

	if len(w.statuses) != 0 || w.body.Len() != 0 || w.flushes != 0 {
		t.Errorf("response writer got statuses %v, body %q, %d flushes after hijack", w.statuses, w.body.String(), w.flushes)
	}
}

// Handlers reach the access loggers of a request through its context.
func TestAccessLoggers(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	if got := AccessLoggers(req); got != nil {
		t.Errorf("AccessLoggers() = %v for a request which is not access logged, want nil", got)
	}

	srv := &Server{Logs: &ServerLogConfig{LoggerNames: map[string]StringArray{"example.com": {"one", "two"}}}}
	req = req.WithContext(context.WithValue(req.Context(), accessLogCtxKey, accessLog{server: srv, logger: zap.NewNop()}))
	if got := AccessLoggers(req); len(got) != 2 {
		t.Errorf("AccessLoggers() returned %d loggers, want the 2 of the host", len(got))
	}
}

// A hijack that fails leaves the recorder usable.
func TestResponseRecorderFailedHijack(t *testing.T) {
	w := &baseRespWriter{}
	rr := NewResponseRecorder(w, new(bytes.Buffer), nil)

	if _, _, err := http.NewResponseController(rr).Hijack(); err == nil {
		t.Fatal("Hijack() on a writer which cannot hijack succeeded")
	}
	if _, err := rr.Write([]byte("ok")); err != nil {
		t.Errorf("Write() after failed hijack error = %v", err)
	}
	if got := w.Written(); got != "ok" {
		t.Errorf("written = %q, want %q", got, "ok")
	}
}

// The wrapper around a hijacked connection counts bytes for the access log;
// RawConn lets long-lived users bypass it.
func TestHijackedConnRawConn(t *testing.T) {
	w := newHijackableRespWriter()
	defer w.peer.Close()
	rr := NewResponseRecorder(w, nil, nil)

	conn, _, err := http.NewResponseController(rr).Hijack()
	if err != nil {
		t.Fatalf("Hijack() error = %v", err)
	}
	defer conn.Close()

	raw, ok := conn.(interface{ RawConn() net.Conn })
	if !ok {
		t.Fatalf("hijacked conn %T has no RawConn", conn)
	}
	if raw.RawConn() != w.conn {
		t.Fatalf("RawConn() = %v, want the connection which was hijacked", raw.RawConn())
	}

	go func() { _, _ = io.Copy(io.Discard, w.peer) }()
	if _, err := conn.Write([]byte("tracked")); err != nil {
		t.Fatal(err)
	}
	if rr.Size() != len("tracked") {
		t.Errorf("Size() = %d after a write on the wrapper, want %d", rr.Size(), len("tracked"))
	}
	if _, err := raw.RawConn().Write([]byte("untracked")); err != nil {
		t.Fatal(err)
	}
	if rr.Size() != len("tracked") {
		t.Errorf("Size() = %d after a write on the raw conn, want it unchanged at %d", rr.Size(), len("tracked"))
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
