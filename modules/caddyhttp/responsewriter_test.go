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
	"go.uber.org/zap/zapcore"
)

func TestServerLogStream(t *testing.T) {
	newReq := func(vars map[string]any) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "http://example.com/ws", nil)
		if vars != nil {
			r = r.WithContext(context.WithValue(r.Context(), VarsCtxKey, vars))
		}
		return r
	}

	t.Run("access logging disabled", func(t *testing.T) {
		s := &Server{}
		if s.LogStream(newReq(nil), zapcore.DebugLevel, "stream closed") {
			t.Error("LogStream() = true on a server without access logs")
		}
	})

	t.Run("skipped host", func(t *testing.T) {
		var buf syncBuffer
		s := &Server{accessLogger: testLogger(buf.Write), Logs: &ServerLogConfig{SkipHosts: []string{"example.com"}}}
		if s.LogStream(newReq(nil), zapcore.DebugLevel, "stream closed") {
			t.Error("LogStream() = true for a host that is skipped")
		}
		if buf.String() != "" {
			t.Errorf("logged: %s", buf.String())
		}
	})

	t.Run("logged at the given level", func(t *testing.T) {
		var buf syncBuffer
		s := &Server{accessLogger: testLogger(buf.Write), Logs: &ServerLogConfig{}}
		if !s.LogStream(newReq(nil), zapcore.WarnLevel, "stream closed", zap.String("result", "completed")) {
			t.Fatal("LogStream() = false")
		}
		out := buf.String()
		for _, want := range []string{`"level":"warn"`, `"msg":"stream closed"`, `"result":"completed"`} {
			if !strings.Contains(out, want) {
				t.Errorf("log %s does not contain %s", out, want)
			}
		}
	})

	t.Run("handshake skipped from the access log", func(t *testing.T) {
		var buf syncBuffer
		s := &Server{accessLogger: testLogger(buf.Write), Logs: &ServerLogConfig{}}
		skipped := newReq(map[string]any{LogSkipVar: true})
		if !s.LogStream(skipped, zapcore.DebugLevel, "stream closed") {
			t.Fatal("LogStream() = false for a request flagged to skip its access log entry")
		}
		if !strings.Contains(buf.String(), `"msg":"stream closed"`) {
			t.Errorf("stream close not logged: %q", buf.String())
		}
	})
}

// hijackSpyWriter is a response writer that can be hijacked and notes
// the header and body writes that reach it, and whether they came
// after the hijack.
type hijackSpyWriter struct {
	header      http.Header
	hijackErr   error
	hijacked    bool
	statuses    []int
	body        bytes.Buffer
	afterHijack int
	flushes     int
	raw, peer   net.Conn
}

func newHijackSpyWriter() *hijackSpyWriter {
	return &hijackSpyWriter{header: make(http.Header)}
}

func (h *hijackSpyWriter) Header() http.Header { return h.header }

func (h *hijackSpyWriter) WriteHeader(code int) {
	if h.hijacked {
		h.afterHijack++
	}
	h.statuses = append(h.statuses, code)
}

func (h *hijackSpyWriter) Write(p []byte) (int, error) {
	if h.hijacked {
		h.afterHijack++
	}
	return h.body.Write(p)
}

func (h *hijackSpyWriter) Flush() { h.flushes++ }

func (h *hijackSpyWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h.hijackErr != nil {
		return nil, nil, h.hijackErr
	}
	h.hijacked = true
	h.raw, h.peer = net.Pipe()
	return h.raw, bufio.NewReadWriter(bufio.NewReader(h.raw), bufio.NewWriter(h.raw)), nil
}

func TestResponseRecorderSwitchingProtocolsIsFinal(t *testing.T) {
	for _, buffering := range []bool{false, true} {
		w := newHijackSpyWriter()
		rr := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool { return buffering })

		rr.WriteHeader(http.StatusSwitchingProtocols)
		// nothing can follow the final response
		rr.WriteHeader(http.StatusOK)

		if rr.Buffered() {
			t.Errorf("buffering=%v: a 101 was buffered", buffering)
		}
		if got := rr.Status(); got != http.StatusSwitchingProtocols {
			t.Errorf("buffering=%v: status = %d, want 101", buffering, got)
		}
		if len(w.statuses) != 1 || w.statuses[0] != http.StatusSwitchingProtocols {
			t.Errorf("buffering=%v: statuses written = %v, want [101]", buffering, w.statuses)
		}
		if err := rr.WriteResponse(); err != nil {
			t.Errorf("buffering=%v: WriteResponse() = %v", buffering, err)
		}
		if len(w.statuses) != 1 {
			t.Errorf("buffering=%v: WriteResponse() wrote again: %v", buffering, w.statuses)
		}
	}
}

func TestResponseRecorderNoWritesAfterHijack(t *testing.T) {
	w := newHijackSpyWriter()
	buf := new(bytes.Buffer)
	// asks for buffering, which a hijacked connection can't do
	rr := NewResponseRecorder(w, buf, func(int, http.Header) bool { return true })
	var readSize int
	rr.(*responseRecorder).setReadSize(&readSize)

	conn, brw, err := http.NewResponseController(rr).Hijack()
	if err != nil {
		t.Fatalf("Hijack() = %v", err)
	}
	if conn == nil || brw == nil {
		t.Fatal("Hijack() returned no connection")
	}
	defer conn.Close()

	if rr.Buffered() {
		t.Error("a hijacked recorder reports buffering")
	}

	rr.WriteHeader(http.StatusOK)
	if n, err := rr.Write([]byte("body")); n != 0 || !errors.Is(err, http.ErrHijacked) {
		t.Errorf("Write() = %d, %v; want 0, %v", n, err, http.ErrHijacked)
	}
	if n, err := rr.(io.ReaderFrom).ReadFrom(strings.NewReader("body")); n != 0 || !errors.Is(err, http.ErrHijacked) {
		t.Errorf("ReadFrom() = %d, %v; want 0, %v", n, err, http.ErrHijacked)
	}
	if err := rr.WriteResponse(); err != nil {
		t.Errorf("WriteResponse() = %v, want nil", err)
	}
	if err := http.NewResponseController(rr).Flush(); err != nil {
		t.Errorf("Flush() = %v, want nil", err)
	}

	if w.afterHijack != 0 {
		t.Errorf("%d header or body writes reached the response after the hijack", w.afterHijack)
	}
	if len(w.statuses) != 0 || w.body.Len() != 0 || buf.Len() != 0 {
		t.Errorf("something was written: statuses=%v body=%q buffer=%q", w.statuses, w.body.String(), buf.String())
	}
	if w.flushes != 0 {
		t.Errorf("%d flushes reached the response after the hijack", w.flushes)
	}
	if rr.Size() != 0 {
		t.Errorf("Size() = %d, want 0", rr.Size())
	}
}

func TestResponseRecorderFailedHijack(t *testing.T) {
	w := newHijackSpyWriter()
	w.hijackErr = errors.New("cannot hijack")
	rr := NewResponseRecorder(w, nil, nil)

	if _, _, err := http.NewResponseController(rr).Hijack(); !errors.Is(err, w.hijackErr) {
		t.Fatalf("Hijack() = %v, want %v", err, w.hijackErr)
	}

	// the response is still usable
	if _, err := rr.Write([]byte("body")); err != nil {
		t.Errorf("Write() after a failed hijack = %v", err)
	}
	if w.body.String() != "body" {
		t.Errorf("body = %q, want %q", w.body.String(), "body")
	}
}

// The wrapper that tracks the traffic of a hijacked connection can be
// bypassed by streams that copy bytes themselves.
func TestHijackedConnUnwrap(t *testing.T) {
	w := newHijackSpyWriter()
	rr := NewResponseRecorder(w, nil, nil)

	conn, _, err := http.NewResponseController(rr).Hijack()
	if err != nil {
		t.Fatalf("Hijack() = %v", err)
	}
	defer conn.Close()

	unwrapper, ok := conn.(interface{ UnwrapHijacked() net.Conn })
	if !ok {
		t.Fatalf("hijacked conn %T has no UnwrapHijacked()", conn)
	}
	if got := unwrapper.UnwrapHijacked(); got != w.raw {
		t.Errorf("UnwrapHijacked() = %v, want the connection that was hijacked", got)
	}

	// writing on the raw connection is not accounted by the recorder...
	go func() {
		buf := make([]byte, 5)
		_, _ = io.ReadFull(w.peer, buf)
	}()
	if _, err := w.raw.Write([]byte("hello")); err != nil {
		t.Fatalf("raw write: %v", err)
	}
	if rr.Size() != 0 {
		t.Errorf("Size() = %d after a raw write, want 0", rr.Size())
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
