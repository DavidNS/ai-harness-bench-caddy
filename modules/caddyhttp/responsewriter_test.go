package caddyhttp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
)

// hijackRespWriter is an http.ResponseWriter that can be hijacked and records
// everything written to it. The connection it hands out is one end of a pipe.
type hijackRespWriter struct {
	header   http.Header
	statuses []int
	body     bytes.Buffer
	hijacked bool
	conn     net.Conn // the end handed out by Hijack
	peer     net.Conn // the other end
}

func newHijackRespWriter() *hijackRespWriter {
	conn, peer := net.Pipe()
	return &hijackRespWriter{header: make(http.Header), conn: conn, peer: peer}
}

func (w *hijackRespWriter) Header() http.Header         { return w.header }
func (w *hijackRespWriter) WriteHeader(statusCode int)  { w.statuses = append(w.statuses, statusCode) }
func (w *hijackRespWriter) Write(d []byte) (int, error) { return w.body.Write(d) }

func (w *hijackRespWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.hijacked = true
	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
}

func TestResponseRecorderHijackStopsWrites(t *testing.T) {
	w := newHijackRespWriter()
	defer w.conn.Close()
	defer w.peer.Close()

	// would buffer everything, if the recorder did not know better
	rec := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool { return true })
	rr := rec.(*responseRecorder)

	if _, _, err := rr.Hijack(); err != nil {
		t.Fatalf("Hijack() error = %v", err)
	}
	if !w.hijacked {
		t.Fatal("the underlying writer was not hijacked")
	}
	if rec.Buffered() {
		t.Error("a hijacked response must not be reported as buffered")
	}

	rec.WriteHeader(http.StatusTeapot)
	if len(w.statuses) != 0 {
		t.Errorf("WriteHeader() after Hijack() wrote statuses %v", w.statuses)
	}
	if n, err := rec.Write([]byte("late")); n != 0 || !errors.Is(err, http.ErrHijacked) {
		t.Errorf("Write() after Hijack() = %d, %v; want 0, %v", n, err, http.ErrHijacked)
	}
	if n, err := rr.ReadFrom(strings.NewReader("late")); n != 0 || !errors.Is(err, http.ErrHijacked) {
		t.Errorf("ReadFrom() after Hijack() = %d, %v; want 0, %v", n, err, http.ErrHijacked)
	}
	if err := rr.FlushError(); !errors.Is(err, http.ErrHijacked) {
		t.Errorf("FlushError() after Hijack() = %v, want %v", err, http.ErrHijacked)
	}
	if err := rec.WriteResponse(); err != nil {
		t.Errorf("WriteResponse() after Hijack() = %v, want nil", err)
	}
	if len(w.statuses) != 0 || w.body.Len() != 0 {
		t.Errorf("the hijacked writer was written to: statuses %v, body %q", w.statuses, w.body.String())
	}
	if rec.Size() != 0 {
		t.Errorf("Size() = %d, want 0", rec.Size())
	}
}

func TestResponseRecorderHijackFailureKeepsRecording(t *testing.T) {
	// a writer that cannot be hijacked
	w := &baseRespWriter{}
	rec := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool { return true })
	rr := rec.(*responseRecorder)

	if _, _, err := rr.Hijack(); err == nil {
		t.Fatal("Hijack() on a writer that is not a Hijacker succeeded")
	}
	if _, err := rec.Write([]byte("buffered")); err != nil {
		t.Fatalf("Write() after failed Hijack() error = %v", err)
	}
	if !rec.Buffered() || rec.Buffer().String() != "buffered" {
		t.Errorf("after a failed Hijack() the recorder must keep recording, got buffered=%v body=%q", rec.Buffered(), rec.Buffer().String())
	}
}

func TestResponseRecorderSwitchingProtocolsIsFinal(t *testing.T) {
	w := newHijackRespWriter()
	defer w.conn.Close()
	defer w.peer.Close()

	buffered := false
	rec := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool {
		buffered = true
		return true
	})

	rec.WriteHeader(http.StatusSwitchingProtocols)
	rec.WriteHeader(http.StatusOK) // nothing follows a 101

	if buffered {
		t.Error("shouldBuffer was consulted for a 101 response")
	}
	if rec.Buffered() {
		t.Error("a 101 response must not be buffered")
	}
	if rec.Status() != http.StatusSwitchingProtocols {
		t.Errorf("Status() = %d, want 101", rec.Status())
	}
	if len(w.statuses) != 1 || w.statuses[0] != http.StatusSwitchingProtocols {
		t.Errorf("statuses written = %v, want exactly one 101", w.statuses)
	}
}

func TestUnwrapHijackedConn(t *testing.T) {
	w := newHijackRespWriter()
	defer w.conn.Close()
	defer w.peer.Close()

	rec := NewResponseRecorder(w, nil, nil)
	conn, _, err := rec.(*responseRecorder).Hijack()
	if err != nil {
		t.Fatalf("Hijack() error = %v", err)
	}
	if conn == w.conn {
		t.Fatal("the recorder is expected to wrap the hijacked connection")
	}
	if got := UnwrapHijackedConn(conn); got != w.conn {
		t.Errorf("UnwrapHijackedConn() = %v, want the raw connection", got)
	}
	if got := UnwrapHijackedConn(w.peer); got != w.peer {
		t.Errorf("UnwrapHijackedConn() of a plain connection = %v, want it unchanged", got)
	}
}

// Stream bytes moved on the raw connection don't count as response size,
// those written to the wrapper handed out by the recorder do.
func TestResponseRecorderSizeIgnoresRawConn(t *testing.T) {
	w := newHijackRespWriter()
	defer w.conn.Close()
	defer w.peer.Close()

	rec := NewResponseRecorder(w, nil, nil)
	conn, _, err := rec.(*responseRecorder).Hijack()
	if err != nil {
		t.Fatalf("Hijack() error = %v", err)
	}

	go io.Copy(io.Discard, w.peer)

	if _, err := UnwrapHijackedConn(conn).Write([]byte("stream bytes")); err != nil {
		t.Fatalf("raw write error = %v", err)
	}
	if rec.Size() != 0 {
		t.Errorf("Size() after raw write = %d, want 0", rec.Size())
	}
	if _, err := conn.Write([]byte("wrapped")); err != nil {
		t.Fatalf("wrapped write error = %v", err)
	}
	if rec.Size() != len("wrapped") {
		t.Errorf("Size() after wrapped write = %d, want %d", rec.Size(), len("wrapped"))
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
