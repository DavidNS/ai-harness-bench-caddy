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

// a http.ResponseWriter mock that can be hijacked, and records what is
// written to it
type hijackableRespWriter struct {
	header   http.Header
	statuses []int
	body     bytes.Buffer
	conn     net.Conn
}

func (w *hijackableRespWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}
func (w *hijackableRespWriter) WriteHeader(statusCode int) { w.statuses = append(w.statuses, statusCode) }
func (w *hijackableRespWriter) Write(d []byte) (int, error) {
	return w.body.Write(d)
}
func (w *hijackableRespWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	var peer net.Conn
	w.conn, peer = net.Pipe()
	_ = peer
	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
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

// TestResponseRecorderNoWriteAfterHijack checks that once the connection is
// hijacked, the response is over: nothing written to the recorder reaches the
// underlying writer or the buffer.
func TestResponseRecorderNoWriteAfterHijack(t *testing.T) {
	w := new(hijackableRespWriter)
	var buf bytes.Buffer
	rec := NewResponseRecorder(w, &buf, func(int, http.Header) bool { return true })

	conn, _, err := http.NewResponseController(rec).Hijack()
	if err != nil {
		t.Fatalf("Hijack() err = %v", err)
	}
	defer conn.Close()

	if rec.Buffered() {
		t.Error("a hijacked response must not be buffered")
	}

	rec.WriteHeader(http.StatusOK)
	if _, err := rec.Write([]byte("late")); !errors.Is(err, http.ErrHijacked) {
		t.Errorf("Write() err = %v, want %v", err, http.ErrHijacked)
	}
	if _, err := rec.(io.ReaderFrom).ReadFrom(strings.NewReader("late")); !errors.Is(err, http.ErrHijacked) {
		t.Errorf("ReadFrom() err = %v, want %v", err, http.ErrHijacked)
	}
	if err := rec.WriteResponse(); err != nil {
		t.Errorf("WriteResponse() err = %v", err)
	}

	if len(w.statuses) != 0 {
		t.Errorf("statuses written after hijack: %v", w.statuses)
	}
	if w.body.Len() != 0 || buf.Len() != 0 {
		t.Errorf("body written after hijack: %q, buffered %q", w.body.String(), buf.String())
	}
	if rec.Status() != 0 || rec.Size() != 0 {
		t.Errorf("Status() = %d, Size() = %d after hijack, want 0 and 0", rec.Status(), rec.Size())
	}
}

// TestResponseRecorderFailedHijack checks that a hijack that fails leaves the
// recorder usable.
func TestResponseRecorderFailedHijack(t *testing.T) {
	w := httptest.NewRecorder() // cannot be hijacked
	rec := NewResponseRecorder(w, nil, nil)

	if _, _, err := http.NewResponseController(rec).Hijack(); !errors.Is(err, http.ErrNotSupported) {
		t.Fatalf("Hijack() err = %v, want %v", err, http.ErrNotSupported)
	}
	rec.WriteHeader(http.StatusTeapot)
	if _, err := rec.Write([]byte("short and stout")); err != nil {
		t.Errorf("Write() err = %v", err)
	}
	if w.Code != http.StatusTeapot || w.Body.String() != "short and stout" {
		t.Errorf("got %d %q, want the response to be written", w.Code, w.Body.String())
	}
}

// TestResponseRecorderSwitchingProtocolsFinal checks that a 101 is written
// at once, never buffered, and is the final response, unlike other 1xx.
func TestResponseRecorderSwitchingProtocolsFinal(t *testing.T) {
	for _, tc := range []struct {
		status       int
		wantBuffered bool
		wantStatuses []int
	}{
		{status: http.StatusSwitchingProtocols, wantBuffered: false, wantStatuses: []int{101}},
		{status: http.StatusEarlyHints, wantBuffered: true, wantStatuses: []int{103}},
	} {
		w := new(hijackableRespWriter)
		rec := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool { return true })

		rec.WriteHeader(tc.status)
		rec.WriteHeader(http.StatusOK) // dropped after a final response; buffered after an informational one

		if rec.Buffered() != tc.wantBuffered {
			t.Errorf("%d: Buffered() = %t, want %t", tc.status, rec.Buffered(), tc.wantBuffered)
		}
		if !equalInts(w.statuses, tc.wantStatuses) {
			t.Errorf("%d: statuses written = %v, want %v", tc.status, w.statuses, tc.wantStatuses)
		}
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestRawConn checks that the connection of a hijacked recorder is unwrapped
// to the one that was hijacked, and that other connections are left alone.
func TestRawConn(t *testing.T) {
	w := new(hijackableRespWriter)
	rec := NewResponseRecorder(w, nil, nil)
	conn, _, err := http.NewResponseController(rec).Hijack()
	if err != nil {
		t.Fatalf("Hijack() err = %v", err)
	}
	defer conn.Close()

	if conn == w.conn {
		t.Fatal("the recorder is expected to wrap the hijacked connection")
	}
	if got := RawConn(conn); got != w.conn {
		t.Errorf("RawConn(recorder conn) = %v, want the hijacked connection", got)
	}
	if got := RawConn(w.conn); got != w.conn {
		t.Errorf("RawConn(plain conn) = %v, want it unchanged", got)
	}
}
