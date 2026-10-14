package caddyhttp

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

// hijackableWriter is a http.ResponseWriter that can be hijacked, and
// records what is written to it.
type hijackableWriter struct {
	header   http.Header
	statuses []int
	body     bytes.Buffer
	conn     net.Conn
}

func (w *hijackableWriter) Header() http.Header         { return w.header }
func (w *hijackableWriter) WriteHeader(statusCode int)  { w.statuses = append(w.statuses, statusCode) }
func (w *hijackableWriter) Write(p []byte) (int, error) { return w.body.Write(p) }
func (w *hijackableWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
}

func TestResponseRecorderNeverBuffers101(t *testing.T) {
	for i, tc := range []struct {
		name         string
		shouldBuffer ShouldBufferFunc
	}{
		{name: "no buffering function"},
		{name: "never buffering", shouldBuffer: func(int, http.Header) bool { return false }},
		{name: "always buffering", shouldBuffer: func(int, http.Header) bool { return true }},
	} {
		w := &hijackableWriter{header: http.Header{}}
		var asked []int
		var shouldBuffer ShouldBufferFunc
		if tc.shouldBuffer != nil {
			shouldBuffer = func(status int, h http.Header) bool {
				asked = append(asked, status)
				return tc.shouldBuffer(status, h)
			}
		}
		rec := NewResponseRecorder(w, new(bytes.Buffer), shouldBuffer)

		rec.WriteHeader(http.StatusSwitchingProtocols)

		if rec.Buffered() {
			t.Errorf("Test %d (%s): the 101 was buffered", i, tc.name)
		}
		if rec.Status() != http.StatusSwitchingProtocols {
			t.Errorf("Test %d (%s): status = %d, want 101", i, tc.name, rec.Status())
		}
		if len(w.statuses) != 1 || w.statuses[0] != http.StatusSwitchingProtocols {
			t.Errorf("Test %d (%s): statuses written = %v, want [101]", i, tc.name, w.statuses)
		}
		if tc.shouldBuffer != nil && len(asked) != 1 {
			t.Errorf("Test %d (%s): the buffering function was asked %d times, want once (observers rely on it)", i, tc.name, len(asked))
		}

		// the 101 is final: another status is ignored
		rec.WriteHeader(http.StatusTeapot)
		if rec.Status() != http.StatusSwitchingProtocols || len(w.statuses) != 1 {
			t.Errorf("Test %d (%s): a second status was accepted: status %d, written %v", i, tc.name, rec.Status(), w.statuses)
		}
	}
}

func TestResponseRecorderRefusesWritesAfterHijack(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	w := &hijackableWriter{header: http.Header{}, conn: server}
	rec := NewResponseRecorder(w, new(bytes.Buffer), nil)

	rec.WriteHeader(http.StatusSwitchingProtocols)
	if _, _, err := http.NewResponseController(rec).Hijack(); err != nil {
		t.Fatalf("hijacking: %v", err)
	}

	rec.WriteHeader(http.StatusTeapot)
	if _, err := rec.Write([]byte("after")); !errors.Is(err, http.ErrHijacked) {
		t.Errorf("Write error = %v, want http.ErrHijacked", err)
	}
	if err := http.NewResponseController(rec).Flush(); !errors.Is(err, http.ErrHijacked) {
		t.Errorf("Flush error = %v, want http.ErrHijacked", err)
	}
	if err := rec.WriteResponse(); err != nil {
		t.Errorf("WriteResponse error = %v, want nil", err)
	}
	if len(w.statuses) != 1 || w.body.Len() != 0 {
		t.Errorf("the response was written to after the hijack: statuses %v, body %q", w.statuses, w.body.String())
	}
	if rec.Status() != http.StatusSwitchingProtocols {
		t.Errorf("status = %d, want 101", rec.Status())
	}
}

func TestUnwrapHijackedConn(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	// without a recorder, the connection is returned as is
	if raw, account := UnwrapHijackedConn(server); raw != server {
		t.Error("an unwrapped connection was not returned as is")
	} else {
		account(1, 1) // must not panic
	}

	w := &hijackableWriter{header: http.Header{}, conn: server}
	rec := NewResponseRecorder(w, nil, nil)
	var readSize int
	rec.(interface{ setReadSize(*int) }).setReadSize(&readSize)

	conn, _, err := http.NewResponseController(rec).Hijack()
	if err != nil {
		t.Fatal(err)
	}
	if conn == server {
		t.Fatal("the hijacked connection is not wrapped by the recorder")
	}

	raw, account := UnwrapHijackedConn(conn)
	if raw != server {
		t.Error("the raw connection was not returned")
	}

	account(5, 7)
	if readSize != 5 {
		t.Errorf("read size = %d, want 5", readSize)
	}
	if rec.Size() != 7 {
		t.Errorf("size = %d, want 7", rec.Size())
	}
}

func TestAccessLoggerNames(t *testing.T) {
	slc := ServerLogConfig{
		DefaultLoggerName: "default",
		LoggerNames: map[string]StringArray{
			"example.com": {"example", ""},
			"*.test":      {"wild"},
		},
	}

	for i, tc := range []struct {
		host string
		want []string
	}{
		{host: "example.com", want: []string{"example", ""}},
		{host: "example.com:8443", want: []string{"example", ""}},
		{host: "a.test", want: []string{"wild"}},
		{host: "other.org", want: []string{"default"}},
	} {
		req := httptest.NewRequest(http.MethodGet, "http://"+tc.host+"/", nil)
		got := slc.AccessLoggerNames(req)
		if len(got) != len(tc.want) {
			t.Errorf("Test %d (%s): names = %q, want %q", i, tc.host, got, tc.want)
			continue
		}
		for j := range got {
			if got[j] != tc.want[j] {
				t.Errorf("Test %d (%s): names = %q, want %q", i, tc.host, got, tc.want)
			}
		}
	}
}
