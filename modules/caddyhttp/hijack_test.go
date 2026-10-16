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
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// After a hijack, nothing may be written to the response again: the 101 is
// written exactly once, and the recorder drops everything that follows.
func TestResponseRecorderNoWriteAfterHijack(t *testing.T) {
	type outcome struct {
		status          int
		buffered        bool
		writeErr        error
		readFromErr     error
		flushErr        error
		writeResponse   error
		headerReadable  bool
		hijackAgainFail bool
	}
	outc := make(chan outcome, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// a response that would be buffered, if it were not hijacked
		rec := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool { return true })

		rec.Header().Set("Upgrade", "echo")
		rec.Header().Set("Connection", "Upgrade")
		rec.WriteHeader(http.StatusSwitchingProtocols)

		conn, brw, err := http.NewResponseController(rec).Hijack()
		if err != nil {
			t.Errorf("hijack failed: %v", err)
			return
		}
		defer conn.Close()

		// everything below must be ignored
		rec.WriteHeader(http.StatusOK)
		rec.WriteHeader(http.StatusInternalServerError)
		var o outcome
		_, o.writeErr = rec.Write([]byte("must not be written"))
		_, o.readFromErr = rec.(io.ReaderFrom).ReadFrom(strings.NewReader("must not be written"))
		o.flushErr = http.NewResponseController(rec).Flush()
		o.writeResponse = rec.WriteResponse()
		o.status = rec.Status()
		o.buffered = rec.Buffered()
		o.headerReadable = rec.Header().Get("Upgrade") == "echo"
		_, _, hijackAgainErr := http.NewResponseController(rec).Hijack()
		o.hijackAgainFail = hijackAgainErr != nil
		outc <- o

		// the raw connection is what carries the data from now on
		_, _ = brw.WriteString("raw")
		_ = brw.Flush()
	}))
	defer srv.Close()

	cli, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer cli.Close()
	_ = cli.SetDeadline(time.Now().Add(10 * time.Second))

	if _, err := io.WriteString(cli, "GET / HTTP/1.1\r\nHost: test\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n"); err != nil {
		t.Fatalf("writing request failed: %v", err)
	}
	br := bufio.NewReader(cli)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("reading response failed: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}

	// the only thing after the 101 is what was written to the raw connection
	rest, _ := io.ReadAll(br)
	if string(rest) != "raw" {
		t.Errorf("data after the 101 = %q, want %q", rest, "raw")
	}

	select {
	case o := <-outc:
		if o.status != http.StatusSwitchingProtocols {
			t.Errorf("recorded status = %d, want 101", o.status)
		}
		if o.buffered {
			t.Error("a 101 must not be buffered")
		}
		for name, err := range map[string]error{
			"Write":         o.writeErr,
			"ReadFrom":      o.readFromErr,
			"Flush":         o.flushErr,
			"WriteResponse": o.writeResponse,
		} {
			if !errors.Is(err, http.ErrHijacked) {
				t.Errorf("%s error = %v, want %v", name, err, http.ErrHijacked)
			}
		}
		if !o.headerReadable {
			t.Error("response headers are not readable after the hijack")
		}
		if !o.hijackAgainFail {
			t.Error("hijacking twice must fail")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not finish")
	}
}

// A 101 is the final response: later status codes don't replace it, and it is
// never buffered even if the buffering function asks for it.
func TestResponseRecorderSwitchingProtocolsIsFinal(t *testing.T) {
	w := httptest.NewRecorder()
	called := false
	rec := NewResponseRecorder(w, new(bytes.Buffer), func(int, http.Header) bool {
		called = true
		return true
	})

	rec.WriteHeader(http.StatusSwitchingProtocols)
	rec.WriteHeader(http.StatusNotFound)

	if rec.Status() != http.StatusSwitchingProtocols {
		t.Errorf("status = %d, want 101", rec.Status())
	}
	if w.Code != http.StatusSwitchingProtocols {
		t.Errorf("underlying status = %d, want 101", w.Code)
	}
	if rec.Buffered() {
		t.Error("a 101 must not be buffered")
	}
	if called {
		t.Error("the buffering function must not decide about a 101")
	}
}

// When the connection can't be hijacked the recorder keeps working normally.
func TestResponseRecorderHijackFailure(t *testing.T) {
	w := httptest.NewRecorder()
	rec := NewResponseRecorder(w, nil, nil)

	if _, _, err := http.NewResponseController(rec).Hijack(); err == nil {
		t.Fatal("hijacking a recorder over a non-hijackable writer should fail")
	}

	rec.WriteHeader(http.StatusTeapot)
	if _, err := rec.Write([]byte("short and stout")); err != nil {
		t.Fatalf("write failed after a failed hijack: %v", err)
	}
	if w.Code != http.StatusTeapot || w.Body.String() != "short and stout" {
		t.Errorf("got %d %q, want 418 %q", w.Code, w.Body.String(), "short and stout")
	}
}

func TestHijackedResponseWriter(t *testing.T) {
	header := http.Header{"X-Test": []string{"1"}}
	w := hijackedResponseWriter{header: header}

	if w.Header().Get("X-Test") != "1" {
		t.Error("header map is not kept")
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("x")); !errors.Is(err, http.ErrHijacked) {
		t.Errorf("Write error = %v, want %v", err, http.ErrHijacked)
	}
	if err := w.FlushError(); !errors.Is(err, http.ErrHijacked) {
		t.Errorf("FlushError error = %v, want %v", err, http.ErrHijacked)
	}
	if _, err := w.ReadFrom(strings.NewReader("x")); !errors.Is(err, http.ErrHijacked) {
		t.Errorf("ReadFrom error = %v, want %v", err, http.ErrHijacked)
	}
}

func TestServerAccessLoggers(t *testing.T) {
	newReq := func(host string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "http://"+host+"/", nil)
		r.Host = host
		return r
	}

	core, logs := observer.New(zapcore.DebugLevel)
	base := zap.New(core).Named("access")

	for i, tc := range []struct {
		name       string
		server     *Server
		host       string
		wantLogger []string // nil means not logged
	}{
		{
			name:   "no access logger",
			server: &Server{Logs: &ServerLogConfig{}},
			host:   "example.com",
		},
		{
			name:   "no log config",
			server: &Server{accessLogger: base},
			host:   "example.com",
		},
		{
			name:       "default logger",
			server:     &Server{accessLogger: base, Logs: &ServerLogConfig{}},
			host:       "example.com:8080",
			wantLogger: []string{"access"},
		},
		{
			name: "named loggers",
			server: &Server{accessLogger: base, Logs: &ServerLogConfig{
				LoggerNames: map[string]StringArray{"example.com": {"one", "two"}},
			}},
			host:       "example.com",
			wantLogger: []string{"access.one", "access.two"},
		},
		{
			name: "skipped host",
			server: &Server{accessLogger: base, Logs: &ServerLogConfig{
				SkipHosts: []string{"example.com"},
			}},
			host: "example.com",
		},
	} {
		loggers := tc.server.AccessLoggers(newReq(tc.host))
		if len(loggers) != len(tc.wantLogger) {
			t.Errorf("Test %d (%s): got %d loggers, want %d", i, tc.name, len(loggers), len(tc.wantLogger))
			continue
		}
		for j, l := range loggers {
			logs.TakeAll()
			l.Info("entry")
			entries := logs.TakeAll()
			if len(entries) != 1 || entries[0].LoggerName != tc.wantLogger[j] {
				t.Errorf("Test %d (%s): logger %d wrote %+v, want logger name %q", i, tc.name, j, entries, tc.wantLogger[j])
			}
		}
	}
}
