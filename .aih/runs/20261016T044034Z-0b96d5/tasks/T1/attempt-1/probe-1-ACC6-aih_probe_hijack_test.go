package caddyhttp

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAihProbeRecorderDropsWritesAfterHijack(t *testing.T) {
	done := make(chan error, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := NewResponseRecorder(w, nil, nil)
		rec.WriteHeader(http.StatusSwitchingProtocols)
		conn, _, err := http.NewResponseController(rec).Hijack()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		rec.WriteHeader(http.StatusTeapot)
		_, werr := rec.Write([]byte("LEAK"))
		ferr := http.NewResponseController(rec).Flush()
		if !errors.Is(werr, http.ErrHijacked) || !errors.Is(ferr, http.ErrHijacked) {
			done <- errors.New("write/flush not rejected")
			return
		}
		_, _ = conn.Write([]byte("raw"))
		done <- nil
	}))
	defer srv.Close()
	c, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.WriteString(c, "GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: e\r\n\r\n")
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, &http.Request{Method: "GET"})
	if err != nil || resp.StatusCode != 101 {
		t.Fatalf("resp %v %v", resp, err)
	}
	rest, _ := io.ReadAll(br)
	if string(rest) != "raw" {
		t.Fatalf("after 101: %q", rest)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
