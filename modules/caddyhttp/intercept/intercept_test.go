package intercept

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
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

func newTestIntercept() Intercept {
	return Intercept{
		// matches every response
		HandleResponse: []caddyhttp.ResponseHandler{{Routes: caddyhttp.RouteList{}}},
		logger:         zap.NewNop(),
	}
}

func newTestRequest() *http.Request {
	r := httptest.NewRequest("GET", "/", nil)
	return r.WithContext(context.WithValue(r.Context(), caddy.ReplacerCtxKey, caddy.NewReplacer()))
}

// A 101 is the final response of an upgrade: it must not be buffered even
// though a handle_response matches every response, and nothing may be
// written once the connection is hijacked.
func TestInterceptUpgradeIsNotBufferedNorWrittenAfterHijack(t *testing.T) {
	spy := &hijackSpy{}
	var writeErr error
	err := newTestIntercept().ServeHTTP(spy, newTestRequest(), caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		w.WriteHeader(http.StatusSwitchingProtocols)
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return err
		}
		defer conn.Close()
		defer spy.peer.Close()

		_, writeErr = w.Write([]byte("late"))
		return nil
	}))
	if err != nil {
		t.Fatalf("ServeHTTP error = %v", err)
	}
	if !errors.Is(writeErr, http.ErrHijacked) {
		t.Errorf("Write() after hijack error = %v, want ErrHijacked", writeErr)
	}
	if len(spy.statuses) != 1 || spy.statuses[0] != http.StatusSwitchingProtocols {
		t.Errorf("underlying statuses = %v, want [101]", spy.statuses)
	}
	if len(spy.body) != 0 {
		t.Errorf("underlying body = %q, want nothing", spy.body)
	}
}

// A hijacked connection must not be handled as a buffered response, even if
// no status was written before the hijack.
func TestInterceptHijackWithoutStatusWritesNothing(t *testing.T) {
	spy := &hijackSpy{}
	err := newTestIntercept().ServeHTTP(spy, newTestRequest(), caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return err
		}
		defer conn.Close()
		defer spy.peer.Close()
		return nil
	}))
	if err != nil {
		t.Fatalf("ServeHTTP error = %v", err)
	}
	if len(spy.statuses) != 0 || len(spy.body) != 0 {
		t.Errorf("wrote statuses %v, body %q after hijack; want nothing", spy.statuses, spy.body)
	}
}

func TestInterceptHijackNotSupported(t *testing.T) {
	var hijackErr error
	err := newTestIntercept().ServeHTTP(httptest.NewRecorder(), newTestRequest(), caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		_, _, hijackErr = http.NewResponseController(w).Hijack()
		w.WriteHeader(http.StatusOK)
		return nil
	}))
	if err != nil {
		t.Fatalf("ServeHTTP error = %v", err)
	}
	if !errors.Is(hijackErr, http.ErrNotSupported) {
		t.Errorf("Hijack() error = %v, want ErrNotSupported", hijackErr)
	}
}
