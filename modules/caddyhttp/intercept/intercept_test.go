package intercept

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

// hijackSpyWriter is a response writer that can be hijacked and notes
// what is written to it, and whether that was after the hijack.
type hijackSpyWriter struct {
	*httptest.ResponseRecorder
	hijacked    bool
	statuses    []int
	afterHijack int
}

func (h *hijackSpyWriter) WriteHeader(code int) {
	if h.hijacked {
		h.afterHijack++
	}
	h.statuses = append(h.statuses, code)
	h.ResponseRecorder.WriteHeader(code)
}

func (h *hijackSpyWriter) Write(p []byte) (int, error) {
	if h.hijacked {
		h.afterHijack++
	}
	return h.ResponseRecorder.Write(p)
}

func (h *hijackSpyWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h.hijacked = true
	conn, _ := net.Pipe()
	return conn, bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)), nil
}

func newInterceptRequest() *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/ws", nil)
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	return r.WithContext(context.WithValue(r.Context(), caddy.ReplacerCtxKey, caddy.NewReplacer()))
}

// A response handler that matches every response, like a bare handle_response.
func newCatchAllIntercept() Intercept {
	return Intercept{
		HandleResponse: []caddyhttp.ResponseHandler{{}},
		logger:         zap.NewNop(),
	}
}

// An upgrade is the final response: it is not intercepted, and once
// the connection is hijacked nothing more is written to the response.
func TestInterceptUpgradeIsNotIntercepted(t *testing.T) {
	w := &hijackSpyWriter{ResponseRecorder: httptest.NewRecorder()}

	var hijackErr error
	err := newCatchAllIntercept().ServeHTTP(w, newInterceptRequest(), caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		w.Header().Set("Upgrade", "websocket")
		w.Header().Set("Connection", "Upgrade")
		w.WriteHeader(http.StatusSwitchingProtocols)
		conn, _, err := http.NewResponseController(w).Hijack()
		hijackErr = err
		if err == nil {
			conn.Close()
		}
		return nil
	}))
	if err != nil {
		t.Fatalf("ServeHTTP() = %v", err)
	}
	if hijackErr != nil {
		t.Fatalf("hijack through intercept failed: %v", hijackErr)
	}
	if len(w.statuses) != 1 || w.statuses[0] != http.StatusSwitchingProtocols {
		t.Errorf("statuses written = %v, want [101]", w.statuses)
	}
	if w.afterHijack != 0 {
		t.Errorf("%d writes reached the response after the hijack", w.afterHijack)
	}
	if w.Body.Len() != 0 {
		t.Errorf("body = %q, want empty", w.Body.String())
	}
}

// A connection hijacked without a response ever being written is not
// a response to intercept either.
func TestInterceptHijackWithoutResponse(t *testing.T) {
	w := &hijackSpyWriter{ResponseRecorder: httptest.NewRecorder()}

	err := newCatchAllIntercept().ServeHTTP(w, newInterceptRequest(), caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return err
		}
		return conn.Close()
	}))
	if err != nil {
		t.Fatalf("ServeHTTP() = %v", err)
	}
	if len(w.statuses) != 0 || w.afterHijack != 0 {
		t.Errorf("response written after hijack: statuses=%v, writes after hijack=%d", w.statuses, w.afterHijack)
	}
}

// Regular responses are still intercepted.
func TestInterceptStillBuffersRegularResponses(t *testing.T) {
	ir := Intercept{
		HandleResponse: []caddyhttp.ResponseHandler{{StatusCode: "418"}},
		logger:         zap.NewNop(),
	}
	w := httptest.NewRecorder()

	err := ir.ServeHTTP(w, newInterceptRequest(), caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("original"))
		return err
	}))
	if err != nil {
		t.Fatalf("ServeHTTP() = %v", err)
	}
	if w.Code != http.StatusTeapot {
		t.Errorf("status = %d, want %d", w.Code, http.StatusTeapot)
	}
	if got := w.Body.String(); got != "original" {
		t.Errorf("body = %q, want %q", got, "original")
	}
}
