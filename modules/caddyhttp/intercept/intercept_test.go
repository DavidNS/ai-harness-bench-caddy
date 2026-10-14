package intercept

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

// statusWriter records the statuses written to it.
type statusWriter struct {
	header   http.Header
	statuses []int
}

func (w *statusWriter) Header() http.Header         { return w.header }
func (w *statusWriter) WriteHeader(statusCode int)  { w.statuses = append(w.statuses, statusCode) }
func (w *statusWriter) Write(p []byte) (int, error) { return len(p), nil }

// Responses that switch protocols or establish a tunnel can't be intercepted:
// the client has to get them right away, as the stream continues from there.
func TestInterceptDoesNotBufferUpgradeAndTunnelResponses(t *testing.T) {
	// replaces the status of any response that it buffers
	ir := Intercept{
		HandleResponse: []caddyhttp.ResponseHandler{{StatusCode: "418"}},
		logger:         zap.NewNop(),
	}

	for i, tc := range []struct {
		name   string
		method string
		status int
		want   int
	}{
		{name: "regular response", method: http.MethodGet, status: http.StatusOK, want: http.StatusTeapot},
		{name: "upgrade", method: http.MethodGet, status: http.StatusSwitchingProtocols, want: http.StatusSwitchingProtocols},
		{name: "established tunnel", method: http.MethodConnect, status: http.StatusOK, want: http.StatusOK},
		{name: "refused tunnel", method: http.MethodConnect, status: http.StatusBadGateway, want: http.StatusTeapot},
		{name: "success for another method", method: http.MethodPost, status: http.StatusOK, want: http.StatusTeapot},
	} {
		req := httptest.NewRequest(tc.method, "http://example.com/", nil)
		req = req.WithContext(context.WithValue(req.Context(), caddy.ReplacerCtxKey, caddy.NewReplacer()))
		w := &statusWriter{header: http.Header{}}

		err := ir.ServeHTTP(w, req, caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
			w.WriteHeader(tc.status)
			return nil
		}))
		if err != nil {
			t.Fatalf("Test %d (%s): %v", i, tc.name, err)
		}

		if len(w.statuses) != 1 || w.statuses[0] != tc.want {
			t.Errorf("Test %d (%s): statuses written = %v, want [%d]", i, tc.name, w.statuses, tc.want)
		}
	}
}
