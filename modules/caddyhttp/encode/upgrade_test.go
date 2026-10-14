package encode

import (
	"net/http"
	"testing"
)

// statusWriter records the statuses written to it.
type statusWriter struct {
	header   http.Header
	statuses []int
	body     []byte
}

func (w *statusWriter) Header() http.Header        { return w.header }
func (w *statusWriter) WriteHeader(statusCode int) { w.statuses = append(w.statuses, statusCode) }
func (w *statusWriter) Write(p []byte) (int, error) {
	w.body = append(w.body, p...)
	return len(p), nil
}

// The 101 of a protocol upgrade is written once and is final: closing the
// writer after the connection has been taken over must not write it again.
func TestUpgradeResponseIsWrittenOnce(t *testing.T) {
	enc := new(Encode)
	w := &statusWriter{header: http.Header{"Content-Length": {"10"}}}

	rw := enc.openResponseWriter("gzip", w, false)
	rw.WriteHeader(http.StatusSwitchingProtocols)
	if err := rw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if len(w.statuses) != 1 || w.statuses[0] != http.StatusSwitchingProtocols {
		t.Errorf("statuses written = %v, want [101]", w.statuses)
	}
	if rw.w != nil {
		t.Error("the upgrade response was set up for encoding")
	}
	if got := w.header.Get("Content-Encoding"); got != "" {
		t.Errorf("Content-Encoding = %q, want none", got)
	}
}
