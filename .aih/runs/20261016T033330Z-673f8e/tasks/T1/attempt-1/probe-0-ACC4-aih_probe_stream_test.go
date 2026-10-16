package reverseproxy

import (
	"testing"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestAihProbeSkipHandshake(t *testing.T) {
	var h Handler
	if err := h.UnmarshalCaddyfile(caddyfile.NewTestDispenser(`reverse_proxy a:1 {
		stream_logs {
			skip_handshake
		}
	}`)); err != nil {
		t.Fatal(err)
	}
	if !h.StreamLogs.SkipHandshake {
		t.Fatal("not set")
	}
}
