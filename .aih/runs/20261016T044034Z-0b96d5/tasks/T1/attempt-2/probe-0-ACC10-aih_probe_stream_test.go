package reverseproxy

import (
	"testing"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestProbeStreamCaddyfileJSON(t *testing.T) {
	h := new(Handler)
	d := caddyfile.NewTestDispenser("reverse_proxy localhost:1 {\nstream_detached\nstream_logs {\nskip_handshake\nlevel warn\n}\n}")
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatal(err)
	}
	if !h.StreamDetached || !h.StreamLogs.SkipHandshake || h.StreamLogs.Level != "warn" {
		t.Fatalf("bad parse: %+v", h)
	}
	lvl, err := h.StreamLogs.parseLevel()
	if err != nil || lvl.String() != "warn" {
		t.Fatalf("level %v %v", lvl, err)
	}
}
