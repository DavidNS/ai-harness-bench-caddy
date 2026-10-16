package reverseproxy

import (
	"encoding/json"
	"testing"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestAihProbeStreamOptions(t *testing.T) {
	var h Handler
	if err := h.UnmarshalCaddyfile(caddyfile.NewTestDispenser(`reverse_proxy a:1 {
		stream_detached
	}`)); err != nil {
		t.Fatal(err)
	}
	var j Handler
	if err := json.Unmarshal([]byte(`{"stream_detached":true}`), &j); err != nil {
		t.Fatal(err)
	}
	if !h.StreamDetached || !j.StreamDetached {
		t.Fatal("not set")
	}
}
