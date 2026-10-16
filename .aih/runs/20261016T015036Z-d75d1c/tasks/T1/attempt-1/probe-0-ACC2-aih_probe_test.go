package reverseproxy

import (
	"encoding/json"
	"testing"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestProbeParse(t *testing.T) {
	h := &Handler{}
	if err := h.UnmarshalCaddyfile(caddyfile.NewTestDispenser("reverse_proxy a:1 {\n stream_detached\n}")); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(h)
	var h2 Handler
	if err := json.Unmarshal(b, &h2); err != nil || !h2.StreamDetached {
		t.Fatal(err)
	}
}
