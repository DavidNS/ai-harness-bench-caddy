package reverseproxy

import (
	"testing"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestAihProbeRemovedUpstreamOnlyClosesItsStreams(t *testing.T) {
	r := &streamRegistry{streams: make(map[string]map[*upgradedStream]struct{})}
	var closedA, closedB int
	a := newUpgradedStream("probe-a:1")
	a.closeConns = func() { closedA++ }
	b := newUpgradedStream("probe-b:1")
	b.closeConns = func() { closedB++ }
	r.add(a)
	r.add(b)

	if n := r.closeUpstream("probe-a:1", streamResultUpstreamRemoved); n != 1 {
		t.Fatalf("closed %d, want 1", n)
	}
	if closedA != 1 || closedB != 0 {
		t.Fatalf("closedA=%d closedB=%d", closedA, closedB)
	}
	s, first := a.finish(streamResultError)
	if !first || s.result != streamResultUpstreamRemoved {
		t.Fatalf("summary %+v first=%v", s, first)
	}
}

func TestAihProbeCaddyfileStreamLogsLevelCase(t *testing.T) {
	h := new(Handler)
	err := h.UnmarshalCaddyfile(caddyfile.NewTestDispenser("reverse_proxy a:1 {\nstream_detached\nstream_logs {\nlevel WARN\nskip_handshake\n}\n}"))
	if err != nil {
		t.Fatal(err)
	}
	if !h.StreamDetached || h.StreamLogs == nil || !h.StreamLogs.SkipHandshake || h.StreamLogs.Level != "WARN" {
		t.Fatalf("%+v", h.StreamLogs)
	}
}
