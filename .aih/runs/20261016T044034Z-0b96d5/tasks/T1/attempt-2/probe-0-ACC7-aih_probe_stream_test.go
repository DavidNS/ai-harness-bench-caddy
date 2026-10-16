package reverseproxy

import "testing"

func TestProbeRegistryCloseOnlyUpstream(t *testing.T) {
	a := newUpgradedStream("probe-a:1")
	b := newUpgradedStream("probe-b:1")
	closedA, closedB := 0, 0
	a.closeConns = func() { closedA++ }
	b.closeConns = func() { closedB++ }
	defer detachedStreams.add(a)()
	defer detachedStreams.add(b)()
	if n := detachedStreams.closeUpstream("probe-a:1", streamResultUpstreamRemoved); n != 1 {
		t.Fatalf("n=%d", n)
	}
	if closedA != 1 || closedB != 0 {
		t.Fatalf("closed %d %d", closedA, closedB)
	}
	s, first := a.finish(streamResultError)
	if !first || s.result != streamResultUpstreamRemoved {
		t.Fatalf("%+v %v", s, first)
	}
	b.finish(streamResultClosed)
}
