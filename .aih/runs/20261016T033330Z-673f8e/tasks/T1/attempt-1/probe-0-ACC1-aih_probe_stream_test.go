package reverseproxy

import "testing"

func TestAihProbeRegistry(t *testing.T) {
	r := newStreamRegistry()
	r.addLive([]string{"a", "b"})
	r.addLive([]string{"a", "b"})
	s := &detachedStream{upstream: "b"}
	rm := r.add(s)
	defer rm()
	if o := r.removeLive([]string{"a", "b"}); len(o) != 0 {
		t.Fatalf("closed %d streams though b is still live", len(o))
	}
	if o := r.removeLive([]string{"a", "b"}); len(o) != 1 {
		t.Fatalf("got %d orphans, want 1", len(o))
	}
}
