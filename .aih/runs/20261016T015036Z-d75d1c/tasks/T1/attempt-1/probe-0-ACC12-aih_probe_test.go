package reverseproxy

import "testing"

func TestProbeRegistryEmpty(t *testing.T) {
	r := &detachedRegistry{streams: make(map[string]map[*detachedStream]struct{})}
	if n, err := r.closeUpstream("none"); n != 0 || err != nil {
		t.Fatal(n, err)
	}
}
