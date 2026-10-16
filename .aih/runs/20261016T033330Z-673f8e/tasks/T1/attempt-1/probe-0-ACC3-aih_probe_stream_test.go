package reverseproxy

import "testing"

func TestAihProbeDefaultNotDetached(t *testing.T) {
	var h Handler
	if h.StreamDetached {
		t.Fatal("default must be false")
	}
}
