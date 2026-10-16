package reverseproxy

import (
	"testing"
	"time"
)

func TestAihProbeMetrics(t *testing.T) {
	before := snapshotStreamMetrics(t)
	streamStarted()
	streamFinished(streamResultError, time.Second, 3, 4)
	after := snapshotStreamMetrics(t)
	if after.active != before.active || after.closed["error"]-before.closed["error"] != 1 {
		t.Fatal("accounting")
	}
}
