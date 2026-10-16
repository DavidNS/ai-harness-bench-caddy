package reverseproxy

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// streamMetricsSnapshot is the state of the stream metrics at some point.
type streamMetricsSnapshot struct {
	active        float64
	closed        map[string]float64
	durationCount uint64
	toBackend     float64
	fromBackend   float64
}

// snapshotStreamMetrics reads the stream metrics through a registry, the way
// a scrape would.
func snapshotStreamMetrics(t *testing.T) streamMetricsSnapshot {
	t.Helper()
	registry := prometheus.NewRegistry()
	initReverseProxyMetrics(&Handler{logger: zap.NewNop()}, registry)
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}

	snap := streamMetricsSnapshot{closed: make(map[string]float64)}
	for _, mf := range families {
		for _, m := range mf.GetMetric() {
			switch mf.GetName() {
			case "caddy_reverse_proxy_streams_active":
				snap.active = m.GetGauge().GetValue()
			case "caddy_reverse_proxy_streams_closed_total":
				for _, l := range m.GetLabel() {
					if l.GetName() == "result" {
						snap.closed[l.GetValue()] = m.GetCounter().GetValue()
					}
				}
			case "caddy_reverse_proxy_stream_duration_seconds":
				snap.durationCount = m.GetHistogram().GetSampleCount()
			case "caddy_reverse_proxy_stream_bytes_to_backend_total":
				snap.toBackend = m.GetCounter().GetValue()
			case "caddy_reverse_proxy_stream_bytes_from_backend_total":
				snap.fromBackend = m.GetCounter().GetValue()
			}
		}
	}
	return snap
}

func TestStreamMetricsAccountEachStreamOnce(t *testing.T) {
	before := snapshotStreamMetrics(t)

	streamStarted()
	streamStarted()
	during := snapshotStreamMetrics(t)
	if got := during.active - before.active; got != 2 {
		t.Fatalf("active streams went up by %v, want 2", got)
	}

	streamFinished(streamResultCompleted, 2*time.Second, 10, 20)
	streamFinished(streamResultTimeout, time.Second, 1, 2)
	after := snapshotStreamMetrics(t)

	if after.active != before.active {
		t.Errorf("active gauge = %v, want it back to %v", after.active, before.active)
	}
	if got := after.closed[streamResultCompleted] - before.closed[streamResultCompleted]; got != 1 {
		t.Errorf("completed streams went up by %v, want 1", got)
	}
	if got := after.closed[streamResultTimeout] - before.closed[streamResultTimeout]; got != 1 {
		t.Errorf("timed out streams went up by %v, want 1", got)
	}
	if got := after.durationCount - before.durationCount; got != 2 {
		t.Errorf("duration observations went up by %d, want 2", got)
	}
	if got := after.toBackend - before.toBackend; got != 11 {
		t.Errorf("bytes to backend went up by %v, want 11", got)
	}
	if got := after.fromBackend - before.fromBackend; got != 22 {
		t.Errorf("bytes from backend went up by %v, want 22", got)
	}
}

// Registering the metrics on a registry that has them already, as happens
// with several reverse proxies in one config, must not fail.
func TestInitReverseProxyMetricsTwiceOnSameRegistry(t *testing.T) {
	registry := prometheus.NewRegistry()
	h := &Handler{logger: zap.NewNop()}
	initReverseProxyMetrics(h, registry)
	initReverseProxyMetrics(h, registry)
}
