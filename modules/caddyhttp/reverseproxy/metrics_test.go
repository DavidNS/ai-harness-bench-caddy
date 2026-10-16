package reverseproxy

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func writeMetric(t *testing.T, m prometheus.Metric) *dto.Metric {
	t.Helper()
	out := new(dto.Metric)
	if err := m.Write(out); err != nil {
		t.Fatalf("reading metric: %v", err)
	}
	return out
}

func activeStreams(t *testing.T, upstream string) float64 {
	t.Helper()
	return writeMetric(t, streamMetrics.active.WithLabelValues(upstream)).GetGauge().GetValue()
}

func closedStreams(t *testing.T, upstream, result string) float64 {
	t.Helper()
	return writeMetric(t, streamMetrics.closedBy.WithLabelValues(upstream, result)).GetCounter().GetValue()
}

func streamBytes(t *testing.T, upstream, direction string) float64 {
	t.Helper()
	return writeMetric(t, streamMetrics.bytes.WithLabelValues(upstream, direction)).GetCounter().GetValue()
}

func observedStreamDurations(t *testing.T, upstream string) uint64 {
	t.Helper()
	obs := streamMetrics.duration.WithLabelValues(upstream)
	return writeMetric(t, obs.(prometheus.Metric)).GetHistogram().GetSampleCount()
}

// Whichever path ends a stream first (the copiers, a timeout, a reload, an
// upstream removal), the stream is accounted exactly once.
func TestStreamMetricsAccountedOnce(t *testing.T) {
	const upstream = "accounted-once.test:1"

	st := newUpgradedStream(upstream)
	if got := activeStreams(t, upstream); got != 1 {
		t.Fatalf("active streams after open = %v, want 1", got)
	}
	st.toBackend.Add(100)
	st.fromBackend.Add(250)
	time.Sleep(5 * time.Millisecond)

	results := []string{
		streamResultClosed, streamResultError, streamResultTimeout,
		streamResultReload, streamResultUpstreamRemoved,
	}

	var (
		wg     sync.WaitGroup
		firsts atomic.Int32
		start  = make(chan struct{})
	)
	for i := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result := results[i%len(results)]
			if i%2 == 0 {
				// an outside close, as done on reload or upstream removal
				st.markResult(result)
			}
			if _, first := st.finish(result); first {
				firsts.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := firsts.Load(); got != 1 {
		t.Fatalf("%d calls accounted the stream, want exactly 1", got)
	}
	if got := activeStreams(t, upstream); got != 0 {
		t.Errorf("active streams after close = %v, want 0", got)
	}

	var closed float64
	for _, result := range results {
		closed += closedStreams(t, upstream, result)
	}
	if closed != 1 {
		t.Errorf("closed streams = %v, want 1", closed)
	}
	if got := observedStreamDurations(t, upstream); got != 1 {
		t.Errorf("observed durations = %d, want 1", got)
	}
	if got := streamBytes(t, upstream, "to_backend"); got != 100 {
		t.Errorf("bytes to backend = %v, want 100", got)
	}
	if got := streamBytes(t, upstream, "from_backend"); got != 250 {
		t.Errorf("bytes from backend = %v, want 250", got)
	}

	// finishing again changes nothing
	if _, first := st.finish(streamResultError); first {
		t.Error("a finished stream was accounted a second time")
	}
	if got := activeStreams(t, upstream); got != 0 {
		t.Errorf("active streams after second finish = %v, want 0", got)
	}
}

// The result imposed by an outside close wins over the result the stream
// ends with, since the latter is only the consequence of the former.
func TestUpgradedStreamResult(t *testing.T) {
	const upstream = "result.test:1"

	for _, tc := range []struct {
		name   string
		marks  []string
		finish string
		want   string
	}{
		{name: "natural result", finish: streamResultClosed, want: streamResultClosed},
		{name: "outside close wins", marks: []string{streamResultReload}, finish: streamResultError, want: streamResultReload},
		{name: "first outside close wins", marks: []string{streamResultUpstreamRemoved, streamResultReload}, finish: streamResultError, want: streamResultUpstreamRemoved},
	} {
		st := newUpgradedStream(upstream)
		for _, m := range tc.marks {
			st.markResult(m)
		}
		summary, first := st.finish(tc.finish)
		if !first {
			t.Errorf("%s: stream not accounted", tc.name)
		}
		if summary.result != tc.want {
			t.Errorf("%s: result = %q, want %q", tc.name, summary.result, tc.want)
		}
		if summary.upstream != upstream {
			t.Errorf("%s: upstream = %q, want %q", tc.name, summary.upstream, upstream)
		}
	}
}

func TestStreamMetricsRegister(t *testing.T) {
	registry := prometheus.NewRegistry()

	// registering again (another handler, a reload) is fine
	streamMetrics.register(registry)
	streamMetrics.register(registry)

	_ = newUpgradedStream("registered.test:1")
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	found := make(map[string]bool)
	for _, f := range families {
		found[f.GetName()] = true
	}
	if !found["caddy_reverse_proxy_streams_active"] {
		t.Errorf("streams_active is not exposed, got %v", found)
	}

	// a clash with something that is not our collector is an error
	clashing := prometheus.NewRegistry()
	clashing.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "caddy",
		Subsystem: "reverse_proxy",
		Name:      "streams_active",
		Help:      "something else",
	}))
	defer func() {
		if recover() == nil {
			t.Error("registering over a clashing collector should panic")
		}
	}()
	streamMetrics.register(clashing)
}
