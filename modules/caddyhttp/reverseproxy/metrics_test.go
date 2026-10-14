package reverseproxy

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"
)

// streamMetricValues is a snapshot of the stream metrics, to compare deltas
// against since the collectors are shared by the whole package.
type streamMetricValues struct {
	active, closed, toBackend, fromBackend float64
	durationCount                          uint64
}

func snapshotStreamMetrics(t *testing.T, result string) streamMetricValues {
	t.Helper()
	var m dto.Metric
	if err := streamMetrics.duration.Write(&m); err != nil {
		t.Fatalf("reading duration histogram: %v", err)
	}
	return streamMetricValues{
		active:        testutil.ToFloat64(streamMetrics.active),
		closed:        testutil.ToFloat64(streamMetrics.closed.WithLabelValues(result)),
		toBackend:     testutil.ToFloat64(streamMetrics.bytes.WithLabelValues("to_backend")),
		fromBackend:   testutil.ToFloat64(streamMetrics.bytes.WithLabelValues("from_backend")),
		durationCount: m.GetHistogram().GetSampleCount(),
	}
}

// settleStreamMetrics waits for the streams of earlier tests, which may
// still be winding down in the background, to be done being accounted for.
func settleStreamMetrics(t *testing.T) {
	t.Helper()
	snapshot := func() []float64 {
		values := []float64{
			testutil.ToFloat64(streamMetrics.active),
			testutil.ToFloat64(streamMetrics.bytes.WithLabelValues("to_backend")),
			testutil.ToFloat64(streamMetrics.bytes.WithLabelValues("from_backend")),
		}
		for _, result := range []string{streamResultCompleted, streamResultError, streamResultTimeout, streamResultClosed} {
			values = append(values, testutil.ToFloat64(streamMetrics.closed.WithLabelValues(result)))
		}
		return values
	}

	last := snapshot()
	stableSince := time.Now()
	deadline := time.Now().Add(5 * time.Second)
	for time.Since(stableSince) < 300*time.Millisecond && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		if cur := snapshot(); !slices.Equal(cur, last) {
			last, stableSince = cur, time.Now()
		}
	}
}

// TestStreamAccounting runs the accounting scenarios: a stream is counted
// once however many close paths fire, and the first recorded result wins.
func TestStreamAccounting(t *testing.T) {
	t.Run("counts once", TestStreamAccountingCountsOnce)
	t.Run("first result wins", TestStreamAccountingFirstResultWins)
}

func TestStreamAccountingCountsOnce(t *testing.T) {
	settleStreamMetrics(t)
	before := snapshotStreamMetrics(t, streamResultCompleted)

	acct := newStreamAccounting()
	if got := testutil.ToFloat64(streamMetrics.active); got != before.active+1 {
		t.Fatalf("active gauge = %v, want %v", got, before.active+1)
	}
	acct.toBackend.Add(5)
	acct.fromBackend.Add(7)

	// many paths try to end the same stream concurrently
	var wg sync.WaitGroup
	firsts := make(chan string, 8)
	for range 8 {
		wg.Go(func() {
			if result, first := acct.finish(streamResultCompleted); first {
				firsts <- result
			}
		})
	}
	wg.Wait()
	close(firsts)

	var winners []string
	for r := range firsts {
		winners = append(winners, r)
	}
	if len(winners) != 1 || winners[0] != streamResultCompleted {
		t.Fatalf("finish() reported first %v times (%v), want exactly once", len(winners), winners)
	}

	after := snapshotStreamMetrics(t, streamResultCompleted)
	if after.active != before.active {
		t.Errorf("active gauge = %v, want %v", after.active, before.active)
	}
	if after.closed != before.closed+1 {
		t.Errorf("closed{completed} = %v, want %v", after.closed, before.closed+1)
	}
	if after.durationCount != before.durationCount+1 {
		t.Errorf("duration samples = %d, want %d", after.durationCount, before.durationCount+1)
	}
	if after.toBackend != before.toBackend+5 {
		t.Errorf("bytes to backend = %v, want %v", after.toBackend, before.toBackend+5)
	}
	if after.fromBackend != before.fromBackend+7 {
		t.Errorf("bytes from backend = %v, want %v", after.fromBackend, before.fromBackend+7)
	}

	// a late attempt changes nothing
	errorsBefore := snapshotStreamMetrics(t, streamResultError).closed
	if _, first := acct.finish(streamResultError); first {
		t.Error("finish() reported first for a stream that was already finished")
	}
	if got := snapshotStreamMetrics(t, streamResultError).closed; got != errorsBefore {
		t.Errorf("closed{error} = %v, want %v", got, errorsBefore)
	}
}

func TestStreamAccountingFirstResultWins(t *testing.T) {
	settleStreamMetrics(t)
	beforeClosed := snapshotStreamMetrics(t, streamResultClosed)
	beforeError := snapshotStreamMetrics(t, streamResultError)

	acct := newStreamAccounting()
	// a reload closes the stream; the copiers then fail as a consequence
	acct.setResult(streamResultClosed)
	acct.setResult(streamResultError)
	result, first := acct.finish(streamResultError)
	if !first || result != streamResultClosed {
		t.Fatalf("finish() = %q, %v; want %q, true", result, first, streamResultClosed)
	}

	if got := snapshotStreamMetrics(t, streamResultClosed).closed; got != beforeClosed.closed+1 {
		t.Errorf("closed{closed} = %v, want %v", got, beforeClosed.closed+1)
	}
	if got := snapshotStreamMetrics(t, streamResultError).closed; got != beforeError.closed {
		t.Errorf("closed{error} = %v, want %v", got, beforeError.closed)
	}
}

func TestInitReverseProxyMetricsRegistersStreamMetrics(t *testing.T) {
	h := &Handler{logger: zap.NewNop()}
	registry := prometheus.NewRegistry()

	// registering again, as every config reload does, must be fine
	initReverseProxyMetrics(h, registry)
	initReverseProxyMetrics(h, registry)

	// vectors only appear once they have children
	acct := newStreamAccounting()
	acct.finish(streamResultCompleted)

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gathering: %v", err)
	}
	got := map[string]bool{}
	for _, f := range families {
		got[f.GetName()] = true
	}
	for _, name := range []string{
		"caddy_reverse_proxy_streams_active",
		"caddy_reverse_proxy_streams_closed_total",
		"caddy_reverse_proxy_stream_duration_seconds",
		"caddy_reverse_proxy_stream_bytes_total",
	} {
		if !got[name] {
			t.Errorf("metric %s is not registered", name)
		}
	}
}

func TestInitReverseProxyMetricsPanicsOnConflict(t *testing.T) {
	h := &Handler{logger: zap.NewNop()}
	registry := prometheus.NewRegistry()

	// same name as a stream metric but a different help string: a real
	// conflict, not a repeated registration of the same collector
	if err := registry.Register(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "caddy",
		Subsystem: "reverse_proxy",
		Name:      "streams_active",
		Help:      "something else",
	})); err != nil {
		t.Fatalf("registering conflicting gauge: %v", err)
	}

	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic on a conflicting registration")
		}
	}()
	initReverseProxyMetrics(h, registry)
}
