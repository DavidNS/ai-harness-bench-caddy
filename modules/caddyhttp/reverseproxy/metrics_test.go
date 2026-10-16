package reverseproxy

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func gaugeValue(vec *prometheus.GaugeVec, labels ...string) float64 {
	var m dto.Metric
	if err := vec.WithLabelValues(labels...).Write(&m); err != nil {
		panic(err)
	}
	return m.GetGauge().GetValue()
}

func counterValue(vec *prometheus.CounterVec, labels ...string) float64 {
	var m dto.Metric
	if err := vec.WithLabelValues(labels...).Write(&m); err != nil {
		panic(err)
	}
	return m.GetCounter().GetValue()
}

func histogramCount(vec *prometheus.HistogramVec, labels ...string) uint64 {
	var m dto.Metric
	obs, err := vec.GetMetricWithLabelValues(labels...)
	if err != nil {
		panic(err)
	}
	if err := obs.(prometheus.Metric).Write(&m); err != nil {
		panic(err)
	}
	return m.GetHistogram().GetSampleCount()
}

func TestStreamStatsMetrics(t *testing.T) {
	const up = "metrics-test-1"

	s := newStreamStats(up, nil, zapcore.DebugLevel)
	if got := gaugeValue(streamMetrics.active, up); got != 1 {
		t.Fatalf("active = %v, want 1", got)
	}
	s.toBackend.Add(5)
	s.fromBackend.Add(7)

	// the first result wins
	s.setResult(streamResultTimeout)
	s.setResult(streamResultCleanup)

	// finishing again, from whichever direction ends last, counts nothing more
	s.finish()
	s.finish()

	if got := gaugeValue(streamMetrics.active, up); got != 0 {
		t.Errorf("active = %v, want 0", got)
	}
	if got := counterValue(streamMetrics.closed, up, streamResultTimeout); got != 1 {
		t.Errorf("closed{timeout} = %v, want 1", got)
	}
	if got := counterValue(streamMetrics.closed, up, streamResultCleanup); got != 0 {
		t.Errorf("closed{cleanup} = %v, want 0", got)
	}
	if got := counterValue(streamMetrics.bytesToBackend, up); got != 5 {
		t.Errorf("bytes to backend = %v, want 5", got)
	}
	if got := counterValue(streamMetrics.bytesFromBackend, up); got != 7 {
		t.Errorf("bytes from backend = %v, want 7", got)
	}
	if got := histogramCount(streamMetrics.duration, up); got != 1 {
		t.Errorf("duration observations = %d, want 1", got)
	}
}

func TestStreamStatsDefaultResult(t *testing.T) {
	const up = "metrics-test-2"

	s := newStreamStats(up, nil, zapcore.DebugLevel)
	s.finish()
	if got := counterValue(streamMetrics.closed, up, streamResultCompleted); got != 1 {
		t.Errorf("closed{completed} = %v, want 1", got)
	}
}

func TestStreamStatsCloseLog(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)

	// the entry is written at the configured level...
	s := newStreamStats("metrics-test-3", []*zap.Logger{zap.New(core), zap.NewNop()}, zapcore.InfoLevel)
	s.toBackend.Add(3)
	s.fromBackend.Add(4)
	s.setResult(streamResultError)
	time.Sleep(time.Millisecond)
	s.finish()
	s.finish()

	entries := logs.FilterMessage("stream closed").All()
	if len(entries) != 1 {
		t.Fatalf("got %d stream close entries, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["upstream"] != "metrics-test-3" || fields["result"] != streamResultError ||
		fields["bytes_to_backend"] != int64(3) || fields["bytes_from_backend"] != int64(4) {
		t.Errorf("unexpected fields: %v", fields)
	}
	if d, ok := fields["duration"].(time.Duration); !ok || d <= 0 {
		t.Errorf("duration = %v, want a positive duration", fields["duration"])
	}

	// ...and not when the logger doesn't enable that level
	s = newStreamStats("metrics-test-3", []*zap.Logger{zap.New(core)}, zapcore.DebugLevel)
	s.finish()
	if got := logs.Len(); got != 1 {
		t.Errorf("got %d entries, want the debug entry to be dropped", got)
	}
}

func TestInitReverseProxyMetricsRegistersStreamMetrics(t *testing.T) {
	h := &Handler{logger: zap.NewNop()}
	registry := prometheus.NewRegistry()
	initReverseProxyMetrics(h, registry)
	// registering again is tolerated
	initReverseProxyMetrics(h, registry)

	s := newStreamStats("metrics-test-4", nil, zapcore.DebugLevel)
	s.finish()

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, f := range families {
		got[f.GetName()] = true
	}
	for _, name := range []string{
		"caddy_reverse_proxy_streams_active",
		"caddy_reverse_proxy_streams_closed_total",
		"caddy_reverse_proxy_stream_duration_seconds",
		"caddy_reverse_proxy_stream_bytes_to_backend_total",
		"caddy_reverse_proxy_stream_bytes_from_backend_total",
	} {
		if !got[name] {
			t.Errorf("metric %s is not registered", name)
		}
	}
}

func TestInitReverseProxyMetricsConflict(t *testing.T) {
	registry := prometheus.NewRegistry()
	// same name, different dimensions: not a duplicate registration
	registry.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "caddy",
		Subsystem: "reverse_proxy",
		Name:      "streams_active",
		Help:      "conflict",
	}))

	defer func() {
		if recover() == nil {
			t.Error("expected a conflicting registration to panic")
		}
	}()
	initReverseProxyMetrics(&Handler{logger: zap.NewNop()}, registry)
}
