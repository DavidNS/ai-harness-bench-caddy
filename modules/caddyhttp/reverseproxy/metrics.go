package reverseproxy

import (
	"errors"
	"runtime/debug"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/caddyserver/caddy/v2"
)

var reverseProxyMetrics = struct {
	once             sync.Once
	upstreamsHealthy *prometheus.GaugeVec
	logger           *zap.Logger
}{}

func initReverseProxyMetrics(handler *Handler, registry *prometheus.Registry) {
	const ns, sub = "caddy", "reverse_proxy"

	upstreamsLabels := []string{"upstream"}
	reverseProxyMetrics.once.Do(func() {
		reverseProxyMetrics.upstreamsHealthy = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "upstreams_healthy",
			Help:      "Health status of reverse proxy upstreams.",
		}, upstreamsLabels)
	})

	// duplicate registration could happen if multiple sites with reverse proxy are configured; so ignore the error because
	// there's no good way to capture having multiple sites with reverse proxy. If this happens, the metrics will be
	// registered twice, but the second registration will be ignored.
	if err := registry.Register(reverseProxyMetrics.upstreamsHealthy); err != nil &&
		!errors.Is(err, prometheus.AlreadyRegisteredError{
			ExistingCollector: reverseProxyMetrics.upstreamsHealthy,
			NewCollector:      reverseProxyMetrics.upstreamsHealthy,
		}) {
		panic(err)
	}
	streamMetrics.register(registry)

	reverseProxyMetrics.logger = handler.logger.Named("reverse_proxy.metrics")
}

// streamMetrics holds the metrics of upgraded streams. Unlike the other
// reverse proxy metrics it exists independently of any registry, since
// detached streams outlive the config (and registry) they were opened in;
// it is registered whenever a handler is provisioned.
var streamMetrics = newStreamMetricsSet()

type streamMetricsSet struct {
	active   *prometheus.GaugeVec
	closedBy *prometheus.CounterVec
	duration *prometheus.HistogramVec
	bytes    *prometheus.CounterVec
}

func newStreamMetricsSet() *streamMetricsSet {
	const ns, sub = "caddy", "reverse_proxy"

	return &streamMetricsSet{
		active: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "streams_active",
			Help:      "Number of upgraded streams (e.g. WebSockets) currently open.",
		}, []string{"upstream"}),
		closedBy: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "streams_closed_total",
			Help:      "Number of upgraded streams that were closed, by result (closed, error, timeout, reload, upstream_removed).",
		}, []string{"upstream", "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "stream_duration_seconds",
			Help:      "How long upgraded streams were open.",
			Buckets:   []float64{0.1, 1, 10, 60, 300, 1800, 3600, 21600, 86400},
		}, []string{"upstream"}),
		bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "stream_bytes_total",
			Help:      "Bytes copied on upgraded streams, by direction (to_backend, from_backend).",
		}, []string{"upstream", "direction"}),
	}
}

// register registers the stream metrics in registry. Registering them
// again, e.g. by another handler or after a reload, is not an error.
func (m *streamMetricsSet) register(registry *prometheus.Registry) {
	for _, c := range []prometheus.Collector{m.active, m.closedBy, m.duration, m.bytes} {
		if err := registry.Register(c); err != nil && !errors.As(err, &prometheus.AlreadyRegisteredError{}) {
			panic(err)
		}
	}
}

// opened accounts for a stream to upstream that was just opened.
func (m *streamMetricsSet) opened(upstream string) {
	m.active.WithLabelValues(upstream).Inc()
}

// closed accounts for a stream that ended. It must be called exactly once
// per opened stream. The closed counter is the last to be updated, so that
// a stream shown as closed has its other metrics in place.
func (m *streamMetricsSet) closed(s streamSummary) {
	m.active.WithLabelValues(s.upstream).Dec()
	m.duration.WithLabelValues(s.upstream).Observe(s.duration.Seconds())
	m.bytes.WithLabelValues(s.upstream, "to_backend").Add(float64(s.toBackend))
	m.bytes.WithLabelValues(s.upstream, "from_backend").Add(float64(s.fromBackend))
	m.closedBy.WithLabelValues(s.upstream, s.result).Inc()
}

type metricsUpstreamsHealthyUpdater struct {
	handler *Handler
}

func newMetricsUpstreamsHealthyUpdater(handler *Handler, ctx caddy.Context) *metricsUpstreamsHealthyUpdater {
	initReverseProxyMetrics(handler, ctx.GetMetricsRegistry())
	reverseProxyMetrics.upstreamsHealthy.Reset()

	return &metricsUpstreamsHealthyUpdater{handler}
}

func (m *metricsUpstreamsHealthyUpdater) init() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				if c := reverseProxyMetrics.logger.Check(zapcore.ErrorLevel, "upstreams healthy metrics updater panicked"); c != nil {
					c.Write(
						zap.Any("error", err),
						zap.ByteString("stack", debug.Stack()),
					)
				}
			}
		}()

		m.update()

		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				m.update()
			case <-m.handler.ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (m *metricsUpstreamsHealthyUpdater) update() {
	for _, upstream := range m.handler.Upstreams {
		labels := prometheus.Labels{"upstream": upstream.Dial}

		gaugeValue := 0.0
		if upstream.Healthy() {
			gaugeValue = 1.0
		}

		reverseProxyMetrics.upstreamsHealthy.With(labels).Set(gaugeValue)
	}
}
