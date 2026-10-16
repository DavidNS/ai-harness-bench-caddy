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

// streamMetrics holds the collectors describing upgraded streams. They are
// package-level, not tied to a config, so that streams which outlive the
// config they were started by (see Handler.StreamDetached) keep being
// accounted for in the same series. Using them does not require them to be
// registered, which initReverseProxyMetrics does for each config.
var streamMetrics = newStreamMetricsCollectors()

type streamMetricsCollectors struct {
	active          *prometheus.GaugeVec
	closed          *prometheus.CounterVec
	duration        *prometheus.HistogramVec
	bytesToBackend  *prometheus.CounterVec
	bytesFromBackend *prometheus.CounterVec
}

func newStreamMetricsCollectors() *streamMetricsCollectors {
	const ns, sub = "caddy", "reverse_proxy"
	return &streamMetricsCollectors{
		active: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "streams_active",
			Help:      "Number of upgraded streams (e.g. WebSockets) currently open.",
		}, []string{"upstream"}),
		closed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "streams_closed_total",
			Help:      "Number of upgraded streams that were closed, by result.",
		}, []string{"upstream", "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "stream_duration_seconds",
			Help:      "Duration of upgraded streams.",
			Buckets:   []float64{1, 10, 60, 300, 1800, 3600, 21600, 86400},
		}, []string{"upstream"}),
		bytesToBackend: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "stream_bytes_to_backend_total",
			Help:      "Bytes sent to the backend over upgraded streams.",
		}, []string{"upstream"}),
		bytesFromBackend: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "stream_bytes_from_backend_total",
			Help:      "Bytes received from the backend over upgraded streams.",
		}, []string{"upstream"}),
	}
}

func (m *streamMetricsCollectors) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.active, m.closed, m.duration, m.bytesToBackend, m.bytesFromBackend}
}

// opened records that a stream to upstream started.
func (m *streamMetricsCollectors) opened(upstream string) {
	m.active.WithLabelValues(upstream).Inc()
}

// finished records that a stream to upstream ended with the given result.
func (m *streamMetricsCollectors) finished(upstream, result string, d time.Duration, toBackend, fromBackend int64) {
	m.active.WithLabelValues(upstream).Dec()
	m.closed.WithLabelValues(upstream, result).Inc()
	m.duration.WithLabelValues(upstream).Observe(d.Seconds())
	m.bytesToBackend.WithLabelValues(upstream).Add(float64(toBackend))
	m.bytesFromBackend.WithLabelValues(upstream).Add(float64(fromBackend))
}

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

	// same reasoning as above for duplicate registrations
	for _, c := range streamMetrics.collectors() {
		if err := registry.Register(c); err != nil && !errors.As(err, &prometheus.AlreadyRegisteredError{}) {
			panic(err)
		}
	}

	reverseProxyMetrics.logger = handler.logger.Named("reverse_proxy.metrics")
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
