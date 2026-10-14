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

// streamMetricSet holds the collectors that observe upgraded streams
// (WebSocket and other Upgrade tunnels, extended CONNECT). The collectors
// are created up front so that streams are always accounted for; they are
// attached to a registry when a handler is provisioned.
type streamMetricSet struct {
	active   *prometheus.GaugeVec
	closed   *prometheus.CounterVec
	duration *prometheus.HistogramVec
	bytes    *prometheus.CounterVec
}

// Values of the "direction" label of the stream bytes metric.
const (
	streamDirectionToBackend   = "to_backend"
	streamDirectionFromBackend = "from_backend"
)

func newStreamMetricSet() *streamMetricSet {
	const ns, sub = "caddy", "reverse_proxy"
	return &streamMetricSet{
		active: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "streams_active",
			Help:      "Number of upgraded streams currently open.",
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
			Help:      "How long upgraded streams were open.",
			Buckets:   []float64{0.1, 1, 10, 60, 300, 900, 3600, 21600, 86400},
		}, []string{"upstream"}),
		bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "stream_bytes_total",
			Help:      "Bytes copied on upgraded streams, by direction relative to the backend.",
		}, []string{"upstream", "direction"}),
	}
}

// register attaches the collectors to registry. Registering collectors
// that are already registered (another handler, or a reload) is fine. Any
// other failure is a programming error in the metric definitions and panics,
// like the other reverse proxy metrics.
func (m *streamMetricSet) register(registry prometheus.Registerer) {
	for _, c := range []prometheus.Collector{m.active, m.closed, m.duration, m.bytes} {
		if err := registry.Register(c); err != nil {
			var already prometheus.AlreadyRegisteredError
			if !errors.As(err, &already) {
				panic(err)
			}
		}
	}
}

// streamMetrics are the process-wide stream metrics.
var streamMetrics = newStreamMetricSet()

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
