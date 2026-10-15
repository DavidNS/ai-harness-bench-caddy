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
	streams          *streamMetrics
	logger           *zap.Logger
}{}

// Results a stream can be accounted with in the streams_closed_total metric.
const (
	streamResultClosed  = "closed"  // both directions ended cleanly
	streamResultTimeout = "timeout" // stream_timeout elapsed
	streamResultError   = "error"   // copy error, or a connection was closed under the stream
	streamResultReload  = "reload"  // closed because of a config reload
)

// streamMetrics holds the metrics of upgraded streams. The collectors are
// shared by all handlers and outlive config reloads, because detached
// streams outlive the config that created them.
type streamMetrics struct {
	active   *prometheus.GaugeVec
	closed   *prometheus.CounterVec
	duration *prometheus.HistogramVec
	bytes    *prometheus.CounterVec
}

func newStreamMetrics() *streamMetrics {
	const ns, sub = "caddy", "reverse_proxy"
	return &streamMetrics{
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
			Help:      "How long upgraded streams were open.",
			Buckets:   []float64{0.1, 1, 10, 60, 300, 1800, 3600, 86400},
		}, []string{"upstream"}),
		bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "stream_bytes_total",
			Help:      "Bytes copied through upgraded streams, by direction relative to the backend.",
		}, []string{"upstream", "direction"}),
	}
}

func (m *streamMetrics) register(registry *prometheus.Registry) {
	for _, c := range []prometheus.Collector{m.active, m.closed, m.duration, m.bytes} {
		// duplicate registration happens when multiple handlers are provisioned;
		// the registered collector is the same one, so the error is harmless
		if err := registry.Register(c); err != nil {
			if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
				panic(err)
			}
		}
	}
}

// streamAccount accounts a single stream in the stream metrics. Nothing is
// recorded when the metrics are not initialized (for example in unit tests
// which build a Handler by hand).
type streamAccount struct {
	metrics  *streamMetrics
	upstream string
	start    time.Time
	once     sync.Once
}

// begin starts accounting a stream to upstream.
func (m *streamMetrics) begin(upstream string) *streamAccount {
	if m != nil {
		m.active.WithLabelValues(upstream).Inc()
	}
	return &streamAccount{metrics: m, upstream: upstream, start: time.Now()}
}

// finish records the end of the stream. Only the first call has an effect,
// so a stream is accounted exactly once however many paths end it.
func (a *streamAccount) finish(result string, toBackend, fromBackend int64) (took time.Duration) {
	a.once.Do(func() {
		took = time.Since(a.start)
		m := a.metrics
		if m == nil {
			return
		}
		m.active.WithLabelValues(a.upstream).Dec()
		m.closed.WithLabelValues(a.upstream, result).Inc()
		m.duration.WithLabelValues(a.upstream).Observe(took.Seconds())
		m.bytes.WithLabelValues(a.upstream, "to_backend").Add(float64(toBackend))
		m.bytes.WithLabelValues(a.upstream, "from_backend").Add(float64(fromBackend))
	})
	return took
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
		reverseProxyMetrics.streams = newStreamMetrics()
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
	reverseProxyMetrics.streams.register(registry)

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
