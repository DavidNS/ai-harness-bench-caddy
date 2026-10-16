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

	// stream metrics; they live as long as the process so that upgraded
	// streams that outlive a config are still accounted for exactly once
	streamsActive          prometheus.Gauge
	streamsClosed          *prometheus.CounterVec
	streamDuration         prometheus.Histogram
	streamBytesToBackend   prometheus.Counter
	streamBytesFromBackend prometheus.Counter

	logger *zap.Logger
}{}

// initReverseProxyMetricCollectors creates the collectors once.
func initReverseProxyMetricCollectors() {
	const ns, sub = "caddy", "reverse_proxy"

	reverseProxyMetrics.once.Do(func() {
		reverseProxyMetrics.upstreamsHealthy = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "upstreams_healthy",
			Help:      "Health status of reverse proxy upstreams.",
		}, []string{"upstream"})
		reverseProxyMetrics.streamsActive = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "streams_active",
			Help:      "Number of upgraded streams (e.g. WebSockets) currently open.",
		})
		reverseProxyMetrics.streamsClosed = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "streams_closed_total",
			Help:      "Number of upgraded streams that were closed, by result.",
		}, []string{"result"})
		reverseProxyMetrics.streamDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "stream_duration_seconds",
			Help:      "How long upgraded streams were open.",
			Buckets:   []float64{.1, .5, 1, 5, 15, 60, 300, 900, 3600, 14400, 86400},
		})
		reverseProxyMetrics.streamBytesToBackend = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "stream_bytes_to_backend_total",
			Help:      "Bytes sent to the backend over upgraded streams.",
		})
		reverseProxyMetrics.streamBytesFromBackend = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "stream_bytes_from_backend_total",
			Help:      "Bytes received from the backend over upgraded streams.",
		})
	})
}

func initReverseProxyMetrics(handler *Handler, registry *prometheus.Registry) {
	initReverseProxyMetricCollectors()

	// duplicate registration could happen if multiple sites with reverse proxy are configured; so ignore the error because
	// there's no good way to capture having multiple sites with reverse proxy. If this happens, the metrics will be
	// registered twice, but the second registration will be ignored.
	for _, c := range []prometheus.Collector{
		reverseProxyMetrics.upstreamsHealthy,
		reverseProxyMetrics.streamsActive,
		reverseProxyMetrics.streamsClosed,
		reverseProxyMetrics.streamDuration,
		reverseProxyMetrics.streamBytesToBackend,
		reverseProxyMetrics.streamBytesFromBackend,
	} {
		if err := registry.Register(c); err != nil && !errors.As(err, &prometheus.AlreadyRegisteredError{}) {
			panic(err)
		}
	}

	reverseProxyMetrics.logger = handler.logger.Named("reverse_proxy.metrics")
}

// streamStarted accounts for a stream that is now open.
func streamStarted() {
	initReverseProxyMetricCollectors()
	reverseProxyMetrics.streamsActive.Inc()
}

// streamFinished accounts for a stream that was closed. It must be called
// exactly once for each call to streamStarted.
func streamFinished(result string, duration time.Duration, toBackend, fromBackend int64) {
	reverseProxyMetrics.streamsActive.Dec()
	reverseProxyMetrics.streamsClosed.WithLabelValues(result).Inc()
	reverseProxyMetrics.streamDuration.Observe(duration.Seconds())
	reverseProxyMetrics.streamBytesToBackend.Add(float64(toBackend))
	reverseProxyMetrics.streamBytesFromBackend.Add(float64(fromBackend))
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
