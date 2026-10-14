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

	// stream metrics are package-level so that detached streams, which outlive
	// the config that created them, keep updating the same collectors, which
	// are registered with the registry of every config that is loaded
	for _, c := range []prometheus.Collector{
		streamMetrics.active,
		streamMetrics.closed,
		streamMetrics.duration,
		streamMetrics.bytes,
	} {
		if err := registry.Register(c); err != nil && !errors.As(err, &prometheus.AlreadyRegisteredError{}) {
			panic(err)
		}
	}

	reverseProxyMetrics.logger = handler.logger.Named("reverse_proxy.metrics")
}

// Possible values of the "result" label of the streams closed metric.
const (
	// streamResultCompleted is for streams that ended with both directions
	// finished cleanly.
	streamResultCompleted = "completed"
	// streamResultError is for streams that ended because of a read or write
	// error on either side, e.g. a peer going away.
	streamResultError = "error"
	// streamResultTimeout is for streams closed by stream_timeout.
	streamResultTimeout = "timeout"
	// streamResultCanceled is for streams whose request was canceled.
	streamResultCanceled = "canceled"
	// streamResultClosed is for streams closed by the proxy because its
	// config was unloaded or its upstream was removed.
	streamResultClosed = "closed"
)

// streamMetrics holds the collectors observing upgraded streams. Each
// stream is accounted for exactly once, when it ends (see upgradedStream.finish).
var streamMetrics = struct {
	active   *prometheus.GaugeVec
	closed   *prometheus.CounterVec
	duration *prometheus.HistogramVec
	bytes    *prometheus.CounterVec
}{
	active: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "caddy",
		Subsystem: "reverse_proxy",
		Name:      "streams_active",
		Help:      "Number of upgraded streams (e.g. WebSockets) currently being proxied.",
	}, []string{"upstream"}),
	closed: prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "caddy",
		Subsystem: "reverse_proxy",
		Name:      "streams_closed_total",
		Help:      "Number of upgraded streams that ended, by result (completed, error, timeout, canceled, closed).",
	}, []string{"upstream", "result"}),
	duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "caddy",
		Subsystem: "reverse_proxy",
		Name:      "stream_duration_seconds",
		Help:      "Duration of upgraded streams that ended.",
		Buckets:   []float64{0.1, 1, 10, 60, 300, 900, 3600, 14400, 86400},
	}, []string{"upstream", "result"}),
	bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "caddy",
		Subsystem: "reverse_proxy",
		Name:      "stream_bytes_total",
		Help:      "Bytes copied by upgraded streams, by direction (to_backend, from_backend).",
	}, []string{"upstream", "direction"}),
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
