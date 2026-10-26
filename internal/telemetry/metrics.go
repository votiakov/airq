package telemetry

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/votiakov/airq/internal/version"
)

// Namespace prefixes every application metric.
const Namespace = "airq"

// NewRegistry returns a private registry (never the global default, so tests get an
// isolated registry each) pre-loaded with runtime, process and build-info collectors.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	v := version.Get()
	buildInfo := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Name:      "build_info",
		Help:      "Build metadata of the running binary; the value is always 1.",
		ConstLabels: prometheus.Labels{
			"version":    v.Version,
			"revision":   v.Revision,
			"go_version": v.GoVersion,
		},
	})
	buildInfo.Set(1)
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		buildInfo,
	)
	return reg
}

// MetricsHandler serves the registry in Prometheus text or OpenMetrics format
// (OpenMetrics is negotiated by Prometheus and carries trace exemplars).
func MetricsHandler(reg *prometheus.Registry, logger *slog.Logger) http.Handler {
	h := promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		ErrorLog:            slog.NewLogLogger(logger.Handler(), slog.LevelError),
		ErrorHandling:       promhttp.ContinueOnError,
		Registry:            reg,
		EnableOpenMetrics:   true,
		MaxRequestsInFlight: 4,
		Timeout:             10 * time.Second,
	})
	return promhttp.InstrumentMetricHandler(reg, h)
}

// Exemplar returns trace exemplar labels for ctx, or nil outside a request.
func Exemplar(ctx context.Context) prometheus.Labels {
	info, ok := RequestInfoFrom(ctx)
	if !ok {
		return nil
	}
	return prometheus.Labels{"trace_id": info.Trace.TraceIDString()}
}

// ObserveWithExemplar records v on o, attaching the request's trace ID as an exemplar
// when available so a latency spike in Grafana links straight to the matching logs.
func ObserveWithExemplar(ctx context.Context, o prometheus.Observer, v float64) {
	if eo, ok := o.(prometheus.ExemplarObserver); ok {
		if ex := Exemplar(ctx); ex != nil {
			eo.ObserveWithExemplar(v, ex)
			return
		}
	}
	o.Observe(v)
}
