package httpapi

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/votiakov/airq/internal/telemetry"
)

type metrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	inFlight prometheus.Gauge
	rejected *prometheus.CounterVec
	panics   prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *metrics {
	ns, sub := telemetry.Namespace, "http"
	m := &metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "requests_total",
			Help: "HTTP requests by method, route pattern and status code.",
		}, []string{"method", "route", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "request_duration_seconds",
			Help:    "HTTP request latency by method, route pattern and status code.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"method", "route", "code"}),
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "requests_in_flight",
			Help: "HTTP requests currently being served.",
		}),
		rejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "rejected_requests_total",
			Help: "API requests rejected before reaching a handler, by reason (rate_limited, overloaded).",
		}, []string{"reason"}),
		panics: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "panics_recovered_total",
			Help: "Handler panics recovered and turned into 500 responses.",
		}),
	}
	reg.MustRegister(m.requests, m.duration, m.inFlight, m.rejected, m.panics)
	m.rejected.WithLabelValues("rate_limited")
	m.rejected.WithLabelValues("overloaded")
	return m
}
