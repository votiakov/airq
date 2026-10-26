// Package httpapi is the HTTP transport: routing, middleware, request validation,
// error mapping (RFC 9457 problem details) and response caching headers.
package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/votiakov/airq/internal/health"
	"github.com/votiakov/airq/internal/telemetry"
	"github.com/votiakov/airq/internal/version"
)

// Options wires the handler's dependencies and limits.
type Options struct {
	Service        Service
	Health         *health.Checker
	Registry       *prometheus.Registry
	Logger         *slog.Logger
	Attribution    string
	RequestTimeout time.Duration
	MaxInFlight    int
	RateLimitRPS   float64
	RateLimitBurst int
	// TrustProxyHeaders trusts X-Forwarded-For and incoming W3C traceparent headers.
	// Enable it only behind a proxy you control: otherwise clients could spoof their
	// address, pick trace IDs or force sampling.
	TrustProxyHeaders bool
	// TracerProvider defaults to the global provider (see telemetry.SetupTracing).
	TracerProvider trace.TracerProvider
}

// NewHandler builds the complete HTTP handler.
//
// Request path: otelhttp (server span) → requestContext → observe (metrics, access
// log) → recoverer → securityHeaders → ServeMux. API routes are additionally wrapped in
// loadShedder → clientLimiter → withTimeout. Operational endpoints (/livez, /readyz,
// /health, /metrics) are deliberately exempt from shedding and rate limiting so
// probes and scrapes keep working when the API is saturated.
func NewHandler(o Options) http.Handler {
	m := newMetrics(o.Registry)
	a := &api{svc: o.Service, attribution: o.Attribution, log: o.Logger}

	shed := newLoadShedder(o.MaxInFlight, m)
	limiter := newClientLimiter(o.RateLimitRPS, o.RateLimitBurst, o.TrustProxyHeaders, m)
	protect := func(h http.HandlerFunc) http.Handler {
		return shed.wrap(limiter.wrap(withTimeout(o.RequestTimeout, h)))
	}

	mux := http.NewServeMux()
	mux.Handle("GET /v1/air-quality/current", protect(a.current))
	mux.Handle("GET /v1/air-quality/forecast", protect(a.forecast))
	mux.Handle("GET /v1/air-quality/{location}", protect(a.currentByLocation))

	mux.Handle("GET /livez", o.Health.LivenessHandler())
	mux.Handle("GET /readyz", o.Health.ReadinessHandler())
	mux.Handle("GET /health", o.Health.HealthHandler())
	mux.Handle("GET /metrics", telemetry.MetricsHandler(o.Registry, o.Logger))
	mux.HandleFunc("GET /{$}", index)

	var h http.Handler = mux
	h = securityHeaders(h)
	h = recoverer(m, o.Logger, h)
	h = observe(mux, m, o.Logger, o.TrustProxyHeaders, h)
	h = requestContext(h)

	tp := o.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	return otelhttp.NewHandler(h, "http.server",
		otelhttp.WithTracerProvider(tp),
		otelhttp.WithPropagators(propagation.TraceContext{}),
		// An untrusted caller's trace context becomes a span link rather than the parent.
		otelhttp.WithPublicEndpointFn(func(*http.Request) bool { return !o.TrustProxyHeaders }),
		// Probes and scrapes are high-volume and uninteresting; they still get metrics.
		otelhttp.WithFilter(func(r *http.Request) bool { return !quietRoutes[r.URL.Path] }),
	)
}

// index lists the API surface so the service is self-describing.
func index(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"service": "airq",
		"version": version.Get().Version,
		"endpoints": []string{
			"GET /v1/air-quality/{location}?country=CC",
			"GET /v1/air-quality/current?lat={lat}&lon={lon}",
			"GET /v1/air-quality/current?location={name}&country=CC",
			"GET /v1/air-quality/forecast?lat={lat}&lon={lon}&hours=1..96",
			"GET /health",
			"GET /livez",
			"GET /readyz",
			"GET /metrics",
		},
	})
}
