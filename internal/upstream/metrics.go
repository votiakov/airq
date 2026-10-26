package upstream

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/votiakov/airq/internal/resilience"
	"github.com/votiakov/airq/internal/telemetry"
)

// Metrics are shared by all upstream clients and distinguished by the "upstream" label.
type Metrics struct {
	calls        *prometheus.CounterVec
	attempts     *prometheus.CounterVec
	duration     *prometheus.HistogramVec
	retries      *prometheus.CounterVec
	inFlight     *prometheus.GaugeVec
	limiterWait  *prometheus.HistogramVec
	circuitState *prometheus.GaugeVec
	transitions  *prometheus.CounterVec
	conns        *prometheus.CounterVec
}

// NewMetrics registers the upstream metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	ns, sub := telemetry.Namespace, "upstream"
	m := &Metrics{
		calls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "calls_total",
			Help: "Logical upstream calls (after retries) by final outcome.",
		}, []string{"upstream", "outcome"}),
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "attempts_total",
			Help: "Individual upstream attempts by outcome, including ones rejected locally (circuit_open, throttled).",
		}, []string{"upstream", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "attempt_duration_seconds",
			Help:    "Latency of upstream attempts that reached the network.",
			Buckets: []float64{.025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"upstream", "outcome"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "retries_total",
			Help: "Upstream retries performed.",
		}, []string{"upstream"}),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "in_flight_requests",
			Help: "Upstream requests currently holding a bulkhead slot.",
		}, []string{"upstream"}),
		limiterWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "rate_limiter_wait_seconds",
			Help:    "Time spent waiting for the outbound quota guard.",
			Buckets: []float64{.001, .01, .05, .1, .25, .5, 1, 2.5},
		}, []string{"upstream"}),
		circuitState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "circuit_state",
			Help: "Circuit breaker state: 0=closed, 1=half_open, 2=open.",
		}, []string{"upstream"}),
		transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "circuit_transitions_total",
			Help: "Circuit breaker state transitions by target state.",
		}, []string{"upstream", "state"}),
		conns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "connections_total",
			Help: "Connections obtained for upstream requests, by whether they were reused from the pool.",
		}, []string{"upstream", "reused"}),
	}
	reg.MustRegister(m.calls, m.attempts, m.duration, m.retries, m.inFlight,
		m.limiterWait, m.circuitState, m.transitions, m.conns)
	return m
}

var outcomes = []string{
	"success", string(KindTimeout), string(KindNetwork), string(KindRateLimited), string(KindServer),
	string(KindClient), string(KindBadResponse), string(KindCircuitOpen), string(KindThrottled), string(KindCanceled),
}

func (m *Metrics) init(upstream string) {
	for _, o := range outcomes {
		m.calls.WithLabelValues(upstream, o)
		m.attempts.WithLabelValues(upstream, o)
	}
	m.retries.WithLabelValues(upstream)
	m.inFlight.WithLabelValues(upstream)
	m.circuitState.WithLabelValues(upstream).Set(float64(resilience.StateClosed))
	for _, s := range []resilience.State{resilience.StateClosed, resilience.StateHalfOpen, resilience.StateOpen} {
		m.transitions.WithLabelValues(upstream, s.String())
	}
	m.conns.WithLabelValues(upstream, "true")
	m.conns.WithLabelValues(upstream, "false")
}
