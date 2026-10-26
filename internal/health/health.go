// Package health implements liveness, readiness and detailed health reporting.
//
//   - /livez  answers "is the process able to serve HTTP?" It checks no
//     dependencies, because restarting a pod never fixes a database outage and doing
//     so turns a dependency blip into a restart storm.
//   - /readyz answers "should the load balancer send traffic here?" It fails while
//     the process is draining for shutdown or when a critical dependency is down.
//   - /health returns a per-component report for humans and dashboards.
//
// Checks are classified as critical or soft. The vendor API and the database are both
// soft: cached data can still be served when the vendor is down, and the database
// only holds the audit log. A soft failure reports "degraded" without taking replicas
// out of rotation, which would turn a partial outage into a total one.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/votiakov/airq/internal/telemetry"
	"github.com/votiakov/airq/internal/version"
)

// Status is the aggregated health.
type Status string

// Aggregated statuses.
const (
	StatusOK       Status = "ok"
	StatusDegraded Status = "degraded" // a soft dependency is failing
	StatusDown     Status = "down"     // a critical dependency is failing, or shutting down
)

// Check is one dependency probe.
type Check struct {
	Name     string
	Critical bool
	Timeout  time.Duration
	Run      func(ctx context.Context) error
}

// CheckResult is the outcome of one Check.
type CheckResult struct {
	Status     string  `json:"status"` // "ok" or "fail"
	Critical   bool    `json:"critical"`
	Error      string  `json:"error,omitempty"`
	DurationMS float64 `json:"duration_ms"`
}

// Report is the /health response body.
type Report struct {
	Status    Status                 `json:"status"`
	Version   version.Info           `json:"version"`
	Uptime    string                 `json:"uptime"`
	CheckedAt time.Time              `json:"checked_at"`
	Checks    map[string]CheckResult `json:"checks"`
}

// Checker runs checks and caches the report briefly so aggressive probing (several
// load balancers, Kubernetes, dashboards) cannot amplify into dependency load.
type Checker struct {
	checks   []Check
	cacheFor time.Duration
	started  time.Time

	mu       sync.Mutex
	last     Report
	lastAt   time.Time
	draining atomic.Bool

	status   *prometheus.GaugeVec
	duration *prometheus.HistogramVec
}

// NewChecker creates a Checker and registers its metrics.
func NewChecker(reg prometheus.Registerer, cacheFor time.Duration, checks ...Check) *Checker {
	c := &Checker{
		checks:   checks,
		cacheFor: cacheFor,
		started:  time.Now(),
		status: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: telemetry.Namespace, Subsystem: "health", Name: "check_up",
			Help: "Result of the last health check: 1 passing, 0 failing.",
		}, []string{"check", "critical"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: telemetry.Namespace, Subsystem: "health", Name: "check_duration_seconds",
			Help:    "Duration of health checks.",
			Buckets: []float64{.001, .005, .01, .05, .1, .25, .5, 1, 2.5},
		}, []string{"check"}),
	}
	reg.MustRegister(c.status, c.duration)
	return c
}

// SetDraining marks the service as shutting down so readiness fails and load
// balancers stop routing new requests here.
func (c *Checker) SetDraining() { c.draining.Store(true) }

// Draining reports whether shutdown has begun.
func (c *Checker) Draining() bool { return c.draining.Load() }

// Report returns the (possibly cached) health report.
func (c *Checker) Report(ctx context.Context) Report {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if c.lastAt.IsZero() || now.Sub(c.lastAt) >= c.cacheFor {
		c.last = c.run(ctx)
		c.lastAt = now
	}
	r := c.last
	r.Uptime = time.Since(c.started).Round(time.Second).String()
	if c.draining.Load() {
		r.Status = StatusDown
	}
	return r
}

func (c *Checker) run(ctx context.Context) Report {
	results := make([]CheckResult, len(c.checks))
	var wg sync.WaitGroup
	for i, chk := range c.checks {
		wg.Go(func() {
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), chk.Timeout)
			defer cancel()
			start := time.Now()
			err := chk.Run(cctx)
			elapsed := time.Since(start)

			res := CheckResult{Status: "ok", Critical: chk.Critical, DurationMS: float64(elapsed.Microseconds()) / 1000}
			up := 1.0
			if err != nil {
				res.Status, res.Error, up = "fail", err.Error(), 0
			}
			results[i] = res
			c.status.WithLabelValues(chk.Name, boolLabel(chk.Critical)).Set(up)
			c.duration.WithLabelValues(chk.Name).Observe(elapsed.Seconds())
		})
	}
	wg.Wait()

	report := Report{Status: StatusOK, Version: version.Get(), CheckedAt: time.Now().UTC(), Checks: map[string]CheckResult{}}
	for i, chk := range c.checks {
		res := results[i]
		report.Checks[chk.Name] = res
		if res.Status == "ok" {
			continue
		}
		if chk.Critical {
			report.Status = StatusDown
		} else if report.Status == StatusOK {
			report.Status = StatusDegraded
		}
	}
	return report
}

// Run refreshes the report every interval until ctx is cancelled, so the
// health_check_up metrics (and alerts built on them) stay current even when nothing
// is probing /health.
func (c *Checker) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.Report(ctx)
		}
	}
}

// LivenessHandler serves /livez.
func (c *Checker) LivenessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

// ReadinessHandler serves /readyz.
func (c *Checker) ReadinessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if c.draining.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
			return
		}
		if rep := c.Report(r.Context()); rep.Status == StatusDown {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": string(rep.Status)})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
}

// HealthHandler serves /health: 200 when ok or degraded, 503 when down.
func (c *Checker) HealthHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rep := c.Report(r.Context())
		code := http.StatusOK
		if rep.Status == StatusDown {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, rep)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func boolLabel(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
