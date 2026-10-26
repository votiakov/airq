package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func get(t *testing.T, h http.Handler) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Error("health responses must not be cached")
	}
	return rec.Code, body
}

func TestAllHealthy(t *testing.T) {
	c := NewChecker(prometheus.NewRegistry(), 0,
		Check{Name: "db", Timeout: time.Second, Run: func(context.Context) error { return nil }})
	code, body := get(t, c.HealthHandler())
	if code != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("code=%d body=%v", code, body)
	}
	if body["version"] == nil || body["uptime"] == nil {
		t.Error("report must include version and uptime")
	}
}

func TestSoftFailureDegradesButStaysReady(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := NewChecker(reg, 0,
		Check{Name: "database", Timeout: time.Second, Run: func(context.Context) error { return errors.New("connection refused") }},
		Check{Name: "ok", Timeout: time.Second, Run: func(context.Context) error { return nil }},
	)
	code, body := get(t, c.HealthHandler())
	if code != http.StatusOK || body["status"] != "degraded" {
		t.Fatalf("code=%d body=%v", code, body)
	}
	db := body["checks"].(map[string]any)["database"].(map[string]any)
	if db["status"] != "fail" || db["error"] != "connection refused" {
		t.Errorf("database check = %v", db)
	}
	if code, _ := get(t, c.ReadinessHandler()); code != http.StatusOK {
		t.Errorf("a soft failure must not fail readiness, got %d", code)
	}
	if got := testutil.ToFloat64(c.status.WithLabelValues("database", "false")); got != 0 {
		t.Errorf("check_up{database} = %v", got)
	}
}

func TestCriticalFailureIsDown(t *testing.T) {
	c := NewChecker(prometheus.NewRegistry(), 0,
		Check{Name: "core", Critical: true, Timeout: time.Second, Run: func(context.Context) error { return errors.New("broken") }})
	if code, body := get(t, c.HealthHandler()); code != http.StatusServiceUnavailable || body["status"] != "down" {
		t.Fatalf("code=%d body=%v", code, body)
	}
	if code, _ := get(t, c.ReadinessHandler()); code != http.StatusServiceUnavailable {
		t.Errorf("readiness = %d", code)
	}
	if code, _ := get(t, c.LivenessHandler()); code != http.StatusOK {
		t.Errorf("liveness must not depend on dependencies, got %d", code)
	}
}

func TestDrainingFailsReadinessOnly(t *testing.T) {
	c := NewChecker(prometheus.NewRegistry(), 0)
	c.SetDraining()
	if !c.Draining() {
		t.Fatal("Draining() = false")
	}
	if code, body := get(t, c.ReadinessHandler()); code != http.StatusServiceUnavailable || body["status"] != "draining" {
		t.Fatalf("readiness while draining: %d %v", code, body)
	}
	if code, _ := get(t, c.HealthHandler()); code != http.StatusServiceUnavailable {
		t.Errorf("health while draining = %d", code)
	}
	if code, _ := get(t, c.LivenessHandler()); code != http.StatusOK {
		t.Errorf("liveness while draining = %d", code)
	}
}

func TestChecksAreCachedAndBounded(t *testing.T) {
	var calls atomic.Int32
	c := NewChecker(prometheus.NewRegistry(), time.Minute,
		Check{Name: "slow", Timeout: 20 * time.Millisecond, Run: func(ctx context.Context) error {
			calls.Add(1)
			<-ctx.Done()
			return ctx.Err()
		}})
	start := time.Now()
	for range 5 {
		get(t, c.HealthHandler())
	}
	if calls.Load() != 1 {
		t.Errorf("check ran %d times, want 1 (cached)", calls.Load())
	}
	if time.Since(start) > time.Second {
		t.Error("check timeout was not enforced")
	}
}
