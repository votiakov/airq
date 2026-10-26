package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/votiakov/airq/internal/mockvendor"
	"github.com/votiakov/airq/internal/store"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type instance struct {
	base   string
	mock   *mockvendor.Server
	logs   *lockedBuffer
	cancel context.CancelFunc
	done   chan error
}

// start boots the real serve() wiring against the mock vendor.
func start(t *testing.T, extra map[string]string) *instance {
	t.Helper()
	mock := mockvendor.New()
	vendor := httptest.NewServer(mock.Handler())
	t.Cleanup(vendor.Close)

	env := map[string]string{
		"HTTP_ADDR":                  "127.0.0.1:0",
		"HTTP_DRAIN_DELAY":           "0s",
		"LOG_LEVEL":                  "debug",
		"UPSTREAM_AIR_QUALITY_URL":   vendor.URL,
		"UPSTREAM_GEOCODING_URL":     vendor.URL,
		"UPSTREAM_BREAKER_THRESHOLD": "2",
		"UPSTREAM_MAX_ATTEMPTS":      "1",
		"RECORDER_FLUSH_INTERVAL":    "50ms",
		"HEALTH_CACHE_TTL":           "0s",
	}
	for k, v := range extra {
		env[k] = v
	}
	logs := &lockedBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	addr := make(chan net.Addr, 1)
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, func(k string) string { return env[k] }, logs, func(a net.Addr) { addr <- a })
	}()
	select {
	case a := <-addr:
		inst := &instance{base: "http://" + a.String(), mock: mock, logs: logs, cancel: cancel, done: done}
		t.Cleanup(func() { inst.stop(t) })
		return inst
	case err := <-done:
		t.Fatalf("serve exited during startup: %v\n%s", err, logs.String())
	case <-time.After(10 * time.Second):
		t.Fatalf("serve did not start\n%s", logs.String())
	}
	return nil
}

func (i *instance) stop(t *testing.T) {
	t.Helper()
	if i.cancel == nil {
		return
	}
	i.cancel()
	i.cancel = nil
	select {
	case err := <-i.done:
		if err != nil {
			t.Errorf("serve returned %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Error("graceful shutdown did not complete")
	}
}

func (i *instance) get(t *testing.T, path string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Get(i.base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp, string(body)
}

func TestServeEndToEnd(t *testing.T) {
	inst := start(t, nil)

	resp, body := inst.get(t, "/v1/air-quality/Berlin?country=DE")
	if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("first request: %d %s %s", resp.StatusCode, resp.Header.Get("X-Cache"), body)
	}
	var payload struct {
		Location struct{ Name string } `json:"location"`
		Current  struct {
			EuropeanAQI *struct{ Category string } `json:"european_aqi"`
		} `json:"current"`
		Attribution string `json:"attribution"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Location.Name != "Berlin" || payload.Current.EuropeanAQI == nil || !strings.Contains(payload.Attribution, "Open-Meteo") {
		t.Errorf("payload = %+v", payload)
	}

	resp, _ = inst.get(t, "/v1/air-quality/current?lat=52.52&lon=13.41")
	if resp.Header.Get("X-Cache") != "HIT" {
		t.Errorf("same grid cell should be a cache hit, got %s", resp.Header.Get("X-Cache"))
	}
	if inst.mock.Calls("/v1/air-quality") != 1 {
		t.Errorf("vendor calls = %d, want 1", inst.mock.Calls("/v1/air-quality"))
	}

	if resp, body := inst.get(t, "/v1/air-quality/forecast?location=Tokyo&hours=12"); resp.StatusCode != http.StatusOK || !strings.Contains(body, `"hourly"`) {
		t.Errorf("forecast: %d %s", resp.StatusCode, body)
	}
	if resp, _ := inst.get(t, "/v1/air-quality/Atlantis"); resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown location = %d", resp.StatusCode)
	}

	_, metrics := inst.get(t, "/metrics")
	for _, series := range []string{
		`airq_build_info{`,
		`airq_http_requests_total{code="200",method="GET",route="/v1/air-quality/{location}"} 1`,
		`airq_http_request_duration_seconds_bucket{`,
		`airq_upstream_calls_total{outcome="success",upstream="air_quality"}`,
		`airq_upstream_attempt_duration_seconds_bucket{`,
		`airq_upstream_circuit_state{upstream="geocoding"} 0`,
		`airq_cache_lookups_total{cache="current",result="hit"} 1`,
		`airq_cache_entries{cache="geocode"}`,
		`airq_health_check_up{`,
		`go_goroutines`,
	} {
		if !strings.Contains(metrics, series) {
			t.Errorf("/metrics missing %s", series)
		}
	}

	if resp, body := inst.get(t, "/health"); resp.StatusCode != http.StatusOK || !strings.Contains(body, `"status":"ok"`) {
		t.Errorf("health: %d %s", resp.StatusCode, body)
	}

	inst.stop(t)
	if !strings.Contains(inst.logs.String(), "shutdown complete") {
		t.Error("graceful shutdown was not logged")
	}
}

func TestServeSurvivesVendorOutage(t *testing.T) {
	inst := start(t, map[string]string{"CACHE_CURRENT_TTL": "1ms"})
	if resp, _ := inst.get(t, "/v1/air-quality/current?lat=10&lon=10"); resp.StatusCode != http.StatusOK {
		t.Fatalf("warm-up = %d", resp.StatusCode)
	}
	time.Sleep(5 * time.Millisecond) // entry is now stale but inside the stale window

	inst.mock.SetFaults(mockvendor.Faults{FailureRate: 1, Status: http.StatusServiceUnavailable})
	resp, _ := inst.get(t, "/v1/air-quality/current?lat=10&lon=10")
	if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Cache") != "STALE" {
		t.Fatalf("expected stale data during outage, got %d %s", resp.StatusCode, resp.Header.Get("X-Cache"))
	}

	// No cached data: the breaker opens after the threshold and fails fast with 503.
	inst.get(t, "/v1/air-quality/current?lat=20&lon=20")
	resp, body := inst.get(t, "/v1/air-quality/current?lat=30&lon=30")
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("expected 503 with Retry-After once the circuit opens, got %d %s", resp.StatusCode, body)
	}
	if _, health := inst.get(t, "/health"); !strings.Contains(health, `"status":"degraded"`) {
		t.Errorf("health should be degraded while the circuit is open: %s", health)
	}
	if resp, _ := inst.get(t, "/readyz"); resp.StatusCode != http.StatusOK {
		t.Errorf("a vendor outage must not fail readiness, got %d", resp.StatusCode)
	}
}

func TestServeWithDatabase(t *testing.T) {
	admin := os.Getenv("TEST_DATABASE_URL")
	if admin == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	dsn := createDatabase(t, admin)

	if err := run(context.Background(), []string{"migrate", "up"}, envOf(map[string]string{"DATABASE_URL": dsn}), io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	var status bytes.Buffer
	if err := run(context.Background(), []string{"migrate", "status"}, envOf(map[string]string{"MIGRATE_DATABASE_URL": dsn}), &status); err != nil ||
		!strings.Contains(status.String(), "create_upstream_responses") || strings.Contains(status.String(), "pending") {
		t.Fatalf("migrate status: %v\n%s", err, status.String())
	}

	inst := start(t, map[string]string{"DATABASE_URL": dsn})
	if resp, _ := inst.get(t, "/v1/air-quality/London"); resp.StatusCode != http.StatusOK {
		t.Fatalf("request = %d", resp.StatusCode)
	}
	requestID := ""
	if resp, _ := inst.get(t, "/v1/air-quality/current?lat=1&lon=1"); resp.StatusCode == http.StatusOK {
		requestID = resp.Header.Get("X-Request-ID")
	}
	_, health := inst.get(t, "/health")
	if !strings.Contains(health, `"database":{"status":"ok"`) || !strings.Contains(health, `"migrations":{"status":"ok"`) {
		t.Errorf("health = %s", health)
	}
	inst.stop(t) // shutdown drains the recorder

	db, err := store.Open(store.Options{URL: dsn, MaxOpenConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var rows, forRequest int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*), count(*) FILTER (WHERE request_id = $1) FROM upstream_responses`, requestID).Scan(&rows, &forRequest); err != nil {
		t.Fatal(err)
	}
	if rows != 3 || forRequest != 1 {
		t.Errorf("persisted %d rows (%d for request %s), want 3 (geocode + 2 air quality) and 1", rows, forRequest, requestID)
	}

	if err := run(context.Background(), []string{"migrate", "down", "1"}, envOf(map[string]string{"DATABASE_URL": dsn}), io.Discard); err != nil {
		t.Fatalf("migrate down: %v", err)
	}
}

func TestServeRejectsInvalidConfig(t *testing.T) {
	err := serve(context.Background(), envOf(map[string]string{"HTTP_MAX_IN_FLIGHT": "zero"}), io.Discard, nil)
	if err == nil || !strings.Contains(err.Error(), "HTTP_MAX_IN_FLIGHT") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunCommands(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"version"}, envOf(nil), &out); err != nil || !strings.Contains(out.String(), `"go_version"`) {
		t.Errorf("version: %v %s", err, out.String())
	}
	if err := run(context.Background(), []string{"bogus"}, envOf(nil), io.Discard); err == nil {
		t.Error("unknown command should fail")
	}
	if err := run(context.Background(), []string{"migrate", "up"}, envOf(nil), io.Discard); err == nil {
		t.Error("migrate without a database URL should fail")
	}
}

func TestHealthcheckCommand(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer ok.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer bad.Close()

	if err := run(context.Background(), []string{"healthcheck", ok.URL + "/livez"}, envOf(nil), io.Discard); err != nil {
		t.Errorf("healthy: %v", err)
	}
	if err := run(context.Background(), []string{"healthcheck", bad.URL}, envOf(nil), io.Discard); err == nil {
		t.Error("unhealthy target should fail")
	}
	if err := run(context.Background(), []string{"healthcheck", "http://example.com/livez"}, envOf(nil), io.Discard); err == nil {
		t.Error("non-loopback targets must be refused")
	}
	u, _ := url.Parse(ok.URL)
	if err := run(context.Background(), []string{"healthcheck"}, envOf(map[string]string{"HTTP_ADDR": ":" + u.Port()}), io.Discard); err != nil {
		t.Errorf("derived from HTTP_ADDR: %v", err)
	}
}

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func createDatabase(t *testing.T, admin string) string {
	t.Helper()
	adminDB, err := sql.Open("pgx", admin)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adminDB.Close() })
	var b [6]byte
	_, _ = rand.Read(b[:])
	name := "airq_e2e_" + hex.EncodeToString(b[:])
	if _, err := adminDB.ExecContext(context.Background(), "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = adminDB.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	u, _ := url.Parse(admin)
	u.Path = "/" + name
	return u.String()
}

func TestServeExportsTraces(t *testing.T) {
	var mu sync.Mutex
	var batches int
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/traces" {
			mu.Lock()
			batches++
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer collector.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL) // read by the OTLP exporter itself

	inst := start(t, map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": collector.URL})
	resp, _ := inst.get(t, "/v1/air-quality/current?lat=52.52&lon=13.41")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	inst.stop(t) // shutdown flushes buffered spans

	mu.Lock()
	defer mu.Unlock()
	if batches == 0 {
		t.Fatal("no spans were exported to the OTLP endpoint")
	}
}
