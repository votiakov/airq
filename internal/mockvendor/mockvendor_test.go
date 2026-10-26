package mockvendor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"

	"github.com/votiakov/airq/internal/airquality"
	"github.com/votiakov/airq/internal/openmeteo"
	"github.com/votiakov/airq/internal/upstream"
)

// The mock is only useful if the real adapter can parse it, so test through it.
func newAdapter(t *testing.T) (*openmeteo.Client, *Server) {
	t.Helper()
	mock := New()
	srv := httptest.NewServer(mock.Handler())
	t.Cleanup(srv.Close)
	m := upstream.NewMetrics(prometheus.NewRegistry())
	mk := func(name string) *upstream.Client {
		c, err := upstream.New(upstream.Config{Name: name, BaseURL: srv.URL, AttemptTimeout: time.Second,
			MaxAttempts: 1, RateLimit: rate.Inf, MaxConcurrent: 4, MaxResponseBytes: 1 << 20, Metrics: m,
			BreakerThreshold: 100, BreakerCooldown: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	return openmeteo.New(mk("air_quality"), mk("geocoding")), mock
}

func TestMockSpeaksOpenMeteo(t *testing.T) {
	c, mock := newAdapter(t)
	ctx := context.Background()

	cur, err := c.Current(ctx, airquality.Coordinates{Latitude: 52.52, Longitude: 13.41})
	if err != nil {
		t.Fatal(err)
	}
	if cur.Current.EuropeanAQI == nil || cur.Current.Pollutants.PM25 == nil || cur.Current.Pollutants.Ammonia == nil {
		t.Errorf("current = %+v", cur.Current)
	}
	again, _ := c.Current(ctx, airquality.Coordinates{Latitude: 52.52, Longitude: 13.41})
	if *again.Current.Pollutants.PM25 != *cur.Current.Pollutants.PM25 {
		t.Error("values must be deterministic within the hour")
	}

	f, err := c.Forecast(ctx, airquality.Coordinates{Latitude: -33.87, Longitude: 151.21}, 5)
	if err != nil || len(f.Hourly) != 5 || f.Hourly[0].Pollutants.Ammonia != nil {
		t.Fatalf("forecast: %v len=%d", err, len(f.Hourly))
	}

	locs, err := c.Geocode(ctx, "paris", "US")
	if err != nil || len(locs) != 1 || locs[0].Admin1 != "Texas" {
		t.Fatalf("geocode = %+v %v", locs, err)
	}
	if locs, _ := c.Geocode(ctx, "atlantis", ""); len(locs) != 0 {
		t.Errorf("unknown place = %+v", locs)
	}
	if mock.Calls("/v1/air-quality") != 3 || mock.Calls("/v1/search") != 2 {
		t.Errorf("call counts = %d/%d", mock.Calls("/v1/air-quality"), mock.Calls("/v1/search"))
	}
}

func TestFaultInjection(t *testing.T) {
	c, mock := newAdapter(t)
	mock.SetFaults(Faults{FailureRate: 1, Status: http.StatusBadGateway})
	_, err := c.Current(context.Background(), airquality.Coordinates{Latitude: 1, Longitude: 1})
	var uerr *upstream.Error
	if !errors.As(err, &uerr) || uerr.Kind != upstream.KindServer || uerr.StatusCode != http.StatusBadGateway {
		t.Fatalf("err = %v", err)
	}
	mock.SetFaults(Faults{})
	if _, err := c.Current(context.Background(), airquality.Coordinates{Latitude: 1, Longitude: 1}); err != nil {
		t.Fatalf("after clearing faults: %v", err)
	}
}

func TestFaultsEndpoint(t *testing.T) {
	mock := New()
	h := mock.Handler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/__faults?failure_rate=0.5&latency=10ms&status=429", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("POST = %d %s", rec.Code, rec.Body)
	}
	mock.mu.Lock()
	f := mock.faults
	mock.mu.Unlock()
	if f.FailureRate != 0.5 || f.Latency != 10*time.Millisecond || f.Status != http.StatusTooManyRequests {
		t.Errorf("faults = %+v", f)
	}
	for _, bad := range []string{"failure_rate=2", "status=200", "latency=5m"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/__faults?"+bad, nil))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s accepted", bad)
		}
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/__faults", nil))
	if rec.Code != http.StatusNoContent || mock.faults.FailureRate != 0 {
		t.Error("DELETE should reset faults")
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/air-quality?latitude=99&longitude=0&current=pm10", nil))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("invalid latitude = %d", rec.Code)
	}
}
