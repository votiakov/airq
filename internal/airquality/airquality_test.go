package airquality

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type unavailableErr struct{}

func (unavailableErr) Error() string     { return "vendor down" }
func (unavailableErr) Unavailable() bool { return true }

type badRequestErr struct{}

func (badRequestErr) Error() string     { return "bad request" }
func (badRequestErr) Unavailable() bool { return false }

type fakeProvider struct {
	mu        sync.Mutex
	calls     atomic.Int32
	err       error
	delay     time.Duration
	locations []Location
	lastHours int
	lastName  string
}

func (p *fakeProvider) setErr(err error) {
	p.mu.Lock()
	p.err = err
	p.mu.Unlock()
}

func (p *fakeProvider) wait(ctx context.Context) error {
	p.calls.Add(1)
	p.mu.Lock()
	err, delay := p.err, p.delay
	p.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (p *fakeProvider) Current(ctx context.Context, c Coordinates) (Current, error) {
	if err := p.wait(ctx); err != nil {
		return Current{}, err
	}
	return Current{Coordinates: c, Current: Reading{Time: time.Unix(int64(p.calls.Load()), 0)}}, nil
}

func (p *fakeProvider) Forecast(ctx context.Context, c Coordinates, hours int) (Forecast, error) {
	if err := p.wait(ctx); err != nil {
		return Forecast{}, err
	}
	p.mu.Lock()
	p.lastHours = hours
	p.mu.Unlock()
	f := Forecast{Coordinates: c}
	for i := range hours {
		f.Hourly = append(f.Hourly, Reading{Time: time.Unix(int64(i*3600), 0)})
	}
	return f, nil
}

func (p *fakeProvider) Geocode(ctx context.Context, name, _ string) ([]Location, error) {
	if err := p.wait(ctx); err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.lastName = name
	p.mu.Unlock()
	return p.locations, nil
}

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newService(t *testing.T, p Provider) (*Service, *testClock, *prometheus.Registry) {
	t.Helper()
	clk := &testClock{t: time.Unix(1_800_000_000, 0)}
	reg := prometheus.NewRegistry()
	s := NewService(p, Options{
		MaxEntries: 100, CurrentTTL: time.Minute, ForecastTTL: time.Minute, GeocodeTTL: time.Hour,
		NotFoundTTL: time.Minute, StaleFor: time.Hour, FetchTimeout: time.Second,
		Registerer: reg, Now: clk.now,
	})
	return s, clk, reg
}

var berlin = Coordinates{Latitude: 52.5244, Longitude: 13.4105}

func TestCurrentCachesAndNormalizes(t *testing.T) {
	p := &fakeProvider{}
	s, clk, _ := newService(t, p)
	ctx := context.Background()

	r1, err := s.Current(ctx, berlin)
	if err != nil || r1.Status != CacheMiss || r1.TTL != time.Minute {
		t.Fatalf("first call: %+v %v", r1, err)
	}
	if r1.Value.Coordinates != (Coordinates{52.52, 13.41}) {
		t.Errorf("provider must receive normalized coordinates, got %+v", r1.Value.Coordinates)
	}

	clk.advance(10 * time.Second)
	// A nearby point rounds to the same grid key and is served from cache.
	r2, err := s.Current(ctx, Coordinates{Latitude: 52.5191, Longitude: 13.4149})
	if err != nil || r2.Status != CacheHit || r2.Age != 10*time.Second || r2.TTL != 50*time.Second {
		t.Fatalf("second call: %+v %v", r2, err)
	}
	if p.calls.Load() != 1 {
		t.Errorf("provider calls = %d, want 1", p.calls.Load())
	}

	clk.advance(time.Minute)
	r3, _ := s.Current(ctx, berlin)
	if r3.Status != CacheMiss || p.calls.Load() != 2 {
		t.Errorf("expired entry must be refetched: %+v calls=%d", r3, p.calls.Load())
	}
}

func TestStaleIfError(t *testing.T) {
	p := &fakeProvider{}
	s, clk, reg := newService(t, p)
	ctx := context.Background()
	if _, err := s.Current(ctx, berlin); err != nil {
		t.Fatal(err)
	}

	clk.advance(2 * time.Minute)
	p.setErr(unavailableErr{})
	r, err := s.Current(ctx, berlin)
	if err != nil || r.Status != CacheStale || r.TTL != 0 || r.Age != 2*time.Minute {
		t.Fatalf("expected stale fallback, got %+v %v", r, err)
	}
	if got := testutil.ToFloat64(counter(t, reg, "airq_cache_stale_served_total", "current")); got != 1 {
		t.Errorf("stale_served = %v", got)
	}

	// Non-availability errors (e.g. invalid request) must surface, not hide behind stale data.
	p.setErr(badRequestErr{})
	if _, err := s.Current(ctx, berlin); !errors.As(err, new(badRequestErr)) {
		t.Fatalf("err = %v, want bad request", err)
	}

	// Past the stale window, failures surface.
	clk.advance(2 * time.Hour)
	p.setErr(unavailableErr{})
	if _, err := s.Current(ctx, berlin); err == nil {
		t.Fatal("expected error once the stale window has passed")
	}
}

func TestCoalescesConcurrentMisses(t *testing.T) {
	p := &fakeProvider{delay: 50 * time.Millisecond}
	s, _, reg := newService(t, p)

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if _, err := s.Current(context.Background(), berlin); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if p.calls.Load() != 1 {
		t.Errorf("provider calls = %d, want 1 (coalesced)", p.calls.Load())
	}
	if got := testutil.ToFloat64(counter(t, reg, "airq_cache_coalesced_requests_total", "current")); got < 1 {
		t.Errorf("coalesced counter = %v", got)
	}
}

func TestCallerCancellationDoesNotPoisonSharedFetch(t *testing.T) {
	p := &fakeProvider{delay: 50 * time.Millisecond}
	s, _, _ := newService(t, p)

	impatient, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := s.Current(impatient, berlin)
		errc <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("impatient caller err = %v", err)
	}

	// A patient caller joining the same flight still gets the data, and it is cached.
	if _, err := s.Current(context.Background(), berlin); err != nil {
		t.Fatalf("patient caller: %v", err)
	}
	r, _ := s.Current(context.Background(), berlin)
	if r.Status != CacheHit || p.calls.Load() != 1 {
		t.Errorf("shared fetch should have been cached: %+v calls=%d", r, p.calls.Load())
	}
}

func TestDeadlineFallsBackToStale(t *testing.T) {
	p := &fakeProvider{}
	s, clk, _ := newService(t, p)
	if _, err := s.Current(context.Background(), berlin); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Minute)
	p.mu.Lock()
	p.delay = 500 * time.Millisecond
	p.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	r, err := s.Current(ctx, berlin)
	if err != nil || r.Status != CacheStale {
		t.Fatalf("slow upstream past the request deadline should yield stale data: %+v %v", r, err)
	}
}

func TestForecastFetchesFullHorizonOnceAndSlices(t *testing.T) {
	p := &fakeProvider{}
	s, _, _ := newService(t, p)
	ctx := context.Background()

	r, err := s.Forecast(ctx, berlin, 3)
	if err != nil || len(r.Value.Hourly) != 3 {
		t.Fatalf("forecast: %v len=%d", err, len(r.Value.Hourly))
	}
	if p.lastHours != MaxForecastHours {
		t.Errorf("provider asked for %d hours, want %d", p.lastHours, MaxForecastHours)
	}
	r.Value.Hourly[0].Time = time.Time{} // mutating the result must not corrupt the cache

	r2, _ := s.Forecast(ctx, berlin, 48)
	if r2.Status != CacheHit || len(r2.Value.Hourly) != 48 || r2.Value.Hourly[0].Time.IsZero() {
		t.Fatalf("second forecast: status=%s len=%d", r2.Status, len(r2.Value.Hourly))
	}
	if r3, _ := s.Forecast(ctx, berlin, 10_000); len(r3.Value.Hourly) != MaxForecastHours {
		t.Errorf("hours must be clamped, got %d", len(r3.Value.Hourly))
	}
	if p.calls.Load() != 1 {
		t.Errorf("provider calls = %d, want 1", p.calls.Load())
	}
}

func TestResolveLocationWithNegativeCaching(t *testing.T) {
	p := &fakeProvider{}
	s, clk, _ := newService(t, p)
	ctx := context.Background()

	if _, err := s.ResolveLocation(ctx, "Atlantis", ""); !errors.Is(err, ErrLocationNotFound) {
		t.Fatalf("err = %v", err)
	}
	if _, err := s.ResolveLocation(ctx, "  ATLANTIS ", ""); !errors.Is(err, ErrLocationNotFound) {
		t.Fatalf("err = %v", err)
	}
	if p.calls.Load() != 1 {
		t.Errorf("negative result must be cached, calls = %d", p.calls.Load())
	}

	clk.advance(2 * time.Minute) // past NotFoundTTL
	p.mu.Lock()
	p.locations = []Location{{Name: "Berlin", CountryCode: "DE", Coordinates: berlin}}
	p.mu.Unlock()
	loc, err := s.ResolveLocation(ctx, "New   York", "us")
	if err != nil || loc.Value.Name != "Berlin" {
		t.Fatalf("resolve: %+v %v", loc, err)
	}
	if p.lastName != "new york" {
		t.Errorf("provider got name %q, want normalized", p.lastName)
	}
}

func TestCoordinatesNormalizeAndKey(t *testing.T) {
	tests := []struct {
		in   Coordinates
		want string
	}{
		{Coordinates{52.524370, 13.410530}, "52.52,13.41"},
		{Coordinates{-0.001, 0.004}, "0.00,0.00"},
		{Coordinates{-33.8688, 151.2093}, "-33.87,151.21"},
		{Coordinates{90, -180}, "90.00,-180.00"},
	}
	for _, tt := range tests {
		if got := tt.in.Normalize().Key(); got != tt.want {
			t.Errorf("%+v -> %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestAQICategories(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	eu := map[float64]string{0: "good", 20: "good", 21: "fair", 40: "fair", 55: "moderate", 80: "poor", 100.6: "extremely_poor", 95: "very_poor", 150: "extremely_poor"}
	for v, want := range eu {
		if got := NewEuropeanAQI(f(v)); got.Category != want {
			t.Errorf("EAQI %v = %s, want %s", v, got.Category, want)
		}
	}
	us := map[float64]string{10: "good", 50: "good", 51: "moderate", 120: "unhealthy_for_sensitive_groups", 180: "unhealthy", 250: "very_unhealthy", 400: "hazardous"}
	for v, want := range us {
		if got := NewUSAQI(f(v)); got.Category != want {
			t.Errorf("US AQI %v = %s, want %s", v, got.Category, want)
		}
	}
	if NewEuropeanAQI(nil) != nil || NewUSAQI(nil) != nil {
		t.Error("missing values must stay nil")
	}
}

func counter(t *testing.T, reg *prometheus.Registry, name, cacheName string) prometheus.Collector {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "cache" && l.GetValue() == cacheName {
					c := prometheus.NewCounter(prometheus.CounterOpts{Name: "copy", Help: "copy"})
					c.Add(m.GetCounter().GetValue())
					return c
				}
			}
		}
	}
	t.Fatalf("metric %s{cache=%q} not found", name, cacheName)
	return nil
}
