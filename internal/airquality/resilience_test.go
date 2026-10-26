package airquality

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// panickingProvider panics in Current, as a nil dereference in an adapter would.
type panickingProvider struct{ fakeProvider }

func (*panickingProvider) Current(context.Context, Coordinates) (Current, error) {
	var m map[string]int
	m["boom"] = 1 //nolint:govet // the nil-map panic is the point of this test
	return Current{}, nil
}

func TestPanicInSharedFetchIsRecoveredAndCounted(t *testing.T) {
	s, _, reg := newService(t, &panickingProvider{})
	// singleflight re-raises a leader's panic in its own goroutine; without recovery
	// inside the fetch this would abort the whole test binary.
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			_, err := s.Current(context.Background(), berlin)
			if !errors.Is(err, errFetchPanic) {
				t.Errorf("err = %v, want errFetchPanic", err)
			}
		})
	}
	wg.Wait()
	if got := counterValue(t, reg, "airq_cache_fetch_panics_recovered_total", "current"); got < 1 {
		t.Errorf("panics counter = %v, want >= 1", got)
	}
	if canServeStale(errFetchPanic) {
		t.Error("a panic is our bug: it must not be hidden behind stale data")
	}
	// The service still works afterwards.
	if _, err := s.ResolveLocation(context.Background(), "x", ""); errors.Is(err, errFetchPanic) {
		t.Errorf("unrelated lookups must be unaffected: %v", err)
	}
}

func TestFlightLeaderDoesNotRefetchWhatAnotherFlightJustStored(t *testing.T) {
	p := &fakeProvider{}
	s, _, _ := newService(t, p)
	ctx := context.Background()
	if _, err := s.Current(ctx, berlin); err != nil {
		t.Fatal(err)
	}
	// Simulate a caller that looked up the cache just before the value above landed.
	res := flight(ctx, s, s.current, "current", berlin.Normalize().Key(),
		func(Current) time.Duration { return time.Minute },
		func(context.Context) (Current, error) { p.calls.Add(1); return Current{}, nil })
	if r := <-res; r.Err != nil {
		t.Fatalf("flight: %v", r.Err)
	}
	if p.calls.Load() != 1 {
		t.Errorf("provider calls = %d, want 1: the flight must re-check the cache", p.calls.Load())
	}
}

func TestStaleWhileRevalidateServesAtOnceAndRefreshesInBackground(t *testing.T) {
	p := &fakeProvider{}
	s, clk, reg := newService(t, p)
	s.opts.RevalidateWindow = 30 * time.Second
	ctx := context.Background()
	first, err := s.Current(ctx, berlin)
	if err != nil {
		t.Fatal(err)
	}

	clk.advance(time.Minute + 5*time.Second) // 5s past expiry, inside the window
	p.mu.Lock()
	p.delay = 100 * time.Millisecond // the refresh is slow; the caller must not wait for it
	p.mu.Unlock()
	start := time.Now()
	r, err := s.Current(ctx, berlin)
	if err != nil || r.Status != CacheRevalidating || !r.Value.Current.Time.Equal(first.Value.Current.Time) {
		t.Fatalf("got %+v %v, want the old value marked revalidating", r, err)
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Errorf("request waited %v for the vendor", time.Since(start))
	}

	deadline := time.Now().Add(2 * time.Second)
	for revalidations(t, reg, "success") < 1 {
		if time.Now().After(deadline) {
			t.Fatal("background refresh never completed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	r, err = s.Current(ctx, berlin)
	if err != nil || r.Status != CacheHit || r.Value.Current.Time.Equal(first.Value.Current.Time) {
		t.Errorf("after the refresh want a fresh hit with new data, got %+v %v", r, err)
	}
	if got := counterValue(t, reg, "airq_cache_stale_served_total", "current"); got != 0 {
		t.Errorf("stale_served = %v: revalidation is not an upstream failure", got)
	}
}

func TestPastTheRevalidateWindowRequestsWaitForTheVendor(t *testing.T) {
	p := &fakeProvider{}
	s, clk, _ := newService(t, p)
	s.opts.RevalidateWindow = 30 * time.Second
	ctx := context.Background()
	if _, err := s.Current(ctx, berlin); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Minute)
	r, err := s.Current(ctx, berlin)
	if err != nil || r.Status != CacheMiss {
		t.Fatalf("got %+v %v, want a synchronous miss", r, err)
	}
}

func TestFailedRevalidationKeepsTheEntryForStaleIfError(t *testing.T) {
	p := &fakeProvider{}
	s, clk, reg := newService(t, p)
	s.opts.RevalidateWindow = 30 * time.Second
	ctx := context.Background()
	if _, err := s.Current(ctx, berlin); err != nil {
		t.Fatal(err)
	}
	clk.advance(time.Minute + time.Second)
	p.setErr(unavailableErr{})
	if r, _ := s.Current(ctx, berlin); r.Status != CacheRevalidating {
		t.Fatalf("status = %v", r.Status)
	}
	deadline := time.Now().Add(2 * time.Second)
	for revalidations(t, reg, "error") < 1 {
		if time.Now().After(deadline) {
			t.Fatal("failed refresh was not counted")
		}
		time.Sleep(5 * time.Millisecond)
	}
	clk.advance(time.Minute) // beyond the revalidate window, inside the stale window
	r, err := s.Current(ctx, berlin)
	if err != nil || r.Status != CacheStale {
		t.Errorf("got %+v %v, want stale-if-error", r, err)
	}
}

func TestMissGateLimitsVendorFetchesPerCaller(t *testing.T) {
	p := &fakeProvider{}
	s, clk, _ := newService(t, p)
	var allowed atomic.Bool
	gate := func() (time.Duration, bool) { return 7 * time.Second, allowed.Load() }
	ctx := WithMissGate(context.Background(), gate)

	// Nothing cached and the caller is out of budget: refused, and no vendor call.
	_, err := s.Current(ctx, berlin)
	var mbe *MissBudgetError
	if !errors.As(err, &mbe) || mbe.RetryAfter != 7*time.Second || p.calls.Load() != 0 {
		t.Fatalf("err = %v calls = %d, want MissBudgetError and no vendor call", err, p.calls.Load())
	}

	allowed.Store(true)
	if _, err := s.Current(ctx, berlin); err != nil {
		t.Fatal(err)
	}
	// Fresh hits are free whatever the budget.
	allowed.Store(false)
	if r, err := s.Current(ctx, berlin); err != nil || r.Status != CacheHit {
		t.Fatalf("hit must not need budget: %+v %v", r, err)
	}
	// Expired and out of budget: the old copy beats refusing.
	clk.advance(2 * time.Minute)
	callsBefore := p.calls.Load()
	if r, err := s.Current(ctx, berlin); err != nil || r.Status != CacheStale || p.calls.Load() != callsBefore {
		t.Fatalf("got %+v %v calls %d->%d, want stale without a vendor call", r, err, callsBefore, p.calls.Load())
	}
}

// revalidations reads airq_cache_revalidations_total{cache="current",result=result}.
func revalidations(t *testing.T, reg *prometheus.Registry, result string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "airq_cache_revalidations_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["cache"] == "current" && labels["result"] == result {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}
