package resilience

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type transitions struct {
	mu   sync.Mutex
	seen []string
}

func (tr *transitions) record(from, to State) {
	tr.mu.Lock()
	tr.seen = append(tr.seen, from.String()+"->"+to.String())
	tr.mu.Unlock()
}

func newTestBreaker(clock *fakeClock, tr *transitions) *Breaker {
	return NewBreaker(BreakerConfig{
		FailureThreshold: 3,
		Cooldown:         10 * time.Second,
		HalfOpenProbes:   1,
		Now:              clock.Now,
		OnStateChange:    tr.record,
	})
}

func call(t *testing.T, b *Breaker, o Outcome) {
	t.Helper()
	done, err := b.Allow()
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	done(o)
}

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	tr := &transitions{}
	b := newTestBreaker(clock, tr)

	call(t, b, Failure)
	call(t, b, Failure)
	call(t, b, Success) // resets the streak
	call(t, b, Failure)
	call(t, b, Failure)
	if b.State() != StateClosed {
		t.Fatalf("state = %s, want closed (streak was reset)", b.State())
	}
	call(t, b, Failure)
	if b.State() != StateOpen {
		t.Fatalf("state = %s, want open", b.State())
	}
	if _, err := b.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Allow while open: err = %v", err)
	}
	if got := b.RetryAfter(); got != 10*time.Second {
		t.Errorf("RetryAfter = %s, want 10s", got)
	}
}

func TestBreakerHalfOpenRecovery(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	tr := &transitions{}
	b := newTestBreaker(clock, tr)
	for range 3 {
		call(t, b, Failure)
	}

	clock.Advance(10 * time.Second)
	if b.State() != StateHalfOpen {
		t.Fatalf("state after cooldown = %s, want half_open", b.State())
	}
	probe, err := b.Allow()
	if err != nil {
		t.Fatalf("probe rejected: %v", err)
	}
	if _, err := b.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatal("second concurrent probe must be rejected")
	}
	probe(Success)
	if b.State() != StateClosed {
		t.Fatalf("state = %s, want closed after successful probe", b.State())
	}

	want := []string{"closed->open", "open->half_open", "half_open->closed"}
	if len(tr.seen) != len(want) {
		t.Fatalf("transitions = %v, want %v", tr.seen, want)
	}
	for i := range want {
		if tr.seen[i] != want[i] {
			t.Fatalf("transitions = %v, want %v", tr.seen, want)
		}
	}
}

func TestBreakerHalfOpenFailureReopens(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	b := newTestBreaker(clock, &transitions{})
	for range 3 {
		call(t, b, Failure)
	}
	clock.Advance(10 * time.Second)
	call(t, b, Failure)
	if b.State() != StateOpen {
		t.Fatalf("state = %s, want open after failed probe", b.State())
	}
	if b.RetryAfter() != 10*time.Second {
		t.Errorf("cooldown should restart, RetryAfter = %s", b.RetryAfter())
	}
}

func TestBreakerIgnoredOutcomes(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	b := newTestBreaker(clock, &transitions{})
	for range 10 {
		call(t, b, Ignored)
	}
	if b.State() != StateClosed {
		t.Fatal("ignored outcomes must not open the circuit")
	}
	for range 3 {
		call(t, b, Failure)
	}
	clock.Advance(10 * time.Second)
	call(t, b, Ignored) // releases the probe slot without deciding
	if b.State() != StateHalfOpen {
		t.Fatalf("state = %s, want still half_open", b.State())
	}
	call(t, b, Success)
	if b.State() != StateClosed {
		t.Fatalf("state = %s, want closed", b.State())
	}
}

func TestBreakerDiscardsStaleGenerationOutcomes(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	b := newTestBreaker(clock, &transitions{})

	slow, err := b.Allow() // admitted while closed
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		call(t, b, Failure)
	}
	clock.Advance(10 * time.Second)
	probe, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}

	slow(Failure) // late report from the old generation must be ignored
	if b.State() != StateHalfOpen {
		t.Fatalf("stale outcome changed state to %s", b.State())
	}
	probe(Success)
	probe(Failure) // done is idempotent
	if b.State() != StateClosed {
		t.Fatalf("state = %s, want closed", b.State())
	}
}

func TestBreakerConcurrentUse(_ *testing.T) {
	b := NewBreaker(BreakerConfig{FailureThreshold: 1000, Cooldown: time.Second})
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			for range 100 {
				if done, err := b.Allow(); err == nil {
					done(Outcome(i % 3))
				}
				_ = b.State()
			}
		})
	}
	wg.Wait()
}

func TestBackoffDelay(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Max: time.Second, Rand: func() float64 { return 0.999999 }}
	for n, want := range []time.Duration{100, 200, 400, 800, 1000, 1000} {
		got := b.Delay(n).Round(time.Millisecond)
		if got != want*time.Millisecond {
			t.Errorf("Delay(%d) = %s, want ~%dms", n, got, want)
		}
	}
	if d := b.Delay(200); d > time.Second || d < 0 {
		t.Errorf("huge retry count must be capped, got %s", d)
	}
	zero := Backoff{Base: time.Second, Max: time.Second, Rand: func() float64 { return 0 }}
	if zero.Delay(3) != 0 {
		t.Error("full jitter must allow a zero delay")
	}
	real := Backoff{Base: 10 * time.Millisecond, Max: 50 * time.Millisecond}
	for n := range 10 {
		if d := real.Delay(n); d < 0 || d >= 50*time.Millisecond {
			t.Errorf("Delay(%d) = %s out of range", n, d)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"", 0, false},
		{"5", 5 * time.Second, true},
		{" 0 ", 0, true},
		{"-1", 0, false},
		{"soon", 0, false},
		{now.Add(30 * time.Second).Format(http.TimeFormat), 30 * time.Second, true},
		{now.Add(-time.Minute).Format(http.TimeFormat), 0, true},
	}
	for _, tt := range tests {
		got, ok := ParseRetryAfter(tt.in, now)
		if got != tt.want || ok != tt.ok {
			t.Errorf("ParseRetryAfter(%q) = %s,%v want %s,%v", tt.in, got, ok, tt.want, tt.ok)
		}
	}
}

func TestSleep(t *testing.T) {
	if err := Sleep(context.Background(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep on cancelled ctx = %v", err)
	}
	if err := Sleep(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep(0) on cancelled ctx = %v", err)
	}
}
