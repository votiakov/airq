package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newTestCache(t *testing.T, maxEntries int) (*Cache[string, int], *clock, *Metrics) {
	t.Helper()
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	m := NewMetrics(prometheus.NewRegistry())
	c := New[string, int](Options{Name: "test", MaxEntries: maxEntries, Metrics: m, Now: clk.now})
	return c, clk, m
}

func TestFreshStaleAndExpired(t *testing.T) {
	c, clk, m := newTestCache(t, 10)

	if _, ok := c.Get("k"); ok {
		t.Fatal("empty cache returned a value")
	}
	c.Set("k", 42, time.Minute, time.Hour)

	e, ok := c.Get("k")
	if !ok || e.Value != 42 || !e.Fresh(clk.t) {
		t.Fatalf("expected fresh hit, got %+v ok=%v", e, ok)
	}

	clk.t = clk.t.Add(2 * time.Minute)
	e, ok = c.Get("k")
	if !ok || e.Fresh(clk.t) {
		t.Fatalf("expected stale entry, got %+v ok=%v", e, ok)
	}
	if e.Age(clk.t) != 2*time.Minute || e.TTL(clk.t) != 0 {
		t.Errorf("age=%s ttl=%s", e.Age(clk.t), e.TTL(clk.t))
	}

	clk.t = clk.t.Add(time.Hour)
	if _, ok := c.Get("k"); ok {
		t.Fatal("entry beyond stale window must be purged")
	}
	if c.Len() != 0 {
		t.Errorf("Len = %d after purge", c.Len())
	}

	for result, want := range map[string]float64{"hit": 1, "stale": 1, "miss": 2} {
		if got := testutil.ToFloat64(m.lookups.WithLabelValues("test", result)); got != want {
			t.Errorf("lookups{result=%s} = %v, want %v", result, got, want)
		}
	}
	if got := testutil.ToFloat64(m.evictions.WithLabelValues("test", "expired")); got != 1 {
		t.Errorf("expired evictions = %v", got)
	}
}

func TestLRUEviction(t *testing.T) {
	c, _, m := newTestCache(t, 2)
	c.Set("a", 1, time.Minute, 0)
	c.Set("b", 2, time.Minute, 0)
	c.Get("a") // a becomes most recently used
	c.Set("c", 3, time.Minute, 0)

	if _, ok := c.Get("b"); ok {
		t.Error("least recently used entry b should have been evicted")
	}
	for _, k := range []string{"a", "c"} {
		if _, ok := c.Get(k); !ok {
			t.Errorf("%s should still be cached", k)
		}
	}
	if got := testutil.ToFloat64(m.evictions.WithLabelValues("test", "capacity")); got != 1 {
		t.Errorf("capacity evictions = %v", got)
	}
	if got := testutil.ToFloat64(m.entries.WithLabelValues("test")); got != 2 {
		t.Errorf("entries gauge = %v", got)
	}
}

func TestOverwriteAndDelete(t *testing.T) {
	c, _, _ := newTestCache(t, 2)
	c.Set("a", 1, time.Minute, 0)
	c.Set("a", 2, time.Minute, 0)
	if e, _ := c.Get("a"); e.Value != 2 || c.Len() != 1 {
		t.Fatalf("overwrite failed: %+v len=%d", e, c.Len())
	}
	c.Delete("a")
	c.Delete("missing")
	if c.Len() != 0 {
		t.Fatal("delete failed")
	}
}

func TestZeroStaleWindow(t *testing.T) {
	c, clk, _ := newTestCache(t, 2)
	c.Set("a", 1, time.Second, 0)
	clk.t = clk.t.Add(time.Second)
	if _, ok := c.Get("a"); ok {
		t.Fatal("with no stale window an expired entry must be a miss")
	}
}

func TestStaleServedMetricAndNilMetrics(t *testing.T) {
	c, _, m := newTestCache(t, 1)
	c.MarkStaleServed()
	if got := testutil.ToFloat64(m.staleServes.WithLabelValues("test")); got != 1 {
		t.Errorf("stale_served = %v", got)
	}

	plain := New[int, int](Options{MaxEntries: 1}) // no metrics: must not panic
	plain.Set(1, 1, time.Minute, 0)
	plain.Set(2, 2, time.Minute, 0)
	plain.Get(1)
	plain.MarkStaleServed()
}

func TestConcurrentAccess(t *testing.T) {
	c := New[string, int](Options{Name: "race", MaxEntries: 50, Metrics: NewMetrics(prometheus.NewRegistry())})
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			for i := range 500 {
				k := fmt.Sprintf("k%d", (g*i)%100)
				c.Set(k, i, time.Millisecond, time.Millisecond)
				c.Get(k)
				if i%50 == 0 {
					c.Delete(k)
				}
			}
		})
	}
	wg.Wait()
	if c.Len() > 50 {
		t.Fatalf("cache exceeded its bound: %d", c.Len())
	}
}
