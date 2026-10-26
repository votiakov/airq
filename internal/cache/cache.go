// Package cache adds freshness policy on top of an LRU: per-entry TTLs and a stale
// window that enables stale-if-error serving (RFC 5861 semantics). An entry past its
// TTL is no longer served by default, but is kept for StaleFor so it can be returned
// when the upstream is failing.
//
// Recency tracking and capacity eviction come from hashicorp/golang-lru's simplelru.
// The non-thread-safe variant is used deliberately: expiry is lazy, and the
// "look up, find it expired, remove it" sequence must be atomic so a concurrent Set
// of a fresh value is never removed by mistake. One mutex here covers that.
package cache

import (
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/votiakov/airq/internal/telemetry"
)

// Entry is a cached value with its freshness metadata.
type Entry[V any] struct {
	Value     V
	StoredAt  time.Time
	ExpiresAt time.Time
}

// Fresh reports whether the entry is within its TTL at now.
func (e Entry[V]) Fresh(now time.Time) bool { return now.Before(e.ExpiresAt) }

// Age is how long ago the entry was stored (used for the HTTP Age header).
func (e Entry[V]) Age(now time.Time) time.Duration { return max(now.Sub(e.StoredAt), 0) }

// TTL is the remaining freshness lifetime (0 once expired).
func (e Entry[V]) TTL(now time.Time) time.Duration { return max(e.ExpiresAt.Sub(now), 0) }

// Options configures a Cache.
type Options struct {
	// Name labels the cache's metrics.
	Name string
	// MaxEntries bounds memory; the least recently used entry is evicted beyond it.
	MaxEntries int
	// Metrics is optional.
	Metrics *Metrics
	// Now is the clock; defaults to time.Now.
	Now func() time.Time
}

// Cache is safe for concurrent use.
type Cache[K comparable, V any] struct {
	name    string
	now     func() time.Time
	metrics *Metrics

	mu  sync.Mutex
	lru *simplelru.LRU[K, item[V]]
}

type item[V any] struct {
	entry      Entry[V]
	staleUntil time.Time
}

// New creates a cache.
func New[K comparable, V any](opts Options) *Cache[K, V] {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	// NewLRU only fails for a non-positive size.
	lru, _ := simplelru.NewLRU[K, item[V]](max(opts.MaxEntries, 1), nil)
	opts.Metrics.init(opts.Name)
	return &Cache[K, V]{name: opts.Name, now: opts.Now, metrics: opts.Metrics, lru: lru}
}

// Get looks key up. It returns ok=false on a miss. A found entry may be stale (check
// Entry.Fresh); entries beyond their stale window are purged and reported as misses.
func (c *Cache[K, V]) Get(key K) (Entry[V], bool) {
	now := c.now()
	c.mu.Lock()
	it, found := c.lru.Get(key)
	if found && !now.Before(it.staleUntil) {
		c.lru.Remove(key)
		n := c.lru.Len()
		c.mu.Unlock()
		c.metrics.evicted(c.name, "expired", n)
		c.metrics.lookup(c.name, "miss")
		return Entry[V]{}, false
	}
	c.mu.Unlock()

	switch {
	case !found:
		c.metrics.lookup(c.name, "miss")
		return Entry[V]{}, false
	case it.entry.Fresh(now):
		c.metrics.lookup(c.name, "hit")
	default:
		c.metrics.lookup(c.name, "stale")
	}
	return it.entry, true
}

// Peek returns the entry for key, fresh or stale, without touching recency or
// metrics. It is for re-checking inside a refresh that already counted its lookup.
func (c *Cache[K, V]) Peek(key K) (Entry[V], bool) {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	it, found := c.lru.Peek(key)
	if !found || !now.Before(it.staleUntil) {
		return Entry[V]{}, false
	}
	return it.entry, true
}

// Set stores value for ttl, keeping it for an additional staleFor as a fallback.
func (c *Cache[K, V]) Set(key K, value V, ttl, staleFor time.Duration) Entry[V] {
	now := c.now()
	e := Entry[V]{Value: value, StoredAt: now, ExpiresAt: now.Add(ttl)}

	c.mu.Lock()
	evicted := c.lru.Add(key, item[V]{entry: e, staleUntil: e.ExpiresAt.Add(max(staleFor, 0))})
	n := c.lru.Len()
	c.mu.Unlock()

	if evicted {
		c.metrics.evicted(c.name, "capacity", n)
	}
	c.metrics.size(c.name, n)
	return e
}

// Delete removes key.
func (c *Cache[K, V]) Delete(key K) {
	c.mu.Lock()
	c.lru.Remove(key)
	n := c.lru.Len()
	c.mu.Unlock()
	c.metrics.size(c.name, n)
}

// Len returns the number of entries (including stale ones).
func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}

// MarkStaleServed records that a stale entry was served because the origin failed.
func (c *Cache[K, V]) MarkStaleServed() { c.metrics.staleServed(c.name) }

// Metrics are shared by all caches and distinguished by the "cache" label.
type Metrics struct {
	lookups     *prometheus.CounterVec
	evictions   *prometheus.CounterVec
	entries     *prometheus.GaugeVec
	staleServes *prometheus.CounterVec
}

// NewMetrics registers the cache metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		lookups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: telemetry.Namespace, Subsystem: "cache", Name: "lookups_total",
			Help: "Cache lookups by result: hit (fresh), stale (expired but still in the stale window) or miss.",
		}, []string{"cache", "result"}),
		evictions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: telemetry.Namespace, Subsystem: "cache", Name: "evictions_total",
			Help: "Entries removed from the cache by reason (capacity or expired).",
		}, []string{"cache", "reason"}),
		entries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: telemetry.Namespace, Subsystem: "cache", Name: "entries",
			Help: "Current number of entries in the cache, including stale ones.",
		}, []string{"cache"}),
		staleServes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: telemetry.Namespace, Subsystem: "cache", Name: "stale_served_total",
			Help: "Stale entries served because the upstream failed (stale-if-error).",
		}, []string{"cache"}),
	}
	reg.MustRegister(m.lookups, m.evictions, m.entries, m.staleServes)
	return m
}

// init pre-creates every series for a cache so rates and alerts see zeros rather than
// absent series.
func (m *Metrics) init(name string) {
	if m == nil {
		return
	}
	for _, r := range []string{"hit", "stale", "miss"} {
		m.lookups.WithLabelValues(name, r)
	}
	for _, r := range []string{"capacity", "expired"} {
		m.evictions.WithLabelValues(name, r)
	}
	m.entries.WithLabelValues(name)
	m.staleServes.WithLabelValues(name)
}

func (m *Metrics) lookup(name, result string) {
	if m != nil {
		m.lookups.WithLabelValues(name, result).Inc()
	}
}

func (m *Metrics) evicted(name, reason string, n int) {
	if m != nil {
		m.evictions.WithLabelValues(name, reason).Inc()
		m.entries.WithLabelValues(name).Set(float64(n))
	}
}

func (m *Metrics) size(name string, n int) {
	if m != nil {
		m.entries.WithLabelValues(name).Set(float64(n))
	}
}

func (m *Metrics) staleServed(name string) {
	if m != nil {
		m.staleServes.WithLabelValues(name).Inc()
	}
}
