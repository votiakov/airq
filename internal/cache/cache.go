// Package cache implements a bounded, in-memory LRU cache with per-entry TTLs and a
// stale window that enables stale-if-error serving (RFC 5861 semantics): an entry past
// its TTL is no longer served by default, but is kept for StaleFor so it can be
// returned when the upstream is failing.
package cache

import (
	"container/list"
	"sync"
	"time"

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
	max     int
	now     func() time.Time
	metrics *Metrics

	mu    sync.Mutex
	items map[K]*list.Element
	lru   *list.List // front = most recently used
}

type item[K comparable, V any] struct {
	key        K
	entry      Entry[V]
	staleUntil time.Time
}

// New creates a cache.
func New[K comparable, V any](opts Options) *Cache[K, V] {
	if opts.MaxEntries < 1 {
		opts.MaxEntries = 1
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	opts.Metrics.init(opts.Name)
	return &Cache[K, V]{
		name:    opts.Name,
		max:     opts.MaxEntries,
		now:     opts.Now,
		metrics: opts.Metrics,
		items:   make(map[K]*list.Element),
		lru:     list.New(),
	}
}

// Get looks key up. It returns ok=false on a miss. A found entry may be stale (check
// Entry.Fresh); entries beyond their stale window are purged and reported as misses.
func (c *Cache[K, V]) Get(key K) (Entry[V], bool) {
	now := c.now()
	c.mu.Lock()
	el, found := c.items[key]
	if !found {
		c.mu.Unlock()
		c.metrics.lookup(c.name, "miss")
		return Entry[V]{}, false
	}
	it := el.Value.(*item[K, V])
	if !now.Before(it.staleUntil) {
		c.removeLocked(el)
		n := len(c.items)
		c.mu.Unlock()
		c.metrics.evicted(c.name, "expired", n)
		c.metrics.lookup(c.name, "miss")
		return Entry[V]{}, false
	}
	c.lru.MoveToFront(el)
	e := it.entry
	c.mu.Unlock()

	if e.Fresh(now) {
		c.metrics.lookup(c.name, "hit")
	} else {
		c.metrics.lookup(c.name, "stale")
	}
	return e, true
}

// Set stores value for ttl, keeping it for an additional staleFor as a fallback.
func (c *Cache[K, V]) Set(key K, value V, ttl, staleFor time.Duration) Entry[V] {
	now := c.now()
	e := Entry[V]{Value: value, StoredAt: now, ExpiresAt: now.Add(ttl)}
	it := &item[K, V]{key: key, entry: e, staleUntil: e.ExpiresAt.Add(max(staleFor, 0))}

	c.mu.Lock()
	if el, ok := c.items[key]; ok {
		el.Value = it
		c.lru.MoveToFront(el)
		n := len(c.items)
		c.mu.Unlock()
		c.metrics.size(c.name, n)
		return e
	}
	c.items[key] = c.lru.PushFront(it)
	evicted := 0
	for len(c.items) > c.max {
		c.removeLocked(c.lru.Back())
		evicted++
	}
	n := len(c.items)
	c.mu.Unlock()

	for range evicted {
		c.metrics.evicted(c.name, "capacity", n)
	}
	c.metrics.size(c.name, n)
	return e
}

// Delete removes key.
func (c *Cache[K, V]) Delete(key K) {
	c.mu.Lock()
	if el, ok := c.items[key]; ok {
		c.removeLocked(el)
	}
	n := len(c.items)
	c.mu.Unlock()
	c.metrics.size(c.name, n)
}

// Len returns the number of entries (including stale ones).
func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// MarkStaleServed records that a stale entry was served because the origin failed.
func (c *Cache[K, V]) MarkStaleServed() { c.metrics.staleServed(c.name) }

func (c *Cache[K, V]) removeLocked(el *list.Element) {
	it := el.Value.(*item[K, V])
	delete(c.items, it.key)
	c.lru.Remove(el)
}

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
