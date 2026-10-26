package airquality

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"

	"github.com/votiakov/airq/internal/cache"
	"github.com/votiakov/airq/internal/telemetry"
)

// ErrLocationNotFound is returned when geocoding finds no match.
var ErrLocationNotFound = errors.New("location not found")

// Provider fetches air quality data from a vendor. Implementations return errors that
// expose `Unavailable() bool` when the vendor (rather than the request) is at fault,
// which enables stale-if-error fallback.
type Provider interface {
	Current(ctx context.Context, c Coordinates) (Current, error)
	Forecast(ctx context.Context, c Coordinates, hours int) (Forecast, error)
	Geocode(ctx context.Context, name, countryCode string) ([]Location, error)
}

// MissGate is consulted before a request that cannot be answered from a fresh cache
// entry (and would therefore spend vendor quota). It returns false, with a suggested
// wait, when the caller has used up its share. The HTTP layer attaches one per client
// so a single caller cannot drain the quota every other caller needs.
type MissGate func() (retryAfter time.Duration, ok bool)

type missGateKey struct{}

// WithMissGate attaches a MissGate to ctx.
func WithMissGate(ctx context.Context, g MissGate) context.Context {
	return context.WithValue(ctx, missGateKey{}, g)
}

// MissBudgetError is returned when the caller's MissGate refuses and no cached copy,
// however old, can stand in.
type MissBudgetError struct{ RetryAfter time.Duration }

func (e *MissBudgetError) Error() string {
	return fmt.Sprintf("uncached lookups are rate limited; retry in %s", e.RetryAfter.Round(time.Second))
}

// errFetchPanic marks a panic recovered inside a shared fetch. It is a bug in this
// service, so it is deliberately not eligible for stale fallback.
var errFetchPanic = errors.New("internal error while fetching upstream data")

// CacheStatus describes how a result was produced.
type CacheStatus string

// Cache statuses, surfaced in the X-Cache and Cache-Status response headers.
const (
	CacheHit   CacheStatus = "hit"
	CacheMiss  CacheStatus = "miss"
	CacheStale CacheStatus = "stale"
	// CacheRevalidating is a just-expired entry served immediately while one
	// background fetch refreshes it (stale-while-revalidate).
	CacheRevalidating CacheStatus = "revalidating"
)

// Result is a value with the cache metadata needed for HTTP caching headers.
type Result[T any] struct {
	Value  T
	Status CacheStatus
	Age    time.Duration // time since the data was fetched from the vendor
	TTL    time.Duration // remaining freshness; 0 for stale results
}

// Options configures a Service.
type Options struct {
	MaxEntries  int
	CurrentTTL  time.Duration
	ForecastTTL time.Duration
	GeocodeTTL  time.Duration
	NotFoundTTL time.Duration
	StaleFor    time.Duration
	// RevalidateWindow is how long after expiry an entry is served at once while a
	// background fetch refreshes it. Zero disables stale-while-revalidate.
	RevalidateWindow time.Duration
	// FetchTimeout bounds a shared (coalesced) upstream fetch, which is detached from
	// any single caller's cancellation.
	FetchTimeout time.Duration
	Registerer   prometheus.Registerer
	Logger       *slog.Logger
	Now          func() time.Time
}

// Service serves air quality data through bounded caches. Concurrent misses for the
// same key are coalesced into one upstream call (singleflight) so a burst of identical
// requests, or a cache expiry under load, cannot stampede the vendor.
type Service struct {
	provider Provider
	opts     Options
	log      *slog.Logger

	current  *cache.Cache[string, Current]
	forecast *cache.Cache[string, Forecast]
	geocode  *cache.Cache[string, []Location]

	flights      singleflight.Group
	coalesced    *prometheus.CounterVec
	panics       *prometheus.CounterVec
	revalidating *prometheus.CounterVec
}

// NewService builds the service and registers its metrics.
func NewService(p Provider, opts Options) *Service {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	cm := cache.NewMetrics(opts.Registerer)
	newCache := func(name string) cache.Options {
		return cache.Options{Name: name, MaxEntries: opts.MaxEntries, Metrics: cm, Now: opts.Now}
	}
	s := &Service{
		provider: p,
		opts:     opts,
		log:      opts.Logger.With(slog.String("component", "airquality")),
		current:  cache.New[string, Current](newCache("current")),
		forecast: cache.New[string, Forecast](newCache("forecast")),
		geocode:  cache.New[string, []Location](newCache("geocode")),
		coalesced: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: telemetry.Namespace, Subsystem: "cache", Name: "coalesced_requests_total",
			Help: "Requests that waited on an identical in-flight upstream fetch instead of issuing their own.",
		}, []string{"cache"}),
		panics: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: telemetry.Namespace, Subsystem: "cache", Name: "fetch_panics_recovered_total",
			Help: "Panics recovered inside a shared upstream fetch (a bug; the request gets a 500 and the process survives).",
		}, []string{"cache"}),
		revalidating: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: telemetry.Namespace, Subsystem: "cache", Name: "revalidations_total",
			Help: "Background refreshes of just-expired entries (stale-while-revalidate) by result.",
		}, []string{"cache", "result"}),
	}
	opts.Registerer.MustRegister(s.coalesced, s.panics, s.revalidating)
	for _, name := range []string{"current", "forecast", "geocode"} {
		s.coalesced.WithLabelValues(name)
		s.panics.WithLabelValues(name)
		s.revalidating.WithLabelValues(name, "success")
		s.revalidating.WithLabelValues(name, "error")
	}
	return s
}

// Current returns the latest reading for coordinates.
func (s *Service) Current(ctx context.Context, c Coordinates) (Result[Current], error) {
	c = c.Normalize()
	return fetch(ctx, s, s.current, "current", c.Key(),
		func(Current) time.Duration { return s.opts.CurrentTTL },
		func(ctx context.Context) (Current, error) { return s.provider.Current(ctx, c) })
}

// Forecast returns the next `hours` hourly readings (1..MaxForecastHours).
func (s *Service) Forecast(ctx context.Context, c Coordinates, hours int) (Result[Forecast], error) {
	c = c.Normalize()
	hours = min(max(hours, 1), MaxForecastHours)
	res, err := fetch(ctx, s, s.forecast, "forecast", c.Key(),
		func(Forecast) time.Duration { return s.opts.ForecastTTL },
		func(ctx context.Context) (Forecast, error) { return s.provider.Forecast(ctx, c, MaxForecastHours) })
	if err != nil {
		return res, err
	}
	// Slice a copy so callers can never mutate the cached forecast.
	f := res.Value
	f.Hourly = append([]Reading(nil), f.Hourly[:min(hours, len(f.Hourly))]...)
	res.Value = f
	return res, nil
}

// ResolveLocation geocodes a place name, optionally restricted to an ISO 3166-1
// alpha-2 country code. Unknown names are cached briefly (negative caching) so
// repeated typos or junk input cannot be used to burn vendor quota.
func (s *Service) ResolveLocation(ctx context.Context, name, countryCode string) (Result[Location], error) {
	name = strings.Join(strings.Fields(strings.ToLower(name)), " ")
	countryCode = strings.ToUpper(countryCode)
	res, err := fetch(ctx, s, s.geocode, "geocode", name+"|"+countryCode,
		func(locs []Location) time.Duration {
			if len(locs) == 0 {
				return s.opts.NotFoundTTL
			}
			return s.opts.GeocodeTTL
		},
		func(ctx context.Context) ([]Location, error) { return s.provider.Geocode(ctx, name, countryCode) })
	if err != nil {
		return Result[Location]{}, err
	}
	if len(res.Value) == 0 {
		return Result[Location]{}, ErrLocationNotFound
	}
	return Result[Location]{Value: res.Value[0], Status: res.Status, Age: res.Age, TTL: res.TTL}, nil
}

func fetch[T any](
	ctx context.Context,
	s *Service,
	c *cache.Cache[string, T],
	cacheName, key string,
	ttl func(T) time.Duration,
	load func(context.Context) (T, error),
) (Result[T], error) {
	entry, found := c.Get(key)
	now := s.opts.Now()
	if found && entry.Fresh(now) {
		return Result[T]{Value: entry.Value, Status: CacheHit, Age: entry.Age(now), TTL: entry.TTL(now)}, nil
	}

	// Just expired: answer from the old entry now and refresh it in the background, so
	// a popular key never makes a client wait on the vendor. The refresh is not
	// charged to the caller's miss budget: one runs per key at a time.
	if found && s.opts.RevalidateWindow > 0 && now.Sub(entry.ExpiresAt) < s.opts.RevalidateWindow {
		ch := flight(ctx, s, c, cacheName, key, ttl, load)
		go func() {
			res := <-ch
			if res.Err != nil {
				s.revalidating.WithLabelValues(cacheName, "error").Inc()
				s.log.DebugContext(ctx, "background revalidation failed", slog.String("cache", cacheName), slog.String("error", res.Err.Error()))
				return
			}
			s.revalidating.WithLabelValues(cacheName, "success").Inc()
		}()
		return Result[T]{Value: entry.Value, Status: CacheRevalidating, Age: entry.Age(now)}, nil
	}

	// The caller must pay for a vendor fetch. If it has used up its share, an old copy
	// is better than nothing; with none, tell it when to come back.
	if gate, _ := ctx.Value(missGateKey{}).(MissGate); gate != nil {
		if retry, ok := gate(); !ok {
			if found {
				return Result[T]{Value: entry.Value, Status: CacheStale, Age: entry.Age(now)}, nil
			}
			return Result[T]{}, &MissBudgetError{RetryAfter: retry}
		}
	}

	ch := flight(ctx, s, c, cacheName, key, ttl, load)
	var err error
	select {
	case res := <-ch:
		if res.Shared {
			s.coalesced.WithLabelValues(cacheName).Inc()
		}
		if res.Err == nil {
			e := res.Val.(cache.Entry[T])
			now := s.opts.Now()
			return Result[T]{Value: e.Value, Status: CacheMiss, Age: e.Age(now), TTL: e.TTL(now)}, nil
		}
		err = res.Err
	case <-ctx.Done():
		err = ctx.Err()
	}

	if found && canServeStale(err) {
		c.MarkStaleServed()
		now := s.opts.Now()
		s.log.WarnContext(ctx, "serving stale data because the upstream failed",
			slog.String("cache", cacheName), slog.Duration("age", entry.Age(now)), slog.String("error", err.Error()))
		return Result[T]{Value: entry.Value, Status: CacheStale, Age: entry.Age(now)}, nil
	}
	return Result[T]{}, err
}

func flightKey(cacheName, key string) string { return cacheName + ":" + key }

// flight joins or starts the shared fetch for key. The fetch is detached from the first
// caller's cancellation (values are kept for log correlation): if that client
// disconnects, everyone else waiting on the flight still gets the result, and it is
// still cached. The flight runs in a goroutine owned by singleflight, where a panic
// would bypass the HTTP recoverer and kill the process, so it is recovered here.
func flight[T any](
	ctx context.Context,
	s *Service,
	c *cache.Cache[string, T],
	cacheName, key string,
	ttl func(T) time.Duration,
	load func(context.Context) (T, error),
) <-chan singleflight.Result {
	return s.flights.DoChan(flightKey(cacheName, key), func() (val any, err error) {
		defer func() {
			if r := recover(); r != nil {
				s.panics.WithLabelValues(cacheName).Inc()
				s.log.ErrorContext(ctx, "panic recovered in upstream fetch",
					slog.String("cache", cacheName), slog.String("panic", fmt.Sprint(r)), slog.String("stack", string(debug.Stack())))
				val, err = nil, errFetchPanic
			}
		}()
		// Another flight may have stored a fresh value between our cache miss and
		// becoming leader; do not spend vendor quota to fetch it again.
		if e, ok := c.Peek(key); ok && e.Fresh(s.opts.Now()) {
			return e, nil
		}
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.opts.FetchTimeout)
		defer cancel()
		v, err := load(fctx)
		if err != nil {
			return nil, err
		}
		return c.Set(key, v, ttl(v), s.opts.StaleFor), nil
	})
}

// canServeStale reports whether err means the vendor could not answer in time, as
// opposed to the request being invalid or the client having gone away.
func canServeStale(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var u interface{ Unavailable() bool }
	return errors.As(err, &u) && u.Unavailable()
}
