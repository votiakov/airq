// Package config loads and validates the service configuration from environment
// variables. Every setting has a production-safe default so the service can start with
// an empty environment; invalid values are collected and reported together so a bad
// deploy fails fast with one actionable error instead of a crash loop of one-at-a-time
// fixes.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config is the complete runtime configuration.
type Config struct {
	Log      Log
	HTTP     HTTP
	Health   Health
	Admin    Admin
	Upstream Upstream
	Cache    Cache
	DB       DB
}

// Log configures structured logging.
type Log struct {
	Level  slog.Level
	Format string // "json" or "text"
}

// HTTP configures the public listener and its protective limits.
type HTTP struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	MaxHeaderBytes    int
	// RequestTimeout is the deadline attached to every API request context.
	RequestTimeout time.Duration
	// ShutdownTimeout bounds how long in-flight requests may take to finish on SIGTERM.
	ShutdownTimeout time.Duration
	// DrainDelay is how long the service keeps serving (while reporting not-ready)
	// after SIGTERM so load balancers can deregister it before connections are closed.
	DrainDelay        time.Duration
	MaxInFlight       int
	RateLimitRPS      float64
	RateLimitBurst    int
	TrustProxyHeaders bool
}

// Health configures dependency checks.
type Health struct {
	// CacheTTL bounds how often checks hit dependencies, however often /health and
	// /readyz are probed.
	CacheTTL time.Duration
	// Interval is how often checks run in the background to keep metrics current.
	Interval time.Duration
}

// Admin configures the optional private listener used for profiling.
type Admin struct {
	Addr         string // empty disables the admin listener
	PprofEnabled bool
}

// Upstream configures the Open-Meteo clients and their resilience policies.
type Upstream struct {
	AirQualityURL    string
	GeocodingURL     string
	APIKey           string // optional; only needed for Open-Meteo commercial plans
	AttemptTimeout   time.Duration
	MaxAttempts      int
	RetryBaseDelay   time.Duration
	RetryMaxDelay    time.Duration
	RateLimitRPS     float64
	RateLimitBurst   int
	MaxConcurrent    int
	BreakerThreshold int
	BreakerCooldown  time.Duration
	MaxResponseBytes int64
}

// Cache configures the in-memory response caches.
type Cache struct {
	MaxEntries  int
	CurrentTTL  time.Duration
	ForecastTTL time.Duration
	GeocodeTTL  time.Duration
	NotFoundTTL time.Duration
	// StaleFor is how long past expiry an entry is kept so it can be served when the
	// upstream is failing (stale-if-error).
	StaleFor time.Duration
}

// DB configures PostgreSQL access and the asynchronous response recorder.
type DB struct {
	URL               string // empty disables persistence entirely
	MaxOpenConns      int
	MaxIdleConns      int
	ConnMaxLifetime   time.Duration
	ConnMaxIdleTime   time.Duration
	StatementTimeout  time.Duration
	MigrateOnStart    bool
	RecorderBuffer    int
	RecorderBatchSize int
	RecorderFlush     time.Duration
	RecorderMaxBody   int
	Retention         time.Duration // 0 disables the retention job
	RetentionInterval time.Duration
}

// Load reads the configuration using getenv (typically os.Getenv).
func Load(getenv func(string) string) (Config, error) {
	l := &loader{getenv: getenv}

	var cfg Config

	cfg.Log.Level = l.level("LOG_LEVEL", slog.LevelInfo)
	cfg.Log.Format = l.str("LOG_FORMAT", "json")

	cfg.HTTP = HTTP{
		Addr:              l.str("HTTP_ADDR", ":8080"),
		ReadHeaderTimeout: l.duration("HTTP_READ_HEADER_TIMEOUT", 5*time.Second),
		ReadTimeout:       l.duration("HTTP_READ_TIMEOUT", 10*time.Second),
		WriteTimeout:      l.duration("HTTP_WRITE_TIMEOUT", 30*time.Second),
		IdleTimeout:       l.duration("HTTP_IDLE_TIMEOUT", 120*time.Second),
		MaxHeaderBytes:    l.integer("HTTP_MAX_HEADER_BYTES", 16<<10),
		RequestTimeout:    l.duration("HTTP_REQUEST_TIMEOUT", 10*time.Second),
		ShutdownTimeout:   l.duration("HTTP_SHUTDOWN_TIMEOUT", 20*time.Second),
		DrainDelay:        l.duration("HTTP_DRAIN_DELAY", 5*time.Second),
		MaxInFlight:       l.integer("HTTP_MAX_IN_FLIGHT", 256),
		RateLimitRPS:      l.float("HTTP_RATE_LIMIT_RPS", 10),
		RateLimitBurst:    l.integer("HTTP_RATE_LIMIT_BURST", 20),
		TrustProxyHeaders: l.boolean("HTTP_TRUST_PROXY_HEADERS", false),
	}

	cfg.Health = Health{
		CacheTTL: l.duration("HEALTH_CACHE_TTL", 2*time.Second),
		Interval: l.duration("HEALTH_INTERVAL", 15*time.Second),
	}

	cfg.Admin = Admin{
		Addr:         l.str("ADMIN_ADDR", ""),
		PprofEnabled: l.boolean("PPROF_ENABLED", false),
	}

	cfg.Upstream = Upstream{
		AirQualityURL:    l.str("UPSTREAM_AIR_QUALITY_URL", "https://air-quality-api.open-meteo.com"),
		GeocodingURL:     l.str("UPSTREAM_GEOCODING_URL", "https://geocoding-api.open-meteo.com"),
		APIKey:           l.str("UPSTREAM_API_KEY", ""),
		AttemptTimeout:   l.duration("UPSTREAM_ATTEMPT_TIMEOUT", 3*time.Second),
		MaxAttempts:      l.integer("UPSTREAM_MAX_ATTEMPTS", 3),
		RetryBaseDelay:   l.duration("UPSTREAM_RETRY_BASE_DELAY", 100*time.Millisecond),
		RetryMaxDelay:    l.duration("UPSTREAM_RETRY_MAX_DELAY", time.Second),
		RateLimitRPS:     l.float("UPSTREAM_RATE_LIMIT_RPS", 5),
		RateLimitBurst:   l.integer("UPSTREAM_RATE_LIMIT_BURST", 10),
		MaxConcurrent:    l.integer("UPSTREAM_MAX_CONCURRENT", 16),
		BreakerThreshold: l.integer("UPSTREAM_BREAKER_THRESHOLD", 5),
		BreakerCooldown:  l.duration("UPSTREAM_BREAKER_COOLDOWN", 30*time.Second),
		MaxResponseBytes: int64(l.integer("UPSTREAM_MAX_RESPONSE_BYTES", 1<<20)),
	}

	cfg.Cache = Cache{
		MaxEntries:  l.integer("CACHE_MAX_ENTRIES", 5000),
		CurrentTTL:  l.duration("CACHE_CURRENT_TTL", 10*time.Minute),
		ForecastTTL: l.duration("CACHE_FORECAST_TTL", 30*time.Minute),
		GeocodeTTL:  l.duration("CACHE_GEOCODE_TTL", 24*time.Hour),
		NotFoundTTL: l.duration("CACHE_NOT_FOUND_TTL", 10*time.Minute),
		StaleFor:    l.duration("CACHE_STALE_FOR", time.Hour),
	}

	cfg.DB = DB{
		URL:               l.str("DATABASE_URL", ""),
		MaxOpenConns:      l.integer("DB_MAX_OPEN_CONNS", 10),
		MaxIdleConns:      l.integer("DB_MAX_IDLE_CONNS", 5),
		ConnMaxLifetime:   l.duration("DB_CONN_MAX_LIFETIME", 30*time.Minute),
		ConnMaxIdleTime:   l.duration("DB_CONN_MAX_IDLE_TIME", 5*time.Minute),
		StatementTimeout:  l.duration("DB_STATEMENT_TIMEOUT", 5*time.Second),
		MigrateOnStart:    l.boolean("DB_MIGRATE_ON_START", false),
		RecorderBuffer:    l.integer("RECORDER_BUFFER", 1024),
		RecorderBatchSize: l.integer("RECORDER_BATCH_SIZE", 100),
		RecorderFlush:     l.duration("RECORDER_FLUSH_INTERVAL", time.Second),
		RecorderMaxBody:   l.integer("RECORDER_MAX_BODY_BYTES", 64<<10),
		Retention:         l.duration("DB_RETENTION", 30*24*time.Hour),
		RetentionInterval: l.duration("DB_RETENTION_INTERVAL", time.Hour),
	}

	cfg.validate(l)

	if len(l.errs) > 0 {
		return Config{}, fmt.Errorf("invalid configuration: %w", errors.Join(l.errs...))
	}
	return cfg, nil
}

func (c *Config) validate(l *loader) {
	l.check(c.Log.Format == "json" || c.Log.Format == "text", "LOG_FORMAT must be json or text, got %q", c.Log.Format)

	h := c.HTTP
	l.check(h.Addr != "", "HTTP_ADDR must not be empty")
	l.positive("HTTP_READ_HEADER_TIMEOUT", h.ReadHeaderTimeout)
	l.positive("HTTP_READ_TIMEOUT", h.ReadTimeout)
	l.positive("HTTP_WRITE_TIMEOUT", h.WriteTimeout)
	l.positive("HTTP_IDLE_TIMEOUT", h.IdleTimeout)
	l.positive("HTTP_REQUEST_TIMEOUT", h.RequestTimeout)
	l.positive("HTTP_SHUTDOWN_TIMEOUT", h.ShutdownTimeout)
	l.check(h.DrainDelay >= 0, "HTTP_DRAIN_DELAY must not be negative")
	l.check(h.WriteTimeout > h.RequestTimeout,
		"HTTP_WRITE_TIMEOUT (%s) must exceed HTTP_REQUEST_TIMEOUT (%s) so handlers can still write their timeout response",
		h.WriteTimeout, h.RequestTimeout)
	l.check(h.MaxHeaderBytes >= 1<<10, "HTTP_MAX_HEADER_BYTES must be at least 1024")
	l.check(h.MaxInFlight >= 1, "HTTP_MAX_IN_FLIGHT must be at least 1")
	l.check(h.RateLimitRPS > 0, "HTTP_RATE_LIMIT_RPS must be positive")
	l.check(h.RateLimitBurst >= 1, "HTTP_RATE_LIMIT_BURST must be at least 1")

	l.check(c.Health.CacheTTL >= 0, "HEALTH_CACHE_TTL must not be negative")
	l.positive("HEALTH_INTERVAL", c.Health.Interval)

	u := c.Upstream
	l.baseURL("UPSTREAM_AIR_QUALITY_URL", u.AirQualityURL)
	l.baseURL("UPSTREAM_GEOCODING_URL", u.GeocodingURL)
	l.positive("UPSTREAM_ATTEMPT_TIMEOUT", u.AttemptTimeout)
	l.check(u.MaxAttempts >= 1 && u.MaxAttempts <= 10, "UPSTREAM_MAX_ATTEMPTS must be between 1 and 10")
	l.positive("UPSTREAM_RETRY_BASE_DELAY", u.RetryBaseDelay)
	l.check(u.RetryMaxDelay >= u.RetryBaseDelay, "UPSTREAM_RETRY_MAX_DELAY must be >= UPSTREAM_RETRY_BASE_DELAY")
	l.check(u.RateLimitRPS > 0, "UPSTREAM_RATE_LIMIT_RPS must be positive")
	l.check(u.RateLimitBurst >= 1, "UPSTREAM_RATE_LIMIT_BURST must be at least 1")
	l.check(u.MaxConcurrent >= 1, "UPSTREAM_MAX_CONCURRENT must be at least 1")
	l.check(u.BreakerThreshold >= 1, "UPSTREAM_BREAKER_THRESHOLD must be at least 1")
	l.positive("UPSTREAM_BREAKER_COOLDOWN", u.BreakerCooldown)
	l.check(u.MaxResponseBytes >= 1<<10, "UPSTREAM_MAX_RESPONSE_BYTES must be at least 1024")
	l.check(c.HTTP.RequestTimeout >= u.AttemptTimeout,
		"HTTP_REQUEST_TIMEOUT (%s) must be >= UPSTREAM_ATTEMPT_TIMEOUT (%s)", c.HTTP.RequestTimeout, u.AttemptTimeout)

	ca := c.Cache
	l.check(ca.MaxEntries >= 1, "CACHE_MAX_ENTRIES must be at least 1")
	l.positive("CACHE_CURRENT_TTL", ca.CurrentTTL)
	l.positive("CACHE_FORECAST_TTL", ca.ForecastTTL)
	l.positive("CACHE_GEOCODE_TTL", ca.GeocodeTTL)
	l.positive("CACHE_NOT_FOUND_TTL", ca.NotFoundTTL)
	l.check(ca.StaleFor >= 0, "CACHE_STALE_FOR must not be negative")

	d := c.DB
	l.check(d.MaxOpenConns >= 1, "DB_MAX_OPEN_CONNS must be at least 1")
	l.check(d.MaxIdleConns >= 0 && d.MaxIdleConns <= d.MaxOpenConns, "DB_MAX_IDLE_CONNS must be between 0 and DB_MAX_OPEN_CONNS")
	l.positive("DB_CONN_MAX_LIFETIME", d.ConnMaxLifetime)
	l.positive("DB_CONN_MAX_IDLE_TIME", d.ConnMaxIdleTime)
	l.positive("DB_STATEMENT_TIMEOUT", d.StatementTimeout)
	l.check(d.RecorderBuffer >= 1, "RECORDER_BUFFER must be at least 1")
	l.check(d.RecorderBatchSize >= 1 && d.RecorderBatchSize <= 1000, "RECORDER_BATCH_SIZE must be between 1 and 1000")
	l.positive("RECORDER_FLUSH_INTERVAL", d.RecorderFlush)
	l.check(d.RecorderMaxBody >= 0, "RECORDER_MAX_BODY_BYTES must not be negative")
	l.check(d.Retention >= 0, "DB_RETENTION must not be negative (0 disables retention)")
	l.positive("DB_RETENTION_INTERVAL", d.RetentionInterval)
}

// LogValue implements slog.LogValuer so the effective configuration can be logged at
// startup without leaking credentials.
func (c Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("log_level", c.Log.Level.String()),
		slog.String("http_addr", c.HTTP.Addr),
		slog.Duration("http_request_timeout", c.HTTP.RequestTimeout),
		slog.Int("http_max_in_flight", c.HTTP.MaxInFlight),
		slog.Float64("http_rate_limit_rps", c.HTTP.RateLimitRPS),
		slog.String("admin_addr", c.Admin.Addr),
		slog.String("upstream_air_quality_url", c.Upstream.AirQualityURL),
		slog.String("upstream_geocoding_url", c.Upstream.GeocodingURL),
		slog.Bool("upstream_api_key_set", c.Upstream.APIKey != ""),
		slog.Int("upstream_max_attempts", c.Upstream.MaxAttempts),
		slog.Duration("cache_current_ttl", c.Cache.CurrentTTL),
		slog.Duration("cache_forecast_ttl", c.Cache.ForecastTTL),
		slog.String("database_url", RedactDSN(c.DB.URL)),
		slog.Duration("db_retention", c.DB.Retention),
	)
}

// RedactDSN hides the password in a PostgreSQL connection string. URL-style DSNs keep
// their host and database for debuggability; anything else is replaced entirely.
func RedactDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "[redacted]"
	}
	q := u.Query()
	if q.Has("password") {
		q.Set("password", "xxxxx")
		u.RawQuery = q.Encode()
	}
	return u.Redacted()
}

type loader struct {
	getenv func(string) string
	errs   []error
}

func (l *loader) lookup(key string) (string, bool) {
	v := strings.TrimSpace(l.getenv(key))
	return v, v != ""
}

func (l *loader) fail(format string, args ...any) {
	l.errs = append(l.errs, fmt.Errorf(format, args...))
}

func (l *loader) check(ok bool, format string, args ...any) {
	if !ok {
		l.fail(format, args...)
	}
}

func (l *loader) positive(key string, d time.Duration) {
	l.check(d > 0, "%s must be positive, got %s", key, d)
}

func (l *loader) str(key, def string) string {
	if v, ok := l.lookup(key); ok {
		return v
	}
	return def
}

func (l *loader) duration(key string, def time.Duration) time.Duration {
	v, ok := l.lookup(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.fail("%s: invalid duration %q (examples: 500ms, 10s, 5m)", key, v)
		return def
	}
	return d
}

func (l *loader) integer(key string, def int) int {
	v, ok := l.lookup(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.fail("%s: invalid integer %q", key, v)
		return def
	}
	return n
}

func (l *loader) float(key string, def float64) float64 {
	v, ok := l.lookup(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		l.fail("%s: invalid number %q", key, v)
		return def
	}
	return f
}

func (l *loader) boolean(key string, def bool) bool {
	v, ok := l.lookup(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.fail("%s: invalid boolean %q (use true or false)", key, v)
		return def
	}
	return b
}

func (l *loader) level(key string, def slog.Level) slog.Level {
	v, ok := l.lookup(key)
	if !ok {
		return def
	}
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(v)); err != nil {
		l.fail("%s: invalid level %q (use debug, info, warn or error)", key, v)
		return def
	}
	return lvl
}

func (l *loader) baseURL(key, raw string) {
	u, err := url.Parse(raw)
	switch {
	case err != nil:
		l.fail("%s: invalid URL: %v", key, err)
	case u.Scheme != "http" && u.Scheme != "https":
		l.fail("%s: scheme must be http or https, got %q", key, u.Scheme)
	case u.Host == "":
		l.fail("%s: host must not be empty", key)
	case u.RawQuery != "" || u.Fragment != "" || u.User != nil:
		l.fail("%s: must not contain credentials, query or fragment", key)
	}
}
