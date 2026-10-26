// Package config loads and validates the service configuration from environment
// variables. Parsing is declarative (caarlos0/env struct tags); cross-field rules live
// in validate. Every setting has a production-safe default so the service can start
// with an empty environment, and invalid values are reported together so a bad deploy
// fails fast with one actionable error instead of a crash loop of one-at-a-time fixes.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config is the complete runtime configuration.
type Config struct {
	Log      Log    `envPrefix:"LOG_"`
	HTTP     HTTP   `envPrefix:"HTTP_"`
	Health   Health `envPrefix:"HEALTH_"`
	Admin    Admin
	Upstream Upstream `envPrefix:"UPSTREAM_"`
	Cache    Cache    `envPrefix:"CACHE_"`
	DB       DB
}

// Log configures structured logging.
type Log struct {
	Level  slog.Level `env:"LEVEL"  envDefault:"info"`
	Format string     `env:"FORMAT" envDefault:"json"` // "json" or "text"
}

// HTTP configures the public listener and its protective limits.
type HTTP struct {
	Addr              string        `env:"ADDR"                envDefault:":8080"`
	ReadHeaderTimeout time.Duration `env:"READ_HEADER_TIMEOUT" envDefault:"5s"`
	ReadTimeout       time.Duration `env:"READ_TIMEOUT"        envDefault:"10s"`
	WriteTimeout      time.Duration `env:"WRITE_TIMEOUT"       envDefault:"30s"`
	IdleTimeout       time.Duration `env:"IDLE_TIMEOUT"        envDefault:"120s"`
	MaxHeaderBytes    int           `env:"MAX_HEADER_BYTES"    envDefault:"16384"`
	// RequestTimeout is the deadline attached to every API request context.
	RequestTimeout time.Duration `env:"REQUEST_TIMEOUT" envDefault:"10s"`
	// ShutdownTimeout bounds how long in-flight requests may take to finish on SIGTERM.
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"20s"`
	// DrainDelay is how long the service keeps serving (while reporting not-ready)
	// after SIGTERM so load balancers can deregister it before connections are closed.
	DrainDelay        time.Duration `env:"DRAIN_DELAY"          envDefault:"5s"`
	MaxInFlight       int           `env:"MAX_IN_FLIGHT"        envDefault:"256"`
	RateLimitRPS      float64       `env:"RATE_LIMIT_RPS"       envDefault:"10"`
	RateLimitBurst    int           `env:"RATE_LIMIT_BURST"     envDefault:"20"`
	TrustProxyHeaders bool          `env:"TRUST_PROXY_HEADERS"  envDefault:"false"`
}

// Health configures dependency checks.
type Health struct {
	// CacheTTL bounds how often checks hit dependencies, however often /health and
	// /readyz are probed.
	CacheTTL time.Duration `env:"CACHE_TTL" envDefault:"2s"`
	// Interval is how often checks run in the background to keep metrics current.
	Interval time.Duration `env:"INTERVAL" envDefault:"15s"`
}

// Admin configures the optional private listener: the health report with raw
// errors, and pprof when enabled. Never publish it.
type Admin struct {
	Addr         string `env:"ADMIN_ADDR"`    // empty disables the admin listener
	PprofEnabled bool   `env:"PPROF_ENABLED"` // only honoured when ADMIN_ADDR is set
}

// Upstream configures the Open-Meteo clients and their resilience policies.
type Upstream struct {
	AirQualityURL    string        `env:"AIR_QUALITY_URL"    envDefault:"https://air-quality-api.open-meteo.com"`
	GeocodingURL     string        `env:"GEOCODING_URL"      envDefault:"https://geocoding-api.open-meteo.com"`
	APIKey           string        `env:"API_KEY"` // optional; only for Open-Meteo commercial plans
	AttemptTimeout   time.Duration `env:"ATTEMPT_TIMEOUT"    envDefault:"3s"`
	MaxAttempts      int           `env:"MAX_ATTEMPTS"       envDefault:"3"`
	RetryBaseDelay   time.Duration `env:"RETRY_BASE_DELAY"   envDefault:"100ms"`
	RetryMaxDelay    time.Duration `env:"RETRY_MAX_DELAY"    envDefault:"1s"`
	RateLimitRPS     float64       `env:"RATE_LIMIT_RPS"     envDefault:"5"`
	RateLimitBurst   int           `env:"RATE_LIMIT_BURST"   envDefault:"10"`
	MaxConcurrent    int           `env:"MAX_CONCURRENT"     envDefault:"16"`
	BreakerThreshold int           `env:"BREAKER_THRESHOLD"  envDefault:"5"`
	BreakerCooldown  time.Duration `env:"BREAKER_COOLDOWN"   envDefault:"30s"`
	MaxResponseBytes int64         `env:"MAX_RESPONSE_BYTES" envDefault:"1048576"`
}

// Cache configures the in-memory response caches.
type Cache struct {
	MaxEntries  int           `env:"MAX_ENTRIES"   envDefault:"5000"`
	CurrentTTL  time.Duration `env:"CURRENT_TTL"   envDefault:"10m"`
	ForecastTTL time.Duration `env:"FORECAST_TTL"  envDefault:"30m"`
	GeocodeTTL  time.Duration `env:"GEOCODE_TTL"   envDefault:"24h"`
	NotFoundTTL time.Duration `env:"NOT_FOUND_TTL" envDefault:"10m"`
	// StaleFor is how long past expiry an entry is kept so it can be served when the
	// upstream is failing (stale-if-error).
	StaleFor time.Duration `env:"STALE_FOR" envDefault:"1h"`
}

// DB configures PostgreSQL access and the asynchronous response recorder.
type DB struct {
	URL               string        `env:"DATABASE_URL"` // empty disables persistence entirely
	MaxOpenConns      int           `env:"DB_MAX_OPEN_CONNS"       envDefault:"10"`
	MaxIdleConns      int           `env:"DB_MAX_IDLE_CONNS"       envDefault:"5"`
	ConnMaxLifetime   time.Duration `env:"DB_CONN_MAX_LIFETIME"    envDefault:"30m"`
	ConnMaxIdleTime   time.Duration `env:"DB_CONN_MAX_IDLE_TIME"   envDefault:"5m"`
	StatementTimeout  time.Duration `env:"DB_STATEMENT_TIMEOUT"    envDefault:"5s"`
	MigrateOnStart    bool          `env:"DB_MIGRATE_ON_START"     envDefault:"false"`
	RecorderBuffer    int           `env:"RECORDER_BUFFER"         envDefault:"1024"`
	RecorderBatchSize int           `env:"RECORDER_BATCH_SIZE"     envDefault:"100"`
	RecorderFlush     time.Duration `env:"RECORDER_FLUSH_INTERVAL" envDefault:"1s"`
	RecorderMaxBody   int           `env:"RECORDER_MAX_BODY_BYTES" envDefault:"65536"`
	Retention         time.Duration `env:"DB_RETENTION"            envDefault:"720h"` // 0 disables the retention job
	RetentionInterval time.Duration `env:"DB_RETENTION_INTERVAL"   envDefault:"1h"`
}

// Load reads the configuration using getenv (typically os.Getenv).
//
// Only the variables Config declares are read, through getenv rather than the process
// environment, which keeps Load deterministic in tests. Values are trimmed, and an
// empty value means "use the default".
func Load(getenv func(string) string) (Config, error) {
	var cfg Config
	params, err := env.GetFieldParams(&cfg)
	if err != nil {
		return Config{}, err
	}
	environ := make(map[string]string, len(params))
	for _, p := range params {
		if v := strings.TrimSpace(getenv(p.Key)); v != "" {
			environ[p.Key] = v
		}
	}
	if err := env.ParseWithOptions(&cfg, env.Options{Environment: environ}); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", attribute(environ, err))
	}
	if err := cfg.validate(); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

// attribute rewrites parse errors to name the environment variable, which is what an
// operator has to fix; env.ParseError only knows the struct field, and field names
// repeat across sections. Each set variable is re-parsed on its own, on the error
// path only.
func attribute(environ map[string]string, err error) error {
	var errs []error
	for _, key := range slices.Sorted(maps.Keys(environ)) {
		perr := env.ParseWithOptions(&Config{}, env.Options{Environment: map[string]string{key: environ[key]}})
		if perr == nil {
			continue
		}
		if pe := (env.ParseError{}); errors.As(perr, &pe) {
			perr = pe.Err
		}
		errs = append(errs, fmt.Errorf("%s=%q: %w", key, environ[key], perr))
	}
	if len(errs) == 0 {
		return err
	}
	return errors.Join(errs...)
}

func (c *Config) validate() error {
	l := &problems{}
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
	l.rate("HTTP_RATE_LIMIT_RPS", h.RateLimitRPS)
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
	l.rate("UPSTREAM_RATE_LIMIT_RPS", u.RateLimitRPS)
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
	return errors.Join(l.errs...)
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

type problems struct {
	errs []error
}

func (l *problems) check(ok bool, format string, args ...any) {
	if !ok {
		l.errs = append(l.errs, fmt.Errorf(format, args...))
	}
}

func (l *problems) positive(key string, d time.Duration) {
	l.check(d > 0, "%s must be positive, got %s", key, d)
}

func (l *problems) rate(key string, f float64) {
	l.check(f > 0 && !math.IsInf(f, 0), "%s must be a positive finite number, got %v", key, f)
}

func (l *problems) baseURL(key, raw string) {
	u, err := url.Parse(raw)
	switch {
	case err != nil:
		l.check(false, "%s: invalid URL: %v", key, err)
	case u.Scheme != "http" && u.Scheme != "https":
		l.check(false, "%s: scheme must be http or https, got %q", key, u.Scheme)
	case u.Host == "":
		l.check(false, "%s: host must not be empty", key)
	case u.RawQuery != "" || u.Fragment != "" || u.User != nil:
		l.check(false, "%s: must not contain credentials, query or fragment", key)
	}
}
