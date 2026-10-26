// Command airq is an air quality API backed by Open-Meteo.
//
// Usage:
//
//	airq [serve]                 run the HTTP service (default)
//	airq migrate up|down [N]|status
//	airq healthcheck [URL]       probe /livez (used by the container HEALTHCHECK)
//	airq version
package main

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/cenkalti/backoff/v5"
	"golang.org/x/time/rate"

	"github.com/votiakov/airq/internal/airquality"
	"github.com/votiakov/airq/internal/config"
	"github.com/votiakov/airq/internal/health"
	"github.com/votiakov/airq/internal/httpapi"
	"github.com/votiakov/airq/internal/openmeteo"
	"github.com/votiakov/airq/internal/store"
	"github.com/votiakov/airq/internal/telemetry"
	"github.com/votiakov/airq/internal/upstream"
	"github.com/votiakov/airq/internal/version"
	"github.com/votiakov/airq/migrations"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop() // restore default handling: a second signal terminates immediately
	}()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "airq:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout io.Writer) error {
	cmd := "serve"
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}
	switch cmd {
	case "serve":
		return serve(ctx, getenv, stdout, nil)
	case "migrate":
		return migrate(ctx, args, getenv, stdout)
	case "healthcheck":
		return healthcheck(ctx, args, getenv)
	case "version":
		return json.NewEncoder(stdout).Encode(version.Get())
	case "help", "-h", "--help":
		_, err := fmt.Fprintln(stdout, "usage: airq [serve | migrate up|down [N]|status | healthcheck [URL] | version]")
		return err
	default:
		return fmt.Errorf("unknown command %q (try: airq help)", cmd)
	}
}

// serve runs the service until ctx is cancelled, then shuts down gracefully:
//
//  1. readiness flips to 503 so load balancers stop sending new requests;
//  2. after HTTP_DRAIN_DELAY (time for deregistration to propagate) the listener
//     stops accepting and in-flight requests get HTTP_SHUTDOWN_TIMEOUT to finish;
//  3. background jobs stop and the recorder flushes its queue;
//  4. the database pool closes;
//  5. buffered trace spans are flushed to the collector.
//
// onListen, if set, is told the bound address (used by tests).
func serve(ctx context.Context, getenv func(string) string, stdout io.Writer, onListen func(net.Addr)) error {
	cfg, err := config.Load(getenv)
	if err != nil {
		return err
	}
	logger := telemetry.NewLogger(stdout, cfg.Log.Level, cfg.Log.Format)
	slog.SetDefault(logger)
	logger.InfoContext(ctx, "starting airq", "build", version.Get(), "config", cfg)

	tp, err := telemetry.SetupTracing(ctx, getenv, logger)
	if err != nil {
		return err
	}
	defer func() { // registered first, so it runs last and captures spans from shutdown itself
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(fctx); err != nil {
			logger.WarnContext(ctx, "flushing trace spans failed", "error", err)
		}
	}()

	reg := telemetry.NewRegistry()
	bgCtx, stopBackground := context.WithCancel(context.WithoutCancel(ctx))
	defer stopBackground()
	var background sync.WaitGroup

	var checks []health.Check
	var recorder upstream.Recorder
	var db *sql.DB
	if cfg.DB.URL == "" {
		logger.WarnContext(ctx, "DATABASE_URL is not set: upstream responses will not be persisted")
	} else {
		db, err = store.Open(store.Options{
			URL: cfg.DB.URL, MaxOpenConns: cfg.DB.MaxOpenConns, MaxIdleConns: cfg.DB.MaxIdleConns,
			ConnMaxLifetime: cfg.DB.ConnMaxLifetime, ConnMaxIdleTime: cfg.DB.ConnMaxIdleTime,
			StatementTimeout: cfg.DB.StatementTimeout,
		})
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()
		dbMetrics := store.NewMetrics(reg, db)

		if cfg.DB.MigrateOnStart {
			if err := migrateOnStart(ctx, getenv, logger); err != nil {
				return fmt.Errorf("migrate on start: %w", err)
			}
		}
		migrator, err := store.NewMigrator(db, migrations.FS, logger)
		if err != nil {
			return err
		}
		if err := store.Ping(ctx, db, 3*time.Second); err != nil {
			// Soft dependency: keep serving; the health report shows "degraded".
			logger.WarnContext(ctx, "database unavailable at startup; continuing without the audit log until it recovers", "error", err)
		}

		rec := store.NewRecorder(db, dbMetrics, store.RecorderOptions{
			Buffer: cfg.DB.RecorderBuffer, BatchSize: cfg.DB.RecorderBatchSize, FlushInterval: cfg.DB.RecorderFlush,
			MaxBodyBytes: cfg.DB.RecorderMaxBody, WriteTimeout: cfg.DB.StatementTimeout, Registerer: reg, Logger: logger,
		})
		recorder = rec
		background.Go(func() { rec.Run(bgCtx, 5*time.Second) })

		if cfg.DB.Retention > 0 {
			ret := store.NewRetention(db, dbMetrics, store.RetentionOptions{
				MaxAge: cfg.DB.Retention, Interval: cfg.DB.RetentionInterval, Registerer: reg, Logger: logger,
			})
			background.Go(func() { ret.Run(bgCtx) })
		}

		checks = append(checks,
			health.Check{Name: "database", Timeout: time.Second, Run: func(ctx context.Context) error {
				return store.Ping(ctx, db, time.Second)
			}},
			health.Check{Name: "migrations", Timeout: time.Second, Run: func(ctx context.Context) error {
				n, err := migrator.Pending(ctx)
				if err != nil {
					return err
				}
				if n > 0 {
					return fmt.Errorf("%d migration(s) pending; run `airq migrate up`", n)
				}
				return nil
			}},
		)
	}

	upstreamMetrics := upstream.NewMetrics(reg)
	httpClient := upstream.NewHTTPClient(cfg.Upstream.AttemptTimeout)
	newUpstream := func(name, baseURL string) (*upstream.Client, error) {
		return upstream.New(upstream.Config{
			Name: name, BaseURL: baseURL, APIKey: cfg.Upstream.APIKey, HTTPClient: httpClient,
			AttemptTimeout: cfg.Upstream.AttemptTimeout, MaxAttempts: cfg.Upstream.MaxAttempts,
			RetryBaseDelay: cfg.Upstream.RetryBaseDelay, RetryMaxDelay: cfg.Upstream.RetryMaxDelay,
			BreakerThreshold: cfg.Upstream.BreakerThreshold, BreakerCooldown: cfg.Upstream.BreakerCooldown,
			BreakerFailureRatio: cfg.Upstream.BreakerFailureRatio, BreakerMinCalls: cfg.Upstream.BreakerMinCalls,
			BreakerWindow: cfg.Upstream.BreakerWindow,
			RateLimit:     rate.Limit(cfg.Upstream.RateLimitRPS), RateBurst: cfg.Upstream.RateLimitBurst,
			MaxConcurrent: cfg.Upstream.MaxConcurrent, MaxResponseBytes: cfg.Upstream.MaxResponseBytes,
			Metrics: upstreamMetrics, Recorder: recorder, Logger: logger,
		})
	}
	aqClient, err := newUpstream("air_quality", cfg.Upstream.AirQualityURL)
	if err != nil {
		return err
	}
	geoClient, err := newUpstream("geocoding", cfg.Upstream.GeocodingURL)
	if err != nil {
		return err
	}
	for _, c := range []*upstream.Client{aqClient, geoClient} {
		checks = append(checks, health.Check{Name: "upstream_" + c.Name(), Timeout: time.Second, Run: func(context.Context) error {
			// Passive check: probing the vendor would spend quota on every scrape.
			if c.CircuitOpen() {
				return errors.New("circuit open: vendor failing, serving cached data where available")
			}
			return nil
		}})
	}

	svc := airquality.NewService(openmeteo.New(aqClient, geoClient), airquality.Options{
		MaxEntries: cfg.Cache.MaxEntries, CurrentTTL: cfg.Cache.CurrentTTL, ForecastTTL: cfg.Cache.ForecastTTL,
		GeocodeTTL: cfg.Cache.GeocodeTTL, NotFoundTTL: cfg.Cache.NotFoundTTL, StaleFor: cfg.Cache.StaleFor,
		RevalidateWindow: cfg.Cache.RevalidateWindow,
		FetchTimeout:     cfg.HTTP.RequestTimeout, Registerer: reg, Logger: logger,
	})
	checker := health.NewChecker(reg, logger, cfg.Health.CacheTTL, checks...)
	initial := checker.Report(ctx)
	logger.InfoContext(ctx, "initial health", "status", initial.Status, "checks", initial.Checks)
	background.Go(func() { checker.Run(bgCtx, cfg.Health.Interval) })

	srv := &http.Server{
		Handler: httpapi.NewHandler(httpapi.Options{
			Service: svc, Health: checker, Registry: reg, Logger: logger, Attribution: openmeteo.Attribution,
			RequestTimeout: cfg.HTTP.RequestTimeout, MaxInFlight: cfg.HTTP.MaxInFlight,
			RateLimitRPS: cfg.HTTP.RateLimitRPS, RateLimitBurst: cfg.HTTP.RateLimitBurst,
			MissRateLimitRPS: cfg.HTTP.MissRateLimitRPS, MissRateLimitBurst: cfg.HTTP.MissRateLimitBurst,
			TrustProxyHeaders: cfg.HTTP.TrustProxyHeaders,
		}),
		ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout,
		ReadTimeout:       cfg.HTTP.ReadTimeout,
		WriteTimeout:      cfg.HTTP.WriteTimeout,
		IdleTimeout:       cfg.HTTP.IdleTimeout,
		MaxHeaderBytes:    cfg.HTTP.MaxHeaderBytes,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	// Listen before serving so a bad address or busy port fails startup immediately.
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.HTTP.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTP.Addr, err)
	}
	serveErr := make(chan error, 2)
	go func() { serveErr <- srv.Serve(ln) }()
	logger.InfoContext(ctx, "listening", "addr", ln.Addr().String())
	if onListen != nil {
		onListen(ln.Addr())
	}

	var admin *http.Server
	if cfg.Admin.Addr != "" {
		admin = &http.Server{Addr: cfg.Admin.Addr, Handler: adminMux(checker, cfg.Admin.PprofEnabled), ReadHeaderTimeout: 5 * time.Second}
		go func() { serveErr <- admin.ListenAndServe() }()
		logger.InfoContext(ctx, "admin listener serving the detailed health report", "addr", cfg.Admin.Addr)
		if cfg.Admin.PprofEnabled {
			logger.WarnContext(ctx, "pprof enabled on admin listener; never expose it publicly", "addr", cfg.Admin.Addr)
		}
	}

	select {
	case err := <-serveErr:
		return fmt.Errorf("server stopped unexpectedly: %w", err)
	case <-ctx.Done():
	}

	logger.InfoContext(ctx, "shutdown started; failing readiness and draining", "drain_delay", cfg.HTTP.DrainDelay)
	checker.SetDraining()
	time.Sleep(cfg.HTTP.DrainDelay)

	sctx, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		logger.ErrorContext(ctx, "graceful shutdown timed out; closing remaining connections", "error", err)
		_ = srv.Close()
	}
	if admin != nil {
		_ = admin.Shutdown(sctx)
	}
	stopBackground()
	background.Wait()
	logger.InfoContext(ctx, "shutdown complete")
	return nil
}

// adminMux serves the private admin listener: the health report with raw errors and,
// when enabled, pprof.
func adminMux(checker *health.Checker, pprofEnabled bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /health", checker.DetailedHealthHandler())
	if !pprofEnabled {
		return mux
	}
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// openMigrationDB opens a small pool for schema changes. MIGRATE_DATABASE_URL lets
// migrations use a privileged (DDL) role while the service itself runs with a
// DML-only role.
func openMigrationDB(getenv func(string) string) (*sql.DB, error) {
	dsn := cmp.Or(getenv("MIGRATE_DATABASE_URL"), getenv("DATABASE_URL"))
	if dsn == "" {
		return nil, errors.New("migrate: set MIGRATE_DATABASE_URL or DATABASE_URL")
	}
	return store.Open(store.Options{
		URL: dsn, MaxOpenConns: 2, MaxIdleConns: 1, ConnMaxLifetime: time.Hour, ConnMaxIdleTime: time.Minute,
		LockTimeout: 10 * time.Second, ApplicationName: "airq-migrate",
	})
}

// migrateOnStart applies pending migrations before serving (DB_MIGRATE_ON_START).
// It suits single-instance setups; elsewhere run `airq migrate up` as a release job.
func migrateOnStart(ctx context.Context, getenv func(string) string, logger *slog.Logger) error {
	db, err := openMigrationDB(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	m, err := store.NewMigrator(db, migrations.FS, logger)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	n, err := m.Up(ctx)
	if err != nil {
		return err
	}
	logger.InfoContext(ctx, "migrations applied on start", "count", n)
	return nil
}

// migrate runs the `airq migrate` subcommand.
func migrate(ctx context.Context, args []string, getenv func(string) string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New("migrate: expected up, down [N] or status")
	}
	logger := telemetry.NewLogger(stdout, slog.LevelInfo, "json")

	db, err := openMigrationDB(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	m, err := store.NewMigrator(db, migrations.FS, logger)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	if err := waitForDB(ctx, db, logger); err != nil {
		return err
	}

	switch args[0] {
	case "up":
		n, err := m.Up(ctx)
		if err != nil {
			return err
		}
		logger.InfoContext(ctx, "migrations complete", "applied", n)
	case "down":
		steps := 1
		if len(args) > 1 {
			if steps, err = strconv.Atoi(args[1]); err != nil || steps < 1 {
				return fmt.Errorf("migrate down: invalid step count %q", args[1])
			}
		}
		n, err := m.Down(ctx, steps)
		if err != nil {
			return err
		}
		logger.InfoContext(ctx, "rollback complete", "rolled_back", n)
	case "status":
		st, err := m.Status(ctx)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "VERSION\tNAME\tAPPLIED AT")
		for _, s := range st {
			at := "pending"
			if s.Applied {
				at = s.AppliedAt.UTC().Format(time.RFC3339)
			}
			_, _ = fmt.Fprintf(tw, "%06d\t%s\t%s\n", s.Version, s.Name, at)
		}
		return tw.Flush()
	default:
		return fmt.Errorf("migrate: unknown action %q", args[0])
	}
	return nil
}

// waitForDB retries until the database accepts connections (containers start in
// parallel), with capped backoff bounded by ctx.
func waitForDB(ctx context.Context, db *sql.DB, logger *slog.Logger) error {
	attempt := 0
	_, err := backoff.Retry(ctx, func() (struct{}, error) {
		err := store.Ping(ctx, db, 3*time.Second)
		if err != nil && attempt%5 == 0 {
			logger.WarnContext(ctx, "waiting for database", "error", err)
		}
		attempt++
		return struct{}{}, err
	}, backoff.WithBackOff(&backoff.ExponentialBackOff{
		InitialInterval:     250 * time.Millisecond,
		RandomizationFactor: backoff.DefaultRandomizationFactor,
		Multiplier:          backoff.DefaultMultiplier,
		MaxInterval:         5 * time.Second,
	}), backoff.WithMaxElapsedTime(0)) // bounded by ctx, not by the library default
	if err != nil {
		return fmt.Errorf("database not reachable: %w", err)
	}
	return nil
}

// healthcheck probes the local liveness endpoint. Distroless images have no shell or
// curl, so the binary checks itself.
func healthcheck(ctx context.Context, args []string, getenv func(string) string) error {
	target := ""
	if len(args) > 0 {
		target = args[0]
	} else {
		addr := getenv("HTTP_ADDR")
		if addr == "" {
			addr = ":8080"
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return fmt.Errorf("healthcheck: invalid HTTP_ADDR %q", addr)
		}
		if host == "" || host == "0.0.0.0" || host == "::" {
			host = "127.0.0.1"
		}
		target = "http://" + net.JoinHostPort(host, port) + "/livez"
	}
	// A self-check must never be usable to reach other hosts.
	u, err := url.Parse(target)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("healthcheck: invalid URL %q", target)
	}
	if ip := net.ParseIP(u.Hostname()); u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return fmt.Errorf("healthcheck: refusing non-loopback target %q", u.Host)
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil) //nolint:gosec // G704: loopback only
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // G704: target restricted to loopback above
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: %s returned %d", target, resp.StatusCode)
	}
	return nil
}
