// Package store owns PostgreSQL access: connection setup, schema migrations, the
// asynchronous upstream response recorder and the retention job.
//
// The database is a soft dependency. The API keeps serving if PostgreSQL is down;
// only the audit log degrades, and that shows up in /health, metrics and logs.
package store

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/votiakov/airq/internal/telemetry"
)

// Options configures the connection pool.
type Options struct {
	URL              string
	MaxOpenConns     int
	MaxIdleConns     int
	ConnMaxLifetime  time.Duration
	ConnMaxIdleTime  time.Duration
	StatementTimeout time.Duration
	// LockTimeout defaults to StatementTimeout. Migrations set it on its own: no
	// statement timeout (backfills may be long) but bounded lock waits, so DDL queued
	// behind a long transaction fails instead of blocking all traffic behind it.
	LockTimeout     time.Duration
	ApplicationName string
}

// Open configures a *sql.DB backed by pgx. It does not connect; use Ping. Server-side
// guards are set per session: statement_timeout stops runaway queries,
// idle_in_transaction_session_timeout reclaims sessions leaked mid-transaction, and
// lock_timeout makes DDL fail fast instead of queueing behind long transactions.
func Open(opts Options) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(opts.URL)
	if err != nil {
		// pgx redacts the password in parse errors.
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if opts.ApplicationName == "" {
		opts.ApplicationName = "airq"
	}
	if _, ok := cfg.RuntimeParams["application_name"]; !ok {
		cfg.RuntimeParams["application_name"] = opts.ApplicationName
	}
	ms := func(d time.Duration) string { return strconv.FormatInt(d.Milliseconds(), 10) }
	if opts.StatementTimeout > 0 {
		cfg.RuntimeParams["statement_timeout"] = ms(opts.StatementTimeout)
		cfg.RuntimeParams["idle_in_transaction_session_timeout"] = ms(4 * opts.StatementTimeout)
	}
	if lt := cmp.Or(opts.LockTimeout, opts.StatementTimeout); lt > 0 {
		cfg.RuntimeParams["lock_timeout"] = ms(lt)
	}

	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(opts.MaxOpenConns)
	db.SetMaxIdleConns(opts.MaxIdleConns)
	db.SetConnMaxLifetime(opts.ConnMaxLifetime)
	db.SetConnMaxIdleTime(opts.ConnMaxIdleTime)
	return db, nil
}

// Metrics instruments database access. Pool health (open, in-use, idle connections,
// waits, closures) comes from the standard DBStats collector as go_sql_* series;
// query-level latency and errors come from QueryDuration.
type Metrics struct {
	queryDuration *prometheus.HistogramVec
}

// NewMetrics registers the pool collector for db and the query histogram.
func NewMetrics(reg prometheus.Registerer, db *sql.DB) *Metrics {
	m := &Metrics{
		queryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: telemetry.Namespace, Subsystem: "db", Name: "query_duration_seconds",
			Help:    "Duration of database operations by operation and outcome.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"operation", "outcome"}),
	}
	reg.MustRegister(m.queryDuration, collectors.NewDBStatsCollector(db, "airq"))
	return m
}

func (m *Metrics) observe(op string, start time.Time, err error) {
	if m == nil {
		return
	}
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	m.queryDuration.WithLabelValues(op, outcome).Observe(time.Since(start).Seconds())
}

// Ping checks connectivity with a bounded timeout (used by health checks).
func Ping(ctx context.Context, db *sql.DB, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("database ping timed out after %s", timeout)
		}
		return fmt.Errorf("database ping: %w", err)
	}
	return nil
}
