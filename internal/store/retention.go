package store

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/votiakov/airq/internal/telemetry"
)

// retentionLockID ensures only one replica prunes at a time.
const retentionLockID int64 = 0x61697271_72657465 // "airqrete"

// RetentionOptions configures a Retention job.
type RetentionOptions struct {
	MaxAge     time.Duration
	Interval   time.Duration
	BatchSize  int
	Registerer prometheus.Registerer
	Logger     *slog.Logger
}

// Retention deletes upstream_responses rows older than MaxAge. Deletes run in small
// batches so no single statement holds locks or generates WAL for long, and the work
// is guarded by an advisory lock so replicas do not duplicate it.
type Retention struct {
	db      *sql.DB
	opts    RetentionOptions
	queries *Metrics
	log     *slog.Logger

	deleted     prometheus.Counter
	runs        *prometheus.CounterVec
	lastSuccess prometheus.Gauge
}

// NewRetention creates the job and registers its metrics.
func NewRetention(db *sql.DB, queries *Metrics, opts RetentionOptions) *Retention {
	if opts.BatchSize < 1 {
		opts.BatchSize = 5000
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	r := &Retention{
		db: db, opts: opts, queries: queries,
		log: opts.Logger.With(slog.String("component", "retention")),
		deleted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: telemetry.Namespace, Subsystem: "retention", Name: "deleted_rows_total",
			Help: "Rows deleted by the retention job.",
		}),
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: telemetry.Namespace, Subsystem: "retention", Name: "runs_total",
			Help: "Retention job runs by result (success, skipped when another replica holds the lock, error).",
		}, []string{"result"}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: telemetry.Namespace, Subsystem: "retention", Name: "last_success_timestamp_seconds",
			Help: "Unix time of the last successful retention run.",
		}),
	}
	for _, res := range []string{"success", "skipped", "error"} {
		r.runs.WithLabelValues(res)
	}
	opts.Registerer.MustRegister(r.deleted, r.runs, r.lastSuccess)
	return r
}

// Run prunes once immediately and then every Interval until ctx is cancelled.
func (r *Retention) Run(ctx context.Context) {
	ticker := time.NewTicker(r.opts.Interval)
	defer ticker.Stop()
	for {
		if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
			r.log.ErrorContext(ctx, "retention run failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// RunOnce deletes expired rows and returns how many were removed.
func (r *Retention) RunOnce(ctx context.Context) (int64, error) {
	start := time.Now()
	n, locked, err := r.prune(ctx)
	r.queries.observe("retention_prune", start, err)
	switch {
	case err != nil:
		r.runs.WithLabelValues("error").Inc()
		return n, err
	case !locked:
		r.runs.WithLabelValues("skipped").Inc()
		return 0, nil
	}
	r.runs.WithLabelValues("success").Inc()
	r.lastSuccess.SetToCurrentTime()
	if n > 0 {
		r.log.InfoContext(ctx, "pruned expired upstream responses", "rows", n, "max_age", r.opts.MaxAge, "duration", time.Since(start))
	}
	return n, nil
}

func (r *Retention) prune(ctx context.Context) (deleted int64, locked bool, err error) {
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = conn.Close() }()

	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, retentionLockID).Scan(&locked); err != nil {
		return 0, false, fmt.Errorf("acquire retention lock: %w", err)
	}
	if !locked {
		return 0, false, nil
	}
	defer func() {
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_, _ = conn.ExecContext(uctx, `SELECT pg_advisory_unlock($1)`, retentionLockID)
	}()

	for {
		res, err := conn.ExecContext(ctx, `
			DELETE FROM upstream_responses
			WHERE id IN (
				SELECT id FROM upstream_responses
				WHERE created_at < now() - make_interval(secs => $1)
				LIMIT $2
			)`, r.opts.MaxAge.Seconds(), r.opts.BatchSize)
		if err != nil {
			return deleted, true, fmt.Errorf("delete expired rows: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return deleted, true, err
		}
		deleted += n
		r.deleted.Add(float64(n))
		if n < int64(r.opts.BatchSize) {
			return deleted, true, nil
		}
		if err := ctx.Err(); err != nil {
			return deleted, true, err
		}
	}
}
