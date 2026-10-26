package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/votiakov/airq/internal/telemetry"
	"github.com/votiakov/airq/internal/upstream"
)

const recorderColumns = 13

// maxErrorLen bounds the stored error message.
const maxErrorLen = 1024

// RecorderOptions configures a Recorder.
type RecorderOptions struct {
	Buffer        int           // queue capacity; records beyond it are dropped
	BatchSize     int           // rows per INSERT
	FlushInterval time.Duration // max time a record waits in the queue
	MaxBodyBytes  int           // larger bodies are stored as NULL with body_truncated
	WriteTimeout  time.Duration // per-batch deadline
	Registerer    prometheus.Registerer
	Logger        *slog.Logger
}

// Recorder persists upstream exchanges to upstream_responses asynchronously.
//
// Record never blocks the request path: exchanges go into a bounded queue that a
// single writer drains in batches. If PostgreSQL is slow or down, the queue fills and
// new records are dropped and counted; requests are never slowed or failed by audit
// logging. Delivery is therefore best-effort (at-most-once), which is the right
// trade-off for an audit trail of a cache-backed read API (see docs/adr/0005).
type Recorder struct {
	db      *sql.DB
	opts    RecorderOptions
	queries *Metrics
	log     *slog.Logger
	queue   chan upstream.Exchange

	records   *prometheus.CounterVec
	batchSize prometheus.Histogram

	errLogMu   sync.Mutex
	lastErrLog time.Time
}

var _ upstream.Recorder = (*Recorder)(nil)

// NewRecorder creates a recorder and registers its metrics. Call Run to start it.
func NewRecorder(db *sql.DB, queries *Metrics, opts RecorderOptions) *Recorder {
	if opts.WriteTimeout <= 0 {
		opts.WriteTimeout = 5 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	r := &Recorder{
		db:      db,
		opts:    opts,
		queries: queries,
		log:     opts.Logger.With(slog.String("component", "recorder")),
		queue:   make(chan upstream.Exchange, max(opts.Buffer, 1)),
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: telemetry.Namespace, Subsystem: "recorder", Name: "records_total",
			Help: "Upstream exchanges handled by the DB recorder: written, dropped (queue full) or failed (insert error).",
		}, []string{"result"}),
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: telemetry.Namespace, Subsystem: "recorder", Name: "batch_size",
			Help:    "Rows per recorder INSERT.",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		}),
	}
	for _, res := range []string{"written", "dropped", "failed"} {
		r.records.WithLabelValues(res)
	}
	opts.Registerer.MustRegister(r.records, r.batchSize,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: telemetry.Namespace, Subsystem: "recorder", Name: "queue_length",
			Help: "Exchanges waiting to be written.",
		}, func() float64 { return float64(len(r.queue)) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: telemetry.Namespace, Subsystem: "recorder", Name: "queue_capacity",
			Help: "Maximum queued exchanges before new ones are dropped.",
		}, func() float64 { return float64(cap(r.queue)) }),
	)
	return r
}

// Record enqueues ex without blocking.
func (r *Recorder) Record(ex upstream.Exchange) {
	select {
	case r.queue <- ex:
	default:
		r.records.WithLabelValues("dropped").Inc()
		r.throttledError("recorder queue full; dropping upstream exchange", nil)
	}
}

// Run writes batches until ctx is cancelled, then drains what is already queued
// (bounded by drainTimeout) and returns.
func (r *Recorder) Run(ctx context.Context, drainTimeout time.Duration) {
	ticker := time.NewTicker(r.opts.FlushInterval)
	defer ticker.Stop()
	batch := make([]upstream.Exchange, 0, r.opts.BatchSize)
	flush := func(ctx context.Context) {
		if len(batch) > 0 {
			r.write(ctx, batch)
			batch = batch[:0]
		}
	}
	for {
		select {
		case ex := <-r.queue:
			batch = append(batch, ex)
			if len(batch) >= r.opts.BatchSize {
				flush(ctx)
			}
		case <-ticker.C:
			flush(ctx)
		case <-ctx.Done():
			dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), drainTimeout)
			defer cancel()
			for {
				select {
				case ex := <-r.queue:
					batch = append(batch, ex)
					if len(batch) >= r.opts.BatchSize {
						flush(dctx)
					}
				default:
					flush(dctx)
					r.log.InfoContext(dctx, "recorder drained")
					return
				}
			}
		}
	}
}

func (r *Recorder) write(ctx context.Context, batch []upstream.Exchange) {
	rows := make([][]any, len(batch))
	for i, ex := range batch {
		rows[i] = r.row(ex)
	}
	err := r.insert(ctx, rows)
	var pgErr *pgconn.PgError
	if err != nil && errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "22") && len(rows) > 1 {
		// A data exception (class 22) is caused by one bad row; retry individually
		// so it cannot take the rest of the batch down with it.
		failed := 0
		for _, row := range rows {
			if e := r.insert(ctx, [][]any{row}); e != nil {
				failed++
				err = e
			}
		}
		r.records.WithLabelValues("written").Add(float64(len(rows) - failed))
		r.records.WithLabelValues("failed").Add(float64(failed))
		if failed > 0 {
			r.throttledError("recorder rows rejected", err, slog.Int("rows", failed))
		}
		return
	}
	if err != nil {
		r.records.WithLabelValues("failed").Add(float64(len(rows)))
		r.throttledError("recorder batch insert failed", err, slog.Int("rows", len(rows)))
		return
	}
	r.records.WithLabelValues("written").Add(float64(len(rows)))
}

func (r *Recorder) insert(ctx context.Context, rows [][]any) error {
	ctx, cancel := context.WithTimeout(ctx, r.opts.WriteTimeout)
	defer cancel()

	var sb strings.Builder
	sb.WriteString(`INSERT INTO upstream_responses (created_at, request_id, upstream, endpoint, query, attempt, status_code, outcome, duration_ms, response_bytes, response_body, body_truncated, error) VALUES `)
	args := make([]any, 0, len(rows)*recorderColumns)
	for i, row := range rows {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('(')
		for j := range recorderColumns {
			if j > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "$%d", i*recorderColumns+j+1)
		}
		sb.WriteByte(')')
		args = append(args, row...)
	}
	start := time.Now()
	_, err := r.db.ExecContext(ctx, sb.String(), args...)
	r.queries.observe("insert_upstream_responses", start, err)
	if err == nil {
		r.batchSize.Observe(float64(len(rows)))
	}
	return err
}

// row converts an exchange to column values, sanitising anything PostgreSQL would
// reject (NUL bytes in text, \u0000 in jsonb, invalid JSON) so one odd response can
// never fail a batch.
func (r *Recorder) row(ex upstream.Exchange) []any {
	query, err := json.Marshal(ex.Query)
	if err != nil || ex.Query == nil {
		query = []byte("{}")
	}
	var body any // NULL unless a usable JSON document
	truncated := false
	switch {
	case len(ex.Body) == 0:
	case len(ex.Body) > r.opts.MaxBodyBytes:
		truncated = true
	case json.Valid(ex.Body) && !bytes.Contains(ex.Body, []byte(`\u0000`)):
		body = string(ex.Body)
	}
	var status any
	if ex.StatusCode != 0 {
		status = ex.StatusCode
	}
	errMsg := ex.Error
	if len(errMsg) > maxErrorLen {
		errMsg = strings.ToValidUTF8(errMsg[:maxErrorLen], "")
	}
	return []any{
		ex.Time.UTC(),
		nullable(clean(ex.RequestID)),
		clean(ex.Upstream),
		clean(ex.Endpoint),
		string(query),
		ex.Attempt,
		status,
		clean(ex.Outcome),
		float64(ex.Duration.Microseconds()) / 1000,
		len(ex.Body),
		body,
		truncated,
		nullable(clean(errMsg)),
	}
}

func (r *Recorder) throttledError(msg string, err error, attrs ...any) {
	r.errLogMu.Lock()
	if time.Since(r.lastErrLog) < 10*time.Second {
		r.errLogMu.Unlock()
		return
	}
	r.lastErrLog = time.Now()
	r.errLogMu.Unlock()
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
	}
	r.log.Error(msg, attrs...) //nolint:sloglint // no request context: runs on the writer goroutine
}

func clean(s string) string { return strings.ReplaceAll(s, "\x00", "") }

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
