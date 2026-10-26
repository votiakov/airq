package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"io/fs"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/votiakov/airq/internal/upstream"
	"github.com/votiakov/airq/migrations"
)

// testDB creates an isolated database for one test and drops it afterwards. Tests
// that need PostgreSQL are skipped unless TEST_DATABASE_URL points at a server whose
// user may CREATE DATABASE (CI provides one; locally: `make test-integration`).
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	admin := os.Getenv("TEST_DATABASE_URL")
	if admin == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping PostgreSQL integration test")
	}
	adminDB, err := Open(Options{URL: admin, MaxOpenConns: 2, MaxIdleConns: 1, ConnMaxLifetime: time.Minute, ConnMaxIdleTime: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adminDB.Close() })

	var b [6]byte
	_, _ = rand.Read(b[:])
	name := "airq_test_" + hex.EncodeToString(b[:])
	ctx := context.Background()
	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create test database: %v", err)
	}

	u, err := url.Parse(admin)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	db, err := Open(Options{
		URL: u.String(), MaxOpenConns: 5, MaxIdleConns: 2, ConnMaxLifetime: time.Minute,
		ConnMaxIdleTime: time.Minute, StatementTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_, _ = adminDB.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	return db
}

func migrated(t *testing.T) *sql.DB {
	t.Helper()
	db := testDB(t)
	m, err := NewMigrator(db, migrations.FS, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

// migrationName mirrors the convention documented in migrations/embed.go.
var migrationName = regexp.MustCompile(`^(\d{6})_[a-z0-9_]+\.sql$`)

// TestEmbeddedMigrationsFollowConventions runs without a database so naming and
// structure mistakes fail fast in every build.
func TestEmbeddedMigrationsFollowConventions(t *testing.T) {
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		t.Fatal(err)
	}
	version := 0
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		m := migrationName.FindStringSubmatch(e.Name())
		if m == nil {
			t.Errorf("%s: must be named NNNNNN_lower_snake_case.sql", e.Name())
			continue
		}
		version++
		if n, _ := strconv.Atoi(m[1]); n != version {
			t.Errorf("%s: versions must be contiguous, want %06d", e.Name(), version)
		}
		body, _ := fs.ReadFile(migrations.FS, e.Name())
		for _, section := range []string{"-- +goose Up", "-- +goose Down"} {
			if !strings.Contains(string(body), section) {
				t.Errorf("%s: missing %q section", e.Name(), section)
			}
		}
	}
	if version == 0 {
		t.Fatal("no migrations embedded")
	}
	if _, err := NewMigrator(&sql.DB{}, migrations.FS, nil); err != nil {
		t.Fatalf("goose rejects the embedded migrations: %v", err)
	}
}

func TestMigratorUpDownUp(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	m, err := NewMigrator(db, migrations.FS, nil)
	if err != nil {
		t.Fatal(err)
	}

	if n, err := m.Pending(ctx); err != nil || n == 0 {
		t.Fatalf("Pending = %d, %v", n, err)
	}
	n, err := m.Up(ctx)
	if err != nil || n == 0 {
		t.Fatalf("Up = %d, %v", n, err)
	}
	if n, err := m.Up(ctx); err != nil || n != 0 {
		t.Fatalf("second Up must be a no-op: %d, %v", n, err)
	}
	if _, err := db.ExecContext(ctx, `SELECT 1 FROM upstream_responses LIMIT 1`); err != nil {
		t.Fatalf("table missing after Up: %v", err)
	}
	status, err := m.Status(ctx)
	if err != nil || !status[0].Applied || status[0].AppliedAt.IsZero() {
		t.Fatalf("Status = %+v, %v", status, err)
	}

	total := len(status)
	if n, err := m.Down(ctx, total); err != nil || n != total {
		t.Fatalf("Down = %d, %v", n, err)
	}
	if _, err := db.ExecContext(ctx, `SELECT 1 FROM upstream_responses`); err == nil {
		t.Fatal("table should be gone after Down")
	}
	if n, err := m.Up(ctx); err != nil || n != total {
		t.Fatalf("re-Up = %d, %v", n, err)
	}
}

func TestMigratorFailureRollsBack(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	m, err := NewMigrator(db, fstest.MapFS{
		"000001_ok.sql":     {Data: []byte("-- +goose Up\nCREATE TABLE ok (a int);\n-- +goose Down\nDROP TABLE ok;")},
		"000002_broken.sql": {Data: []byte("-- +goose Up\nCREATE TABLE half (a int);\nSELECT * FROM does_not_exist;\n-- +goose Down\nSELECT 1;")},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Up(ctx); err == nil {
		t.Fatal("expected failure")
	}
	if _, err := db.ExecContext(ctx, `SELECT 1 FROM half`); err == nil {
		t.Error("failed migration must be rolled back atomically")
	}
	if n, _ := m.Pending(ctx); n != 1 {
		t.Errorf("Pending = %d, want 1 (first migration kept)", n)
	}
}

func TestMigratorNoTransaction(t *testing.T) {
	db := testDB(t)
	m, err := NewMigrator(db, fstest.MapFS{
		"000001_t.sql": {Data: []byte("-- +goose Up\nCREATE TABLE c (a int);\n-- +goose Down\nDROP TABLE c;")},
		"000002_idx.sql": {Data: []byte("-- +goose NO TRANSACTION\n-- +goose Up\n" +
			"CREATE INDEX CONCURRENTLY IF NOT EXISTS c_a_idx ON c (a);\n-- +goose Down\nDROP INDEX CONCURRENTLY c_a_idx;")},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := m.Up(context.Background()); err != nil || n != 2 {
		t.Fatalf("Up = %d, %v (CONCURRENTLY fails inside a transaction)", n, err)
	}
}

func TestMigratorPendingHasNoSideEffects(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	m, err := NewMigrator(db, migrations.FS, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := m.Pending(ctx); err != nil || n == 0 {
		t.Fatalf("Pending = %d, %v", n, err)
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('goose_db_version') IS NOT NULL`).Scan(&exists); err != nil || exists {
		t.Fatalf("Pending must not create the version table (exists=%v, err=%v)", exists, err)
	}
}

func TestMigratorConcurrentRunsApplyOnce(t *testing.T) {
	db := testDB(t)
	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0
	for range 4 {
		wg.Go(func() {
			m, err := NewMigrator(db, migrations.FS, nil)
			if err != nil {
				t.Error(err)
				return
			}
			n, err := m.Up(context.Background())
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			total += n
			mu.Unlock()
		})
	}
	wg.Wait()
	files, _ := fs.Glob(migrations.FS, "*.sql")
	if total != len(files) {
		t.Fatalf("migrations applied %d times in total, want %d (advisory lock)", total, len(files))
	}
}

func TestOpenSetsSessionGuards(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	for setting, want := range map[string]string{
		"application_name":                    "airq",
		"statement_timeout":                   "5s",
		"lock_timeout":                        "5s",
		"idle_in_transaction_session_timeout": "20s",
	} {
		var got string
		if err := db.QueryRowContext(ctx, "SHOW "+setting).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s = %q, want %q", setting, got, want)
		}
	}
}

func TestOpenRejectsInvalidURL(t *testing.T) {
	_, err := Open(Options{URL: "postgres://user:hunter2@[::1:5432/db"})
	if err == nil {
		t.Fatal("expected parse error")
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("parse error leaks the password: %v", err)
	}
}

func newRecorder(db *sql.DB, reg *prometheus.Registry, buffer int) *Recorder {
	return NewRecorder(db, NewMetrics(reg, db), RecorderOptions{
		Buffer: buffer, BatchSize: 10, FlushInterval: 20 * time.Millisecond,
		MaxBodyBytes: 64, Registerer: reg,
	})
}

func TestRecorderPersistsAndSanitises(t *testing.T) {
	db := migrated(t)
	reg := prometheus.NewRegistry()
	rec := newRecorder(db, reg, 100)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { rec.Run(ctx, 5*time.Second); close(done) }()

	base := upstream.Exchange{
		Time: time.Now(), RequestID: "req-1", Upstream: "air_quality", Endpoint: "/v1/air-quality",
		Query: map[string]string{"latitude": "52.52"}, Attempt: 1, StatusCode: 200, Outcome: "success",
		Duration: 1500 * time.Microsecond,
	}
	cases := []struct {
		mutate    func(*upstream.Exchange)
		wantBody  bool
		truncated bool
	}{
		{func(e *upstream.Exchange) { e.Body = []byte(`{"ok":true}`) }, true, false},
		{func(e *upstream.Exchange) { e.Body = []byte(`<html>not json</html>`) }, false, false},
		{func(e *upstream.Exchange) { e.Body = []byte(`{"s":"` + strings.Repeat("x", 100) + `"}`) }, false, true},
		{func(e *upstream.Exchange) { e.Body = []byte(`{"s":"nul\u0000"}`) }, false, false},
		{func(e *upstream.Exchange) {
			e.StatusCode, e.Outcome, e.Error, e.RequestID = 0, "network", "dial\x00 failed", ""
		}, false, false},
	}
	for i, c := range cases {
		ex := base
		ex.Attempt = i + 1
		c.mutate(&ex)
		rec.Record(ex)
	}
	cancel()
	<-done // Run drains the queue on shutdown

	rows, err := db.QueryContext(context.Background(),
		`SELECT attempt, response_body IS NOT NULL, body_truncated, status_code, request_id, error, duration_ms FROM upstream_responses ORDER BY attempt`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	i := 0
	for rows.Next() {
		var attempt int
		var hasBody, truncated bool
		var status sql.NullInt32
		var reqID, errMsg sql.NullString
		var durationMS float64
		if err := rows.Scan(&attempt, &hasBody, &truncated, &status, &reqID, &errMsg, &durationMS); err != nil {
			t.Fatal(err)
		}
		c := cases[i]
		if hasBody != c.wantBody || truncated != c.truncated {
			t.Errorf("row %d: body=%v truncated=%v, want %v/%v", attempt, hasBody, truncated, c.wantBody, c.truncated)
		}
		if attempt == 1 && (durationMS != 1.5 || status.Int32 != 200 || reqID.String != "req-1") {
			t.Errorf("row 1 = duration %v status %v request %v", durationMS, status, reqID)
		}
		if attempt == 5 && (status.Valid || reqID.Valid || errMsg.String != "dial failed") {
			t.Errorf("row 5: status=%v request=%v error=%q", status, reqID, errMsg.String)
		}
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if i != len(cases) {
		t.Fatalf("persisted %d rows, want %d", i, len(cases))
	}
	if got := testutil.ToFloat64(rec.records.WithLabelValues("written")); got != float64(len(cases)) {
		t.Errorf("written = %v", got)
	}
}

func TestRecorderDropsWhenQueueFull(t *testing.T) {
	db, err := Open(Options{URL: "postgres://u:p@127.0.0.1:1/none", MaxOpenConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	rec := newRecorder(db, reg, 2)
	for range 5 {
		rec.Record(upstream.Exchange{}) // Run not started: nothing drains
	}
	if got := testutil.ToFloat64(rec.records.WithLabelValues("dropped")); got != 3 {
		t.Errorf("dropped = %v, want 3", got)
	}
	if got := testutil.ToFloat64(rec.records.WithLabelValues("written")); got != 0 {
		t.Errorf("written = %v", got)
	}
}

func TestRecorderCountsFailuresWhenDatabaseIsDown(t *testing.T) {
	db, err := Open(Options{URL: "postgres://u:p@127.0.0.1:1/none?connect_timeout=1", MaxOpenConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	rec := newRecorder(db, reg, 10)
	rec.Record(upstream.Exchange{Time: time.Now(), Upstream: "u", Endpoint: "/", Attempt: 1, Outcome: "success"})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec.Run(ctx, 2*time.Second)

	if got := testutil.ToFloat64(rec.records.WithLabelValues("failed")); got != 1 {
		t.Errorf("failed = %v, want 1", got)
	}
}

func TestRetentionPrunesOnlyExpiredRows(t *testing.T) {
	db := migrated(t)
	ctx := context.Background()
	_, err := db.ExecContext(ctx, `
		INSERT INTO upstream_responses (created_at, upstream, endpoint, attempt, outcome, duration_ms)
		SELECT now() - make_interval(days => d), 'air_quality', '/v1/air-quality', 1, 'success', 1
		FROM generate_series(1, 40) AS d`)
	if err != nil {
		t.Fatal(err)
	}

	reg := prometheus.NewRegistry()
	r := NewRetention(db, NewMetrics(reg, db), RetentionOptions{
		MaxAge: 30*24*time.Hour + time.Hour, Interval: time.Hour, BatchSize: 3, Registerer: reg,
	})
	n, err := r.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Errorf("deleted %d rows, want 10 (days 31-40) across several batches", n)
	}
	var left int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM upstream_responses`).Scan(&left); err != nil || left != 30 {
		t.Errorf("rows left = %d, %v", left, err)
	}
	if got := testutil.ToFloat64(r.deleted); got != 10 {
		t.Errorf("deleted metric = %v", got)
	}
	if got := testutil.ToFloat64(r.lastSuccess); got == 0 {
		t.Error("last success timestamp not set")
	}
}

func TestRetentionSkipsWhenAnotherReplicaHoldsTheLock(t *testing.T) {
	db := migrated(t)
	ctx := context.Background()
	holder, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Close() }()
	if _, err := holder.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, retentionLockID); err != nil {
		t.Fatal(err)
	}

	reg := prometheus.NewRegistry()
	r := NewRetention(db, NewMetrics(reg, db), RetentionOptions{MaxAge: time.Hour, Interval: time.Hour, Registerer: reg})
	if _, err := r.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(r.runs.WithLabelValues("skipped")); got != 1 {
		t.Errorf("skipped runs = %v", got)
	}
}

func TestPoolMetricsAreExported(t *testing.T) {
	db := testDB(t)
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg, db)
	if err := Ping(context.Background(), db, time.Second); err != nil {
		t.Fatal(err)
	}
	m.observe("ping", time.Now(), nil)
	n, err := testutil.GatherAndCount(reg, "go_sql_open_connections", "airq_db_query_duration_seconds")
	if err != nil || n != 2 {
		t.Fatalf("gathered %d series, err %v", n, err)
	}
}

func TestOpenLockTimeoutWithoutStatementTimeout(t *testing.T) {
	admin := os.Getenv("TEST_DATABASE_URL")
	if admin == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping PostgreSQL integration test")
	}
	db, err := Open(Options{URL: admin, MaxOpenConns: 1, ConnMaxLifetime: time.Minute, LockTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var stmt, lockT string
	if err := db.QueryRowContext(context.Background(), `SELECT current_setting('statement_timeout'), current_setting('lock_timeout')`).
		Scan(&stmt, &lockT); err != nil {
		t.Fatal(err)
	}
	if stmt != "0" || lockT != "10s" {
		t.Errorf("statement_timeout=%q lock_timeout=%q, want 0 and 10s (migration session)", stmt, lockT)
	}
}
