package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"net/url"
	"os"
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

func TestLoadMigrations(t *testing.T) {
	migs, err := LoadMigrations(fstest.MapFS{
		"000002_second.up.sql":   {Data: []byte("-- airq:no-transaction\nCREATE INDEX CONCURRENTLY x ON t (a);")},
		"000001_first.up.sql":    {Data: []byte("CREATE TABLE t (a int);")},
		"000001_first.down.sql":  {Data: []byte("DROP TABLE t;")},
		"README.md":              {Data: []byte("ignored")},
		"000010_tenth.up.sql":    {Data: []byte("SELECT 1;")},
		"000010_tenth.down.sql":  {Data: []byte("SELECT 1;")},
		"subdir/000099_x.up.sql": {Data: []byte("ignored: only the top level is read")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(migs) != 3 || migs[0].Version != 1 || migs[1].Version != 2 || migs[2].Version != 10 {
		t.Fatalf("unexpected order: %+v", migs)
	}
	if migs[0].NoTx || !migs[1].NoTx {
		t.Error("no-transaction directive not detected")
	}
	if migs[0].Down == "" || migs[0].Checksum == "" {
		t.Error("down script or checksum missing")
	}
}

func TestLoadMigrationsRejectsMistakes(t *testing.T) {
	tests := map[string]fstest.MapFS{
		"bad name":       {"1-first.sql": {Data: []byte("x")}},
		"missing up":     {"000001_first.down.sql": {Data: []byte("x")}},
		"empty up":       {"000001_first.up.sql": {Data: []byte("  ")}},
		"name conflict":  {"000001_a.up.sql": {Data: []byte("x")}, "000001_b.down.sql": {Data: []byte("x")}},
		"version zero":   {"000000_zero.up.sql": {Data: []byte("x")}},
		"uppercase name": {"000001_First.up.sql": {Data: []byte("x")}},
	}
	for name, fsys := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadMigrations(fsys); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestEmbeddedMigrationsAreValid(t *testing.T) {
	migs, err := LoadMigrations(migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	if len(migs) == 0 {
		t.Fatal("no migrations embedded")
	}
	for i, m := range migs {
		if m.Version != int64(i+1) {
			t.Errorf("migration versions must be contiguous: got %d at position %d", m.Version, i)
		}
		if strings.TrimSpace(m.Down) == "" {
			t.Errorf("migration %d has no down script", m.Version)
		}
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

	// The migrator's session overrides must not leak into pooled connections.
	var timeout string
	if err := db.QueryRowContext(ctx, `SHOW statement_timeout`).Scan(&timeout); err != nil || timeout != "5s" {
		t.Errorf("statement_timeout after migration = %q, %v; want 5s", timeout, err)
	}
}

func TestMigratorDetectsChecksumDrift(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	orig := fstest.MapFS{"000001_t.up.sql": {Data: []byte("CREATE TABLE drift (a int);")}}
	m, _ := NewMigrator(db, orig, nil)
	if _, err := m.Up(ctx); err != nil {
		t.Fatal(err)
	}
	edited := fstest.MapFS{"000001_t.up.sql": {Data: []byte("CREATE TABLE drift (a bigint);")}}
	m2, _ := NewMigrator(db, edited, nil)
	if _, err := m2.Up(ctx); err == nil || !strings.Contains(err.Error(), "modified after it was applied") {
		t.Fatalf("expected drift error, got %v", err)
	}
}

func TestMigratorFailureRollsBack(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	m, _ := NewMigrator(db, fstest.MapFS{
		"000001_ok.up.sql":     {Data: []byte("CREATE TABLE ok (a int);")},
		"000002_broken.up.sql": {Data: []byte("CREATE TABLE half (a int); SELECT * FROM does_not_exist;")},
	}, nil)
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
	m, _ := NewMigrator(db, fstest.MapFS{
		"000001_t.up.sql":   {Data: []byte("CREATE TABLE c (a int);")},
		"000002_idx.up.sql": {Data: []byte("-- airq:no-transaction\nCREATE INDEX CONCURRENTLY IF NOT EXISTS c_a_idx ON c (a);")},
	}, nil)
	if n, err := m.Up(context.Background()); err != nil || n != 2 {
		t.Fatalf("Up = %d, %v (CONCURRENTLY fails inside a transaction)", n, err)
	}
}

func TestMigratorConcurrentRunsApplyOnce(t *testing.T) {
	db := testDB(t)
	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0
	for range 4 {
		wg.Go(func() {
			m, _ := NewMigrator(db, migrations.FS, nil)
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
	migs, _ := LoadMigrations(migrations.FS)
	if total != len(migs) {
		t.Fatalf("migrations applied %d times in total, want %d (advisory lock)", total, len(migs))
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
