package store

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// migrationLockID is the pg_advisory_lock key that serialises migrations across
// replicas, jobs and humans running `airq migrate` at the same time.
const migrationLockID int64 = 0x61697271_6d696772 // "airqmigr"

// noTxDirective on the first line runs a migration outside a transaction.
const noTxDirective = "-- airq:no-transaction"

var migrationFile = regexp.MustCompile(`^(\d+)_([a-z0-9_]+)\.(up|down)\.sql$`)

// Migration is one versioned schema change.
type Migration struct {
	Version  int64
	Name     string
	Up       string
	Down     string
	NoTx     bool
	Checksum string
}

// MigrationStatus pairs a migration with its applied state.
type MigrationStatus struct {
	Migration
	Applied   bool
	AppliedAt time.Time
}

// LoadMigrations parses migrations from fsys. Any .sql file that does not follow the
// naming convention is an error, so a typo cannot silently skip a migration.
func LoadMigrations(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	byVersion := map[int64]*Migration{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".sql") {
			continue
		}
		m := migrationFile.FindStringSubmatch(name)
		if m == nil {
			return nil, fmt.Errorf("migration %q does not match NNNNNN_name.(up|down).sql", name)
		}
		version, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil || version < 1 {
			return nil, fmt.Errorf("migration %q: invalid version", name)
		}
		body, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		mig := byVersion[version]
		if mig == nil {
			mig = &Migration{Version: version, Name: m[2]}
			byVersion[version] = mig
		}
		if mig.Name != m[2] {
			return nil, fmt.Errorf("migration %d has conflicting names %q and %q", version, mig.Name, m[2])
		}
		switch m[3] {
		case "up":
			if mig.Up != "" {
				return nil, fmt.Errorf("duplicate up migration for version %d", version)
			}
			mig.Up = string(body)
			sum := sha256.Sum256(body)
			mig.Checksum = hex.EncodeToString(sum[:])
			mig.NoTx = strings.HasPrefix(strings.TrimSpace(mig.Up), noTxDirective)
		case "down":
			mig.Down = string(body)
		}
	}
	out := make([]Migration, 0, len(byVersion))
	for _, m := range byVersion {
		if strings.TrimSpace(m.Up) == "" {
			return nil, fmt.Errorf("migration %d (%s) has no up script", m.Version, m.Name)
		}
		out = append(out, *m)
	}
	slices.SortFunc(out, func(a, b Migration) int { return cmp.Compare(a.Version, b.Version) })
	return out, nil
}

// Migrator applies migrations.
type Migrator struct {
	db         *sql.DB
	migrations []Migration
	log        *slog.Logger
}

// NewMigrator loads migrations from fsys.
func NewMigrator(db *sql.DB, fsys fs.FS, logger *slog.Logger) (*Migrator, error) {
	migs, err := LoadMigrations(fsys)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Migrator{db: db, migrations: migs, log: logger.With(slog.String("component", "migrator"))}, nil
}

// Up applies all pending migrations and returns how many were applied.
func (m *Migrator) Up(ctx context.Context) (int, error) {
	applied := 0
	err := m.withLock(ctx, func(conn *sql.Conn) error {
		state, err := m.appliedState(ctx, conn)
		if err != nil {
			return err
		}
		for _, mig := range m.migrations {
			if _, done := state[mig.Version]; done {
				continue
			}
			if err := m.apply(ctx, conn, mig); err != nil {
				return err
			}
			applied++
		}
		return nil
	})
	return applied, err
}

// Down rolls back the most recent `steps` applied migrations.
func (m *Migrator) Down(ctx context.Context, steps int) (int, error) {
	rolled := 0
	err := m.withLock(ctx, func(conn *sql.Conn) error {
		state, err := m.appliedState(ctx, conn)
		if err != nil {
			return err
		}
		for i := len(m.migrations) - 1; i >= 0 && rolled < steps; i-- {
			mig := m.migrations[i]
			if _, done := state[mig.Version]; !done {
				continue
			}
			if strings.TrimSpace(mig.Down) == "" {
				return fmt.Errorf("migration %d (%s) has no down script", mig.Version, mig.Name)
			}
			start := time.Now()
			err := inTx(ctx, conn, func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, mig.Down); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, `DELETE FROM schema_migrations WHERE version = $1`, mig.Version)
				return err
			})
			if err != nil {
				return fmt.Errorf("roll back migration %d (%s): %w", mig.Version, mig.Name, err)
			}
			m.log.InfoContext(ctx, "rolled back migration", "version", mig.Version, "name", mig.Name, "duration", time.Since(start))
			rolled++
		}
		return nil
	})
	return rolled, err
}

// Status reports every known migration and whether it is applied.
func (m *Migrator) Status(ctx context.Context) ([]MigrationStatus, error) {
	var out []MigrationStatus
	err := m.withLock(ctx, func(conn *sql.Conn) error {
		state, err := m.appliedState(ctx, conn)
		if err != nil {
			return err
		}
		for _, mig := range m.migrations {
			st := MigrationStatus{Migration: mig}
			if at, ok := state[mig.Version]; ok {
				st.Applied, st.AppliedAt = true, at.appliedAt
			}
			out = append(out, st)
		}
		return nil
	})
	return out, err
}

// Pending returns how many migrations are not yet applied. It is read-only (no lock,
// no DDL) so the least-privileged runtime role can call it from health checks.
func (m *Migrator) Pending(ctx context.Context) (int, error) {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()
	var exists bool
	if err := conn.QueryRowContext(ctx, `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&exists); err != nil {
		return 0, err
	}
	if !exists {
		return len(m.migrations), nil
	}
	state, err := m.readState(ctx, conn)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, mig := range m.migrations {
		if _, ok := state[mig.Version]; !ok {
			n++
		}
	}
	return n, nil
}

func (m *Migrator) apply(ctx context.Context, conn *sql.Conn, mig Migration) error {
	start := time.Now()
	record := func(ctx context.Context, ex interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	},
	) error {
		_, err := ex.ExecContext(ctx,
			`INSERT INTO schema_migrations (version, name, checksum, execution_ms) VALUES ($1, $2, $3, $4)`,
			mig.Version, mig.Name, mig.Checksum, time.Since(start).Milliseconds())
		return err
	}

	var err error
	if mig.NoTx {
		// Non-transactional migrations (e.g. CREATE INDEX CONCURRENTLY) must be
		// idempotent (IF NOT EXISTS) because a failure can leave partial changes.
		if _, err = conn.ExecContext(ctx, mig.Up); err == nil {
			err = record(ctx, conn)
		}
	} else {
		err = inTx(ctx, conn, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, mig.Up); err != nil {
				return err
			}
			return record(ctx, tx)
		})
	}
	if err != nil {
		return fmt.Errorf("apply migration %d (%s): %w", mig.Version, mig.Name, err)
	}
	m.log.InfoContext(ctx, "applied migration", "version", mig.Version, "name", mig.Name, "duration", time.Since(start))
	return nil
}

type appliedMigration struct {
	checksum  string
	appliedAt time.Time
}

// appliedState ensures schema_migrations exists, then reads it and verifies that
// applied migrations have not been edited since (checksum drift), which would mean
// environments diverge.
func (m *Migrator) appliedState(ctx context.Context, conn *sql.Conn) (map[int64]appliedMigration, error) {
	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version      bigint      PRIMARY KEY,
		name         text        NOT NULL,
		checksum     text        NOT NULL,
		execution_ms bigint      NOT NULL,
		applied_at   timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return nil, fmt.Errorf("ensure schema_migrations: %w", err)
	}
	return m.readState(ctx, conn)
}

// readState reads schema_migrations and verifies checksums of applied migrations.
func (m *Migrator) readState(ctx context.Context, conn *sql.Conn) (map[int64]appliedMigration, error) {
	rows, err := conn.QueryContext(ctx, `SELECT version, checksum, applied_at FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	defer func() { _ = rows.Close() }()

	state := map[int64]appliedMigration{}
	for rows.Next() {
		var v int64
		var a appliedMigration
		if err := rows.Scan(&v, &a.checksum, &a.appliedAt); err != nil {
			return nil, err
		}
		state[v] = a
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	known := map[int64]bool{}
	for _, mig := range m.migrations {
		known[mig.Version] = true
		if a, ok := state[mig.Version]; ok && a.checksum != mig.Checksum {
			return nil, fmt.Errorf("migration %d (%s) was modified after it was applied; add a new migration instead", mig.Version, mig.Name)
		}
	}
	for v := range state {
		if !known[v] {
			// Normal during a rollback to an older binary; schema changes are
			// backwards compatible by policy, so warn rather than refuse to start.
			m.log.WarnContext(ctx, "database has a migration unknown to this binary", "version", v)
		}
	}
	return state, nil
}

// withLock runs fn on a dedicated connection holding the migration advisory lock.
func (m *Migrator) withLock(ctx context.Context, fn func(*sql.Conn) error) error {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// Migrations may legitimately run longer than the application's statement_timeout
	// and must wait for a concurrent migrator; the caller's ctx bounds both.
	if _, err := conn.ExecContext(ctx, `SET statement_timeout = 0; SET lock_timeout = 0`); err != nil {
		return err
	}
	defer func() {
		// Session settings would otherwise leak into the pool with this connection.
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(uctx, `RESET ALL`); err != nil {
			m.log.ErrorContext(ctx, "reset migration session", "error", err)
		}
	}()
	// Session-level lock: waits for any concurrent migrator.
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(uctx, `SELECT pg_advisory_unlock($1)`, migrationLockID); err != nil {
			m.log.ErrorContext(ctx, "release migration lock", "error", err)
		}
	}()
	// Individual DDL statements must not queue forever behind application traffic.
	if _, err := conn.ExecContext(ctx, `SET lock_timeout = '10s'`); err != nil {
		return err
	}
	return fn(conn)
}

func inTx(ctx context.Context, conn *sql.Conn, fn func(*sql.Tx) error) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	return tx.Commit()
}
