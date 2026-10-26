package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// Migrator applies schema migrations with pressly/goose. Runs are serialised across
// replicas, jobs and humans by a PostgreSQL session advisory lock, and each migration
// runs in its own transaction unless annotated `-- +goose NO TRANSACTION`.
type Migrator struct {
	db       *sql.DB
	provider *goose.Provider
}

// MigrationStatus pairs a migration file with its applied state.
type MigrationStatus struct {
	Version   int64
	Name      string
	Applied   bool
	AppliedAt time.Time
}

// NewMigrator loads the migrations in fsys. logger may be nil.
func NewMigrator(db *sql.DB, fsys fs.FS, logger *slog.Logger) (*Migrator, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	// Poll for the lock every second for up to five minutes: another instance may be
	// mid-migration during a rolling deploy.
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockTimeout(1, 300))
	if err != nil {
		return nil, err
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, fsys,
		goose.WithSessionLocker(locker),
		goose.WithSlog(logger),
		goose.WithDisableGlobalRegistry(true), // SQL migrations only; no init()-registered Go code
	)
	if err != nil {
		return nil, fmt.Errorf("load migrations: %w", err)
	}
	return &Migrator{db: db, provider: p}, nil
}

// Up applies every pending migration and returns how many were applied.
func (m *Migrator) Up(ctx context.Context) (int, error) {
	res, err := m.provider.Up(ctx)
	return len(res), err
}

// Down rolls back up to steps migrations, newest first, and returns how many were
// rolled back.
func (m *Migrator) Down(ctx context.Context, steps int) (int, error) {
	for n := range steps {
		if _, err := m.provider.Down(ctx); err != nil {
			if errors.Is(err, goose.ErrNoNextVersion) {
				return n, nil
			}
			return n, err
		}
	}
	return steps, nil
}

// Status lists every known migration with its applied state.
func (m *Migrator) Status(ctx context.Context) ([]MigrationStatus, error) {
	st, err := m.provider.Status(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]MigrationStatus, 0, len(st))
	for _, s := range st {
		out = append(out, MigrationStatus{
			Version:   s.Source.Version,
			Name:      path.Base(s.Source.Path),
			Applied:   s.State == goose.StateApplied,
			AppliedAt: s.AppliedAt,
		})
	}
	return out, nil
}

// Pending counts unapplied migrations. It has no side effects, so it is safe for
// health checks run by the least-privileged application role: goose would otherwise
// create its version table on first use.
func (m *Migrator) Pending(ctx context.Context) (int, error) {
	var tracked bool
	if err := m.db.QueryRowContext(ctx, `SELECT to_regclass('goose_db_version') IS NOT NULL`).Scan(&tracked); err != nil {
		return 0, err
	}
	if !tracked {
		return len(m.provider.ListSources()), nil
	}
	st, err := m.Status(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, s := range st {
		if !s.Applied {
			n++
		}
	}
	return n, nil
}
