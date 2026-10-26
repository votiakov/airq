// Package migrations embeds the SQL schema migrations so the binary is the single
// deployable artefact. They run with pressly/goose (`airq migrate`), so the goose CLI
// can also apply them if ever needed.
//
// Rules:
//   - One file per version, NNNNNN_description.sql, with `-- +goose Up` and
//     `-- +goose Down` sections.
//   - Append-only: an applied migration is never edited, renamed or deleted (CI fails
//     the build if one is); fix forward with a new version.
//   - Each runs in a transaction unless annotated `-- +goose NO TRANSACTION` (needed
//     for CREATE INDEX CONCURRENTLY).
//   - Every change is backwards compatible with the previous release (expand/contract),
//     so old and new versions can run side by side during a rolling deploy.
package migrations

import "embed"

// FS holds every *.sql migration.
//
//go:embed *.sql
var FS embed.FS
