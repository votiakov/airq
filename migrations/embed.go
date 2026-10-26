// Package migrations embeds the SQL schema migrations so the binary is the single
// deployable artefact. Files follow golang-migrate naming
// (NNNNNN_description.up.sql / .down.sql), so the standard migrate CLI can also run
// them if the team ever prefers it over `airq migrate`.
//
// Rules: migrations are append-only (applied files are checksummed and must never be
// edited), each runs in its own transaction unless its first line is
// `-- airq:no-transaction` (needed for CREATE INDEX CONCURRENTLY), and every change
// must be backwards compatible with the previous release (expand/contract).
package migrations

import "embed"

// FS holds every *.sql migration.
//
//go:embed *.sql
var FS embed.FS
