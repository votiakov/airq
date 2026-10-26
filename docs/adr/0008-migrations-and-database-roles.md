# ADR 0008: Migrations as a separate step, with least-privilege database roles

- Status: accepted
- Date: 2026-10-26

## Context

The brief requires that every database change lives in a migration file. Two
production concerns follow:

- **Who runs migrations, and when.** Running them inside the service at startup is
  simple, but every replica races for the schema, and the long-lived service needs
  DDL rights.
- **What the service may do.** A service connected as the database owner can drop
  tables. A SQL injection or a compromised process then gets the whole database.

## Decision

- **goose** (`pressly/goose/v3` Provider API) with SQL files embedded in the binary
  ([ADR 0001](0001-dependency-policy.md) has the comparison with golang-migrate):
  - each migration runs in a transaction, so failures roll back on Postgres;
  - `-- +goose NO TRANSACTION` for `CREATE INDEX CONCURRENTLY`;
  - a session advisory lock serialises concurrent runners.
- **Migrations are a separate one-shot step**: `airq migrate up` as the compose
  `migrate` service, or a pre-deploy Job in production. The service starts only after
  it succeeds. `DB_MIGRATE_ON_START` remains for single-instance setups.
- **Two roles**, created once by `deploy/postgres/initdb/10-roles.sh`:
  - `airq_migrator` owns the `public` schema and runs DDL. It is used only by
    `airq migrate` (`MIGRATE_DATABASE_URL`).
  - `airq_app`, the service, gets:
    - `SELECT` on tables by default privileges;
    - `INSERT` and `DELETE` on `upstream_responses` (migration 000002);
    - nothing else: no `UPDATE`, no DDL, no write access to `goose_db_version`;
    - a connection limit of 50;
    - role-level `statement_timeout` and `idle_in_transaction_session_timeout`.

  `PUBLIC` loses `CONNECT` and `CREATE`.
- **Grants live in migrations**, so they are versioned with the tables they protect.
  The grant migration is a no-op where the role does not exist (for example, the
  integration-test database).
- **Rules:**
  - migrations are append-only, named `NNNNNN_name.sql`, and enforced by a CI guard,
    because goose stores no checksums;
  - changes follow expand/contract, so the previous release runs on the new schema
    during rolling deploys;
  - Down sections exist, but rollback is normally by redeploying the old image.
- **The `migrations` health check** (read-only, through `to_regclass`, so it works for
  the DML role) reports when the running release is ahead of the schema.

## Alternatives considered

| Option | Why not |
|---|---|
| `golang-migrate` | "Dirty" state after a failed migration needs a manual `force`, and drivers are registered through blank imports |
| Atlas (declarative) | Powerful diffing, but heavier tooling and a separate binary; the brief asks for migration files, which goose keeps as plain SQL |
| Migrate on service startup with the owner role | Replicas race, and the service keeps DDL rights forever |
| One role for everything | Simplest, and the largest blast radius |

## Consequences

- The service cannot change the schema, rewrite audit rows or tamper with migration
  state. `scripts/smoke.sh` asserts each denial against real Postgres.
- Deploys gain a step (run migrations first), which CI/CD and compose encode.
- Role creation is environment bootstrap (`initdb` locally; Terraform or the DBA in
  production), not a migration. It needs superuser rights, and a migration should
  never hold those.
