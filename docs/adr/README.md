# Architecture decision records

Each record captures one significant decision: the context, what was decided, the
alternatives that were considered, and the consequences we accept.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-dependency-policy.md) | Dependency policy, and what is built vs. borrowed | accepted (revised) |
| [0002](0002-open-meteo-vendor.md) | Open-Meteo as the air quality vendor | accepted |
| [0003](0003-caching.md) | In-process caching with stale-if-error and request coalescing | accepted |
| [0004](0004-upstream-resilience.md) | Layered resilience for vendor calls | accepted |
| [0005](0005-best-effort-audit-recorder.md) | Asynchronous, best-effort audit logging to PostgreSQL | accepted |
| [0006](0006-health-semantics.md) | Health semantics: soft dependencies degrade, they do not fail readiness | accepted |
| [0007](0007-observability.md) | Prometheus metrics, OpenTelemetry traces, structured logs, SLO alerts | accepted |
| [0008](0008-migrations-and-database-roles.md) | Migrations as a separate step, with least-privilege database roles | accepted |

New records copy the structure of an existing one, take the next number, and are
never rewritten once accepted. A changed decision gets a new record that supersedes
the old one, or a dated "History" entry for a revision, as in 0001.
