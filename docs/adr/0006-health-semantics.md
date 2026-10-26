# ADR 0006: Health semantics — soft dependencies degrade, they do not fail readiness

- Status: accepted
- Date: 2026-10-26

## Context

Orchestrators act on health endpoints: a failing liveness probe restarts the
container, and a failing readiness probe removes it from the load balancer. Health
checks that are too eager cause outages: a database blip that fails every replica's
readiness at once turns a partial degradation into a total one.

airq has two external dependencies, and neither is needed to serve a useful answer:

- **Open-Meteo:** cached and stale-window data can still be served, and uncached
  locations fail fast with `503` and `Retry-After`.
- **PostgreSQL:** only holds the audit log ([ADR 0005](0005-best-effort-audit-recorder.md)).

## Decision

- **`/livez`** checks nothing beyond the process serving HTTP. Restarting does not fix
  a dependency.
- **`/readyz`** fails (`503`) while draining for shutdown, or if a check marked
  *critical* fails. **No check is critical today.** The `Critical` flag exists so that
  a future hard dependency can be declared explicitly.
- **`/health`** reports each check and an aggregate status:
  - `ok`;
  - `degraded` (a soft check is failing; HTTP `200`);
  - `down` (a critical check is failing, or the instance is draining; HTTP `503`).
- **Checks:**
  - `database`: a ping;
  - `migrations`: pending versions, read-only, safe for the DML role;
  - `upstream_<name>`: circuit state, with no vendor call. Probing a third party from
    every replica every few seconds would spend quota and tell us less than the real
    traffic does.
- **Results are cached** for `HEALTH_CACHE_TTL` (2s) and refreshed every
  `HEALTH_INTERVAL` (15s) in the background. Probe frequency cannot amplify into
  dependency load, and `airq_health_check_up` stays current for alerts.
- **Public `/health` redacts errors** to `timeout` or `unavailable`. The raw error is
  logged on each state change, and served by a detailed report on the private admin
  listener.

## Alternatives considered

| Option | Why not |
|---|---|
| Fail readiness when the database is down | Every replica would leave rotation at once for a dependency the API does not need |
| Fail readiness when the vendor circuit is open | Same, and it would stop serving cached data, which is the point of the cache |
| [`alexliesenfeld/health`](https://github.com/alexliesenfeld/health) | Evaluated (v0.8.1): its statuses are `up`, `down` and `unknown`, with no soft or degraded state and no draining state ([ADR 0001](0001-dependency-policy.md)) |
| Raw errors on public `/health` | Leaked database user, database name and internal hostnames, found during the e2e run |

## Consequences

- Dependency outages show up as `degraded` health, alerts and dashboards, not as
  replicas disappearing.
- A replica that is truly broken in a way checks cannot see (for example, a handler
  deadlock) is caught by SLO alerts, not by probes. That is the usual trade-off for
  cheap, dependency-free liveness.
- Shutdown is clean: readiness fails first, the load balancer drains, then
  connections close ([Operations](../OPERATIONS.md#deploying)).
