# Operations

How to run, observe and repair airq. Every Prometheus alert links to its runbook
section below.

## Service level objectives

Both SLOs cover the API routes (`/v1/...`), measured at the service over a 30-day window.

| SLO | Target | SLI (recording rule) | Error budget |
|---|---|---|---|
| Availability | 99.5% of requests do not return 5xx | `1 - airq:api_request_errors:ratio_rate5m` | 0.5%, about 3.6 hours of total outage per 30 days |
| Latency | 99% of requests complete within 1s | `airq:api_request_latency_under_1s:ratio_rate5m` | 1% of requests may be slower |

Notes:
- **Shedding counts.** `503`s from load shedding or an open circuit count against
  availability. The SLO is what clients experience, and shedding is a symptom we want
  to see.
- **Client errors do not.** `4xx` responses (bad input, unknown place, rate limited)
  are excluded.
- **Stale answers succeed.** A stale answer during a vendor outage counts as a success:
  it is the designed behaviour, and `AirqServingStaleData` tracks it.

**Error budget policy.** While more than half of the monthly budget is spent, feature
work waits behind reliability fixes. Releases that burned budget get a written review.

Burn-rate alerting follows the multi-window, multi-burn-rate approach from the Google
SRE workbook:

| Alert | Condition | Meaning |
|---|---|---|
| `AirqErrorBudgetBurnFast` (page) | error ratio > 14.4 × 0.5% over both 5m and 1h | 2% of the monthly budget gone in an hour; exhausted in about 2 days |
| `AirqErrorBudgetBurnSlow` (ticket) | error ratio > 6 × 0.5% over both 30m and 6h | 5% gone in 6 hours; exhausted in 5 days |

The short window makes the alert reset quickly after recovery; the long window stops
brief spikes from paging. The rules have unit tests in
`deploy/prometheus/tests/airq.rules.test.yml` (`make rules-test`).

## Where to look

| Question | Tool |
|---|---|
| Is it healthy now? | `curl localhost:8080/health`, Grafana "airq overview" → *Health and runtime* |
| What is failing, and since when? | Grafana: *Service*, *Upstream*, *Database* rows; Prometheus → Alerts |
| Why was this request slow or failing? | Exemplar dot on a latency panel → Jaeger trace; or search logs by `request_id` |
| What did the vendor actually return? | `SELECT * FROM upstream_responses WHERE request_id = '…'` (`make psql`) |
| What is the effective config? | First log line, `"msg":"starting airq"` (secrets redacted) |
| Which build is running? | `airq_build_info`, `/health` → `version`, `airq version` |

Correlation: every response carries `X-Request-ID`. The same ID appears:
- on every log line for that request;
- as `request_id` on its audit rows;
- as the `airq.request_id` span attribute.

When the caller sent no ID, the request ID equals the trace ID. So one ID finds the
logs, the trace and the vendor payloads.

```sh
docker compose logs airq | jq -c 'select(.request_id == "…")'
docker compose logs airq | jq -c 'select(.level == "ERROR" or .level == "WARN")'
docker compose logs airq | jq -c 'select(.msg == "http request" and .status >= 500)'
```

The admin listener (`ADMIN_ADDR`, never published) serves `/health` with raw error
messages and, with `PPROF_ENABLED=true`, `/debug/pprof/`.

## Alerts and runbooks

| Alert | Severity | Fires when |
|---|---|---|
| [AirqTargetDown](#airqtargetdown) | critical | Prometheus cannot scrape an instance for 1m |
| [AirqErrorBudgetBurnFast / Slow](#airqerrorbudgetburn) | critical / warning | 5xx ratio burning the availability budget 14.4× / 6× too fast |
| [AirqHighLatency](#airqhighlatency) | warning | < 99% of requests under 1s for 10m |
| [AirqUpstreamCircuitOpen](#airqupstreamcircuitopen) | warning | A vendor circuit has been open for 1m |
| [AirqUpstreamErrors](#airqupstreamerrors) | warning | > 20% of vendor calls failing for 10m |
| [AirqServingStaleData](#airqservingstaledata) | info | Stale entries served for 15m |
| [AirqLoadShedding](#airqloadshedding) | warning | Requests rejected at `HTTP_MAX_IN_FLIGHT` for 5m |
| [AirqPanics](#airqpanics) | warning | Any recovered handler panic in 10m |
| [AirqDatabaseDown](#airqdatabasedown) | warning | Database health check failing for 2m |
| [AirqMigrationsPending](#airqmigrationspending) | warning | Schema behind the running release for 10m |
| [AirqRecorderLosingRecords](#airqrecorderlosingrecords) | warning | Audit rows dropped or failing for 10m |
| [AirqRetentionStale](#airqretentionstale) | warning | No successful retention run for 3h |
| [AirqDBPoolSaturated](#airqdbpoolsaturated) | warning | > 90% of DB connections in use for 10m |

Database alerts are warnings, not pages: the API keeps serving without PostgreSQL, so
these lose audit data, not availability.

### AirqTargetDown

**Meaning:** Prometheus cannot scrape `/metrics`. The process is down, or not
reachable from Prometheus.

**Impact:** Possibly total for that instance. Also, every other alert is blind for it.

**Diagnose:**
- `docker compose ps airq`. Check the container state and restart count.
- `docker compose logs --tail 100 airq`:
  - a config error at startup lists every invalid variable;
  - `server stopped unexpectedly` means the listener failed;
  - an OOM kill shows as exit code 137.
- `curl -i localhost:8080/livez` from the host, then from the Prometheus container.
  If only the latter fails, the problem is networking or service discovery.

**Mitigate:**
- Fix the config and restart.
- For OOM, look at `go_memstats_heap_inuse_bytes` before the crash. Lower
  `CACHE_MAX_ENTRIES` or `RECORDER_BUFFER`, or raise the memory limit together with
  `GOMEMLIMIT`.
- Roll back if a deploy caused it.

### AirqErrorBudgetBurn

Covers `AirqErrorBudgetBurnFast` (page now) and `AirqErrorBudgetBurnSlow` (fix within
the day).

**Meaning:** Too many API requests return 5xx.

**Diagnose:** Grafana *Responses by status code* shows which code dominates.

| Code | Usual cause | Next step |
|---|---|---|
| `503` + `airq_http_rejected_requests_total{reason="overloaded"}` | Load shedding | [AirqLoadShedding](#airqloadshedding) |
| `503` + circuit open | Vendor outage with cold cache | [AirqUpstreamCircuitOpen](#airqupstreamcircuitopen) |
| `502` | Vendor returning errors or garbage | [AirqUpstreamErrors](#airqupstreamerrors) |
| `504` | Vendor slow; attempts exhausting the request deadline | Check *Attempt latency p95*, [AirqUpstreamErrors](#airqupstreamerrors) |
| `500` | Our bug, or a panic | `jq 'select(.level=="ERROR")'` on logs; [AirqPanics](#airqpanics) |

**Mitigate:** Roll back if the burn started with a deploy (`airq_build_info` changes
mark deploys). Otherwise follow the linked runbook.

### AirqHighLatency

**Meaning:** Fewer than 99% of API requests complete within 1s.

**Diagnose:**
- *Latency percentiles*: click an exemplar on the p99 line to open a slow trace in
  Jaeger. The span tree shows where the time went:
  - vendor attempts and retries;
  - quota guard wait (`airq_upstream_rate_limiter_wait_seconds`);
  - bulkhead wait;
  - or the handler itself.
- A falling cache hit ratio (*Hit ratio by cache*) after a deploy or restart is
  expected while the caches warm up.

**Mitigate:**
- **Vendor slow:** nothing to fix locally. Stale-if-error and the breaker limit the
  damage. Consider lowering `UPSTREAM_ATTEMPT_TIMEOUT` so retries happen sooner.
- **Quota waits:** traffic outgrew `UPSTREAM_RATE_LIMIT_RPS`. Raise it only within the
  vendor's terms, or raise cache TTLs.
- **CPU throttling:** check the container CPU limit against `process_cpu_seconds_total`.

### AirqUpstreamCircuitOpen

**Meaning:** At least `UPSTREAM_BREAKER_THRESHOLD` consecutive vendor attempts failed,
so calls fail fast for `UPSTREAM_BREAKER_COOLDOWN`. A single probe then tests recovery
(half-open, value 1 in `airq_upstream_circuit_state`).

**Impact:**
- Cached and stale-window locations are still served.
- Uncached locations get `503` with `Retry-After`.
- `/health` reports `degraded`; readiness is unaffected by design.

**Diagnose:**
- *Calls by outcome* (`timeout`, `server_error`, `network`, `rate_limited`).
- `docker compose logs airq | jq 'select(.msg=="upstream attempt failed")'` shows
  the raw errors.
- Check the [Open-Meteo status](https://open-meteo.com/) and try
  `curl 'https://air-quality-api.open-meteo.com/v1/air-quality?latitude=52.52&longitude=13.41&current=pm10'`
  from the host.

**Mitigate:**
- **`rate_limited`:** we are over the vendor's fair-use limit. Lower
  `UPSTREAM_RATE_LIMIT_RPS` or raise cache TTLs.
- **Network errors:** check egress, DNS, proxies and firewalls.
- **Vendor outage:** wait. The breaker probes automatically. If the outage lasts and
  stale data is aging out, raise `CACHE_STALE_FOR` to keep serving older data. That is
  a product decision: answers get older.

### AirqUpstreamErrors

**Meaning:** More than 20% of logical vendor calls (after retries) fail, without
necessarily tripping the breaker. The vendor is flaky rather than down.

**Diagnose:**
- *Retries and attempts by outcome*.
- Recent failure rows:
  `SELECT outcome, status_code, error, count(*) FROM upstream_responses WHERE created_at > now() - interval '15 minutes' AND outcome <> 'success' GROUP BY 1,2,3;`
- `bad_response` means the vendor changed its response shape or size. Compare
  `response_body` against `internal/openmeteo` and check `UPSTREAM_MAX_RESPONSE_BYTES`.
- `client_error` should never happen: it means our validation let through something
  the vendor rejects. Treat it as a bug.

**Mitigate:** Same as [AirqUpstreamCircuitOpen](#airqupstreamcircuitopen). For a
schema change, ship an adapter fix.

### AirqServingStaleData

**Meaning:** Expired entries are being served because the vendor failed
(stale-if-error). This is informational: the system is degrading as designed.

**Action:**
- Find the vendor-side cause through the upstream alerts.
- Note that stale data stops once entries pass `CACHE_STALE_FOR` (1h). After that,
  uncached-equivalent locations start failing.

### AirqLoadShedding

**Meaning:** More than `HTTP_MAX_IN_FLIGHT` API requests were in flight, so new ones
got an immediate `503`.

**Diagnose:**
- *In-flight and rejected requests*.
- A traffic increase in *Requests by route*?
- Or slower requests holding slots longer (latency panels; the vendor slowing down
  raises concurrency at the same traffic level)?

**Mitigate:**
- **Real traffic:** scale out. Limits are per replica.
- **Slow vendor:** fix that first. Raising `HTTP_MAX_IN_FLIGHT` only raises memory use
  and queueing.
- **One abusive client:** per-client limits (`HTTP_RATE_LIMIT_*`) should already
  return `429`. Check that `HTTP_TRUST_PROXY_HEADERS` is set correctly behind a proxy,
  or every client looks like the proxy's IP.

### AirqPanics

**Meaning:** A handler panicked. The request got a `500` and the process survived.

**Diagnose:** `jq 'select(.msg=="panic recovered")'` on the logs. The line includes
the stack trace and the request ID.

**Mitigate:** It is a bug: reproduce it from the request in the log, then write a test
and fix it. Roll back if a deploy introduced it.

### AirqDatabaseDown

**Meaning:** The `database` health check (a ping with a 1s timeout) is failing.

**Impact:**
- The API is unaffected.
- Audit rows fail to insert and are counted (`AirqRecorderLosingRecords` follows).
- Retention pauses.

**Diagnose:**
- The admin listener's `/health` has the raw error, as does the `health check failing`
  log line.
- `docker compose ps postgres` and `docker compose logs postgres`.
- Disk full and authentication errors are the usual suspects (look for
  `No space left on device`).

**Mitigate:**
- Restore PostgreSQL. The pool reconnects automatically; nothing to restart.
- After a password rotation, update `DATABASE_URL` and restart the service.

### AirqMigrationsPending

**Meaning:** The embedded migrations include versions the database has not applied:
the release is ahead of the schema.

**Impact:**
- Usually none right away, since expand/contract keeps old schemas compatible.
- Features that rely on the new schema may fail.

**Diagnose:** `make migrate-status`, or `airq migrate status` with
`MIGRATE_DATABASE_URL` set.

**Mitigate:**
- Run the migration step: `docker compose run --rm migrate`, or the pre-deploy Job.
- If it fails, read its log. Each migration runs in a transaction, so a failure leaves
  the schema unchanged. Fix the migration in a *new* file, because applied files are
  immutable.

### AirqRecorderLosingRecords

**Meaning:** Audit rows are being `dropped` (queue full: the database is too slow or
down) or `failed` (inserts erroring).

**Impact:** Gaps in the audit trail. API responses are unaffected by design ([ADR 0005](adr/0005-best-effort-audit-recorder.md)).

**Diagnose:**
- *Recorder records* and *Recorder queue*: a queue pinned at capacity means the
  database cannot keep up.
- `jq 'select(.component == "recorder")'` on the logs (errors are rate-limited to
  avoid log floods).
- *Query latency p95 by operation* for `insert_upstream_responses`.

**Mitigate:**
- **Database down:** see [AirqDatabaseDown](#airqdatabasedown).
- **Slow inserts:**
  - check locks (`pg_stat_activity`) and disk;
  - raise `RECORDER_BATCH_SIZE` to cut round trips;
  - raise `RECORDER_BUFFER` to ride out longer blips, at a memory cost of up to
    `RECORDER_BUFFER × RECORDER_MAX_BODY_BYTES`.

### AirqRetentionStale

**Meaning:** No successful retention run in 3 hours, so the audit table keeps growing.

**Diagnose:**
- *Retention* panel and `airq_retention_runs_total{result}`:
  - `skipped` everywhere means another session holds the advisory lock (a stuck
    replica?);
  - `error` means the delete is failing (the logs have the cause, often
    `statement_timeout`).
- With `DB_RETENTION=0` the job is disabled and this alert should be silenced.

**Mitigate:**
- **Statement timeouts:** a 5,000-row batch (`store.RetentionOptions.BatchSize`) takes
  longer than `DB_STATEMENT_TIMEOUT` on this hardware. Raise the timeout, or run a
  manual batched delete off-peak.
- **A stuck lock holder:** find it with
  `SELECT pid, application_name FROM pg_locks JOIN pg_stat_activity USING (pid) WHERE locktype = 'advisory';`
  and restart that replica.

### AirqDBPoolSaturated

**Meaning:** More than 90% of `DB_MAX_OPEN_CONNS` connections are in use. Requests for
a connection wait (`go_sql_wait_count_total`).

**Diagnose:**
- *Connection pool* and *Pool waits*.
- Slow queries in *Query latency p95 by operation*.
- `pg_stat_activity` for long-running sessions.

**Mitigate:**
- Usually slow queries hold connections, so fix those first.
- Raise `DB_MAX_OPEN_CONNS` only while the total across replicas stays under the
  `airq_app` connection limit (50) and Postgres's `max_connections`.

## Metrics catalogue

All application metrics share the `airq_` prefix. Route labels use mux patterns,
never raw paths, so cardinality stays bounded.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `airq_http_requests_total` | counter | `method`, `route`, `code` | Requests served |
| `airq_http_request_duration_seconds` | histogram | `method`, `route`, `code` | Latency, with trace exemplars |
| `airq_http_requests_in_flight` | gauge | | Requests being served |
| `airq_http_rejected_requests_total` | counter | `reason` (`rate_limited`, `overloaded`) | Rejected before reaching a handler |
| `airq_http_panics_recovered_total` | counter | | Recovered handler panics |
| `airq_cache_lookups_total` | counter | `cache`, `result` (`hit`, `stale`, `miss`) | Cache lookups |
| `airq_cache_entries` | gauge | `cache` | Entries, including stale ones |
| `airq_cache_evictions_total` | counter | `cache`, `reason` (`capacity`, `expired`) | Evictions |
| `airq_cache_stale_served_total` | counter | `cache` | Stale entries served because the vendor failed |
| `airq_cache_coalesced_requests_total` | counter | `cache` | Requests that joined an in-flight fetch |
| `airq_upstream_calls_total` | counter | `upstream`, `outcome` | Logical vendor calls, after retries |
| `airq_upstream_attempts_total` | counter | `upstream`, `outcome` | Individual attempts, including ones rejected locally (`circuit_open`, `throttled`) |
| `airq_upstream_attempt_duration_seconds` | histogram | `upstream`, `outcome` | Latency of attempts that reached the network |
| `airq_upstream_retries_total` | counter | `upstream` | Retries performed |
| `airq_upstream_circuit_state` | gauge | `upstream` | 0 closed, 1 half-open, 2 open |
| `airq_upstream_circuit_transitions_total` | counter | `upstream`, `state` | Breaker transitions |
| `airq_upstream_in_flight_requests` | gauge | `upstream` | Bulkhead slots in use |
| `airq_upstream_rate_limiter_wait_seconds` | histogram | `upstream` | Time waiting for the outbound quota guard |
| `airq_upstream_connections_total` | counter | `upstream`, `reused` | Connection reuse (keep-alive health) |
| `airq_db_query_duration_seconds` | histogram | `operation`, `outcome` | Database operations |
| `airq_recorder_records_total` | counter | `result` (`written`, `dropped`, `failed`) | Audit exchanges handled |
| `airq_recorder_batch_size` | histogram | | Rows per `INSERT` |
| `airq_recorder_queue_length` / `_capacity` | gauge | | Queue depth and bound |
| `airq_retention_runs_total` | counter | `result` (`success`, `skipped`, `error`) | Retention runs |
| `airq_retention_deleted_rows_total` | counter | | Rows deleted |
| `airq_retention_last_success_timestamp_seconds` | gauge | | Last successful run |
| `airq_health_check_up` | gauge | `check`, `critical` | 1 passing, 0 failing |
| `airq_health_check_duration_seconds` | histogram | `check` | Check latency |
| `airq_build_info` | gauge | `version`, `revision`, `go_version` | Always 1; changes mark deploys |
| `go_sql_*` | various | `db_name` | `database/sql` pool stats (open, in use, idle, waits, closes) |
| `go_*`, `process_*` | various | | Go runtime and process metrics |
| `promhttp_metric_handler_*` | various | | Scrape handler self-metrics |

Recording rules (`deploy/prometheus/rules/airq.rules.yml`):
- `airq:api_requests:rate5m`
- `airq:api_request_errors:ratio_rate{5m,30m,1h,6h}`
- `airq:api_request_latency_under_1s:ratio_rate5m`
- `route:airq_http_request_duration_seconds:p99_5m`
- `cache:airq_cache_lookups:hit_ratio_5m`
- `upstream:airq_upstream_calls:error_ratio_5m`

## Health endpoints

| Endpoint | Use for | Fails when |
|---|---|---|
| `/livez` | Liveness probe, container `HEALTHCHECK` | The process cannot serve HTTP |
| `/readyz` | Readiness probe, load balancer | Draining for shutdown (no dependency is critical) |
| `/health` | Humans, dashboards, synthetic checks | Never fails for soft dependencies: reports `degraded` with `200` |

Results are cached for `HEALTH_CACHE_TTL` (2s) and refreshed every `HEALTH_INTERVAL`
(15s) in the background, so probes are cheap and `airq_health_check_up` is current
without polling.

## Deploying

1. **Migrate first.** Run `airq migrate up` with the migrator credentials (the compose
   `migrate` service; in Kubernetes a pre-deploy Job). The advisory lock makes
   concurrent runs safe. Migrations must be backward compatible with the release
   still running (expand/contract):
   - add a nullable column or a new table, deploy, backfill, then enforce constraints
     or drop old columns in a later release;
   - create indexes with `CREATE INDEX CONCURRENTLY` in a `-- +goose NO TRANSACTION`
     migration.
2. **Roll out the new image.** On SIGTERM each instance:
   1. fails `/readyz` (`shutdown started` log);
   2. keeps serving for `HTTP_DRAIN_DELAY` (5s) while the load balancer deregisters it;
   3. stops accepting connections and waits up to `HTTP_SHUTDOWN_TIMEOUT` (20s) for
      in-flight requests;
   4. drains the audit queue and flushes trace spans;
   5. logs `shutdown complete` and exits 0.

   A second signal exits immediately. The orchestrator grace period must exceed 25s
   (compose: `stop_grace_period: 30s`).
3. **Verify** with `airq_build_info` (new revision), the error and latency SLIs,
   `/health`, and, for a release, `make e2e` against staging.

**Rollback:** redeploy the previous image. Because of expand/contract it runs against
the migrated schema. Roll back a migration (`airq migrate down 1`) only when its Down
section is known to be safe. The audit table's Down drops data.

**Configuration changes** need a restart. Invalid configuration stops the new
instance at startup with every problem listed, and the old one keeps serving.

## Capacity and tuning

| Symptom | Knob | Trade-off |
|---|---|---|
| Low cache hit ratio | `CACHE_*_TTL`, `CACHE_MAX_ENTRIES` | Staler data / more memory |
| Vendor quota pressure (`rate_limited`, quota waits) | `UPSTREAM_RATE_LIMIT_RPS` (within vendor terms), TTLs | — |
| Breaker flapping | `UPSTREAM_BREAKER_THRESHOLD`, `UPSTREAM_BREAKER_COOLDOWN` | Slower detection / slower recovery |
| Shedding under normal load | Scale out; `HTTP_MAX_IN_FLIGHT` | Memory and tail latency |
| Audit drops during DB blips | `RECORDER_BUFFER` | Memory: up to `RECORDER_BUFFER × RECORDER_MAX_BODY_BYTES` |
| Audit table growth | `DB_RETENTION` | History kept |
| GC pressure near the memory limit | `GOMEMLIMIT` (about 80% of the container limit) | CPU spent on GC |

Each replica holds its own caches, client limits and vendor quota guard. With N
replicas the vendor sees up to N × `UPSTREAM_RATE_LIMIT_RPS`, so size the setting for
the replica count.

## Chaos drills

Run against the mock stack (`make up-mock`) with Grafana and Prometheus → Alerts open.

| Drill | Command | Expected |
|---|---|---|
| Vendor down | `make chaos-vendor-down` | Cached locations: `200`; others: `502` until the breaker opens, then a fast `503` with `Retry-After`; `/health` degraded; `/readyz` 200; `AirqUpstreamCircuitOpen` fires after 1m |
| Vendor slow | `make chaos-vendor-slow` | Attempts time out at 3s; `504`s or stale responses; latency SLO alert |
| Vendor flaky | `make chaos-vendor-flaky` | Retries absorb most failures; `airq_upstream_retries_total` rises; the SLI barely moves |
| Database down | `make chaos-db-down` | API unaffected; `/health` degraded; `airq_recorder_records_total{result="failed"}` rises; recovers by itself on `make chaos-reset` |
| Graceful shutdown | `docker compose stop airq` while running load | No failed requests; logs show `shutdown started` → `shutdown complete`; exit code 0 |

`scripts/smoke.sh` automates the vendor-down, breaker and shutdown checks in CI.

## Data

- **Audit log:** `upstream_responses`, pruned after `DB_RETENTION` (30 days). It holds
  vendor responses only: no client data beyond the request ID and the query
  coordinates or place name.
- **Backups:** the compose volume `pgdata` is for local use. In production, use the
  platform's managed backups with point-in-time recovery. The audit log can be rebuilt
  from nothing, but losing it loses the history.
- **Prometheus** keeps 7 days locally. Grafana state is provisioned from the repository.

## Known limitations and follow-ups

- **Per-replica caches and limits.** A shared cache (Redis) and a global vendor quota
  become worthwhile at high replica counts ([ADR 0003](adr/0003-caching.md)).
- **No authentication.** Put an API gateway or auth proxy in front before exposing the
  service publicly ([Security](SECURITY.md)).
- **Plain inserts and deletes for the audit log.** Switch to `COPY` and time
  partitioning when write volume grows.
- **Alerting is not routed.** Alertmanager routing (pager, chat) depends on the target
  environment and is not bootstrapped.
