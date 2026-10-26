# airq

An air quality API in Go that fronts [Open-Meteo](https://open-meteo.com/en/docs/air-quality-api).
It resolves place names, forwards requests to the vendor and caches the answers. It
keeps serving when the vendor or the database fails, and records every vendor exchange
in PostgreSQL. Metrics, traces, logs, SLO alerts and a dashboard come wired up.

## Run it

Requires Docker with Compose v2.

```sh
docker compose up --build
```

That one command builds the image, starts PostgreSQL, applies the migrations and starts
the service with its observability stack. The first run downloads about 2.5 GB of
images.

| What | URL |
|---|---|
| API | http://localhost:8080 |
| Prometheus metrics | http://localhost:8080/metrics |
| Health report | http://localhost:8080/health |
| Grafana ("airq overview" dashboard, no login) | http://localhost:3000 |
| Prometheus (SLO rules and alerts) | http://localhost:9090 |
| Jaeger (traces) | http://localhost:16686 |

Try it:

```sh
curl -i localhost:8080/v1/air-quality/london          # by place name
curl -i localhost:8080/v1/air-quality/london          # again: X-Cache: HIT
curl -i 'localhost:8080/v1/air-quality/current?lat=52.52&lon=13.41'
curl -i 'localhost:8080/v1/air-quality/forecast?location=Paris&country=FR&hours=48'
curl -s localhost:8080/health | jq
curl -s localhost:8080/metrics | grep ^airq_
```

If a port is taken, override it, for example `AIRQ_PORT=18080 GRAFANA_PORT=13000 docker compose up --build`.
All overridable settings are in [`.env.example`](.env.example).

**Offline, or to break things on purpose:** run against the bundled Open-Meteo emulator,
which supports fault injection.

```sh
docker compose -f compose.yaml -f compose.mock.yaml up --build   # or: make up-mock
```

Stop with `Ctrl-C`, then `docker compose down`. Add `-v` to delete the data volumes.

## API

All API routes are `GET` (and `HEAD`) and return JSON. Errors use
[RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) `application/problem+json` with a
stable `code` and the `request_id`.

| Endpoint | Description |
|---|---|
| `GET /v1/air-quality/{location}` | Current conditions for a place name, for example `/v1/air-quality/london`. Optional `?country=GB` disambiguates. |
| `GET /v1/air-quality/current?lat=&lon=` | Current conditions by coordinates. `?location=&country=` also works. |
| `GET /v1/air-quality/forecast?lat=&lon=&hours=` | Hourly forecast, `hours` 1–96 (default 24). `?location=` also works. |
| `GET /health` | Per-dependency report: `ok`, `degraded` (a soft dependency is failing) or `down`. |
| `GET /livez` | Liveness: the process can serve HTTP. Checks no dependencies. |
| `GET /readyz` | Readiness: `503` only while draining for shutdown. |
| `GET /metrics` | Prometheus/OpenMetrics exposition, with trace exemplars. |

The response includes pollutant concentrations, the European AQI (`good` … `extremely_poor`)
and US AQI (`good` … `hazardous`) with their categories, the resolved location and an attribution notice.

Caching is visible in every response:
- `X-Cache: HIT|MISS|STALE`, an RFC 9211 `Cache-Status` and `Age`.
- `Cache-Control: max-age` set to the remaining freshness.
- A strong `ETag`, so `If-None-Match` returns `304`.

Status codes:

| Status | When |
|---|---|
| `400` | Invalid input, with per-field errors. |
| `404` | Unknown place. |
| `429` | Per-client rate limit hit. Includes `Retry-After`. |
| `503` | Overloaded, or the vendor is down and nothing is cached. Includes `Retry-After`. |
| `502` / `504` | The vendor failed, or timed out. |

## How it behaves when things break

| Failure | What clients see | What operators see |
|---|---|---|
| Vendor errors or is slow | Retries with jittered backoff inside the request deadline, then the last good answer for up to an hour (`X-Cache: STALE`) | `airq_upstream_*`, `AirqUpstreamErrors`, `AirqServingStaleData` |
| Vendor keeps failing | Circuit opens; uncached locations get a fast `503` with `Retry-After` | `airq_upstream_circuit_state` = 2, `AirqUpstreamCircuitOpen`, `/health` degraded |
| PostgreSQL down | Nothing: the API keeps serving; audit rows are dropped and counted | `AirqDatabaseDown`, `AirqRecorderLosingRecords`, `/health` degraded |
| Traffic spike | Per-client `429`; past `HTTP_MAX_IN_FLIGHT`, immediate `503` instead of queueing | `airq_http_rejected_requests_total`, `AirqLoadShedding` |
| SIGTERM (deploy) | In-flight requests finish; readiness fails first so the load balancer drains | `shutdown started` / `shutdown complete` logs |

The design behind each of these is in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

### Chaos drills

With the mock stack running (`make up-mock`):

```sh
make chaos-vendor-down    # every vendor call returns 503
make chaos-vendor-slow    # 4s vendor latency, beyond the 3s attempt timeout
make chaos-vendor-flaky   # 30% of vendor calls fail
make chaos-db-down        # stop PostgreSQL
make chaos-reset          # back to normal
```

Watch the Grafana dashboard, `/health` and the Prometheus alerts page while they run.

## Configuration

Environment variables only, following the twelve-factor model. Invalid values stop the
service at startup with every problem listed at once. The effective configuration is
logged at startup, with secrets redacted.

| Variable | Default | Purpose |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Listen address |
| `HTTP_REQUEST_TIMEOUT` | `10s` | Deadline for each API request, including retries |
| `HTTP_READ_HEADER_TIMEOUT` / `HTTP_READ_TIMEOUT` / `HTTP_WRITE_TIMEOUT` / `HTTP_IDLE_TIMEOUT` | `5s` / `10s` / `30s` / `120s` | Server timeouts (slow-client protection) |
| `HTTP_MAX_HEADER_BYTES` | `16384` | Request header size limit |
| `HTTP_MAX_IN_FLIGHT` | `256` | Concurrent API requests before shedding with `503` |
| `HTTP_RATE_LIMIT_RPS` / `HTTP_RATE_LIMIT_BURST` | `10` / `20` | Per-client-IP token bucket |
| `HTTP_TRUST_PROXY_HEADERS` | `false` | Trust `X-Forwarded-For` and incoming `traceparent` (only behind your own proxy) |
| `HTTP_DRAIN_DELAY` / `HTTP_SHUTDOWN_TIMEOUT` | `5s` / `20s` | Graceful shutdown: not-ready period, then the in-flight deadline |
| `UPSTREAM_AIR_QUALITY_URL` / `UPSTREAM_GEOCODING_URL` | Open-Meteo | Vendor base URLs |
| `UPSTREAM_API_KEY` | — | Only for Open-Meteo commercial plans. Never logged, traced or stored. |
| `UPSTREAM_ATTEMPT_TIMEOUT` / `UPSTREAM_MAX_ATTEMPTS` | `3s` / `3` | Per-attempt deadline and attempt cap |
| `UPSTREAM_RETRY_BASE_DELAY` / `UPSTREAM_RETRY_MAX_DELAY` | `100ms` / `1s` | Exponential backoff with jitter |
| `UPSTREAM_RATE_LIMIT_RPS` / `UPSTREAM_RATE_LIMIT_BURST` | `5` / `10` | Outbound quota guard |
| `UPSTREAM_MAX_CONCURRENT` | `16` | Bulkhead: concurrent vendor calls per upstream |
| `UPSTREAM_BREAKER_THRESHOLD` / `UPSTREAM_BREAKER_COOLDOWN` | `5` / `30s` | Consecutive failures to open the circuit, and the open period |
| `UPSTREAM_MAX_RESPONSE_BYTES` | `1048576` | Vendor response size cap |
| `CACHE_MAX_ENTRIES` | `5000` | Per-cache LRU bound |
| `CACHE_CURRENT_TTL` / `CACHE_FORECAST_TTL` / `CACHE_GEOCODE_TTL` / `CACHE_NOT_FOUND_TTL` | `10m` / `30m` / `24h` / `10m` | Freshness per data type |
| `CACHE_STALE_FOR` | `1h` | How long expired entries may be served while the vendor fails |
| `DATABASE_URL` | — | PostgreSQL DSN for the service role. Empty disables the audit log. |
| `MIGRATE_DATABASE_URL` | `DATABASE_URL` | DSN for `airq migrate` (the DDL role) |
| `DB_MAX_OPEN_CONNS` / `DB_MAX_IDLE_CONNS` | `10` / `5` | Pool size |
| `DB_CONN_MAX_LIFETIME` / `DB_CONN_MAX_IDLE_TIME` | `30m` / `5m` | Connection recycling |
| `DB_STATEMENT_TIMEOUT` | `5s` | Server-side `statement_timeout` and `lock_timeout` |
| `DB_MIGRATE_ON_START` | `false` | Apply migrations at startup (for single-instance setups) |
| `RECORDER_BUFFER` / `RECORDER_BATCH_SIZE` / `RECORDER_FLUSH_INTERVAL` / `RECORDER_MAX_BODY_BYTES` | `1024` / `100` / `1s` / `65536` | Async audit-log writer |
| `DB_RETENTION` / `DB_RETENTION_INTERVAL` | `720h` / `1h` | Delete audit rows older than this (`0` disables) |
| `HEALTH_CACHE_TTL` / `HEALTH_INTERVAL` | `2s` / `15s` | Health probe caching and background refresh |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / `json` | `debug`…`error`; `json` or `text` |
| `ADMIN_ADDR` / `PPROF_ENABLED` | — / `false` | Private listener with the detailed health report (raw errors) and optional pprof |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Enables OTLP/HTTP trace export. Standard `OTEL_*` variables apply. |

The binary also has operational subcommands: `airq migrate up|down [N]|status`,
`airq healthcheck [URL]` (used by the container `HEALTHCHECK`) and `airq version`.

## Development

Requires Go 1.27 and Docker. Run `make` for the full list.

```sh
make test               # unit tests with the race detector
make test-integration   # all tests, including PostgreSQL, on a throwaway database
make lint               # golangci-lint, same version as CI
make rules-test         # promtool: Prometheus config and alert unit tests
make e2e                # mock stack + scripts/smoke.sh + teardown
```

Layout:

```
cmd/airq          service entry point and subcommands (serve, migrate, healthcheck, version)
cmd/mockvendor    Open-Meteo emulator with fault injection
internal/
  airquality      domain model, AQI categories, caching service (stale-if-error, coalescing)
  openmeteo       vendor adapter (air quality + geocoding)
  upstream        resilient HTTP client: timeouts, retries, circuit breaker, bulkhead, quota
  cache           bounded LRU with TTL and a stale window
  httpapi         handlers, middleware chain, problem+json, RED metrics
  health          liveness, readiness, health report
  store           PostgreSQL pool, migrations, async audit recorder, retention
  telemetry       slog setup, Prometheus registry, OpenTelemetry tracing
  config          environment configuration and validation
migrations/       SQL migrations (embedded in the binary)
deploy/           Postgres roles, Prometheus rules and tests, Grafana provisioning
scripts/smoke.sh  end-to-end smoke test against the compose stack
```

## CI/CD

GitHub Actions ([`.github/workflows`](.github/workflows)), with every action pinned to a
commit SHA:

- **Lint:** golangci-lint (gosec, errorlint, noctx, sloglint, promlinter and others), `go mod tidy`, shellcheck, actionlint.
- **Test:** the race detector on shuffled tests against a real PostgreSQL service, plus a check that the integration tests did not skip.
- **Build:** cross-compiles for Linux, macOS and Windows on amd64 and arm64 (Windows on amd64 only).
- **Security:**
  - govulncheck;
  - Trivy scans of the repository and the image (CVEs, secrets, misconfiguration), sent to code scanning, failing the build on fixable HIGH/CRITICAL findings;
  - CodeQL for Go and for the workflows;
  - dependency review with a licence allow-list on PRs.
- **Ops config:** `promtool check config` and alert unit tests, compose validation, and a check that every alert's runbook link resolves.
- **Migrations:** a guard that applied migrations are never edited.
- **E2E:** builds the image, starts the compose stack with the mock vendor and runs [`scripts/smoke.sh`](scripts/smoke.sh): API, caching, DB roles, circuit breaker, Prometheus, Jaeger and graceful shutdown.
- **Publish** (main and `v*` tags): multi-arch image to GHCR with SBOM and SLSA provenance, signed keylessly with cosign.
- **PR hygiene:** Conventional Commit titles; Dependabot for Go modules, base images and actions.

## Documentation

- [Architecture](docs/ARCHITECTURE.md): components, request flow, caching and resilience design.
- [Operations](docs/OPERATIONS.md): SLOs, alerts and runbooks, metrics catalogue, deploys, migrations.
- [Security](docs/SECURITY.md): threat model, controls, supply chain, residual risks.
- [Decision records](docs/adr): each significant decision with its tradeoffs. [ADR 0001](docs/adr/0001-dependency-policy.md) lists what was built in-house and which libraries were used, with the reason for each.
- [Prompt chain](docs/PROMPTS.md): the prompts this project was built from, and what came out of each.

Air quality data: [Open-Meteo](https://open-meteo.com/) (CC BY 4.0), based on CAMS ENSEMBLE data provided by ECMWF.
