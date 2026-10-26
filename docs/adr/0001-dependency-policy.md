# ADR 0001: Dependency policy, and what is built vs. borrowed

- Status: accepted (revised after review feedback, see "History")
- Date: 2026-10-26

## Context

The brief asked for "standard lib, fewest dependencies". The first iteration read that
too literally. It hand-rolled a circuit breaker, a retry/backoff loop, an LRU, an
environment-config loader, a migration runner and W3C trace-context handling. Review
feedback pointed out that "few dependencies" does not mean reinventing the wheel. Code
we write ourselves is code we have to test, secure and maintain, and widely used
libraries carry years of edge-case fixes that a fresh implementation does not.

## Decision

A dependency is adopted when **all** of these hold:

1. **It solves a general, well-understood problem** (retries, circuit breaking, LRU,
   migrations, tracing). We do not hand-write mechanisms whose subtle bugs (half-open
   races, jitter, lock handling) are already fixed upstream.
2. **It is the community default or close to it**: widely imported, actively
   maintained, permissively licensed, and has a small, stable API.
3. **Its transitive cost is proportionate.** Small libraries should have few or no
   dependencies. Heavy ones (OpenTelemetry) must buy a capability we cannot reasonably
   build.

Code stays in-house when:

- it is **domain policy** rather than mechanism (which failures are retryable, what may
  be served stale, how AQI is categorised);
- the stdlib already does the job well (`net/http` routing, `log/slog`,
  `encoding/json`, `database/sql`);
- the available library **does the wrong thing for us**: it leaks secrets, lacks
  semantics we need, or drags in a framework. Each such case is listed below with its
  reason.

The libraries supply mechanisms and our packages own the policy. For example,
`internal/upstream` decides that a 4xx is not the vendor's fault, while gobreaker
decides when the circuit opens.

## Inventory

### Previously hand-rolled, now replaced by a library

| Concern | Was | Now | Why |
|---|---|---|---|
| Circuit breaker | `internal/resilience/breaker.go` (closed/open/half-open, generations) | [`sony/gobreaker/v2`](https://github.com/sony/gobreaker) `TwoStepCircuitBreaker` | De facto Go breaker, zero dependencies. It handles generation tracking, the half-open probe limit and `IsExcluded`, so caller cancellations and our own 4xx do not trip it. It does not expose time-until-half-open, so we record the open transition in `OnStateChange` to compute `Retry-After`. |
| Retries + backoff | `internal/resilience/backoff.go`: full-jitter `Delay`, `Sleep`, a hand-written retry loop | [`cenkalti/backoff/v5`](https://github.com/cenkalti/backoff) `Retry` | Most widely used Go backoff library, zero dependencies, context-aware. It supports `Permanent` (non-retryable) and `RetryAfterError` (vendor hint). `WithMaxElapsedTime` set to the remaining request deadline gives the "never sleep past the caller's deadline" budget. |
| Bulkhead, load shedder | Buffered-channel semaphores | [`golang.org/x/sync/semaphore`](https://pkg.go.dev/golang.org/x/sync/semaphore) | Go-team maintained, context-aware `Acquire`, and `TryAcquire` for shedding. It expresses intent more clearly than channel tricks. |
| LRU cache | `container/list` + map implementation | [`hashicorp/golang-lru/v2`](https://github.com/hashicorp/golang-lru) `simplelru` | Standard Go LRU, generic, zero dependencies. The non-thread-safe `simplelru` is used under our own mutex because lazy expiry must make "get, find expired, remove" atomic; the thread-safe wrapper cannot do that without races. |
| Environment config | Typed getters (`str`, `duration`, `integer`, …) with defaults | [`caarlos0/env/v11`](https://github.com/caarlos0/env) struct tags | Declarative defaults next to fields, zero dependencies, aggregates all parse errors. We kept cross-field validation (no tag-based validator expresses "write timeout must exceed request timeout" clearly). We also attribute parse errors to the env var: the library reports Go field names, which repeat across sections. |
| Schema migrations | Hand-written runner: file parsing, checksums, advisory lock, per-migration transactions, no-tx directive | [`pressly/goose/v3`](https://github.com/pressly/goose) Provider API + `lock.NewPostgresSessionLocker` | Mature, library-first API. Takes `*sql.DB` and `embed.FS`; SQL in transactions by default; `-- +goose NO TRANSACTION` for `CREATE INDEX CONCURRENTLY`; Postgres session advisory locking; structured logging via slog. Chosen over golang-migrate, whose "dirty version" model needs manual `force` after a failed migration (goose relies on Postgres transactional DDL), and which wires drivers through blank imports. Goose has no checksums, so drift detection moved to a CI guard that fails if an existing migration file is modified, renamed or deleted. |
| Distributed tracing | `internal/telemetry/trace.go`: W3C `traceparent` parse/generate; IDs only, no spans or export | OpenTelemetry: [`otel`](https://github.com/open-telemetry/opentelemetry-go), `sdk`, `otlptracehttp`, [`otelhttp`](https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp), [`exaring/otelpgx`](https://github.com/exaring/otelpgx) | The industry standard. Real spans with export to any OTLP backend (Jaeger in compose), standard `OTEL_*` configuration, W3C propagation, and DB spans from the pgx tracer hook. This is the only heavy dependency (OTLP brings protobuf and gRPC types; the unstripped binary grew from about 22 MB to 34 MB). Distributed tracing cannot reasonably be built in-house. |
| Conditional GET | Hand-written `If-None-Match` matcher | `net/http.ServeContent` (stdlib) | Already in the stdlib: RFC 9110 weak ETag comparison, 304 handling, HEAD and Range. |

### Library or stdlib from the start

| Concern | Choice | Why |
|---|---|---|
| HTTP server and routing | `net/http` `ServeMux` (Go 1.22+ method and wildcard patterns) | Covers every routing need here (methods, `{location}` wildcard, `{$}`). chi/gin/echo would add a framework for no capability. |
| Structured logging | `log/slog` | Stdlib, structured, JSON, context-aware; zap/zerolog are faster but irrelevant at this scale. |
| JSON | `encoding/json` | Stdlib; payloads are small. |
| Metrics | [`prometheus/client_golang`](https://github.com/prometheus/client_golang) | Native Prometheus exposition, exemplars, OpenMetrics, standard Go/process collectors and `DBStatsCollector`. Preferred over OTel metrics because reviewers scrape `/metrics` directly and Prometheus is the target backend. |
| Postgres driver | [`jackc/pgx/v5`](https://github.com/jackc/pgx) via `database/sql` (`pgx/stdlib`) | The Go Postgres driver. Behind `database/sql` because goose and `collectors.NewDBStatsCollector` take `*sql.DB`. pgx is still used for config parsing, runtime params and the tracer hook. |
| Client-side rate limiting | [`golang.org/x/time/rate`](https://pkg.go.dev/golang.org/x/time/rate) | Go-team token bucket, used for the outbound vendor quota guard and for per-client inbound limits. |
| Request coalescing | [`golang.org/x/sync/singleflight`](https://pkg.go.dev/golang.org/x/sync/singleflight) | Go-team implementation; prevents cache stampedes. |
| Tests | stdlib `testing`, `httptest`, `testing/fstest`, `prometheus/testutil`, OTel `tracetest` | No assertion library needed; tests read as plain Go. |
| Build info | `runtime/debug.ReadBuildInfo` + ldflags | Stdlib; no extra tooling. |
| CLI | `os.Args` dispatch in `cmd/airq` | Four subcommands (`serve`, `migrate`, `healthcheck`, `version`) do not justify cobra or urfave/cli. |

### Still built in-house, and why

| Component | Where | Why not a library |
|---|---|---|
| Upstream failure taxonomy and policy (`Kind`, retryable vs unavailable vs excluded, `Retry-After` parsing) | `internal/upstream/errors.go`, `retryafter.go` | Domain policy: it drives retries, breaker accounting, stale fallback and the HTTP status we return. `Retry-After` parsing is about 15 lines on top of `http.ParseTime`; backoff/v5 consumes a duration but does not parse headers. |
| Upstream spans | `internal/upstream/client.go` | `otelhttp.NewTransport` records `url.full` with the query string, which would export the vendor API key, and it injects `traceparent` into requests to a third party. Hand-made spans give a logical-call span plus one client span per attempt, a redacted URL, and no propagation. |
| Freshness policy (TTL + stale-if-error window, lazy expiry) | `internal/cache` (around 100 lines) | Expiring caches (golang-lru `expirable`, ristretto, otter) support one TTL, not "fresh until T, still servable on error until T+N" per entry. `viccon/sturdyc` covers stale serving but brings its own refresh and batching model that would replace the service layer. |
| Health model (critical vs soft dependencies, `degraded`, draining, cached + background refresh) | `internal/health` | [`alexliesenfeld/health`](https://github.com/alexliesenfeld/health) was evaluated (v0.8.1). Its aggregate status is only `up`/`down`/`unknown`, with no notion of a soft dependency that degrades without failing readiness and no draining state, which is the core of our readiness semantics. |
| HTTP middleware: request ID, RED metrics labelled by route pattern, access log, panic recovery, security headers, problem+json for mux 404/405 | `internal/httpapi/middleware.go` | `promhttp.InstrumentHandler*` needs a per-route wrapper to get a route label. One outer middleware using `ServeMux.Handler(r)` gives bounded-cardinality labels for every route, including unmatched ones. `httpsnoop` (already a transitive dependency via otelhttp) is unnecessary: the recorder implements `Unwrap`, so `http.ResponseController` reaches `Flush`/`Hijack`. Each piece is short and specific to this service's contract. |
| Per-client inbound rate limiter | `internal/httpapi/middleware.go` | About 30 lines: `x/time/rate` buckets in our bounded LRU. `sethvargo/go-limiter` and `ulule/limiter` add stores and abstractions we do not need for in-process limiting. |
| RFC 9457 problem details | `internal/httpapi/problem.go` | A struct and a writer; libraries in this space are thin and less maintained than the code they would replace. |
| Input validation | `internal/httpapi/handlers.go` | Four query parameters with specific messages; `go-playground/validator` (reflection, tag DSL) is heavier than the rules. |
| Async audit recorder (bounded queue, batched multi-row INSERT, per-row retry on data errors) | `internal/store/recorder.go` | Domain-specific delivery semantics: best-effort and never blocking a request. At higher volume the swap is pgx `CopyFrom` or a queue, not a library. |
| Retention job (batched deletes under an advisory lock) | `internal/store/retention.go` | A few SQL statements. The scale path is time partitioning with `pg_partman` (drop partitions instead of deleting rows), documented as a follow-up. |
| Vendor emulator with fault injection | `internal/mockvendor`, `cmd/mockvendor` | Runs in-process in tests (fast, deterministic, no containers) and as a compose service. WireMock, Toxiproxy or mockserver could replace the compose role but not the in-process test fixture. |
| Container health probe | `airq healthcheck` subcommand | Distroless images have no shell or curl; the binary probes itself. |
| slog trace correlation | `internal/telemetry/logging.go` (`contextHandler`, about 15 lines) | The `otelslog` bridge sends records to an OTLP log pipeline. We want JSON on stdout (collected by the platform) carrying `trace_id`/`span_id`, which is a few lines on `slog.Handler`. |

## Consequences

- About 1,000 lines of hand-written mechanism (breaker, backoff, LRU, config loader,
  migration runner, trace context) were deleted, along with their tests. The libraries'
  own test suites now cover that ground, and our tests focus on our policy.
- The direct dependency list grew from 4 to 15 modules, most of them OpenTelemetry. All
  are permissively licensed (MIT, BSD, Apache-2.0, MPL-2.0 for golang-lru).
  `govulncheck`, Dependabot and dependency review in CI keep them patched.
- Goose's `go.mod` lists many database drivers, but Go module graph pruning means only
  the packages we import are built: goose added three small indirect modules to our
  `go.mod`.
- Swapping the migration runner reset the version table (`goose_db_version`). Nothing
  was deployed, so no data migration was needed.

## History

- v1: minimal-dependency interpretation, with hand-rolled mechanisms.
- v2 (this revision): libraries adopted per the policy above, following review feedback
  (see [`docs/PROMPTS.md`](../PROMPTS.md), prompt 3).
