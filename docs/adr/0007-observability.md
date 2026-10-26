# ADR 0007: Observability — Prometheus metrics, OpenTelemetry traces, structured logs, SLO alerts

- Status: accepted
- Date: 2026-10-26

## Context

Reviewers will scrape `/metrics`, and operators need to answer three questions: "is
it working?", "what is broken?" and "why was this request slow?" Each feature must
come with metrics. The stack has to run locally with one command and carry over to a
real platform without changes.

## Decision

**Metrics: Prometheus `client_golang`** on a private registry (never the global one,
so tests get isolated registries).

- **RED** for HTTP: rate, errors and duration, labelled by **route pattern**, method
  and code. Raw paths are never used as labels, so `/v1/air-quality/{location}` stays
  one series whatever clients send.
- **USE** for resources:
  - bulkhead in-flight;
  - quota-guard wait;
  - `database/sql` pool stats;
  - recorder queue;
  - Go runtime and process metrics.
- **Domain:**
  - cache hit, stale and miss counts, plus coalescing and evictions;
  - upstream calls, attempts and retries by outcome;
  - breaker state and transitions;
  - health gauges;
  - build info.
- Histograms carry **trace exemplars** (sampled spans only), served over OpenMetrics.
- Naming is checked by `promlinter`. Series that alerts depend on (such as the
  recorder results) are pre-initialised, so their rates exist before the first event.

**Traces: OpenTelemetry**, exported over OTLP/HTTP when `OTEL_EXPORTER_OTLP_ENDPOINT`
is set; spans are created but not exported otherwise.

- `otelhttp` creates a server span per request. Probes and `/metrics` are filtered
  out to keep traces about real traffic.
- Vendor calls get a hand-made logical span plus a client span per attempt ([ADR 0004](0004-upstream-resilience.md)).
- `otelpgx` adds DB spans, without connection details.
- Background batches (recorder, retention) start **root spans with links** to the
  requests they serve, rather than parenting under long-lived contexts.
- Propagation is W3C TraceContext only. Inbound trace context is trusted only behind
  our own proxy.

**Logs: `log/slog` JSON on stdout.**

- Every line in a request carries `request_id`, `trace_id` and `span_id`.
- There is one access log line per request, with known query parameters only.
- Probe successes are logged at debug level, to keep volume down.
- Errors keep the details that clients never see.

**SLOs and alerting** (Prometheus rules, provisioned in compose):

- 99.5% availability and 99% of requests under 1s.
- Recording rules feed multi-window, multi-burn-rate alerts (14.4× over 1h and 5m; 6×
  over 6h and 30m).
- Symptom alerts cover the vendor, the database, shedding, panics, retention and pool
  saturation.
- Every alert has a runbook URL into [Operations](../OPERATIONS.md), and CI checks that
  the anchors exist.
- `promtool test rules` unit-tests the alerts.

**Local backends:**
- Prometheus, with exemplar storage;
- Grafana, with a provisioned dashboard and exemplar links to Jaeger;
- Jaeger v2.

## Alternatives considered

| Option | Why not (now) |
|---|---|
| OpenTelemetry metrics SDK with a Prometheus exporter | One more translation layer; `client_golang` is the reference for Prometheus exposition, exemplars and the standard runtime collectors. Revisit if the platform standardises on OTLP metrics. |
| `otelslog` bridge (logs over OTLP) | We want logs on stdout for the platform to collect; correlation needs only a few lines on `slog.Handler` |
| Static threshold alerts on error rate | Page on brief blips, and miss slow burns; burn-rate alerts tie paging to user impact |
| Tempo or Zipkin instead of Jaeger | Any OTLP backend works unchanged; Jaeger v2 is a single small container with a UI |

## Consequences

- Every feature has metrics and tests. `scripts/smoke.sh` asserts the key series
  exist on a real stack.
- With OTLP, the binary grew from about 22 MB to 34 MB unstripped (about 30 MB
  stripped in the image).
- Cardinality is bounded by design:
  - route patterns;
  - fixed outcome and reason enums;
  - upstream names from configuration;
  - no user input in labels.
