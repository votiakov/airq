# ADR 0004: Layered resilience for vendor calls

- Status: accepted
- Date: 2026-10-26

## Context

The vendor is a third party outside our control. It can be slow, return errors or
garbage, rate-limit us, or disappear. Naive clients turn those failures into a worse
outage on our side in four ways:
- unbounded waits pile up goroutines and connections;
- retries multiply load on an already struggling vendor;
- a slow dependency eats all capacity;
- callers wait longer than they are willing to.

## Decision

`internal/upstream` is a vendor-agnostic client that applies, per logical call:

1. **A failure taxonomy** (`Kind`): `timeout`, `network`, `rate_limited`,
   `server_error`, `client_error`, `bad_response`, `circuit_open`, `throttled`,
   `canceled`. It drives:
   - retry decisions;
   - breaker accounting;
   - stale-if-error eligibility (`Unavailable()`);
   - the HTTP status returned to clients;
   - metric labels.
2. **Deadline-budgeted retries** (`cenkalti/backoff/v5`): up to `UPSTREAM_MAX_ATTEMPTS`
   attempts, using exponential backoff randomised ±50% (100ms base, 1s cap).
   - Only `timeout`, `network`, `rate_limited` and `server_error` are retried.
   - A vendor `Retry-After` is honoured.
   - The total elapsed time is capped at the caller's remaining deadline, so a retry
     never sleeps past it.
3. **A per-attempt timeout** (`UPSTREAM_ATTEMPT_TIMEOUT`, 3s), plus transport-level
   dial, TLS and response-header timeouts. One slow attempt cannot use the whole
   request budget, so a retry still has time to succeed.
4. **A circuit breaker** (`sony/gobreaker`):
   - It opens after `UPSTREAM_BREAKER_THRESHOLD` (5) consecutive failures and stays
     open for `UPSTREAM_BREAKER_COOLDOWN` (30s).
   - It then admits a single half-open probe.
   - Client errors, caller cancellations and locally rejected attempts are excluded,
     so neither a bad request nor an impatient client can trip it.
   - While open, callers get `circuit_open` with `Retry-After` set to the remaining
     cooldown, which we pass on to clients as a `503`.
5. **A bulkhead** (`x/sync/semaphore`, `UPSTREAM_MAX_CONCURRENT` per upstream): caps
   concurrent vendor calls, so a slow vendor cannot absorb every goroutine and socket.
6. **An outbound quota guard** (`x/time/rate`): a token bucket in front of the vendor
   that waits within the deadline, or fails as `throttled`. It protects the vendor's
   fair-use limits from our own bursts.
7. **Response hygiene:**
   - a 1 MiB body cap;
   - JSON decoded into typed structs;
   - redirects refused, because a redirect could carry the API key to another host;
   - TLS 1.2 minimum;
   - connection pooling, with reuse measured.
8. **Privacy:**
   - the API key is stripped from spans, errors and the audit log;
   - no trace context is propagated to the third party.

Every layer is observable through `airq_upstream_*` metrics, a span per attempt and an
audit row per attempt.

## Alternatives considered

| Option | Why not |
|---|---|
| Hand-written breaker and backoff | Version 1 of this project did that. Replaced per [ADR 0001](0001-dependency-policy.md): mature libraries already handle the edge cases (half-open races, jitter). |
| A failure-rate breaker over a sliding window | Noisy at low traffic, where five consecutive failures is a clearer signal. `gobreaker`'s `ReadyToTrip` makes switching a one-line change if traffic grows. |
| `failsafe-go` (all policies in one library) | Capable, but one framework owning all policies obscures the order of layers we want explicit. The individual libraries are smaller and better known. |
| `hystrix-go` | Unmaintained |
| A service mesh (Envoy retries and outlier detection) | Not available in the local stack, and it does not know our domain rules (what can be served stale, which 4xx is our bug). Complementary in production, if a mesh is present. |
| `otelhttp.NewTransport` for vendor spans | Records the full URL including the API key, and injects `traceparent` into third-party requests |

## Consequences

- Worst-case latency per request is bounded by `HTTP_REQUEST_TIMEOUT`, whatever the
  vendor does.
- During a vendor outage the breaker sheds load in microseconds instead of holding
  connections for seconds, and stale-if-error keeps popular locations served.
- A flaky vendor is mostly absorbed by retries. `airq_upstream_retries_total` makes the
  hidden cost visible.
- Limits are per replica. Scaling out raises total vendor load, so per-replica
  settings must account for the replica count ([Operations](../OPERATIONS.md#capacity-and-tuning)).
