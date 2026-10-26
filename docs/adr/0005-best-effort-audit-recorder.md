# ADR 0005: Asynchronous, best-effort audit logging to PostgreSQL

- Status: accepted
- Date: 2026-10-26

## Context

The brief asks, as a bonus, to log vendor responses to a database table and to
instrument the connection. The audit trail answers a specific question: "what did the
vendor return for request X?"

The audit trail is not the product. Clients come for air quality data, which is
available from cache even when the vendor is down. If writing the audit row sits on
the request path, every database slowdown becomes API latency, and every database
outage becomes an API outage, for a read-only API that does not need the database to
answer.

## Decision

- **Never block a request on the audit log.** `Recorder.Record` puts the exchange on a
  bounded channel (`RECORDER_BUFFER`). When the channel is full, the exchange is
  dropped and counted (`airq_recorder_records_total{result="dropped"}`).
- **Bound memory, not just count.** A response body is copied into the queue only if
  it is under `RECORDER_MAX_BODY_BYTES`. Larger ones are recorded without the body,
  with `body_truncated = true`. Worst-case queue memory is therefore
  `RECORDER_BUFFER × RECORDER_MAX_BODY_BYTES`, even with the database stalled.
- **Batch writes.** A single writer goroutine flushes up to `RECORDER_BATCH_SIZE` rows
  per multi-row `INSERT`, or every `RECORDER_FLUSH_INTERVAL`, with bind parameters
  only. One statement per batch keeps round trips and WAL overhead low.
- **Isolate bad rows.** If a batch fails for a data reason, it is retried row by row,
  so one bad row (say, an unexpected body) cannot lose its neighbours. Failures are
  counted as `failed`, and error logs are rate-limited.
- **Bound every write** with the statement timeout. On shutdown, drain the queue
  within a fixed time, after the HTTP server has stopped.
- **Make it observable:**
  - written, dropped and failed counters;
  - queue length and capacity;
  - batch size and insert latency;
  - a root span per batch, linked to the requests whose exchanges it carries;
  - the `AirqRecorderLosingRecords` alert.
- **Keep data for `DB_RETENTION`** (30 days), deleted in batches by the retention job
  under an advisory lock.

## Alternatives considered

| Option | Why not |
|---|---|
| Synchronous `INSERT` in the request | Couples API availability and latency to PostgreSQL for an audit-only concern |
| Transactional outbox | Needs a transaction to attach to; we have no business write to make atomic |
| Durable queue (Kafka, NATS JetStream, SQS) between the service and the DB | Exactly-once or at-least-once audit at the cost of another critical system. Justified only if the audit log becomes a compliance record. |
| Ship structured logs to a log store instead | Viable, and the access and attempt logs already carry the same IDs. The brief asked for a database table, and SQL makes incident queries (failures per upstream, the bodies behind one request) straightforward. |
| `COPY` instead of multi-row `INSERT` | Faster at high volume, but loses per-row fallback and is harder to instrument. It is the next step if write volume grows. |

## Consequences

- The API's availability does not depend on PostgreSQL (verified by the
  `chaos-db-down` drill: requests keep succeeding and `/health` reports `degraded`).
- Audit rows can be lost:
  - when the queue overflows during a long database outage;
  - on a hard crash (SIGKILL, OOM) with rows still queued.

  Loss is always counted and alerted on, never silent.
- Rows land up to `RECORDER_FLUSH_INTERVAL` after the response, which is fine for
  forensic use.
