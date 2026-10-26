# Re-score of the vendor integration after fixes (commit 24ba6ec)

> Produced by a second, independent review agent (Haiku), read-only, from reading the code and the gobreaker source. No new proofs were written. Transcribed from the agent's message, which was truncated at the end; scores are integers as in the [original review](2026-10-26-vendor-integration-review.md).

**Mean: 7.6 / 10 (107 / 14), up from 6.8.**

| # | Criterion | Was | Now | Note |
|---|---|---|---|---|
| 1 | Timeouts and deadlines | 8 | 8 | Still missing: FetchTimeout vs attempts x attempt timeout, cooldown vs attempt timeout (L5) |
| 2 | Retry policy | 7 | 8 | 408/425 retried, refusals not, Retry-After capped; 501/505/511 still retried |
| 3 | Circuit breaking | 7 | 8 | One outcome per call, ratio trip; ratio denominator counts excluded calls (new, M-1) |
| 4 | Concurrency isolation | 7 | 8 | Panic recovered; stale-while-revalidate refreshes bypass the miss budget (M-1) |
| 5 | Quota toward vendor | 6 | 7 | Per-client miss budget, IPv6 /64; global limiter still static, many IPs can still saturate it |
| 6 | Response validation | 5 | 7 | No upper bound on timestamps; all-null readings accepted |
| 7 | Cache and stampede | 7 | 8 | SWR and in-flight re-check |
| 8 | Graceful degradation | 6 | 8 | Refusals now stale-eligible; WARN log per stale request (L3) |
| 9 | Error taxonomy | 7 | 8 | Distinct `rejected` kind, 502 `upstream_rejected` |
| 10 | Client-facing headers | 7 | 8 | `max-age` fixed; client limiter's Retry-After is uncapped |
| 11 | Input validation and data protection | 8 | 8 | Unchanged |
| 12 | Observability | 8 | 8 | No vendor status label on call metrics |
| 13 | Testability | 6 | 7 | About 14 new tests; mock vendor still cannot inject malformed/slow/reset/Retry-After faults (L4) |
| 14 | Configuration safety | 6 | 6 | `HTTP_TRUST_PROXY_HEADERS` still defaults to false (warning only); http base URL accepted with a key (L8) |

## Original findings
- **Fixed:** H1, H2, M1, M5, M6, M7.
- **Partly fixed:** M2 (lower bound only), M3 (per-client only; SWR bypasses it), M4 (IPv6 and warning added, default unchanged).
- **Not fixed (low severity):** L1 (501/505 retried), L3, L4, L5, L8, L10 (`openedAt` uses wall clock).

The agent's list of further new findings was cut off in transit; the ones above are all that was received.
