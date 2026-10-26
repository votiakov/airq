> Independent review produced by a separate agent that read the code cold (docs only afterwards). Proofs ran as throwaway tests injected with `go test -overlay`; they are not part of the repository. Findings are recorded here as found, before any fixes. See [PROMPTS.md](../PROMPTS.md), prompt 6.

# airq: vendor-call patterns and client-facing responses, independent review

Reviewer method: code and tests read first; `docs/`, `README.md` and deploy rules read only afterwards (section 5).
Repo untouched. Proofs were run as throwaway tests injected with `go test -overlay` (files in this directory:
`up_zz_test.go`, `aq_zz_test.go`, `om_zz_test.go`, `overlay.json`). Existing suites for upstream, airquality,
httpapi, cache, openmeteo and mockvendor pass under `-race`. `go vet` is clean.

## 1. Criteria and scores

| # | Criterion (what 10/10 looks like) | Score | Verdict |
|---|---|---|---|
| 1 | Timeouts and deadline propagation (every hop bounded, budgets nest, retries never outlive the caller) | 8 | Per-attempt, per-flight and per-request deadlines nest correctly; config cross-checks are partial |
| 2 | Retry policy and idempotency (only safe, plausibly-transient failures retried, vendor hints honoured but bounded) | 7 | Sound core; Retry-After is unbounded and can overflow; a few status codes misclassified |
| 3 | Circuit breaking (trips on real vendor-health signals, correct half-open, no false trips or blind spots) | 7 | Half-open and exclusions are right; blind to vendor-side 4xx, consecutive-only, counts attempts |
| 4 | Concurrency isolation and backpressure (bulkheads, coalescing, shedding, no way for one caller or one panic to hurt all) | 7 | Good layering; a panic in a coalesced fetch kills the process; no per-client fairness upstream |
| 5 | Rate and quota management toward the vendor (adaptive, fair, abuse-resistant) | 6 | Static 5 rps token bucket; one client can drain it; no feedback from 429s |
| 6 | Response validation and defensive parsing (reject semantically empty or implausible payloads) | 5 | Size cap, decode and ragged-column checks exist; empty or zero-valued payloads are accepted and cached |
| 7 | Cache correctness, freshness, stampede protection | 7 | Singleflight, bounded LRU, stale window all good; no revalidate-in-flight, no SWR, unfair eviction |
| 8 | Graceful degradation (serve last good data for every class of vendor fault) | 6 | Excellent for 5xx/timeouts/429; zero stale fallback for vendor 4xx (401/403/404/410) |
| 9 | Error taxonomy and HTTP status mapping | 7 | Clear taxonomy; vendor Retry-After forwarded verbatim; breaker can mask the root cause |
| 10 | Client-facing error format and headers (RFC 9457, Retry-After, validators, cache headers) | 7 | RFC 9457, ETag/304/HEAD correct; Age plus remaining-TTL max-age double-counts |
| 11 | Input validation and data protection (nothing unescaped to vendor; secrets absent from logs, spans, errors, clients) | 8 | Strong; minor residuals |
| 12 | Observability of vendor interactions | 8 | Metrics, spans, exemplars, audit log, alerts; missing logical-call latency and vendor-status labels |
| 13 | Testability and coverage of failure modes | 6 | Many good tests; mock vendor cannot emit malformed, slow-body, reset or Retry-After faults; key blind spots untested |
| 14 | Configuration safety | 6 | Thorough validation, but unsafe defaults behind a proxy and missing cross-field checks |

**Overall (unweighted mean): 6.8 / 10** (sum 95 / 14).
**Holistic: 7.0 / 10.** The resilience skeleton is real and mostly correctly built, but the claim of production-grade
robustness is overstated while a vendor panic can crash the whole process, vendor-side 4xx bypass both the breaker and
stale fallback, and semantically empty vendor payloads are cached as good data.

---

## 2. Per-criterion detail

### 1. Timeouts and deadline propagation: 8
Done well:
- Per-attempt `context.WithTimeout` wraps the body read too, so slow-drip bodies are bounded (`internal/upstream/client.go:426`, body read at `:448` under `actx`). `ResponseHeaderTimeout`, dial and TLS timeouts are set (`:199-213`).
- Retry loop is capped to `time.Until(deadline) - 100ms` (`:304-310`), so no backoff sleep starts that cannot finish. Shown by the existing `TestRetryStopsWhenDeadlineBudgetIsExhausted`.
- The coalesced fetch is detached from the first caller but has its own `FetchTimeout` (`internal/airquality/service.go:175`). Each caller still races its own `ctx.Done()` (`:196`), so followers keep their own deadline and fall back to stale on expiry.
- Request deadline attached before the handler (`internal/httpapi/middleware.go:336`). `WriteTimeout > RequestTimeout` is enforced (`internal/config/config.go:183`).

Gaps:
- `FetchTimeout` is wired to `HTTP_REQUEST_TIMEOUT` (`cmd/airq/main.go:213`). Config only requires `RequestTimeout >= AttemptTimeout` (`config.go:207`). With `UPSTREAM_ATTEMPT_TIMEOUT=8s` and the default 10s, retries are silently disabled (retry budget exhausted after the first attempt). No warning. Suspected, reasoned from code.
- Limiter and bulkhead waits consume the same flight budget, so a saturated limiter turns into "throttled" rather than a clean queue timeout. Acceptable but not documented.
- Vendor `Retry-After` can exceed the whole budget; handled by giving up (good), but see finding M1.

To reach 10: enforce `FetchTimeout >= MaxAttempts*AttemptTimeout + backoff` or log the effective attempt count at startup; expose the effective budget as a gauge.

### 2. Retry policy and idempotency: 7
Done well:
- Only GET, so idempotent by construction. Retryable set is timeout, network, 429, 5xx (`errors.go:67-75`). Permanent errors short-circuit (`client.go:272`).
- Backoff is capped and jittered (`:281-287`), honours Retry-After, notifies a metric.
- Local throttling and breaker rejections are non-retryable (right: retrying them is pointless).

Gaps:
- Retry-After is not bounded and the parser overflows. `parseRetryAfter("9223372037")` returns `-2562047h47m16s` (confirmed; `internal/upstream/retryafter.go:21`, `time.Duration(secs) * time.Second` wraps). A negative value falls through to default backoff. A huge-but-valid one is forwarded to clients (see M1).
- 408 and 425 are treated as non-retryable client errors (`client.go:470`; confirmed in test: hits=1). 501 and 505 are retried three times (confirmed hits=3). Both are minor.
- Each retry takes a limiter token and a bulkhead slot (`client.go:320`), so a failing vendor multiplies outbound call volume 3x against a 5 rps quota. This is correct for quota safety but means retries and fresh traffic compete.
- Docs say "full jitter"; the code uses cenkalti's `RandomizationFactor` 0.5 (±50%), not full jitter (`client.go:281-286`). See section 5.

To reach 10: clamp parsed Retry-After to a configured max (for example 60s) and guard overflow; treat 408/425 as retryable and 501/505 as permanent; use full jitter or correct the docs.

### 3. Circuit breaking: 7
Done well:
- Two-step breaker with `MaxRequests: 1` half-open probe (`client.go:166-168`). Verified in gobreaker source (`gobreaker.go:237-250`) that excluded outcomes release the half-open slot via `validRequests()` (`counter.go:36`), so a cancelled probe does not wedge the breaker.
- Local rejections and cancellations are excluded (`:184-192`, `done(errNotSent)` at `:321`).
- Separate breakers for air-quality and geocoding (`openmeteo.go:40`), and a vendor outage does not fail readiness (soft check, `main.go:201-207`).
- Retry-After on open circuit is computed from the transition time (`:417-420`).

Gaps:
- Vendor 4xx (401/403/404/410, bad or revoked API key, retired endpoint) are `KindClient`, excluded from the breaker (`client.go:191`), never retried and not stale-eligible. The breaker never opens, every miss re-hits the vendor and returns 502 (confirmed statuses, section H1).
- It counts attempts, not logical calls, and trips only on consecutive failures (`:170`). Confirmed: threshold 2 with a 500-ing vendor gives hits=2 and the caller receives `circuit_open`, not `server_error`, so the original cause is masked in that response and the metrics `calls_total{outcome}`. A 50 percent flaky vendor with interleaved successes never opens it (reasoned).
- `openedAt` uses `time.Now()` while everything else uses the injectable clock (`:174`), so the Retry-After estimate cannot be tested deterministically. Minor.

To reach 10: add a rate-based trip (failure ratio over a window with a minimum request count), classify auth/quota/gone 4xx as vendor-health failures, and surface the root cause when the breaker opens mid-call.

### 4. Concurrency isolation and backpressure: 7
Done well:
- Bulkhead per upstream (`client.go:400`), global in-flight load shedder with immediate 503 and `Retry-After: 1` (`middleware.go:254-265`), singleflight coalescing keyed by cache and key (`service.go:171`), operational endpoints exempt from shedding (`router.go:63-66`).
- Follower and leader cancellation semantics are right and tested (`TestCallerCancellationDoesNotPoisonSharedFetch`).

Gaps:
- A panic in the coalesced fetch bypasses `recoverer`. `singleflight.DoChan` re-panics in a new goroutine. Confirmed: a nil-map write inside `Provider.Current` killed the test process with `panic: assignment to entry in nil map` raised from `singleflight.go:167` (finding H2). Every handler panic is otherwise survivable (`middleware.go:208-227`), which makes this asymmetric and easy to miss.
- No fairness among clients for the shared outbound budget (see criterion 5).
- A caller that did `c.Get` (miss) just before the leader's `c.Set` and then calls `DoChan` after the flight is forgotten starts a second vendor call; there is no cache re-check inside the flight (`service.go:166-181`). Narrow window, suspected.
- Load shedder holds a semaphore slot for slow clients up to `RequestTimeout` (10s); with 256 slots, a modest slow-loris-style set of cache-miss requests during a vendor brownout can shed legitimate traffic. Reasoned.

To reach 10: recover inside the flight and convert to an error; re-check the cache inside the flight; per-client outbound fairness.

### 5. Rate and quota management toward the vendor: 6
Done well:
- Token bucket (5 rps, burst 10) per upstream before the bulkhead (`config.go:85-86`, `client.go:391`), `Wait` fails fast when the reservation would outlive the deadline, and the wait is observed as a metric (`:398`).
- Caching, negative caching and singleflight collapse most demand.

Gaps:
- Static budget with no feedback: a vendor 429 or `Retry-After` does not lower the outbound rate or pause the limiter; only the breaker reacts after five consecutive failures (`client.go:464-466`, no limiter interaction).
- Client limit (10 rps per IP, `config.go:55`) is double the vendor budget (5 rps). One client requesting unique coordinates (about 1.1 km grid, so effectively unbounded key space) at 10 rps can consume the entire outbound budget; other users' cache misses then get `KindThrottled` and a 503 (reasoned from `limiter.Wait`, `client.go:391-397`). Cache hits are unaffected.
- LRU of 5,000 entries shared across clients lets the same flood evict useful entries (`cache.go:110`, `config.go:95`). Suspected, not load-tested.
- Retry-After from the vendor is never fed back to the limiter.

To reach 10: weight or cap outbound misses per client, adaptive limiter reacting to 429 and Retry-After, and an admission policy that prefers refreshing existing keys over creating new ones.

### 6. Response validation and defensive parsing: 5
Done well:
- 1 MiB cap with `LimitReader(max+1)` (`client.go:448-455`), redirects disabled, non-200 statuses classified, trailing garbage rejected by `json.Unmarshal` (confirmed: `{"a":1} {"b":2}` gives `bad_response`), nullable pollutants handled (`*float64`), ragged hourly columns rejected (`openmeteo.go` `readings`).

Gaps (all confirmed with `om_zz_test.go`):
- `{"current":{}}` is accepted: `time` becomes `1970-01-01T00:00:00Z`, coordinates `0,0`, all pollutants null. It is cached as fresh for the full TTL (10m) and served as 200 (`openmeteo.go` `Current`; `service.go:181`).
- `{"hourly":{"time":[]}}` is accepted as a valid empty forecast (0 hours), cached 30m.
- Geocoding result missing `latitude`/`longitude` decodes to 0,0 (non-pointer floats) and then drives an air-quality query for Null Island (`geoResponse`, `openmeteo.go`). Result coordinates are never range-checked.
- No Content-Type check, no data-recency check (a `current.time` days old is served as "current"), no plausibility bounds (negative concentrations, AQI far out of range).
- A vendor 200 with `{}` for an unknown place could be negatively cached as "not found" for 10m (`service.go:142-146`). Suspected.

To reach 10: required-field validation (time present and within a sane window of now, coordinates in range, non-empty hourly with expected length), reject-and-do-not-cache on violation (already classified as bad_response/stale-eligible via `contractError`).

### 7. Cache correctness, freshness, stampede protection: 7
Done well:
- One mutex covers look-up-and-purge (`cache.go:78-102`); TTL plus stale window; LRU bound; key normalisation with -0 folded (`model.go:24-40`); forecast cached once at 96h and sliced into a copy (`service.go:119-133`); negative caching for geocode; singleflight with detached context.
- The cache stores values only after a successful load (`service.go:181`), so errors are never cached.

Gaps:
- No stale-while-revalidate: every request arriving after expiry for a hot key blocks on the vendor for up to the full fetch (up to about 10s with retries) even though a usable value exists (`service.go:166-171`).
- No re-check inside the flight (see criterion 4); TTL is fixed and ignores vendor data time (`config.go:96-99`).
- `Units` map in cached `Current`/`Forecast` is shared by reference with every response (read-only today; a latent aliasing hazard).
- Key space is user-controlled (see criterion 5).
- Longitude `-180` and `180` normalise to different keys for the same meridian. Cosmetic.

To reach 10: SWR with background refresh, in-flight cache re-check, expiry derived from vendor data time or interval, copy-on-read for maps.

### 8. Graceful degradation: 6
Done well:
- Stale-if-error with an hour window for timeouts, network, 429, 5xx, bad responses, circuit open, local throttling and caller-deadline expiry (`errors.go:55-64`, `service.go:200-206, 212-218`). Marked `X-Cache: STALE`, `Cache-Control: no-cache`, `Cache-Status: ...detail=stale-if-error` (`handlers.go:194-197`). Readiness is not coupled to vendor health. Tested end to end (`TestServeSurvivesVendorOutage`).

Gaps:
- Vendor 4xx classes that really mean "vendor cannot serve us" (401 and 403 for a revoked or over-quota key, 404 and 410 for a moved endpoint, 408, 425) are `KindClient`, so `Unavailable()` is false (confirmed for 401, 403, 404, 408, 410, 425). Cached data that could be served is replaced by a 502 and the error ratio alerts (finding H1). The service tests assert only the case "bad request must surface" (`airquality_test.go:157-161`), which is correct for 400 and wrong for the rest.
- Stale responses carry no signalling in the body, and no `Warning`/age cap beyond the one-hour window. Acceptable, but clients that ignore headers cannot tell.
- During an outage every stale-served request logs a WARN line (`service.go:203`), which becomes a log flood at traffic rates.
- Per-key only: no partial-degradation path (for example serving `current` when geocoding is down for an unseen name is impossible by nature).

To reach 10: classify by vendor-health semantics, not by "4xx"; SWR; rate-limit the stale WARN log and rely on the counter.

### 9. Error taxonomy and HTTP status mapping: 7
Done well:
- Kinds map to stable codes: not found 404, circuit open or throttled or vendor 429 503 with Retry-After, timeout 504, vendor 5xx/network/bad response 502, vendor 4xx 502 `upstream_rejected` plus an ERROR log, client gone 499, unknown 500 with no detail leaked (`handlers.go:131-169`; `TestErrorMapping`).
- Vendor error details are logged, never returned.

Gaps:
- Vendor-supplied Retry-After is forwarded unclamped to API clients: confirmed that the upstream error carries 24h for `Retry-After: 86400` (`client.go:465-466`), and `handlers.go:153-154` prints it as seconds. A vendor (or an attacker on the path with a plain-HTTP base URL) controls how long clients back off.
- The breaker-open-mid-call case returns `circuit_open` (503) where the underlying fault was 5xx (502), so the status for the call that tripped it differs from the earlier ones. Minor.
- 429 from the vendor is mapped to 503 with Retry-After defaulting to 1s when the vendor sent none (`handlers.go:153`), inviting a retry storm from clients. A floor of 1s is low for a vendor quota event.
- `upstream_rejected` conflates "our bug" (400) with "vendor says we are not allowed" (401/403), with the same advice in the log (`handlers.go:160-163`).

To reach 10: clamp Retry-After; separate auth/quota from validation failures; preserve root cause when a breaker trips.

### 10. Client-facing error format and headers: 7
Done well:
- RFC 9457 with `request_id`, `instance`, field errors and a stable `code` (`problem.go`); mux 404/405 rewritten to the same shape (`middleware.go:175-205`); `Cache-Control: no-store` on errors; `X-Request-ID`; strong ETag from body hash and `http.ServeContent` for If-None-Match, HEAD and Range (`handlers.go:185-208`); `Cache-Status` per RFC 9211; security headers.
- ETag is stable across hit, miss and stale because the body has no fetch timestamps, so revalidation works across cache states.

Gaps:
- `max-age` is set to the remaining TTL while `Age` is also sent (`handlers.go:192, 199, 202`). Per RFC 9111 a downstream cache computes freshness as `max-age - Age`, so it undercounts the remaining life (a 6-minute-old entry with 4 min left is treated as fresh for 0 min). Suspected, from RFC reading; effect is wasted revalidations.
- `public` on location-derived responses is fine, but no `Vary` is needed; no `stale-if-error`/`stale-while-revalidate` directives are emitted to downstream caches.
- 502/504 carry no Retry-After; optional.
- `If-None-Match` is evaluated only after the full lookup and possible vendor fetch; a 304 costs the same as a 200 on a miss. Fine.

To reach 10: send `max-age` equal to the full freshness lifetime or drop `Age`; add stale-if-error directives.

### 11. Input validation and data protection: 8
Done well:
- Coordinates parsed, range-checked, NaN/Inf rejected, length-capped (`handlers.go:254-269`); location name restricted to letters, marks, digits and a small punctuation set with a 2-100 rune limit (`:274-288`); country is `[A-Z]{2}`; `hours` bounded; the vendor query is built with `url.Values` and `Encode()` so no raw input reaches the URL (`client.go:262`, `openmeteo.go` `baseQuery`/`Geocode`). Client input cannot inject path or query because the path is a constant.
- API key never in logs, spans, errors, audit rows (`redact`, `redactedURL`, `redactedQuery`; tests `TestSpansNeverLeakTheAPIKeyOrPropagateToTheVendor`, `TestNetworkErrorsNeverLeakAPIKey`). Redirects disabled so the key cannot be sent elsewhere. Trace context deliberately not propagated to the vendor. Unknown query params redacted in the access log. Request IDs charset-checked.

Gaps:
- `strconv.ParseFloat` also accepts hex floats and underscores variants (for example `0x1p-2`); harmless because values are re-formatted before use (suspected, not run).
- Config allows `http://` base URLs with an API key (`config.go:295-300`), sending the key in clear text. Suspected misconfiguration risk only.
- Vendor `reason` is logged raw up to 200 chars and the recorder stores up to 64 KiB of raw vendor bodies; low risk, but unbounded provenance of strings in logs.
- Unicode normalisation (NFC vs NFD) is not applied to location names, so equivalent names create separate cache entries and vendor calls.

### 12. Observability of vendor interactions: 8
Done well:
- Calls, attempts, retries, attempt latency (with exemplars), limiter wait, in-flight, circuit state and transitions, connection reuse (`upstream/metrics.go`); cache lookups, evictions, stale served, coalesced requests; a span per logical call and per attempt; structured per-attempt logs; async audit recorder; alerts for circuit open, error ratio, stale serving, load shedding, panics (`deploy/prometheus/rules/airq.rules.yml:86-110`).

Gaps:
- No logical-call latency histogram (only per-attempt), so retry-inclusive latency, the thing users feel, is not measurable per upstream.
- No vendor status code label (429 vs 503 vs 401 are all folded into `kind`), and no metric for honoured Retry-After or stale age distribution.
- Stale WARN per request (see criterion 8).
- Alert on vendor `client_error` is not distinguished; the 401/403 case would show as generic upstream errors only if the ratio query includes that outcome (not verified; the rule file's recording rule was not traced).

### 13. Testability and coverage of failure modes: 6
Done well:
- Real httptest servers for the client, injectable clock in the service, fakes for the provider, fault injection in `mockvendor` (failure rate, status, latency), full-stack tests (`cmd/airq/main_test.go:124-215`). Tests exist for retries, Retry-After, deadline budget, attempt timeout, breaker open and half-open recovery, oversize, invalid JSON, redirect, key leakage, stale-if-error, coalescing, cancellation, 499, HEAD, rate limiting, shedding, panics.

Gaps (each is a defect I found that no test covers):
- Mock vendor can only return a chosen status and latency (`mockvendor.go:26-31, 85-100`). It cannot emit malformed or truncated bodies, slow-drip bodies, connection resets, HTML 200, empty-but-valid payloads, large or HTTP-date Retry-After, or 401/403 semantic faults (Retry-After is hard-wired to "1").
- No test for Retry-After overflow or an absurd value (`retryafter_test.go` stops at 30s).
- No test for panics inside the coalesced fetch, for 401/403/404 with a warm cache, or for semantically empty 200 payloads (`TestContractViolations` covers only absent blocks and ragged columns).
- No fuzz or property tests for parsing, no load/soak test for the quota and eviction interactions, no test for a retry racing the deadline at the margin.

### 14. Configuration safety: 6
Done well:
- Declarative parsing, all validation errors reported together, bounded ranges (`MaxAttempts` 1-10), positive durations, `WriteTimeout > RequestTimeout`, credentials redacted in the logged config (`LogValue`), admin listener separate from public, DB soft dependency.

Gaps:
- `HTTP_TRUST_PROXY_HEADERS` defaults to false (`config.go:57`), so behind a load balancer every user shares one `RemoteAddr` and one 10 rps bucket (`middleware.go:319-332`). Safe against spoofing, unsafe for availability. Docs mention the trade-off; the default will bite on first deploy. Also IPv6 clients are limited per address, not per /64.
- Missing cross-field checks: fetch timeout vs attempts x attempt timeout; breaker cooldown vs attempt timeout; Retry-After ceiling does not exist as a setting.
- Default base URLs are the free Open-Meteo hosts, while `UPSTREAM_API_KEY` is "only for commercial plans". I believe commercial access uses separate hosts, so setting only the key would not do what it implies (unverified, no network access used).
- `http://` base URL accepted with a key (criterion 11).

---

## 3. Findings ranked by severity

### Critical
None found. No finding allows remote code execution, data exposure or unauthenticated data corruption.

### High

**H1. Vendor 4xx (401, 403, 404, 408, 410, 425) bypass both the breaker and stale fallback.** Confirmed.
- `internal/upstream/client.go:470-471` (all 4xx become `KindClient`), `errors.go:59` (`Unavailable()` false), `client.go:191` (excluded from breaker), `service.go:200` (stale needs `canServeStale`).
- Proof: `up_zz_test.go` `TestZZStatuses`: each of those statuses yields kind `client_error`, `unavailable(stale ok)=false`, hits=1.
- Scenario: the API key is revoked or the plan quota is exhausted (403), or the vendor retires or moves the endpoint (404/410). Every request, including those with warm cached entries, gets 502 `upstream_rejected`. The breaker never opens, so each request still spends vendor quota. Stale data that would have kept clients working is discarded. The documented promise "vendor errors, then the last good answer for up to an hour" fails exactly when it is most needed.
- Fix: a new kind (for example `KindAuth` or `KindVendorRejected`) for 401/403/404/408/410/425 that counts against the breaker, is stale-eligible, and raises a distinct alert; keep 400/422 as `KindClient`.

**H2. A panic inside a coalesced fetch crashes the whole process.** Confirmed.
- `internal/airquality/service.go:171-182` (`DoChan` runs `load`, which calls the provider and adapter).
- Proof: `aq_zz_test.go` `TestZZPanicInFlight`: the test binary aborted with `panic: assignment to entry in nil map`, re-raised by `singleflight.go:167`, despite the caller's `recover`. `httpapi.recoverer` (`middleware.go:208`) cannot catch it either, because the panic runs in a goroutine spawned by singleflight.
- Scenario: any future bug or unexpected vendor shape that triggers a nil dereference or index panic in `openmeteo` or `upstream` takes down every in-flight request on the instance, and a poison request can repeat it after each restart.
- Fix: inside the `DoChan` closure, `defer func(){ if r := recover(); r != nil { err = fmt.Errorf("panic in fetch: %v", r) } }()` (named results), log the stack, and count it in `airq_http_panics_total`.

### Medium

**M1. Retry-After is unbounded, can overflow, and is forwarded to clients.** Confirmed.
- `internal/upstream/retryafter.go:21` (multiplication overflow), `client.go:465-469` (stored), `handlers.go:153-154` (emitted).
- Proof: `TestZZRetryAfterHuge`: `9223372037` parses to `-2562047h47m16s`; `TestZZRetryAfterPropagates`: a vendor `Retry-After: 86400` gives `RetryAfter=24h0m0s` after a single hit.
- Scenario: the vendor (or anything on a plain-HTTP path) sets a huge value. Clients are told to wait a day; an overflowing value silently falls back to default backoff.
- Fix: clamp in `parseRetryAfter` (for example max 120s), check `secs > maxSecs` before multiplying, and clamp again in the handler.

**M2. Semantically empty or zero-valued vendor payloads are accepted, cached and served as good data.** Confirmed.
- `internal/openmeteo/openmeteo.go` (`Current`: only checks `resp.Current != nil`; `Forecast`: only `Hourly != nil`; `Geocode`: no field checks), `service.go:181`.
- Proof: `om_zz_test.go` `TestZZDefensive`: `{"current":{}}` yields time 1970-01-01 and coords 0,0 with no error; `{"hourly":{"time":[]}}` yields 0 hours with no error; a geocode result with only a name yields coordinates 0,0.
- Scenario: a vendor partial outage returning `200` with skeleton JSON poisons the cache for 10 to 30 minutes with unusable data, and for geocoding drives queries at (0,0). Because it is cached fresh, it also suppresses stale-if-error for the whole TTL.
- Fix: validate required fields and ranges (time within a window of now, non-empty hourly with the expected count, coordinate ranges) and return `contractError` so the value is not cached and stale is served.

**M3. One client can exhaust the shared vendor quota and evict other users' cache entries.** Suspected (reasoned from config and code).
- `internal/config/config.go:55` (client 10 rps) vs `:85` (vendor 5 rps, burst 10); `client.go:391-397`; key space from `Normalize` at 2 decimals; `config.go:95` (5,000 entries).
- Scenario: a client iterates distinct coordinates at 10 rps; the vendor limiter saturates, other users' uncached requests fail with 503 `upstream_unavailable` (`KindThrottled`), and the LRU churns. Cache hits still work, so impact is on new locations.
- Fix: per-client cap on cache misses (or a lower per-client rate for cold keys), reserve part of the vendor budget for refreshing existing keys, and consider admission control for the cache.

**M4. Default proxy setting collapses per-client rate limiting behind a load balancer.** Confirmed by code, suspected in impact.
- `config.go:57`, `middleware.go:319-332`.
- Scenario: deployed behind an ingress with `HTTP_TRUST_PROXY_HEADERS=false`, all users share `RemoteAddr` and one 10 rps bucket; legitimate traffic gets 429 almost immediately. IPv6 clients can also rotate addresses within a /64 to bypass per-address limits.
- Fix: log a startup warning when neither a proxy setting nor a direct-exposure flag is chosen, key IPv6 by /64, and document the deployment matrix in the config.

**M5. Breaker counts attempts and trips only on consecutive failures; it masks the root cause on the call that trips it.** Confirmed (counting and masking), suspected (flaky never trips).
- `client.go:170`, `:314-318`, `:272`.
- Proof: `TestZZBreakerMidCall`: threshold 2 against a 500 vendor gives hits=2 and the caller sees `circuit_open`.
- Scenario: a vendor failing at 50 percent never opens the circuit while consuming retries and quota; conversely, a single logical call can trip the breaker, and that caller gets a 503 where earlier callers got 502.
- Fix: windowed failure-ratio trip, count per logical call, and return the last real error when the breaker opens mid-retry.

**M6. No in-flight cache re-check and no stale-while-revalidate.** Suspected.
- `service.go:166-181`.
- Scenario (a): a caller that missed just before another flight's `Set` starts a redundant vendor call. Scenario (b): for a popular key, all requests after expiry wait on a synchronous vendor fetch (up to about 10s with retries) although a usable entry exists.
- Fix: re-check `c.Get` as the first step in the flight closure; serve the stale entry immediately and refresh in the background when within a "soft" window.

**M7. `Age` plus remaining-TTL `max-age` double-counts.** Suspected (RFC 9111 reasoning).
- `handlers.go:192, 199, 202`.
- Scenario: downstream caches and browsers compute `max-age - Age`, shortening or nullifying reuse and increasing load on this service.
- Fix: emit `max-age` as the full lifetime or omit `Age`.

### Low

- **L1.** 408/425 not retried; 501/505 retried (`client.go:467-471`, confirmed in `TestZZStatuses`).
- **L2.** Docs claim "full jitter"; code uses ±50 percent randomisation (`client.go:281-286`). Docs also say the body "must be JSON" but there is no Content-Type check (`client.go:458`), only a decode.
- **L3.** Stale-served WARN log per request floods logs during an outage (`service.go:203`).
- **L4.** `mockvendor` cannot inject malformed bodies, slow bodies, resets, empty payloads or custom Retry-After (`mockvendor.go:26-31, 94-96`).
- **L5.** No config cross-check for `FetchTimeout` against attempts x attempt timeout (`config.go:207`, `main.go:213`); retries can be silently disabled.
- **L6.** Geocode takes only the top hit (`count=1`, `openmeteo.go` `Geocode`), so ambiguous names resolve silently; no Unicode normalisation of names; `lon=-180` and `180` have different keys.
- **L7.** `ParseFloat` accepts hex-float syntax (`handlers.go:261`); harmless after re-formatting. Suspected, not run.
- **L8.** `http://` base URL accepted with an API key (`config.go:295-300`).
- **L9.** Commercial API key with the default free hosts may not do what the setting implies. Unverified; needs vendor docs or a key.
- **L10.** `openedAt` uses wall clock instead of the injected `Now` (`client.go:174`).
- **L11.** Cached `Units` maps are shared by reference across responses (`openmeteo.go` `units`, `model.go:87, 95`); read-only today.

---

## 4. What is genuinely strong

- Clear layering with sensible defaults: breaker, then quota, then bulkhead, then attempt, with the right things excluded from the breaker (cancellation, local throttling).
- Deadline arithmetic: retries never sleep past the budget; follower and leader contexts are separated correctly.
- Leakage control is exemplary and tested: no key in logs, spans, errors or audit rows; no trace propagation to the vendor; redirects off.
- Error responses are consistent, machine-readable and do not leak internals; conditional requests and HEAD work.
- Observability and alerting are above average for a service this size.

---

## 5. Doc/code mismatches (read after forming the view)

| Doc claim | Code reality |
|---|---|
| `docs/ARCHITECTURE.md:112-113`: "exponential backoff with full jitter" | `client.go:281-286` uses cenkalti `ExponentialBackOff` with `RandomizationFactor` 0.5 (±50%), not full jitter |
| `docs/ARCHITECTURE.md:109`: body "must be JSON" | No Content-Type check; any 200 body that decodes is accepted (`client.go:458-463`) |
| Package doc `internal/upstream/client.go:11`: "strict JSON decoding" | `json.Unmarshal` without `DisallowUnknownFields` (`:459`); unknown fields tolerated (arguably right, but not "strict") |
| `docs/ARCHITECTURE.md:119-121` and `README.md:90`: if the vendor fails an expired entry is served | Not true for vendor 4xx including 401/403/404/410 (finding H1) |
| `README.md:90-91`, `docs/OPERATIONS.md:442`: vendor down gives cached 200s, others 502 until the breaker opens | Holds for 5xx; for vendor 4xx the breaker never opens (finding H1) |
| `docs/ARCHITECTURE.md:112`: "a 429's Retry-After is honoured" | Honoured, but unbounded and forwarded to clients; overflows for very large values (finding M1) |
| `docs/OPERATIONS.md:19`: 4xx excluded from the availability SLO | The vendor-4xx case is returned as 502 and so counts against availability (consistent with the SLO text "5xx", but contradicts the intent that vendor 4xx are "our request was invalid", `errors.go:18`) |

Docs otherwise matched the code on the points I checked (breaker Retry-After derivation, bulkhead and quota values, stale header set, ETag/304/HEAD via `ServeContent`, 429/503 semantics).

---

## 6. What I could not verify

- Real Open-Meteo behaviour: actual statuses and headers for quota and key failures, whether 429 includes `Retry-After`, the commercial host names, and whether an empty `{}` is ever returned for unknown places. No network calls to the vendor were made; I relied on the repository's fixtures.
- Behaviour under real load: quota starvation (M3), LRU churn, shedder interaction with slow clients (criterion 4) and the in-flight stampede window (M6) are reasoned, not measured.
- Downstream cache behaviour for the `Age`/`max-age` combination (M7) is based on the RFC, not tested against a specific CDN.
- The database recorder, retention, health checker internals and the end-to-end suite that needs PostgreSQL (`TestServeWithDatabase`) were not exercised; Docker was not used.
- Whether the Prometheus `upstream:airq_upstream_calls:error_ratio_5m` recording rule includes `client_error` outcomes (affects whether H1 would page).
- `go vet` was run only on upstream, airquality, httpapi and cache; the whole module was not linted.
