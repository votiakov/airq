# Prompt chain

This project was bootstrapped by an AI coding agent (Claude Code) working from the
prompts below. They are recorded verbatim so reviewers can see exactly what was asked
and how the agent interpreted it. The commit history (`git log --oneline`) shows how
the work was split up.

## 1. Initial goal

> bootstrap an app in Go, ideally using standard lib, fewest dependencies - the app needs
> to ingegrate with 3d party API provider https://openweathermap.org/api Air quality api.
> Everything should be committed. Commit often. Use conventional commits. App must be
> dockerised from the start. Must be able to run with a single command locally. Readme
> needs to reflect that. Database is postgres. All changes to db must be in migration
> files. The app must have best practices in observability, reliability, and operational
> excellence.
>
> The service should:
> 1. Accepts requests for sending onto the vendor API
> 2. Caches responses appropriately
> 3. Implements comprehensive observability
> 4. Handles failures gracefully
> 5. Bonus: log responses to a database table using mysql or postgres, and instrument the
>    connection
>
> We will read your project and attempt to run it, checking endpoints like /metrics.
> This is intentionally open ended. We're looking to see what sort of patterns you've seen
> for making a robust, observable, API in production, with as much safety and guards as
> you can think of. We want to see your chain of prompts and outcome.
>
> API Endpoints — Your service should expose endpoints. Examples might be:
> `GET /weather/{location}`, `GET /music/{genre}`
>
> Then you will need the usuals:
> `GET /health` — Returns service health status;
> `GET /metrics` — Exposes Prometheus-compatible metrics
>
> Each feature must have tests and metrics. It must use GitHub actions for CI/CD - unit
> tests, linters, compilers, docker image building and scanning, security scan etc.
>
> Bootstrap everything that does not need my input at this stage. Document all decisions
> and tradeoffs, document architecture and operational excellence and security posture.

## 2. Correction (sent while the agent was researching vendor APIs)

> Correction - use Open-Meteo API as it does not need an api key

## 3. Correction: don't reinvent the wheel

> Correction: "few dependencies and use go standard lib" does not mean reinventing the
> wheel for everything... Use best practice libraries where makes sense. Give me the
> full list of everything that was reinvented and what was used from existing lib -
> each which explanation why this decision was made.

Outcome: the circuit breaker, backoff/retry loop, channel semaphores, LRU, env loader,
migration runner, W3C trace-context code and ETag matcher were replaced. Their
replacements are gobreaker, backoff/v5, x/sync/semaphore, golang-lru, caarlos0/env,
goose, OpenTelemetry and `http.ServeContent`, each in its own `refactor:` or `feat:`
commit. The full built-vs-borrowed inventory, with the reason for every entry, is
[ADR 0001](adr/0001-dependency-policy.md).

## 4. Acceptance plan

> run parallel agent (sonnet) to create a full testing/acceptance plan for the build. do
> not run it yet - plan it and show it to me for approval

Outcome: a Sonnet subagent drafted the plan (read-only, in parallel with the refactor):
about 200 checks across build, tests, the single-command run, API, caching, resilience,
health, shutdown, database, observability, security, CI, docs and load. It was
presented for approval and has not been executed as a whole. Gaps it found were fixed
directly because they were defects, not tests:
- the recorder could pin about 1 GiB of vendor bodies while the database was stalled;
- client-supplied secrets in query strings reached the access log;
- the service had no least-privilege database role;
- ADR 0005 was referenced but missing.

## 5. Automated goal check

The session ran with the initial goal as a completion condition. When the agent
stopped to ask for plan approval, the check replied that the goal was not yet met:
- no CI/CD workflows;
- no README, architecture, operations or security docs;
- no Makefile;
- no verified end-to-end run of the compose stack.

Outcome:
- **Disk.** The Docker disk was full, which blocked Postgres and had also crashed a
  container from another project. The agent freed only the space its own session had
  used: its test container, its volumes and the 1.8 GB Grafana image. Everything else
  was left alone.
- **Smoke test.** The agent wrote `scripts/smoke.sh` and ran it against the real stack
  with the mock vendor. All 30 checks passed after fixing two script bugs: a counter
  evaluated only once, and Jaeger v2's `/api/v3` path.
- **Vulnerability found and fixed.** The run showed the public `/health` returning raw
  driver errors: database user, database name and internal DNS. It was fixed in
  `fix(health)`.
- **Database-down drill** run by hand: the API kept serving, `/health` reported
  `degraded`, and it recovered without a restart.
- **Added:** the GitHub Actions pipeline (CI, CodeQL, PR checks, Dependabot), the
  Makefile, `.env.example`, the README, the architecture, operations and security
  docs, and ADRs 0002–0008.

## 6. Independent review of the vendor integration

> Run an independent agent reviewing all the patterns you implement around the calls to
> the third party, response to the user. Create criteria and rate them /10

Outcome:
- **Reviewer.** A separate agent read the code and tests cold, before any docs, and
  wrote its own 14 criteria. It scored the integration 6.8/10 on average (7.0
  holistic), with no critical findings.
- **Proofs.** Its proofs ran as throwaway `go test -overlay` tests, outside the
  repository. The main agent re-ran them and they reproduced.
- **High findings:**
  - vendor 401/403/404/410 skip both stale fallback and the circuit breaker;
  - a panic inside a coalesced (singleflight) fetch crashes the process.
- **Medium findings:**
  - an unbounded `Retry-After`;
  - empty 200 payloads cached as good data;
  - `Age` double-counted against `max-age`;
  - quota fairness;
  - the proxy-trust default;
  - the breaker's consecutive-only counting.
- **Docs.** It also found the docs claiming "full jitter" where the code uses ±50%
  randomisation.

The report is kept verbatim in
[`docs/reviews/2026-10-26-vendor-integration-review.md`](reviews/2026-10-26-vendor-integration-review.md).
Fixes were held back for the user's go-ahead.

## How the agent worked through it

1. **Research before code.** Checked the toolchain (Go 1.22 locally, so the agent pulled
   Go 1.27.3 via `GOTOOLCHAIN`) and looked up current versions of dependencies, base
   images and GitHub Actions. Actions were resolved to commit SHAs so they could be pinned.
2. **Vendor switch.** Dropped OpenWeatherMap for Open-Meteo after the correction. Called the
   live Air Quality and Geocoding APIs with `curl` to capture the real response shapes,
   including error bodies and the empty geocoding result, before writing the client.
3. **Design.** Wrote down the decisions and tradeoffs as ADRs in [`docs/adr`](adr) before
   or alongside each feature.
4. **Build in vertical slices.** Each feature landed with its own tests and Prometheus
   metrics, in small conventional commits.
5. **Verify.**
   - Ran `go test -race` (including the PostgreSQL integration tests) and
     `golangci-lint` before each commit.
   - Ran promtool and actionlint on the ops config and workflows.
   - Ran `scripts/smoke.sh` against the real compose stack.
   - Every pinned action SHA was checked against its release tag through the GitHub
     API.
