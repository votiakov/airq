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

Outcome: a Sonnet subagent drafted the plan (read-only, in parallel with the refactor)
for approval before anything was executed.

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
5. **Verify.** Ran `go test -race`, `golangci-lint` and a `docker compose` smoke test
   against the real stack before each infrastructure commit.
