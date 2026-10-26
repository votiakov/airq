# Security posture

What airq protects, against whom, how, and what is left to the platform it runs on.

## Scope and assumptions

airq is a read-only public data API:
- it holds no user accounts or personal data;
- its only secrets are database credentials and an optional vendor API key;
- its main risks are availability (abuse, resource exhaustion, a hostile or broken
  vendor), integrity of the audit log, and leaking internals or credentials.

It is designed to run behind a TLS-terminating ingress or load balancer, on a private
network shared with PostgreSQL and the telemetry backends. The local compose stack
reproduces that topology without TLS.

## Trust boundaries

```
 Internet ──TLS──▶ ingress/LB ──HTTP──▶ airq :8080 ──HTTPS──▶ Open-Meteo (third party)
                                         │  │
                    private network ◀────┘  └──▶ PostgreSQL, OTLP collector
                    (admin :ADMIN_ADDR, Prometheus scrape)
```

| Boundary | Untrusted input | Controls |
|---|---|---|
| Client → airq | Paths, query strings, headers, request rate, connection behaviour | Validation, limits, timeouts, rate limiting, load shedding (below) |
| Proxy headers | `X-Forwarded-For`, `traceparent`, `X-Request-ID` | Ignored unless `HTTP_TRUST_PROXY_HEADERS=true`. Request IDs are length- and charset-checked. |
| Vendor → airq | Response status, headers, body size and shape, latency, redirects | Size cap, JSON decoded into typed structs (anything else is a `bad_response`), per-attempt timeouts, redirects refused, breaker and bulkhead |
| airq → PostgreSQL | — | Parameterised SQL only, least-privilege role, statement and lock timeouts |

## Threats and controls

Each threat below is grouped by STRIDE category.

| Threat | Control | Where / verified by |
|---|---|---|
| **Spoofing.** A client forges `X-Forwarded-For` to dodge rate limits, or `traceparent` to join our traces | Proxy headers are trusted only when explicitly configured, using the right-most `X-Forwarded-For` entry. An untrusted `traceparent` becomes a span *link*, not the parent. | `TestClientIP`, `TestIncomingTraceContextIsTrustedOnlyBehindProxy` |
| **Tampering.** SQL injection | No string-built SQL from input. Every query uses bind parameters, including the batched multi-row insert. Validated input never reaches SQL except as values. | `internal/store`, gosec, CodeQL |
| **Tampering.** A compromised service alters the schema, rewrites audit history or migration state | `airq_app` has only `SELECT`/`INSERT`/`DELETE` on the audit table (`DELETE` for retention), `SELECT` elsewhere, no DDL, no `UPDATE`. DDL runs as `airq_migrator` in a separate step. | `deploy/postgres/initdb`, migration 000002, `scripts/smoke.sh` (asserts the denials against real Postgres) |
| **Repudiation.** Not knowing what the vendor returned for a given request | Every attempt is recorded with its request ID, status, outcome and body. Logs, traces and rows share the request ID. | `upstream_responses`, [Operations](OPERATIONS.md#where-to-look) |
| **Information disclosure.** Vendor API key leaks | The key is added to the outbound URL only at send time. It is stripped from span `url.full`, error messages, audit `query` and logs; redirects are refused, so it cannot be forwarded to another host; config logging shows only `upstream_api_key_set`. | `TestSpansNeverLeakTheAPIKey…`, `TestNetworkErrorsNeverLeakAPIKey`, recorder test |
| **Information disclosure.** Database credentials leak | DSNs are redacted in logs (`RedactDSN`). Credentials come from the environment, never from the image or repository. | `internal/config` tests |
| **Information disclosure.** Internals leak through errors | Clients get RFC 9457 problems with stable codes and generic messages. Vendor errors, stack traces and driver messages are logged only. Public `/health` reduces failures to `timeout`/`unavailable`; raw errors are on the private admin listener. | `TestErrorMapping`, `TestPublicReportRedactsErrorsAndLogsTransitions` |
| **Information disclosure.** Secrets a client puts in our URL end up in logs | The access log records values only for the five known query parameters; anything else is logged as `REDACTED`. | `TestAccessLogRedactsUnknownQueryParameters` |
| **Information disclosure.** Our traces leak to the vendor | No trace propagation headers are sent to the third party. | `TestSpansNeverLeakTheAPIKeyOrPropagateToTheVendor` |
| **Denial of service.** Request floods | Per-client token bucket (`429`), a global in-flight cap with immediate `503` (no unbounded queue), a request deadline | `TestRateLimitingPerClient`, `TestLoadShedding` |
| **Denial of service.** Slow or oversized clients (slowloris, header bombs) | `ReadHeaderTimeout` 5s, read/write/idle timeouts, a 16 KiB header cap. The API takes no request bodies. | `cmd/airq` server config |
| **Denial of service.** Memory exhaustion through high-cardinality input | Coordinates rounded; place names capped at 100 runes; every cache and the per-client limiter table bounded LRUs; metric labels limited to route patterns | `internal/cache`, `TestMetricsUseRoutePatternsNotPaths` |
| **Denial of service.** A hostile or broken vendor (huge body, slow drip, redirect) | 1 MiB body cap, per-attempt deadline, response-header timeout, bulkhead, circuit breaker, redirects refused | `internal/upstream` tests |
| **Denial of service.** Database slowness spreading into the API | Async bounded recorder; statement and lock timeouts; pool limits; role connection limit (50) | [ADR 0005](adr/0005-best-effort-audit-recorder.md) |
| **Elevation of privilege.** Container breakout or lateral movement | Distroless static image (no shell or package manager), non-root user, read-only root filesystem, all capabilities dropped, `no-new-privileges`, CPU, memory and pids limits | `Dockerfile`, `compose.yaml`, CI asserts the image user |

## Endpoints exposure

| Endpoint | Exposure | Notes |
|---|---|---|
| `/v1/*` | Public | Rate limited, shed under load |
| `/livez`, `/readyz` | Public port, meant for the orchestrator | No dependency details |
| `/health` | Public port | Status per check, version and uptime. Errors redacted. Restrict at the ingress if build metadata is considered sensitive. |
| `/metrics` | Public port, meant for Prometheus | Bounded label sets, no secrets. **Block it at the ingress in production**, or serve it on the admin listener. It shares the port for zero-config scraping. |
| Admin (`ADMIN_ADDR`) | Private only, disabled by default | Raw health errors; pprof only with `PPROF_ENABLED=true` |

## Transport

- TLS terminates at the ingress. The service does not set HSTS, because HSTS belongs
  on the TLS endpoint that owns the hostname. If the service ever terminates TLS
  itself, add HSTS there.
- Outbound calls to Open-Meteo use HTTPS with TLS 1.2 or later and system roots
  (distroless ships CA certificates).
- The local compose stack uses `sslmode=disable` on a private bridge network, where
  PostgreSQL is not published to the host. **Production DSNs must use
  `sslmode=verify-full`.**
- No CORS headers are sent: the API is not meant to be called from browsers on other
  origins. Add an explicit allow-list if that changes; never `*` with credentials.
- Response headers: `X-Content-Type-Options: nosniff`,
  `Content-Security-Policy: default-src 'none'; frame-ancestors 'none'`,
  `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`.

## Secrets

| Secret | Source | Handling |
|---|---|---|
| `DATABASE_URL` (service role) | Environment, from the orchestrator's secret store | Redacted in logs |
| `MIGRATE_DATABASE_URL` (DDL role) | Environment of the migration job only | Never given to the long-running service |
| `UPSTREAM_API_KEY` (optional) | Environment | Never logged, traced or stored (see above) |
| Local defaults (`postgres`, `migrator`, `app`, Grafana `admin`) | `compose.yaml` defaults, overridable through `.env` | For local use only. Postgres is not published; Grafana is anonymous read-only. |

`.env` files are git-ignored, and Trivy's secret scanner runs on every push.

## Supply chain

- **Dependencies:** a small, deliberate set ([ADR 0001](adr/0001-dependency-policy.md)),
  verified by `go mod verify` in the image build. Updates come through Dependabot,
  and PRs run dependency review with a licence allow-list.
- **Vulnerability scanning:**
  - `govulncheck` reports only vulnerabilities reachable from our code;
  - Trivy scans the repository (dependencies, secrets, Dockerfile and compose
    misconfiguration) and the built image;
  - CodeQL (security-extended) runs on the Go code and the workflows;
  - golangci-lint includes gosec.

  Builds fail on fixable HIGH/CRITICAL findings. Everything is reported to GitHub code
  scanning, and a weekly run catches CVEs in unchanged code.
- **Pinning:** base images and compose images are pinned by digest. GitHub Actions are
  pinned to commit SHAs, with tag comments for Dependabot. Workflows default to
  `contents: read`, and each job requests only the permissions it needs.
- **Build integrity:** reproducible flags (`-trimpath`, `CGO_ENABLED=0`, `GOTOOLCHAIN=local`).
  Published images carry an SBOM and SLSA provenance (`mode=max`), and are signed
  keylessly with cosign through GitHub OIDC. To verify:

  ```sh
  cosign verify ghcr.io/votiakov/airq@sha256:<digest> \
    --certificate-identity-regexp 'https://github.com/votiakov/airq/.github/workflows/ci.yml@.*' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com
  ```
- **Migrations:** applied migration files are immutable (CI guard), so the schema
  cannot silently diverge between environments.

## Residual risks and follow-ups

| Risk | Why it is accepted now | Follow-up |
|---|---|---|
| No authentication or per-tenant quotas | Public, read-only data; abuse is bounded by rate limits and shedding | API gateway with keys/OAuth and per-key quotas before any paid or private use |
| Per-replica rate limits | Simple and dependency-free | Gateway or shared limiter for global limits |
| `/metrics` and `/health` on the public port | Zero-config local scraping | Block at the ingress, or move them to the admin listener in production |
| Audit log holds vendor responses for 30 days | Needed for incident analysis; contains no client personal data | Revisit retention if queries ever carry user-identifying data |
| Best-effort audit log (drops under DB failure) | Availability over completeness for a read API | Durable queue if the audit log becomes a compliance record |
| Vendor data integrity (we trust Open-Meteo's values) | Reputable source; responses are schema-validated | Plausibility bounds on pollutant values if consumers depend on them |

## Reporting a vulnerability

Please report suspected vulnerabilities privately via GitHub Security Advisories
("Report a vulnerability" on the repository's Security tab) rather than in a public issue.
