#!/usr/bin/env bash
# End-to-end smoke test against a running stack that uses the mock vendor:
#
#   docker compose -f compose.yaml -f compose.mock.yaml up -d --build --wait
#   scripts/smoke.sh
#
# Run by CI (job "e2e") and by `make smoke`. Needs curl and jq. Exits non-zero on the
# first failed check. SMOKE_STOP=1 also checks that airq stops cleanly on SIGTERM
# (this leaves the airq container stopped).
set -euo pipefail

AIRQ=${AIRQ_URL:-http://localhost:${AIRQ_PORT:-8080}}
MOCK=${MOCK_URL:-http://localhost:${MOCK_PORT:-8090}}
PROM=${PROMETHEUS_URL:-http://localhost:${PROMETHEUS_PORT:-9090}}
JAEGER=${JAEGER_URL:-http://localhost:${JAEGER_PORT:-16686}}
read -r -a COMPOSE <<<"${COMPOSE:-docker compose -f compose.yaml -f compose.mock.yaml}"

ok() { printf '  ok    %s\n' "$*"; }
fail() {
  printf '  FAIL  %s\n' "$*" >&2
  exit 1
}
section() { printf '\n%s\n' "$*"; }

# eventually SECONDS CMD... retries CMD once a second until it succeeds or time is up.
eventually() {
  local deadline=$((SECONDS + $1))
  shift
  until "$@" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || return 1
    sleep 1
  done
}

# status URL [CURL ARGS...] prints the HTTP status code.
status() {
  local url=$1
  shift
  curl -s -o /dev/null -w '%{http_code}' "$@" "$url"
}

# expect_status WANT URL [CURL ARGS...]
expect_status() {
  local want=$1 url=$2 got
  shift 2
  got=$(status "$url" "$@")
  [[ $got == "$want" ]] || fail "GET $url: status $got, want $want"
  ok "GET ${url#"$AIRQ"} -> $got"
}

header() { tr -d '\r' | awk -v h="$1" 'tolower($1)==tolower(h)":"{print $2}'; }

psql_admin() { "${COMPOSE[@]}" exec -T postgres psql -U postgres -d airq -tAqc "$1"; }
audit_rows() { psql_admin 'SELECT count(*) FROM upstream_responses'; }
has_audit_rows() { (($(audit_rows) > 0)); }

section "Probes"
expect_status 200 "$AIRQ/livez"
expect_status 200 "$AIRQ/readyz"
health=$(curl -fsS "$AIRQ/health")
for check in database migrations upstream_air_quality upstream_geocoding; do
  [[ $(jq -r ".checks.$check.status" <<<"$health") == ok ]] || fail "health check $check: $(jq -c ".checks.$check" <<<"$health")"
done
[[ $(jq -r .status <<<"$health") == ok ]] || fail "health status: $health"
ok "/health: ok, with database, migrations and both upstreams ok"

section "API"
hdrs=$(curl -fsS -D - -o /tmp/airq-smoke-body.json "$AIRQ/v1/air-quality/london")
[[ $(jq -r .location.name /tmp/airq-smoke-body.json) == London ]] || fail "unexpected body: $(head -c 300 /tmp/airq-smoke-body.json)"
jq -e '.current.european_aqi.value | numbers' /tmp/airq-smoke-body.json >/dev/null || fail "missing european_aqi"
[[ $(header X-Cache <<<"$hdrs") == MISS ]] || fail "first request X-Cache: $(header X-Cache <<<"$hdrs"), want MISS"
ok "GET /v1/air-quality/london -> 200, AQI present, X-Cache MISS"

hdrs=$(curl -fsS -D - -o /dev/null "$AIRQ/v1/air-quality/london")
[[ $(header X-Cache <<<"$hdrs") == HIT ]] || fail "second request X-Cache: $(header X-Cache <<<"$hdrs"), want HIT"
etag=$(header ETag <<<"$hdrs")
ok "repeat request served from cache (X-Cache HIT, ETag $etag)"
expect_status 304 "$AIRQ/v1/air-quality/london" -H "If-None-Match: $etag"
expect_status 200 "$AIRQ/v1/air-quality/current?lat=48.85&lon=2.35"
expect_status 200 "$AIRQ/v1/air-quality/forecast?location=Berlin&hours=24"
expect_status 400 "$AIRQ/v1/air-quality/current?lat=999&lon=0"
expect_status 404 "$AIRQ/v1/air-quality/atlantis"
expect_status 404 "$AIRQ/no-such-route"
ctype=$(curl -s -D - -o /dev/null "$AIRQ/v1/air-quality/current?lat=999&lon=0" | header Content-Type)
[[ $ctype == application/problem+json* ]] || fail "error Content-Type: $ctype"
ok "errors are application/problem+json"

section "Metrics"
metrics=$(curl -fsS "$AIRQ/metrics")
for series in \
  'airq_http_requests_total{code="200",method="GET",route="/v1/air-quality/{location}"}' \
  'airq_http_requests_total{code="304",method="GET",route="/v1/air-quality/{location}"}' \
  'airq_cache_lookups_total' \
  'airq_upstream_calls_total' \
  'airq_upstream_circuit_state{upstream="air_quality"} 0' \
  'airq_health_check_up{check="database",critical="false"} 1' \
  'airq_db_query_duration_seconds_bucket' \
  'go_sql_open_connections' \
  'airq_build_info'; do
  grep -qF "$series" <<<"$metrics" || fail "/metrics missing $series"
done
ok "/metrics exposes HTTP, cache, upstream, health, DB and build series"

section "Database audit log"
eventually 15 has_audit_rows || fail "no rows in upstream_responses"
ok "upstream responses recorded ($(audit_rows) rows)"
written=$(curl -fsS "$AIRQ/metrics" | awk '$1 == "airq_recorder_records_total{result=\"written\"}" {print $2}')
[[ -n $written && $written != 0 ]] || fail "recorder written counter is ${written:-missing}"
ok "recorder reports $written written records"
# The service role must be refused (not merely fail to connect) when it tries DDL or
# touches migration state.
denied() {
  local out
  out=$("${COMPOSE[@]}" exec -T postgres psql -U airq_app -d airq -tAqc "$1" 2>&1) && return 1
  grep -q 'permission denied' <<<"$out"
}
denied 'CREATE TABLE smoke_probe ()' || fail "service role airq_app was not denied CREATE TABLE"
ok "service role cannot run DDL"
denied 'DELETE FROM goose_db_version' || fail "service role airq_app was not denied writes to migration state"
ok "service role cannot modify migration state"
denied 'UPDATE upstream_responses SET error = NULL' || fail "service role airq_app was not denied UPDATE on the audit log"
ok "audit log is append-only for the service role (no UPDATE)"

section "Graceful degradation"
curl -fsS -X POST "$MOCK/__faults?failure_rate=1&status=503" >/dev/null
trap 'curl -fsS -X DELETE "$MOCK/__faults" >/dev/null || true' EXIT
expect_status 200 "$AIRQ/v1/air-quality/london"
ok "cached location still served while the vendor fails"
for _ in $(seq 1 6); do status "$AIRQ/v1/air-quality/current?lat=35.68&lon=139.69" >/dev/null; done
eventually 10 bash -c "curl -fsS '$AIRQ/metrics' | grep -qF 'airq_upstream_circuit_state{upstream=\"air_quality\"} 2'" ||
  fail "circuit breaker did not open"
ok "circuit breaker opened after repeated vendor failures"
resp=$(curl -s -D - -o /dev/null "$AIRQ/v1/air-quality/current?lat=35.68&lon=139.69")
[[ $(head -1 <<<"$resp" | awk '{print $2}') == 503 ]] || fail "uncached request with open circuit: $(head -1 <<<"$resp")"
[[ -n $(header Retry-After <<<"$resp") ]] || fail "503 without Retry-After"
ok "uncached request fails fast: 503 with Retry-After $(header Retry-After <<<"$resp")s"
eventually 10 bash -c "curl -fsS '$AIRQ/health' | jq -e '.status == \"degraded\"'" || fail "health did not report degraded"
expect_status 200 "$AIRQ/readyz"
ok "/health degraded while /readyz stays ready (vendor is a soft dependency)"
curl -fsS -X DELETE "$MOCK/__faults" >/dev/null

section "Prometheus"
eventually 60 bash -c "curl -fsS '$PROM/api/v1/query?query=up%7Bjob%3D%22airq%22%7D' | jq -e '.data.result[0].value[1] == \"1\"'" ||
  fail "Prometheus is not scraping airq"
ok "Prometheus scrapes airq (up == 1)"
rules=$(curl -fsS "$PROM/api/v1/rules" | jq '[.data.groups[].rules[]] | length')
((rules > 0)) || fail "no rules loaded"
ok "Prometheus loaded $rules recording and alerting rules"

section "Tracing"
eventually 30 bash -c "curl -fsS '$JAEGER/api/v3/services' | jq -e '.services | index(\"airq\")'" ||
  fail "no airq traces in Jaeger"
ok "traces exported to Jaeger"

if [[ ${SMOKE_STOP:-0} == 1 ]]; then
  section "Graceful shutdown"
  "${COMPOSE[@]}" stop airq >/dev/null 2>&1
  cid=$("${COMPOSE[@]}" ps -aq airq)
  code=$(docker inspect -f '{{.State.ExitCode}}' "$cid")
  [[ $code == 0 ]] || fail "airq exited with $code"
  docker logs "$cid" 2>&1 | grep -q '"msg":"shutdown complete"' || fail "no 'shutdown complete' log line"
  ok "airq drained and exited 0 on SIGTERM"
fi

printf '\nsmoke test passed\n'
