# Developer shortcuts. Everything here is a thin wrapper: the single command to run
# the stack is still `docker compose up --build`.

SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE      := docker compose
COMPOSE_MOCK := docker compose -f compose.yaml -f compose.mock.yaml
GOLANGCI_LINT_VERSION := v2.15.0
TEST_PG_IMAGE := postgres:18-alpine@sha256:10a5c69a83035ab5239f5620d708520e8b28f5aa1f2b20143e6024532edadfb5
TEST_PG_PORT  ?= 55432
MOCK_URL      ?= http://localhost:$(or $(MOCK_PORT),8090)

# Stamp images built by compose with the git revision.
export AIRQ_VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
export AIRQ_REVISION ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

##@ Run

.PHONY: up
up: ## Start the full stack against the real Open-Meteo API
	$(COMPOSE) up --build

.PHONY: up-mock
up-mock: ## Start the full stack against the fault-injectable mock vendor
	$(COMPOSE_MOCK) up --build

.PHONY: down
down: ## Stop the stack (keeps data volumes)
	$(COMPOSE_MOCK) down

.PHONY: clean
clean: ## Stop the stack and delete its volumes
	$(COMPOSE_MOCK) down -v

.PHONY: logs
logs: ## Follow the service logs
	$(COMPOSE) logs -f airq

.PHONY: psql
psql: ## Open psql on the stack database (as the admin user)
	$(COMPOSE) exec postgres psql -U postgres -d airq

.PHONY: migrate-status
migrate-status: ## Show applied and pending migrations
	$(COMPOSE) run --rm migrate migrate status

##@ Test

.PHONY: test
test: ## Unit tests with the race detector (PostgreSQL tests skip without TEST_DATABASE_URL)
	go test -race -shuffle=on ./...

.PHONY: test-integration
test-integration: ## All tests, including PostgreSQL integration tests, on a throwaway database
	@docker rm -f airq-test-pg >/dev/null 2>&1 || true
	docker run -d --rm --name airq-test-pg -e POSTGRES_PASSWORD=postgres \
	  -p 127.0.0.1:$(TEST_PG_PORT):5432 --tmpfs /var/lib/postgresql:rw,size=512m $(TEST_PG_IMAGE) >/dev/null
	@until docker exec airq-test-pg pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; do sleep 1; done
	TEST_DATABASE_URL='postgres://postgres:postgres@127.0.0.1:$(TEST_PG_PORT)/postgres?sslmode=disable' \
	  go test -race -shuffle=on -count=1 ./...; status=$$?; docker rm -f airq-test-pg >/dev/null; exit $$status

.PHONY: cover
cover: ## Write an HTML coverage report to coverage.html
	go test -covermode=atomic -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

.PHONY: lint
lint: ## Run golangci-lint (same version as CI)
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

.PHONY: vuln
vuln: ## Report known vulnerabilities reachable from the code
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

.PHONY: rules-test
rules-test: ## Validate Prometheus config and run the alert unit tests
	docker run --rm -v "$(CURDIR)/deploy/prometheus:/etc/prometheus:ro" --entrypoint promtool \
	  prom/prometheus:v3.15.1@sha256:fa3302279710be2f21a8301ec17a8b4f2301fd829bd113839b3b6b02b6f191a2 \
	  check config /etc/prometheus/prometheus.yml
	docker run --rm -v "$(CURDIR)/deploy/prometheus:/etc/prometheus:ro" -w /etc/prometheus --entrypoint promtool \
	  prom/prometheus:v3.15.1@sha256:fa3302279710be2f21a8301ec17a8b4f2301fd829bd113839b3b6b02b6f191a2 \
	  test rules tests/airq.rules.test.yml

.PHONY: smoke
smoke: ## Run the e2e smoke test against a running mock stack (make up-mock)
	scripts/smoke.sh

.PHONY: e2e
e2e: ## Start the mock stack, run the smoke test, tear everything down
	$(COMPOSE_MOCK) up -d --build --wait --wait-timeout 180 airq prometheus
	scripts/smoke.sh; status=$$?; $(COMPOSE_MOCK) down -v; exit $$status

##@ Chaos drills (mock stack)

.PHONY: chaos-vendor-down
chaos-vendor-down: ## Make every vendor call fail with 503
	curl -fsS -X POST '$(MOCK_URL)/__faults?failure_rate=1&status=503'; echo

.PHONY: chaos-vendor-flaky
chaos-vendor-flaky: ## Fail 30% of vendor calls
	curl -fsS -X POST '$(MOCK_URL)/__faults?failure_rate=0.3&status=500'; echo

.PHONY: chaos-vendor-slow
chaos-vendor-slow: ## Add 4s of vendor latency (beyond the 3s attempt timeout)
	curl -fsS -X POST '$(MOCK_URL)/__faults?latency=4s'; echo

.PHONY: chaos-db-down
chaos-db-down: ## Stop PostgreSQL (the API keeps serving; health shows degraded)
	$(COMPOSE) stop postgres

.PHONY: chaos-reset
chaos-reset: ## Clear vendor faults and restart PostgreSQL
	curl -fsS -X DELETE '$(MOCK_URL)/__faults'; echo
	$(COMPOSE) start postgres

##@ Build

.PHONY: build
build: ## Build the binaries into bin/
	CGO_ENABLED=0 go build -trimpath -o bin/ ./cmd/...

.PHONY: image
image: ## Build the service image as airq:local
	docker build --build-arg VERSION=$(AIRQ_VERSION) --build-arg REVISION=$(AIRQ_REVISION) -t airq:local .
