.PHONY: test test-integration web web-test build fmt fmt-check gofumpt goimports lint golangci-lint

RUST_CARGO ?= cargo

LOCALBIN ?= $(shell pwd)/bin
GOLANGCILINT_VERSION ?= v2.13.2
GOLANGCILINT_ROOT := $(LOCALBIN)/golangci-lint-$(GOLANGCILINT_VERSION)
GOLANGCILINT := $(GOLANGCILINT_ROOT)/golangci-lint
GOFUMPT_VERSION ?= v0.11.0
GOFUMPT_ROOT := $(LOCALBIN)/gofumpt-$(GOFUMPT_VERSION)
GOFMT := $(GOFUMPT_ROOT)/gofumpt
GOIMPORTS_VERSION ?= v0.49.0
GOIMPORTS_ROOT := $(LOCALBIN)/goimports-$(GOIMPORTS_VERSION)
GOIMPORTS := $(GOIMPORTS_ROOT)/goimports
GOIMPORTS_LOCAL_PREFIXES := github.com/cocoonstack/

## Target OSes for lint
GOOSES ?= linux darwin

golangci-lint: $(GOLANGCILINT)
$(GOLANGCILINT):
	GOBIN=$(GOLANGCILINT_ROOT) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCILINT_VERSION)

gofumpt: $(GOFMT)
$(GOFMT):
	GOBIN=$(GOFUMPT_ROOT) go install mvdan.cc/gofumpt@$(GOFUMPT_VERSION)

goimports: $(GOIMPORTS)
$(GOIMPORTS):
	GOBIN=$(GOIMPORTS_ROOT) go install golang.org/x/tools/cmd/goimports@$(GOIMPORTS_VERSION)

fmt: gofumpt goimports
	$(GOFMT) -l -w .
	$(GOIMPORTS) -l -w --local '$(GOIMPORTS_LOCAL_PREFIXES)' .

fmt-check: gofumpt goimports
	@test -z "$$($(GOFMT) -l .)" || { echo "Files need formatting (gofumpt):"; $(GOFMT) -l .; exit 1; }
	@test -z "$$($(GOIMPORTS) -l .)" || { echo "Files need formatting (goimports):"; $(GOIMPORTS) -l .; exit 1; }

lint: golangci-lint ## Run golangci-lint on every target OS, integration harness included
	@for goos in $(GOOSES); do \
		echo "==> golangci-lint GOOS=$$goos"; \
		GOOS=$$goos $(GOLANGCILINT) run ./... || exit 1; \
		echo "==> golangci-lint GOOS=$$goos -tags integration ./internal/integration"; \
		GOOS=$$goos $(GOLANGCILINT) run --build-tags integration ./internal/integration || exit 1; \
	done

test:
	GOWORK=off go test ./...

test-integration:
	@set -e; trap 'docker compose -f compose.test.yaml down -v' EXIT; \
		docker compose -f compose.test.yaml up -d --wait; \
		$(RUST_CARGO) build --manifest-path ../Cargo.toml -p gw-server; \
		CP_TEST_PG_URL='postgres://postgres:controlplane@127.0.0.1:55432/controlplane?sslmode=disable' \
		CP_TEST_REDIS_URL='redis://127.0.0.1:56379/0' \
		CP_TEST_GW_BIN="$$(cd .. && pwd)/target/debug/gw" \
		GOWORK=off go test -tags=integration -count=1 -v ./internal/integration; \
		GW_TEST_PG_URL='postgres://postgres:controlplane@127.0.0.1:55432/controlplane' \
		GW_TEST_REDIS_URL='redis://127.0.0.1:56379' \
		$(RUST_CARGO) test --manifest-path ../Cargo.toml -p gw-state; \
		GW_TEST_PG_URL='postgres://postgres:controlplane@127.0.0.1:55432/controlplane' \
		$(RUST_CARGO) test --manifest-path ../Cargo.toml -p gw-server --test e2e admin_config_publish_reloads_from_store

web-test:
	npm --prefix web test
	npm --prefix web run build

web:
	npm --prefix web run build

build: web
	GOWORK=off go build ./cmd/control-plane
