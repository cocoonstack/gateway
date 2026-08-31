.PHONY: test test-integration web web-test build fmt fmt-check gofumpt goimports

RUST_CARGO ?= cargo

LOCALBIN ?= $(shell pwd)/bin
GOFUMPT_VERSION ?= v0.11.0
GOFUMPT_ROOT := $(LOCALBIN)/gofumpt-$(GOFUMPT_VERSION)
GOFMT := $(GOFUMPT_ROOT)/gofumpt
GOIMPORTS_VERSION ?= v0.49.0
GOIMPORTS_ROOT := $(LOCALBIN)/goimports-$(GOIMPORTS_VERSION)
GOIMPORTS := $(GOIMPORTS_ROOT)/goimports
GOIMPORTS_LOCAL_PREFIXES := github.com/cocoonstack/

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
