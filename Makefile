BIN := gw

.PHONY: all build release test lint fmt fmt-check deny docker run clean cloc control-plane control-plane-test control-plane-integration

all: fmt lint test build

build:
	cargo build --workspace

release:
	cargo build --release -p gw-server --locked

test:
	cargo test --workspace

lint:
	cargo clippy --workspace --all-targets -- -D warnings

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

deny:
	cargo deny check

docker:
	docker build -t gateway .

run:
	cargo run -p gw-server

control-plane:
	$(MAKE) -C control-plane build

control-plane-test:
	$(MAKE) -C control-plane test web-test

control-plane-integration:
	$(MAKE) -C control-plane test-integration

cloc:
	cloc --exclude-dir=target,dist,node_modules --exclude-ext=json \
		--not-match-f='(_test\.go|\.test\.ts)$$' .

clean:
	cargo clean
