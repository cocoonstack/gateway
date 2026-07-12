BIN := ap

.PHONY: all build release test lint fmt fmt-check deny clean docker run

all: fmt lint test build

build:
	cargo build --workspace

release:
	cargo build --release -p ap-server --locked

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
	cargo run -p ap-server

clean:
	cargo clean
