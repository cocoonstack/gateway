# Development

## Build & check

```bash
make all         # fmt + lint + test + build
make test        # cargo test --workspace
make lint        # cargo clippy --workspace --all-targets -- -D warnings
make fmt         # cargo fmt --all
make deny        # cargo deny check (advisories + licenses)
make release     # optimized gw-server binary (--locked)
make dist        # cross-platform release artifacts (mirrors CI)
make docker      # build the container image
make run         # cargo run -p gw-server
```

CI runs fmt/clippy/test and `cargo deny` on every push. Releases are cut by
[dist](https://opensource.axo.dev/cargo-dist/) on a `v*` tag — it
cross-compiles the binaries, generates the install script and checksums, and
publishes the GitHub release (`.github/workflows/release.yml` is generated;
edit `dist-workspace.toml` and run `dist generate`). The container image is
built separately on tags (`.github/workflows/docker.yml`). Edition 2024; the
workspace denies `unwrap`/`expect`/undocumented `unsafe` outside tests.

## Workspace layout

Crates are strictly layered — lower layers never depend on higher ones:

```
server → views → handler → {dag, engines} → {models, state} → {protocol, config} → consts
```

| Crate | Role |
|-------|------|
| `consts` | error codes, the `Protocol` enum |
| `models` | request/response types, typed params, usage, cost, token estimation |
| `protocol` | OpenAI/Anthropic wire types, cross-protocol DSL |
| `config` | YAML config, provider presets, name indices |
| `state` | auth, account pool, health, cache; `Store` and `Governance` seams |
| `engines` | per-protocol engines behind the `Transport` seam, SSE, SigV4 |
| `dag` | the 4-layer request pipeline (precomputed plan) |
| `handler` | online/offline orchestration, DLP/blocklist plugins |
| `task` | background tasks (daily quota reset) |
| `views` | axum HTTP/WebSocket handlers, streaming, metrics |
| `server` | binary: wires config + state + transport, serves the router |

## Seams

Every boundary to the outside world is a trait with a deterministic default, so
the whole pipeline runs offline in tests:

| Trait | Default | Alternative |
|-------|---------|-------------|
| `Transport` | dispatch (mock in-process, HTTP for real URLs) | force mock / force HTTP |
| `Store` | in-memory | SQLite (`sqlx`) |
| `Governance` | in-memory counters | Redis |
| `TokenEncoder` | tiktoken cl100k BPE | heuristic fallback |

## Testing

Unit tests live beside their code; integration tests are in `crates/*/tests/`.
Engine golden tests assert exact request wire shapes and response parsing
against recorded fixtures. `crates/server/tests/e2e.rs` boots the full router
in-process and exercises every surface offline. Tests that need real
infrastructure gate on an env var (e.g. `GW_TEST_REDIS_URL`) and no-op when it
is unset. A release micro-benchmark lives in `crates/server/tests/bench.rs`:

```bash
cargo test --release -p gw-server --test bench -- --ignored --nocapture
```
