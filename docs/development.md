# Development

## Build & check

```bash
make all         # fmt + lint + test + build
make test        # cargo test --workspace
make lint        # cargo clippy --workspace --all-targets -- -D warnings
make fmt         # cargo fmt --all
make deny        # cargo deny check (advisories + licenses)
make release     # optimized gw-server binary (--locked)
make docker      # build the container image
make run         # cargo run -p gw-server
```

CI runs fmt/clippy/test and `cargo deny` on every push to `main` and every pull request. A `v*` tag cuts one
GitHub release (`.github/workflows/release.yml`): native runners build `gw`
for linux/darwin × amd64/arm64, goreleaser builds the `control-plane` binaries
and the web-asset tarball, attaches everything, and generates the changelog.
The tag is the version — the release build stamps it into the workspace before
compiling, so nothing needs bumping in `Cargo.toml`. Multi-arch container
images for both components go to ghcr on the same tag
(`.github/workflows/docker.yml`). Edition 2024; the workspace denies
`unwrap`/`expect`/undocumented `unsafe` outside tests.

## Workspace layout

Crates are strictly layered — lower layers never depend on higher ones:

```
server → views → handler → {dag, engines} → {models, state} → {protocol, config} → consts
```

| Crate | Role |
|-------|------|
| `consts` | error codes, the `Protocol` enum |
| `models` | request/response types, typed params, usage, cost, token estimation |
| `protocol` | OpenAI/Anthropic wire types + cross-protocol conversions |
| `config` | YAML config, provider presets, name indices |
| `state` | auth, account pool, health, cache; `Store` and `Governance` seams |
| `engines` | per-protocol engines behind the `Transport` seam, SSE, SigV4 |
| `dag` | the 4-layer request pipeline, nodes in declaration order |
| `handler` | online/offline orchestration, DLP/blocklist plugins |
| `task` | background tasks: quota reset, content purge, usage rollup, availability flush + alerts, alert dispatch |
| `views` | axum HTTP/WebSocket handlers, streaming, metrics |
| `server` | binary: wires config + state + transport, serves the router |

## Seams

Every boundary to the outside world is a trait with a deterministic default, so
the whole pipeline runs offline in tests:

| Trait | Default | Alternative |
|-------|---------|-------------|
| `Transport` | dispatch (mock in-process, HTTP for real URLs) | force mock / force HTTP |
| `Store` | in-memory | SQLite / Postgres (fleet) |
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

It is a manual diagnostic rather than a CI merge gate; measured HTTP-level
numbers and the load-test recipe are in [Performance](performance.md).

## Live vendor matrix

`scripts/live-matrix/` drives a running gateway against real vendors with real
keys — the check the mock cannot make. `live.yaml` declares one account per
vendor (keys come from the named env vars, never from the file) with prices
and `token_rate` weights chosen so every billing dimension is visible;
`live_matrix.py` runs, per provider group, non-streaming and streaming chat,
`/v1/messages` (native and cross-protocol), thinking (budget/adaptive/effort
dialects, signed replay through a tool loop), prompt cache (Anthropic
breakpoints, automatic prefix caching on OpenAI/DeepSeek/Qwen), the response
cache, embeddings, rerank, image and async video (submit, poll to `done`, one
settle row). For every call it recomputes the weighted total
and cost from the *wire* usage with the configured prices and compares them
to the newest ledger row — an oracle independent of the gateway's own
arithmetic — and asserts that reasoning content and cache reads/writes reached
the client.

```bash
export OPENAI_API_KEY=... ANTHROPIC_API_KEY=...          # every api_key_env in live.yaml
GW_ADMIN_TOKEN=admin-live GW_CONFIG=scripts/live-matrix/live.yaml ./target/release/gw &
python3 scripts/live-matrix/live_matrix.py               # or: ... anthropic bedrock
```

Last full run (2026-08-19): all cases across Anthropic, OpenAI (incl.
gpt-realtime-mini through `/v1/realtime` and sora-2 video), Gemini, DeepSeek,
MiniMax (incl. Hailuo video), Qwen/DashScope (incl. wan2.2-t2v-plus video),
Qianfan, Moonshot, SiliconFlow (incl. Wan2.2 video), OpenRouter, Cohere/Jina
rerank, xAI Grok (chat, Responses, image, video), Kling video, Brave search,
Bedrock (InvokeModel, Converse, Llama), a local Ollama through the generic
OpenAI-compatible path, OpenAI moderations/TTS/STT, the DashScope legacy wire
and the Gemini Live realtime dialect — every ledger row matched the oracle
exactly.
