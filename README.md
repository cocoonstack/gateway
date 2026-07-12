# Gateway

Single-binary LLM access point in Rust (binary: `ap`): OpenAI- and
Anthropic-compatible APIs in front of pluggable model providers, with
key-based auth, quotas, rate limits, failover, and a billing ledger.

**Documentation: [cmgs.github.io/gateway](https://cmgs.github.io/gateway/)** (source in [`docs/`](docs/)).

## Highlights

- **OpenAI + Anthropic compatible surface** — `/v1/chat/completions`, `/v1/completions`, `/v1/responses`, `/v1/messages`, `/v1/embeddings`, `/v1/images/{generations,edits}`, `/v1/audio/{speech,transcriptions}`, `/v1/batches` + `/v1/files`, `/v1/models`, `/v1/realtime` (WebSocket) — streaming and non-streaming
- **Cross-protocol conversion** — serve Anthropic-style `/v1/messages` on OpenAI-protocol models and vice versa, including streaming event mapping
- **Staged request pipeline** — a 4-layer DAG per request: model resolve / quota / cache lookup → account selection (priority, PTU-first, failover) → rate limits + engine call (retry on upstream 5xx) → usage extraction, billing, cache store
- **Governance built in** — access-key auth, daily token quotas, QPS / QPM / TPM limits at key, product, and model level, request-level TTL cache, account cooldown and recovery, DLP redaction and blocklist plugins
- **Providers behind traits** — engines talk to upstreams through a `Transport` seam; accounts with a real endpoint go over HTTP (reqwest + rustls), accounts without one are served by a deterministic in-process mock; AWS SigV4 signing included
- **Observability built in** — Prometheus `/metrics` (per-route request/status counters, per-pipeline-stage latency, token counters), structured access logs
- **One binary, one YAML** — no external services required; in-process state, optional SQLite persistence, graceful shutdown

## Quick Start

```bash
# Run with the embedded demo config (mock upstreams, zero egress)
cargo run -p ap-server

# Chat completion
curl -s localhost:8080/v1/chat/completions \
  -H 'authorization: Bearer ak-demo-123' -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hello"}]}'

# Anthropic-style messages, streaming SSE
curl -sN localhost:8080/v1/messages \
  -H 'x-api-key: ak-demo-123' -H 'content-type: application/json' \
  -d '{"model":"claude-sonnet","stream":true,"max_tokens":128,"messages":[{"role":"user","content":"hi"}]}'

# Your own config
AP_GATEWAY_CONF=conf/gateway.yaml cargo run -p ap-server

# Go live: give an account `endpoint` + `api_key_env` in the config — that's it.
# AP_TRANSPORT=mock forces zero egress; AP_TRANSPORT=http disables the mock.
```

Guides: [Architecture](docs/architecture.md) · [Configuration](docs/configuration.md) · [Roadmap](ROADMAP.md)

## Docker

```bash
docker build -t gateway .
docker run -p 8080:8080 gateway            # embedded demo config
docker run -p 8080:8080 -v $PWD/conf/gateway.yaml:/etc/gateway.yaml \
  -e AP_GATEWAY_CONF=/etc/gateway.yaml gateway
```

The image binds `0.0.0.0` (`AP_HOST`) and ships a `/health` HEALTHCHECK.
Published multi-arch to `ghcr.io/cmgs/gateway` on push.

## Development

```bash
make all      # fmt + lint + test + build
make test     # cargo test --workspace
make lint     # clippy -D warnings
make fmt      # cargo fmt --all
make deny     # cargo deny check (advisories + licenses)
make release  # optimized ap-server binary (--locked)
make docker   # build the container image
```

CI runs fmt/clippy/test + `cargo deny` on every push; tagged `v*` pushes
build multi-arch binaries (release) and a multi-arch image (docker).

## License

This project is licensed under the GNU Affero General Public License v3.0. See [`LICENSE`](./LICENSE).
