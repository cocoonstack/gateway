# Gateway

Single-binary LLM access point in Rust (binary: `gw`): OpenAI- and
Anthropic-compatible APIs in front of pluggable model providers, with
key-based auth, quotas, rate limits, failover, and a billing ledger.

**Documentation: [cocoonstack.github.io/gateway](https://cocoonstack.github.io/gateway/)** (source in [`docs/`](docs/)).

## Highlights

- **OpenAI + Anthropic compatible surface** — `/v1/chat/completions`, `/v1/completions`, `/v1/responses`, `/v1/messages`, `/v1/embeddings`, `/v1/images/{generations,edits}`, `/v1/audio/{speech,transcriptions}`, `/v1/batches` + `/v1/files`, `/v1/models`, `/v1/realtime` (WebSocket) — streaming and non-streaming
- **Cross-protocol conversion** — serve Anthropic-style `/v1/messages` on OpenAI-protocol models and vice versa, including streaming event mapping
- **Staged request pipeline** — a 4-layer DAG per request: model resolve / quota / cache lookup → account selection (priority, PTU-first, failover) → rate limits + engine call (retry on upstream 5xx) → usage extraction, billing, cache store
- **Governance built in** — access-key auth, daily token quotas, QPS / QPM / TPM limits at key, product, and model level, request-level TTL cache, account cooldown and recovery, DLP redaction and blocklist plugins. Admission reserves then settles, so concurrent requests can't overshoot a quota
- **Multi-tenant** — keys carry a tenant; tenants get a pooled QPS bucket, a model entitlement allowlist, per-(key, model) quota defaults with an optional fallback-model degrade, key lifecycle (expiry/ban), and tenant-scoped admin tokens. Billing records charged cost and (optionally) vendor cost per row, so margin is queryable per tenant × model
- **Per-user billing & enterprise audit** — every ledger row attributes to an effective end user (the key's `owner`, else the request's `x-gw-user` / `user` hint) with a `request_id`, so cost rolls up per user (`/admin/usage/users`) and a soft per-user daily budget applies on every surface. Per-tenant content policy adds blocklist action tiers (block / flag / shadow), regex recognizers, secret masking, and an external-moderation seam; every hit is recorded without prompt text. An admin-operation trail (key CRUD / config / reload, with source IP) and optional at-rest content retention complete the audit surfaces (`/admin/audit/*`)
- **Fleet-ready** — run N instances behind a load balancer: Postgres shares config (versioned + a change feed), the access-key table, the ledger/files/batches store, and a distributed batch queue any instance drains; Redis shares rate/quota/TPM counters, account health, and optionally the response cache. Single-node stays zero-dependency
- **Providers behind traits** — engines talk to upstreams through a `Transport` seam; accounts with a real endpoint go over HTTP (reqwest + rustls), accounts without one are served by a deterministic in-process mock; AWS SigV4 signing included
- **Observability built in** — Prometheus `/metrics` (per-route request/status counters, per-pipeline-stage latency, token counters), structured access logs
- **One binary, one YAML** — no external services required to start; in-process state by default, SQLite for one-node durability, Postgres + Redis for a shared fleet; graceful shutdown

## Quick Start

```bash
# Run with the embedded demo config (mock upstreams, zero egress)
cargo run -p gw-server

# Chat completion
curl -s localhost:8080/v1/chat/completions \
  -H 'authorization: Bearer ak-demo-123' -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hello"}]}'

# Anthropic-style messages, streaming SSE
curl -sN localhost:8080/v1/messages \
  -H 'x-api-key: ak-demo-123' -H 'content-type: application/json' \
  -d '{"model":"claude-sonnet","stream":true,"max_tokens":128,"messages":[{"role":"user","content":"hi"}]}'

# Your own config
GW_CONFIG=conf/gateway.yaml cargo run -p gw-server

# Go live: give an account `endpoint` + `api_key_env` in the config — that's it.
# GW_TRANSPORT=mock forces zero egress; GW_TRANSPORT=http disables the mock.
```

Guides: [Examples](docs/examples.md) · [API](docs/api.md) · [Providers](docs/providers.md) · [Governance](docs/governance.md) · [Observability](docs/observability.md) · [Deployment](docs/deployment.md) · [Configuration](docs/configuration.md) · [Architecture](docs/architecture.md) · [Development](docs/development.md) · [Roadmap](https://github.com/cocoonstack/gateway/issues/1)

## Docker

```bash
docker build -t gateway .
docker run -p 8080:8080 gateway            # embedded demo config
docker run -p 8080:8080 -v $PWD/conf/gateway.yaml:/etc/gateway.yaml \
  -e GW_CONFIG=/etc/gateway.yaml gateway
```

The image binds `0.0.0.0` (`GW_HOST`) and ships a `/health` HEALTHCHECK.
Published multi-arch to `ghcr.io/cocoonstack/gateway` on `v*` tags.

## Development

```bash
make all      # fmt + lint + test + build
make test     # cargo test --workspace
make lint     # clippy -D warnings
make fmt      # cargo fmt --all
make deny     # cargo deny check (advisories + licenses)
make release  # optimized `gw` binary (--locked)
make docker   # build the container image
```

CI runs fmt/clippy/test + `cargo deny` on every push to `main` and every PR;
tagged `v*` pushes build multi-arch binaries (release) and a multi-arch image
(docker).

## License

This project is licensed under the GNU Affero General Public License v3.0. See [`LICENSE`](./LICENSE).
