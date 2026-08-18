# Gateway

Single-binary LLM access point in Rust (binary: `gw`): OpenAI- and
Anthropic-compatible APIs in front of pluggable model providers, with
key-based auth, quotas, rate limits, failover, and a billing ledger.

```
client ──► /v1/* (OpenAI + Anthropic surfaces, streaming SSE, realtime WS)
       ──► pipeline: resolve/quota/cache → account select (PTU, failover)
                     → rate limits → engine → usage → billing ledger
       ──► providers: real endpoints over HTTP · in-process mock for the rest
```

## Guides

- [Examples](examples.md) — copy-paste curl for the main surfaces, going live
- [API reference](api.md) — every endpoint, auth, streaming, batch/files
- [Providers](providers.md) — presets, going live, failover, SigV4
- [Governance](governance.md) — access keys, rate limits, quotas, cache, safety
- [Observability](observability.md) — `/metrics`, access logs, the ledger
- [Deployment](deployment.md) — binary, Docker, env vars, multi-replica, control plane
- [Running a fleet](multi-instance.md) — nginx LB, shared state, affinity
- [Configuration](configuration.md) — the full `gateway.yaml` reference
- [Architecture](architecture.md) — crate layout, pipeline, trait seams
- [Performance](performance.md) — measured per-node throughput and latency, how to reproduce
- [Development](development.md) — build, test, workspace map, contributing

## Repository

Source and issue tracker:
[github.com/cocoonstack/gateway](https://github.com/cocoonstack/gateway).
