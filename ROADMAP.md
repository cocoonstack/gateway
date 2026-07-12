# Roadmap

Direction: a production-grade, open-source LLM gateway. Every external
dependency — model providers, storage, rate limiting, caching, metrics,
token counting, HTTP — sits behind a trait with a default open-source
implementation, so any piece can be swapped without touching the pipeline.

The offline test surface stays first-class: the default build has zero
egress and the full suite runs without network or external services.

## M0 — Open-source bootstrap (done)

- [x] Workspace renamed to `ap-*` crates, binary `ap`, env prefix `AP_`
- [x] Internal-heritage references scrubbed; all comments in English
- [x] Concise `README.md`, `docs/`, MIT license, GitHub Actions CI

## M1 — Interfaces and real transport

Promote the trait seams from "mock now, real later" to "default
open-source impl now, alternatives pluggable":

- [x] `Transport`: HTTP (reqwest + rustls) promoted from feature flag to
      the default scheme-routing dispatch (`mock://` sentinels stay
      in-process); pooled client, 60s timeout. Per-provider timeout/retry
      policy lands in M3. Mock transport remains the test default.
- [x] `TokenEncoder`: default BPE via `tiktoken-rs` cl100k_base (heuristic
      stays as a zero-dep fallback). Capability only — wiring lands with
      token-aware PTU sizing.
- [x] `Store` (billing ledger, files, batches): async trait with
      `MemoryStore` (default, tests) and `SqliteStore` (sqlx, WAL; selected
      by `storage.sqlite_path`, survives restarts).
- [x] `RateLimiter`: GCRA via `governor`; quota/window counters in-memory
      KV. The `Governance` trait (M5) makes the whole set pluggable.
- [x] `Cache`: request-level TTL cache backed by `moka` (per-entry TTL,
      bounded capacity) behind the existing facade.

## M2 — First-class providers: OpenAI, Anthropic, Gemini

- [x] Provider presets: `providers:` config (kinds openai / anthropic /
      gemini) expand into accounts with preset endpoints + key env vars;
      models take a `provider:` shorthand
- [x] Config-driven catalog: the per-model wire-type enum is retired — a
      model binds a name to one of 19 `Protocol`s (directly or via its
      provider), and a `provider:` binding pins it to that provider's
      accounts
- [x] OpenAI-protocol path verified live against an OpenAI-compatible
      upstream: non-stream + streaming SSE + streamed-usage billing all
      confirmed end to end (provider preset + `endpoint:` override)
- [ ] Anthropic / Gemini live verification (pending credentials); recorded
      live fixtures replayable offline
- [ ] Streaming fidelity hardening per provider: SSE frame alignment
      across vendors
- [ ] Provider auth: bearer (OpenAI), x-api-key + anthropic-version
      (Anthropic), OAuth/API key (Gemini)
- [ ] Long-tail OpenAI-compatible vendors served by a generic
      openai-compatible provider entry (base_url + key)

## M3 — Streaming and resilience hardening

- [x] Per-account timeout + connect-phase retry policy
      (`timeout_seconds`/`connect_retries` on accounts and provider
      presets; a request that reached the vendor is never replayed)
- [x] Client cancellation propagates to the upstream by drop: axum drops
      the handler future on disconnect, aborting the reqwest call
- [x] Circuit breaking via account cooldown (consecutive failures →
      cooldown → auto-recovery)
- [x] Incremental SSE forwarding: upstream frames flow through a bounded
      channel to the client as they arrive (openai-chat surface; verified
      live — 31 frames spread over 0.44s vs a single flush before); the
      channel bound is the backpressure seam

## M4 — Observability

- [x] `metrics` facade with a Prometheus `/metrics` endpoint: request
      counters (surface/protocol/status), token counters, request duration,
      per-pipeline-node latency, cache hits, ledger write failures
- [x] Access log finalized (ak/product/model/protocol/account/status/
      tokens/latency)
- [x] Ledger snapshot pagination (`?limit=`, count stays the total);
      retention policy still open
- [x] Ledger retention (`storage.ledger_max_rows`, oldest pruned on write)

## M5 — Persistence backends

- [ ] SQLite `Store` shipping as the packaged default
- [ ] Optional Redis backend for distributed rate limiting / quotas
      (multi-replica deployments)

## M6 — Long tail

- [ ] Realtime WebSocket bridging to real upstreams
- [ ] Batch workflow (files → batch → poll → results) against real
      providers
- [ ] Additional providers: DeepSeek, Qwen, OpenRouter, local runtimes
      (Ollama/vLLM)
