# Architecture

Cargo workspace, 11 crates, strictly layered — lower layers never depend
on higher ones:

```
server → views → handler → {dag, engines} → {models, state} → {protocol, config} → consts
```

| Crate       | Layer | Role |
|-------------|-------|------|
| `consts`    | L0    | error codes, the `Protocol` enum |
| `models`    | L1    | request/response domain types, typed params, usage, cost |
| `protocol`  | L1    | OpenAI / Anthropic wire types, DSL response transforms |
| `config`    | L1    | YAML config loading (`conf/gateway.yaml`) |
| `state`     | L2    | auth, account pool, quotas, rate limits, ledger, batch/file stores (in-process defaults; Postgres/Redis fleet backends) |
| `engines`   | L3    | engine implementations behind the `Transport` seam, SSE decoding, usage extraction, SigV4 |
| `dag`       | L3    | 4-layer pipeline executor + nodes |
| `handler`   | L4    | online/offline orchestration, DLP/blocklist plugins |
| `task`      | L5    | background tasks (daily quota reset) |
| `views`     | L5    | axum HTTP/WebSocket handlers, protocol conversion |
| `server`    | L6    | binary entrypoint: config + state + transport wiring |

## Request flow

```
client ──► views (auth, parse, protocol normalize)
       ──► handler (pre plugins: blocklist, then DLP redact)
       ──► dag: preprocess        resolve model, quota check, cache lookup
              account_select      priority / PTU-first / cooldown-aware selection
              model_access        rate limits, engine call, retry-on-5xx failover
              post_process        usage → billing ledger, cache store
       ──► handler (post plugins) ──► views (JSON or SSE re-emit)
```

A client disconnect does not cancel the pipeline: every request runs on
its own task (`run_pipeline` / `spawn_stream_pipeline`), so once admitted,
quota and ledger accounting run to completion even if the caller goes
away. Spawned background work (offline batches) likewise outlives the
submitting request.

The DAG executes four fixed layers; nodes within a layer are
topologically ordered by declared dependencies. `account_select` and
`model_access` form a retry loop: an upstream 5xx excludes the failed
account and reselects once; a PTU→paygo switch is recorded as
`ptu_spillover` in the ledger.

## Seams (traits)

Every boundary to the outside world is a trait with a deterministic
default, so the whole pipeline is testable offline:

- **`Transport`** — engines never own an HTTP client. The server default is
  `DispatchTransport`: accounts without an endpoint resolve to `mock://`
  sentinel URLs served in-process by `MockTransport` (deterministic
  vendor-shaped replies); real URLs go over `HttpTransport` (reqwest +
  rustls). `GW_TRANSPORT=mock` forces zero egress, `GW_TRANSPORT=http`
  disables the mock. Tests inject `MockTransport` directly.
- **`TokenEncoder`** — prompt token estimation capability (tiktoken
  cl100k_base BPE default, zero-dependency heuristic fallback). Not yet on
  a request path: its consumer is token-aware PTU sizing, which is not
  ported yet.
- **`Store`** — durable records (billing ledger, uploaded files, batch
  jobs). `MemoryStore` by default; `SqliteStore` when `storage.sqlite_path`
  is configured; `PostgresStore` when `storage.postgres_url` is — shared
  across a fleet.
- **`KeyStore`** — the live access-key table. `AkAuth` (in-process DashMap)
  by default; `PostgresKeyStore` with `storage.postgres_url` — fleet-shared
  behind a 2s auth cache, admin key CRUD survives restarts.
- **`HealthStore`** — account cooldown/recovery. In-process breaker by
  default; `RedisHealth` with `storage.redis_url` — a tripped account is
  skipped by every instance.
- **`Governance`** — rate/quota/TPM counters. In-process by default;
  `RedisGovernance` shares them (including pooled tenant QPS) fleet-wide.
- **config store** — with `storage.postgres_url`, config is versioned
  documents in Postgres (`PUT /admin/config` publishes; a LISTEN/NOTIFY
  change feed reloads every instance).
- **metrics facade** — `metrics` crate macros throughout; the server
  installs a Prometheus recorder and serves `/metrics`.
- Planned: see the [issue tracker](https://github.com/cocoonstack/gateway/issues).

## Testing

- Unit tests per crate; engine golden tests assert exact request wire
  shapes (via a recording transport) and response/usage parsing against
  recorded vendor fixtures.
- `crates/server/tests/e2e.rs` boots the full router in-process and
  exercises every API surface offline.
- `live_path.rs` drives the real reqwest stack against a loopback vendor to
  prove the go-live path end to end.
