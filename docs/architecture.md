# Architecture

Cargo workspace, 12 crates, strictly layered — lower layers never depend
on higher ones:

```
server → views → handler → {dag, engines} → {models, state} → {protocol, config} → {consts, utils}
```

| Crate       | Layer | Role |
|-------------|-------|------|
| `consts`    | L0    | error codes, the `Protocol` enum |
| `utils`     | L0    | shared utilities |
| `models`    | L1    | request/response domain types, typed params, usage, cost |
| `protocol`  | L1    | OpenAI / Anthropic wire types, DSL response transforms |
| `config`    | L1    | YAML config loading (`conf/gateway.yaml`) |
| `state`     | L2    | auth, account pool, quotas, rate limits, ledger, batch/file stores (in-process) |
| `engines`   | L3    | engine implementations behind the `Transport` seam, SSE decoding, usage extraction, SigV4 |
| `dag`       | L3    | 4-layer pipeline executor + nodes |
| `handler`   | L4    | online/offline orchestration, DLP/blocklist plugins |
| `task`      | L5    | background tasks (daily quota reset) |
| `views`     | L5    | axum HTTP/WebSocket handlers, protocol conversion |
| `server`    | L6    | binary entrypoint: config + state + transport wiring |

## Request flow

```
client ──► views (auth, parse, protocol normalize)
       ──► handler (pre plugins: DLP redact, blocklist)
       ──► dag: preprocess        resolve model, quota check, cache lookup
              account_select      priority / PTU-first / cooldown-aware selection
              model_access        rate limits, engine call, retry-on-5xx failover
              post_process        usage → billing ledger, cache store
       ──► handler (post plugins) ──► views (JSON or SSE re-emit)
```

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
  rustls). `AP_TRANSPORT=mock` forces zero egress, `AP_TRANSPORT=http`
  disables the mock. Tests inject `MockTransport` directly.
- **`TokenEncoder`** — prompt token estimation capability (tiktoken
  cl100k_base BPE default, zero-dependency heuristic fallback). Not yet on
  a request path: its consumer is token-aware PTU sizing, which is not
  ported yet.
- **`Store`** — durable records (billing ledger, uploaded files, batch
  jobs). `MemoryStore` by default; `SqliteStore` (sqlx) when
  `storage.sqlite_path` is configured, surviving restarts.
- Planned (see [ROADMAP](../ROADMAP.md)): `Provider` (M2), metrics
  facade (M4), Redis rate/quota backends (M5).

## Testing

- Unit tests per crate; engine golden tests assert exact request wire
  shapes (via a recording transport) and response/usage parsing against
  recorded vendor fixtures.
- `crates/server/tests/e2e.rs` boots the full router in-process and
  exercises every API surface offline.
- `live_path.rs` drives the real reqwest stack against a loopback vendor to
  prove the go-live path end to end.
