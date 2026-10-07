# Running a fleet

Multiple `gw` instances behind a load balancer (e.g. nginx). What must be
shared, what stays local, and what the LB needs to do.

## What each instance holds

| State | Backend | Shared across the fleet? |
|-------|---------|--------------------------|
| Rate limits / quotas / TPM (`Governance`) | Redis (`storage.redis_url`) | ✅ when Redis is set (includes pooled tenant QPS) |
| Account health / cooldown (`HealthStore`) | Redis (`storage.redis_url`) | ✅ when Redis is set — one instance's cooldown benches the account for all |
| Per-model availability counts (`AvailStore`, behind `/admin/models/status`) | Redis (`storage.redis_url`) | ✅ when Redis is set — every instance's samples land in the same minute buckets (one hour retained); in-process otherwise |
| Config: keys/models/providers/tenants (`ConfigStore`) | Postgres (`storage.postgres_url`) | ✅ when Postgres is set — versioned documents + a change feed |
| Access-key table (`KeyStore`) | Postgres (`storage.postgres_url`) | ✅ when Postgres is set — admin key CRUD is fleet-wide in under a second and survives restarts; a key's MCP servers and tool allowlists are stored with it |
| Per-user budget overrides (`UserBudgetStore`) | Postgres (`storage.postgres_url`) | ✅ when Postgres is set — fleet-wide in under a second; in-process and lost on restart otherwise |
| Billing ledger / files / batches / video jobs (`Store`) | Postgres (`storage.postgres_url`), else SQLite | ✅ with Postgres (a video poll may land on any instance; the settle claim is one atomic row update); SQLite stays per-node |
| Request cache | in-process (moka), or Redis with `shared_cache: true` | ⚠️ per-instance by default; fleet-shared when `shared_cache` is set |
| Thinking-signature audit | in-process only | ⚠️ per-instance; a continuation landing on another instance finds no anchor and fails open (forwarded, not rejected) |
| Monthly cost counters (`Governance`) | Redis (`storage.redis_url`) | ✅ when Redis is set (62-day keys); in-process they are swept at the daily reset |
| Per-account latency (`stability.latency_routing`) | in-process only | ⚠️ per-instance; each instance ranks on its own samples, so a cold instance re-probes accounts the rest already measured |
| MCP OAuth tokens, session binding, live-stream counts | in-process only | ⚠️ per-instance: up to N token fetches per server; a session id binds to the first key that presents it on each instance, so route `/mcp/` sticky by the `Mcp-Session-Id` header: every request after the initialize then reaches the one instance holding the binding |

**A correct fleet = one Postgres (`storage.postgres_url`) + one Redis
(`storage.redis_url`) shared by every instance.** Without them each instance
counts, authenticates, and records on its own.

## Load balancer

Use the sample [`deploy/nginx.conf`](../deploy/nginx.conf). The essentials:

- **SSE**: `proxy_buffering off` and a long `proxy_read_timeout` — otherwise
  nginx buffers the whole stream or cuts long generations.
- **MCP** (`/mcp/`): SSE like the rest, plus a hash on `Mcp-Session-Id` so
  every request that carries a session id reaches the instance holding its
  binding (the id-less initialize may land elsewhere).
- **WebSocket** (`/v1/realtime`): the `Upgrade`/`Connection: upgrade` headers
  and a long read timeout. A WS connection pins to one instance for its life,
  so no session store is needed.
- **Health**: point the upstream health check at `/health`.
- **Metrics**: scrape `/metrics` on each instance directly (Prometheus service
  discovery), not through the LB — the LB would spread scrapes across
  instances and blur per-instance data.

## Session affinity

- **Chat/completions/embeddings/etc.** are stateless — any instance, no
  affinity needed.
- **Realtime WebSocket** pins naturally (the connection lives on one instance).
- **Batch**: with the Postgres store, submission persists the items and any
  instance's drain loop claims and runs the batch (`FOR UPDATE SKIP LOCKED`),
  so execution survives the submitter restarting and a crashed executor's
  work is requeued. Known behavior: when a stalled executor is reclaimed, its
  one in-flight item may run twice — two real upstream calls, both billed
  (results themselves dedup, first writer wins). On a local store
  (memory/sqlite) the job runs on the receiving instance; polling
  `GET /v1/batches/{id}` needs the submitting instance (use `ip_hash` on
  `/v1/batches`).

## Dynamic config

With `storage.postgres_url` set, config lives in the Postgres config store as
versioned documents; the local YAML file only seeds an empty store. To change
config fleet-wide, `PUT /admin/config` (global admin token) on any instance:
the document is validated, stored as a new version, and every instance —
including the publisher — reloads through the store's change feed, atomically
and with no dropped connections. `SIGHUP` and `POST /admin/reload` still
re-read the source for single-node or file-based setups. Because the stored
document includes `listen`, give each instance its own port with `GW_PORT`
(and `GW_HOST`) rather than a per-instance file.

Access keys are higher-churn and have their own seam: `/admin/keys` CRUD
writes the shared Postgres key table directly (no config publish needed); a
key created, re-quota'd, banned, or revoked on one instance is live on all in
under a second: the write and a Postgres NOTIFY naming the key commit
together, and every instance drops that key from its auth cache when the
NOTIFY arrives. A lost listener connection flushes the whole cache, again once
it is listening, and an entry idle for five minutes or cached for an hour is
refetched as the backstop for anything missed. The table holds key ids, not keys; the first
start of a release with ids rewrites an older table in place, so upgrade every
instance together — an instance still on the older release fails every key
until it is replaced. The rewrite is one-way: back up the table first, since
an older release cannot start on it. Per-key governance counters (daily quota,
TPM, QPS, model quotas, abuse) restart from zero at the upgrade. See [API — Admin](api.md#admin-dynamic-config).
