# Running a fleet

Multiple `gw` instances behind a load balancer (e.g. nginx). What must be
shared, what stays local, and what the LB needs to do.

## What each instance holds

| State | Backend | Shared across the fleet? |
|-------|---------|--------------------------|
| Rate limits / quotas / TPM (`Governance`) | Redis (`storage.redis_url`) | ✅ when Redis is set (includes pooled tenant QPS) |
| Account health / cooldown (`HealthStore`) | Redis (`storage.redis_url`) | ✅ when Redis is set — one instance's cooldown benches the account for all |
| Config: keys/models/providers/tenants (`ConfigStore`) | Postgres (`storage.postgres_url`) | ✅ when Postgres is set — versioned documents + a change feed |
| Access-key table (`KeyStore`) | Postgres (`storage.postgres_url`) | ✅ when Postgres is set — admin key CRUD is fleet-wide within ~2s and survives restarts |
| Billing ledger / files / batches (`Store`) | Postgres (`storage.postgres_url`), else SQLite | ✅ with Postgres; SQLite stays per-node |
| Request cache | in-process (moka), or Redis with `shared_cache: true` | ⚠️ per-instance by default; fleet-shared when `shared_cache` is set |

**A correct fleet = one Postgres (`storage.postgres_url`) + one Redis
(`storage.redis_url`) shared by every instance.** Without them each instance
counts, authenticates, and records on its own.

## Load balancer

Use the sample [`deploy/nginx.conf`](../deploy/nginx.conf). The essentials:

- **SSE**: `proxy_buffering off` and a long `proxy_read_timeout` — otherwise
  nginx buffers the whole stream or cuts long generations.
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
re-read the source for single-node or file-based setups.

Access keys are higher-churn and have their own seam: `/admin/keys` CRUD
writes the shared Postgres key table directly (no config publish needed); a
key created, re-quota'd, banned, or revoked on one instance is live on all
within the ~2s auth-cache TTL. See [API — Admin](api.md#admin-dynamic-config).
