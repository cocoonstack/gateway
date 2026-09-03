# Deployment

## Binary

Install a tagged release (Linux/macOS, x86_64/arm64) with the generated script:

```bash
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/cocoonstack/gateway/releases/latest/download/gw-server-installer.sh | sh
```

Or build from source:

```bash
make release            # target/release/gw, built --locked
GW_CONFIG=/etc/gateway.yaml ./target/release/gw
```

### Environment

| Variable | Effect |
|----------|--------|
| `GW_CONFIG` | config file path; unset uses the embedded demo config |
| `GW_HOST` | override `listen.host` (containers set `0.0.0.0`) |
| `GW_PORT` | override `listen.port` |
| `GW_TRANSPORT` | `mock` (zero egress) / `http` (no mock) / unset (auto-route) |
| `GW_CONTENT_KEY` | 64 hex chars (32 bytes); seals retained content at rest. Without it, `full` retention stores redacted text instead of raw |
| `GW_ADMIN_TOKEN` | global admin bearer named by the default config's `admin.token_env`; unset leaves `/admin/*` and `/internal/*` answering 404 |
| `RUST_LOG` | log level, e.g. `info`, `gw_views=debug` |
| provider key vars | named by each account's `api_key_env` |

The process drains on SIGINT/SIGTERM (graceful shutdown of in-flight requests).

## Docker

```bash
docker build -t gateway .
docker run -p 8080:8080 gateway            # embedded demo config
docker run -p 8080:8080 \
  -v $PWD/conf/gateway.yaml:/etc/gateway.yaml \
  -e GW_CONFIG=/etc/gateway.yaml \
  -e OPENAI_API_KEY=sk-... \
  gateway
```

The image is a slim non-root runtime, binds `0.0.0.0`, and has a `/health`
HEALTHCHECK. Tagged `v*` pushes publish a multi-arch image to
`ghcr.io/cocoonstack/gateway`.

## Multi-replica

State that must be shared across replicas has a backend:

```yaml
storage:
  postgres_url: "postgres://gw:secret@db:5432/gw"  # fleet config + keys + ledger/files/batches
  redis_url: "redis://redis:6379"      # shared rate limits + quotas + account health
  ledger_max_rows: 1000000             # prune oldest billing rows past the cap
  postgres_max_connections: 10         # store and key-store connection-pool size
  # sqlite_path: /var/lib/gw/store.db  # single-node alternative to postgres_url
```

- **Durable records** (ledger, files, batches, async video jobs — pruned 30
  days after submit): SQLite when `sqlite_path` is set (survives restarts),
  otherwise in-memory. The single-node SQLite store
  sweeps orphaned `pending`/`running` batch jobs to `failed` on startup; the
  Postgres store deliberately does not (another live instance may still be
  executing them — stale claims are requeued via the fleet drain instead).
  Billing rows batch off the request path on the SQL backends: `SIGTERM`
  flushes the queue before exit, so only a `SIGKILL` between a response and
  the next flush loses rows: the queue plus the batch in flight, 4352 rows at
  the defaults (4096 queued + 256 in flight). A full queue falls back to the
  awaited write, so an accepted request never loses its row under overload.
- **Rate limits & quotas**: shared in Redis when `redis_url` is set (keys
  namespaced under `gw:`, windows self-expire), otherwise in-process. Without
  Redis, each replica limits independently.

## Control plane

The browser console in [`control-plane/`](../control-plane/) deploys beside the
gateway fleet: a single Go binary serving the built web assets, plus its own
Postgres (identities) and Redis (sessions) — never the gateway's tables.
Releases ship `ghcr.io/cocoonstack/gateway-control-plane` (multi-arch) and
binary tarballs for linux/darwin × amd64/arm64 with the web assets bundled.

Point it at the fleet with `CP_GATEWAY_TARGETS`, give it the global admin token
via `CP_GATEWAY_ADMIN_TOKEN`, and hand each tenant's scoped token to
`CP_GATEWAY_TENANT_TOKENS` (`tenant=token,...` matching each tenant's
`admin_token_env`) so tenant-admin key mutations run under the gateway's own
tenant scope and fail closed without one. Full reference:
[`control-plane/README.md`](../control-plane/README.md).
