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
  # sqlite_path: /var/lib/gw/store.db  # single-node alternative to postgres_url
```

- **Durable records** (ledger, files, batches): SQLite when `sqlite_path` is
  set (survives restarts), otherwise in-memory. The single-node SQLite store
  sweeps orphaned `pending`/`running` batch jobs to `failed` on startup; the
  Postgres store deliberately does not (another live instance may still be
  executing them — stale claims are requeued via the fleet drain instead).
- **Rate limits & quotas**: shared in Redis when `redis_url` is set (keys
  namespaced under `gw:`, windows self-expire), otherwise in-process. Without
  Redis, each replica limits independently.
