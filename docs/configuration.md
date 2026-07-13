# Configuration

One YAML file configures the gateway. Resolution order:

1. `GW_CONFIG=<path>` — explicit config file
2. otherwise the embedded default (the repo's `conf/gateway.yaml`)

`GW_HOST` / `GW_PORT` override `listen.host` / `listen.port` at runtime
(the container image sets `GW_HOST=0.0.0.0`).

## Sections

### `listen`

```yaml
listen:
  host: 127.0.0.1
  port: 8080
```

### `storage` — durable records and fleet backends

```yaml
storage:
  sqlite_path: /var/lib/gw/store.db   # empty/absent = in-memory
  postgres_url: postgres://gw:secret@db.internal/gw   # fleet-shared backend
  redis_url: redis://cache.internal:6379              # shared counters + health
  ledger_max_rows: 100000             # prune oldest billing rows past the cap; 0 = unlimited
```

The billing ledger, uploaded files, and batch jobs live here. In-memory by
default (lost on restart); a SQLite path makes them durable on one node.
`postgres_url` turns Postgres into the fleet backend: the source of truth for
config (versioned documents + a change feed every instance follows), the
shared access-key table, and the shared ledger/files/batches store.
`redis_url` shares rate/quota/TPM counters and account-health cooldowns across
instances.

### `access_keys` — client authentication and per-key governance

```yaml
access_keys:
  - ak: ak-demo-123          # bearer / x-api-key value clients send
    product: demo            # product group (for product-level QPM)
    tenant: acme             # optional; absent = the unrestricted `default` tenant
    qps: 100                 # per-key request rate
    daily_token_quota: 1000000
    tokens_per_minute: 600   # optional TPM window limit
    expires_at_epoch_secs: 1767225600  # optional expiry (403 after)
    banned: false            # optional; a banned key 403s but stays listed
    model_quotas:            # optional per-model daily caps (override tenant defaults)
      gpt-4o: 200000
```

### `tenants` — pooled limits, entitlement, quota defaults

```yaml
tenants:
  - name: acme
    qps: 50                  # pooled across ALL of acme's keys
    models: [gpt-4o, gpt-4o-mini]   # entitlement allowlist; absent = every model
    model_quotas:            # per-model daily-token defaults, applied per key
      gpt-4o: 100000
    fallback_model: gpt-4o-mini     # over-quota requests degrade here instead of failing
    admin_token_env: ACME_ADMIN_TOKEN   # optional tenant-scoped /admin token
```

Keys without a `tenant` join the implicit `default` tenant (no pooled limits,
entitled to every model), so a flat config keeps working unchanged. The model
catalog (`GET /v1/models`) filters to the caller's entitlement.

### `models` — public model names and dispatch

```yaml
models:
  - name: gpt-4o                     # name clients request
    protocol: openai-chat            # wire protocol (or set `provider:` instead)
    input_price_per_1k_micros: 2500  # billing rates (micros per 1k tokens)
    output_price_per_1k_micros: 10000
    qpm: 60                          # optional model-level rate limit
    cache_ttl_seconds: 60            # optional request-level response cache
```

### `providers` — first-class provider presets

```yaml
providers:
  - name: openai
    kind: openai              # openai | anthropic | gemini | deepseek | openrouter
    api_key_env: OPENAI_API_KEY
    # endpoint / timeout_seconds / connect_retries may be set here too and
    # are inherited by the synthesized account
models:
  - name: gpt-4o
    provider: openai          # fills the protocol with the kind's default
                              # and pins the model to that provider's accounts
```

A provider entry expands into an upstream account with the kind's preset
base URL (overridable via `endpoint:`, e.g. for OpenAI-compatible
vendors) and served wire types; an explicit account with the same name
wins. Gemini auth alignment is pending live verification.

### `accounts` — upstream credential slots

```yaml
accounts:
  - name: openai-main
    provider: openai
    priority: 1                # lower = preferred
    tier: ptu                  # ptu (provisioned, preferred) | paygo (default)
    protocols: ["openai-chat", "embeddings"]
    endpoint: ""               # empty → mock transport; real base URL → real upstream
    timeout_seconds: 60        # upstream request timeout (default 60)
    connect_retries: 1         # connect-phase retries; an in-flight request is never replayed
    api_key_env: ""            # env var name holding the API key (never the key itself)
    secret_key_env: ""         # AWS only: env var of the secret key (api_key_env = access key id)
```

Secrets never live in config files: `api_key_env` names an environment
variable that is read per request.

### `security`, `stability`, `products`

```yaml
security:
  dlp_redact: true             # redact emails/phone numbers before egress
  blocklist: ["badword"]       # reject requests containing listed terms

stability:
  failure_threshold: 3         # consecutive failures before an account cools down
  cooldown_seconds: 300

products:
  - name: myproduct
    qpm: 120                   # product-level request rate
```

## Observability

`GET /metrics` serves the Prometheus registry: `gateway_requests_total`
(route/status), `gateway_request_duration_seconds`,
`gateway_node_duration_seconds` (pipeline stage), `gateway_tokens_total`,
`gateway_cache_hits_total`, `gateway_ledger_write_failures_total`, and
`gateway_upstream_connect_retries_total` (account). One structured access
log line per request goes to stdout.

## Going live against real upstreams

```bash
export OPENAI_KEY=sk-...        # your key, in your environment
# account in YAML: endpoint: "https://api.openai.com", api_key_env: "OPENAI_KEY"
cargo run -p gw-server
```

Accounts with an `endpoint` egress to it; accounts without one are served
by the in-process mock. `GW_TRANSPORT` overrides the routing: `mock`
forces zero egress (nothing leaves the process), `http` disables the mock
so misconfigured accounts fail loudly instead of returning fake data.
