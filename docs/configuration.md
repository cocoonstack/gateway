# Configuration

One YAML file configures the gateway. Resolution order:

1. `GW_CONFIG=<path>` — explicit config file
2. otherwise the embedded default (the repo's `conf/gateway.yaml`)

`GW_HOST` / `GW_PORT` override `listen.host` / `listen.port` at runtime
(the container image sets `GW_HOST=0.0.0.0`). `GW_CONTENT_KEY` (64 hex chars =
32 bytes) is the deployment key that seals retained content at rest; without it,
`full` retention refuses to store raw text and falls back to redacted.

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
  shared_cache: false                 # also share the response cache in Redis (needs redis_url)
  ledger_max_rows: 100000             # prune oldest billing rows past the cap; 0 = unlimited
```

The billing ledger, uploaded files, and batch jobs live here. In-memory by
default (lost on restart); a SQLite path makes them durable on one node.
`postgres_url` turns Postgres into the fleet backend: the source of truth for
config (versioned documents + a change feed every instance follows), the
shared access-key table, the shared ledger/files/batches store, and a
distributed batch queue (any instance claims and runs submitted batches).
`redis_url` shares rate/quota/TPM counters and account-health cooldowns across
instances; `shared_cache: true` additionally moves the request cache into
Redis so a hit on one instance serves the fleet (off = each instance caches
in-process, a miss just recomputes).

### `access_keys` — client authentication and per-key governance

```yaml
access_keys:
  - ak: ak-demo-123          # bearer / x-api-key value clients send
    product: demo            # product group (for product-level QPM)
    tenant: acme             # optional; absent = the unrestricted `default` tenant
    owner: alice             # optional; binds the key to one end user (authoritative
                             # for per-user attribution; a shared key omits it and
                             # falls back to the request's `x-gw-user` / `user`)
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
    model_prices:            # optional per-model charged-price override for this tenant
      gpt-4o: {input_price_per_1k_micros: 5000, output_price_per_1k_micros: 20000}
    user_daily_token_quota: 100000  # optional soft per-end-user daily cap
    security:                # optional; overrides the global `security:` WHOLE for this tenant
      blocklist: ["forbidden"]
      blocklist_action: flag        # block | flag | shadow
      detect_secrets: true
      regex_rules:
        - {name: ssn, pattern: '\d{3}-\d{2}-\d{4}', action: block}
    retention:               # optional prompt/response retention; absent = retain nothing
      content: redacted      # none | redacted | full  (full needs GW_CONTENT_KEY)
      days: 30               # purge after N days; 0 = keep until manually purged
```

Keys without a `tenant` join the implicit `default` tenant (no pooled limits,
entitled to every model), so a flat config keeps working unchanged. The model
catalog (`GET /v1/models`) filters to the caller's entitlement.

`user_daily_token_quota`, `security`, and `retention` are enterprise controls
detailed in [Governance](governance.md); `security` replaces the global policy
outright when present (it is not merged field-by-field).

### `models` — public model names and dispatch

```yaml
models:
  - name: gpt-4o                     # name clients request
    protocol: openai-chat            # wire protocol (or set `provider:` instead)
    input_price_per_1k_micros: 2500  # billing rates (micros per 1k tokens)
    output_price_per_1k_micros: 10000
    qpm: 60                          # optional model-level rate limit
    cache_ttl_seconds: 60            # optional request-level response cache
    token_rate:                      # optional per-component billing weights
      read_cache: 0.1                #   cache reads at 10% of the input price
      write_cache: 1.25              #   (prompt/completion/reasoning default 1.0)
    variants:                        # optional weighted canary split, sticky per user
      - {model: gpt-4o, weight: 90}  #   self-reference keeps a share here
      - {model: gpt-4o-next, weight: 10}
```

`token_rate` weights scale cost and quota consumption per token component; the
ledger's prompt/completion columns stay vendor-reported, while `total_tokens`
is the weighted platform total. `variants` splits a public name across other
declared same-protocol models (one level, no realtime): entitlement and the
per-(AK, model) daily counter judge the public name, billing prices the served
variant, and the response echoes the requested name. Selection hashes the
effective user, so a user sticks to one backend across the fleet.

### `providers` — first-class provider presets

```yaml
providers:
  - name: openai
    kind: openai              # openai | anthropic | gemini | deepseek | openrouter
    api_key_env: OPENAI_API_KEY
    # endpoint / timeout_seconds / connect_retries / secret_key_env may be
    # set here too and are inherited by the synthesized account
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
                               # timeout_seconds bounds a non-streaming request whole; a streaming
                               # one gets it on the headers and then per gap between chunks
    api_key_env: ""            # env var name holding the API key (never the key itself)
    secret_key_env: ""         # AWS only: env var of the secret key (api_key_env = access key id)
    cost_input_price_per_1k_micros: 100   # optional: what this vendor charges us (margin accounting)
    cost_output_price_per_1k_micros: 400
```

Secrets never live in config files: `api_key_env` names an environment
variable that is read per request. The optional `cost_*_price` fields record
what the vendor charges, so the ledger carries `vendor_cost_micros` alongside
the charged `cost_micros` and margin is queryable per tenant/model via
`GET /admin/usage`.

### `security`, `stability`, `products`

```yaml
security:                      # global default; a tenant may override it whole
  dlp_redact: true             # redact emails/phone numbers, both directions
  detect_secrets: true         # also mask API keys / credentials in inbound text
  blocklist: ["badword"]       # reject/flag requests containing listed terms
  blocklist_action: block      # block (deny) | flag (record) | shadow (trial a rule)
  regex_rules:                 # named recognizers, each with its own action
    - {name: ssn, pattern: '\d{3}-\d{2}-\d{4}', action: block}
  moderate: false              # route inbound text through the wired external moderator
  moderation_fail_open: false  # on a moderator error: admit (true) or deny (false)

stability:
  failure_threshold: 3         # consecutive failures before an account cools down
  cooldown_seconds: 300
  availability_window_minutes: 5   # /admin/models/status judgment window (max 60)
  unstable_error_rate: 0.1         # window error rate that reports `unstable`
  unavailable_error_rate: 0.5      # ... and `unavailable`
  availability_min_samples: 20     # fewer samples than this reports `no_data`

products:
  - name: myproduct
    qpm: 120                   # product-level request rate
```

Every rule that fires (block / flag / DLP / moderation) is recorded without the
prompt text to the security-event stream (`GET /admin/audit/events`). The same
policy runs on the realtime WebSocket, so it is not a bypass. `moderate` needs a
moderator wired into the handler — the default one allows everything. See
[Governance](governance.md#enterprise-content-policy).

### `admin` — the runtime-admin gate

```yaml
admin:
  token_env: GW_ADMIN_TOKEN    # env var holding the global admin bearer token;
                               # absent (and no tenant admin_token_env) = the whole
                               # /admin/* surface answers 404
```

The global token manages everything; a tenant's `admin_token_env` token is
scoped to that tenant (see [API — Admin](api.md#admin-dynamic-config)).

### Top-level flags

```yaml
trust_proxy_headers: false     # audit source IP: false = the real TCP peer (unforgeable);
                               # true = trust x-real-ip / rightmost x-forwarded-for hop
                               # (only behind a proxy that sets them)
```

## Observability

`GET /metrics` serves the Prometheus registry: `gateway_requests_total`
(route/status), `gateway_request_duration_seconds`,
`gateway_node_duration_seconds` (pipeline stage), `gateway_tokens_total`,
`gateway_cache_hits_total`, `gateway_ledger_write_failures_total`, and
`gateway_upstream_connect_retries_total` (account). One structured access
log line per successfully served request goes to stdout.

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
