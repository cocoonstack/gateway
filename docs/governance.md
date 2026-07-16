# Governance

Per-request controls run as pipeline stages before and after the engine call.
All limits are enforced in-process by default; set `storage.redis_url` to share
them across replicas (see [Deployment](deployment.md)).

## Access keys

Each `access_keys` entry is one credential with its own governance:

```yaml
access_keys:
  - ak: ak-demo-123
    product: demo            # product group (for product-level QPM)
    tenant: acme             # optional tenant membership
    qps: 100                 # per-key request rate
    daily_token_quota: 1000000
    tokens_per_minute: 600   # optional TPM window
    expires_at_epoch_secs: 1767225600  # optional; expired keys 403
    banned: false            # optional; banned keys 403
    model_quotas: {gpt-4o: 200000}     # optional per-model daily caps
```

## Tenants

A tenant groups keys under shared governance: one pooled QPS bucket for all
its keys, a model entitlement allowlist (unlisted models 403 and disappear
from `GET /v1/models`), per-model daily-token quota defaults (each key metered
separately against the same value; per-key `model_quotas` override), and an
optional `fallback_model` — an over-quota request degrades to it instead of
failing (the response echoes the requested model name; the ledger records both
requested and served). The per-key daily cap stays the hard backstop, and
unconfigured (key, model) pairs never touch a counter.

## Limits

| Limit | Scope | Config |
|-------|-------|--------|
| QPS | per access key | `access_keys[].qps` |
| QPS | pooled per tenant | `tenants[].qps` |
| Daily tokens | per access key | `access_keys[].daily_token_quota` (fleet/Redis: rolls at UTC midnight; single-node in-memory: a ~daily background reset) |
| Daily tokens | per (key, model) | `tenants[].model_quotas` default, `access_keys[].model_quotas` override |
| TPM | per access key | `access_keys[].tokens_per_minute` |
| QPM | per model | `models[].qpm` |
| QPM | per product | `products[].qpm` |

Exceeding any limit returns `429`. QPS uses a smooth GCRA limiter in-process (a
fixed 1s window in Redis); the token/window counters are fixed windows. When
Redis is configured and unreachable, limits fail open (requests pass) and a
warning is logged — a persistent outage never silently wedges the gateway.

Daily-token and TPM admission **reserve then settle**: on admission a cheap
estimate (prompt heuristic + requested `max_tokens`) is reserved atomically, so
concurrent in-flight requests count against the budget instead of all passing a
stale check and jointly overshooting. Billing settles the reservation to actual
usage; a failed request refunds it. Charged price is the model's list price, or
a tenant's `model_prices` override; when an account declares `cost_*_price` the
ledger also records the vendor cost, so margin is queryable via `/admin/usage`.

A streaming response that breaks after delivery has begun (client disconnect or
upstream failure) is billed for what was delivered: the vendor's usage frame
never arrives, so the token count is estimated from the request and the
delivered text. A disconnect *before* any bytes are sent bills nothing.

## Request cache

A model with `cache_ttl_seconds` set caches non-streaming responses for that
TTL (bounded, moka-backed). A cache hit is **free**: it short-circuits account
selection, the engine call, and billing/quota — a hit consumes no quota and
writes no ledger record. Offline batch items bypass the cache entirely (read
and write) so per-item billing stays accurate.

```yaml
models:
  - name: cached-mini
    protocol: openai-chat
    cache_ttl_seconds: 60
```

## Content safety

`security.dlp_redact` redacts emails and phone numbers from inbound content
(chat messages, the Responses body, and the family typed params) and from the
outbound message; `security.blocklist` rejects requests containing listed terms
with a `content_filter` finish (not billed).

```yaml
security:
  dlp_redact: true
  blocklist: ["badword"]
```

Outbound redaction needs the whole message (a masked span may straddle two SSE
deltas), so **with `dlp_redact` enabled a streaming response is buffered and the
redacted text replayed** rather than forwarded token-by-token — DLP trades
incremental delivery for a guarantee that no unmasked text reaches the client.
Turn `dlp_redact` off to keep incremental delivery; note the embedded demo
config ships with it on.

## Per-user attribution and billing

Every ledger row carries a `user_id`, `request_id`, and `created_at_epoch_secs`.
The effective user is the key's `owner` (one key = one user, the enterprise
model) if set, else request metadata — the `x-gw-user` header, OpenAI's `user`
field, or Anthropic's `metadata.user_id`. `owner` is authoritative; the
metadata hint is only trusted for shared keys. This holds on every surface —
REST, realtime (the `x-gw-user` hint is captured at WS connect), and batch
(each item's `user`, or the submitter's `x-gw-user`, is persisted with the item
so a fleet drainer still attributes and budgets it). `GET /admin/usage/users?user=&since=&until=`
returns per-(user, model) cost over a billing period (add `format=csv` for
export). `TenantConf.user_daily_token_quota` sets a soft per-user daily cap.

A background task folds completed ledger minutes into durable per-(minute,
tenant, user, model) rollup buckets (recomputing a trailing 20-minute window
each pass, so late rows and missed ticks self-heal). Usage queries are served
from those buckets plus the raw ledger tail, so per-user cost stays correct
after `storage.ledger_max_rows` prunes old billing rows — size the cap to hold
at least the backfill window of traffic. Once a period is served from buckets,
its `since`/`until` bounds are minute-aligned.

## Enterprise content policy

`security:` is global by default; a tenant may override it whole with
`tenants[].security`. Beyond the blocklist and DLP, a policy can carry
`blocklist_action` (`block` denies, `flag` records, `shadow` trials a rule),
named `regex_rules` (each with its own action), and `detect_secrets` (masks API
keys / credentials). Every rule that fires is recorded — without the offending
text — to the security-event stream, queryable at `GET /admin/audit/events`.
Set `moderate: true` to route inbound text through an external moderator wired
into the handler (`moderation_fail_open` picks the posture on a moderator
error).

The same policy applies on every surface, realtime included: a `/v1/realtime`
WebSocket runs the blocklist, regex rules, DLP redaction, and the external
moderator on inbound frames, so it is not a bypass. Every hit is audited: inbound
block/flag/moderation and DLP redactions are recorded per frame; outbound DLP
redactions (which stream token-by-token) are summed across the turn and recorded
as one event at the turn boundary — the redaction still applies to every frame,
only its audit is aggregated (a store write per token would be too hot).

## Audit trails

- **Content-safety events** (`/admin/audit/events`): who/which-rule/what-action,
  no prompt text; tenant-scoped.
- **Admin operations** (`/admin/audit/ops`, global admin only): every key CRUD,
  config publish, and reload with actor, action, target, and source IP. A
  config publish is recorded even when the local reload fails, since the version
  is already the fleet's source of truth. The source IP is the real TCP peer;
  set `trust_proxy_headers: true` (top level) to instead trust `x-real-ip` /
  `x-forwarded-for` — do that ONLY behind a proxy that sets them, or a direct
  client could forge the recorded IP.
- **Content retention** (`tenants[].retention`): `none` (default), `redacted`
  (PII/secrets stripped), or `full` (raw). Stored in `request_content`, sealed
  at rest with XChaCha20-Poly1305 under `GW_CONTENT_KEY` (64 hex chars).
  Retention owns its redaction, so `redacted` — and a keyless `full` that falls
  back to it — never persists raw secrets/PII even if the tenant forwards
  traffic with DLP off. `full` refuses to store raw without a key. `days` sets
  expiry; an hourly purge deletes elapsed content. Read back with
  `GET /admin/audit/content/{request_id}` (tenant-scoped; sealed rows are
  unsealed when the key is present, else returned as `content: null`).
