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
| Daily tokens | per access key | `access_keys[].daily_token_quota` (reset by a background task) |
| Daily tokens | per (key, model) | `tenants[].model_quotas` default, `access_keys[].model_quotas` override |
| TPM | per access key | `access_keys[].tokens_per_minute` |
| QPM | per model | `models[].qpm` |
| QPM | per product | `products[].qpm` |

Exceeding any limit returns `429`. QPS uses a smooth GCRA limiter in-process (a
fixed 1s window in Redis); the token/window counters are fixed windows. When
Redis is configured and unreachable, limits fail open (requests pass) and a
warning is logged — a persistent outage never silently wedges the gateway.

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

`security.dlp_redact` redacts emails and phone numbers from inbound and
outbound content; `security.blocklist` rejects requests containing listed terms
with a `content_filter` finish (not billed).

```yaml
security:
  dlp_redact: true
  blocklist: ["badword"]
```
