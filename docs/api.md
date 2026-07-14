# API reference

All requests authenticate with an access key, sent either way:

```
Authorization: Bearer <ak>
x-api-key: <ak>
```

A missing or unknown key is `401`. Errors use a consistent envelope:

```json
{"error": {"message": "...", "code": "3002", "type": "gateway_error"}}
```

The Anthropic-compatible surface (`/v1/messages`) instead emits Anthropic's
error shape, so its SDKs can dispatch on it:

```json
{"type": "error", "error": {"type": "invalid_request_error", "message": "..."}}
```

## OpenAI-compatible

| Method | Path | Notes |
|--------|------|-------|
| POST | `/v1/chat/completions` | streaming + non-streaming |
| POST | `/v1/completions` | legacy text completion (`prompt`) |
| POST | `/v1/responses` | Responses API, streaming + non-streaming |
| POST | `/v1/embeddings` | |
| POST | `/v1/images/generations` | |
| POST | `/v1/images/edits` | source image + optional mask (base64) |
| POST | `/v1/audio/speech` | TTS, returns audio bytes |
| POST | `/v1/audio/transcriptions` | STT, JSON carries base64 audio |
| GET | `/v1/models` | configured public model names |

### Chat completions

```bash
curl -s localhost:8080/v1/chat/completions \
  -H 'authorization: Bearer ak-demo-123' -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}'
```

Set `"stream": true` for an SSE response. Frames arrive incrementally as the
upstream produces them; the final frame carries `usage` and `finish_reason`,
then `data: [DONE]`. Multimodal `content` arrays, `tools`/`tool_choice`, and
`tool_calls` responses are supported and passed through.

## Anthropic-compatible

| Method | Path | Notes |
|--------|------|-------|
| POST | `/v1/messages` | streaming + non-streaming |

`/v1/messages` works on both Anthropic-protocol models and OpenAI-protocol
models — the gateway converts between the two, including the streaming event
sequence (`message_start` → `content_block_*` → `message_delta` →
`message_stop`) and `stop_reason`/`finish_reason` mapping.

## Batch & files

| Method | Path | Notes |
|--------|------|-------|
| POST | `/v1/files` | upload JSONL: `{"purpose":"batch","file":"<content>"}` |
| GET | `/v1/files/{id}` | file metadata |
| GET | `/v1/files/{id}/content` | raw content |
| POST | `/v1/batches` | `{"input_file_id":"..."}` or inline `{"items":[...]}` |
| GET | `/v1/batches/{id}` | status (`pending`/`running`/`completed`/`failed`) + results |

Each JSONL line is `{"body": {"model": ..., "messages": [...]}}`. A batch runs
every item through the same pipeline as a live request (auth, quota, limits,
billing all apply per item).

Files and batches are owned by the uploading key's tenant. A file or batch
belonging to another tenant answers `404` (not `403`, so sequential ids can't be
probed for cross-tenant existence), and an `input_file_id` from another tenant
is rejected the same way.

## Realtime

`GET /v1/realtime` upgrades to a WebSocket; select the model with
`?model=<name>` (must be a realtime-family model). Authenticate with an
`Authorization: Bearer <ak>` header, or — for browser clients that cannot set
headers — a `gw-api-key.<ak>` entry in the `Sec-WebSocket-Protocol` list.

The session is refused at accept if the tenant is not entitled to the model.
A realtime model bound to an account with a real `endpoint` bridges the session
to that vendor's realtime WebSocket: a transparent relay, with the gateway
enforcing the same governance chain as the REST path per generation — tenant and
AK QPS, product/model QPM, per-(key, model) and daily-token quota, TPM — plus
billing (shared pricing) from the vendor's usage. Content security also applies:
the blocklist gates inbound frames and DLP redacts text fields in both
directions (per frame — a PII span straddling two deltas is beyond a relay
that cannot buffer). Each generation re-checks the
key, so a key banned, expired, or revoked (or a model de-entitled) mid-session
stops generating. An endpoint-less account serves a local mock session (OpenAI
Realtime event shape) for offline development.

## Introspection

| Method | Path | Notes |
|--------|------|-------|
| GET | `/health` | liveness |
| GET | `/metrics` | Prometheus registry (see [Observability](observability.md)) |
| GET | `/internal/ledger` | billing records; `?limit=N` pages (newest first, `count` is the total) |
| GET | `/internal/accounts` | account pool view with health |

`/internal/*` is an operator surface: keep it off the public load balancer
(the sample nginx config in [multi-instance](multi-instance.md) restricts it
to the operator network).

## Admin (dynamic config)

`/admin/*` lets operators change config at runtime without a redeploy. It is
disabled (routes 404) unless `admin.token_env` names an env var holding a bearer
token; every request must present `Authorization: Bearer <token>`. Keep the
surface on a private network regardless.

| Method | Path | Notes |
|--------|------|-------|
| POST | `/admin/reload` | re-read config from source and swap it in atomically (global token only) |
| PUT | `/admin/config` | validate + publish a new config document to the fleet config store; every instance reloads via the change feed (global token; needs `storage.postgres_url`) |
| GET | `/admin/keys` | list keys (a tenant token sees only its own tenant's) |
| POST | `/admin/keys` | create/replace a key: `{ak, product, tenant?, qps, daily_token_quota, tokens_per_minute?, expires_at_epoch_secs?, banned?, model_quotas?}` |
| PATCH | `/admin/keys/{ak}` | update any of `qps` / `daily_token_quota` / `tokens_per_minute` / `expires_at_epoch_secs` (null clears) / `banned` |
| DELETE | `/admin/keys/{ak}` | revoke a key |
| GET | `/admin/usage` | ledger rollup by tenant × model (requests, tokens, charged `cost_micros`, `vendor_cost_micros` for margin); `?tenant=` filter for the global token |

Two token tiers: the global token (`admin.token_env`) manages everything; a
tenant's `admin_token_env` token manages only that tenant's keys and usage
(cross-tenant keys answer 404, reload/config-publish answer 403).

A reload rebuilds the AK table (config keys), models, providers, tenants, and
accounts while preserving the runtime seams — governance counters, the durable
store, account health, and the response cache. Per-account upstream policy
(`timeout_seconds` / `connect_retries`) is pushed into the live transport, and
the response cache is invalidated (a reload may remap a model), so a published
change takes effect without a restart. Storage-backend URL changes
(`storage.postgres_url` / `redis_url` / `sqlite_path`) still need a restart.
Reload is also triggered by `SIGHUP` and, with the Postgres config store, by
any instance publishing via `PUT /admin/config`.

Keys have their own lifecycle: the config file's `access_keys` are the boot
baseline and are re-applied on every reload, while keys created via
`/admin/keys` survive reloads. With `storage.postgres_url` set the key table is
fleet-shared and persistent — a key created on one instance is valid on all
within ~2s and survives restarts.
