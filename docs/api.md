# API reference

All requests authenticate with an access key, sent either way:

```
Authorization: Bearer <ak>
x-api-key: <ak>
```

A missing or unknown key is `401`. Every error carries one classification
from a closed, Bedrock-derived set, on two machine channels: the
`x-amzn-errortype` response header (PascalCase name, e.g.
`ThrottlingException`) and the body's `code` field (snake_case, e.g.
`throttling_exception`). The envelope shape follows the surface — OpenAI
surfaces use the OpenAI error object:

```json
{"error": {"message": "...", "type": "rate_limit_error", "param": null, "code": "throttling_exception"}}
```

The Anthropic-compatible surface (`/v1/messages`) emits Anthropic's error
shape, so its SDKs can dispatch on it (`code` is additive):

```json
{"type": "error", "error": {"type": "rate_limit_error", "code": "throttling_exception", "message": "..."}}
```

A terminal upstream failure (account failover and the model's
`fallback_models` chain exhausted) is `424` with
`code: "model_error_exception"`, plus `original_status_code` (when the
last upstream tried returned a status) and `resource_name` (the requested
model) inside the error object. Retry on 408/429/500/503 with backoff (honor
`retry-after`); never on the rest. Mid-stream failures arrive as a terminal
SSE error frame carrying the same `code` field (`model_stream_error_exception`
for a generic upstream break after the stream committed). An unknown route
answers the envelope's `404`; a wrong method on a known route is a `400`
`validation_exception`, not a `405`.

For per-user attribution on a shared key, send `x-gw-user: <id>` (it also reads
OpenAI's body `user` field and Anthropic's `metadata.user_id`). A key's own
`owner` overrides the hint, so a key issued to one user always bills to that
user. See [Governance](governance.md#per-user-attribution-and-billing).

## OpenAI-compatible

| Method | Path | Notes |
|--------|------|-------|
| POST | `/v1/chat/completions` | streaming + non-streaming |
| POST | `/v1/completions` | legacy text completion (`prompt`) |
| POST | `/v1/responses` | Responses API, streaming + non-streaming; the body (`reasoning`, `include`, reasoning items) and the vendor's event stream pass through verbatim; a `responses` model reached from `/v1/chat/completions` or `/v1/messages` gets its Responses body built from the normalized turns (`input` items, `instructions`, `function_call`/`function_call_output`, `max_output_tokens`, flattened tools, `reasoning.effort` from an effort or a thinking budget, and `store: false` unless the client sets it, as on Chat Completions) and streams as that surface's own frames — image parts and `response_format` do not cross onto that wire; a model on any other wire is not served from `/v1/responses`, whose body has no normalized turns |
| POST | `/v1/embeddings` | |
| POST | `/v1/images/generations` | |
| POST | `/v1/images/edits` | source image + optional mask (base64) |
| POST | `/v1/videos/generations` | `{model, prompt, duration?, aspect_ratio?, resolution?, image?}`, mapped to the account's dialect; a synchronous vendor answers with the video, an async one with its handle |
| GET | `/v1/videos/{id}` | the vendor's poll, proxied in its own dialect; see Video |
| GET | `/v1/videos/{id}/content` | the finished clip's bytes, proxied (Sora and Hailuo) |
| POST | `/v1/audio/speech` | TTS, returns audio bytes |
| POST | `/v1/audio/transcriptions` | STT, JSON carries base64 audio: `{"model":"...","audio_b64":"<base64>","language":"en"}` |
| POST | `/v1/audio/translations` | STT translated to English (same request shape) |
| POST | `/v1/moderations` | content moderation; `input` string or array, native results pass through |
| GET | `/v1/models` | configured public model names |

A typed surface serves the model its protocol names: naming a model of another
protocol answers `400 "`<model>` is not a <surface> model"`, the same shape the
realtime upgrade uses, and the request is refused before dispatch so no engine
builds a body for it and no upstream call is made. Two pairings are not identity
and are served normally: an `aws-embed` model answers `/v1/embeddings`, and
`/v1/completions` carries its prompt as a turn, so any wire answers it.

## Rerank

| Method | Path | Notes |
|--------|------|-------|
| POST | `/v1/rerank` | Cohere/Jina-compatible: `{model, query, documents, top_n?}` → `{results: [{index, relevance_score}]}` |

## Search

| Method | Path | Notes |
|--------|------|-------|
| POST | `/v1/search` | web search as a routed backend: `{model, query, count?}` (`count` defaults to 3, clamped to 1-20); a `brave` provider speaks the Brave Search API (the vendor body passes through), each search bills one unit at the model's `unit_price_micros` |

### Chat completions

```bash
curl -s localhost:8080/v1/chat/completions \
  -H 'authorization: Bearer ak-demo-123' -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}'
```

Set `"stream": true` for an SSE response. Frames arrive incrementally as the
upstream produces them; the final frame carries `usage` and `finish_reason`,
then `data: [DONE]`. Multimodal `content` arrays, `tools`/`tool_choice`, and
`tool_calls` responses are supported and passed through. Vendor fields on a
call pass through both ways, so a client that echoes the assistant turn keeps
the `extra_content.google.thought_signature` Gemini 3 requires. On an
`anthropic-messages` model the reply's `tool_use` blocks render as
`tool_calls` (streamed with `index`) and `stop_reason` maps to `finish_reason`;
on a `responses` model `tools` and `tool_choice` are flattened into the
Responses shape.

### Reasoning

Ask for reasoning the OpenAI way (`reasoning_effort`: `none` | `minimal` |
`low` | `medium` | `high` | `xhigh` | `max`) or the OpenRouter way
(`reasoning: {effort, max_tokens, enabled}`); a vendor's own knob in the body
(`thinking`, `enable_thinking`, …) passes through untouched and wins. The
mapping per model family:

| Family | Request | Response |
|--------|---------|----------|
| OpenAI / compatible | `reasoning_effort` forwarded; `max_tokens` becomes `max_completion_tokens` when reasoning is engaged; an Anthropic-dialect budget (`thinking.budget_tokens`, OpenRouter `max_tokens`) maps to the nearest tier — 1024 `low`, 4096 `medium`, 16384 `high`, 24576 `xhigh`, 32768 `max` — and vendors accept different subsets (live: gpt-5-mini `minimal`–`high`, gpt-5.4-mini `none`–`xhigh`; past the last tier the vendor answers 400). An OpenAI id OpenAI serves itself is clamped to the tiers its generation takes on that wire, since either end is a 400: no generation takes `max` on chat completions (5.0/5.1 stop at `high`, 5.2 on at `xhigh`), Responses takes it from 5.6 on, GPT-6 Astra and the o-series cannot turn reasoning off so `none`/`minimal` become `low` (GPT-6 Sol and Luna take `none`; the o-series stop at `xhigh` on chat and `high` on Responses), and 5.0 knows `minimal` but not `none`. An Anthropic `thinking: {type: disabled}` becomes effort `none` for OpenAI's own reasoning families, clamped the same way, and is left out for every other vendor. The clamp runs on the assembled body, so a native `/v1/responses` passthrough gets it too. A vendor-prefixed id keeps its ceiling — OpenRouter (`openai/gpt-…`) normalizes tiers itself, and Bedrock (`openai.gpt-…`) takes `max` from every generation through `reasoning_config` — but a `gpt-6-astra` id anywhere still turns `none`/`minimal` into `low`, and Bedrock never takes `minimal` | `reasoning_content` / `reasoning` string and `reasoning_details` units forwarded |
| Anthropic ≤ 4.5 | `thinking: {type: enabled, budget_tokens}` — fixed budget per effort level (`low` 1024, `medium` 4096, `high` 16384, `xhigh` 24576, `max` 32768), `max_tokens` topped up by the budget | thinking blocks → `reasoning_content` + `reasoning_details` |
| Anthropic 4.6+ | `thinking: {type: adaptive}` + `output_config.effort` (`display: summarized` from 4.7 on; `xhigh` clamps to `high` on 4.6, which predates it); `temperature` / `top_p` / `top_k` are dropped for 4.7+, which rejects them | same |

A request without `max_tokens` gets 1024 on the Anthropic wire, or 16384 on the
models that think by default (the 5 family, Fable, Mythos), and the thinking
budget tops the cap up only there; a non-Claude model on Bedrock Converse gets
no cap unless the client sets one.

Sampling knobs the client sent along a gateway-mapped effort (`temperature`,
`top_p`, `top_k`) are dropped for Anthropic, which rejects them with thinking on.
OpenAI refuses `temperature` other than its default, `top_p` and both penalties
for exactly as long as a request reasons — every generation from 5.1 on takes
them at effort `none` or with no effort at all (GPT-6 Sol and Luna, which reason
by default, only at `none`), while 5.0, GPT-6 Astra and the o-series take them never,
since none of them can turn reasoning off — so the gateway drops those four once the
request reasons, on chat, Responses and a native Responses body alike. Bedrock
refuses `temperature`/`topP` for an `openai.gpt-<n>` or `xai.grok-<n>` id whatever
the effort, its own validation rather than the model's, so they drop from
`inferenceConfig` unconditionally there. `logprobs`, `top_logprobs` and `stop`, which those models also
refuse, are left in: dropping them would silently withhold data the client asked
for or move where generation stops, so the vendor's own 400 says so instead.
OpenRouter swallows every one of these itself, so its ids keep what the client
sent.

The reply carries the reasoning prose as `message.reasoning_content` and its
units as `message.reasoning_details` — `reasoning.text` (with `signature`
when Anthropic signed it), `reasoning.encrypted` (redacted thinking), each
tagged `format: "anthropic-claude-v1"`, plus a compatible vendor's own units
verbatim. Streams carry the same two fields in `delta` (prose as it arrives,
each unit once complete). Replay the assistant message as received: signed
units become Anthropic thinking blocks ahead of the turn (unsigned prose is
dropped — the vendor rejects it), and go to OpenAI-compatible vendors as
`reasoning_content` / `reasoning_details`. Requests that engage reasoning or
replay signed units are pinned to their requested model (see [Extended
thinking](#extended-thinking)); `usage.completion_tokens_details.reasoning_tokens`
reports the reasoning share when the vendor does, and
`usage.prompt_tokens_details` carries `cached_tokens` (cache reads) plus
`cache_write_tokens` — OpenAI's own name, reported for Anthropic-family writes
too. Cache writes ride inside `prompt_tokens` on this wire, so a client can
reconcile the write premium; the Responses surface reports the same two under
`input_tokens_details`.

## Anthropic-compatible

| Method | Path | Notes |
|--------|------|-------|
| POST | `/v1/messages` | streaming + non-streaming; the client's `anthropic-beta` header is forwarded to Anthropic-wire upstreams (as the `anthropic_beta` body list on Bedrock) |

`/v1/messages` works on both Anthropic-protocol models and OpenAI-protocol
models — the gateway converts between the two, including the streaming event
sequence (`message_start` → `content_block_*` → `message_delta` →
`message_stop`) and `stop_reason`/`finish_reason` mapping. Tools convert with
the wire: `input_schema` becomes `parameters`, `tool_choice` `{type: any}` /
`{type: tool, name}` become `required` / `{type: function}`,
`disable_parallel_tool_use` and `parallel_tool_calls` map onto each other in
both directions between OpenAI and Messages wires (`anthropic-messages` /
`aws-anthropic`). On Converse a Claude model takes the policy as a whole
`tool_choice` in `additionalModelRequestFields` (Bedrock refuses the flag next
to `toolConfig.toolChoice`); other families keep `parallel_tool_calls` there
for the model to accept or reject. Converse has no `tool_choice: none`: the
gateway drops `toolConfig` for it. Once the conversation carries
`tool_use`/`tool_result` turns Bedrock requires the tools: a Claude model keeps
them and takes the `none` through `additionalModelRequestFields`; any other
family gets Bedrock's 400, because keeping the tools would let the model call
one the client disabled.
The chat surface's top-level `user` never reaches an Anthropic wire, which
rejects it; every other unrecognized field still passes through as sent. A reply that carries `tool_use` blocks
reports
`stop_reason: tool_use` even where the vendor said `stop` (OpenAI does for a
forced tool). On an OpenAI-protocol
model, `thinking` (a budget, or `output_config.effort`) becomes
`reasoning_effort` and the model's reasoning prose comes back as an unsigned
`thinking` block ahead of the answer (streamed as `thinking_delta`); replaying
it sends the prose on as `reasoning_content`.

### Extended thinking

On an `anthropic-messages` model, `/v1/messages` preserves signed
`thinking`/`redacted_thinking` blocks natively — non-streaming content and the
streaming event sequence (`thinking_delta`/`signature_delta`) pass through
unmodified, including from compatible upstreams that ignore `stream: true`.

Requests that engage reasoning on any surface (`thinking: {"type": "enabled"
| "adaptive"}`, a `reasoning_effort`, or a continuation carrying signed
blocks or `reasoning_details`) are pinned to their requested model: over-quota
fallback and moderation degrade will not move them, because a signature only
replays against the model that produced it. A variant split is sticky instead:
per user id when the request carries one, else per conversation, keyed on the
first user turn that a signed replay keeps verbatim, so every turn lands on the
variant that produced the reasoning, including on a model that thinks without
being asked. Only a reasoning continuation with neither (a native Responses
`input`) stays pinned to the requested model.

Tool-loop continuations are audited against what the gateway served for the
same key, model, and tool id within the last ten minutes: a modified protected
sequence is rejected locally (400) before reaching the vendor; unknown or
expired anchors fail open. Disabling thinking on a continuation and stripping
the blocks — as the Anthropic API requires — is accepted. Signed thinking
whose prose the tenant's policy cannot serve (blocklist or DLP hits) is
stripped from the response; the visible turn still serves.

## Video

`POST /v1/videos/generations` runs the pipeline like any family (auth, limits,
routing, a ledger row) and returns the vendor's reply as is. The wire follows
the serving account's provider label when it names a dialect, else its preset kind: `openai` speaks Sora's `/v1/videos`
(`seconds`, `size`, a video object back, the finished clip via
`GET /v1/videos/{id}/content`), `siliconflow` Wan's `video/submit` +
`video/status`, `alibaba`/`dashscope` the DashScope task API (async header,
`output.task_id`, poll `/api/v1/tasks/{id}`), `minimax` Hailuo's
`video_generation` + `query/video_generation` + file content, `kling` Kling's
`videos/text2video` (`model_name`, string durations, a `{code, data}` envelope),
and anything else the generic `videos/generations` shape. Each dialect forwards
only the fields its vendor takes (`resolution` never reaches Kling; Wan takes no
duration).

When the reply is an async handle, the gateway remembers which key, model and
account it belongs to; `GET /v1/videos/{id}` spends the polling key's rate
limits like any request, then proxies the vendor's poll on that account (`404`
for an unknown id or another tenant's). The first poll, including the poll that
precedes a content download, that reaches the dialect's done state bills the
submitting key the clip's whole seconds when the vendor reports a duration
(Sora's `seconds`, xAI's `video.duration`, DashScope's
`usage.video_duration`), else one unit per delivered video (Wan, Hailuo) — at
the `unit_price_micros` quoted at submit (a reprice or removal of the model
while the clip renders does not change it), taking the vendor cost from xAI's
`usage.cost_in_usd_ticks` (1 tick = 10⁻¹⁰ USD) when present; that ledger row's
`request_id` is the video id. Later polls and downloads do not bill again;
failed and expired jobs bill nothing beyond the submit row. Jobs are kept 30
days.

## MCP

| Method | Path | Notes |
|--------|------|-------|
| POST / GET / DELETE | `/mcp/{server}` | Model Context Protocol (Streamable HTTP) proxy to the configured `mcp_servers[]` entry: the JSON-RPC message goes up with `Accept`, `Content-Type`, `Mcp-Session-Id`, `MCP-Protocol-Version` and `Last-Event-ID`, the server's static bearer or an OAuth access token the gateway fetched (nothing when the server declares neither) is attached upstream, and `Content-Type` and `Mcp-Session-Id` come back; replies stream back as the server sends them unless the key's tool allowlist filters a `tools/list` or the tenant's `security.moderate` reviews a result, which buffer the reply whole (up to `max_reply_bytes`); `timeout_seconds` bounds POST and DELETE, the GET listen stream is unbounded in time and counted against `max_live_streams_per_key` (and refused for a tenant under `security.moderate`) |

The access key rides as usual (`Authorization: Bearer` or `x-api-key`); a
server the key is not entitled to (`access_keys[].mcp_servers`) answers like an
unknown one (`404`), so server names cannot be probed. When the key has an
allowlist for that server (`access_keys[].mcp_tools`), `tools/list` results are
filtered to it and a `tools/call` outside it answers a JSON-RPC error (`-32000`)
without reaching the server; a `tools/call` without a string `params.name` is
a `400`. Every `tools/call` — served or denied — is a `mcp` security event
(`rule = mcp:<server>`, `action = call:<tool>` / `deny:<tool>`), written before
the call is forwarded so a dropped connection cannot erase it. `Mcp-Session-Id`
is bound to the key that first received it; another key presenting it gets
`404`. When the key's tenant sets `security.moderate`, a served `tools/call`,
`resources/read` or `prompts/get` result is buffered (up to the server's
`max_reply_bytes`) and every prose field of it reviewed by the configured
moderator before it reaches the client — regardless of the reply's HTTP status,
and including a JSON-RPC error's own message. A mask rewrites the text in
place, a denial — or a reply the gateway could not parse — replaces the whole
reply with one JSON-RPC error (`-32001`, the moderator's reason), and either
lands as a `mcp` security event (`rule = moderation`, `action = mask` /
`block`). Because the GET listen stream carries server-pushed content the proxy
cannot review, a reviewed tenant may not open one (`403`); other tenants may,
and for them `Last-Event-ID` is still not forwarded. Listen streams count
against `max_live_streams_per_key`. JSON-RPC batches are refused (400); the
key's QPS and the tenant's pooled QPS apply. A server declared with `oauth` is
called with an access token the gateway fetches from the server's token
endpoint; a `401` from the server fetches a fresh token and retries the call
once. Upstream failures answer a generic `502`; the server's endpoint and the
identity provider's error text stay in the gateway log.

## Batch & files

| Method | Path | Notes |
|--------|------|-------|
| POST | `/v1/files` | upload JSONL: `{"purpose":"batch","file":"<content>"}` |
| GET | `/v1/files/{id}` | file metadata |
| GET | `/v1/files/{id}/content` | raw content |
| DELETE | `/v1/files/{id}` | delete an uploaded file (tenant-owned) |
| POST | `/v1/batches` | `{"input_file_id":"..."}` or inline `{"items":[...]}`; answers `202` with `{id, status, total}` |
| GET | `/v1/batches/{id}` | status (`pending`/`running`/`completed`/`failed`) + results `{index, ok, message, total_tokens, finish_reason?, tool_calls?}` (`finish_reason` is absent for an item that failed before it produced an outcome) |

Each JSONL line is `{"body": {...}}` and each inline item is a
`/v1/chat/completions` request body. Every item runs on the batch's `model`, or
on the first JSONL line's when the batch names none. A batch runs every item
through the same pipeline as a live request (auth, quota, limits, billing all
apply per item, and the submission itself spends one request against the key
QPS, tenant QPS and product QPM); an item a content rule blocks reports
`ok: false` with `finish_reason: "content_filter"`. Attribution inverts the
REST precedence: a per-item `user` field wins over the connection's `x-gw-user`
header, so a shared-key batch keeps per-item attribution.

Files and batches are owned by the uploading key's tenant. A file or batch
belonging to another tenant answers `404` (not `403`, so sequential ids can't be
probed for cross-tenant existence), and an `input_file_id` from another tenant
is rejected the same way.

## Realtime

`GET /v1/realtime` upgrades to a WebSocket; select the model with
`?model=<name>` (must be a realtime-family model). Authenticate with an
`Authorization: Bearer <ak>` header, or — for browser clients that cannot set
headers — a `gw-api-key.<ak>` entry in the `Sec-WebSocket-Protocol` list.

The session is refused at accept if the tenant is not entitled to the model,
or with `429` when the key already holds `max_live_streams_per_key` realtime
sessions and MCP listen streams.
A realtime model bound to an account with a real `endpoint` bridges the session
to that vendor's realtime WebSocket: a transparent relay, with the gateway
enforcing the same governance chain as the REST path per generation — tenant and
AK QPS, product/model QPM, per-(key, model) and daily-token quota, TPM — plus
billing (shared pricing) from the vendor's usage. The full content policy also
applies, so the WebSocket is not a bypass: the blocklist, regex recognizers, and
(when enabled) the external moderator gate inbound frames, and DLP — emails,
phone numbers, and credential masking — redacts text fields in both directions
(per frame — a PII span straddling two deltas is beyond a relay that cannot
buffer). Every hit is audited without prompt text; per-user attribution comes
from the `x-gw-user` hint captured at connect. A client frame the gateway
cannot parse as JSON answers an in-band `validation_exception` and is not
relayed, since no control could read it. Each generation re-checks the
key: a key banned, expired, suspended or revoked mid-session gets its
`access_denied_exception` and the session closes, and a model de-entitled
mid-session stops generating. If a turn delivers output but disconnects before its usage
boundary, the delivered text or audio is billed from an estimate; a turn that
delivered nothing is refunded. An endpoint-less account serves a local mock
session (OpenAI Realtime event shape) for offline development.

The wire follows the account's preset kind (else its provider label): a
`gemini` account bridges Google's Live API socket (binary frames relayed
as-is, the key on the query string, `setup.model` rewritten to the entitled
served model) and admits each turn on `clientContent.turnComplete` — the
dialect's own generation signal — settling the `usageMetadata` that rides the
completed turn; `realtimeInput` audio turns and a second
`clientContent` during an active generation answer an in-band error until they
have an admission point. Every other account speaks the OpenAI Realtime shape
and admits on `response.create`.

## Introspection

| Method | Path | Notes |
|--------|------|-------|
| GET | `/health` | liveness |
| GET | `/metrics` | Prometheus registry (see [Observability](observability.md)) |
| GET | `/internal/ledger` | billing records; `?limit=N` returns the N most recent, default 100 (oldest-first within the page; `count` is the total); global admin token only |
| GET | `/internal/accounts` | account pool view with health; global admin token only |

`/internal/*` is an operator surface: it answers only to the global admin
bearer (`admin.token_env`; 404 while no admin token at all is configured, 403 to a tenant token), and the raw rows
span every tenant. Keep it off the public load balancer regardless (the sample
nginx config in [multi-instance](multi-instance.md) restricts it to the
operator network).

## Admin (dynamic config)

`/admin/*` lets operators change config at runtime without a redeploy. It is
disabled (routes 404) unless a token is configured — the global `admin.token_env`
or at least one tenant's `admin_token_env`; every request must present
`Authorization: Bearer <token>`. Keep the surface on a private network
regardless.

| Method | Path | Notes |
|--------|------|-------|
| POST | `/admin/reload` | re-read config from source and swap it in atomically (global token only) |
| GET | `/admin/config` | current fleet config version and raw YAML (global token; needs `storage.postgres_url`) |
| POST | `/admin/config/validate` | validate a config document without publishing it (global token) |
| PUT | `/admin/config` | validate + publish a new config document to the fleet config store; every instance reloads via the change feed; `?expected_version=` publishes only while that is still the head — a moved head answers 409 (global token; needs `storage.postgres_url`) |
| GET | `/admin/config/versions` | retained config versions, newest first (the store keeps the newest 20); `?limit=` (default 20) (global token; needs `storage.postgres_url`) |
| POST | `/admin/config/versions/{id}/rollback` | republish a retained document as a new head and reload (global token; needs `storage.postgres_url`) |
| GET | `/admin/keys` | list keys with computed `status` / `available`, `?offset=&limit=` paged (default 200, every listing caps `limit` at 10 000; a tenant token sees only its own tenant's); `?ak=` exact lookup answers a 0/1-key page — a foreign key is an empty page, never a 404 oracle |
| POST | `/admin/keys` | create/replace a key: `{ak, product, tenant?, owner?, qps, daily_token_quota, tokens_per_minute?, expires_at_epoch_secs?, banned?, model_quotas?}` (`owner` binds the key to one end user — authoritative for attribution) |
| PATCH | `/admin/keys/{ak}` | update any of `qps` / `daily_token_quota` / `tokens_per_minute` / `expires_at_epoch_secs` (null clears) / `banned` / `suspended_until_epoch_secs` (null lifts an abuse suspension early); a tenant token may set `banned: true` but can neither lift a ban nor touch `suspended_until_epoch_secs` (403) |
| DELETE | `/admin/keys/{ak}` | revoke a key |
| GET | `/admin/usage` | ledger rollup by tenant × model (requests, tokens, charged `cost_micros`, `vendor_cost_micros` for margin); `?tenant=` filter for the global token; tenant-scoped — a tenant token reads `vendor_cost_micros` as 0 |
| GET | `/admin/usage/users` | per-user cost rollup (user × model) over a billing period: `?since=&until=` (unix secs), `?user=` filter, `?format=csv` export; tenant-scoped — a tenant token reads `vendor_cost_micros` as 0 (operator-only margin basis) |
| GET | `/admin/usage/series` | bounded dashboard series: `?bucket=hour|day&since=&until=&user=`; `?tenant=` filter for the global token; tenant-scoped (vendor cost redacted like `/admin/usage/users`), maximum 400 points |
| GET | `/admin/models/status` | per-model availability over the recent window (`available` / `unstable` / `unavailable` / `no_data`), judged from client-visible outcomes against `stability.*` thresholds; attributes to the requested public name under a `variants` split; realtime models sample per vendor-metered turn (a turn billed from an estimate is not sampled) and on session-fatal upstream errors; tenant-scoped |
| GET | `/admin/audit/events` | content-safety hits (blocklist / regex / DLP / moderation) recorded without prompt text; `?limit=`; tenant-scoped |
| GET | `/admin/audit/ops` | admin-operation trail (key CRUD, config publish, reload) with actor, target, and source IP; `?limit=`; global token only |
| GET | `/admin/audit/content/{request_id}` | retained prompt/response and terminal result for one request, unsealed when `GW_CONTENT_KEY` is set (sealed rows without it return `content: null`); tenant-scoped |
| GET | `/admin/audit/content?user=` | retained rows for one attributed end user, newest first; metadata only by default, `?include=bodies` inlines content; `?limit=` (default 200, max 1000); tenant-scoped |
| DELETE | `/admin/audit/content?user=` | erase all retained content for one end user — retained rows, batch result messages, leftover batch inputs (GDPR/PIPL); tenant-scoped, audited atomically as `content_erase` |

For a tenant with prompt/response retention enabled, each completed request
attempts to add one `kind: "terminal"` row. A non-streaming row is written after
the HTTP view has rendered its final status; a streaming row is written after
the detached pipeline settles. Its content is a small JSON object:
`state` (`success`, `error`, or `client_closed`), `http_status`, and
`stream_committed`; an error also carries the external `code` and, when an
upstream HTTP reply supplied one, `original_status_code`. The row is written
after request accounting settles, contains no provider message or user content,
and is first-writer-wins for `(tenant, user_id, request_id)`. A committed stream
can deliver its error frame just before this row becomes visible. Retention is
best-effort: absence after bounded polling remains unknown (for example, a
store or process failure) and must not be interpreted as success.
A terminal row reports the request outcome only; optional prompt/response rows
remain best-effort and may be absent.

Two token tiers: the global token (`admin.token_env`) manages everything; a
tenant's `admin_token_env` token manages only that tenant's keys, usage, and
content-safety events, scoped to its own tenant (cross-tenant keys answer 404;
reload, config-publish, and the cross-tenant `/admin/audit/ops` trail answer
403).

A reload rebuilds the AK table (config keys), models, providers, tenants, and
accounts while preserving the runtime seams — governance counters, the durable
store, account health, and the response cache. Per-account timeout/connect
policy is refreshed in the live transport; `retry_status` stays on the selected
account snapshot so an in-flight request cannot borrow another vendor's replay
permission. The response cache is invalidated (a reload may remap a model), so
a published change takes effect without a restart. Storage-backend URL changes
(`storage.postgres_url` / `redis_url` / `sqlite_path`) still need a restart.
Reload is also triggered by `SIGHUP` and, with the Postgres config store, by
any instance publishing via `PUT /admin/config`.

Keys have their own lifecycle: the config file's `access_keys` are the boot
baseline and are re-applied on every reload, while keys created via
`/admin/keys` survive reloads. With `storage.postgres_url` set the key table is
fleet-shared and persistent — a key created on one instance is valid on all
within ~2s and survives restarts.
