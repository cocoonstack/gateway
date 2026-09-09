# Security model

What the gateway trusts, what it checks on every request, what it records,
and what an operator must put around it. Every statement here names the
config or code path that enforces it; the review that produced this page and
its findings are summarized at the end.

## Trust boundaries

| Party | Holds | May |
|-------|-------|-----|
| API customer | an access key (`access_keys[].ak`, or one created through the admin API) | call the model, MCP and batch surfaces its key is entitled to, within its quotas |
| Tenant admin | the tenant's `admin_token_env` bearer | read and change its own tenant's keys, usage, content-safety events; nothing global |
| Operator | the global `admin.token_env` bearer, the process environment, the config source | everything: config publish and reload, every tenant, `/internal/*`, the upstream credentials |
| Upstream vendor / MCP server | its own endpoint | is trusted only to answer; its errors are mapped, its headers are filtered, its credentials never leave the process |

The gateway never holds a customer's vendor credentials: every upstream key,
Bedrock token, MCP bearer and OAuth client secret is read from the
environment variable the config names, at call time, and appears in no
response, log line, audit row or trace.

## Authentication

- Access keys ride as `Authorization: Bearer <ak>` or `x-api-key: <ak>`; a
  missing or unknown key is a `401`. Keys have a lifecycle: `expires_at_epoch_secs`,
  `banned`, and an automatic abuse suspension (`abuse.tiers`) each fail
  authentication with a distinct `403`, on every surface (REST, realtime — where
  the key is re-checked per turn — batches, MCP). Logs, traces and the ledger
  carry `ak_id`, a SHA-256 fingerprint of the key, never the credential.
- Admin routes are absent (`404`) until an admin token is configured. Two
  tiers apply: the global token (`admin.token_env`) manages everything; a
  tenant's `admin_token_env` token manages only that tenant. A tenant token on
  a global-only route (reload, config publish, `/internal/*`, the cross-tenant
  `/admin/audit/ops` trail) is a `403`; a tenant token looking up a foreign key
  gets an empty page or a `404`, never a confirmation the key exists.
- The web control plane keeps its own identity store and browser sessions and
  reaches the gateway only through the admin API with the tokens above.

## Authorization and tenant scoping

- A key belongs to one tenant (the implicit `default` tenant when undeclared).
  Model entitlement (`tenants[].models`) is enforced before the cache and the
  engine, so a tenant cannot be served another tenant's cached answer or reach
  a model it is not entitled to — including through a fallback chain, where
  unentitled entries are skipped.
- Files, batches and retained content are tenant-owned: a foreign id answers
  `404`, not `403`, so ids cannot be probed.
- MCP: a key reaches only the servers named in its `mcp_servers` entitlement
  (any other name answers `404`), and only the tools in its `mcp_tools`
  allowlist; `tools/list` is filtered to the allowlist and a `tools/call`
  outside it is refused inside the gateway (JSON-RPC `-32000`). An MCP session
  id is bound to the key that first received it. Keys created through the
  admin API carry no MCP entitlement. Only a fixed set of MCP headers crosses
  the proxy in either direction; the caller's `Authorization` never reaches
  the server and the server's headers never reach the caller beyond
  `Content-Type` and `Mcp-Session-Id`. The proxy's HTTP client follows no
  redirects, so a server or token endpoint cannot steer a credentialed
  request elsewhere. JSON-RPC batches are refused.

## Content controls

- Per-tenant `security` policy: blocklist (block / flag / shadow), regex
  recognizers, secret detection, DLP redaction of emails and phone numbers in
  both directions (a redacted stream is buffered and replayed so no unmasked
  span leaves), and an external moderator (`moderation:`, AWS Bedrock
  Guardrails) over inbound text and — for the MCP proxy — over tool results.
  Signed thinking blocks are never rewritten; a mask that would land in one
  fails the request instead.
- A tenant admin may ban its own keys but can neither lift a ban nor change an
  abuse suspension: those are platform sanctions the global token owns.
- Every hit is a security event (`/admin/audit/events`) carrying the rule,
  action and hit count — never the prompt text. Admin mutations land in
  `/admin/audit/ops` with the source IP (the TCP peer; the rightmost
  `x-forwarded-for` hop only under `trust_proxy_headers`). Retained content (`tenants[].retention`)
  is sealed at rest with `GW_CONTENT_KEY`; without the key, `full` retention
  degrades to redacted text.

## Failure postures

Stated so an operator can choose them deliberately:

- Redis unreachable: rate limits, quotas and budgets **fail open** and a
  warning is logged; account health treats every account as healthy.
- Moderator unreachable: `security.moderation_fail_open` decides — `false`
  (default) denies the request or tool result, `true` admits it.
- Upstream failure: account failover within the model, then the model's
  `fallback_models` chain, only while nothing has been sent to the client.
  Upstream and identity-provider errors reach the customer as a generic
  `502`; endpoints and error text stay in the gateway log.
- Abuse of long-lived connections: realtime sessions and MCP listen streams
  are capped per key (`max_live_streams_per_key`); realtime turns and every
  MCP request spend the key's QPS permits; replies the proxy must buffer are
  capped per server (`max_reply_bytes`); `x-gw-user` is capped at 256 bytes,
  since it keys governance counters.
- Billing store unreachable: rows queue in a bounded repair queue and apply
  backpressure rather than being dropped.

## Operating recommendations

- Terminate TLS in front of the gateway; it listens on plain HTTP.
- Keep `/admin/*`, `/internal/*` and `/metrics` off the public load balancer
  (the sample nginx config in [multi-instance](multi-instance.md) does this)
  and leave `GW_ADMIN_TOKEN` unset on instances that do not need the admin API.
- Put MCP servers on a private network the gateway alone can reach; the
  gateway is their only client and holds their credentials.
- Give each customer their own key with `qps`, `daily_token_quota`,
  `tokens_per_minute` and a cost budget, and route alerts
  (`alerts.webhook_url_env`) somewhere watched.
- Rotate upstream and MCP credentials by changing the environment and
  reloading; OAuth tokens the gateway fetched are dropped on reload.

## Review record

A targeted adversarial review of the admin plane and the MCP proxy ran on
2026-09-09 against three lenses: authentication and tenant scoping;
injection, smuggling and protocol edge cases; resource exhaustion and
information leakage. Fixed in the same change: unbounded buffering of MCP
replies, redirect following by the MCP client (credential replay and SSRF),
upstream endpoints and identity-provider error text in customer-visible
errors, `resources/read` and `prompts/get` results escaping review, multi-line
SSE `data` and other unparsable replies passing through unreviewed, structured
tool output escaping a mask, stream resumption replaying results past the
review, unbound MCP session ids, tool calls audited only after the upstream
answered, a `tools/call` without a string tool name skipping every gate,
realtime sessions and listen streams with no per-key cap, in-process monthly
counters keyed by `x-gw-user` never evicted, a tenant token lifting bans and
suspensions, raw access keys in denial messages, admin listings without a page
ceiling, and MCP server names probe-able through 404-vs-403.

Accepted as is, with the reason: the ledger, security-event and retained-
content tables store the access key itself (the ledger joins usage by it;
database read access sits inside the operator boundary — protect the database
like the config); the listener has no header-read or idle timeout of its own
(a proxy in front terminates slow clients, which the deployment guidance
requires anyway); a video download is held in memory for the clip's size; a
config reload can race one in-flight token fetch for at most one token
lifetime; duplicate JSON keys parse last-wins here and possibly first-wins on
a non-compliant upstream (a compliant server behaves identically); the
cross-tenant key guard reads a two-second key cache before mutating, which
would need a key to change tenant inside that window.
