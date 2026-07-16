# Observability

## Metrics

`GET /metrics` renders the Prometheus registry.

| Metric | Type | Labels |
|--------|------|--------|
| `gateway_requests_total` | counter | `route`, `status` |
| `gateway_request_duration_seconds` | histogram | `route` |
| `gateway_node_duration_seconds` | histogram | `node` (pipeline stage) |
| `gateway_tokens_total` | counter | `kind` (prompt/completion) |
| `gateway_cache_hits_total` | counter | — |
| `gateway_ledger_write_failures_total` | counter | — |
| `gateway_upstream_connect_retries_total` | counter | `account` |

`gateway_requests_total` is recorded by router middleware, so every response —
including error statuses and the realtime WebSocket upgrade — is counted, which
makes error-rate dashboards possible. All labels are bounded (route templates,
status codes, protocol/stage names) — no per-key or per-model cardinality.

## Access log

One structured line per successfully served request goes to stdout (via
`tracing`; control level with `RUST_LOG`), carrying `surface`, `request_id`,
`ak`, `product`, `user_id`, `model`, `protocol`, `account`, `prompt_tokens`,
`completion_tokens`, `total_tokens`, and `latency_ms`. Errored requests are
counted by `gateway_requests_total{status}` rather than logged. `request_id`
joins the access log to the ledger row and the audit events for the same
request.

## Billing ledger

`GET /internal/ledger?limit=N` returns the most recent `N` billing records,
oldest-first within the page; `count` is always the true total, independent of
the page size. Records persist when a SQLite store is configured and can be
capped with `storage.ledger_max_rows`. Each record carries `request_id`, the
access key, product, `tenant`, `user_id` (effective end user), the requested
`model` and the `served_model` (differs after a quota fallback), protocol,
account, token counts, charged `cost_micros` and `vendor_cost_micros`,
`created_at_epoch_secs`, the PTU-spillover flag, and an `estimated` flag (set
when counts came from an aborted stream rather than a vendor usage payload).
Per-user usage additionally rolls into durable minute buckets every minute, so
`GET /admin/usage/users` stays correct after `ledger_max_rows` pruning (see
[Governance](governance.md#per-user-attribution-and-billing)).

## Audit trails

Three operator-facing audit surfaces, all under the gated `/admin` prefix and
covered in [Governance](governance.md#audit-trails): `GET /admin/audit/events`
(content-safety hits, no prompt text), `GET /admin/audit/ops` (admin-plane
mutations with source IP), and `GET /admin/usage/users` (per-user cost). Content
retention, when a tenant enables it, is read back via
`GET /admin/audit/content/{request_id}`.
