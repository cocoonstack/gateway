# Performance

What the gateway itself costs per request, measured end to end over HTTP
against the in-process mock upstream — so the numbers are the gateway's own
overhead (HTTP, auth, DAG pipeline, admission reserve/settle, DLP scan,
billing ledger), not any vendor's generation speed. A real upstream adds its
own latency and is the actual throughput bound; the gateway's job is to stay
invisible next to it.

## Setup

- Release binary (`cargo build --release`), default `conf/gateway.yaml`
  (`security.dlp_redact: true`, in-memory store, no Redis/Postgres), key
  `ak-bench` (unthrottled)
- Apple M2 Max (12 cores, 64 GB); load generator [`oha`](https://github.com/hatoo/oha)
  on the same host, so the gateway competes with it for cores
- Every run 100 % HTTP 200 unless stated; latencies are client-observed

## Results

| Surface | Body | Concurrency | Requests/s | p50 | p99 |
|---|---|---:|---:|---:|---:|
| `/v1/chat/completions` | 1-turn, ~90 B | 64 | 92,800 | 0.64 ms | 1.53 ms |
| `/v1/chat/completions` | 1-turn, ~90 B | 256 | 89,400 | 2.56 ms | 8.22 ms |
| `/v1/chat/completions` | 1-turn, ~90 B | 1024 | 92,900 | 9.78 ms | 30.8 ms |
| `/v1/chat/completions` `stream: true` | 1-turn, SSE (3 frames + `[DONE]`) | 256 | 68,400 | 3.47 ms | 8.72 ms |
| `/v1/chat/completions` | 16-turn prose, 52 KB, ~13k tokens reserved | 64 | 41,000 | 1.51 ms | 3.17 ms |
| `/v1/chat/completions` | 16-turn prose, 52 KB, ~13k tokens reserved | 256 | 40,200 | 6.01 ms | 14.5 ms |
| `/v1/messages` | 1-turn | 256 | 91,700 | 2.57 ms | 6.86 ms |
| `/v1/messages` `stream: true` | 1-turn, SSE | 256 | 60,400 | 3.79 ms | 13.3 ms |
| `/v1/embeddings` | 1 input | 256 | 90,000 | 2.53 ms | 8.56 ms |

Run-to-run spread on the same host is about ±5 % on throughput and wider on
p99 (an earlier run of the same table on a quieter machine read 99k requests/s
at p99 5.8 ms for the 256-concurrency chat row); compare rows within one run,
not across days. The 2026-08-18 hygiene round was measured A/B against the
pre-round binary under identical conditions: every row within ±4 % (chat 256:
88.2k → 89.4k; 52 KB 256: 39.7k → 40.2k; embeddings 93.4k → 90.0k), no
directional change.

In-process (router driven directly, no sockets — `crates/server/tests/bench.rs`):
serial p50 25 µs / p99 34 µs per chat request (52.5 ms per 2,000), ~202,000
requests/s with 64 workers; the 48 KB-body request clone the CallEngine node
pays costs ~4–5 µs.

Read as capacity: ~5.5 M requests/minute on small bodies and ~2.4 M/minute on
52 KB bodies per node — at the 52 KB shape that is ~2 GB/s of request bodies
and roughly 5×10⁸ reserved tokens/s (3×10¹⁰ tokens/minute) through the
counting, admission and billing path. Any vendor's rate limits and generation
speed sit orders of magnitude below that; the gateway is not the ceiling.

## What the runs also showed

- Governance holds under load: the 52 KB run at 256 concurrency exhausted
  `ak-bench`'s 10⁹ daily-token quota mid-run and every further request was a
  clean `service_quota_exceeded_exception` — reserve-then-settle admission
  does not overshoot at 40k requests/s.
- Resources: the gateway used ~8 cores at 256 concurrency. Memory grows only
  in the in-memory store, which keeps every ledger row (~1 KB per request);
  point `storage` at SQLite/Postgres for anything longer than a benchmark.
- Reasoning, tool-call, cache and DLP work all rides the same path. The
  in-process benchmark is a manual diagnostic rather than a CI merge gate
  (see [Development](development.md)).

## Against other gateways

Same host (a 384-thread Linux box, Debian 13), same load generator, same
upstream — a second gateway instance in mock mode reached over HTTP, so every
arm pays a real HTTP hop — same 90-byte chat body at 64 concurrency for 20 s,
arms interleaved over three rounds with the order swapped, each gateway on
its shipped defaults (LiteLLM 1.100.0 proxy with 8 workers, Bifrost 2.0.0 from
its binary release, this gateway from `cargo build --release`):

| Gateway | Requests/s (3 rounds) | p50 | p99 |
|---|---:|---:|---:|
| this gateway (Rust) | 104,800 / 103,000 / 117,600 | 0.50–0.52 ms | 0.79–0.91 ms |
| Bifrost (Go) | 17,900 / 19,500 / 21,500 | 1.32–1.44 ms | 21.7–25.7 ms |
| LiteLLM (Python) | 1,770 / 1,920 / 1,770 | 19–34 ms | 45–162 ms |

Every run was 100 % HTTP 200. The numbers measure each gateway's own overhead
in front of an instant upstream; a real vendor adds the same generation time
to all three.

The feature rounds are gated the same way: the night that added the OTLP span,
cost budgets, the guardrail moderator and the MCP proxy was measured A/B on
that host (before vs after, interleaved, mock upstream, 256 concurrency):
small chat 219k → 217k requests/s, 57 KB chat 157k → 191k, streamed
`/v1/messages` 5.6k → 5.7k — no regression; the unconfigured features cost
nothing per request by construction.

## Reproduce

```bash
cargo build --release
./target/release/gw &                       # default config, mock upstream
echo '{"model":"gpt-4o","messages":[{"role":"user","content":"benchmark round"}]}' > body.json
oha -z 15s -c 256 -m POST -H 'Authorization: Bearer ak-bench' \
    -H 'content-type: application/json' -D body.json \
    http://127.0.0.1:8080/v1/chat/completions
```
