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
| `/v1/chat/completions` | 1-turn, ~90 B | 64 | 98,000 | 0.62 ms | 1.30 ms |
| `/v1/chat/completions` | 1-turn, ~90 B | 256 | 99,300 | 2.40 ms | 5.78 ms |
| `/v1/chat/completions` | 1-turn, ~90 B | 1024 | 93,500 | 9.40 ms | 29.3 ms |
| `/v1/chat/completions` `stream: true` | 1-turn, SSE (3 frames + `[DONE]`) | 256 | 69,300 | 3.45 ms | 8.83 ms |
| `/v1/chat/completions` | 16-turn prose, 52 KB, ~13k tokens reserved | 64 | 54,200 | 1.09 ms | 2.57 ms |
| `/v1/chat/completions` | 16-turn prose, 52 KB, ~13k tokens reserved | 256 | 40,600 | 5.97 ms | 14.6 ms |
| `/v1/messages` | 1-turn | 256 | 91,700 | 2.54 ms | 7.58 ms |
| `/v1/messages` `stream: true` | 1-turn, SSE | 256 | 61,200 | 3.90 ms | 10.2 ms |
| `/v1/embeddings` | 1 input | 256 | 94,000 | 2.49 ms | 6.27 ms |

In-process (router driven directly, no sockets — `crates/server/tests/bench.rs`):
serial p50 24 µs / p99 34 µs per chat request, ~194,000 requests/s with 64
workers.

Read as capacity: ~6 M requests/minute on small bodies and ~2.4 M/minute on
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
- Reasoning, tool-call, cache and DLP work all rides the same path: the
  in-process bench is re-run before every merge and gates on parity with
  `main` (see [Development](development.md)).

## Reproduce

```bash
cargo build --release
./target/release/gw &                       # default config, mock upstream
echo '{"model":"gpt-4o","messages":[{"role":"user","content":"benchmark round"}]}' > body.json
oha -z 15s -c 256 -m POST -H 'Authorization: Bearer ak-bench' \
    -H 'content-type: application/json' -D body.json \
    http://127.0.0.1:8080/v1/chat/completions
```
