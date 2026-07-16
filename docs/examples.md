# Examples

Every snippet below runs against `cargo run -p gw-server` with the embedded
demo config (mock upstreams, zero egress) unless it says otherwise. The demo
key is `ak-demo-123`.

## Chat completion

```bash
curl -s localhost:8080/v1/chat/completions \
  -H 'authorization: Bearer ak-demo-123' -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hello"}]}'
```

```json
{"id":"chatcmpl-local-1","object":"chat.completion","model":"gpt-4o",
 "choices":[{"index":0,"message":{"role":"assistant","content":"..."},"finish_reason":"stop"}],
 "usage":{"prompt_tokens":5,"completion_tokens":10,"total_tokens":15}}
```

## Streaming

```bash
curl -sN localhost:8080/v1/chat/completions \
  -H 'authorization: Bearer ak-demo-123' -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"count to 3"}]}'
```

```
data: {"choices":[{"delta":{"content":"one"},"finish_reason":null}]}
data: {"choices":[{"delta":{"content":" two"},"finish_reason":null}]}
data: {"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{...}}
data: [DONE]
```

The final data frame carries `usage` and `finish_reason`. Frames arrive
as the upstream produces them only when `security.dlp_redact` is off;
the embedded demo config ships with it **on**, so the stream is buffered
and replayed post-redaction (see [governance.md](governance.md)).

## Anthropic messages

```bash
curl -sN localhost:8080/v1/messages \
  -H 'x-api-key: ak-demo-123' -H 'content-type: application/json' \
  -d '{"model":"claude-sonnet","stream":true,"max_tokens":128,
       "messages":[{"role":"user","content":"hi"}]}'
```

```
event: message_start
data: {"type":"message_start","message":{...}}
event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}
event: content_block_delta
data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"..."}}
event: content_block_stop
data: {"type":"content_block_stop","index":0}
event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{...}}
event: message_stop
data: {"type":"message_stop"}
```

`/v1/messages` also works on OpenAI-protocol models — the gateway converts.

## Tools

```bash
curl -s localhost:8080/v1/chat/completions \
  -H 'authorization: Bearer ak-demo-123' -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"weather in NYC?"}],
       "tools":[{"type":"function","function":{"name":"get_weather",
         "parameters":{"type":"object","properties":{"city":{"type":"string"}}}}}]}'
```

The response omits `content` and sets `finish_reason:"tool_calls"` with the
call in `choices[0].message.tool_calls`.

## Embeddings, images, audio

```bash
curl -s localhost:8080/v1/embeddings \
  -H 'authorization: Bearer ak-demo-123' -H 'content-type: application/json' \
  -d '{"model":"text-embedding-3","input":"embed me"}'

curl -s localhost:8080/v1/images/generations \
  -H 'authorization: Bearer ak-demo-123' -H 'content-type: application/json' \
  -d '{"model":"dall-e-3","prompt":"a red cube","n":1}'

curl -s localhost:8080/v1/audio/speech \
  -H 'authorization: Bearer ak-demo-123' -H 'content-type: application/json' \
  -d '{"model":"tts-1","input":"hello"}'
```

## Batch workflow

```bash
# 1. upload a JSONL file (one request per line)
FID=$(curl -s localhost:8080/v1/files \
  -H 'authorization: Bearer ak-demo-123' -H 'content-type: application/json' \
  -d '{"purpose":"batch","file":"{\"body\":{\"model\":\"gpt-4o\",\"messages\":[{\"role\":\"user\",\"content\":\"one\"}]}}\n{\"body\":{\"model\":\"gpt-4o\",\"messages\":[{\"role\":\"user\",\"content\":\"two\"}]}}"}' \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')

# 2. create a batch from the file
BID=$(curl -s localhost:8080/v1/batches \
  -H 'authorization: Bearer ak-demo-123' -H 'content-type: application/json' \
  -d "{\"input_file_id\":\"$FID\"}" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')

# 3. poll for results
curl -s localhost:8080/v1/batches/$BID -H 'authorization: Bearer ak-demo-123'
```

## Observability

```bash
curl -s localhost:8080/metrics | grep gateway_
curl -s 'localhost:8080/internal/ledger?limit=5'   # operator surface, no key check — keep it off the public LB
```

## Going live against a real provider

```yaml
# my.yaml
listen: {host: 127.0.0.1, port: 8080}
access_keys:
  - {ak: ak-live, product: live, qps: 20, daily_token_quota: 10000000}
providers:
  - name: openai
    kind: openai
    api_key_env: OPENAI_API_KEY
    endpoint: "https://api.openai.com"   # or an OpenAI-compatible relay
models:
  - {name: gpt-4o-mini, provider: openai,
     input_price_per_1k_micros: 150, output_price_per_1k_micros: 600}
```

```bash
export OPENAI_API_KEY=sk-...
GW_CONFIG=my.yaml cargo run -p gw-server
curl -s localhost:8080/v1/chat/completions \
  -H 'authorization: Bearer ak-live' -H 'content-type: application/json' \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}'
```

Add a second provider (`kind: anthropic`, `kind: deepseek`, …) and more
`models:` to route several vendors through one gateway. See
[Providers](providers.md) and [Configuration](configuration.md).
