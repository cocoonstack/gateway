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

The response sets `finish_reason:"tool_calls"` with the call in
`choices[0].message.tool_calls`; `content` carries any text the model emitted
alongside and is omitted otherwise.

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
export GW_ADMIN_TOKEN=change-me                    # names the global admin bearer (see conf/gateway.yaml)
curl -s -H "Authorization: Bearer $GW_ADMIN_TOKEN" \
  'localhost:8080/internal/ledger?limit=5'         # operator surface — global token + private network
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

## Claude on AWS Bedrock

```yaml
accounts:
  - {name: bedrock, provider: aws, endpoint: "https://bedrock-runtime.us-east-1.amazonaws.com",
     api_key_env: AWS_BEARER_TOKEN_BEDROCK, protocols: ["aws-anthropic", "aws-llama"]}
models:
  - {name: us.anthropic.claude-sonnet-4-5-20250929-v1:0, protocol: aws-anthropic, prompt_cache: true}
  - {name: us.meta.llama3-3-70b-instruct-v1:0, protocol: aws-llama}
```

```bash
export AWS_BEARER_TOKEN_BEDROCK=bedrock-api-key-...   # or AWS_ACCESS_KEY_ID + secret_key_env
curl -s localhost:8080/v1/chat/completions \
  -H 'authorization: Bearer ak-live' -H 'content-type: application/json' \
  -d '{"model":"us.anthropic.claude-sonnet-4-5-20250929-v1:0","stream":true,"reasoning_effort":"low",
       "messages":[{"role":"user","content":"What is 17*23?"}]}'
```

The same request shapes as any other model: reasoning comes back as
`reasoning_content` + signed `reasoning_details`, tools and thinking replay
through the loop, and `/v1/messages` speaks to it natively.

## Grok (xAI)

```yaml
providers:
  - {name: xai, kind: xai, api_key_env: XAI_API_KEY}
models:
  - {name: grok-4.6, provider: xai,
     input_price_per_1k_micros: 2000, output_price_per_1k_micros: 6000, token_rate: {read_cache: 0.25}}
  - {name: grok-imagine-image-2.0, provider: xai, protocol: image, unit_price_micros: 70000}
  - {name: grok-imagine-video-1.5, provider: xai, protocol: video, unit_price_micros: 100000}
```

The preset covers xAI's OpenAI-compatible surfaces (`/v1/chat/completions`,
`/v1/responses`, `/v1/images/generations`), its async video
(`/v1/videos/generations` answers `{request_id}`; poll `GET /v1/videos/{id}`
until `done` — the first `done` bills the clip's seconds at the unit price and
records xAI's `cost_in_usd_ticks` as the vendor cost) and its realtime voice
socket (`protocol: realtime`, model `grok-voice-latest`, billed by the delivered
output estimate since xAI reports no usage). `reasoning_effort` passes through
verbatim — grok-4.6 takes `low`…`xhigh`, grok-4.3 also `none`, and a model
answers 400 for a value it does not list — and the usage's `cached_tokens` /
`reasoning_tokens` land in the ledger like any OpenAI-shaped vendor. Anthropic
clients reach Grok through `/v1/messages` (the gateway converts; xAI's own
Anthropic-compatible endpoint is deprecated).

## Cursor and other bring-your-own-key clients

Cursor's *Models → API Keys* lets you point its OpenAI, Anthropic and Google
keys at another base URL. Requests are relayed by Cursor's servers, so the
gateway must be reachable from the internet over HTTPS (a public host or a
tunnel — never `localhost`).

- **OpenAI key + "Override OpenAI Base URL"** → `https://gw.example.com/v1`
  (Cursor appends `/chat/completions`), key = a gateway access key. Add each
  gateway model name under *Model Names* — any configured model, Claude and
  Gemini included: the gateway serves them on the OpenAI wire with streaming
  and tool calls, which is what Cursor's Agent/Ask modes send.
- **Anthropic key + base URL** → `https://gw.example.com`; the access key rides
  as `x-api-key`, and `/v1/messages` serves every model natively or by
  conversion.

Tab completion and inline edit stay on Cursor's own models regardless of keys,
and Cursor shows only the final answer (reasoning prose is dropped client-side).
Verified on the gateway side by the live matrix (streamed chat with tools,
`x-api-key` auth, `/v1/models`); driving the Cursor client itself was not part
of that run.
