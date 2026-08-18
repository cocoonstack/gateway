# Providers

A provider is an upstream vendor the gateway calls. Two ways to configure one:
a **preset** (recommended) or a raw **account**.

## Presets

A `providers:` entry expands into an account with the kind's base URL, served
protocols, and auth style — going live is `kind` + `api_key_env`:

```yaml
providers:
  - name: openai
    kind: openai
    api_key_env: OPENAI_API_KEY
models:
  - name: gpt-4o
    provider: openai      # fills the protocol and pins the model to openai's accounts
```

### Kinds

| kind | base URL | protocols | auth |
|------|----------|-----------|------|
| `openai` | `https://api.openai.com` | openai-chat, embeddings, image, tts, stt, responses, completions, realtime, moderations | `Bearer` |
| `anthropic` | `https://api.anthropic.com` | anthropic-messages | `x-api-key` + `anthropic-version` |
| `gemini` | `https://generativelanguage.googleapis.com` | gemini | `x-goog-api-key` |
| `deepseek` | `https://api.deepseek.com` | openai-chat | `Bearer` |
| `openrouter` | `https://openrouter.ai/api` | openai-chat | `Bearer` (its `reasoning_details` shape is the one this gateway emits, so signed Anthropic reasoning round-trips through tool loops; verified live on free and paid models) |
| `moonshot` | `https://api.moonshot.cn` | openai-chat | `Bearer` (Kimi K2 thinking: `reasoning_content` in and out, `thinking: {type: disabled}` passes through; the vendor's `/anthropic` base also works as `kind: anthropic` + `endpoint`) |
| `siliconflow` | `https://api.siliconflow.cn` | openai-chat, embeddings, rerank, tts, stt, image | `Bearer` (Qwen3 `enable_thinking`, DeepSeek/GLM/Kimi/MiniMax hosted models, bge/Qwen3 embeddings and rerankers, CosyVoice TTS, SenseVoice STT, Kolors images — all verified live) |

Any other OpenAI-compatible vendor (Qwen, Ollama, vLLM, a relay) uses
`kind: openai` with an `endpoint:` override:

```yaml
providers:
  - name: myvendor
    kind: openai
    endpoint: "https://my-relay.example.com"
    api_key_env: MYVENDOR_KEY
```

### Rerank

`/v1/rerank` speaks the Cohere/Jina shape (`{model, query, documents, top_n?}`),
so Cohere and Jina need no preset — a raw account on the `rerank` protocol
with the vendor's base URL, and models pinned by `provider`:

```yaml
accounts:
  - {name: cohere, provider: cohere, endpoint: "https://api.cohere.com", api_key_env: COHERE_API_KEY, protocols: ["rerank"]}
  - {name: jina,   provider: jina,   endpoint: "https://api.jina.ai",   api_key_env: JINA_API_KEY,   protocols: ["rerank"]}
models:
  - {name: rerank-v3.5, protocol: rerank, provider: cohere}
  - {name: jina-reranker-v3, protocol: rerank, provider: jina}
```

Jina reports `usage.total_tokens`, which bills as prompt tokens; Cohere bills
by search units (`meta.billed_units.search_units`), priced by the model's
`unit_price_micros`. Both verified live.

## Native (non-OpenAI) wire engines

Some vendors are addressed in their own wire dialect rather than an
OpenAI-compatible shape, via a raw `accounts:` entry pinned to the vendor's
`protocol`. Those that stream do so natively (incremental deltas + billed
usage); the rest are marked non-streaming below and always answer buffered:

| protocol | vendor | endpoint | notes |
|----------|--------|----------|-------|
| `gemini` | Google Gemini | `https://generativelanguage.googleapis.com` | `x-goog-api-key`; streams via `streamGenerateContent`; thinking tokens billed as reasoning |
| `dashscope` | Alibaba Qwen (native) | `https://dashscope-intl.aliyuncs.com` | `Bearer`; streams via `X-DashScope-SSE` + `incremental_output` |
| `anthropic-messages` | any Anthropic-compatible endpoint (e.g. MiniMax) | vendor's `/anthropic` base | `x-api-key`; some report `input_tokens` only in `message_delta` — handled |
| `ernie` | Baidu Ernie (Wenxin) | `https://aip.baidubce.com` | a `bce-v3/…` key goes as `Bearer`, a legacy token as the `access_token` query param (non-streaming); Qianfan's OpenAI-compatible `https://qianfan.baidubce.com/v2` also works as `kind: openai` + `endpoint` |
| `aws-anthropic` | Anthropic Claude on AWS Bedrock | `https://bedrock-runtime.<region>.amazonaws.com` | SigV4 (see below); model name = the Bedrock model id (`anthropic.claude-…`, `us.anthropic.claude-…`); the full Messages engine (system, tools, thinking dialects by generation, prompt-cache breakpoints, signed reasoning) on the InvokeModel wire — `anthropic_version` in the body, model and streaming in the path; streams via InvokeModelWithResponseStream (EventStream frames decoded into the same event sequence) |
| `aws-cohere` | Cohere Command on AWS Bedrock | `https://bedrock-runtime.<region>.amazonaws.com` | SigV4 (see below); model name = the Bedrock model id; Command R (`{message, chat_history, preamble}` → `text`) and the legacy Command `generations[]` shape; usage from Bedrock's `x-amzn-bedrock-*-token-count` headers, or `amazon-bedrock-invocationMetrics` on a stream |
| `aws-llama` | Meta Llama on AWS Bedrock | `https://bedrock-runtime.<region>.amazonaws.com` | SigV4 (see below); model name = the Bedrock model id (`meta.llama3-1-8b-instruct-v1:0`); usage from the token-count headers / invocation metrics, else the body counts |
| `minimax-v1` | MiniMax legacy v1 (`abab*`) | `https://api.minimax.chat` | `Bearer` (non-streaming); kept for existing accounts — the vendor has retired it for new ones; new integrations should use MiniMax's OpenAI-/Anthropic-compatible endpoints |

The factory also dispatches `video`, `search`, generic `audio`, and
`passthrough` protocols (kling-video and brave-search ship example accounts in
the default config).

```yaml
accounts:
  - name: qwen
    provider: alibaba
    endpoint: "https://dashscope-intl.aliyuncs.com"
    api_key_env: DASHSCOPE_API_KEY
    protocols: ["dashscope"]
models:
  - name: qwen-turbo
    protocol: dashscope
```

A preset also accepts `endpoint`, `timeout_seconds`, `connect_retries`, `retry_status`, and
`secret_key_env`, inherited by every account naming the provider for whatever
the account leaves unset (an explicit `endpoint: "mock://…"` keeps an account
on the mock transport; an explicit `retry_status: []` disables replays the
provider declared). An explicit `accounts:` entry with the same name wins
over the preset.

## Going live

1. Put the key in the process environment: `export OPENAI_API_KEY=sk-...`
   (keys never live in the config file — the account names an env var).
2. Configure the provider/account with a real `endpoint` and `api_key_env`.
3. Start the gateway. Requests egress to the real vendor and the ledger records
   real usage.

Exercised against the real vendors, end to end through the gateway: OpenAI
(every surface above including realtime, files/batches, prompt caching on
Anthropic, tool loops and reasoning on both), Anthropic, Gemini, DeepSeek,
MiniMax and Moonshot/Kimi (OpenAI- and Anthropic-compatible endpoints), Qwen/DashScope
(both compatible endpoints), Baidu Qianfan v2 and the native Ernie wire,
Cohere and Jina rerank, SiliconFlow (chat, embeddings, rerank, TTS, STT, images), OpenRouter, and OpenAI/Anthropic relays. AWS Bedrock is verified
against AWS up to an accepted SigV4 signature (the account was not
allowlisted for models) and end to end — Claude, Llama and Cohere, buffered
and streamed — against the
[ministack](https://github.com/ministackorg/ministack) Bedrock emulator's
family-faithful InvokeModel replies, EventStream framing and token-count
headers.

`GW_TRANSPORT` overrides transport routing: unset (or any value other than
`mock`/`http`) routes `mock://` sentinel URLs in-process and real URLs over
HTTP; `mock` forces zero egress; `http` disables the mock so a misconfigured
account fails loudly.

An account's `timeout_seconds` bounds a non-streaming request end to end. A
streaming request instead gets that bound on the response headers and then on
each gap between chunks — an actively flowing generation is never cut short by
the total budget, while a stalled stream fails at the gap.

## Accounts, failover, and health

Multiple accounts can serve the same protocol. Selection is by `priority`
(lower first), round-robin within a tie, with PTU-tier accounts preferred over
paygo. On an upstream 5xx the failed account is excluded and another is tried
once (a PTU→paygo switch is flagged `ptu_spillover`). Consecutive failures put
an account into cooldown (`stability.failure_threshold` / `cooldown_seconds`),
and it auto-recovers on expiry. A streaming response that already sent bytes to
the client is never failed over, but a provider error that breaks such a stream
still counts against the account's health and the model's availability; a plain
client disconnect counts as neither.

## AWS SigV4

AWS Bedrock accounts sign requests with SigV4. Set `api_key_env` to the access
key id's env var and `secret_key_env` to the secret key's; both must resolve or
the account falls back to inert mock credentials. The signing region is read
from the endpoint host (`bedrock-runtime.<region>.amazonaws.com`, default
`us-east-1`), so a local emulator works with `endpoint: http://localhost:4566`.

```yaml
accounts:
  - {name: bedrock, provider: aws, endpoint: "https://bedrock-runtime.us-east-1.amazonaws.com",
     api_key_env: AWS_ACCESS_KEY_ID, secret_key_env: AWS_SECRET_ACCESS_KEY,
     protocols: ["aws-anthropic", "aws-llama", "aws-cohere"]}
models:
  - {name: us.anthropic.claude-sonnet-4-5-20250929-v1:0, protocol: aws-anthropic}
  - {name: meta.llama3-1-8b-instruct-v1:0, protocol: aws-llama}
```
