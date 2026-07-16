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
| `openai` | `https://api.openai.com` | openai-chat, embeddings, image, tts, stt, responses, completions, realtime | `Bearer` |
| `anthropic` | `https://api.anthropic.com` | anthropic-messages | `x-api-key` + `anthropic-version` |
| `gemini` | `https://generativelanguage.googleapis.com` | gemini | `x-goog-api-key` |
| `deepseek` | `https://api.deepseek.com` | openai-chat | `Bearer` |
| `openrouter` | `https://openrouter.ai/api` | openai-chat | `Bearer` |

Any other OpenAI-compatible vendor (Qwen, Ollama, vLLM, a relay) uses
`kind: openai` with an `endpoint:` override:

```yaml
providers:
  - name: myvendor
    kind: openai
    endpoint: "https://my-relay.example.com"
    api_key_env: MYVENDOR_KEY
```

## Native (non-OpenAI) wire engines

Some vendors are addressed in their own wire dialect rather than an
OpenAI-compatible shape, via a raw `accounts:` entry pinned to the vendor's
`protocol`. All of these stream natively (incremental deltas + billed usage):

| protocol | vendor | endpoint | notes |
|----------|--------|----------|-------|
| `gemini` | Google Gemini | `https://generativelanguage.googleapis.com` | `x-goog-api-key`; streams via `streamGenerateContent`; thinking tokens billed as reasoning |
| `dashscope` | Alibaba Qwen (native) | `https://dashscope-intl.aliyuncs.com` | `Bearer`; streams via `X-DashScope-SSE` + `incremental_output` |
| `anthropic-messages` | any Anthropic-compatible endpoint (e.g. MiniMax) | vendor's `/anthropic` base | `x-api-key`; some report `input_tokens` only in `message_delta` — handled |
| `ernie` | Baidu Ernie (Wenxin) | `https://aip.baidubce.com` | `access_token` query param (non-streaming) |
| `aws-cohere` | Cohere Command on AWS Bedrock | `https://bedrock-runtime.<region>.amazonaws.com` | SigV4 (see below; non-streaming) |
| `aws-llama` | Meta Llama on AWS Bedrock | `https://bedrock-runtime.<region>.amazonaws.com` | SigV4 (see below; non-streaming) |
| `minimax-v1` | MiniMax legacy v1 (`abab*`) | `https://api.minimax.chat` | `Bearer`; kept for existing accounts — the vendor has retired it for new ones; new integrations should use MiniMax's OpenAI-/Anthropic-compatible endpoints |

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

A preset also accepts `endpoint`, `timeout_seconds`, `connect_retries`, and
`secret_key_env`, inherited by the synthesized account. An explicit `accounts:`
entry with the same name wins over the preset.

## Going live

1. Put the key in the process environment: `export OPENAI_API_KEY=sk-...`
   (keys never live in the config file — the account names an env var).
2. Configure the provider/account with a real `endpoint` and `api_key_env`.
3. Start the gateway. Requests egress to the real vendor and the ledger records
   real usage.

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
the client is never failed over.

## AWS SigV4

AWS Bedrock accounts sign requests with SigV4. Set `api_key_env` to the access
key id's env var and `secret_key_env` to the secret key's; both must resolve or
the account falls back to inert mock credentials.
