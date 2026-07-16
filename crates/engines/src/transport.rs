//! Transport isolation — the egress seam. Engines never hold an HTTP client:
//! they build an [`UpstreamRequest`] and hand it to a [`Transport`].
//! [`MockTransport`] fabricates deterministic vendor responses (the test
//! default); `HttpTransport` and the scheme-routing `DispatchTransport` (the
//! server default) live in `http_transport`.

use std::sync::Arc;

use gw_consts::Protocol;
use gw_models::{GResult, GatewayError};
use serde_json::{Value, json};

/// Fixed "created" timestamp for deterministic mock payloads.
pub const MOCK_CREATED: i64 = 1_720_000_000;
/// 1x1 PNG-ish placeholder bytes, base64. Deterministic image/audio payload.
pub const MOCK_B64: &str = "TU9DS0JZVEVT"; // "MOCKBYTES"

/// A vendor-bound request an engine built, ready to hand to a [`Transport`].
#[derive(Debug, Clone)]
pub struct UpstreamRequest {
    pub protocol: Protocol,
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub stream: bool,
    /// upstream account slot handling this call (used by failover/downtime simulation).
    pub account: String,
}

/// Body of an upstream response: buffered JSON, buffered SSE bytes, or live
/// SSE bytes yielded as the vendor sends them.
pub enum UpstreamBody {
    Json(Vec<u8>),
    Sse(Vec<u8>),
    SseStream(futures::stream::BoxStream<'static, Result<bytes::Bytes, String>>),
}

impl std::fmt::Debug for UpstreamBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpstreamBody::Json(b) => f.debug_tuple("Json").field(&b.len()).finish(),
            UpstreamBody::Sse(b) => f.debug_tuple("Sse").field(&b.len()).finish(),
            UpstreamBody::SseStream(_) => f.write_str("SseStream(..)"),
        }
    }
}

#[derive(Debug)]
pub struct UpstreamResponse {
    pub status: u16,
    pub body: UpstreamBody,
}

impl UpstreamResponse {
    /// Drain a live SSE stream into buffered bytes; Json/Sse pass through.
    /// Engines that don't forward incrementally call this once up front.
    /// Capped so a hostile upstream can't OOM the buffered path.
    pub async fn buffered(mut self) -> GResult<Self> {
        const MAX_BUFFERED_SSE: usize = 64 * 1024 * 1024;
        if let UpstreamBody::SseStream(mut s) = self.body {
            use futures::StreamExt;
            let mut buf = Vec::new();
            while let Some(item) = s.next().await {
                let bytes = item.map_err(|e| {
                    GatewayError::new(
                        gw_consts::ErrCode::FED_RESP_RPC_FAILED,
                        502,
                        format!("upstream stream failed: {e}"),
                    )
                })?;
                buf.extend_from_slice(&bytes);
                if buf.len() > MAX_BUFFERED_SSE {
                    return Err(GatewayError::new(
                        gw_consts::ErrCode::FED_RESP_RPC_FAILED,
                        502,
                        format!("upstream sse body exceeds {MAX_BUFFERED_SSE} bytes"),
                    ));
                }
            }
            self.body = UpstreamBody::Sse(buf);
        }
        Ok(self)
    }
}

#[async_trait::async_trait]
pub trait Transport: Send + Sync + std::fmt::Debug {
    async fn send(&self, req: UpstreamRequest) -> GResult<UpstreamResponse>;

    /// Apply reloaded per-account upstream policy (timeouts/retries) live.
    /// Default no-op: only the HTTP-backed transports carry policy.
    fn reload_policies(
        &self,
        _default: crate::http_transport::UpstreamPolicy,
        _per_account: std::collections::HashMap<String, crate::http_transport::UpstreamPolicy>,
    ) {
    }
}

pub type SharedTransport = Arc<dyn Transport>;

/// Deterministic fake vendor: parses the engine-built request body (so request
/// construction is exercised too) and answers in the vendor's wire shape,
/// routed by URL path segment. An account whose name contains "down" gets a
/// 503 — the DAG failover trigger.
#[derive(Debug, Default)]
pub struct MockTransport;

impl MockTransport {
    /// Deterministic pseudo token count: ~1 token per 4 chars, min 1.
    fn tokens(s: &str) -> i64 {
        ((s.chars().count() as i64) / 4).max(1)
    }

    fn last_user_text(messages: &[Value]) -> String {
        messages
            .iter()
            .rev()
            .find(|m| m["role"] == "user")
            .and_then(|m| {
                m["content"].as_str().map(str::to_owned).or_else(|| {
                    m["content"].as_array().map(|blocks| {
                        blocks
                            .iter()
                            .filter_map(|b| b["text"].as_str())
                            .collect::<String>()
                    })
                })
            })
            .unwrap_or_default()
    }

    fn parse(body: &[u8], what: &str) -> GResult<Value> {
        serde_json::from_slice(body).map_err(|e| {
            GatewayError::internal(format!("mock: bad {what} request body")).with_source(e)
        })
    }

    fn ok_json(v: Value) -> GResult<UpstreamResponse> {
        Ok(UpstreamResponse {
            status: 200,
            body: UpstreamBody::Json(v.to_string().into_bytes()),
        })
    }

    /// image_url parts in the last user message (multimodal detection).
    fn image_count(messages: &[Value]) -> usize {
        messages
            .iter()
            .rev()
            .find(|m| m["role"] == "user")
            .and_then(|m| m["content"].as_array())
            .map(|parts| parts.iter().filter(|p| p["type"] == "image_url").count())
            .unwrap_or(0)
    }

    fn openai_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "openai")?;
        let model = body["model"].as_str().unwrap_or("mock-model").to_owned();
        let msgs = body["messages"].as_array().cloned().unwrap_or_default();
        let user = Self::last_user_text(&msgs);
        let images = Self::image_count(&msgs);
        let img_note = if images > 0 {
            format!("[saw {images} image(s)] ")
        } else {
            String::new()
        };
        let reply = format!("[mock-openai:{model}] {img_note}you said: {user}");
        let (pt, ct) = (Self::tokens(&user) + 3, Self::tokens(&reply));

        // tools present → the model requests a call to the first tool
        let tools = body["tools"].as_array().cloned().unwrap_or_default();
        if let Some(first_tool) = tools.first() {
            let name = first_tool["function"]["name"]
                .as_str()
                .unwrap_or("tool")
                .to_owned();
            let call = json!({"id":"call-mock-1","type":"function",
                "function":{"name":name,"arguments":format!("{{\"echo\":{}}}", Value::String(user.clone()))}});
            if req.stream {
                let frames = [
                    json!({"id":"chatcmpl-mock","object":"chat.completion.chunk","created":MOCK_CREATED,"model":model,
                        "choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[call]},"finish_reason":null}]}),
                    json!({"id":"chatcmpl-mock","object":"chat.completion.chunk","created":MOCK_CREATED,"model":model,
                        "choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}],
                        "usage":{"prompt_tokens":pt,"completion_tokens":ct,"total_tokens":pt+ct}}),
                ];
                return Ok(UpstreamResponse {
                    status: 200,
                    body: UpstreamBody::Sse(Self::sse_bytes(&frames, true)),
                });
            }
            return Self::ok_json(json!({
                "id":"chatcmpl-mock","object":"chat.completion","created":MOCK_CREATED,"model":model,
                "choices":[{"index":0,
                    "message":{"role":"assistant","content":null,"tool_calls":[call]},
                    "finish_reason":"tool_calls"}],
                "usage":{"prompt_tokens":pt,"completion_tokens":ct,"total_tokens":pt+ct}
            }));
        }

        if req.stream {
            let (a, b) = Self::split_half(&reply);
            let frames = [
                json!({"id":"chatcmpl-mock","object":"chat.completion.chunk","created":MOCK_CREATED,"model":model,
                    "choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}),
                json!({"id":"chatcmpl-mock","object":"chat.completion.chunk","created":MOCK_CREATED,"model":model,
                    "choices":[{"index":0,"delta":{"content":a},"finish_reason":null}]}),
                json!({"id":"chatcmpl-mock","object":"chat.completion.chunk","created":MOCK_CREATED,"model":model,
                    "choices":[{"index":0,"delta":{"content":b},"finish_reason":null}]}),
                json!({"id":"chatcmpl-mock","object":"chat.completion.chunk","created":MOCK_CREATED,"model":model,
                    "choices":[{"index":0,"delta":{},"finish_reason":"stop"}],
                    "usage":{"prompt_tokens":pt,"completion_tokens":ct,"total_tokens":pt+ct}}),
            ];
            Ok(UpstreamResponse {
                status: 200,
                body: UpstreamBody::Sse(Self::sse_bytes(&frames, true)),
            })
        } else {
            Self::ok_json(json!({
                "id": "chatcmpl-mock", "object": "chat.completion", "created": MOCK_CREATED,
                "model": model,
                "choices": [{"index":0, "message": {"role":"assistant","content":reply}, "finish_reason":"stop"}],
                "usage": {"prompt_tokens": pt, "completion_tokens": ct, "total_tokens": pt+ct}
            }))
        }
    }

    /// Two stream deltas, split at a char boundary — the reply embeds user
    /// text, and a byte-midpoint split would panic the pipeline task.
    fn split_half(s: &str) -> (&str, &str) {
        let mut mid = s.len() / 2;
        while !s.is_char_boundary(mid) {
            mid -= 1;
        }
        s.split_at(mid)
    }

    fn sse_bytes(frames: &[Value], done: bool) -> Vec<u8> {
        let mut sse = String::new();
        for f in frames {
            sse.push_str("data: ");
            sse.push_str(&f.to_string());
            sse.push_str("\n\n");
        }
        if done {
            sse.push_str("data: [DONE]\n\n");
        }
        sse.into_bytes()
    }

    fn anthropic_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "anthropic")?;
        let model = body["model"].as_str().unwrap_or("mock-claude").to_owned();
        let user = Self::last_user_text(body["messages"].as_array().unwrap_or(&vec![]));
        let sys = body["system"].as_str().unwrap_or_default();
        let sys_note = if sys.is_empty() {
            String::new()
        } else {
            format!("[sys:{sys}] ")
        };
        let reply = format!("[mock-anthropic:{model}] {sys_note}you said: {user}");
        let (it, ot) = (Self::tokens(&user) + 3, Self::tokens(&reply));

        // tools present → tool_use block reply
        let tools = body["tools"].as_array().cloned().unwrap_or_default();
        if let Some(first_tool) = tools.first() {
            let name = first_tool["name"].as_str().unwrap_or("tool").to_owned();
            return Self::ok_json(json!({
                "id": "msg-mock", "type": "message", "role": "assistant", "model": model,
                "content": [{"type":"tool_use","id":"tu-mock-1","name":name,"input":{"echo":user}}],
                "stop_reason": "tool_use",
                "usage": {"input_tokens": it, "output_tokens": ot}
            }));
        }

        if req.stream {
            // standard anthropic streaming event sequence
            let (a, b) = Self::split_half(&reply);
            let frames = [
                json!({"type":"message_start","message":{"id":"msg-mock","type":"message",
                    "role":"assistant","model":model,"content":[],"stop_reason":null,
                    "usage":{"input_tokens":it,"output_tokens":0}}}),
                json!({"type":"content_block_start","index":0,
                    "content_block":{"type":"text","text":""}}),
                json!({"type":"content_block_delta","index":0,
                    "delta":{"type":"text_delta","text":a}}),
                json!({"type":"content_block_delta","index":0,
                    "delta":{"type":"text_delta","text":b}}),
                json!({"type":"content_block_stop","index":0}),
                json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},
                    "usage":{"output_tokens":ot}}),
                json!({"type":"message_stop"}),
            ];
            return Ok(UpstreamResponse {
                status: 200,
                body: UpstreamBody::Sse(Self::sse_bytes(&frames, false)),
            });
        }

        Self::ok_json(json!({
            "id": "msg-mock", "type": "message", "role": "assistant", "model": model,
            "content": [{"type":"text","text":reply}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": it, "output_tokens": ot}
        }))
    }

    fn dashscope_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "dashscope")?;
        let user = Self::last_user_text(body["input"]["messages"].as_array().unwrap_or(&vec![]));
        let reply = format!("[mock-dashscope] you said: {user}");
        let (it, ot) = (Self::tokens(&user) + 3, Self::tokens(&reply));
        if req.stream {
            // real wire: LF framing, `data:` without a space, id:/event:/comment
            // lines, finish_reason the literal string "null" until the final
            // frame, usage cumulative per frame
            let (a, b) = Self::split_half(&reply);
            let frame = |i: usize, content: &str, fr: &str, out: i64| {
                format!(
                    "id:{i}\nevent:result\n:HTTP_STATUS/200\ndata:{}\n\n",
                    json!({"output":{"choices":[{"message":{"content":content,"role":"assistant"},
                                                 "finish_reason":fr}]},
                           "usage":{"input_tokens":it,"output_tokens":out,"total_tokens":it+out},
                           "request_id":"dashscope-mock"})
                )
            };
            let sse = format!(
                "{}{}{}",
                frame(1, a, "null", ot / 2),
                frame(2, b, "null", ot),
                frame(3, "", "stop", ot),
            );
            return Ok(UpstreamResponse {
                status: 200,
                body: UpstreamBody::Sse(sse.into_bytes()),
            });
        }
        Self::ok_json(json!({
            "output": {"choices": [{"finish_reason": "stop",
                "message": {"role": "assistant", "content": reply}}]},
            "usage": {"input_tokens": it, "output_tokens": ot, "total_tokens": it + ot},
            "request_id": "dashscope-mock"
        }))
    }

    fn ernie_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "ernie")?;
        let user = Self::last_user_text(body["messages"].as_array().unwrap_or(&vec![]));
        let reply = format!("[mock-ernie] you said: {user}");
        let (pt, ct) = (Self::tokens(&user) + 3, Self::tokens(&reply));
        Self::ok_json(json!({
            "id": "as-mock", "object": "chat.completion", "created": MOCK_CREATED,
            "result": reply, "is_truncated": false, "need_clear_history": false,
            "usage": {"prompt_tokens": pt, "completion_tokens": ct, "total_tokens": pt + ct}
        }))
    }

    fn minimax_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "minimax-v1")?;
        let user = body["messages"]
            .as_array()
            .and_then(|ms| ms.iter().rev().find(|m| m["sender_type"] == "USER"))
            .and_then(|m| m["text"].as_str())
            .unwrap_or_default()
            .to_owned();
        let reply = format!("[mock-minimax] you said: {user}");
        Self::ok_json(json!({
            "created": MOCK_CREATED, "model": body["model"],
            "reply": reply,
            "choices": [{"text": reply}],
            "usage": {"total_tokens": Self::tokens(&user) + Self::tokens(&reply) + 3},
            "base_resp": {"status_code": 0, "status_msg": ""}
        }))
    }

    fn cohere_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "cohere")?;
        let user = body["message"].as_str().unwrap_or_default().to_owned();
        let reply = format!("[mock-cohere] you said: {user}");
        Self::ok_json(json!({
            "response_id": "cohere-mock", "generation_id": "gen-mock",
            "text": reply, "finish_reason": "COMPLETE",
            "meta": {"tokens": {"input_tokens": Self::tokens(&user) + 3,
                                  "output_tokens": Self::tokens(&reply)}}
        }))
    }

    fn llama_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "llama")?;
        let prompt = body["prompt"].as_str().unwrap_or_default();
        let reply = format!(
            "[mock-llama] completion of {} chars",
            prompt.chars().count()
        );
        Self::ok_json(json!({
            "generation": reply,
            "prompt_token_count": Self::tokens(prompt),
            "generation_token_count": Self::tokens(&reply),
            "stop_reason": "stop"
        }))
    }

    /// Vertex/Gemini generateContent wire shape (SSE frames when streaming —
    /// Gemini has no `[DONE]` sentinel; the stream just ends).
    fn vertex_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "vertex")?;
        let user: String = body["contents"]
            .as_array()
            .and_then(|cs| cs.last())
            .and_then(|c| c["parts"].as_array())
            .map(|ps| ps.iter().filter_map(|p| p["text"].as_str()).collect())
            .unwrap_or_default();
        let reply = format!("[mock-vertex] you said: {user}");
        let (pt, ct) = (Self::tokens(&user) + 3, Self::tokens(&reply));
        if req.stream {
            let (a, b) = Self::split_half(&reply);
            let frames = [
                json!({"candidates":[{"content":{"role":"model","parts":[{"text": a}]},"index":0}]}),
                json!({"candidates":[{"content":{"role":"model","parts":[{"text": b}]},"index":0}]}),
                json!({"candidates":[{"content":{"role":"model","parts":[]},"finishReason":"STOP","index":0}],
                       "usageMetadata":{"promptTokenCount":pt,"candidatesTokenCount":ct,"totalTokenCount":pt+ct}}),
            ];
            return Ok(UpstreamResponse {
                status: 200,
                body: UpstreamBody::Sse(Self::sse_bytes(&frames, false)),
            });
        }
        Self::ok_json(json!({
            "candidates": [{"content": {"role": "model", "parts": [{"text": reply}]},
                             "finishReason": "STOP"}],
            "usageMetadata": {"promptTokenCount": pt, "candidatesTokenCount": ct,
                               "totalTokenCount": pt + ct}
        }))
    }

    fn embeddings_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "embeddings")?;
        let inputs: Vec<String> = match &body["input"] {
            Value::String(s) => vec![s.clone()],
            Value::Array(a) => a
                .iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect(),
            _ => vec![],
        };
        // deterministic 8-dim vector from byte sums
        let data: Vec<Value> = inputs
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let base: i64 = s.bytes().map(|b| b as i64).sum();
                let emb: Vec<f64> = (0..8)
                    .map(|k| (((base + k * 7) % 100) as f64) / 100.0)
                    .collect();
                json!({"object": "embedding", "index": i, "embedding": emb})
            })
            .collect();
        let pt: i64 = inputs.iter().map(|s| Self::tokens(s)).sum();
        Self::ok_json(json!({
            "object": "list", "data": data, "model": body["model"],
            "usage": {"prompt_tokens": pt, "total_tokens": pt}
        }))
    }

    fn image_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "image")?;
        let n = body["n"].as_i64().unwrap_or(1).clamp(1, 4);
        let data: Vec<Value> = (0..n).map(|_| json!({"b64_json": MOCK_B64})).collect();
        Self::ok_json(json!({"created": MOCK_CREATED, "data": data}))
    }

    fn audio_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "audio")?;
        if req.url.ends_with("/audio/transcriptions") {
            Self::ok_json(
                json!({"text": "[mock-stt] transcribed audio", "language": body["language"]}),
            )
        } else if req.url.ends_with("/audio/speech") {
            let chars = body["input"].as_str().map(|s| s.len()).unwrap_or(0) as i64;
            Self::ok_json(json!({"audio_b64": MOCK_B64, "characters": chars}))
        } else {
            Self::ok_json(json!({"audio_b64": MOCK_B64, "kind": "audio-other"}))
        }
    }

    fn video_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "video")?;
        Self::ok_json(json!({
            "task_id": "video-task-mock", "status": "succeeded",
            "video_url": "mock://videos/out.mp4", "prompt": body["prompt"]
        }))
    }

    fn search_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "search")?;
        let q = body["query"].as_str().unwrap_or("").to_owned();
        let n = body["count"].as_i64().unwrap_or(3).clamp(1, 10);
        let results: Vec<Value> = (0..n)
            .map(|i| {
                json!({"title": format!("result {} for {q}", i + 1),
                       "url": format!("mock://search/{}", i + 1),
                       "snippet": format!("[mock-search] snippet {} about {q}", i + 1)})
            })
            .collect();
        Self::ok_json(json!({"query": q, "results": results}))
    }

    fn passthrough_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        Self::ok_json(json!({"ok": true, "protocol": req.protocol.as_str(), "echo": body}))
    }

    /// Legacy text-completions reply (the `.../completions` endpoint):
    /// `choices[].text` (not chat's message.content), usage same as openai.
    fn completions_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "completions")?;
        let model = body["model"].as_str().unwrap_or("mock-model").to_owned();
        let prompt = body["prompt"].as_str().unwrap_or_default();
        let reply = format!("[mock-completions:{model}] you said: {prompt}");
        let (pt, ct) = (Self::tokens(prompt) + 3, Self::tokens(&reply));
        Self::ok_json(json!({
            "id": "cmpl-mock",
            "object": "text_completion",
            "created": MOCK_CREATED,
            "model": model,
            "choices": [{"text": reply, "index": 0, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": pt, "completion_tokens": ct, "total_tokens": pt + ct},
        }))
    }

    /// OpenAI Responses API reply: `output` holds a message item with
    /// output_text content; usage uses the Responses input/output dialect.
    fn responses_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "responses")?;
        // `input` may be a plain string or an array of input items.
        let input: String = match &body["input"] {
            Value::String(s) => s.clone(),
            Value::Array(items) => items
                .iter()
                .filter_map(|it| {
                    it["content"].as_str().map(str::to_owned).or_else(|| {
                        it["content"]
                            .as_array()
                            .map(|cs| cs.iter().filter_map(|c| c["text"].as_str()).collect())
                    })
                })
                .collect::<Vec<_>>()
                .join(""),
            _ => String::new(),
        };
        let model = body["model"].as_str().unwrap_or("responses");
        let reply = format!("[mock-responses:{model}] you said: {input}");
        let (it, ot) = (Self::tokens(&input) + 3, Self::tokens(&reply));
        let usage = json!({
            "input_tokens": it,
            "output_tokens": ot,
            "total_tokens": it + ot,
            "output_tokens_details": {"reasoning_tokens": 0},
        });
        if req.stream {
            // Responses streaming event sequence: text deltas, then completed.
            let (a, b) = Self::split_half(&reply);
            let frames = [
                json!({"type": "response.created", "response": {"model": model, "status": "in_progress"}}),
                json!({"type": "response.output_text.delta", "delta": a}),
                json!({"type": "response.output_text.delta", "delta": b}),
                json!({"type": "response.completed",
                    "response": {"model": model, "status": "completed", "usage": usage}}),
            ];
            return Ok(UpstreamResponse {
                status: 200,
                body: UpstreamBody::Sse(Self::sse_bytes(&frames, true)),
            });
        }
        Self::ok_json(json!({
            "id": "resp_mock",
            "object": "response",
            "model": model,
            "status": "completed",
            "output": [{
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": reply}],
            }],
            "usage": usage,
        }))
    }
}

#[async_trait::async_trait]
impl Transport for MockTransport {
    async fn send(&self, req: UpstreamRequest) -> GResult<UpstreamResponse> {
        // downtime simulation: account name containing "down" → upstream 503 (triggers DAG failover)
        if req.account.contains("down") {
            return Err(GatewayError::new(
                gw_consts::ErrCode::FED_RESP_RPC_FAILED,
                503,
                format!("mock upstream unavailable for account {}", req.account),
            ));
        }
        // name containing "erroring" → 200 body with an error envelope (propagation test)
        if req.account.contains("erroring") {
            return Self::ok_json(json!({
                "error": {"code": "400", "type": "invalid_request_error",
                          "message": "mock vendor rejected the request"}
            }));
        }
        // route by the URL path segment the family engine chose
        let u = req.url.as_str();
        if u.contains("dashscope") {
            self.dashscope_reply(&req)
        } else if u.contains("wenxinworkshop") {
            self.ernie_reply(&req)
        } else if u.contains("minimax") {
            self.minimax_reply(&req)
        } else if u.contains("cohere") {
            self.cohere_reply(&req)
        } else if u.contains("meta.llama") {
            self.llama_reply(&req)
        } else if u.contains("/messages") {
            self.anthropic_reply(&req)
        } else if u.contains(":generateContent") || u.contains(":streamGenerateContent") {
            self.vertex_reply(&req)
        } else if u.contains("/embeddings") {
            self.embeddings_reply(&req)
        } else if u.contains("/images") {
            self.image_reply(&req)
        } else if u.contains("/audio/") {
            self.audio_reply(&req)
        } else if u.contains("/videos") {
            self.video_reply(&req)
        } else if u.contains("/search") {
            self.search_reply(&req)
        } else if u.contains("/responses") {
            self.responses_reply(&req)
        } else if u.contains("/v1/completions") {
            // `/v1/chat/completions` does NOT contain `/v1/completions` — legacy endpoint only
            self.completions_reply(&req)
        } else if u.contains("/passthrough") {
            self.passthrough_reply(&req)
        } else {
            self.openai_reply(&req)
        }
    }
}
