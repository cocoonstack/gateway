//! Deterministic fake vendor behind the [`Transport`] seam: the test default
//! and the transport for endpoint-less accounts.

use std::sync::atomic::{AtomicU64, Ordering};

use gw_consts::Protocol;
use gw_models::{GResult, GatewayError};
use serde_json::{Value, json};

use crate::transport::{
    HeaderMap, MOCK_B64, MOCK_CREATED, Transport, UpstreamBody, UpstreamRequest, UpstreamResponse,
};

/// Deterministic fake vendor: parses the engine-built body and answers in the
/// vendor's wire shape; an account named "…down…" gets a 503 (the failover trigger).
#[derive(Debug, Default)]
pub struct MockTransport;

impl MockTransport {
    /// Deterministic pseudo token count: ~1 token per 4 chars, min 1.
    fn tokens(s: &str) -> i64 {
        ((s.chars().count() as i64) / 4).max(1)
    }

    fn sys_note(sys: &str) -> String {
        if sys.is_empty() {
            String::new()
        } else {
            format!("[sys:{sys}] ")
        }
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
            body: UpstreamBody::Json(bytes::Bytes::from(v.to_string())),
            headers: HeaderMap::new(),
        })
    }

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
        let model = body["model"].as_str().unwrap_or("mock-model");
        let empty = Vec::new();
        let msgs = body["messages"].as_array().unwrap_or(&empty);
        let user = Self::last_user_text(msgs);
        let images = Self::image_count(msgs);
        let img_note = if images > 0 {
            format!("[saw {images} image(s)] ")
        } else {
            String::new()
        };
        let reply = format!("[mock-openai:{model}] {img_note}you said: {user}");
        let (pt, ct) = (Self::tokens(&user) + 3, Self::tokens(&reply));

        if let Some(first_tool) = body["tools"].as_array().and_then(|t| t.first()) {
            let name = first_tool["function"]["name"].as_str().unwrap_or("tool");
            let call = json!({"id":"call-mock-1","type":"function",
                "function":{"name":name,"arguments":format!("{{\"echo\":{}}}", Value::String(user))}});
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
                    headers: HeaderMap::new(),
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
                headers: HeaderMap::new(),
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
        let model = body["model"].as_str().unwrap_or("mock-claude");
        let user = Self::last_user_text(body["messages"].as_array().unwrap_or(&vec![]));
        let sys_note = Self::sys_note(body["system"].as_str().unwrap_or_default());
        let reply = format!("[mock-anthropic:{model}] {sys_note}you said: {user}");
        let (it, ot) = (Self::tokens(&user) + 3, Self::tokens(&reply));

        if let Some(first_tool) = body["tools"].as_array().and_then(|t| t.first()) {
            let name = first_tool["name"].as_str().unwrap_or("tool");
            return Self::ok_json(json!({
                "id": "msg-mock", "type": "message", "role": "assistant", "model": model,
                "content": [{"type":"tool_use","id":"tu-mock-1","name":name,"input":{"echo":user}}],
                "stop_reason": "tool_use",
                "usage": {"input_tokens": it, "output_tokens": ot}
            }));
        }

        if req.stream {
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
                headers: HeaderMap::new(),
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
            // real wire: LF framing, `data:` sans space, id:/event: lines, "null" finish_reason
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
                headers: HeaderMap::new(),
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
            .unwrap_or_default();
        let reply = format!("[mock-minimax] you said: {user}");
        Self::ok_json(json!({
            "created": MOCK_CREATED, "model": body["model"],
            "reply": reply,
            "choices": [{"text": reply}],
            "usage": {"total_tokens": Self::tokens(user) + Self::tokens(&reply) + 3},
            "base_resp": {"status_code": 0, "status_msg": ""}
        }))
    }

    /// Bedrock InvokeModel by request protocol; a stream request answers in
    /// the EventStream wire (`chunk` events carrying the family's frames).
    fn bedrock_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        if req.protocol == Protocol::AwsConverse {
            return self.converse_reply(req);
        }
        let reply = match req.protocol {
            Protocol::AwsAnthropic => self.anthropic_reply(req)?,
            Protocol::AwsEmbed => self.embed_reply(req)?,
            Protocol::AwsLlama => self.llama_reply(req)?,
            _ => return Err(GatewayError::internal("mock bedrock protocol")),
        };
        if !req.stream {
            return Ok(reply);
        }
        let frames: Vec<Vec<u8>> = match reply.body {
            UpstreamBody::Sse(bytes) => crate::sse::SseDecoder::decode_all(&bytes)
                .map_err(GatewayError::internal)?
                .0
                .into_iter()
                .map(String::into_bytes)
                .collect(),
            UpstreamBody::Json(bytes) => {
                let mut v: Value = serde_json::from_slice(&bytes)
                    .map_err(|e| GatewayError::internal("mock bedrock reply").with_source(e))?;
                if v.get("generation").is_some() {
                    let text = v["generation"].as_str().unwrap_or_default().to_owned();
                    let (a, b) = Self::split_half(&text);
                    vec![
                        json!({"generation": a, "prompt_token_count": v["prompt_token_count"],
                               "generation_token_count": null, "stop_reason": null}),
                        json!({"generation": b, "prompt_token_count": null,
                               "generation_token_count": v["generation_token_count"],
                               "stop_reason": "stop"}),
                    ]
                } else if v.get("text").is_some() {
                    let text = v["text"].as_str().unwrap_or_default().to_owned();
                    let (a, b) = Self::split_half(&text);
                    vec![
                        json!({"is_finished": false, "event_type": "text-generation", "text": a}),
                        json!({"is_finished": false, "event_type": "text-generation", "text": b}),
                        json!({"is_finished": true, "event_type": "stream-end",
                               "finish_reason": "COMPLETE",
                               "amazon-bedrock-invocationMetrics": {
                                   "inputTokenCount": v["meta"]["tokens"]["input_tokens"].take(),
                                   "outputTokenCount": v["meta"]["tokens"]["output_tokens"].take()}}),
                    ]
                } else {
                    // a JSON answer to a stream request (the tool-use reply) stays JSON
                    return Ok(UpstreamResponse {
                        status: reply.status,
                        body: UpstreamBody::Json(bytes),
                        headers: reply.headers,
                    });
                }
                .into_iter()
                .map(|f| f.to_string().into_bytes())
                .collect()
            }
            UpstreamBody::SseStream(_) => Vec::new(),
        };
        let mut wire = Vec::new();
        for frame in frames {
            wire.extend(crate::eventstream::encode_event(
                "chunk",
                &crate::eventstream::chunk_payload(&frame),
            ));
        }
        Ok(UpstreamResponse {
            status: 200,
            body: UpstreamBody::SseStream(Box::pin(futures::stream::once(async move {
                Ok(bytes::Bytes::from(wire))
            }))),
            headers: reply.headers,
        })
    }

    /// Bedrock Converse: `{system, messages[{role, content[{text}]}], toolConfig}`
    /// → `{output.message, stopReason, usage}`; a stream is the typed
    /// EventStream sequence (messageStart … metadata).
    fn converse_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "converse")?;
        let model = req
            .url
            .rsplit("/model/")
            .next()
            .and_then(|rest| rest.split('/').next())
            .unwrap_or("mock");
        let user = body["messages"]
            .as_array()
            .and_then(|ms| ms.iter().rev().find(|m| m["role"] == "user"))
            .and_then(|m| m["content"][0]["text"].as_str())
            .unwrap_or_default()
            .to_owned();
        let sys_note = Self::sys_note(body["system"][0]["text"].as_str().unwrap_or_default());
        let reply = format!("[mock-converse:{model}] {sys_note}you said: {user}");
        let (it, ot) = (Self::tokens(&user) + 3, Self::tokens(&reply));
        if let Some(tool) = body["toolConfig"]["tools"][0]["toolSpec"]["name"].as_str() {
            return Self::ok_json(json!({
                "output": {"message": {"role": "assistant", "content": [
                    {"toolUse": {"toolUseId": "tu-mock-1", "name": tool, "input": {"echo": user}}}]}},
                "stopReason": "tool_use",
                "usage": {"inputTokens": it, "outputTokens": ot, "totalTokens": it + ot}
            }));
        }
        if !req.stream {
            return Self::ok_json(json!({
                "output": {"message": {"role": "assistant", "content": [{"text": reply}]}},
                "stopReason": "end_turn",
                "usage": {"inputTokens": it, "outputTokens": ot, "totalTokens": it + ot}
            }));
        }
        let (a, b) = Self::split_half(&reply);
        let mut wire = Vec::new();
        for (event_type, payload) in [
            ("messageStart", json!({"role": "assistant"})),
            (
                "contentBlockDelta",
                json!({"contentBlockIndex": 0, "delta": {"text": a}}),
            ),
            (
                "contentBlockDelta",
                json!({"contentBlockIndex": 0, "delta": {"text": b}}),
            ),
            ("contentBlockStop", json!({"contentBlockIndex": 0})),
            ("messageStop", json!({"stopReason": "end_turn"})),
            (
                "metadata",
                json!({"usage": {"inputTokens": it, "outputTokens": ot, "totalTokens": it + ot}}),
            ),
        ] {
            wire.extend(crate::eventstream::encode_event(
                event_type,
                payload.to_string().as_bytes(),
            ));
        }
        Ok(UpstreamResponse {
            status: 200,
            body: UpstreamBody::SseStream(Box::pin(futures::stream::once(async move {
                Ok(bytes::Bytes::from(wire))
            }))),
            headers: HeaderMap::new(),
        })
    }

    fn embed_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "embed")?;
        if let Some(text) = body["inputText"].as_str() {
            return Self::ok_json(json!({"embedding": [0.1, 0.2, 0.3],
                "inputTextTokenCount": Self::tokens(text)}));
        }
        let n = body["texts"].as_array().map_or(0, Vec::len);
        let mut reply = Self::ok_json(json!({"embeddings": vec![vec![0.1, 0.2, 0.3]; n]}))?;
        // real embed replies carry ONLY the input-count header, never the output one
        reply.headers.insert(
            "x-amzn-bedrock-input-token-count",
            reqwest::header::HeaderValue::from(n as u64 * 4),
        );
        Ok(reply)
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
                headers: HeaderMap::new(),
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
        let inputs: Vec<&str> = match &body["input"] {
            Value::String(s) => vec![s.as_str()],
            Value::Array(a) => a.iter().filter_map(Value::as_str).collect(),
            _ => vec![],
        };
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
        // edits arrive as multipart; the `n` text part is the only field read
        let n = if req.body.starts_with(b"--") {
            Self::form_field(&req.body, "n")
                .and_then(|n| n.parse::<i64>().ok())
                .unwrap_or(1)
        } else {
            Self::parse(&req.body, "image")?["n"].as_i64().unwrap_or(1)
        }
        .clamp(1, 4);
        let data: Vec<Value> = (0..n).map(|_| json!({"b64_json": MOCK_B64})).collect();
        Self::ok_json(json!({"created": MOCK_CREATED, "data": data}))
    }

    /// A text field of a multipart body: the bytes between the part header's
    /// blank line and the next CRLF.
    fn form_field<'a>(body: &'a [u8], name: &str) -> Option<&'a str> {
        let text = std::str::from_utf8(body).ok()?;
        let marker = format!("name=\"{name}\"\r\n\r\n");
        let start = text.find(&marker)? + marker.len();
        let end = text[start..].find("\r\n")? + start;
        Some(&text[start..end])
    }

    fn moderations_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "moderations")?;
        let results: Vec<Value> = body["input"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .map(|i| {
                        let flagged = i.as_str().is_some_and(|s| s.contains("unsafe"));
                        json!({"flagged": flagged, "categories": {"unsafe": flagged}})
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self::ok_json(json!({"id": "modr-mock", "model": body["model"], "results": results}))
    }

    fn rerank_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        let body = Self::parse(&req.body, "rerank")?;
        let query = body["query"].as_str().unwrap_or_default().to_lowercase();
        let mut scored: Vec<(usize, f64)> = body["documents"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .enumerate()
                    .map(|(i, d)| {
                        let doc = d.as_str().unwrap_or_default().to_lowercase();
                        let hits = query.split_whitespace().filter(|w| doc.contains(w)).count();
                        (i, hits as f64)
                    })
                    .collect()
            })
            .unwrap_or_default();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        let top_n = body["top_n"].as_u64().unwrap_or(scored.len() as u64) as usize;
        let results: Vec<Value> = scored
            .into_iter()
            .take(top_n)
            .map(|(index, relevance_score)| json!({"index": index, "relevance_score": relevance_score}))
            .collect();
        Self::ok_json(json!({
            "results": results,
            "usage": {"total_tokens": (query.len() as i64 / 4).max(1)}
        }))
    }

    fn audio_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        if req.url.ends_with("/audio/transcriptions") {
            let language = Self::form_field(&req.body, "language");
            return Self::ok_json(json!({
                "text": "[mock-stt] transcribed audio", "language": language,
                "usage": {"type": "duration", "seconds": 5}
            }));
        } else if req.url.ends_with("/audio/translations") {
            return Self::ok_json(json!({"text": "[mock-stt] translated audio"}));
        }
        let body = Self::parse(&req.body, "audio")?;
        if req.url.ends_with("/audio/speech") {
            let chars = body["input"].as_str().map(|s| s.len()).unwrap_or(0) as i64;
            Self::ok_json(json!({"audio_b64": MOCK_B64, "characters": chars}))
        } else {
            Self::ok_json(json!({"audio_b64": MOCK_B64, "kind": "audio-other"}))
        }
    }

    /// Async hosts answer a handle on submit; a poll's state is spelled by the
    /// id (`…pending…`, `…failed…`, else done). One dialect per mock host.
    fn video_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let slug = |v: &Value| -> String {
            let s: String = v["prompt"]
                .as_str()
                .or_else(|| v["input"]["prompt"].as_str())
                .unwrap_or_default()
                .chars()
                .map(|c| if c.is_alphanumeric() { c } else { '-' })
                .collect();
            format!("{s}-{}", SEQ.fetch_add(1, Ordering::Relaxed))
        };
        if req.url.contains("openai.com") {
            if req.url.ends_with("/content") {
                return Ok(UpstreamResponse {
                    status: 200,
                    body: UpstreamBody::Json(bytes::Bytes::from_static(b"MOCK-MP4")),
                    headers: HeaderMap::new(),
                });
            }
            if req.method == "GET" {
                let id = req.url.rsplit('/').next().unwrap_or_default();
                return Self::ok_json(if id.contains("pending") {
                    json!({"id": id, "object": "video", "status": "in_progress", "progress": 40})
                } else {
                    json!({"id": id, "object": "video", "status": "completed",
                           "progress": 100, "seconds": "4", "size": "720x1280"})
                });
            }
            let body = Self::parse(&req.body, "video")?;
            return Self::ok_json(json!({
                "id": format!("video_{}", slug(&body)), "object": "video",
                "status": "queued", "progress": 0,
                "seconds": body["seconds"], "size": body["size"]
            }));
        }
        if req.url.contains("/videos/text2video/") {
            let id = req.url.rsplit('/').next().unwrap_or_default();
            return Self::ok_json(if id.contains("pending") {
                json!({"code": 0, "request_id": "kling-trace",
                       "data": {"task_id": id, "task_status": "processing"}})
            } else {
                json!({"code": 0, "request_id": "kling-trace",
                       "data": {"task_id": id, "task_status": "succeed",
                                 "task_result": {"videos": [{"id": "v1", "duration": "5",
                                     "url": format!("mock://videos/{id}.mp4")}]}}})
            });
        }
        if req.url.contains("/videos/text2video") {
            let body = Self::parse(&req.body, "video")?;
            if body["model_name"].is_null() {
                return Self::ok_json(json!({"code": 1200, "message": "model_name is required"}));
            }
            return Self::ok_json(json!({"code": 0, "request_id": "kling-trace",
                                        "data": {"task_id": format!("kl-{}", slug(&body)),
                                                  "task_status": "submitted"}}));
        }
        if req.url.contains("/video/submit") {
            let body = Self::parse(&req.body, "video")?;
            return Self::ok_json(json!({"requestId": format!("sf-{}", slug(&body))}));
        }
        if req.url.contains("/video/status") {
            let body = Self::parse(&req.body, "video")?;
            let id = body["requestId"].as_str().unwrap_or_default();
            return Self::ok_json(if id.contains("pending") {
                json!({"status": "InProgress"})
            } else {
                json!({"status": "Succeed",
                       "results": {"videos": [{"url": format!("mock://videos/{id}.mp4")}],
                                    "seed": 7}})
            });
        }
        if req.url.contains("video-synthesis") {
            let body = Self::parse(&req.body, "video")?;
            return Self::ok_json(json!({"output": {
                "task_id": format!("ds-{}", slug(&body)), "task_status": "PENDING"
            }, "request_id": "mock-ds"}));
        }
        if req.url.contains("/api/v1/tasks/") {
            let id = req.url.rsplit('/').next().unwrap_or_default();
            return Self::ok_json(if id.contains("pending") {
                json!({"output": {"task_id": id, "task_status": "RUNNING"}})
            } else {
                json!({"output": {"task_id": id, "task_status": "SUCCEEDED",
                                   "video_url": format!("mock://videos/{id}.mp4")},
                       "usage": {"video_duration": 5, "video_count": 1}})
            });
        }
        if req.url.contains("/files/retrieve") {
            let id = req.url.rsplit('=').next().unwrap_or_default();
            return Self::ok_json(json!({
                "file": {"file_id": id, "purpose": "video_generation",
                          "download_url": format!("mock://video-cdn.minimax.io/{id}.mp4")},
                "base_resp": {"status_code": 0}
            }));
        }
        if req.url.contains("video-cdn") {
            return Ok(UpstreamResponse {
                status: 200,
                body: UpstreamBody::Json(bytes::Bytes::from_static(b"MOCK-MP4")),
                headers: HeaderMap::new(),
            });
        }
        if req.url.contains("/query/video_generation") {
            let id = req.url.rsplit('=').next().unwrap_or_default();
            return Self::ok_json(if id.contains("pending") {
                json!({"task_id": id, "status": "Processing", "base_resp": {"status_code": 0}})
            } else {
                json!({"task_id": id, "status": "Success", "file_id": format!("file-{id}"),
                       "base_resp": {"status_code": 0}})
            });
        }
        if req.url.contains("/video_generation") {
            let body = Self::parse(&req.body, "video")?;
            return Self::ok_json(json!({"task_id": format!("mm-{}", slug(&body)),
                                        "base_resp": {"status_code": 0}}));
        }
        if req.method == "GET" {
            let id = req.url.rsplit('/').next().unwrap_or_default();
            return Self::ok_json(if id.contains("pending") {
                json!({"status": "pending", "progress": 40})
            } else if id.contains("failed") {
                json!({"status": "failed", "error": "mock generation failed"})
            } else {
                json!({"status": "done", "progress": 100,
                       "video": {"url": format!("mock://videos/{id}.mp4"), "duration": 2},
                       "usage": {"cost_in_usd_ticks": 1_600_000_000}})
            });
        }
        let body = Self::parse(&req.body, "video")?;
        if req.url.contains("x.ai") {
            return Self::ok_json(json!({"request_id": format!("vid-{}", slug(&body))}));
        }
        Self::ok_json(json!({
            "task_id": "video-task-mock", "status": "succeeded",
            "video_url": "mock://videos/out.mp4", "prompt": body["prompt"]
        }))
    }

    fn search_reply(&self, req: &UpstreamRequest) -> GResult<UpstreamResponse> {
        if req.url.contains("/res/v1/web/search") {
            let params = req.url.split_once('?').map_or("", |(_, params)| params);
            let param = |name| {
                params.split('&').find_map(|pair| {
                    let (key, value) = pair.split_once('=')?;
                    (key == name).then_some(value)
                })
            };
            let q = param("q")
                .map(|q| {
                    percent_encoding::percent_decode_str(q)
                        .decode_utf8_lossy()
                        .into_owned()
                })
                .unwrap_or_default();
            let count = param("count")
                .and_then(|count| count.parse::<i64>().ok())
                .unwrap_or(3)
                .clamp(1, 20);
            let results: Vec<Value> = (0..count)
                .map(|i| {
                    json!({"title": format!("brave result {} for {q}", i + 1),
                           "url": format!("mock://brave/{}", i + 1),
                           "description": format!("[mock-brave] about {q}")})
                })
                .collect();
            return Self::ok_json(json!({"query": {"original": q},
                                        "web": {"results": results}}));
        }
        let body = Self::parse(&req.body, "search")?;
        let q = body["query"].as_str().unwrap_or("");
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
        let model = body["model"].as_str().unwrap_or("mock-model");
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
        let input: std::borrow::Cow<str> = match &body["input"] {
            Value::String(s) => s.as_str().into(),
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
                .join("")
                .into(),
            _ => "".into(),
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
                headers: HeaderMap::new(),
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
        // an account named …down… answers 503 (the DAG failover trigger)
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
        if req.protocol == Protocol::Video {
            return self.video_reply(&req);
        }
        if req.protocol == Protocol::Search {
            return self.search_reply(&req);
        }
        let u = req.url.as_str();
        if u.contains("/model/") {
            self.bedrock_reply(&req)
        } else if u.contains("dashscope") {
            self.dashscope_reply(&req)
        } else if u.contains("wenxinworkshop") {
            self.ernie_reply(&req)
        } else if u.contains("minimax") {
            self.minimax_reply(&req)
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
        } else if u.contains("/moderations") {
            self.moderations_reply(&req)
        } else if u.contains("/rerank") {
            self.rerank_reply(&req)
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
