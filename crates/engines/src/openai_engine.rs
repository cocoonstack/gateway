//! OpenAI-protocol engine: builds the vendor chat request (full param
//! passthrough), sends it via [`Transport`], parses the JSON or SSE reply into
//! `GatewayResponse` + stream chunks, and keeps the raw usage subtree on
//! `raw_usage` for the CommonUsage DAG node.

use gw_models::{GResult, GatewayError, GatewayResponse};
use serde_json::{Map, Value, json};

use crate::base::base_engine;
use crate::engine::{EngineOutcome, ModelEngine, StreamChunk};
use crate::transport::{UpstreamBody, UpstreamRequest};

base_engine!(OpenAiEngine);

impl OpenAiEngine {
    /// Rebuild the OpenAI wire message, moving each turn's payload out:
    /// multimodal parts win over flat text; assistant tool_calls and tool
    /// results pass through losslessly.
    fn wire_messages(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.base.request.message)
            .into_iter()
            .map(|m| {
                let mut msg = Map::new();
                msg.insert("role".into(), m.role.into());
                // OpenAI: assistant tool-call turns carry content: null
                let content = match (m.parts, m.content) {
                    (Some(parts), _) => parts,
                    (None, c) if c.is_empty() && m.tool_calls.is_some() => Value::Null,
                    (None, c) => c.into(),
                };
                msg.insert("content".into(), content);
                if let Some(tc) = m.tool_calls {
                    msg.insert("tool_calls".into(), tc);
                }
                if let Some(id) = m.tool_call_id {
                    msg.insert("tool_call_id".into(), id.into());
                }
                Value::Object(msg)
            })
            .collect()
    }

    fn build_upstream(&mut self) -> GResult<UpstreamRequest> {
        let messages = Value::Array(self.wire_messages());
        let param = self.base.param()?;
        let protocol = param.protocol;
        let mut body = Map::new();
        body.insert("model".into(), param.model_name.clone().into());
        body.insert("messages".into(), messages);
        body.insert("stream".into(), self.base.request.stream.into());
        // OpenAI omits usage from streamed responses UNLESS this is set — without
        // it every streaming call would bill 0 tokens.
        if self.base.request.stream {
            body.insert("stream_options".into(), json!({"include_usage": true}));
        }

        if let Some(p) = self.base.chat_params() {
            macro_rules! put {
                ($k:literal, $v:expr) => {
                    if let Some(v) = $v {
                        body.insert($k.into(), json!(v));
                    }
                };
            }
            put!("temperature", p.temperature);
            put!("top_p", p.top_p);
            put!("max_tokens", p.max_tokens);
            put!("presence_penalty", p.presence_penalty);
            put!("frequency_penalty", p.frequency_penalty);
            put!("logprobs", p.logprobs);
            put!("top_logprobs", p.top_logprobs);
            if let Some(v) = &p.stop {
                body.insert("stop".into(), v.clone());
            }
            if let Some(v) = &p.tools {
                body.insert("tools".into(), normalize_tools_openai(v));
            }
            if let Some(v) = &p.tool_choice {
                body.insert("tool_choice".into(), v.clone());
            }
            if let Some(v) = &p.response_format {
                body.insert("response_format".into(), v.clone());
            }
            if let Some(s) = &p.system {
                // openai surface's system goes through messages; injected here for
                // cross-protocol (anthropic→openai family) requests
                let mut msgs = vec![json!({"role": "system", "content": s})];
                if let Some(Value::Array(existing)) = body.remove("messages") {
                    msgs.extend(existing);
                }
                body.insert("messages".into(), Value::Array(msgs));
            }
        }
        let raw = self.base.take_raw();
        crate::base::merge_raw_extras_owned(&mut body, raw);

        Ok(UpstreamRequest {
            protocol,
            method: "POST".to_owned(),
            url: format!(
                "{}/v1/chat/completions",
                self.base.base_url("mock://api.openai.com")
            ),
            headers: self.base.bearer_headers(),
            body: crate::base::body_bytes(&Value::Object(body))?,
            stream: self.base.request.stream,
            account: self.base.account(),
        })
    }

    fn parse_json(&self, status: u16, body: &[u8]) -> GResult<EngineOutcome> {
        let mut v: Value = serde_json::from_slice(body)
            .map_err(|e| GatewayError::internal("parse openai response").with_source(e))?;
        // surface vendor error envelopes instead of silently returning empty
        if let Some(err) = crate::engine::vendor_error(status, &v) {
            return Err(err);
        }
        let mut resp = GatewayResponse {
            message: crate::engine::take_string(&mut v, "/choices/0/message/content")
                .unwrap_or_default(),
            tool_calls: v
                .pointer_mut("/choices/0/message/tool_calls")
                .map(Value::take)
                .filter(|t| !t.is_null()),
            model: crate::engine::take_string(&mut v, "/model").unwrap_or_default(),
            finish_reason: crate::engine::take_string(&mut v, "/choices/0/finish_reason")
                .unwrap_or_default(),
            ..Default::default()
        };
        if let Some(calls) = &mut resp.tool_calls {
            crate::engine::normalize_tool_arguments(calls);
        }
        apply_openai_usage(&mut resp, &v["usage"]);
        Ok(EngineOutcome::with_status(resp, status))
    }

    /// Buffered or live SSE reply through the shared pump.
    async fn run_sse(&self, status: u16, body: UpstreamBody) -> GResult<EngineOutcome> {
        let mut resp = GatewayResponse::default();
        let mut full = String::new();
        let r = crate::pump::pump_sse("openai", body, self.base.request.stream_tx.clone(), |v| {
            apply_sse_event(v, status, &mut resp, &mut full)
        })
        .await?;
        resp.message = full;
        if let Some(calls) = &mut resp.tool_calls {
            crate::engine::normalize_tool_arguments(calls);
        }
        Ok(EngineOutcome::from_pump(resp, status, r))
    }
}

#[async_trait::async_trait]
impl ModelEngine for OpenAiEngine {
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let up = self.build_upstream()?;
        let reply = self.base.transport.send(up).await?;
        match reply.body {
            UpstreamBody::Json(bytes) => self.parse_json(reply.status, &bytes),
            body => self.run_sse(reply.status, body).await,
        }
    }
}

/// Apply one decoded SSE event to the accumulating response; returns the
/// chunks the event yields.
fn apply_sse_event(
    mut v: Value,
    status: u16,
    resp: &mut GatewayResponse,
    full: &mut String,
) -> GResult<Vec<StreamChunk>> {
    if let Some(err) = crate::engine::vendor_error(status, &v) {
        return Err(err);
    }
    let mut chunks = Vec::new();
    if resp.model.is_empty() {
        resp.model = v["model"].as_str().unwrap_or_default().to_owned();
    }
    let delta = &v["choices"][0]["delta"];
    if let Some(text) = delta["content"].as_str()
        && !text.is_empty()
    {
        full.push_str(text);
        chunks.push(StreamChunk {
            delta: text.to_owned(),
            finish_reason: None,
            ..Default::default()
        });
    }
    let tool_calls = v
        .pointer_mut("/choices/0/delta/tool_calls")
        .map(Value::take)
        .filter(|t| !t.is_null());
    if let Some(mut tool_calls) = tool_calls {
        merge_tool_call_fragments(&mut resp.tool_calls, &tool_calls);
        if let Some(calls) = tool_calls.as_array_mut() {
            calls.retain_mut(|call| {
                let Some(fields) = call.as_object_mut() else {
                    return true;
                };
                if let Some(function) = fields.get_mut("function").and_then(Value::as_object_mut) {
                    function.remove("arguments");
                }
                fields.iter().any(|(name, value)| match name.as_str() {
                    "index" => false,
                    "function" => value
                        .as_object()
                        .is_none_or(|function| !function.is_empty()),
                    _ => true,
                })
            });
        }
        if tool_calls.as_array().is_none_or(|calls| !calls.is_empty()) {
            chunks.push(StreamChunk {
                tool_calls: Some(tool_calls),
                ..Default::default()
            });
        }
    }
    if let Some(fr) = v["choices"][0]["finish_reason"].as_str() {
        if let Some(calls) = resp.tool_calls.as_mut() {
            crate::engine::normalize_tool_arguments(calls);
            let arguments: Vec<Value> = calls
                .as_array()
                .into_iter()
                .flatten()
                .enumerate()
                .filter_map(|(index, call)| {
                    call.pointer("/function/arguments")
                        .and_then(Value::as_str)
                        .map(|arguments| {
                            json!({"index": index, "function": {"arguments": arguments}})
                        })
                })
                .collect();
            if !arguments.is_empty() {
                // Arguments are actionable only at finish; emitting them once lets
                // split vendor fragments be normalized without rebuilding per delta.
                chunks.push(StreamChunk {
                    tool_calls: Some(Value::Array(arguments)),
                    ..Default::default()
                });
            }
        }
        resp.finish_reason = fr.to_owned();
        chunks.push(StreamChunk {
            delta: String::new(),
            finish_reason: Some(fr.to_owned()),
            ..Default::default()
        });
    }
    if v.get("usage").map(|u| !u.is_null()).unwrap_or(false) {
        apply_openai_usage(resp, &v["usage"]);
    }
    Ok(chunks)
}

/// OpenAI streams tool calls as fragments keyed by `index`: the first fragment
/// of a call carries id/type/function.name, later ones append to
/// function.arguments. Overwriting would keep only the last fragment.
pub fn merge_tool_call_fragments(acc: &mut Option<Value>, fragment: &Value) {
    let Some(frags) = fragment.as_array() else {
        return;
    };
    let calls = acc.get_or_insert_with(|| Value::Array(Vec::new()));
    let Some(calls) = calls.as_array_mut() else {
        return;
    };
    for f in frags {
        let idx = f["index"]
            .as_u64()
            .map(|i| i as usize)
            .unwrap_or(calls.len());
        while calls.len() <= idx {
            calls.push(json!({"function": {}}));
        }
        let call = &mut calls[idx];
        for key in ["id", "type"] {
            if let Some(v) = f.get(key).filter(|v| !v.is_null())
                && call.get(key).is_none()
            {
                call[key] = v.clone();
            }
        }
        if let Some(name) = f["function"]["name"].as_str()
            && call["function"].get("name").is_none()
        {
            call["function"]["name"] = json!(name);
        }
        if let Some(args) = f["function"]["arguments"].as_str() {
            // append in place — rebuilding the string per fragment is quadratic
            if let Value::String(acc) = &mut call["function"]["arguments"] {
                acc.push_str(args);
            } else {
                call["function"]["arguments"] = json!(args);
            }
        }
    }
}

/// Tool definitions in the OpenAI wire shape. Cross-protocol requests carry
/// anthropic-shaped defs ({name, description, input_schema}) — wrap those into
/// the function envelope; native defs pass through.
fn normalize_tools_openai(tools: &Value) -> Value {
    let Some(arr) = tools.as_array() else {
        return tools.clone();
    };
    Value::Array(
        arr.iter()
            .map(|t| {
                if t.get("input_schema").is_some() && t.get("function").is_none() {
                    json!({"type": "function", "function": {
                        "name": t["name"],
                        "description": t["description"],
                        "parameters": t["input_schema"],
                    }})
                } else {
                    t.clone()
                }
            })
            .collect(),
    )
}

/// Copy token fields + keep the raw usage subtree bytes for the DAG node.
fn apply_openai_usage(resp: &mut GatewayResponse, usage: &Value) {
    if usage.is_null() {
        return;
    }
    // floor upstream counts so a negative can't refund quota or bill negative
    resp.prompt_tokens = crate::engine::tok(&usage["prompt_tokens"]);
    resp.completion_tokens = crate::engine::tok(&usage["completion_tokens"]);
    resp.total_tokens = crate::engine::tok(&usage["total_tokens"]);
    resp.raw_usage = Some(usage.clone());
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use gw_consts::Protocol;
    use gw_models::{ChatMsg, ChatParams, GatewayRequest, ModelParamV2, TypedParams};

    use super::*;
    use crate::transport::MockTransport;

    fn req(stream: bool) -> GatewayRequest {
        GatewayRequest {
            stream,
            message: vec![ChatMsg::text("user", "hello world")],
            model_param_v2: Some(ModelParamV2::with_name(Protocol::OpenaiChat, "gpt-4o")),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn non_stream_parses_message_and_usage() {
        let mut e = OpenAiEngine::new(req(false), Arc::new(MockTransport));
        let out = e.run().await.unwrap();
        assert!(out.response.message.contains("you said: hello world"));
        assert_eq!(out.response.model, "gpt-4o");
        assert!(out.response.total_tokens > 0);
        assert!(out.response.raw_usage.is_some());
        assert!(out.chunks.is_empty());
    }

    #[tokio::test]
    async fn stream_survives_non_ascii_reply() {
        for n in [40, 41, 42] {
            let text = "界".repeat(n);
            let mut r = req(true);
            r.message = vec![ChatMsg::text("user", text.as_str())];
            let mut e = OpenAiEngine::new(r, Arc::new(MockTransport));
            let out = e.run().await.unwrap();
            assert!(out.response.message.contains(&text), "n={n}");
            assert!(out.chunks.len() >= 3, "n={n}");
        }
    }

    #[tokio::test]
    async fn stream_decodes_chunks_and_final_usage() {
        let mut e = OpenAiEngine::new(req(true), Arc::new(MockTransport));
        let out = e.run().await.unwrap();
        assert!(out.chunks.len() >= 3);
        assert!(out.response.message.contains("you said: hello world"));
        assert_eq!(out.response.finish_reason, "stop");
        assert!(out.response.total_tokens > 0);
    }

    #[tokio::test]
    async fn tools_produce_tool_calls() {
        let mut r = req(false);
        if let Some(p) = r.model_param_v2.as_mut() {
            p.typed = Some(TypedParams::Chat(ChatParams {
                tools: Some(json!([{"type":"function",
                    "function":{"name":"get_weather","parameters":{}}}])),
                ..Default::default()
            }));
        }
        let mut e = OpenAiEngine::new(r, Arc::new(MockTransport));
        let out = e.run().await.unwrap();
        assert_eq!(out.response.finish_reason, "tool_calls");
        let tc = out.response.tool_calls.expect("tool calls");
        assert_eq!(tc[0]["function"]["name"], "get_weather");
    }

    #[tokio::test]
    async fn stream_tools_forward_tool_call_chunks() {
        let mut r = req(true);
        if let Some(p) = r.model_param_v2.as_mut() {
            p.typed = Some(TypedParams::Chat(ChatParams {
                tools: Some(json!([{"type":"function",
                    "function":{"name":"get_weather","parameters":{}}}])),
                ..Default::default()
            }));
        }
        let mut e = OpenAiEngine::new(r, Arc::new(MockTransport));
        let out = e.run().await.unwrap();
        assert!(
            out.chunks.iter().any(|c| c.tool_calls.is_some()),
            "stream must carry tool_calls chunks"
        );
        let tc = out.response.tool_calls.expect("accumulated tool calls");
        assert_eq!(tc[0]["function"]["name"], "get_weather");
    }

    #[test]
    fn tool_call_fragments_merge_by_index() {
        let mut acc = None;
        merge_tool_call_fragments(
            &mut acc,
            &json!([{"index":0,"id":"call_1","type":"function",
                "function":{"name":"get_weather","arguments":"{\"ci"}}]),
        );
        merge_tool_call_fragments(
            &mut acc,
            &json!([{"index":0,"function":{"arguments":"ty\":\"sf\"}"}}]),
        );
        let calls = acc.unwrap();
        assert_eq!(calls[0]["id"], "call_1");
        assert_eq!(calls[0]["function"]["name"], "get_weather");
        assert_eq!(calls[0]["function"]["arguments"], "{\"city\":\"sf\"}");
    }

    #[test]
    fn split_tool_argument_repair_reaches_stream_chunks() {
        let mut resp = GatewayResponse::default();
        let mut full = String::new();
        let events = [
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"shell","arguments":"{}"}}]}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"command\":\"ls\"}"}}]}}]}),
            json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
        ];
        let mut chunks = Vec::new();
        for event in events {
            chunks.extend(apply_sse_event(event, 200, &mut resp, &mut full).unwrap());
        }
        assert_eq!(
            resp.tool_calls.as_ref().unwrap()[0]["function"]["arguments"],
            "{\"command\":\"ls\"}"
        );
        let tool_chunks: Vec<&Value> = chunks
            .iter()
            .filter_map(|chunk| chunk.tool_calls.as_ref())
            .collect();
        assert_eq!(tool_chunks.len(), 2);
        assert_eq!(tool_chunks[0][0]["id"], "call_1");
        assert_eq!(tool_chunks[0][0]["function"]["name"], "shell");
        let emitted: String = chunks
            .iter()
            .filter_map(|chunk| {
                chunk
                    .tool_calls
                    .as_ref()?
                    .pointer("/0/function/arguments")?
                    .as_str()
            })
            .collect();
        assert_eq!(emitted, "{\"command\":\"ls\"}");
    }

    #[tokio::test]
    async fn multimodal_parts_reach_the_vendor() {
        let mut r = req(false);
        r.message = vec![ChatMsg {
            role: "user".into(),
            content: "what is this?".into(),
            parts: Some(json!([
                {"type":"text","text":"what is this?"},
                {"type":"image_url","image_url":{"url":"data:image/png;base64,xx"}}
            ])),
            ..Default::default()
        }];
        let mut e = OpenAiEngine::new(r, Arc::new(MockTransport));
        let out = e.run().await.unwrap();
        assert!(
            out.response.message.contains("[saw 1 image(s)]"),
            "{}",
            out.response.message
        );
    }

    #[tokio::test]
    async fn sampling_params_pass_through() {
        let mut r = req(false);
        if let Some(p) = r.model_param_v2.as_mut() {
            p.typed = Some(TypedParams::Chat(ChatParams {
                temperature: Some(0.3),
                max_tokens: Some(64),
                ..Default::default()
            }));
            p.raw = json!({"seed": 42});
        }
        let mut e = OpenAiEngine::new(r, Arc::new(MockTransport));
        let out = e.run().await.unwrap();
        assert!(out.response.message.contains("you said:"));
    }
}
