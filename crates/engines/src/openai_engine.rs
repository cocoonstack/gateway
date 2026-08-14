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

        if let Some(gw_models::TypedParams::Chat(p)) = self.base.take_typed() {
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
            if let Some(v) = p.stop {
                body.insert("stop".into(), v);
            }
            if let Some(v) = p.tools {
                body.insert("tools".into(), normalize_tools_openai(v));
            }
            if let Some(v) = p.tool_choice {
                body.insert("tool_choice".into(), v);
            }
            if let Some(v) = p.response_format {
                body.insert("response_format".into(), v);
            }
            if let Some(s) = p.system {
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
            replay_account: self.base.replay_account(),
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
        let usage = v.pointer_mut("/usage").map(Value::take).unwrap_or_default();
        apply_openai_usage(&mut resp, usage);
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
    let mut tool_calls = None;
    if let Some(delta) = v
        .get_mut("choices")
        .and_then(|choices| choices.get_mut(0))
        .and_then(|choice| choice.get_mut("delta"))
    {
        if let Some(Value::String(text)) = delta.get_mut("content").map(Value::take)
            && !text.is_empty()
        {
            full.push_str(&text);
            chunks.push(StreamChunk {
                delta: text,
                finish_reason: None,
                ..Default::default()
            });
        }
        tool_calls = delta
            .get_mut("tool_calls")
            .map(Value::take)
            .filter(|t| !t.is_null());
    }
    if let Some(mut tool_calls) = tool_calls {
        withhold_block_open_arguments(&mut tool_calls);
        merge_tool_call_fragments(&mut resp.tool_calls, &tool_calls);
        chunks.push(StreamChunk {
            tool_calls: Some(tool_calls),
            ..Default::default()
        });
    }
    if let Some(fr) = v["choices"][0]["finish_reason"].as_str() {
        if let Some(chunk) = reemit_withheld_arguments(resp.tool_calls.as_mut()) {
            chunks.push(chunk);
        }
        resp.finish_reason = fr.to_owned();
        chunks.push(StreamChunk {
            delta: String::new(),
            finish_reason: Some(fr.to_owned()),
            ..Default::default()
        });
    }
    if let Some(usage) = v.pointer_mut("/usage") {
        apply_openai_usage(resp, usage.take());
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
        // an index-less fragment continues the open call; indices stay contiguous
        let idx = f["index"]
            .as_u64()
            .map_or(calls.len().saturating_sub(1), |i| i as usize)
            .min(calls.len());
        if idx == calls.len() {
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

/// Hold back the `arguments: "{}"` some vendors open a tool call with — the
/// client would otherwise concatenate it onto the real object that follows.
fn withhold_block_open_arguments(fragment: &mut Value) {
    let Some(frags) = fragment.as_array_mut() else {
        return;
    };
    let opened = |field: &Value| field.as_str().is_some_and(|s| !s.is_empty());
    for f in frags {
        if !f.get("id").is_some_and(opened)
            && !f
                .get("function")
                .and_then(|func| func.get("name"))
                .is_some_and(opened)
        {
            continue;
        }
        if let Some(function) = f.get_mut("function").and_then(Value::as_object_mut)
            && function.get("arguments").and_then(Value::as_str) == Some("{}")
        {
            function.remove("arguments");
        }
    }
}

/// Close out [`withhold_block_open_arguments`]: a call whose arguments are
/// still missing or empty at finish really took none — deliver its `{}`.
fn reemit_withheld_arguments(acc: Option<&mut Value>) -> Option<StreamChunk> {
    let calls = acc?.as_array_mut()?;
    let mut withheld = Vec::new();
    for (index, call) in calls.iter_mut().enumerate() {
        let Some(function) = call.get_mut("function").and_then(Value::as_object_mut) else {
            continue;
        };
        let absent = match function.get("arguments") {
            None => true,
            Some(Value::String(s)) => s.trim().is_empty(),
            Some(_) => false,
        };
        if absent {
            function.insert("arguments".to_owned(), json!("{}"));
            withheld.push(json!({"index": index, "function": {"arguments": "{}"}}));
        }
    }
    (!withheld.is_empty()).then(|| StreamChunk {
        tool_calls: Some(Value::Array(withheld)),
        ..Default::default()
    })
}

/// Tool definitions in the OpenAI wire shape. Cross-protocol requests carry
/// anthropic-shaped defs ({name, description, input_schema}) — wrap those into
/// the function envelope; native defs pass through.
fn normalize_tools_openai(tools: Value) -> Value {
    let arr = match tools {
        Value::Array(arr) => arr,
        other => return other,
    };
    Value::Array(
        arr.into_iter()
            .map(|mut t| {
                if t.get("input_schema").is_some() && t.get("function").is_none() {
                    json!({"type": "function", "function": {
                        "name": t["name"].take(),
                        "description": t["description"].take(),
                        "parameters": t["input_schema"].take(),
                    }})
                } else {
                    t
                }
            })
            .collect(),
    )
}

/// Copy token fields + keep the raw usage subtree bytes for the DAG node.
fn apply_openai_usage(resp: &mut GatewayResponse, usage: Value) {
    if usage.is_null() {
        return;
    }
    // floor upstream counts so a negative can't refund quota or bill negative
    resp.prompt_tokens = crate::engine::tok(&usage["prompt_tokens"]);
    resp.completion_tokens = crate::engine::tok(&usage["completion_tokens"]);
    resp.total_tokens = crate::engine::tok(&usage["total_tokens"]);
    resp.raw_usage = Some(usage);
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
    fn an_index_less_fragment_continues_the_open_call() {
        let mut acc = None;
        merge_tool_call_fragments(
            &mut acc,
            &json!([{"id":"call_1","function":{"name":"shell","arguments":"{\"a"}}]),
        );
        merge_tool_call_fragments(&mut acc, &json!([{"function":{"arguments":"\":1}"}}]));
        let calls = acc.unwrap();
        assert_eq!(calls.as_array().unwrap().len(), 1);
        assert_eq!(calls[0]["function"]["arguments"], "{\"a\":1}");
    }

    #[test]
    fn a_far_index_extends_the_accumulator_by_one_slot() {
        let mut acc = None;
        merge_tool_call_fragments(
            &mut acc,
            &json!([{"index":900_000_000,"function":{"arguments":"{}"}}]),
        );
        assert_eq!(acc.unwrap().as_array().unwrap().len(), 1);
    }

    #[test]
    fn a_split_block_open_never_reaches_the_wire() {
        let (chunks, resp) = stream_events([
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"shell","arguments":"{}"}}]}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"command\":\"ls\"}"}}]}}]}),
            json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
        ]);
        assert_eq!(emitted_arguments(&chunks), "{\"command\":\"ls\"}");
        let opener = chunks[0].tool_calls.as_ref().unwrap();
        assert_eq!(opener[0]["id"], "call_1");
        assert_eq!(opener[0]["function"]["name"], "shell");
        assert_eq!(
            resp.tool_calls.unwrap()[0]["function"]["arguments"],
            "{\"command\":\"ls\"}"
        );
    }

    #[test]
    fn a_conforming_no_arg_call_is_not_doubled() {
        let (chunks, resp) = stream_events([
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"now","arguments":""}}]}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{}"}}]}}]}),
            json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
        ]);
        assert_eq!(emitted_arguments(&chunks), "{}");
        assert_eq!(resp.tool_calls.unwrap()[0]["function"]["arguments"], "{}");
    }

    #[test]
    fn conforming_argument_fragments_still_stream_one_by_one() {
        let (chunks, _) = stream_events([
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"shell","arguments":""}}]}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"comm"}}]}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"and\":\"ls\"}"}}]}}]}),
            json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
        ]);
        assert_eq!(chunks.iter().filter(|c| c.tool_calls.is_some()).count(), 3);
        assert_eq!(emitted_arguments(&chunks), "{\"command\":\"ls\"}");
    }

    #[test]
    fn a_vendor_that_never_sends_arguments_gets_them_at_finish() {
        let (chunks, resp) = stream_events([
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"now","arguments":""}}]}}]}),
            json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
        ]);
        assert_eq!(emitted_arguments(&chunks), "{}");
        assert_eq!(resp.tool_calls.unwrap()[0]["function"]["arguments"], "{}");
    }

    #[test]
    fn a_no_argument_call_delivers_its_empty_object_once() {
        let (chunks, resp) = stream_events([
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"now","arguments":"{}"}}]}}]}),
            json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
            json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}],"usage":{"total_tokens":3}}),
        ]);
        assert_eq!(emitted_arguments(&chunks), "{}");
        assert_eq!(resp.tool_calls.unwrap()[0]["function"]["arguments"], "{}");
    }

    fn stream_events<const N: usize>(events: [Value; N]) -> (Vec<StreamChunk>, GatewayResponse) {
        let mut resp = GatewayResponse::default();
        let mut full = String::new();
        let mut chunks = Vec::new();
        for event in events {
            chunks.extend(apply_sse_event(event, 200, &mut resp, &mut full).unwrap());
        }
        (chunks, resp)
    }

    fn emitted_arguments(chunks: &[StreamChunk]) -> String {
        chunks
            .iter()
            .filter_map(|chunk| chunk.tool_calls.as_ref()?.as_array())
            .flatten()
            .filter_map(|call| call.pointer("/function/arguments")?.as_str())
            .collect()
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
