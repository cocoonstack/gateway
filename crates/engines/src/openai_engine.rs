//! OpenAI-protocol engine: builds the vendor chat request (full param
//! passthrough), sends it via [`Transport`], parses the JSON or SSE reply into
//! `GatewayResponse` + stream chunks, and keeps the raw usage subtree on
//! `raw_usage` for the CommonUsage DAG node.

use gw_models::{GResult, GatewayError, GatewayResponse};
use gw_protocol::reasoning::is_thinking_block;
use serde_json::{Map, Value, json};

use crate::base::base_engine;
use crate::engine::{EngineOutcome, ModelEngine, StreamChunk};
use crate::transport::{UpstreamBody, UpstreamRequest};

base_engine!(OpenAiEngine);

impl OpenAiEngine {
    /// The OpenAI wire messages, each turn's payload moved out: parts win over
    /// flat text, tool_calls and tool results pass through.
    fn wire_messages(&mut self) -> Vec<Value> {
        let mut out = Vec::new();
        for m in std::mem::take(&mut self.base.request.message) {
            match m.parts {
                // a Messages-surface turn: its thinking/tool_use/tool_result blocks have OpenAI
                // counterparts
                Some(Value::Array(parts)) if parts.iter().any(is_native_block) => {
                    native_turn(&m.role, parts, m.reasoning_content, &mut out);
                }
                parts => {
                    let mut msg = Map::new();
                    msg.insert("role".into(), m.role.into());
                    // OpenAI: assistant tool-call turns carry content: null
                    let content = match (parts, m.content) {
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
                    if let Some(reasoning) = m.reasoning_content {
                        msg.insert("reasoning_content".into(), reasoning.into());
                    }
                    if let Some(details) = m.reasoning_details {
                        msg.insert("reasoning_details".into(), details);
                    }
                    out.push(Value::Object(msg));
                }
            }
        }
        out
    }

    fn build_upstream(&mut self) -> GResult<UpstreamRequest> {
        let messages = Value::Array(self.wire_messages());
        let param = self.base.param()?;
        let protocol = param.protocol;
        let reasoning_model = openai_reasoning_model(&param.model_name);
        let mut body = Map::new();
        body.insert("model".into(), param.model_name.clone().into());
        body.insert("messages".into(), messages);
        body.insert("stream".into(), self.base.request.stream.into());
        // without this OpenAI omits usage from streams and every streaming call bills 0
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
            // OpenAI's reasoning families 400 on max_tokens; compatible vendors know only
            // max_tokens
            if reasoning_model {
                put!("max_completion_tokens", p.max_tokens);
            } else {
                put!("max_tokens", p.max_tokens);
            }
            put!(
                "reasoning_effort",
                p.reasoning
                    .and_then(|reasoning| reasoning_effort(*reasoning))
            );
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
                // a cross-protocol (anthropic→openai) request carries its system outside messages
                let mut system = Map::with_capacity(2);
                system.insert("role".into(), "system".into());
                system.insert("content".into(), Value::String(s));
                let mut msgs = vec![Value::Object(system)];
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
            method: "POST",
            url: self
                .base
                .openai_url("mock://api.openai.com", "chat/completions"),
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
        // one walk to the first choice; JSON-pointer lookups allocate per call
        let mut choice = v
            .get_mut("choices")
            .and_then(|choices| choices.get_mut(0))
            .map(Value::take)
            .unwrap_or_default();
        let mut message = match choice.get_mut("message").map(Value::take) {
            Some(Value::Object(message)) => message,
            _ => Map::new(),
        };
        let mut resp = GatewayResponse {
            message: take_str(&mut message, "content").unwrap_or_default(),
            reasoning: take_str(&mut message, "reasoning_content")
                .or_else(|| take_str(&mut message, "reasoning"))
                .unwrap_or_default(),
            reasoning_details: match message.remove("reasoning_details") {
                Some(Value::Array(details)) => Some(details),
                _ => None,
            },
            tool_calls: message.remove("tool_calls").filter(|t| !t.is_null()),
            model: match v.get_mut("model").map(Value::take) {
                Some(Value::String(model)) => model,
                _ => String::new(),
            },
            finish_reason: match choice.get_mut("finish_reason").map(Value::take) {
                Some(Value::String(finish)) => finish,
                _ => String::new(),
            },
            ..Default::default()
        };
        if let Some(calls) = &mut resp.tool_calls {
            crate::engine::normalize_tool_arguments(calls);
        }
        let usage = v.get_mut("usage").map(Value::take).unwrap_or_default();
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
        .and_then(Value::as_object_mut)
    {
        if let Some(text) = take_str(delta, "content")
            && !text.is_empty()
        {
            full.push_str(&text);
            chunks.push(StreamChunk {
                delta: text,
                finish_reason: None,
                ..Default::default()
            });
        }
        let reasoning = take_str(delta, "reasoning_content")
            .or_else(|| take_str(delta, "reasoning"))
            .unwrap_or_default();
        let reasoning_details = match delta.remove("reasoning_details") {
            Some(Value::Array(details)) => Some(details),
            _ => None,
        };
        if !reasoning.is_empty() || reasoning_details.is_some() {
            resp.reasoning.push_str(&reasoning);
            chunks.push(StreamChunk {
                reasoning,
                reasoning_details,
                ..Default::default()
            });
        }
        tool_calls = delta.remove("tool_calls").filter(|t| !t.is_null());
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
    if let Some(usage) = v.get_mut("usage") {
        apply_openai_usage(resp, usage.take());
    }
    Ok(chunks)
}

/// Merge one `index`-keyed tool-call fragment: the first carries id/type/name,
/// later ones append to `function.arguments`.
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

/// Tool definitions in the OpenAI wire shape: anthropic-shaped defs are wrapped
/// into the function envelope, native defs pass through.
fn normalize_tools_openai(tools: Value) -> Value {
    let arr = match tools {
        Value::Array(arr) => arr,
        other => return other,
    };
    Value::Array(
        arr.into_iter()
            .map(|mut t| {
                // built by hand: json! would deep-copy the schema
                if t.get("input_schema").is_some() && t.get("function").is_none() {
                    let mut function = Map::with_capacity(3);
                    function.insert("name".into(), t["name"].take());
                    function.insert("description".into(), t["description"].take());
                    function.insert("parameters".into(), t["input_schema"].take());
                    let mut tool = Map::with_capacity(2);
                    tool.insert("type".into(), "function".into());
                    tool.insert("function".into(), Value::Object(function));
                    Value::Object(tool)
                } else {
                    t
                }
            })
            .collect(),
    )
}

/// The request's `reasoning_effort`: the client's own, else derived from
/// `output_config.effort`, a `thinking` budget or an OpenRouter budget;
/// `adaptive` without an effort and `disabled` leave the vendor default.
fn reasoning_effort(reasoning: gw_models::ReasoningParam) -> Option<String> {
    if let Some(effort) = reasoning.effort {
        return Some(effort);
    }
    if let Some(Value::String(effort)) = reasoning
        .output_config
        .and_then(|mut config| config.get_mut("effort").map(Value::take))
    {
        return Some(effort);
    }
    let budget = reasoning.budget_tokens.or_else(|| {
        reasoning
            .thinking
            .filter(|thinking| thinking["type"] == "enabled")
            .and_then(|thinking| thinking["budget_tokens"].as_i64())
    })?;
    Some(gw_protocol::reasoning::budget_effort(budget).to_owned())
}

/// OpenAI's own reasoning families — o-series and GPT-5 onward.
fn openai_reasoning_model(model: &str) -> bool {
    match model.as_bytes() {
        [b'o', minor, ..] => minor.is_ascii_digit(),
        [b'g', b'p', b't', b'-', major, ..] => (b'5'..=b'9').contains(major),
        _ => false,
    }
}

fn is_native_block(block: &Value) -> bool {
    is_thinking_block(block)
        || matches!(
            block["type"].as_str(),
            Some("tool_use" | "tool_result" | "image")
        )
}

/// A Messages-surface turn on the OpenAI wire: thinking → `reasoning_content`,
/// `tool_use` → `tool_calls`, `tool_result` → leading `role: tool` messages,
/// `image` → `image_url` parts.
fn native_turn(role: &str, parts: Vec<Value>, reasoning: Option<String>, out: &mut Vec<Value>) {
    let mut msg = Map::new();
    msg.insert("role".into(), role.into());
    let mut content = Vec::new();
    if role == gw_consts::role::AI {
        let mut prose = String::new();
        let mut tool_use = Vec::new();
        for part in parts {
            match part["type"].as_str() {
                Some("thinking") => prose.push_str(part["thinking"].as_str().unwrap_or_default()),
                Some("redacted_thinking") => {}
                Some("tool_use") => tool_use.push(part),
                _ => content.push(gw_protocol::anthropic::image_to_image_url(part)),
            }
        }
        if !tool_use.is_empty() {
            let calls = gw_protocol::anthropic::tool_use_to_tool_calls(tool_use, &mut 0);
            msg.insert("tool_calls".into(), Value::Array(calls));
        }
        if let Some(reasoning) = reasoning.or((!prose.is_empty()).then_some(prose)) {
            msg.insert("reasoning_content".into(), reasoning.into());
        }
    } else {
        for mut part in parts {
            if part["type"] != "tool_result" {
                content.push(gw_protocol::anthropic::image_to_image_url(part));
                continue;
            }
            let text = match part["content"].take() {
                Value::String(text) => text,
                Value::Array(blocks) => gw_protocol::anthropic::blocks_text(&blocks),
                _ => String::new(),
            };
            let mut result = Map::with_capacity(3);
            result.insert("role".into(), "tool".into());
            result.insert("tool_call_id".into(), part["tool_use_id"].take());
            result.insert("content".into(), Value::String(text));
            out.push(Value::Object(result));
        }
    }
    // content is null only on a tool-call turn; a turn left with neither is dropped
    if content.is_empty() {
        if !msg.contains_key("tool_calls") {
            return;
        }
        msg.insert("content".into(), Value::Null);
    } else {
        msg.insert("content".into(), Value::Array(content));
    }
    out.push(Value::Object(msg));
}

fn take_str(object: &mut Map<String, Value>, key: &str) -> Option<String> {
    match object.remove(key) {
        Some(Value::String(s)) => Some(s),
        _ => None,
    }
}

/// Copy token fields + keep the raw usage subtree for the DAG node.
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

    #[derive(Debug)]
    struct Reply(&'static str, bool);

    #[async_trait::async_trait]
    impl crate::transport::Transport for Reply {
        async fn send(
            &self,
            _req: crate::transport::UpstreamRequest,
        ) -> gw_models::GResult<crate::transport::UpstreamResponse> {
            let bytes = self.0.as_bytes().to_vec();
            Ok(crate::transport::UpstreamResponse {
                status: 200,
                body: if self.1 {
                    UpstreamBody::Sse(bytes)
                } else {
                    UpstreamBody::Json(bytes.into())
                },
                headers: Default::default(),
            })
        }
    }

    #[tokio::test]
    async fn non_stream_captures_reasoning_prose_and_details() {
        let body = r#"{"model":"deepseek","choices":[{"message":{"content":"391","reasoning_content":"17*23","reasoning_details":[{"type":"reasoning.text","text":"17*23"}]},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#;
        let out = OpenAiEngine::new(req(false), Arc::new(Reply(body, false)))
            .run()
            .await
            .unwrap();
        assert_eq!(out.response.message, "391");
        assert_eq!(out.response.reasoning, "17*23");
        assert_eq!(out.response.reasoning_details.unwrap()[0]["text"], "17*23");

        let alias = r#"{"model":"grok","choices":[{"message":{"content":"391","reasoning":"via reasoning"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#;
        let out = OpenAiEngine::new(req(false), Arc::new(Reply(alias, false)))
            .run()
            .await
            .unwrap();
        assert_eq!(out.response.reasoning, "via reasoning");
        assert!(out.response.reasoning_details.is_none());
    }

    #[tokio::test]
    async fn stream_forwards_reasoning_deltas_and_details() {
        let sse = concat!(
            "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"reasoning_content\":\"17*\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"reasoning\":\"23\",\"reasoning_details\":[{\"type\":\"reasoning.text\",\"text\":\"23\",\"index\":0}]}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"391\"},\"finish_reason\":\"stop\"}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":3,\"total_tokens\":4,\"completion_tokens_details\":{\"reasoning_tokens\":2}}}\n\n",
            "data: [DONE]\n\n",
        );
        let out = OpenAiEngine::new(req(true), Arc::new(Reply(sse, true)))
            .run()
            .await
            .unwrap();
        let reasoning: Vec<&str> = out.chunks.iter().map(|c| c.reasoning.as_str()).collect();
        assert_eq!(reasoning, ["17*", "23", "", ""]);
        assert_eq!(
            out.chunks[1].reasoning_details.as_ref().unwrap()[0]["text"],
            "23"
        );
        assert_eq!(out.chunks[2].delta, "391");
        assert_eq!(out.response.reasoning, "17*23");
        assert_eq!(out.response.message, "391");
    }
}
