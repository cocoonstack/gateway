//! Anthropic-messages-protocol engine: system, tools/tool_use, multimodal text
//! blocks, and streaming (the standard anthropic SSE event sequence). Marks
//! `is_messages_protocol` so the usage extractor applies the Anthropic map.

use gw_models::{GResult, GatewayError, GatewayResponse};
use gw_protocol::reasoning::{ThinkingDialect, is_thinking_block};
use serde_json::{Map, Value, json};

use crate::base::base_engine;
use crate::engine::{EngineOutcome, ModelEngine, StreamChunk};
use crate::transport::{UpstreamBody, UpstreamRequest};

base_engine!(ClaudeEngine);

impl ClaudeEngine {
    fn build_upstream(&mut self) -> GResult<UpstreamRequest> {
        let system_text = self.base.system_text();
        let mut messages: Vec<Value> = Vec::new();
        for m in std::mem::take(&mut self.base.request.message) {
            if m.role == gw_consts::role::SYSTEM {
                continue;
            }
            let content = m.parts.unwrap_or(Value::String(m.content));
            if m.role == gw_consts::role::TOOL {
                let block = tool_result_block(m.tool_call_id, content);
                push_turn(&mut messages, "user", Value::Array(vec![block]));
                continue;
            }
            let role = if m.role == gw_consts::role::AI {
                "assistant"
            } else {
                "user"
            };
            // Claude wants replayed thinking blocks ahead of the tool_use they produced
            let thinking: Vec<Value> = match m.reasoning_details {
                Some(Value::Array(details)) => details
                    .into_iter()
                    .filter_map(gw_protocol::reasoning::detail_to_thinking_block)
                    .collect(),
                _ => Vec::new(),
            };
            let content = if thinking.is_empty() && m.tool_calls.is_none() {
                content
            } else {
                let mut blocks = thinking;
                blocks.extend(content_blocks(content));
                if let Some(Value::Array(calls)) = m.tool_calls {
                    blocks.extend(gw_protocol::anthropic::tool_calls_to_tool_use(calls));
                }
                Value::Array(blocks)
            };
            push_turn(&mut messages, role, content);
        }

        let prompt_cache = self.base.request.prompt_cache;
        if prompt_cache
            && let Some(last) = messages.last_mut()
            && last["role"] == "user"
        {
            last["content"] = Value::Array(cached_blocks(last["content"].take()));
        }
        let param = self.base.param()?;
        let protocol = param.protocol;
        let mut body = Map::new();
        body.insert("model".into(), param.model_name.clone().into());
        body.insert("messages".into(), Value::Array(messages));
        body.insert("stream".into(), self.base.request.stream.into());
        let mut max_tokens = 1024;
        // only gateway-mapped thinking drops the sampling knobs Anthropic rejects
        let mut mapped = false;
        if let Some(gw_models::TypedParams::Chat(p)) = self.base.take_typed() {
            if let Some(mt) = p.max_tokens {
                max_tokens = mt;
            }
            if let Some(reasoning) = p.reasoning {
                let reasoning = *reasoning;
                let param = self.base.param()?;
                // a client-sent thinking passthrough wins over an effort mapping
                if reasoning.thinking.is_none()
                    && param.raw.get("thinking").is_none()
                    && let Some((thinking, output_config, budget)) = map_thinking(
                        gw_protocol::reasoning::anthropic_thinking_dialect(&param.model_name),
                        reasoning.effort.as_deref(),
                        reasoning.budget_tokens,
                    )
                {
                    body.insert("thinking".into(), thinking);
                    if let Some(output_config) = output_config {
                        body.insert("output_config".into(), output_config);
                    }
                    // the answer budget rides on top of the thinking budget
                    if max_tokens <= budget {
                        max_tokens = budget.saturating_add(max_tokens);
                    }
                    mapped = true;
                }
                if let Some(thinking) = reasoning.thinking {
                    body.insert("thinking".into(), thinking);
                }
                if let Some(output_config) = reasoning.output_config {
                    body.insert("output_config".into(), output_config);
                }
            }
            if !mapped {
                if let Some(t) = p.temperature {
                    body.insert("temperature".into(), json!(t));
                }
                if let Some(t) = p.top_p {
                    body.insert("top_p".into(), json!(t));
                }
            }
            if let Some(tools) = p.tools {
                body.insert("tools".into(), normalize_tools_anthropic(tools));
            }
            if let Some(tc) = p.tool_choice {
                body.insert("tool_choice".into(), tc);
            }
            // Anthropic's field is `stop_sequences` (array), not OpenAI's `stop`
            if let Some(stop) = p.stop {
                body.insert("stop_sequences".into(), stop);
            }
        }
        body.insert("max_tokens".into(), json!(max_tokens));
        if !system_text.is_empty() {
            let system = if prompt_cache {
                Value::Array(cached_blocks(Value::String(system_text)))
            } else {
                Value::String(system_text)
            };
            body.insert("system".into(), system);
        }
        let mut raw = self.base.take_raw();
        if mapped && let Some(extra) = raw.as_object_mut() {
            extra.remove("top_k");
        }
        crate::base::merge_raw_extras_owned(&mut body, raw);

        Ok(UpstreamRequest {
            protocol,
            method: "POST".to_owned(),
            url: format!(
                "{}/v1/messages",
                self.base.base_url("mock://api.anthropic.com")
            ),
            headers: vec![
                ("content-type".into(), "application/json".into()),
                ("x-api-key".into(), self.base.api_key()),
                // Anthropic API mandates this header; a real call 400s without it.
                ("anthropic-version".into(), "2023-06-01".into()),
            ],
            body: crate::base::body_bytes(&Value::Object(body))?,
            stream: self.base.request.stream,
            account: self.base.account(),
            replay_account: self.base.replay_account(),
        })
    }

    fn parse_json(&self, status: u16, bytes: &[u8]) -> GResult<EngineOutcome> {
        let mut v: Value = serde_json::from_slice(bytes)
            .map_err(|e| GatewayError::internal("parse anthropic response").with_source(e))?;
        if let Some(err) = crate::engine::vendor_error(status, &v) {
            return Err(err);
        }
        let preserve_native = self.base.request.preserve_anthropic_wire;
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut reasoning_details: Vec<Value> = Vec::new();
        let mut tool_use: Vec<Value> = Vec::new();
        let mut content = v.get_mut("content").map(Value::take);
        if let Some(Value::Array(blocks)) = &mut content {
            for (index, b) in blocks.iter_mut().enumerate() {
                match b["type"].as_str() {
                    Some("text") => text.push_str(b["text"].as_str().unwrap_or_default()),
                    // the block stays in `content` only when the native wire is kept
                    Some("tool_use") => {
                        tool_use.push(if preserve_native { b.clone() } else { b.take() })
                    }
                    Some("thinking" | "redacted_thinking") if !preserve_native => {
                        reasoning.push_str(b["thinking"].as_str().unwrap_or_default());
                        reasoning_details.extend(gw_protocol::reasoning::thinking_block_to_detail(
                            b.take(),
                            index,
                        ));
                    }
                    _ => {}
                }
            }
        }
        let usage = &v["usage"];
        let input = crate::engine::tok(&usage["input_tokens"]);
        let output = crate::engine::tok(&usage["output_tokens"]);
        let raw_usage = v.get_mut("usage").map(Value::take).filter(|u| !u.is_null());
        let resp = GatewayResponse {
            message: text,
            reasoning,
            reasoning_details: (!reasoning_details.is_empty()).then_some(reasoning_details),
            tool_calls: if tool_use.is_empty() {
                None
            } else {
                Some(Value::Array(tool_use))
            },
            model: crate::engine::take_string(&mut v, "/model").unwrap_or_default(),
            finish_reason: crate::engine::take_string(&mut v, "/stop_reason").unwrap_or_default(),
            is_messages_protocol: true,
            prompt_tokens: input,
            completion_tokens: output,
            total_tokens: input.saturating_add(output),
            raw_usage,
            anthropic_content: if preserve_native { content } else { None },
            ..Default::default()
        };
        let mut outcome = EngineOutcome::with_status(resp, status);
        if self.base.request.stream && self.base.request.preserve_anthropic_wire {
            outcome.chunks = anthropic_native_chunks(&outcome.response, self.base.model_override());
        }
        Ok(outcome)
    }

    /// Buffered or live anthropic event sequence through the shared pump.
    async fn run_sse(&self, status: u16, body: UpstreamBody) -> GResult<EngineOutcome> {
        let mut resp = GatewayResponse {
            is_messages_protocol: true,
            ..Default::default()
        };
        let model_override = self.base.model_override().map(str::to_owned);
        let mut st = SseState::new(self.base.request.preserve_anthropic_wire, model_override);
        let r = crate::pump::pump_sse(
            "anthropic",
            body,
            self.base.request.stream_tx.clone(),
            |v| st.apply(v, status, &mut resp),
        )
        .await?;
        st.finish(&mut resp);
        Ok(EngineOutcome::from_pump(resp, status, r))
    }
}

#[async_trait::async_trait]
impl ModelEngine for ClaudeEngine {
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let up = self.build_upstream()?;
        let reply = self.base.transport.send(up).await?;
        match reply.body {
            UpstreamBody::Json(b) => self.parse_json(reply.status, &b),
            body => self.run_sse(reply.status, body).await,
        }
    }
}

/// Some compatible upstreams ignore `stream:true` and return a complete JSON
/// message. Re-emit standard native events so thinking proof is not lost at
/// the streaming surface.
pub fn anthropic_native_chunks(
    response: &GatewayResponse,
    model_override: Option<&str>,
) -> Vec<StreamChunk> {
    let stream_model = model_override.unwrap_or(&response.model);
    // Replay the upstream's own usage object so cache-token detail survives;
    // it is not on GatewayResponse yet (common_usage is a later DAG step).
    let start_usage = response
        .raw_usage
        .as_ref()
        .filter(|usage| usage.is_object())
        .cloned()
        .unwrap_or_else(|| json!({"input_tokens":response.prompt_tokens,"output_tokens":0}));
    let mut events = vec![json!({
        "type":"message_start",
        "message":{
            "type":"message",
            "role":"assistant",
            "model":stream_model,
            "content":[],
            "stop_reason":null,
            "usage":start_usage
        }
    })];
    if let Some(blocks) = response
        .anthropic_content
        .as_ref()
        .and_then(Value::as_array)
    {
        for (index, block) in blocks.iter().enumerate() {
            let mut start = block.clone();
            let mut deltas = Vec::new();
            match block.get("type").and_then(Value::as_str) {
                Some("thinking") => {
                    if let Some(object) = start.as_object_mut() {
                        object.insert("thinking".to_owned(), "".into());
                        object.insert("signature".to_owned(), "".into());
                    }
                    if let Some(thinking) = block.get("thinking").and_then(Value::as_str)
                        && !thinking.is_empty()
                    {
                        deltas.push(json!({"type":"thinking_delta","thinking":thinking}));
                    }
                    if let Some(signature) = block.get("signature").and_then(Value::as_str)
                        && !signature.is_empty()
                    {
                        deltas.push(json!({"type":"signature_delta","signature":signature}));
                    }
                }
                Some("text") => {
                    if let Some(object) = start.as_object_mut() {
                        object.insert("text".to_owned(), "".into());
                    }
                    if let Some(text) = block.get("text").and_then(Value::as_str)
                        && !text.is_empty()
                    {
                        deltas.push(json!({"type":"text_delta","text":text}));
                    }
                }
                Some("tool_use") => {
                    if let Some(object) = start.as_object_mut() {
                        object.insert("input".to_owned(), json!({}));
                    }
                    if let Some(input) = block.get("input") {
                        deltas.push(json!({
                            "type":"input_json_delta",
                            "partial_json":input.to_string()
                        }));
                    }
                }
                _ => {}
            }
            events.push(json!({
                "type":"content_block_start",
                "index":index,
                "content_block":start
            }));
            for delta in deltas {
                events.push(json!({
                    "type":"content_block_delta",
                    "index":index,
                    "delta":delta
                }));
            }
            events.push(json!({"type":"content_block_stop","index":index}));
        }
    }
    let stop_reason = if response.finish_reason.is_empty() {
        "end_turn"
    } else {
        &response.finish_reason
    };
    events.push(json!({
        "type":"message_delta",
        "delta":{"stop_reason":stop_reason,"stop_sequence":null},
        "usage":{"output_tokens":response.completion_tokens}
    }));
    events.push(json!({"type":"message_stop"}));
    events
        .into_iter()
        .map(|event| StreamChunk {
            native_event: Some(event),
            ..Default::default()
        })
        .collect()
}

/// A `role: "tool"` message as a `tool_result` block; empty output omits
/// `content` rather than sending an empty text, which the upstream rejects.
/// An OpenAI-dialect effort/budget as `(thinking, output_config, budget)` in
/// this model's dialect; `none` or unknown vocabulary leaves the model default.
fn map_thinking(
    dialect: ThinkingDialect,
    effort: Option<&str>,
    budget: Option<i64>,
) -> Option<(Value, Option<Value>, i64)> {
    use gw_protocol::reasoning::{budget_effort, effort_budget};
    if effort == Some("none") {
        return None;
    }
    let budget = budget.or_else(|| effort.and_then(effort_budget))?;
    let thinking = match dialect {
        ThinkingDialect::Budget => {
            return Some((
                json!({"type": "enabled", "budget_tokens": budget}),
                None,
                budget,
            ));
        }
        ThinkingDialect::Adaptive => json!({"type": "adaptive"}),
        ThinkingDialect::AdaptiveSummarized => {
            json!({"type": "adaptive", "display": "summarized"})
        }
    };
    let effort = match effort {
        Some("minimal") | None => budget_effort(budget),
        Some(effort) => effort,
    };
    Some((thinking, Some(json!({"effort": effort})), budget))
}

fn tool_result_block(tool_use_id: Option<String>, content: Value) -> Value {
    let mut block = Map::new();
    block.insert("type".into(), "tool_result".into());
    block.insert("tool_use_id".into(), tool_use_id.unwrap_or_default().into());
    if !is_empty_content(&content) {
        block.insert("content".into(), content);
    }
    Value::Object(block)
}

fn is_empty_content(content: &Value) -> bool {
    match content {
        Value::String(text) => text.is_empty(),
        Value::Array(blocks) => blocks.is_empty(),
        _ => true,
    }
}

fn content_blocks(content: Value) -> Vec<Value> {
    match content {
        Value::Array(blocks) => blocks,
        Value::String(text) if !text.is_empty() => {
            let mut block = Map::new();
            block.insert("type".into(), "text".into());
            block.insert("text".into(), Value::String(text));
            vec![Value::Object(block)]
        }
        _ => Vec::new(),
    }
}

/// Append a turn; empty content is dropped and same-role neighbours merge into
/// one block list, since the anthropic wire alternates roles and rejects empty
/// turns.
fn push_turn(messages: &mut Vec<Value>, role: &str, content: Value) {
    if is_empty_content(&content) {
        return;
    }
    if let Some(last) = messages.last_mut()
        && last["role"] == role
    {
        let mut blocks = content_blocks(last["content"].take());
        blocks.extend(content_blocks(content));
        last["content"] = Value::Array(blocks);
        return;
    }
    // built by hand — json! interpolation would deep-copy the moved content
    let mut msg = Map::new();
    msg.insert("role".into(), role.into());
    msg.insert("content".into(), content);
    messages.push(Value::Object(msg));
}

/// Content as blocks with a prompt-cache breakpoint on the last one; a
/// breakpoint caches everything before it (tools, system, history), so each
/// turn of a conversation re-reads the previous prefix at the cache rate. A
/// breakpoint the client already placed there wins (it may carry a ttl).
fn cached_blocks(content: Value) -> Vec<Value> {
    let mut blocks = content_blocks(content);
    if let Some(Value::Object(block)) = blocks.last_mut() {
        block
            .entry("cache_control")
            .or_insert_with(|| json!({"type": "ephemeral"}));
    }
    blocks
}

/// Tool definitions in the anthropic wire shape. Cross-protocol requests carry
/// OpenAI-shaped defs ({type:"function", function:{name, parameters}}) —
/// flatten those; native defs pass through.
fn normalize_tools_anthropic(tools: Value) -> Value {
    let arr = match tools {
        Value::Array(arr) => arr,
        other => return other,
    };
    Value::Array(
        arr.into_iter()
            .map(|mut t| {
                if let Some(f) = t.get_mut("function") {
                    json!({
                        "name": f["name"].take(),
                        "description": f["description"].take(),
                        "input_schema": f["parameters"].take(),
                    })
                } else {
                    t
                }
            })
            .collect(),
    )
}

/// Accumulating state for the anthropic streaming event sequence, shared by
/// the buffered and live decode paths.
struct SseState {
    preserve_native: bool,
    model_override: Option<String>,
    full: String,
    input: i64,
    output: i64,
    /// message_start's usage overlaid by message_delta's, for the CommonUsage step
    usage: Map<String, Value>,
    tool_blocks: Vec<Value>,
    native_blocks: Vec<Value>,
    /// the in-flight block: every block natively, thinking blocks on the chat surface
    open_native: Option<Value>,
    open_native_index: usize,
    open_native_input: String,
    /// in-flight tool_use block: (skeleton from content_block_start,
    /// accumulated input_json_delta fragments)
    open_tool: Option<(Value, String)>,
}

impl SseState {
    fn new(preserve_native: bool, model_override: Option<String>) -> Self {
        Self {
            preserve_native,
            model_override,
            full: String::new(),
            input: 0,
            output: 0,
            usage: Map::new(),
            tool_blocks: Vec::new(),
            native_blocks: Vec::new(),
            open_native: None,
            open_native_index: 0,
            open_native_input: String::new(),
            open_tool: None,
        }
    }

    fn push_visible(chunks: &mut Vec<StreamChunk>, chunk: StreamChunk) {
        if chunk.native_event.is_some()
            || !chunk.delta.is_empty()
            || !chunk.reasoning.is_empty()
            || chunk.reasoning_details.is_some()
            || chunk.tool_calls.is_some()
            || chunk.finish_reason.is_some()
        {
            chunks.push(chunk);
        }
    }

    /// Apply one decoded event; returns the chunks it yields. Takes the event
    /// by value so the native forward is a move, not a per-event deep clone.
    fn apply(
        &mut self,
        mut v: Value,
        status: u16,
        resp: &mut GatewayResponse,
    ) -> GResult<Vec<StreamChunk>> {
        if let Some(err) = crate::engine::vendor_error(status, &v) {
            return Err(err);
        }
        let mut chunks = Vec::new();
        let mut native_chunk = StreamChunk::default();
        match v["type"].as_str().unwrap_or_default() {
            "message_start" => {
                resp.model = v["message"]["model"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                self.input = v["message"]["usage"]["input_tokens"].as_i64().unwrap_or(0);
                let usage = v.get_mut("message").and_then(|m| m.get_mut("usage"));
                self.overlay_usage(usage);
            }
            "content_block_start" => {
                let index = v["index"].as_u64().unwrap_or_default() as usize;
                if self.preserve_native {
                    self.open_native = Some(v["content_block"].clone());
                    self.open_native_input.clear();
                } else if is_thinking_block(&v["content_block"]) {
                    self.open_native = Some(v["content_block"].take());
                    self.open_native_index = index;
                }
                if v["content_block"]["type"] == "tool_use" {
                    self.open_tool = Some((v["content_block"].clone(), String::new()));
                }
            }
            "content_block_delta" => {
                if let Some(t) = v["delta"]["text"].as_str() {
                    self.full.push_str(t);
                    // native consumers read the event; the delta would be dead weight
                    if !self.preserve_native {
                        native_chunk.delta = t.to_owned();
                    }
                    if let Some(block) = self.open_native.as_mut()
                        && let Some(Value::String(text)) = block.get_mut("text")
                    {
                        text.push_str(t);
                    }
                }
                // the native event keeps its delta; the chat surface moves it out
                if self.preserve_native {
                    if let Some(t) = v["delta"]["thinking"].as_str() {
                        self.append_thinking(t);
                    }
                } else if let Some(Value::String(t)) = v
                    .get_mut("delta")
                    .and_then(|delta| delta.get_mut("thinking"))
                    .map(Value::take)
                {
                    self.append_thinking(&t);
                    resp.reasoning.push_str(&t);
                    native_chunk.reasoning = t;
                }
                if let Some(signature) = v["delta"]["signature"].as_str()
                    && let Some(block) = self.open_native.as_mut()
                {
                    block["signature"] = signature.into();
                }
                if let Some(pj) = v["delta"]["partial_json"].as_str()
                    && let Some((_, buf)) = self.open_tool.as_mut()
                {
                    buf.push_str(pj);
                    if self.preserve_native {
                        self.open_native_input.push_str(pj);
                    }
                }
            }
            "content_block_stop" => {
                if let Some(mut block) = self.open_native.take() {
                    if block["type"] == "tool_use"
                        && let Ok(parsed) = serde_json::from_str::<Value>(&self.open_native_input)
                    {
                        block["input"] = parsed;
                    }
                    if self.preserve_native {
                        self.native_blocks.push(block);
                    } else {
                        native_chunk.reasoning_details =
                            gw_protocol::reasoning::thinking_block_to_detail(
                                block,
                                self.open_native_index,
                            )
                            .map(|detail| vec![detail]);
                    }
                }
                self.open_native_input.clear();
                if let Some((mut block, buf)) = self.open_tool.take() {
                    if let Ok(parsed) = serde_json::from_str::<Value>(&buf) {
                        block["input"] = parsed;
                    }
                    native_chunk.tool_calls = Some(Value::Array(vec![block.clone()]));
                    self.tool_blocks.push(block);
                }
            }
            "message_delta" => {
                if let Some(sr) = v["delta"]["stop_reason"].as_str() {
                    resp.finish_reason = sr.to_owned();
                    native_chunk.finish_reason = Some(sr.to_owned());
                }
                self.output = v["usage"]["output_tokens"].as_i64().unwrap_or(self.output);
                // Anthropic reports input_tokens in message_start; some
                // compatible vendors (MiniMax) only report it here.
                if let Some(it) = v["usage"]["input_tokens"].as_i64() {
                    self.input = it;
                }
                let usage = v.get_mut("usage");
                self.overlay_usage(usage);
            }
            // message_stop and forward-compatible events carry no normalized
            // fields; they reach the client only via the native event below.
            _ => {}
        }
        if self.preserve_native {
            let mut event = v;
            if event["type"] == "message_start"
                && let Some(model) = self.model_override.as_ref()
                && let Some(message) = event.get_mut("message").and_then(Value::as_object_mut)
            {
                message.insert("model".to_owned(), model.clone().into());
            }
            native_chunk.native_event = Some(event);
        }
        Self::push_visible(&mut chunks, native_chunk);
        Ok(chunks)
    }

    fn finish(self, resp: &mut GatewayResponse) {
        if !self.tool_blocks.is_empty() {
            resp.tool_calls = Some(Value::Array(self.tool_blocks));
        }
        if self.preserve_native && !self.native_blocks.is_empty() {
            resp.anthropic_content = Some(Value::Array(self.native_blocks));
        }
        resp.message = self.full;
        let (input, output) = (self.input.max(0), self.output.max(0));
        resp.prompt_tokens = input;
        resp.completion_tokens = output;
        crate::engine::fill_total_if_zero(resp);
        if !self.usage.is_empty() {
            resp.raw_usage = Some(Value::Object(self.usage));
        }
    }

    fn append_thinking(&mut self, t: &str) {
        if let Some(block) = self.open_native.as_mut()
            && let Some(Value::String(thinking)) = block.get_mut("thinking")
        {
            thinking.push_str(t);
        }
    }

    /// The native event keeps its usage; the chat surface moves it out.
    fn overlay_usage(&mut self, usage: Option<&mut Value>) {
        let usage = match usage {
            Some(usage) if self.preserve_native => usage.clone(),
            Some(usage) => usage.take(),
            None => return,
        };
        if let Value::Object(usage) = usage {
            for (key, value) in usage {
                if !value.is_null() {
                    self.usage.insert(key, value);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use gw_consts::Protocol;
    use gw_models::{ChatMsg, ChatParams, GatewayRequest, ModelParamV2, TypedParams};

    use super::*;
    use crate::transport::MockTransport;

    fn base_req() -> GatewayRequest {
        GatewayRequest {
            message: vec![ChatMsg::text("user", "ping")],
            model_param_v2: Some(ModelParamV2::with_name(
                Protocol::AnthropicMessages,
                "claude-sonnet",
            )),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn parses_messages_reply() {
        let mut e = ClaudeEngine::new(base_req(), Arc::new(MockTransport));
        let out = e.run().await.unwrap();
        assert!(out.response.message.contains("you said: ping"));
        assert!(out.response.is_messages_protocol);
        assert_eq!(out.response.finish_reason, "end_turn");
        assert!(out.response.total_tokens > 0);
    }

    #[tokio::test]
    async fn stream_decodes_native_event_sequence() {
        let mut r = base_req();
        r.stream = true;
        let mut e = ClaudeEngine::new(r, Arc::new(MockTransport));
        let out = e.run().await.unwrap();
        assert!(out.chunks.len() >= 2, "chunks: {:?}", out.chunks);
        assert!(out.response.message.contains("you said: ping"));
        assert_eq!(out.response.finish_reason, "end_turn");
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
    }

    #[derive(Debug)]
    struct SseReply(&'static str);

    #[async_trait::async_trait]
    impl crate::transport::Transport for SseReply {
        async fn send(
            &self,
            _req: crate::transport::UpstreamRequest,
        ) -> gw_models::GResult<crate::transport::UpstreamResponse> {
            Ok(crate::transport::UpstreamResponse {
                status: 200,
                body: crate::transport::UpstreamBody::Sse(self.0.as_bytes().to_vec()),
            })
        }
    }

    #[derive(Debug)]
    struct JsonReply(&'static [u8]);

    #[async_trait::async_trait]
    impl crate::transport::Transport for JsonReply {
        async fn send(
            &self,
            _req: crate::transport::UpstreamRequest,
        ) -> gw_models::GResult<crate::transport::UpstreamResponse> {
            Ok(crate::transport::UpstreamResponse {
                status: 200,
                body: crate::transport::UpstreamBody::Json(self.0.to_vec().into()),
            })
        }
    }

    #[tokio::test]
    async fn native_thinking_blocks_survive_non_stream() {
        let body = br#"{
            "id":"msg_1",
            "type":"message",
            "role":"assistant",
            "model":"claude-sonnet",
            "content":[
                {"type":"thinking","thinking":"private reasoning","signature":"opaque-signature"},
                {"type":"redacted_thinking","data":"opaque-redacted-data"},
                {"type":"text","text":"answer"}
            ],
            "stop_reason":"end_turn",
            "stop_sequence":"STOP",
            "usage":{"input_tokens":11,"output_tokens":7}
        }"#;
        let mut request = base_req();
        request.preserve_anthropic_wire = true;
        let mut engine = ClaudeEngine::new(request, Arc::new(JsonReply(body)));
        let outcome = engine.run().await.unwrap();

        assert_eq!(outcome.response.message, "answer");
        assert_eq!(
            outcome.response.anthropic_content,
            Some(json!([
                {"type":"thinking","thinking":"private reasoning","signature":"opaque-signature"},
                {"type":"redacted_thinking","data":"opaque-redacted-data"},
                {"type":"text","text":"answer"}
            ]))
        );
    }

    #[tokio::test]
    async fn chat_surface_turns_thinking_blocks_into_reasoning() {
        let body = br#"{
            "id":"msg_1","type":"message","role":"assistant","model":"claude-sonnet",
            "content":[
                {"type":"thinking","thinking":"private reasoning","signature":"opaque-signature"},
                {"type":"redacted_thinking","data":"opaque-redacted-data"},
                {"type":"text","text":"answer"},
                {"type":"tool_use","id":"tool-1","name":"probe","input":{"ok":true}}
            ],
            "stop_reason":"tool_use","usage":{"input_tokens":11,"output_tokens":7}
        }"#;
        let outcome = ClaudeEngine::new(base_req(), Arc::new(JsonReply(body)))
            .run()
            .await
            .unwrap();
        assert_eq!(outcome.response.message, "answer");
        assert_eq!(outcome.response.reasoning, "private reasoning");
        assert_eq!(
            outcome.response.reasoning_details,
            Some(vec![
                json!({"type":"reasoning.text","text":"private reasoning","signature":"opaque-signature","format":"anthropic-claude-v1","index":0}),
                json!({"type":"reasoning.encrypted","data":"opaque-redacted-data","format":"anthropic-claude-v1","index":1}),
            ])
        );
        assert!(outcome.response.anthropic_content.is_none());
        assert_eq!(outcome.response.tool_calls.unwrap()[0]["name"], "probe");
    }

    #[tokio::test]
    async fn streaming_request_preserves_thinking_when_upstream_returns_json() {
        let body = br#"{
            "id":"msg_1","type":"message","role":"assistant","model":"claude-sonnet",
            "content":[
                {"type":"thinking","thinking":"private reasoning","signature":"opaque-signature"},
                {"type":"tool_use","id":"tool-1","name":"probe","input":{"ok":true}}
            ],
            "stop_reason":"tool_use","usage":{"input_tokens":11,"output_tokens":7}
        }"#;
        let mut request = base_req();
        request.stream = true;
        request.preserve_anthropic_wire = true;
        let outcome = ClaudeEngine::new(request, Arc::new(JsonReply(body)))
            .run()
            .await
            .unwrap();

        assert!(outcome.chunks.iter().any(|chunk| {
            chunk.native_event.as_ref().is_some_and(|event| {
                event.pointer("/delta/type").and_then(Value::as_str) == Some("signature_delta")
                    && event.pointer("/delta/signature").and_then(Value::as_str)
                        == Some("opaque-signature")
            })
        }));
        assert!(outcome.chunks.iter().any(|chunk| {
            chunk.native_event.as_ref().is_some_and(|event| {
                event.get("type").and_then(Value::as_str) == Some("message_stop")
            })
        }));
    }

    #[tokio::test]
    async fn native_thinking_stream_preserves_signature_delta_and_order() {
        let sse = concat!(
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-sonnet\",\"usage\":{\"input_tokens\":11}}}\n\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\",\"signature\":\"stale-start\"}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"private reasoning\"}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"intermediate-signature\"}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"opaque-signature\"}}\n\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"answer\"}}\n\n",
            "data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":7}}\n\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        );
        let mut request = base_req();
        request.stream = true;
        request.preserve_anthropic_wire = true;
        let mut engine = ClaudeEngine::new(request, Arc::new(SseReply(sse)));
        let outcome = engine.run().await.unwrap();

        assert_eq!(
            outcome.response.anthropic_content,
            Some(json!([
                {"type":"thinking","thinking":"private reasoning","signature":"opaque-signature"},
                {"type":"text","text":"answer"}
            ]))
        );
        let event_types = outcome
            .chunks
            .iter()
            .filter_map(|chunk| {
                chunk
                    .native_event
                    .as_ref()
                    .and_then(|event| event["type"].as_str())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            event_types,
            [
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_delta",
                "content_block_delta",
                "content_block_stop",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop"
            ]
        );
    }

    #[test]
    fn cross_protocol_stream_turns_thinking_into_reasoning_chunks() {
        let mut state = SseState::new(false, None);
        let mut response = GatewayResponse::default();
        let mut chunks = Vec::new();
        for event in [
            json!({"type":"message_start","message":{"model":"claude-sonnet","usage":{"input_tokens":1}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"priv"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"ate"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"opaque"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"redacted_thinking","data":"blob"}}),
            json!({"type":"content_block_stop","index":1}),
            json!({"type":"content_block_start","index":2,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":2,"delta":{"type":"text_delta","text":"answer"}}),
            json!({"type":"content_block_stop","index":2}),
            json!({"type":"message_stop"}),
        ] {
            chunks.extend(state.apply(event, 200, &mut response).unwrap());
        }
        assert!(chunks.iter().all(|c| c.native_event.is_none()));
        assert!(
            state.native_blocks.is_empty() && state.open_native.is_none(),
            "a cross-protocol stream must not buffer a native copy"
        );
        let reasoning: Vec<&str> = chunks.iter().map(|c| c.reasoning.as_str()).collect();
        assert_eq!(reasoning, ["priv", "ate", "", "", ""]);
        let details: Vec<Value> = chunks
            .iter()
            .filter_map(|c| c.reasoning_details.clone())
            .flatten()
            .collect();
        assert_eq!(
            details,
            [
                json!({"type":"reasoning.text","text":"private","signature":"opaque","format":"anthropic-claude-v1","index":0}),
                json!({"type":"reasoning.encrypted","data":"blob","format":"anthropic-claude-v1","index":1}),
            ]
        );
        assert_eq!(chunks[4].delta, "answer");
        assert_eq!(response.reasoning, "private");
        assert!(response.reasoning_details.is_none());
    }

    #[test]
    fn native_chunk_rebuild_normalizes_stop_reason_and_keeps_cache_usage() {
        let response = GatewayResponse {
            model: "claude-sonnet".to_owned(),
            prompt_tokens: 11,
            completion_tokens: 7,
            raw_usage: Some(
                json!({"input_tokens":11,"output_tokens":7,"cache_read_input_tokens":8}),
            ),
            anthropic_content: Some(json!([{"type":"text","text":"answer"}])),
            ..Default::default()
        };
        let chunks = anthropic_native_chunks(&response, None);
        let events: Vec<_> = chunks
            .iter()
            .filter_map(|chunk| chunk.native_event.as_ref())
            .collect();

        assert_eq!(
            events[0].pointer("/message/usage/cache_read_input_tokens"),
            Some(&json!(8)),
            "the upstream usage object rides the synthetic message_start"
        );
        let delta = events
            .iter()
            .find(|event| event["type"] == "message_delta")
            .unwrap();
        assert_eq!(
            delta.pointer("/delta/stop_reason"),
            Some(&json!("end_turn")),
            "an absent stop_reason must not serialize as an empty string"
        );
    }

    #[tokio::test]
    async fn stream_decodes_tool_use_blocks() {
        let sse = concat!(
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-sonnet\",\"usage\":{\"input_tokens\":7}}}\n\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tu_1\",\"name\":\"get_weather\",\"input\":{}}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"city\\\":\"}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"sf\\\"}\"}}\n\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"input_tokens\":9,\"output_tokens\":5}}\n\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        );
        let mut r = base_req();
        r.stream = true;
        let mut e = ClaudeEngine::new(r, Arc::new(SseReply(sse)));
        let out = e.run().await.unwrap();
        assert_eq!(out.response.finish_reason, "tool_use");
        let tc = out.response.tool_calls.expect("tool_use blocks");
        assert_eq!(tc[0]["name"], "get_weather");
        assert_eq!(tc[0]["input"]["city"], "sf");
        assert!(out.chunks.iter().any(|c| c.tool_calls.is_some()));
        assert_eq!(out.response.prompt_tokens, 9);
        assert_eq!(out.response.completion_tokens, 5);
    }

    #[tokio::test]
    async fn system_and_tools() {
        let mut r = base_req();
        r.message.insert(0, ChatMsg::text("system", "be brief"));
        if let Some(p) = r.model_param_v2.as_mut() {
            p.typed = Some(TypedParams::Chat(ChatParams {
                tools: Some(json!([{"name":"get_weather","description":"d","input_schema":{}}])),
                ..Default::default()
            }));
        }
        let mut e = ClaudeEngine::new(r, Arc::new(MockTransport));
        let out = e.run().await.unwrap();
        assert_eq!(out.response.finish_reason, "tool_use");
        let tc = out.response.tool_calls.expect("tool_use blocks");
        assert_eq!(tc[0]["type"], "tool_use");
        assert_eq!(tc[0]["name"], "get_weather");
    }
}
