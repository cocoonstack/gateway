//! Anthropic-messages-protocol engine: system, tools/tool_use, multimodal text
//! blocks, and streaming (the standard anthropic SSE event sequence). Marks
//! `is_messages_protocol` so the usage extractor applies the Anthropic map.

use gw_models::{GResult, GatewayError, GatewayResponse};
use serde_json::{Map, Value, json};

use crate::base::base_engine;
use crate::engine::{EngineOutcome, ModelEngine, StreamChunk};
use crate::transport::{UpstreamBody, UpstreamRequest};

base_engine!(ClaudeEngine);

impl ClaudeEngine {
    fn build_upstream(&self) -> GResult<UpstreamRequest> {
        let param = self.base.param()?;
        let system_text = self.base.system_text();
        let mut messages: Vec<Value> = Vec::new();
        for m in &self.base.request.message {
            if m.role == gw_consts::role::SYSTEM {
                continue;
            }
            let role = if m.role == gw_consts::role::AI {
                "assistant"
            } else {
                "user"
            };
            // preserve multimodal content blocks, mirroring the OpenAI path's `parts`
            let content = match &m.parts {
                Some(parts) => parts.clone(),
                None => Value::String(m.content.clone()),
            };
            messages.push(json!({"role": role, "content": content}));
        }

        let mut body = Map::new();
        body.insert("model".into(), param.model_name.clone().into());
        body.insert("messages".into(), Value::Array(messages));
        body.insert("stream".into(), self.base.request.stream.into());
        let mut max_tokens = 1024;
        if let Some(p) = self.base.chat_params() {
            if let Some(mt) = p.max_tokens {
                max_tokens = mt;
            }
            if let Some(t) = p.temperature {
                body.insert("temperature".into(), json!(t));
            }
            if let Some(t) = p.top_p {
                body.insert("top_p".into(), json!(t));
            }
            if let Some(tools) = &p.tools {
                body.insert("tools".into(), normalize_tools_anthropic(tools));
            }
            if let Some(tc) = &p.tool_choice {
                body.insert("tool_choice".into(), tc.clone());
            }
            // Anthropic's field is `stop_sequences` (array), not OpenAI's `stop`
            if let Some(stop) = &p.stop {
                body.insert("stop_sequences".into(), stop.clone());
            }
        }
        body.insert("max_tokens".into(), json!(max_tokens));
        if !system_text.is_empty() {
            body.insert("system".into(), system_text.into());
        }
        crate::base::merge_raw_extras(&mut body, &param.raw);

        Ok(UpstreamRequest {
            protocol: param.protocol,
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
        })
    }

    fn parse_json(&self, status: u16, bytes: &[u8]) -> GResult<EngineOutcome> {
        let mut v: Value = serde_json::from_slice(bytes)
            .map_err(|e| GatewayError::internal("parse anthropic response").with_source(e))?;
        if let Some(err) = crate::engine::vendor_error(status, &v) {
            return Err(err);
        }
        let mut text = String::new();
        let mut tool_use: Vec<Value> = Vec::new();
        let content = v.get_mut("content").map(Value::take);
        if let Some(Value::Array(blocks)) = &content {
            for b in blocks {
                match b["type"].as_str() {
                    Some("text") => text.push_str(b["text"].as_str().unwrap_or_default()),
                    Some("tool_use") => tool_use.push(b.clone()),
                    _ => {}
                }
            }
        }
        let usage = &v["usage"];
        let input = crate::engine::tok(&usage["input_tokens"]);
        let output = crate::engine::tok(&usage["output_tokens"]);
        let raw_usage_json = if usage.is_null() {
            vec![]
        } else {
            serde_json::to_vec(usage).unwrap_or_default()
        };
        let resp = GatewayResponse {
            message: text,
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
            raw_usage_json,
            anthropic_content: if self.base.request.preserve_anthropic_wire {
                content
            } else {
                None
            },
            ..Default::default()
        };
        let mut outcome = EngineOutcome::with_status(resp, status);
        if self.base.request.stream && self.base.request.preserve_anthropic_wire {
            let model_override = self
                .base
                .request
                .model_param_v2
                .as_ref()
                .and_then(|param| param.fallback_from.as_deref());
            outcome.chunks = anthropic_native_chunks(&outcome.response, model_override);
        }
        Ok(outcome)
    }

    /// Buffered or live anthropic event sequence through the shared pump.
    async fn run_sse(&self, status: u16, body: UpstreamBody) -> GResult<EngineOutcome> {
        let mut resp = GatewayResponse {
            is_messages_protocol: true,
            ..Default::default()
        };
        let model_override = self
            .base
            .request
            .model_param_v2
            .as_ref()
            .and_then(|param| param.fallback_from.clone());
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
    async fn run(&self) -> GResult<EngineOutcome> {
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
    let mut events = vec![json!({
        "type":"message_start",
        "message":{
            "type":"message",
            "role":"assistant",
            "model":stream_model,
            "content":[],
            "stop_reason":null,
            "usage":{"input_tokens":response.prompt_tokens,"output_tokens":0}
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
    events.push(json!({
        "type":"message_delta",
        "delta":{"stop_reason":response.finish_reason,"stop_sequence":null},
        "usage":{"output_tokens":response.completion_tokens}
    }));
    events.push(json!({"type":"message_stop"}));
    events
        .into_iter()
        .map(|event| StreamChunk {
            anthropic_event: Some(event),
            ..Default::default()
        })
        .collect()
}

/// Tool definitions in the anthropic wire shape. Cross-protocol requests carry
/// OpenAI-shaped defs ({type:"function", function:{name, parameters}}) —
/// flatten those; native defs pass through.
fn normalize_tools_anthropic(tools: &Value) -> Value {
    let Some(arr) = tools.as_array() else {
        return tools.clone();
    };
    Value::Array(
        arr.iter()
            .map(|t| {
                if let Some(f) = t.get("function") {
                    json!({
                        "name": f["name"],
                        "description": f["description"],
                        "input_schema": f["parameters"],
                    })
                } else {
                    t.clone()
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
    tool_blocks: Vec<Value>,
    native_blocks: Vec<Value>,
    open_native: Option<Value>,
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
            tool_blocks: Vec::new(),
            native_blocks: Vec::new(),
            open_native: None,
            open_native_input: String::new(),
            open_tool: None,
        }
    }

    fn push_visible(chunks: &mut Vec<StreamChunk>, chunk: StreamChunk) {
        if chunk.anthropic_event.is_some()
            || !chunk.delta.is_empty()
            || chunk.tool_calls.is_some()
            || chunk.finish_reason.is_some()
        {
            chunks.push(chunk);
        }
    }

    /// Apply one decoded event; returns the chunks it yields.
    fn apply(
        &mut self,
        v: &Value,
        status: u16,
        resp: &mut GatewayResponse,
    ) -> GResult<Vec<StreamChunk>> {
        if let Some(err) = crate::engine::vendor_error(status, v) {
            return Err(err);
        }
        let mut chunks = Vec::new();
        let mut native_event = self.preserve_native.then(|| v.clone());
        if v.get("type").and_then(Value::as_str) == Some("message_start")
            && let Some(model) = self.model_override.as_ref()
            && let Some(message) = native_event
                .as_mut()
                .and_then(|event| event.get_mut("message"))
                .and_then(Value::as_object_mut)
        {
            message.insert("model".to_owned(), model.clone().into());
        }
        let mut native_chunk = StreamChunk {
            anthropic_event: native_event,
            ..Default::default()
        };
        match v["type"].as_str().unwrap_or_default() {
            "message_start" => {
                resp.model = v["message"]["model"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                self.input = v["message"]["usage"]["input_tokens"].as_i64().unwrap_or(0);
                Self::push_visible(&mut chunks, native_chunk);
            }
            "content_block_start" => {
                self.open_native = Some(v["content_block"].clone());
                self.open_native_input.clear();
                if v["content_block"]["type"] == "tool_use" {
                    self.open_tool = Some((v["content_block"].clone(), String::new()));
                }
                Self::push_visible(&mut chunks, native_chunk);
            }
            "content_block_delta" => {
                if let Some(t) = v["delta"]["text"].as_str() {
                    self.full.push_str(t);
                    native_chunk.delta = t.to_owned();
                    if let Some(block) = self.open_native.as_mut()
                        && let Some(Value::String(text)) = block.get_mut("text")
                    {
                        text.push_str(t);
                    }
                }
                if let Some(t) = v["delta"]["thinking"].as_str()
                    && let Some(block) = self.open_native.as_mut()
                    && let Some(Value::String(thinking)) = block.get_mut("thinking")
                {
                    thinking.push_str(t);
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
                    self.open_native_input.push_str(pj);
                }
                Self::push_visible(&mut chunks, native_chunk);
            }
            "content_block_stop" => {
                if let Some(mut block) = self.open_native.take() {
                    if block["type"] == "tool_use"
                        && let Ok(parsed) = serde_json::from_str::<Value>(&self.open_native_input)
                    {
                        block["input"] = parsed;
                    }
                    self.native_blocks.push(block);
                }
                self.open_native_input.clear();
                if let Some((mut block, buf)) = self.open_tool.take() {
                    if let Ok(parsed) = serde_json::from_str::<Value>(&buf) {
                        block["input"] = parsed;
                    }
                    native_chunk.tool_calls = Some(Value::Array(vec![block.clone()]));
                    self.tool_blocks.push(block);
                }
                Self::push_visible(&mut chunks, native_chunk);
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
                Self::push_visible(&mut chunks, native_chunk);
            }
            // message_stop and forward-compatible events are visible only on
            // the native Anthropic surface.
            _ => Self::push_visible(&mut chunks, native_chunk),
        }
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
        resp.total_tokens = input.saturating_add(output);
        resp.raw_usage_json =
            serde_json::to_vec(&json!({"input_tokens": input, "output_tokens": output}))
                .unwrap_or_default();
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
        let e = ClaudeEngine::new(base_req(), Arc::new(MockTransport));
        let out = e.run().await.unwrap();
        assert!(out.response.message.contains("you said: ping"));
        assert!(out.response.is_messages_protocol);
        assert_eq!(out.response.finish_reason, "end_turn");
        assert!(out.response.total_tokens > 0);
    }

    #[tokio::test]
    async fn stream_decodes_anthropic_event_sequence() {
        let mut r = base_req();
        r.stream = true;
        let e = ClaudeEngine::new(r, Arc::new(MockTransport));
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
        let engine = ClaudeEngine::new(request, Arc::new(JsonReply(body)));
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
            chunk.anthropic_event.as_ref().is_some_and(|event| {
                event.pointer("/delta/type").and_then(Value::as_str) == Some("signature_delta")
                    && event.pointer("/delta/signature").and_then(Value::as_str)
                        == Some("opaque-signature")
            })
        }));
        assert!(outcome.chunks.iter().any(|chunk| {
            chunk.anthropic_event.as_ref().is_some_and(|event| {
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
        let engine = ClaudeEngine::new(request, Arc::new(SseReply(sse)));
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
                    .anthropic_event
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
    fn cross_protocol_stream_drops_native_only_chunks() {
        let mut state = SseState::new(false, None);
        let mut response = GatewayResponse::default();
        for event in [
            json!({"type":"message_start","message":{"model":"claude-sonnet","usage":{"input_tokens":1}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"private"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"opaque"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_stop"}),
        ] {
            assert!(
                state.apply(&event, 200, &mut response).unwrap().is_empty(),
                "native-only events must not commit a non-Anthropic stream"
            );
        }
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
        let e = ClaudeEngine::new(r, Arc::new(SseReply(sse)));
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
        let e = ClaudeEngine::new(r, Arc::new(MockTransport));
        let out = e.run().await.unwrap();
        assert_eq!(out.response.finish_reason, "tool_use");
        let tc = out.response.tool_calls.expect("tool_use blocks");
        assert_eq!(tc[0]["type"], "tool_use");
        assert_eq!(tc[0]["name"], "get_weather");
    }
}
