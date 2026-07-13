//! Anthropic-messages-protocol engine.
//!
//! Full messages surface: system, tools/tool_use, multimodal text blocks, and
//! streaming (parses the standard anthropic SSE event sequence message_start →
//! content_block_delta → message_delta → message_stop). Marks `is_messages_protocol`
//! so the usage extractor applies the Anthropic field map.

use chrono::Utc;
use gw_models::{
    GResult, GatewayError, GatewayRequest, GatewayResponse, Recorder, SimpleRecorder, TypedParams,
};
use serde_json::{Map, Value, json};

use crate::engine::{EngineOutcome, ModelEngine, StreamChunk};
use crate::sse::SseDecoder;
use crate::transport::{SharedTransport, UpstreamBody, UpstreamRequest};

pub struct ClaudeEngine {
    request: GatewayRequest,
    transport: SharedTransport,
    recorder: SimpleRecorder,
}

impl ClaudeEngine {
    pub fn new(request: GatewayRequest, transport: SharedTransport) -> Self {
        Self {
            request,
            transport,
            recorder: SimpleRecorder::new(Utc::now()),
        }
    }

    fn build_upstream(&self) -> GResult<UpstreamRequest> {
        let param = self
            .request
            .model_param_v2
            .as_ref()
            .ok_or_else(|| GatewayError::bad_request("missing model param"))?;
        // system prompt: typed.system takes priority; system turns in messages are merged in too
        let mut system_text = String::new();
        let mut messages: Vec<Value> = Vec::new();
        for m in &self.request.message {
            if m.role == gw_consts::role::SYSTEM {
                system_text.push_str(&m.content);
                continue;
            }
            let role = if m.role == gw_consts::role::AI {
                "assistant"
            } else {
                "user"
            };
            // preserve multimodal content blocks (image/text) when present; the
            // OpenAI path already does this via `parts` — mirror it for anthropic.
            let content = match &m.parts {
                Some(parts) => parts.clone(),
                None => Value::String(m.content.clone()),
            };
            messages.push(json!({"role": role, "content": content}));
        }

        let mut body = Map::new();
        body.insert("model".into(), param.model_name.clone().into());
        body.insert("messages".into(), Value::Array(messages));
        body.insert("stream".into(), self.request.stream.into());
        let mut max_tokens = 1024;
        if let Some(TypedParams::Chat(p)) = &param.typed {
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
                body.insert("tools".into(), tools.clone());
            }
            if let Some(tc) = &p.tool_choice {
                body.insert("tool_choice".into(), tc.clone());
            }
            // Anthropic's field is `stop_sequences` (array), not OpenAI's `stop`.
            // views puts the request's stop_sequences into p.stop → forward it here.
            if let Some(stop) = &p.stop {
                body.insert("stop_sequences".into(), stop.clone());
            }
            if let Some(s) = &p.system {
                system_text = format!("{s}{system_text}");
            }
        }
        body.insert("max_tokens".into(), json!(max_tokens));
        if !system_text.is_empty() {
            body.insert("system".into(), system_text.into());
        }
        if let Value::Object(extra) = &param.raw {
            for (k, v) in extra {
                body.entry(k.clone()).or_insert(v.clone());
            }
        }

        // go-live seam: real endpoint + key when the account is configured, else
        // the inert mock sentinel (MockTransport routes by the `/v1/messages` path).
        let account = self.request.account.as_ref();
        let base = account
            .map(|a| a.base_url("mock://api.anthropic.com").to_owned())
            .unwrap_or_else(|| "mock://api.anthropic.com".to_owned());
        let key = account
            .and_then(|a| a.api_key())
            .unwrap_or_else(|| "mock".to_owned());
        Ok(UpstreamRequest {
            protocol: param.protocol,
            method: "POST".to_owned(),
            url: format!("{base}/v1/messages"),
            headers: vec![
                ("content-type".into(), "application/json".into()),
                ("x-api-key".into(), key),
                // Anthropic API mandates this header; a real call 400s without it.
                ("anthropic-version".into(), "2023-06-01".into()),
            ],
            body: Value::Object(body).to_string().into_bytes(),
            stream: self.request.stream,
            account: self
                .request
                .account
                .as_ref()
                .map(|a| a.name.clone())
                .unwrap_or_default(),
        })
    }

    fn parse_json(&self, status: u16, bytes: &[u8]) -> GResult<EngineOutcome> {
        let v: Value = serde_json::from_slice(bytes)
            .map_err(|e| GatewayError::internal("parse anthropic response").with_source(e))?;
        if let Some(err) = crate::engine::vendor_error(status, &v) {
            return Err(err);
        }
        let mut text = String::new();
        let mut tool_use: Vec<Value> = Vec::new();
        if let Some(blocks) = v["content"].as_array() {
            for b in blocks {
                match b["type"].as_str() {
                    Some("text") => text.push_str(b["text"].as_str().unwrap_or_default()),
                    Some("tool_use") => tool_use.push(b.clone()),
                    _ => {}
                }
            }
        }
        let usage = &v["usage"];
        let input = usage["input_tokens"].as_i64().unwrap_or(0);
        let output = usage["output_tokens"].as_i64().unwrap_or(0);
        let resp = GatewayResponse {
            message: text,
            tool_calls: if tool_use.is_empty() {
                None
            } else {
                Some(Value::Array(tool_use))
            },
            model: v["model"].as_str().unwrap_or_default().to_owned(),
            finish_reason: v["stop_reason"].as_str().unwrap_or_default().to_owned(),
            is_messages_protocol: true,
            prompt_tokens: input,
            completion_tokens: output,
            total_tokens: input + output,
            raw_usage_json: if usage.is_null() {
                vec![]
            } else {
                usage.to_string().into_bytes()
            },
            http_code: status as i64,
            ..Default::default()
        };
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            ..Default::default()
        })
    }

    /// Decode a fully buffered anthropic streaming event sequence.
    fn parse_sse(&self, status: u16, bytes: &[u8]) -> GResult<EngineOutcome> {
        let (events, _done) = SseDecoder::decode_all(bytes);
        let mut resp = GatewayResponse {
            is_messages_protocol: true,
            http_code: status as i64,
            ..Default::default()
        };
        let mut st = SseState::default();
        let mut chunks = Vec::new();
        for ev in events {
            let v: Value = serde_json::from_slice(ev.as_bytes())
                .map_err(|e| GatewayError::internal("parse anthropic sse frame").with_source(e))?;
            chunks.extend(st.apply(&v, status, &mut resp)?);
        }
        st.finish(&mut resp);
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            chunks,
            ..Default::default()
        })
    }

    /// Incremental variant of `parse_sse`: frames are decoded as vendor bytes
    /// arrive and forwarded through `stream_tx` when the request carries one
    /// (same live-pump contract as the OpenAI engine).
    async fn pump_sse(
        &self,
        status: u16,
        mut s: futures::stream::BoxStream<'static, Result<bytes::Bytes, String>>,
    ) -> GResult<EngineOutcome> {
        use futures::StreamExt;
        let tx = self.request.stream_tx.clone();
        let mut dec = SseDecoder::default();
        let mut st = SseState::default();
        let mut chunks = Vec::new();
        let mut resp = GatewayResponse {
            is_messages_protocol: true,
            http_code: status as i64,
            ..Default::default()
        };
        let mut sent_any = false;
        while let Some(item) = s.next().await {
            let bytes = item.map_err(|e| {
                if sent_any {
                    GatewayError::client_closed(format!("upstream stream failed mid-response: {e}"))
                } else {
                    GatewayError::new(
                        gw_consts::ErrCode::FED_RESP_RPC_FAILED,
                        502,
                        format!("upstream stream failed: {e}"),
                    )
                }
            })?;
            for data in dec.feed(&bytes) {
                let v: Value = serde_json::from_str(&data).map_err(|e| {
                    GatewayError::internal("parse anthropic sse frame").with_source(e)
                })?;
                for chunk in st.apply(&v, status, &mut resp)? {
                    match &tx {
                        Some(tx) => {
                            tx.send(chunk)
                                .await
                                .map_err(|_| GatewayError::client_closed("client stream closed"))?;
                            sent_any = true;
                        }
                        None => chunks.push(chunk),
                    }
                }
            }
        }
        st.finish(&mut resp);
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            chunks,
            streamed_live: sent_any,
            ..Default::default()
        })
    }
}

#[async_trait::async_trait]
impl ModelEngine for ClaudeEngine {
    async fn run(&self) -> GResult<EngineOutcome> {
        let up = self.build_upstream()?;
        let reply = self.transport.send(up).await?;
        match reply.body {
            UpstreamBody::Json(b) => self.parse_json(reply.status, &b),
            UpstreamBody::Sse(b) => self.parse_sse(reply.status, &b),
            UpstreamBody::SseStream(s) => self.pump_sse(reply.status, s).await,
        }
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.recorder
    }
}

/// Accumulating state for the anthropic streaming event sequence, shared by
/// the buffered and live decode paths.
#[derive(Default)]
struct SseState {
    full: String,
    input: i64,
    output: i64,
    tool_blocks: Vec<Value>,
    /// in-flight tool_use block: (skeleton from content_block_start,
    /// accumulated input_json_delta fragments)
    open_tool: Option<(Value, String)>,
}

impl SseState {
    /// Apply one decoded event; returns the chunks it yields.
    fn apply(
        &mut self,
        v: &Value,
        status: u16,
        resp: &mut GatewayResponse,
    ) -> GResult<Vec<StreamChunk>> {
        // mid-stream error frame → surface it
        if let Some(err) = crate::engine::vendor_error(status, v) {
            return Err(err);
        }
        let mut chunks = Vec::new();
        match v["type"].as_str().unwrap_or_default() {
            "message_start" => {
                resp.model = v["message"]["model"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                self.input = v["message"]["usage"]["input_tokens"].as_i64().unwrap_or(0);
            }
            "content_block_start" => {
                if v["content_block"]["type"] == "tool_use" {
                    self.open_tool = Some((v["content_block"].clone(), String::new()));
                }
            }
            "content_block_delta" => {
                if let Some(t) = v["delta"]["text"].as_str() {
                    self.full.push_str(t);
                    chunks.push(StreamChunk {
                        delta: t.to_owned(),
                        finish_reason: None,
                        ..Default::default()
                    });
                }
                if let Some(pj) = v["delta"]["partial_json"].as_str()
                    && let Some((_, buf)) = self.open_tool.as_mut()
                {
                    buf.push_str(pj);
                }
            }
            "content_block_stop" => {
                if let Some((mut block, buf)) = self.open_tool.take() {
                    if let Ok(parsed) = serde_json::from_str::<Value>(&buf) {
                        block["input"] = parsed;
                    }
                    chunks.push(StreamChunk {
                        tool_calls: Some(json!([block.clone()])),
                        ..Default::default()
                    });
                    self.tool_blocks.push(block);
                }
            }
            "message_delta" => {
                if let Some(sr) = v["delta"]["stop_reason"].as_str() {
                    resp.finish_reason = sr.to_owned();
                    chunks.push(StreamChunk {
                        delta: String::new(),
                        finish_reason: Some(sr.to_owned()),
                        ..Default::default()
                    });
                }
                self.output = v["usage"]["output_tokens"].as_i64().unwrap_or(self.output);
            }
            _ => {} // message_stop
        }
        Ok(chunks)
    }

    fn finish(self, resp: &mut GatewayResponse) {
        if !self.tool_blocks.is_empty() {
            resp.tool_calls = Some(Value::Array(self.tool_blocks));
        }
        resp.message = self.full;
        resp.prompt_tokens = self.input;
        resp.completion_tokens = self.output;
        resp.total_tokens = self.input + self.output;
        resp.raw_usage_json = json!({"input_tokens": self.input, "output_tokens": self.output})
            .to_string()
            .into_bytes();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::MockTransport;
    use gw_consts::Protocol;
    use gw_models::{ChatMsg, ChatParams, ModelParamV2};
    use std::sync::Arc;

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

    #[tokio::test]
    async fn stream_decodes_tool_use_blocks() {
        let sse = concat!(
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-sonnet\",\"usage\":{\"input_tokens\":7}}}\n\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tu_1\",\"name\":\"get_weather\",\"input\":{}}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"city\\\":\"}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"sf\\\"}\"}}\n\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":5}}\n\n",
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
        // mock answers tool requests with a tool_use block
        assert_eq!(out.response.finish_reason, "tool_use");
        let tc = out.response.tool_calls.expect("tool_use blocks");
        assert_eq!(tc[0]["type"], "tool_use");
        assert_eq!(tc[0]["name"], "get_weather");
    }
}
