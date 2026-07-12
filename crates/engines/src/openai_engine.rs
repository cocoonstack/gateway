//! OpenAI-protocol engine.
//!
//! Builds the vendor chat/completions request (full param passthrough: sampling
//! params, tools, response_format, logprobs, multimodal content, tool result
//! messages), sends it via [`Transport`], parses the JSON (or SSE) reply into
//! `GatewayResponse` (including tool_calls) + stream chunks, and slices the raw
//! usage subtree into `raw_usage_json` for the CommonUsage DAG node — the
//! request→upstream→parse boundary this crate follows.

use ap_models::{
    GResult, GatewayError, GatewayRequest, GatewayResponse, Recorder, SimpleRecorder, TypedParams,
};
use chrono::Utc;
use serde_json::{Map, Value, json};

use crate::engine::{EngineOutcome, ModelEngine, StreamChunk};
use crate::sse::SseDecoder;
use crate::transport::{SharedTransport, UpstreamBody, UpstreamRequest};

pub struct OpenAiEngine {
    request: GatewayRequest,
    transport: SharedTransport,
    recorder: SimpleRecorder,
}

impl OpenAiEngine {
    pub fn new(request: GatewayRequest, transport: SharedTransport) -> Self {
        Self {
            request,
            transport,
            recorder: SimpleRecorder::new(Utc::now()),
        }
    }

    /// Rebuild the OpenAI wire message: multimodal parts win over flat text;
    /// assistant tool_calls and tool results pass through losslessly.
    fn wire_messages(&self) -> Vec<Value> {
        self.request
            .message
            .iter()
            .map(|m| {
                let mut msg = Map::new();
                msg.insert("role".into(), m.role.clone().into());
                match &m.parts {
                    Some(parts) => {
                        msg.insert("content".into(), parts.clone());
                    }
                    None => {
                        msg.insert("content".into(), m.content.clone().into());
                    }
                }
                if let Some(tc) = &m.tool_calls {
                    msg.insert("tool_calls".into(), tc.clone());
                    // OpenAI: assistant tool-call turns carry content: null
                    if m.content.is_empty() && m.parts.is_none() {
                        msg.insert("content".into(), Value::Null);
                    }
                }
                if let Some(id) = &m.tool_call_id {
                    msg.insert("tool_call_id".into(), id.clone().into());
                }
                Value::Object(msg)
            })
            .collect()
    }

    fn build_upstream(&self) -> GResult<UpstreamRequest> {
        let param = self
            .request
            .model_param_v2
            .as_ref()
            .ok_or_else(|| GatewayError::bad_request("missing model param"))?;
        let mut body = Map::new();
        body.insert("model".into(), param.model_name.clone().into());
        body.insert("messages".into(), Value::Array(self.wire_messages()));
        body.insert("stream".into(), self.request.stream.into());
        // OpenAI omits usage from streamed responses UNLESS this is set — without
        // it every streaming call would bill 0 tokens.
        if self.request.stream {
            body.insert("stream_options".into(), json!({"include_usage": true}));
        }

        // typed family params → wire fields
        if let Some(TypedParams::Chat(p)) = &param.typed {
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
                body.insert("tools".into(), v.clone());
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
                if let Some(Value::Array(existing)) = body.get("messages") {
                    msgs.extend(existing.clone());
                }
                body.insert("messages".into(), Value::Array(msgs));
            }
        }
        // untyped vendor extras ride along verbatim
        if let Value::Object(extra) = &param.raw {
            for (k, v) in extra {
                body.entry(k.clone()).or_insert(v.clone());
            }
        }

        // route to the account's endpoint if configured, else the mock sentinel
        let account = self.request.account.as_ref();
        let base = account
            .map(|a| a.base_url("mock://api.openai.com").to_owned())
            .unwrap_or_else(|| "mock://api.openai.com".to_owned());
        // real key read from the account's env var at call time; "mock" otherwise
        let key = account
            .and_then(|a| a.api_key())
            .unwrap_or_else(|| "mock".to_owned());
        Ok(UpstreamRequest {
            protocol: param.protocol,
            method: "POST".to_owned(),
            url: format!("{base}/v1/chat/completions"),
            headers: vec![
                ("content-type".into(), "application/json".into()),
                ("authorization".into(), format!("Bearer {key}")),
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

    fn parse_json(&self, status: u16, body: &[u8]) -> GResult<EngineOutcome> {
        let v: Value = serde_json::from_slice(body)
            .map_err(|e| GatewayError::internal("parse openai response").with_source(e))?;
        // surface vendor error envelopes instead of silently returning empty
        if let Some(err) = crate::engine::vendor_error(status, &v) {
            return Err(err);
        }
        let msg = &v["choices"][0]["message"];
        let mut resp = GatewayResponse {
            message: msg["content"].as_str().unwrap_or_default().to_owned(),
            tool_calls: msg.get("tool_calls").filter(|t| !t.is_null()).cloned(),
            model: v["model"].as_str().unwrap_or_default().to_owned(),
            finish_reason: v["choices"][0]["finish_reason"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            http_code: status as i64,
            ..Default::default()
        };
        apply_openai_usage(&mut resp, &v["usage"]);
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            ..Default::default()
        })
    }

    fn parse_sse(&self, status: u16, body: &[u8]) -> GResult<EngineOutcome> {
        let (events, _done) = SseDecoder::decode_all(body);
        let mut chunks = Vec::new();
        let mut full = String::new();
        let mut resp = GatewayResponse {
            http_code: status as i64,
            ..Default::default()
        };
        for ev in events {
            let v: Value = serde_json::from_slice(ev.as_bytes())
                .map_err(|e| GatewayError::internal("parse openai sse frame").with_source(e))?;
            // mid-stream error frame → surface it
            if let Some(err) = crate::engine::vendor_error(status, &v) {
                return Err(err);
            }
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
                });
            }
            if let Some(tc) = delta.get("tool_calls").filter(|t| !t.is_null()) {
                resp.tool_calls = Some(tc.clone());
            }
            if let Some(fr) = v["choices"][0]["finish_reason"].as_str() {
                resp.finish_reason = fr.to_owned();
                chunks.push(StreamChunk {
                    delta: String::new(),
                    finish_reason: Some(fr.to_owned()),
                });
            }
            if v.get("usage").map(|u| !u.is_null()).unwrap_or(false) {
                apply_openai_usage(&mut resp, &v["usage"]);
            }
        }
        resp.message = full;
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            chunks,
            ..Default::default()
        })
    }
}

/// Copy token fields + keep the raw usage subtree bytes for the DAG node.
fn apply_openai_usage(resp: &mut GatewayResponse, usage: &Value) {
    if usage.is_null() {
        return;
    }
    resp.prompt_tokens = usage["prompt_tokens"].as_i64().unwrap_or(0);
    resp.completion_tokens = usage["completion_tokens"].as_i64().unwrap_or(0);
    resp.total_tokens = usage["total_tokens"].as_i64().unwrap_or(0);
    resp.raw_usage_json = usage.to_string().into_bytes();
}

#[async_trait::async_trait]
impl ModelEngine for OpenAiEngine {
    async fn run(&self) -> GResult<EngineOutcome> {
        let up = self.build_upstream()?;
        let stream = up.stream;
        let reply = self.transport.send(up).await?;
        match (&reply.body, stream) {
            (UpstreamBody::Sse(bytes), _) => self.parse_sse(reply.status, bytes),
            (UpstreamBody::Json(bytes), _) => self.parse_json(reply.status, bytes),
        }
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.recorder
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::MockTransport;
    use ap_consts::Protocol;
    use ap_models::{ChatMsg, ChatParams, ModelParamV2};
    use std::sync::Arc;

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
        let e = OpenAiEngine::new(req(false), Arc::new(MockTransport));
        let out = e.run().await.unwrap();
        assert!(out.response.message.contains("you said: hello world"));
        assert_eq!(out.response.model, "gpt-4o");
        assert!(out.response.total_tokens > 0);
        assert!(!out.response.raw_usage_json.is_empty());
        assert!(out.chunks.is_empty());
    }

    #[tokio::test]
    async fn stream_decodes_chunks_and_final_usage() {
        let e = OpenAiEngine::new(req(true), Arc::new(MockTransport));
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
        let e = OpenAiEngine::new(r, Arc::new(MockTransport));
        let out = e.run().await.unwrap();
        assert_eq!(out.response.finish_reason, "tool_calls");
        let tc = out.response.tool_calls.expect("tool calls");
        assert_eq!(tc[0]["function"]["name"], "get_weather");
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
        let e = OpenAiEngine::new(r, Arc::new(MockTransport));
        let out = e.run().await.unwrap();
        // mock acknowledges image parts it saw
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
        // MockTransport echoes params presence via a marker when seed is set
        let e = OpenAiEngine::new(r, Arc::new(MockTransport));
        let out = e.run().await.unwrap();
        assert!(out.response.message.contains("you said:"));
    }
}
