//! Bespoke vendor wire shapes: these vendors do NOT speak the OpenAI protocol;
//! each engine builds the vendor's real request shape and parses its real
//! response shape (the mock answers in the same shapes). AWS engines compute a
//! real SigV4 Authorization header.

use gw_models::{GResult, GatewayError, GatewayResponse};
use serde_json::{Value, json};

use crate::base::base_engine;
use crate::bedrock::{bedrock_header_usage, bedrock_invoke, bedrock_stream, invocation_metrics};
use crate::engine::{EngineOutcome, ModelEngine, StreamChunk};

base_engine!(ErnieEngine);

#[async_trait::async_trait]
impl ModelEngine for ErnieEngine {
    /// Baidu Ernie (Wenxin): /wenxinworkshop/chat/{model}, a v2 API key
    /// (`bce-v3/…`) as Bearer or a legacy OAuth token as `?access_token=`.
    /// Request {messages,[temperature]}; response {result, usage{...}, is_truncated}.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        let messages: Vec<Value> = self
            .base
            .request
            .message
            .iter()
            .filter(|m| m.role != gw_consts::role::SYSTEM)
            .map(|m| {
                json!({"role": if m.role == gw_consts::role::AI {"assistant"} else {"user"},
                             "content": m.content})
            })
            .collect();
        let mut body = json!({});
        body["messages"] = Value::Array(messages);
        // ernie's system is a top-level field (system turns are filtered above)
        let system = self.base.system_text();
        if !system.is_empty() {
            body["system"] = system.into();
        }
        if let Some(p) = self.base.chat_params()
            && let Some(t) = p.temperature
        {
            body["temperature"] = json!(t);
        }
        let key = self.base.api_key();
        let mut url = format!(
            "{}/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/{model}",
            self.base.base_url("mock://aip.baidubce.com"),
        );
        let mut headers = Vec::new();
        if key.starts_with("bce-v3/") {
            headers.push(("authorization".to_owned(), format!("Bearer {key}")));
        } else {
            url.push_str("?access_token=");
            url.push_str(&key);
        }
        let (status, mut v) = self.base.post_json(&url, headers, body).await?;
        let message = crate::engine::take_string(&mut v, "/result").unwrap_or_default();
        let usage = &v["usage"];
        let prompt_tokens = crate::engine::tok(&usage["prompt_tokens"]);
        let completion_tokens = crate::engine::tok(&usage["completion_tokens"]);
        let total_tokens = crate::engine::tok(&usage["total_tokens"]);
        let resp = GatewayResponse {
            message,
            model,
            finish_reason: if v["is_truncated"].as_bool().unwrap_or(false) {
                "length".into()
            } else {
                "stop".into()
            },
            prompt_tokens,
            completion_tokens,
            total_tokens,
            raw_usage: v.get_mut("usage").map(Value::take).filter(|u| !u.is_null()),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }
}

base_engine!(MinimaxV1Engine);

#[async_trait::async_trait]
impl ModelEngine for MinimaxV1Engine {
    /// MiniMax v1: messages use sender_type USER/BOT + text;
    /// response {reply, usage{total_tokens}, base_resp{status_code,status_msg}}.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        let messages: Vec<Value> = self
            .base
            .request
            .message
            .iter()
            .filter(|m| m.role != gw_consts::role::SYSTEM)
            .map(|m| {
                json!({"sender_type": if m.role == gw_consts::role::AI {"BOT"} else {"USER"},
                       "text": m.content})
            })
            .collect();
        let mut body = json!({"model": model});
        body["messages"] = Value::Array(messages);
        // v1 carries the system instruction as top-level `prompt` + role_meta
        let system = self.base.system_text();
        if !system.is_empty() {
            body["prompt"] = system.into();
            body["role_meta"] = json!({"user_name": "USER", "bot_name": "BOT"});
        }
        let url = format!(
            "{}/v1/text/chatcompletion",
            self.base.base_url("mock://api.minimax.chat")
        );
        let (status, mut v) = self
            .base
            .post_json(&url, self.base.bearer_headers(), body)
            .await?;
        // base_resp non-zero is an error (minimax's business error-code convention)
        let code = v["base_resp"]["status_code"].as_i64().unwrap_or(0);
        if code != 0 {
            return Err(GatewayError::new(
                gw_consts::ErrCode::FED_RESP_STATUS_NOT_ZERO,
                502,
                format!("minimax base_resp {code}: {}", v["base_resp"]["status_msg"]),
            ));
        }
        let total = crate::engine::tok(&v["usage"]["total_tokens"]);
        let resp = GatewayResponse {
            message: crate::engine::take_string(&mut v, "/reply").unwrap_or_default(),
            model,
            finish_reason: "stop".into(),
            total_tokens: total,
            raw_usage: v.get_mut("usage").map(Value::take).filter(|u| !u.is_null()),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }
}

base_engine!(CohereEngine);

impl CohereEngine {
    fn build_body(&self) -> Value {
        let mut history: Vec<Value> = self
            .base
            .request
            .message
            .iter()
            .filter(|m| m.role != gw_consts::role::SYSTEM)
            .map(|m| {
                let role = if m.role == gw_consts::role::AI {
                    "CHATBOT"
                } else {
                    "USER"
                };
                json!({"role": role, "message": m.content})
            })
            .collect();
        let message = history
            .pop()
            .map(|mut last| last["message"].take())
            .unwrap_or(Value::String(String::new()));
        let mut body = json!({});
        body["message"] = message;
        body["chat_history"] = Value::Array(history);
        // cohere's system slot is `preamble` (system turns are filtered above)
        let system = self.base.system_text();
        if !system.is_empty() {
            body["preamble"] = system.into();
        }
        if let Some(p) = self.base.chat_params()
            && let Some(mt) = p.max_tokens
        {
            body["max_tokens"] = json!(mt);
        }
        body
    }
}

#[async_trait::async_trait]
impl ModelEngine for CohereEngine {
    /// AWS Bedrock Cohere Command: {message, chat_history[{role USER/CHATBOT, message}]};
    /// response {text, finish_reason} (legacy Command: {generations: [{text, finish_reason}]}),
    /// billed counts in the Bedrock headers; streams `{text, event_type, finish_reason}` frames.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        let body = self.build_body();
        if self.base.request.stream {
            let mut resp = GatewayResponse {
                model,
                is_messages_protocol: true,
                ..Default::default()
            };
            let mut full = String::new();
            let (status, r) = bedrock_stream(&mut self.base, body, |v| {
                let mut chunks = Vec::new();
                if let Some(t) = v["text"].as_str()
                    && !t.is_empty()
                    && v["event_type"] != "stream-end"
                {
                    full.push_str(t);
                    chunks.push(StreamChunk {
                        delta: t.to_owned(),
                        ..Default::default()
                    });
                }
                if let Some(fr) = v["finish_reason"].as_str() {
                    resp.finish_reason = cohere_finish_reason(Some(fr));
                    chunks.push(StreamChunk {
                        finish_reason: Some(resp.finish_reason.clone()),
                        ..Default::default()
                    });
                }
                invocation_metrics(&v, &mut resp);
                Ok(chunks)
            })
            .await?;
            resp.message = full;
            resp.total_tokens = resp.prompt_tokens.saturating_add(resp.completion_tokens);
            resp.raw_usage = Some(
                json!({"input_tokens": resp.prompt_tokens, "output_tokens": resp.completion_tokens}),
            );
            return Ok(EngineOutcome::from_pump(resp, status, r));
        }
        let (status, mut v, headers) = bedrock_invoke(&mut self.base, &model, body).await?;
        let message = crate::engine::take_string(&mut v, "/text")
            .or_else(|| crate::engine::take_string(&mut v, "/generations/0/text"))
            .unwrap_or_default();
        let finish_reason = cohere_finish_reason(
            v["finish_reason"]
                .as_str()
                .or_else(|| v["generations"][0]["finish_reason"].as_str()),
        );
        let meta = &v["meta"];
        let body_count = |key: &str| match crate::engine::tok(&meta["billed_units"][key]) {
            0 => crate::engine::tok(&meta["tokens"][key]),
            n => n,
        };
        let (input, output) = bedrock_header_usage(
            &headers,
            (body_count("input_tokens"), body_count("output_tokens")),
        );
        let resp = GatewayResponse {
            message,
            model,
            finish_reason,
            prompt_tokens: input,
            completion_tokens: output,
            total_tokens: input.saturating_add(output),
            raw_usage: Some(json!({"input_tokens": input, "output_tokens": output})),
            is_messages_protocol: true, // anthropic's usage fields align with cohere's input/output
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }
}

fn cohere_finish_reason(vendor: Option<&str>) -> String {
    match vendor {
        None | Some("COMPLETE") => "stop".to_owned(),
        Some("MAX_TOKENS") => "length".to_owned(),
        Some(other) => other.to_lowercase(),
    }
}

base_engine!(LlamaEngine);

#[async_trait::async_trait]
impl ModelEngine for LlamaEngine {
    /// AWS Bedrock Llama: {prompt, max_gen_len, temperature};
    /// response {generation, prompt_token_count, generation_token_count, stop_reason},
    /// streamed as the same shape per delta.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        // llama is completion-style: collapse the conversation into a prompt
        let prompt: String = self
            .base
            .request
            .message
            .iter()
            .map(|m| format!("{}: {}\n", m.role, m.content))
            .collect::<String>()
            + "assistant: ";
        let mut body = json!({});
        body["prompt"] = prompt.into();
        if let Some(p) = self.base.chat_params() {
            if let Some(mt) = p.max_tokens {
                body["max_gen_len"] = json!(mt);
            }
            if let Some(t) = p.temperature {
                body["temperature"] = json!(t);
            }
        }
        if self.base.request.stream {
            let mut resp = GatewayResponse {
                model,
                ..Default::default()
            };
            let mut full = String::new();
            let (status, r) = bedrock_stream(&mut self.base, body, |v| {
                let mut chunks = Vec::new();
                if let Some(t) = v["generation"].as_str()
                    && !t.is_empty()
                {
                    full.push_str(t);
                    chunks.push(StreamChunk {
                        delta: t.to_owned(),
                        ..Default::default()
                    });
                }
                if let Some(n) = v["prompt_token_count"].as_i64() {
                    resp.prompt_tokens = n;
                }
                if let Some(n) = v["generation_token_count"].as_i64() {
                    resp.completion_tokens = n;
                }
                if let Some(sr) = v["stop_reason"].as_str() {
                    resp.finish_reason = sr.to_owned();
                    chunks.push(StreamChunk {
                        finish_reason: Some(sr.to_owned()),
                        ..Default::default()
                    });
                }
                invocation_metrics(&v, &mut resp);
                Ok(chunks)
            })
            .await?;
            resp.message = full;
            let total = resp.prompt_tokens.saturating_add(resp.completion_tokens);
            resp.total_tokens = total;
            resp.raw_usage = Some(json!({
                "prompt_tokens": resp.prompt_tokens, "completion_tokens": resp.completion_tokens,
                "total_tokens": total
            }));
            return Ok(EngineOutcome::from_pump(resp, status, r));
        }
        let (status, mut v, headers) = bedrock_invoke(&mut self.base, &model, body).await?;
        let (pt, ct) = bedrock_header_usage(
            &headers,
            (
                crate::engine::tok(&v["prompt_token_count"]),
                crate::engine::tok(&v["generation_token_count"]),
            ),
        );
        let total = pt.saturating_add(ct);
        let resp = GatewayResponse {
            message: crate::engine::take_string(&mut v, "/generation").unwrap_or_default(),
            model,
            finish_reason: crate::engine::take_string(&mut v, "/stop_reason")
                .unwrap_or_else(|| "stop".to_owned()),
            prompt_tokens: pt,
            completion_tokens: ct,
            total_tokens: total,
            raw_usage: Some(
                json!({"prompt_tokens": pt, "completion_tokens": ct, "total_tokens": total}),
            ),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }
}

base_engine!(DashScopeEngine);

impl DashScopeEngine {
    fn build_body(&self, stream: bool) -> GResult<Value> {
        let model = self.base.model_name()?.to_owned();
        let messages: Vec<Value> = self
            .base
            .request
            .message
            .iter()
            .map(|m| {
                json!({"role": if m.role == gw_consts::role::AI {"assistant"}
                                 else if m.role == gw_consts::role::SYSTEM {"system"}
                                 else {"user"},
                       "content": m.content})
            })
            .collect();
        let mut parameters = json!({"result_format": "message"});
        if stream {
            // deltas instead of the full-text-so-far in every frame
            parameters["incremental_output"] = json!(true);
        }
        if let Some(p) = self.base.chat_params() {
            if let Some(t) = p.temperature {
                parameters["temperature"] = json!(t);
            }
            if let Some(t) = p.top_p {
                parameters["top_p"] = json!(t);
            }
            if let Some(mt) = p.max_tokens {
                parameters["max_tokens"] = json!(mt);
            }
        }
        let mut body = json!({"model": model, "input": {}});
        body["input"]["messages"] = Value::Array(messages);
        body["parameters"] = parameters;
        Ok(body)
    }

    fn url(&self) -> String {
        format!(
            "{}/api/v1/services/aigc/text-generation/generation",
            self.base.base_url("mock://dashscope.aliyuncs.com")
        )
    }

    fn headers(&self, stream: bool) -> Vec<(String, String)> {
        let mut h = self.base.bearer_headers();
        if stream {
            // DashScope streams only when this header is present
            h.push(("X-DashScope-SSE".into(), "enable".into()));
        }
        h
    }

    /// Native DashScope streaming: SSE frames decoded as they arrive and
    /// forwarded through `stream_tx` (the live-pump contract).
    async fn run_stream(&mut self) -> GResult<EngineOutcome> {
        let body = self.build_body(true)?;
        let reply = self
            .base
            .post_raw(&self.url(), self.headers(true), body, true)
            .await?;
        let status = reply.status;
        let mut resp = GatewayResponse {
            model: self.base.model_name()?.to_owned(),
            ..Default::default()
        };
        crate::pump::reject_json_error("dashscope", status, &reply.body)?;
        let mut full = String::new();
        let r = crate::pump::pump_sse(
            "dashscope",
            reply.body,
            self.base.request.stream_tx.clone(),
            |v| dashscope_apply_frame(&v, status, &mut resp, &mut full),
        )
        .await?;
        resp.message = full;
        crate::engine::fill_total_if_zero(&mut resp);
        resp.common_usage = dashscope_common_usage(&resp);
        Ok(EngineOutcome::from_pump(resp, status, r))
    }
}

#[async_trait::async_trait]
impl ModelEngine for DashScopeEngine {
    /// Ali DashScope native wire (not the openai-compatible mode):
    /// {model, input:{messages}, parameters:{result_format:"message",…}};
    /// response {output:{choices:[{message,finish_reason}]}, usage{input/output/total_tokens}}.
    /// Streaming: `X-DashScope-SSE: enable` + `incremental_output`.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        if self.base.request.stream {
            return self.run_stream().await;
        }
        let body = self.build_body(false)?;
        let (status, mut v) = self
            .base
            .post_json(&self.url(), self.headers(false), body)
            .await?;
        let mut resp = GatewayResponse {
            message: crate::engine::take_string(&mut v, "/output/choices/0/message/content")
                .unwrap_or_default(),
            model: self.base.model_name()?.to_owned(),
            finish_reason: crate::engine::take_string(&mut v, "/output/choices/0/finish_reason")
                .unwrap_or_else(|| "stop".to_owned()),
            ..Default::default()
        };
        dashscope_apply_usage(&v["usage"], &mut resp);
        crate::engine::fill_total_if_zero(&mut resp);
        resp.common_usage = dashscope_common_usage(&resp);
        Ok(EngineOutcome::with_status(resp, status))
    }
}

/// Apply one DashScope SSE frame; returns the chunks it yields. Running
/// frames carry the literal string "null" as finish_reason; usage is
/// cumulative — the last frame's counts win.
fn dashscope_apply_frame(
    v: &Value,
    status: u16,
    resp: &mut GatewayResponse,
    full: &mut String,
) -> GResult<Vec<StreamChunk>> {
    if let Some(err) = crate::engine::vendor_error(status, v) {
        return Err(err);
    }
    let mut chunks = Vec::new();
    let choice = &v["output"]["choices"][0];
    if let Some(t) = choice["message"]["content"].as_str()
        && !t.is_empty()
    {
        full.push_str(t);
        chunks.push(StreamChunk {
            delta: t.to_owned(),
            ..Default::default()
        });
    }
    if let Some(fr) = choice["finish_reason"].as_str()
        && !fr.is_empty()
        && fr != "null"
    {
        resp.finish_reason = fr.to_owned();
        chunks.push(StreamChunk {
            finish_reason: Some(fr.to_owned()),
            ..Default::default()
        });
    }
    dashscope_apply_usage(&v["usage"], resp);
    Ok(chunks)
}

fn dashscope_apply_usage(usage: &Value, resp: &mut GatewayResponse) {
    if usage.is_null() {
        return;
    }
    if let Some(it) = usage["input_tokens"].as_i64() {
        resp.prompt_tokens = it.max(0);
    }
    if let Some(ot) = usage["output_tokens"].as_i64() {
        resp.completion_tokens = ot.max(0);
    }
    if let Some(tt) = usage["total_tokens"].as_i64() {
        resp.total_tokens = tt.max(0);
    }
    if let Some(cached) = usage["prompt_tokens_details"]["cached_tokens"].as_i64() {
        resp.read_cached_prompt_tokens = cached.max(0);
    }
}

fn dashscope_common_usage(resp: &GatewayResponse) -> Option<gw_models::CommonUsage> {
    Some(gw_models::CommonUsage::from_openai_parts(
        resp.prompt_tokens,
        resp.completion_tokens,
        resp.read_cached_prompt_tokens,
        0,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use gw_consts::Protocol;
    use gw_models::{ChatMsg, GatewayRequest, ModelParamV2};

    use super::*;
    use crate::transport::{
        HeaderMap, MockTransport, SharedTransport, Transport, UpstreamBody, UpstreamRequest,
        UpstreamResponse,
    };

    /// A Bedrock reply with the billed-token headers AWS stamps on InvokeModel.
    #[derive(Debug)]
    struct BedrockReply(&'static str, i64, i64);

    #[async_trait::async_trait]
    impl Transport for BedrockReply {
        async fn send(&self, _req: UpstreamRequest) -> GResult<UpstreamResponse> {
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-amzn-bedrock-input-token-count",
                self.1.to_string().parse().unwrap(),
            );
            headers.insert(
                "x-amzn-bedrock-output-token-count",
                self.2.to_string().parse().unwrap(),
            );
            Ok(UpstreamResponse {
                status: 200,
                body: UpstreamBody::Json(self.0.as_bytes().to_vec().into()),
                headers,
            })
        }
    }

    fn req(mt: Protocol, name: &str) -> GatewayRequest {
        GatewayRequest {
            message: vec![ChatMsg::text("user", "hello bespoke")],
            model_param_v2: Some(ModelParamV2::with_name(mt, name)),
            ..Default::default()
        }
    }

    fn t() -> SharedTransport {
        Arc::new(MockTransport)
    }

    #[tokio::test]
    async fn ernie_wire_shape() {
        let mut e = ErnieEngine::new(req(Protocol::Ernie, "ernie-4.0"), t());
        let out = e.run().await.unwrap();
        assert!(
            out.response
                .message
                .contains("[mock-ernie] you said: hello bespoke")
        );
        assert!(out.response.total_tokens > 0);
        assert_eq!(out.response.finish_reason, "stop");
    }

    #[tokio::test]
    async fn minimax_v1_wire_shape() {
        let mut e = MinimaxV1Engine::new(req(Protocol::MinimaxV1, "abab6.5"), t());
        let out = e.run().await.unwrap();
        assert!(
            out.response
                .message
                .contains("[mock-minimax] you said: hello bespoke")
        );
        assert!(out.response.total_tokens > 0);
    }

    #[tokio::test]
    async fn cohere_wire_shape() {
        let mut e = CohereEngine::new(req(Protocol::AwsCohere, "cohere.command-r-v1:0"), t());
        let out = e.run().await.unwrap();
        assert!(
            out.response
                .message
                .contains("[mock-cohere] you said: hello bespoke")
        );
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
    }

    #[tokio::test]
    async fn bedrock_headers_bill_command_r_and_the_legacy_command_shape_parses() {
        let mut e = CohereEngine::new(
            req(Protocol::AwsCohere, "cohere.command-r-v1:0"),
            Arc::new(BedrockReply(
                r#"{"response_id":"r","text":"hi there","finish_reason":"COMPLETE"}"#,
                57,
                9,
            )),
        );
        let out = e.run().await.unwrap();
        assert_eq!(out.response.message, "hi there");
        assert_eq!(out.response.finish_reason, "stop");
        assert_eq!(
            (out.response.prompt_tokens, out.response.completion_tokens),
            (57, 9)
        );

        let mut e = CohereEngine::new(
            req(Protocol::AwsCohere, "cohere.command-text-v14"),
            Arc::new(BedrockReply(
                r#"{"generations":[{"id":"g","text":"legacy hi","finish_reason":"MAX_TOKENS"}],"prompt":""}"#,
                12,
                4,
            )),
        );
        let out = e.run().await.unwrap();
        assert_eq!(out.response.message, "legacy hi");
        assert_eq!(out.response.finish_reason, "length");
        assert_eq!(out.response.total_tokens, 16);

        let mut e = LlamaEngine::new(
            req(Protocol::AwsLlama, "meta.llama3-1-8b-instruct-v1:0"),
            Arc::new(BedrockReply(
                r#"{"generation":"yo","prompt_token_count":1,"generation_token_count":1,"stop_reason":"stop"}"#,
                30,
                20,
            )),
        );
        let out = e.run().await.unwrap();
        assert_eq!(
            (out.response.prompt_tokens, out.response.completion_tokens),
            (30, 20)
        );
    }

    #[tokio::test]
    async fn bedrock_streams_reassemble_text_and_take_the_invocation_metrics() {
        let mut r = req(Protocol::AwsLlama, "meta.llama3-1-8b-instruct-v1:0");
        r.stream = true;
        let out = LlamaEngine::new(r, t()).run().await.unwrap();
        assert!(
            out.response.message.contains("[mock-llama]"),
            "{:?}",
            out.response
        );
        assert_eq!(out.response.finish_reason, "stop");
        assert!(out.chunks.iter().any(|c| c.finish_reason.is_some()));
        assert!(out.response.total_tokens > 0);

        let mut r = req(Protocol::AwsCohere, "cohere.command-r-v1:0");
        r.stream = true;
        let out = CohereEngine::new(r, t()).run().await.unwrap();
        assert!(
            out.response
                .message
                .contains("[mock-cohere] you said: hello bespoke"),
            "{:?}",
            out.response
        );
        assert_eq!(out.response.finish_reason, "stop");
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
    }

    #[tokio::test]
    async fn llama_wire_shape() {
        let mut e = LlamaEngine::new(
            req(Protocol::AwsLlama, "meta.llama3-70b-instruct-v1:0"),
            t(),
        );
        let out = e.run().await.unwrap();
        assert!(out.response.message.contains("[mock-llama]"));
        assert!(out.response.total_tokens > 0);
    }
    #[tokio::test]
    async fn dashscope_stream_decodes_frames() {
        let mut r = req(Protocol::Dashscope, "qwen-max");
        r.stream = true;
        let mut e = DashScopeEngine::new(r, t());
        let out = e.run().await.unwrap();
        assert!(out.chunks.len() >= 3, "chunks: {:?}", out.chunks);
        assert!(
            out.response
                .message
                .contains("[mock-dashscope] you said: hello bespoke")
        );
        assert_eq!(out.response.finish_reason, "stop");
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
        assert!(out.chunks.iter().any(|c| c.finish_reason.is_some()));
    }

    #[tokio::test]
    async fn dashscope_wire_shape() {
        let mut e = DashScopeEngine::new(req(Protocol::Dashscope, "qwen-max"), t());
        let out = e.run().await.unwrap();
        assert!(
            out.response
                .message
                .contains("[mock-dashscope] you said: hello bespoke")
        );
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
        assert_eq!(out.response.finish_reason, "stop");
    }
}
