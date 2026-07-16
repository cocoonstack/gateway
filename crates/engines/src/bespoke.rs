//! Bespoke vendor wire shapes: these vendors do NOT speak the OpenAI protocol;
//! each engine builds the vendor's real request shape and parses its real
//! response shape (the mock answers in the same shapes). AWS engines compute a
//! real SigV4 Authorization header.

use gw_models::{GResult, GatewayError, GatewayResponse};
use serde_json::{Value, json};

use crate::base::{Base, base_engine};
use crate::engine::{EngineOutcome, ModelEngine, StreamChunk};
use crate::sigv4::{SigV4Params, sign};

/// Deterministic SigV4 date for the mock round; live calls would stamp now().
const MOCK_AMZ_DATE: &str = "20250101T000000Z";

/// SigV4 headers for a bedrock-style call. `creds` = real `(access_key, secret_key)`
/// at go-live (from the account's env-var pair), else the inert mock credentials.
fn aws_headers(
    host: &str,
    uri: &str,
    payload: &[u8],
    creds: Option<(&str, &str)>,
) -> Vec<(String, String)> {
    let amz_date = MOCK_AMZ_DATE;
    let (access_key, secret_key) = creds.unwrap_or(("AKIDMOCK", "mock-secret"));
    let (_, authorization) = sign(&SigV4Params {
        access_key,
        secret_key,
        region: "us-east-1",
        service: "bedrock",
        amz_date,
        method: "POST",
        canonical_uri: uri,
        canonical_query: "",
        headers: &[("host", host), ("x-amz-date", amz_date)],
        payload,
    });
    vec![
        ("host".into(), host.into()),
        ("x-amz-date".into(), amz_date.into()),
        ("authorization".into(), authorization),
        // Bedrock InvokeModel requires accept; content-type is added by post_json.
        ("accept".into(), "application/json".into()),
    ]
}

/// One Bedrock invoke: host + scheme from the account endpoint at go-live
/// (else the mock sentinel); SigV4 signs this same host so URL and signature
/// agree. Raw extras merge before signing so the signature covers the exact
/// bytes sent — and the body serializes once, not per layer.
async fn bedrock_invoke(base: &Base, uri: &str, mut body: Value) -> GResult<(u16, Value)> {
    let root = base.base_url("mock://bedrock-runtime.us-east-1.amazonaws.com");
    let host = root
        .split_once("://")
        .map(|(_, h)| h)
        .unwrap_or(&root)
        .to_owned();
    if let Some(obj) = body.as_object_mut() {
        crate::base::merge_raw_extras(obj, &base.param()?.raw);
    }
    let payload = crate::base::body_bytes(&body)?;
    let creds = base.aws_credentials();
    let headers = aws_headers(
        &host,
        uri,
        &payload,
        creds
            .as_ref()
            .map(|(a, s): &(String, String)| (a.as_str(), s.as_str())),
    );
    base.post_json_bytes(&format!("{root}{uri}"), headers, payload)
        .await
}

base_engine!(ErnieEngine);

#[async_trait::async_trait]
impl ModelEngine for ErnieEngine {
    /// Baidu Ernie (Wenxin): /wenxinworkshop/chat/{model}?access_token=…
    /// Request {messages,[temperature]}; response {result, usage{...}, is_truncated}.
    async fn run(&self) -> GResult<EngineOutcome> {
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
        // move owned values into the body — json! interpolation would deep-copy them
        let mut body = json!({});
        body["messages"] = Value::Array(messages);
        // ernie's system is a top-level field (system turns are filtered above)
        let system = self.base.system_text();
        if !system.is_empty() {
            body["system"] = json!(system);
        }
        if let Some(p) = self.base.chat_params()
            && let Some(t) = p.temperature
        {
            body["temperature"] = json!(t);
        }
        // Baidu auth is an access_token query param; real token from the env var at go-live
        let url = format!(
            "{}/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/{model}?access_token={}",
            self.base.base_url("mock://aip.baidubce.com"),
            self.base.api_key(),
        );
        let (status, v) = self.base.post_json(&url, vec![], body).await?;
        let usage = &v["usage"];
        let resp = GatewayResponse {
            message: v["result"].as_str().unwrap_or_default().to_owned(),
            model,
            finish_reason: if v["is_truncated"].as_bool().unwrap_or(false) {
                "length".into()
            } else {
                "stop".into()
            },
            prompt_tokens: crate::engine::tok(&usage["prompt_tokens"]),
            completion_tokens: crate::engine::tok(&usage["completion_tokens"]),
            total_tokens: crate::engine::tok(&usage["total_tokens"]),
            raw_usage_json: serde_json::to_vec(usage).unwrap_or_default(),
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
    async fn run(&self) -> GResult<EngineOutcome> {
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
            body["prompt"] = json!(system);
            body["role_meta"] = json!({"user_name": "USER", "bot_name": "BOT"});
        }
        let url = format!(
            "{}/v1/text/chatcompletion",
            self.base.base_url("mock://api.minimax.chat")
        );
        let (status, v) = self
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
            message: v["reply"].as_str().unwrap_or_default().to_owned(),
            model,
            finish_reason: "stop".into(),
            total_tokens: total,
            raw_usage_json: serde_json::to_vec(&v["usage"]).unwrap_or_default(),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }
}

base_engine!(CohereEngine);

#[async_trait::async_trait]
impl ModelEngine for CohereEngine {
    /// AWS Bedrock Cohere Command: {message, chat_history[{role USER/CHATBOT, message}]};
    /// response {text, finish_reason, meta{tokens{input_tokens,output_tokens}}}.
    async fn run(&self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
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
        // move owned values into the body — json! interpolation would deep-copy them
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
            body["preamble"] = json!(system);
        }
        if let Some(p) = self.base.chat_params()
            && let Some(mt) = p.max_tokens
        {
            body["max_tokens"] = json!(mt);
        }
        let (status, v) =
            bedrock_invoke(&self.base, "/model/cohere.command-r/invoke", body).await?;
        let tokens = &v["meta"]["tokens"];
        let (input, output) = (
            crate::engine::tok(&tokens["input_tokens"]),
            crate::engine::tok(&tokens["output_tokens"]),
        );
        let resp = GatewayResponse {
            message: v["text"].as_str().unwrap_or_default().to_owned(),
            model,
            finish_reason: v["finish_reason"].as_str().unwrap_or("stop").to_lowercase(),
            prompt_tokens: input,
            completion_tokens: output,
            total_tokens: input.saturating_add(output),
            raw_usage_json: serde_json::to_vec(
                &json!({"input_tokens": input, "output_tokens": output}),
            )
            .unwrap_or_default(),
            is_messages_protocol: true, // anthropic's usage fields align with cohere's input/output
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }
}

base_engine!(LlamaEngine);

#[async_trait::async_trait]
impl ModelEngine for LlamaEngine {
    /// AWS Bedrock Llama: {prompt, max_gen_len, temperature};
    /// response {generation, prompt_token_count, generation_token_count, stop_reason}.
    async fn run(&self) -> GResult<EngineOutcome> {
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
        let (status, v) = bedrock_invoke(
            &self.base,
            "/model/meta.llama3-70b-instruct-v1/invoke",
            body,
        )
        .await?;
        let (pt, ct) = (
            crate::engine::tok(&v["prompt_token_count"]),
            crate::engine::tok(&v["generation_token_count"]),
        );
        let total = pt.saturating_add(ct);
        let resp = GatewayResponse {
            message: v["generation"].as_str().unwrap_or_default().to_owned(),
            model,
            finish_reason: v["stop_reason"].as_str().unwrap_or("stop").to_owned(),
            prompt_tokens: pt,
            completion_tokens: ct,
            total_tokens: total,
            raw_usage_json: serde_json::to_vec(
                &json!({"prompt_tokens": pt, "completion_tokens": ct, "total_tokens": total}),
            )
            .unwrap_or_default(),
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
        // move owned values into the body — json! interpolation would deep-copy them
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
    async fn run_stream(&self) -> GResult<EngineOutcome> {
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
            |v| dashscope_apply_frame(v, status, &mut resp, &mut full),
        )
        .await?;
        resp.message = full;
        crate::engine::fill_total_if_zero(&mut resp);
        resp.raw_usage_json = dashscope_raw_usage(&resp);
        Ok(EngineOutcome::from_pump(resp, status, r))
    }
}

#[async_trait::async_trait]
impl ModelEngine for DashScopeEngine {
    /// Ali DashScope native wire (not the openai-compatible mode):
    /// {model, input:{messages}, parameters:{result_format:"message",…}};
    /// response {output:{choices:[{message,finish_reason}]}, usage{input/output/total_tokens}}.
    /// Streaming: `X-DashScope-SSE: enable` + `incremental_output`.
    async fn run(&self) -> GResult<EngineOutcome> {
        if self.base.request.stream {
            return self.run_stream().await;
        }
        let body = self.build_body(false)?;
        let (status, v) = self
            .base
            .post_json(&self.url(), self.headers(false), body)
            .await?;
        let choice = &v["output"]["choices"][0];
        let mut resp = GatewayResponse {
            message: choice["message"]["content"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            model: self.base.model_name()?.to_owned(),
            finish_reason: choice["finish_reason"]
                .as_str()
                .unwrap_or("stop")
                .to_owned(),
            ..Default::default()
        };
        dashscope_apply_usage(&v["usage"], &mut resp);
        crate::engine::fill_total_if_zero(&mut resp);
        resp.raw_usage_json = dashscope_raw_usage(&resp);
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

/// usage dialect normalized to the openai shape at the engine boundary.
fn dashscope_raw_usage(resp: &GatewayResponse) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "prompt_tokens": resp.prompt_tokens,
        "completion_tokens": resp.completion_tokens,
        "total_tokens": resp.total_tokens,
        "prompt_tokens_details": {"cached_tokens": resp.read_cached_prompt_tokens},
    }))
    .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{MockTransport, SharedTransport};
    use gw_consts::Protocol;
    use gw_models::{ChatMsg, GatewayRequest, ModelParamV2};
    use std::sync::Arc;

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
        let e = ErnieEngine::new(req(Protocol::Ernie, "ernie-4.0"), t());
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
        let e = MinimaxV1Engine::new(req(Protocol::MinimaxV1, "abab6.5"), t());
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
        let e = CohereEngine::new(req(Protocol::AwsCohere, "command-r"), t());
        let out = e.run().await.unwrap();
        assert!(
            out.response
                .message
                .contains("[mock-cohere] you said: hello bespoke")
        );
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
    }

    #[tokio::test]
    async fn llama_wire_shape() {
        let e = LlamaEngine::new(req(Protocol::AwsLlama, "llama3-70b"), t());
        let out = e.run().await.unwrap();
        assert!(out.response.message.contains("[mock-llama]"));
        assert!(out.response.total_tokens > 0);
    }
    #[tokio::test]
    async fn dashscope_stream_decodes_frames() {
        let mut r = req(Protocol::Dashscope, "qwen-max");
        r.stream = true;
        let e = DashScopeEngine::new(r, t());
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
        let e = DashScopeEngine::new(req(Protocol::Dashscope, "qwen-max"), t());
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
