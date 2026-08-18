//! Bespoke vendor wire shapes: these vendors do NOT speak the OpenAI protocol;
//! each engine builds the vendor's real request shape and parses its real
//! response shape (the mock answers in the same shapes). AWS engines compute a
//! real SigV4 Authorization header.

use gw_models::{GResult, GatewayError, GatewayResponse};
use serde_json::{Value, json};

use crate::base::{Base, base_engine};
use crate::engine::{EngineOutcome, ModelEngine, StreamChunk};
use crate::sigv4::{SigV4Params, sign};
use crate::transport::HeaderMap;

/// Deterministic SigV4 date for the mock round; live calls stamp now.
const MOCK_AMZ_DATE: &str = "20250101T000000Z";

/// SigV4 headers for a bedrock-style call. `creds` = real `(access_key, secret_key)`
/// at go-live (from the account's env-var pair), else the inert mock credentials.
/// The region is the endpoint host's (`bedrock-runtime.<region>.amazonaws.com`).
fn aws_headers(
    host: &str,
    uri: &str,
    payload: &[u8],
    creds: Option<(&str, &str)>,
) -> Vec<(String, String)> {
    let stamped;
    let amz_date = match creds {
        Some(_) => {
            stamped = amz_date_now();
            stamped.as_str()
        }
        None => MOCK_AMZ_DATE,
    };
    let (access_key, secret_key) = creds.unwrap_or(("AKIDMOCK", "mock-secret"));
    let region = host
        .strip_prefix("bedrock-runtime.")
        .and_then(|rest| rest.split('.').next())
        .filter(|region| !region.is_empty())
        .unwrap_or("us-east-1");
    let canonical = canonical_uri(uri);
    let (_, authorization) = sign(&SigV4Params {
        access_key,
        secret_key,
        region,
        service: "bedrock",
        amz_date,
        method: "POST",
        canonical_uri: &canonical,
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

fn amz_date_now() -> String {
    amz_date(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0),
    )
}

/// A UTC epoch second as SigV4's `YYYYMMDDTHHMMSSZ` (civil-from-days, no
/// calendar dependency).
fn amz_date(secs: i64) -> String {
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (h, m, s) = (rem / 3600, rem % 3600 / 60, rem % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(mo <= 2);
    format!("{y:04}{mo:02}{d:02}T{h:02}{m:02}{s:02}Z")
}

fn invoke_uri(model: &str) -> String {
    format!("/model/{model}/invoke")
}

/// SigV4's canonical URI: the wire path with every byte outside the
/// unreserved set percent-encoded once (`:` in `…-v1:0` → `%3A`), `/` kept.
fn canonical_uri(path: &str) -> String {
    let mut out = String::with_capacity(path.len() + 8);
    for byte in path.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// One Bedrock invoke: host + scheme from the account endpoint at go-live
/// (else the mock sentinel); SigV4 signs this same host so URL and signature
/// agree. Raw extras merge before signing so the signature covers the exact
/// bytes sent — and the body serializes once, not per layer.
async fn bedrock_invoke(
    base: &mut Base,
    uri: &str,
    mut body: Value,
) -> GResult<(u16, Value, HeaderMap)> {
    if let Some(obj) = body.as_object_mut() {
        let raw = base.take_raw();
        crate::base::merge_raw_extras_owned(obj, raw);
    }
    let root = base.base_url("mock://bedrock-runtime.us-east-1.amazonaws.com");
    let host = root.split_once("://").map(|(_, h)| h).unwrap_or(root);
    let payload = crate::base::body_bytes(&body)?;
    let creds = base.aws_credentials();
    let headers = aws_headers(
        host,
        uri,
        &payload,
        creds
            .as_ref()
            .map(|(a, s): &(String, String)| (a.as_str(), s.as_str())),
    );
    base.post_json_bytes(&format!("{root}{uri}"), headers, payload)
        .await
}

/// Bedrock stamps every InvokeModel reply with the billed token counts; the
/// bodies carry them only for some families (Llama yes, Command R only when
/// streaming), so the headers win when present.
fn bedrock_header_usage(headers: &HeaderMap, body: (i64, i64)) -> (i64, i64) {
    let count = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i64>().ok())
    };
    match (
        count("x-amzn-bedrock-input-token-count"),
        count("x-amzn-bedrock-output-token-count"),
    ) {
        (Some(input), Some(output)) => (input, output),
        _ => body,
    }
}

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

#[async_trait::async_trait]
impl ModelEngine for CohereEngine {
    /// AWS Bedrock Cohere Command: {message, chat_history[{role USER/CHATBOT, message}]};
    /// response {text, finish_reason, meta{tokens{input_tokens,output_tokens}}}.
    async fn run(&mut self) -> GResult<EngineOutcome> {
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
        let uri = invoke_uri(self.base.model_name()?);
        let (status, mut v, headers) = bedrock_invoke(&mut self.base, &uri, body).await?;
        // Command R answers {text, finish_reason}; the legacy Command models
        // answer {generations: [{text, finish_reason}]}
        let message = crate::engine::take_string(&mut v, "/text")
            .or_else(|| crate::engine::take_string(&mut v, "/generations/0/text"))
            .unwrap_or_default();
        let finish_reason = match v["finish_reason"]
            .as_str()
            .or_else(|| v["generations"][0]["finish_reason"].as_str())
        {
            None | Some("COMPLETE") => "stop".to_owned(),
            Some("MAX_TOKENS") => "length".to_owned(),
            Some(other) => other.to_lowercase(),
        };
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

base_engine!(LlamaEngine);

#[async_trait::async_trait]
impl ModelEngine for LlamaEngine {
    /// AWS Bedrock Llama: {prompt, max_gen_len, temperature};
    /// response {generation, prompt_token_count, generation_token_count, stop_reason}.
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
        let uri = invoke_uri(self.base.model_name()?);
        let (status, mut v, headers) = bedrock_invoke(&mut self.base, &uri, body).await?;
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
        MockTransport, SharedTransport, Transport, UpstreamBody, UpstreamRequest, UpstreamResponse,
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

    #[test]
    fn amz_date_is_utc_sigv4_shaped_and_region_comes_from_the_host() {
        assert_eq!(amz_date(1_709_251_199), "20240229T235959Z");
        assert_eq!(amz_date(0), "19700101T000000Z");
        assert_eq!(amz_date(1_786_988_810), "20260817T174650Z");
        assert!(amz_date_now().starts_with("20"));
        assert_eq!(
            canonical_uri("/model/meta.llama3-1-8b-instruct-v1:0/invoke"),
            "/model/meta.llama3-1-8b-instruct-v1%3A0/invoke"
        );

        let live = aws_headers(
            "bedrock-runtime.eu-west-1.amazonaws.com",
            "/model/x/invoke",
            b"{}",
            Some(("AKIDEXAMPLE", "secret")),
        );
        let auth = &live.iter().find(|(k, _)| k == "authorization").unwrap().1;
        assert!(auth.contains("/eu-west-1/bedrock/aws4_request"), "{auth}");
        let date = &live.iter().find(|(k, _)| k == "x-amz-date").unwrap().1;
        assert_ne!(date, MOCK_AMZ_DATE);
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
