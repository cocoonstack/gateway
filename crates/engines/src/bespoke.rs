//! Bespoke vendor wire shapes — first batch of per-vendor fidelity.
//!
//! These four vendors do NOT speak the OpenAI protocol; each engine builds the
//! vendor's real request shape and parses its real response shape (the mock
//! answers in the same shapes). AWS engines compute a real SigV4 Authorization
//! header (pure computation; inert against the mock, live over real HTTP).

use ap_models::{
    GResult, GatewayError, GatewayRequest, GatewayResponse, Recorder, SimpleRecorder, TypedParams,
};
use chrono::Utc;
use serde_json::{Value, json};

use crate::engine::{EngineOutcome, ModelEngine};
use crate::sigv4::{SigV4Params, sign};
use crate::transport::{SharedTransport, UpstreamBody, UpstreamRequest};

struct Base {
    request: GatewayRequest,
    transport: SharedTransport,
    recorder: SimpleRecorder,
}

impl Base {
    fn new(request: GatewayRequest, transport: SharedTransport) -> Self {
        Self {
            request,
            transport,
            recorder: SimpleRecorder::new(Utc::now()),
        }
    }

    fn account(&self) -> String {
        self.request
            .account
            .as_ref()
            .map(|a| a.name.clone())
            .unwrap_or_default()
    }

    /// The go-live seam: the account's configured endpoint when set, else the
    /// `mock_sentinel` (offline); same seam as the OpenAI/family engines.
    fn base_url(&self, mock_sentinel: &str) -> String {
        self.request
            .account
            .as_ref()
            .map(|a| a.base_url(mock_sentinel).to_owned())
            .unwrap_or_else(|| mock_sentinel.to_owned())
    }

    /// The account's API key (env var at call time when live), else inert "mock".
    fn api_key(&self) -> String {
        self.request
            .account
            .as_ref()
            .and_then(|a| a.api_key())
            .unwrap_or_else(|| "mock".to_owned())
    }

    /// AWS `(access_key, secret_key)` from the account's env-var pair, if both set.
    fn aws_credentials(&self) -> Option<(String, String)> {
        self.request
            .account
            .as_ref()
            .and_then(|a| a.aws_credentials())
    }

    fn model_name(&self) -> GResult<&str> {
        self.request
            .model_param_v2
            .as_ref()
            .map(|p| p.model_name.as_str())
            .ok_or_else(|| GatewayError::bad_request("missing model param"))
    }

    fn chat_params(&self) -> Option<&ap_models::ChatParams> {
        match self.request.model_param_v2.as_ref()?.typed.as_ref()? {
            TypedParams::Chat(p) => Some(p),
            _ => None,
        }
    }

    async fn post_json(
        &self,
        url: &str,
        mut headers: Vec<(String, String)>,
        mut body: Value,
    ) -> GResult<(u16, Value)> {
        let param = self
            .request
            .model_param_v2
            .as_ref()
            .ok_or_else(|| GatewayError::bad_request("missing model param"))?;
        // Forward caller-set passthrough params the per-vendor extraction didn't
        // cover: some vendor SDKs serialize the whole param object, so every
        // field the caller set reaches the vendor. We cherry-pick a few typed
        // fields per engine, then let `raw` carry the rest — matching the
        // openai/claude engines. `or_insert` keeps typed fields authoritative.
        if let (Some(obj), Value::Object(extra)) = (body.as_object_mut(), &param.raw) {
            for (k, v) in extra {
                obj.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
        // ensure JSON content-type (real vendors reject POST without it). For the
        // AWS engines this is currently an unsigned header — signing content-type
        // into SigV4 is a live-integration refinement.
        if !headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        {
            headers.insert(0, ("content-type".into(), "application/json".into()));
        }
        let up = UpstreamRequest {
            protocol: param.protocol,
            method: "POST".to_owned(),
            url: url.to_owned(),
            headers,
            body: body.to_string().into_bytes(),
            stream: false,
            account: self.account(),
        };
        let reply = self.transport.send(up).await?;
        let bytes = match &reply.body {
            UpstreamBody::Json(b) => b,
            UpstreamBody::Sse(_) => return Err(GatewayError::internal("unexpected sse body")),
        };
        let v: Value = serde_json::from_slice(bytes)
            .map_err(|e| GatewayError::internal("parse vendor response").with_source(e))?;
        // generic vendor-error safety net (bespoke engines add their own vendor-
        // specific checks, e.g. minimax base_resp, on top of this).
        if let Some(err) = crate::engine::vendor_error(reply.status, &v) {
            return Err(err);
        }
        Ok((reply.status, v))
    }
}

/// SigV4 headers for a bedrock-style call. `creds` = real `(access_key, secret_key)`
/// at go-live (from the account's env-var pair), else the inert mock credentials.
fn aws_headers(
    host: &str,
    uri: &str,
    payload: &[u8],
    creds: Option<(&str, &str)>,
) -> Vec<(String, String)> {
    let amz_date = "20250101T000000Z"; // deterministic for the mock round
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

macro_rules! bespoke_engine {
    ($name:ident) => {
        pub struct $name {
            base: Base,
        }
        impl $name {
            pub fn new(request: GatewayRequest, transport: SharedTransport) -> Self {
                Self {
                    base: Base::new(request, transport),
                }
            }
        }
    };
}

// ------------------------------------------------- Baidu Ernie (Wenxin)

bespoke_engine!(ErnieEngine);

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
            .filter(|m| m.role != ap_consts::role::SYSTEM)
            .map(|m| {
                json!({"role": if m.role == ap_consts::role::AI {"assistant"} else {"user"},
                             "content": m.content})
            })
            .collect();
        let mut body = json!({"messages": messages});
        if let Some(p) = self.base.chat_params() {
            if let Some(t) = p.temperature {
                body["temperature"] = json!(t);
            }
            // ernie's system is a top-level field
            if let Some(s) = &p.system {
                body["system"] = json!(s);
            }
        }
        // Baidu auth is an access_token query param; real token from the account
        // env var at go-live, else the inert "mock" sentinel.
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
            prompt_tokens: usage["prompt_tokens"].as_i64().unwrap_or(0),
            completion_tokens: usage["completion_tokens"].as_i64().unwrap_or(0),
            total_tokens: usage["total_tokens"].as_i64().unwrap_or(0),
            raw_usage_json: usage.to_string().into_bytes(),
            http_code: status as i64,
            ..Default::default()
        };
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            ..Default::default()
        })
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

// ------------------------------------------------- MiniMax v1 (abab chatcompletion)

bespoke_engine!(MinimaxV1Engine);

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
            .filter(|m| m.role != ap_consts::role::SYSTEM)
            .map(|m| {
                json!({"sender_type": if m.role == ap_consts::role::AI {"BOT"} else {"USER"},
                       "text": m.content})
            })
            .collect();
        let body = json!({"model": model, "messages": messages});
        let url = format!(
            "{}/v1/text/chatcompletion",
            self.base.base_url("mock://api.minimax.chat")
        );
        let auth = vec![(
            "authorization".into(),
            format!("Bearer {}", self.base.api_key()),
        )];
        let (status, v) = self.base.post_json(&url, auth, body).await?;
        // base_resp non-zero is an error (minimax's business error-code convention)
        let code = v["base_resp"]["status_code"].as_i64().unwrap_or(0);
        if code != 0 {
            return Err(GatewayError::new(
                ap_consts::ErrCode::FED_RESP_STATUS_NOT_ZERO,
                502,
                format!("minimax base_resp {code}: {}", v["base_resp"]["status_msg"]),
            ));
        }
        let total = v["usage"]["total_tokens"].as_i64().unwrap_or(0);
        let resp = GatewayResponse {
            message: v["reply"].as_str().unwrap_or_default().to_owned(),
            model,
            finish_reason: "stop".into(),
            total_tokens: total,
            raw_usage_json: v["usage"].to_string().into_bytes(),
            http_code: status as i64,
            ..Default::default()
        };
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            ..Default::default()
        })
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

// ------------------------------------------------- AWS Bedrock: Cohere Command

bespoke_engine!(CohereEngine);

#[async_trait::async_trait]
impl ModelEngine for CohereEngine {
    /// AWS Bedrock Cohere Command: {message, chat_history[{role USER/CHATBOT, message}]};
    /// response {text, finish_reason, meta{tokens{input_tokens,output_tokens}}}.
    async fn run(&self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        let mut history: Vec<Value> = Vec::new();
        let mut message = String::new();
        for m in &self.base.request.message {
            if m.role == ap_consts::role::SYSTEM {
                continue;
            }
            let role = if m.role == ap_consts::role::AI {
                "CHATBOT"
            } else {
                "USER"
            };
            history.push(json!({"role": role, "message": m.content}));
        }
        if let Some(last) = history.pop() {
            message = last["message"].as_str().unwrap_or_default().to_owned();
        }
        let mut body = json!({"message": message, "chat_history": history});
        if let Some(p) = self.base.chat_params()
            && let Some(mt) = p.max_tokens
        {
            body["max_tokens"] = json!(mt);
        }
        let uri = "/model/cohere.command-r/invoke".to_owned();
        // host + scheme from the account endpoint at go-live (else mock sentinel);
        // SigV4 signs this same host so URL and signature agree. Real AWS keys need
        // an Account access_key/secret_key pair (the remaining go-live item).
        let base = self
            .base
            .base_url("mock://bedrock-runtime.us-east-1.amazonaws.com");
        let host = base
            .split_once("://")
            .map(|(_, h)| h)
            .unwrap_or(&base)
            .to_owned();
        let payload = body.to_string().into_bytes();
        let creds = self.base.aws_credentials();
        let headers = aws_headers(
            &host,
            &uri,
            &payload,
            creds.as_ref().map(|(a, s)| (a.as_str(), s.as_str())),
        );
        let url = format!("{base}{uri}");
        let (status, v) = self.base.post_json(&url, headers, body).await?;
        let tokens = &v["meta"]["tokens"];
        let (input, output) = (
            tokens["input_tokens"].as_i64().unwrap_or(0),
            tokens["output_tokens"].as_i64().unwrap_or(0),
        );
        let resp = GatewayResponse {
            message: v["text"].as_str().unwrap_or_default().to_owned(),
            model,
            finish_reason: v["finish_reason"].as_str().unwrap_or("stop").to_lowercase(),
            prompt_tokens: input,
            completion_tokens: output,
            total_tokens: input + output,
            raw_usage_json: json!({"input_tokens": input, "output_tokens": output})
                .to_string()
                .into_bytes(),
            is_messages_protocol: true, // anthropic's usage fields align with cohere's input/output
            http_code: status as i64,
            ..Default::default()
        };
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            ..Default::default()
        })
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

// ------------------------------------------------- AWS Bedrock: Llama

bespoke_engine!(LlamaEngine);

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
        let mut body = json!({"prompt": prompt});
        if let Some(p) = self.base.chat_params() {
            if let Some(mt) = p.max_tokens {
                body["max_gen_len"] = json!(mt);
            }
            if let Some(t) = p.temperature {
                body["temperature"] = json!(t);
            }
        }
        let uri = "/model/meta.llama3-70b-instruct-v1/invoke".to_owned();
        // host/scheme from the account endpoint at go-live; SigV4 signs the same
        // host. Real AWS keys need an Account key-pair (the remaining go-live item).
        let base = self
            .base
            .base_url("mock://bedrock-runtime.us-east-1.amazonaws.com");
        let host = base
            .split_once("://")
            .map(|(_, h)| h)
            .unwrap_or(&base)
            .to_owned();
        let payload = body.to_string().into_bytes();
        let creds = self.base.aws_credentials();
        let headers = aws_headers(
            &host,
            &uri,
            &payload,
            creds.as_ref().map(|(a, s)| (a.as_str(), s.as_str())),
        );
        let url = format!("{base}{uri}");
        let (status, v) = self.base.post_json(&url, headers, body).await?;
        let (pt, ct) = (
            v["prompt_token_count"].as_i64().unwrap_or(0),
            v["generation_token_count"].as_i64().unwrap_or(0),
        );
        let resp = GatewayResponse {
            message: v["generation"].as_str().unwrap_or_default().to_owned(),
            model,
            finish_reason: v["stop_reason"].as_str().unwrap_or("stop").to_owned(),
            prompt_tokens: pt,
            completion_tokens: ct,
            total_tokens: pt + ct,
            raw_usage_json:
                json!({"prompt_tokens": pt, "completion_tokens": ct, "total_tokens": pt + ct})
                    .to_string()
                    .into_bytes(),
            http_code: status as i64,
            ..Default::default()
        };
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            ..Default::default()
        })
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

// ------------------------------------------------- Ali DashScope (Qwen native)

bespoke_engine!(DashScopeEngine);

#[async_trait::async_trait]
impl ModelEngine for DashScopeEngine {
    /// Ali DashScope native wire (not the openai-compatible mode):
    /// {model, input:{messages}, parameters:{result_format:"message",…}};
    /// response {output:{choices:[{message,finish_reason}]}, usage{input/output/total_tokens}}.
    async fn run(&self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        let messages: Vec<Value> = self
            .base
            .request
            .message
            .iter()
            .map(|m| {
                json!({"role": if m.role == ap_consts::role::AI {"assistant"}
                                 else if m.role == ap_consts::role::SYSTEM {"system"}
                                 else {"user"},
                       "content": m.content})
            })
            .collect();
        let mut parameters = json!({"result_format": "message"});
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
        let body = json!({"model": model, "input": {"messages": messages},
                          "parameters": parameters});
        let ds_url = format!(
            "{}/api/v1/services/aigc/text-generation/generation",
            self.base.base_url("mock://dashscope.aliyuncs.com")
        );
        let (status, v) = self
            .base
            .post_json(
                &ds_url,
                vec![(
                    "authorization".into(),
                    format!("Bearer {}", self.base.api_key()),
                )],
                body,
            )
            .await?;
        let choice = &v["output"]["choices"][0];
        let usage = &v["usage"];
        let (input, output) = (
            usage["input_tokens"].as_i64().unwrap_or(0),
            usage["output_tokens"].as_i64().unwrap_or(0),
        );
        // usage dialect normalized to the openai shape at the engine boundary (same as Vertex)
        let raw_usage = json!({"prompt_tokens": input, "completion_tokens": output,
                                "total_tokens": usage["total_tokens"].as_i64().unwrap_or(input + output)});
        let resp = GatewayResponse {
            message: choice["message"]["content"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            model,
            finish_reason: choice["finish_reason"]
                .as_str()
                .unwrap_or("stop")
                .to_owned(),
            prompt_tokens: input,
            completion_tokens: output,
            total_tokens: input + output,
            raw_usage_json: raw_usage.to_string().into_bytes(),
            http_code: status as i64,
            ..Default::default()
        };
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            ..Default::default()
        })
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::MockTransport;
    use ap_consts::Protocol;
    use ap_models::{ChatMsg, ModelParamV2};
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
