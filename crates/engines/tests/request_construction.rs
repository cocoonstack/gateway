//! Request-construction alignment: verify engines build vendor-correct request
//! bodies (the other half of the round-trip; response parsing is covered by
//! golden_fixtures.rs). Fully offline.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use gw_consts::Protocol;
use gw_engines::transport::{Transport, UpstreamBody, UpstreamRequest, UpstreamResponse};
use gw_engines::{
    AudioEngine, AudioKind, ClaudeEngine, CohereEngine, CompletionsEngine, DashScopeEngine,
    EmbeddingsEngine, ErnieEngine, ImageEngine, LlamaEngine, MinimaxV1Engine, ModelEngine,
    OpenAiEngine, ResponsesEngine, SearchEngine, VertexEngine, VideoEngine,
};
use gw_models::{
    ChatMsg, ChatParams, EmbeddingParams, GResult, GatewayRequest, ImageParams, ModelParamV2,
    SearchParams, SttParams, TtsParams, TypedParams, VideoParams,
};
use serde_json::Value;

#[derive(Debug, Default)]
struct RecordingTransport {
    seen: Mutex<Option<UpstreamRequest>>,
    reply: Vec<u8>,
}

impl RecordingTransport {
    fn new(reply: &str) -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(None),
            reply: reply.as_bytes().to_vec(),
        })
    }
    fn body_json(&self) -> Value {
        let g = self.seen.lock().unwrap();
        let req = g.as_ref().expect("engine sent a request");
        serde_json::from_slice(&req.body).expect("request body is json")
    }
    fn url(&self) -> String {
        self.seen.lock().unwrap().as_ref().unwrap().url.clone()
    }
    fn header(&self, name: &str) -> Option<String> {
        let g = self.seen.lock().unwrap();
        g.as_ref()
            .unwrap()
            .headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    }
}

#[async_trait]
impl Transport for RecordingTransport {
    async fn send(&self, req: UpstreamRequest) -> GResult<UpstreamResponse> {
        *self.seen.lock().unwrap() = Some(req);
        Ok(UpstreamResponse {
            status: 200,
            body: UpstreamBody::Json(bytes::Bytes::from(self.reply.clone())),
        })
    }
}

fn chat_req(mt: Protocol, name: &str) -> GatewayRequest {
    GatewayRequest {
        message: vec![
            ChatMsg::text("system", "be brief"),
            ChatMsg::text("user", "hello"),
        ],
        model_param_v2: Some(ModelParamV2::with_name(mt, name)),
        ..Default::default()
    }
}

#[tokio::test]
async fn openai_request_shape() {
    let t = RecordingTransport::new(
        r#"{"model":"gpt","choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#,
    );
    let mut req = chat_req(Protocol::OpenaiChat, "gpt-4o");
    if let Some(p) = req.model_param_v2.as_mut() {
        p.typed = Some(TypedParams::Chat(ChatParams {
            temperature: Some(0.5),
            max_tokens: Some(256),
            ..Default::default()
        }));
    }
    let _ = OpenAiEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(b["model"], "gpt-4o");
    assert_eq!(b["messages"][0]["role"], "system");
    assert_eq!(b["messages"][1]["role"], "user");
    assert_eq!(b["messages"][1]["content"], "hello");
    assert_eq!(b["stream"], false);
    assert_eq!(b["temperature"], 0.5);
    assert_eq!(b["max_tokens"], 256);
    assert!(
        t.url().ends_with("/v1/chat/completions"),
        "url: {}",
        t.url()
    );
    assert_eq!(
        t.header("content-type").as_deref(),
        Some("application/json")
    );
    assert!(t.header("authorization").unwrap().starts_with("Bearer "));
}

#[tokio::test]
async fn openai_streaming_requests_usage() {
    let t = RecordingTransport::new(
        r#"{"model":"gpt","choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#,
    );
    let mut req = chat_req(Protocol::OpenaiChat, "gpt-4o");
    req.stream = true;
    let _ = OpenAiEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(b["stream"], true);
    assert_eq!(b["stream_options"]["include_usage"], true);
}

#[tokio::test]
async fn anthropic_request_shape() {
    let t = RecordingTransport::new(
        r#"{"model":"claude-test","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#,
    );
    let mut req = chat_req(Protocol::AnthropicMessages, "claude-sonnet");
    if let Some(p) = req.model_param_v2.as_mut() {
        p.typed = Some(TypedParams::Chat(ChatParams {
            max_tokens: Some(512),
            stop: Some(serde_json::json!(["STOP", "END"])),
            ..Default::default()
        }));
    }
    let _ = ClaudeEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(b["model"], "claude-sonnet");
    assert_eq!(b["stop_sequences"][0], "STOP");
    assert_eq!(b["stop_sequences"][1], "END");
    assert!(b.get("stop").is_none());
    assert_eq!(b["system"], "be brief");
    assert_eq!(b["messages"].as_array().unwrap().len(), 1);
    assert_eq!(b["messages"][0]["role"], "user");
    assert_eq!(b["max_tokens"], 512);
    assert!(t.url().ends_with("/v1/messages"));
    assert!(t.header("x-api-key").is_some());
    assert_eq!(
        t.header("content-type").as_deref(),
        Some("application/json")
    );
    assert_eq!(t.header("anthropic-version").as_deref(), Some("2023-06-01"));
}

#[tokio::test]
async fn anthropic_multimodal_content_preserved() {
    let t = RecordingTransport::new(
        r#"{"model":"claude-test","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#,
    );
    let mut req = GatewayRequest {
        model_param_v2: Some(ModelParamV2::with_name(
            Protocol::AnthropicMessages,
            "claude-sonnet",
        )),
        ..Default::default()
    };
    let mut msg = ChatMsg::text("user", "what is in this image?");
    msg.parts = Some(serde_json::json!([
        {"type":"text","text":"what is in this image?"},
        {"type":"image","source":{"type":"base64","media_type":"image/png","data":"iVBORw0KGgo="}}
    ]));
    req.message = vec![msg];
    let _ = ClaudeEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    let content = &b["messages"][0]["content"];
    assert!(content.is_array(), "content should be blocks: {content}");
    assert_eq!(content[0]["type"], "text");
    assert_eq!(content[1]["type"], "image");
    assert_eq!(content[1]["source"]["media_type"], "image/png");
}

#[tokio::test]
async fn anthropic_signed_thinking_continuation_is_preserved() {
    let transport = RecordingTransport::new(
        r#"{"model":"claude-test","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#,
    );
    let mut assistant = ChatMsg::text("assistant", String::new());
    assistant.parts = Some(serde_json::json!([
        {"type":"thinking","thinking":"summary","signature":"opaque-signature"},
        {"type":"redacted_thinking","data":"opaque-redacted-data"},
        {"type":"tool_use","id":"toolu_1","name":"probe","input":{}}
    ]));
    let mut result = ChatMsg::text("user", String::new());
    result.parts = Some(serde_json::json!([
        {"type":"tool_result","tool_use_id":"toolu_1","content":"ready"}
    ]));
    let mut param = ModelParamV2::with_name(Protocol::AnthropicMessages, "claude-sonnet");
    param.typed = Some(TypedParams::Chat(ChatParams {
        max_tokens: Some(2048),
        ..Default::default()
    }));
    param.raw = serde_json::json!({
        "thinking":{"type":"enabled","budget_tokens":1024}
    });
    let request = GatewayRequest {
        message: vec![assistant, result],
        model_param_v2: Some(param),
        ..Default::default()
    };

    let _ = ClaudeEngine::new(request, transport.clone())
        .run()
        .await
        .unwrap();
    let body = transport.body_json();

    assert_eq!(
        body["messages"][0]["content"][0]["signature"],
        "opaque-signature"
    );
    assert_eq!(
        body["messages"][0]["content"][1]["data"],
        "opaque-redacted-data"
    );
    assert_eq!(body["messages"][1]["content"][0]["tool_use_id"], "toolu_1");
    assert_eq!(body["thinking"]["budget_tokens"], 1024);
}

#[tokio::test]
async fn vertex_request_shape() {
    let t = RecordingTransport::new(
        r#"{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}"#,
    );
    let mut req = chat_req(Protocol::Gemini, "gemini-pro");
    if let Some(p) = req.model_param_v2.as_mut() {
        p.typed = Some(TypedParams::Chat(ChatParams {
            temperature: Some(0.4),
            max_tokens: Some(128),
            ..Default::default()
        }));
    }
    let _ = VertexEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    let contents = b["contents"].as_array().unwrap();
    assert_eq!(contents.last().unwrap()["parts"][0]["text"], "hello");
    assert!(
        t.url()
            .contains("/v1beta/models/gemini-pro:generateContent"),
        "url: {}",
        t.url()
    );
    assert!(t.header("x-goog-api-key").is_some());
    assert!(t.header("authorization").is_none());
    assert_eq!(b["generationConfig"]["temperature"], 0.4);
    assert_eq!(b["generationConfig"]["maxOutputTokens"], 128);
}

#[tokio::test]
async fn go_live_seam_routes_to_configured_endpoint() {
    use gw_models::Account;

    let t = RecordingTransport::new(
        r#"{"model":"claude-test","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#,
    );
    let mut req = chat_req(Protocol::AnthropicMessages, "claude-sonnet");
    req.account = Some(std::sync::Arc::new(Account {
        name: "live-anthropic".into(),
        endpoint: "https://api.anthropic.com".into(),
        ..Default::default()
    }));
    let _ = ClaudeEngine::new(req, t.clone()).run().await.unwrap();
    assert_eq!(t.url(), "https://api.anthropic.com/v1/messages");
    assert!(
        !t.url().starts_with("mock://"),
        "must not be the mock sentinel"
    );
    assert!(t.header("x-api-key").is_some());

    let t2 = RecordingTransport::new(
        r#"{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}"#,
    );
    let mut req2 = chat_req(Protocol::Gemini, "gemini-pro");
    req2.account = Some(std::sync::Arc::new(Account {
        name: "live-gemini".into(),
        endpoint: "https://generativelanguage.googleapis.com".into(),
        ..Default::default()
    }));
    let _ = VertexEngine::new(req2, t2.clone()).run().await.unwrap();
    assert!(
        t2.url()
            .starts_with("https://generativelanguage.googleapis.com/v1beta/models/"),
        "url: {}",
        t2.url()
    );
    assert!(t2.url().ends_with(":generateContent"));
    assert!(t2.header("x-goog-api-key").is_some());
}

#[tokio::test]
async fn go_live_seam_aws_sigv4_uses_real_credentials() {
    use gw_models::Account;
    // SAFETY: unique test-local var names; no concurrent reader.
    unsafe {
        std::env::set_var("GW_TEST_AWS_AK", "AKIAREALEXAMPLE123");
        std::env::set_var("GW_TEST_AWS_SK", "realsecretkeyvalue");
    }

    let t = RecordingTransport::new(
        r#"{"text":"ok","meta":{"tokens":{"input_tokens":1,"output_tokens":1}}}"#,
    );
    let mut req = chat_req(Protocol::AwsCohere, "cohere.command-r");
    req.account = Some(std::sync::Arc::new(Account {
        name: "live-bedrock".into(),
        endpoint: "https://bedrock-runtime.eu-west-1.amazonaws.com".into(),
        api_key_env: "GW_TEST_AWS_AK".into(),
        secret_key_env: "GW_TEST_AWS_SK".into(),
        ..Default::default()
    }));
    let _ = CohereEngine::new(req, t.clone()).run().await.unwrap();
    assert!(
        t.url()
            .starts_with("https://bedrock-runtime.eu-west-1.amazonaws.com/model/"),
        "url: {}",
        t.url()
    );
    let auth = t
        .header("authorization")
        .expect("sigv4 authorization header");
    assert!(
        auth.contains("Credential=AKIAREALEXAMPLE123/"),
        "SigV4 must sign with the real access key, got: {auth}"
    );
    assert!(
        !auth.contains("AKIDMOCK"),
        "must not use the mock access key"
    );
    // SAFETY: unique test-local var names; no concurrent reader.
    unsafe {
        std::env::remove_var("GW_TEST_AWS_AK");
        std::env::remove_var("GW_TEST_AWS_SK");
    }
}

#[tokio::test]
async fn go_live_seam_bespoke_dashscope() {
    use gw_models::Account;
    let t = RecordingTransport::new(
        r#"{"output":{"text":"ok","finish_reason":"stop"},"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}"#,
    );
    let mut req = chat_req(Protocol::Dashscope, "qwen-max");
    req.account = Some(std::sync::Arc::new(Account {
        name: "live-dashscope".into(),
        endpoint: "https://dashscope.aliyuncs.com".into(),
        ..Default::default()
    }));
    let _ = DashScopeEngine::new(req, t.clone()).run().await.unwrap();
    assert!(
        t.url()
            .starts_with("https://dashscope.aliyuncs.com/api/v1/services/"),
        "url: {}",
        t.url()
    );
    assert!(!t.url().starts_with("mock://"));
    assert!(t.header("authorization").unwrap().starts_with("Bearer "));
}

#[tokio::test]
async fn legacy_completions_sends_prompt_not_messages() {
    let t = RecordingTransport::new(
        r#"{"id":"cmpl-1","object":"text_completion","model":"instruct","choices":[{"text":"ok","index":0,"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}"#,
    );
    let mut req = chat_req(Protocol::Completions, "gpt-3.5-turbo-instruct");
    req.message = vec![ChatMsg::text("user", "once upon a time")];
    if let Some(p) = req.model_param_v2.as_mut() {
        p.typed = Some(TypedParams::Chat(ChatParams {
            max_tokens: Some(64),
            temperature: Some(0.7),
            ..Default::default()
        }));
    }
    let _ = CompletionsEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(b["prompt"], "once upon a time");
    assert_eq!(b["max_tokens"], 64);
    assert_eq!(b["temperature"], 0.7);
    assert!(b.get("messages").is_none(), "must not be chat-shaped");
    assert!(
        t.url().ends_with("/v1/completions"),
        "must hit /v1/completions, got {}",
        t.url()
    );
    assert!(!t.url().contains("chat"), "must not be the chat endpoint");
}

#[tokio::test]
async fn responses_api_forwards_native_body() {
    let t = RecordingTransport::new(
        r#"{"id":"resp_1","object":"response","model":"gpt-5","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}"#,
    );
    let mut req = GatewayRequest {
        model_param_v2: Some(ModelParamV2::with_name(Protocol::Responses, "gpt-5")),
        ..Default::default()
    };
    req.model_param_v2.as_mut().unwrap().raw = serde_json::json!({
        "input": [{"role":"user","content":"hi"}],
        "instructions": "be brief",
        "max_output_tokens": 256
    });
    let _ = ResponsesEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(b["instructions"], "be brief");
    assert_eq!(b["max_output_tokens"], 256);
    assert_eq!(b["input"][0]["role"], "user");
    assert_eq!(b["model"], "gpt-5");
    assert!(t.url().contains("/responses"), "url: {}", t.url());
    assert!(b.get("messages").is_none(), "must not be chat-shaped");
}

#[tokio::test]
async fn vertex_multimodal_image_becomes_inline_data() {
    let t = RecordingTransport::new(
        r#"{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}"#,
    );
    let mut req = GatewayRequest {
        model_param_v2: Some(ModelParamV2::with_name(Protocol::Gemini, "gemini-pro")),
        ..Default::default()
    };
    let mut msg = ChatMsg::text("user", "what is this?");
    msg.parts = Some(serde_json::json!([
        {"type":"text","text":"what is this?"},
        {"type":"image_url","image_url":{"url":"data:image/png;base64,iVBORw0KGgo="}}
    ]));
    req.message = vec![msg];
    let _ = VertexEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    let parts = b["contents"][0]["parts"].as_array().unwrap();
    assert_eq!(parts[0]["text"], "what is this?");
    assert_eq!(parts[1]["inlineData"]["mimeType"], "image/png");
    assert_eq!(parts[1]["inlineData"]["data"], "iVBORw0KGgo=");
    assert!(!b.to_string().contains("image_url"), "openai shape leaked");
}

#[tokio::test]
async fn ernie_request_shape() {
    let t = RecordingTransport::new(
        r#"{"result":"ok","usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#,
    );
    let _ = ErnieEngine::new(chat_req(Protocol::Ernie, "ernie-4.0"), t.clone())
        .run()
        .await
        .unwrap();
    let b = t.body_json();
    assert_eq!(b["messages"][0]["role"], "user");
    assert_eq!(b["messages"][0]["content"], "hello");
    assert!(t.url().contains("wenxinworkshop"), "url: {}", t.url());
    assert_eq!(
        t.header("content-type").as_deref(),
        Some("application/json")
    );
}

#[tokio::test]
async fn system_prompt_reaches_every_bespoke_wire() {
    let ernie = RecordingTransport::new(r#"{"result":"ok","usage":{}}"#);
    ErnieEngine::new(chat_req(Protocol::Ernie, "ernie-4.0"), ernie.clone())
        .run()
        .await
        .unwrap();
    assert_eq!(ernie.body_json()["system"], "be brief", "ernie system slot");

    let minimax = RecordingTransport::new(
        r#"{"reply":"ok","usage":{"total_tokens":1},"base_resp":{"status_code":0}}"#,
    );
    MinimaxV1Engine::new(chat_req(Protocol::MinimaxV1, "abab6.5"), minimax.clone())
        .run()
        .await
        .unwrap();
    let mb = minimax.body_json();
    assert_eq!(mb["prompt"], "be brief", "minimax carries system as prompt");
    assert!(
        mb["messages"]
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["sender_type"] != "USER" || m["text"] != "be brief"),
        "system must not be downgraded to a USER turn: {mb}"
    );

    let cohere = RecordingTransport::new(
        r#"{"text":"ok","finish_reason":"COMPLETE","meta":{"tokens":{"input_tokens":1,"output_tokens":1}}}"#,
    );
    CohereEngine::new(chat_req(Protocol::AwsCohere, "command-r"), cohere.clone())
        .run()
        .await
        .unwrap();
    let cb = cohere.body_json();
    assert_eq!(cb["preamble"], "be brief", "cohere system slot");
    assert!(
        cb["chat_history"]
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["message"] != "be brief"),
        "system must not leak into chat_history: {cb}"
    );
}

#[tokio::test]
async fn vertex_system_goes_to_system_instruction() {
    let t = RecordingTransport::new(
        r#"{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{}}"#,
    );
    VertexEngine::new(chat_req(Protocol::Gemini, "gemini-pro"), t.clone())
        .run()
        .await
        .unwrap();
    let b = t.body_json();
    assert_eq!(b["systemInstruction"]["parts"][0]["text"], "be brief");
    assert_eq!(
        b["contents"].as_array().unwrap().len(),
        1,
        "only the user turn is in contents: {b}"
    );
    assert_eq!(b["contents"][0]["role"], "user");
    assert_eq!(b["contents"][0]["parts"][0]["text"], "hello");
}

#[tokio::test]
async fn bespoke_forwards_raw_passthrough_params() {
    let t = RecordingTransport::new(
        r#"{"result":"ok","usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#,
    );
    let mut req = chat_req(Protocol::Ernie, "ernie-4.0");
    if let Some(p) = req.model_param_v2.as_mut() {
        p.typed = Some(TypedParams::Chat(ChatParams {
            temperature: Some(0.3),
            ..Default::default()
        }));
        p.raw = serde_json::json!({"penalty_score": 1.5, "top_p": 0.8, "temperature": 0.99});
    }
    let _ = ErnieEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(b["penalty_score"], 1.5, "raw param must reach vendor");
    assert_eq!(b["top_p"], 0.8, "raw param must reach vendor");
    assert_eq!(
        b["temperature"], 0.3,
        "typed field stays authoritative over raw"
    );
}

#[tokio::test]
async fn minimax_v1_request_shape() {
    let t = RecordingTransport::new(
        r#"{"reply":"ok","usage":{"total_tokens":2},"base_resp":{"status_code":0,"status_msg":""}}"#,
    );
    let _ = MinimaxV1Engine::new(chat_req(Protocol::MinimaxV1, "abab6.5"), t.clone())
        .run()
        .await
        .unwrap();
    let b = t.body_json();
    assert_eq!(b["model"], "abab6.5");
    assert_eq!(b["messages"][0]["sender_type"], "USER");
    assert_eq!(b["messages"][0]["text"], "hello");
    assert!(t.url().contains("minimax"), "url: {}", t.url());
}

#[tokio::test]
async fn cohere_request_shape_with_sigv4() {
    let t = RecordingTransport::new(
        r#"{"text":"ok","finish_reason":"COMPLETE","meta":{"tokens":{"input_tokens":1,"output_tokens":1}}}"#,
    );
    let _ = CohereEngine::new(chat_req(Protocol::AwsCohere, "command-r"), t.clone())
        .run()
        .await
        .unwrap();
    let b = t.body_json();
    assert_eq!(b["message"], "hello");
    assert!(b["chat_history"].is_array());
    let auth = t.header("authorization").expect("SigV4 auth header");
    assert!(
        auth.starts_with("AWS4-HMAC-SHA256 Credential="),
        "auth: {auth}"
    );
    assert_eq!(t.header("accept").as_deref(), Some("application/json"));
    assert!(auth.contains("SignedHeaders=") && auth.contains("Signature="));
}

#[tokio::test]
async fn llama_request_shape_with_sigv4() {
    let t = RecordingTransport::new(
        r#"{"generation":"ok","prompt_token_count":1,"generation_token_count":1,"stop_reason":"stop"}"#,
    );
    let _ = LlamaEngine::new(chat_req(Protocol::AwsLlama, "llama3-70b"), t.clone())
        .run()
        .await
        .unwrap();
    let b = t.body_json();
    assert!(
        b["prompt"].as_str().unwrap().contains("hello"),
        "prompt: {}",
        b["prompt"]
    );
    assert!(
        t.header("authorization")
            .unwrap()
            .starts_with("AWS4-HMAC-SHA256")
    );
}

#[tokio::test]
async fn dashscope_request_shape() {
    let t = RecordingTransport::new(
        r#"{"output":{"choices":[{"finish_reason":"stop","message":{"content":"ok"}}]},"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}"#,
    );
    let _ = DashScopeEngine::new(chat_req(Protocol::Dashscope, "qwen-max"), t.clone())
        .run()
        .await
        .unwrap();
    let b = t.body_json();
    assert_eq!(b["model"], "qwen-max");
    assert_eq!(b["input"]["messages"][0]["role"], "system");
    assert_eq!(b["input"]["messages"][1]["content"], "hello");
    assert_eq!(b["parameters"]["result_format"], "message");
    assert!(t.url().contains("dashscope"), "url: {}", t.url());
}

fn typed_req(mt: Protocol, name: &str, typed: TypedParams) -> GatewayRequest {
    let mut p = ModelParamV2::with_name(mt, name);
    p.typed = Some(typed);
    GatewayRequest {
        model_param_v2: Some(p),
        ..Default::default()
    }
}

#[tokio::test]
async fn embeddings_request_shape() {
    let t = RecordingTransport::new(
        r#"{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1]}],"usage":{"prompt_tokens":1,"total_tokens":1}}"#,
    );
    let req = typed_req(
        Protocol::Embeddings,
        "text-embedding-3",
        TypedParams::Embeddings(EmbeddingParams {
            input: vec!["a".into(), "b".into()],
            dimensions: Some(256),
        }),
    );
    let _ = EmbeddingsEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(b["model"], "text-embedding-3");
    assert_eq!(b["input"][0], "a");
    assert_eq!(b["input"][1], "b");
    assert_eq!(b["dimensions"], 256);
    assert!(t.url().contains("/embeddings"), "url: {}", t.url());
    assert_eq!(
        t.header("content-type").as_deref(),
        Some("application/json")
    );
}

#[tokio::test]
async fn image_request_shape() {
    let t = RecordingTransport::new(r#"{"created":1,"data":[{"b64_json":"x"}]}"#);
    let req = typed_req(
        Protocol::Image,
        "dall-e-3",
        TypedParams::Image(ImageParams {
            prompt: "a cat".into(),
            n: 2,
            size: Some("1024x1024".into()),
            ..Default::default()
        }),
    );
    let _ = ImageEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(b["model"], "dall-e-3");
    assert_eq!(b["prompt"], "a cat");
    assert_eq!(b["n"], 2);
    assert!(t.url().ends_with("/images/generations"), "url: {}", t.url());
    assert!(b.get("image").is_none());
}

#[tokio::test]
async fn image_edit_routes_to_edits_endpoint() {
    let t = RecordingTransport::new(r#"{"created":1,"data":[{"b64_json":"AAAA"}]}"#);
    let req = typed_req(
        Protocol::Image,
        "dall-e-2",
        TypedParams::Image(ImageParams {
            prompt: "add a hat".into(),
            n: 1,
            size: None,
            image: Some("c3JjaW1n".into()),
            mask: Some("bWFzaw==".into()),
        }),
    );
    let _ = ImageEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(b["prompt"], "add a hat");
    assert_eq!(b["image"], "c3JjaW1n");
    assert_eq!(b["mask"], "bWFzaw==");
    assert!(t.url().ends_with("/images/edits"), "url: {}", t.url());
}

#[tokio::test]
async fn tts_request_shape() {
    let t = RecordingTransport::new(r#"{"audio_b64":"x","characters":3}"#);
    let req = typed_req(
        Protocol::Tts,
        "tts-1",
        TypedParams::AudioTts(TtsParams {
            input: "read this".into(),
            voice: Some("alloy".into()),
            response_format: Some("mp3".into()),
        }),
    );
    let _ = AudioEngine::new(req, t.clone(), AudioKind::Tts)
        .run()
        .await
        .unwrap();
    let b = t.body_json();
    assert_eq!(b["model"], "tts-1");
    assert_eq!(b["input"], "read this");
    assert_eq!(b["voice"], "alloy");
    assert_eq!(b["response_format"], "mp3");
    assert!(t.url().ends_with("/audio/speech"), "url: {}", t.url());
}

#[tokio::test]
async fn stt_request_shape() {
    let t = RecordingTransport::new(r#"{"text":"transcribed"}"#);
    let req = typed_req(
        Protocol::Stt,
        "whisper-1",
        TypedParams::AudioStt(SttParams {
            audio_b64: "TU9DSw==".into(),
            language: Some("en".into()),
            translate: false,
        }),
    );
    let _ = AudioEngine::new(req, t.clone(), AudioKind::Stt)
        .run()
        .await
        .unwrap();
    let b = t.body_json();
    assert_eq!(b["model"], "whisper-1");
    assert_eq!(b["audio_b64"], "TU9DSw==");
    assert!(
        t.url().ends_with("/audio/transcriptions"),
        "url: {}",
        t.url()
    );
}

#[tokio::test]
async fn video_request_shape() {
    let t = RecordingTransport::new(
        r#"{"task_id":"v","status":"succeeded","video_url":"mock://v.mp4"}"#,
    );
    let req = typed_req(
        Protocol::Video,
        "kling-video",
        TypedParams::Video(VideoParams {
            prompt: "a dog surfing".into(),
            duration_seconds: Some(5),
            resolution: Some("1080p".into()),
        }),
    );
    let _ = VideoEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(b["model"], "kling-video");
    assert_eq!(b["prompt"], "a dog surfing");
    assert_eq!(b["duration_seconds"], 5);
    assert_eq!(b["resolution"], "1080p");
    assert!(t.url().contains("/videos"), "url: {}", t.url());
}

#[tokio::test]
async fn search_request_shape() {
    let t = RecordingTransport::new(
        r#"{"query":"q","results":[{"title":"t","url":"u","snippet":"s"}]}"#,
    );
    let req = typed_req(
        Protocol::Search,
        "brave-search",
        TypedParams::Search(SearchParams {
            query: "rust dag".into(),
            count: 5,
        }),
    );
    let _ = SearchEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(b["query"], "rust dag");
    assert_eq!(b["count"], 5);
    assert!(t.url().contains("/search"), "url: {}", t.url());
}

#[tokio::test]
async fn anthropic_chat_history_tool_round_trip() {
    let t = RecordingTransport::new(
        r#"{"model":"claude-test","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#,
    );
    let mut call = ChatMsg::text("assistant", "I'll list them.");
    call.tool_calls = Some(serde_json::json!([
        {"id":"toolu_1","type":"function","function":{"name":"shell","arguments":"{\"command\":\"ls\"}"}},
        {"id":"toolu_2","type":"function","function":{"name":"shell","arguments":"{\"command\":\"mkdir -p /tmp/x\"}"}}
    ]));
    let mut result1 = ChatMsg::text("tool", "a.txt");
    result1.tool_call_id = Some("toolu_1".into());
    let mut result2 = ChatMsg::text("tool", "");
    result2.tool_call_id = Some("toolu_2".into());
    let req = GatewayRequest {
        message: vec![
            ChatMsg::text("user", "list files"),
            call,
            result1,
            result2,
            ChatMsg::text("user", "Time is almost up."),
        ],
        model_param_v2: Some(ModelParamV2::with_name(
            Protocol::AnthropicMessages,
            "claude-sonnet",
        )),
        ..Default::default()
    };
    let _ = ClaudeEngine::new(req, t.clone()).run().await.unwrap();
    let msgs = t.body_json()["messages"].clone();
    assert_eq!(msgs.as_array().unwrap().len(), 3, "turns: {msgs}");
    assert_eq!(msgs[0]["role"], "user");
    assert_eq!(msgs[0]["content"], "list files");

    let assistant = &msgs[1];
    assert_eq!(assistant["role"], "assistant");
    assert_eq!(assistant["content"][0]["type"], "text");
    assert_eq!(assistant["content"][0]["text"], "I'll list them.");
    assert_eq!(assistant["content"][1]["type"], "tool_use");
    assert_eq!(assistant["content"][1]["id"], "toolu_1");
    assert_eq!(assistant["content"][1]["name"], "shell");
    assert_eq!(assistant["content"][1]["input"]["command"], "ls");
    assert_eq!(assistant["content"][2]["id"], "toolu_2");

    let results = &msgs[2];
    assert_eq!(results["role"], "user");
    let blocks = results["content"].as_array().unwrap();
    assert_eq!(blocks.len(), 3, "results turn: {results}");
    assert_eq!(blocks[0]["type"], "tool_result");
    assert_eq!(blocks[0]["tool_use_id"], "toolu_1");
    assert_eq!(blocks[0]["content"], "a.txt");
    assert_eq!(blocks[1]["tool_use_id"], "toolu_2");
    assert!(
        blocks[1].get("content").is_none(),
        "empty tool output must omit content: {}",
        blocks[1]
    );
    assert_eq!(blocks[2]["type"], "text");
    assert_eq!(blocks[2]["text"], "Time is almost up.");
}

#[tokio::test]
async fn anthropic_chat_history_drops_empty_turns_and_alternates() {
    let t = RecordingTransport::new(
        r#"{"model":"claude-test","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#,
    );
    let mut silent_call = ChatMsg::text("assistant", "");
    silent_call.tool_calls = Some(serde_json::json!([
        {"id":"toolu_9","type":"function","function":{"name":"shell","arguments":"{}"}}
    ]));
    let mut result = ChatMsg::text("tool", "done");
    result.tool_call_id = Some("toolu_9".into());
    let req = GatewayRequest {
        message: vec![
            ChatMsg::text("user", "first"),
            ChatMsg::text("assistant", ""),
            ChatMsg::text("user", "again"),
            silent_call,
            result,
        ],
        model_param_v2: Some(ModelParamV2::with_name(
            Protocol::AnthropicMessages,
            "claude-sonnet",
        )),
        ..Default::default()
    };
    let _ = ClaudeEngine::new(req, t.clone()).run().await.unwrap();
    let msgs = t.body_json()["messages"].clone();
    let roles: Vec<&str> = msgs
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["role"].as_str().unwrap())
        .collect();
    assert_eq!(roles, ["user", "assistant", "user"], "turns: {msgs}");
    assert_eq!(msgs[0]["content"][0]["text"], "first");
    assert_eq!(msgs[0]["content"][1]["text"], "again");
    assert_eq!(msgs[1]["content"][0]["type"], "tool_use");
    assert_eq!(msgs[1]["content"].as_array().unwrap().len(), 1);
    assert_eq!(msgs[2]["content"][0]["tool_use_id"], "toolu_9");
}

#[tokio::test]
async fn anthropic_prompt_cache_marks_system_and_latest_user_turn() {
    let reply = r#"{"model":"claude-test","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#;
    let t = RecordingTransport::new(reply);
    let mut req = chat_req(Protocol::AnthropicMessages, "claude-sonnet");
    req.message.push(ChatMsg::text("assistant", "hi"));
    req.message.push(ChatMsg::text("user", "and now?"));
    req.prompt_cache = true;
    let _ = ClaudeEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(b["system"][0]["text"], "be brief");
    assert_eq!(b["system"][0]["cache_control"]["type"], "ephemeral");
    let msgs = b["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 3);
    assert_eq!(
        msgs[0]["content"], "hello",
        "earlier turns stay unmarked: {msgs:?}"
    );
    let last = &msgs[2]["content"];
    assert_eq!(last[0]["text"], "and now?");
    assert_eq!(last[0]["cache_control"]["type"], "ephemeral");

    let own = RecordingTransport::new(reply);
    let mut req = chat_req(Protocol::AnthropicMessages, "claude-sonnet");
    let mut last = ChatMsg::text("user", "");
    last.parts = Some(serde_json::json!([
        {"type": "text", "text": "long-lived", "cache_control": {"type": "ephemeral", "ttl": "1h"}}
    ]));
    req.message.push(last);
    req.prompt_cache = true;
    let _ = ClaudeEngine::new(req, own.clone()).run().await.unwrap();
    let b = own.body_json();
    assert_eq!(
        b["messages"][0]["content"][1]["cache_control"]["ttl"], "1h",
        "a client-placed breakpoint is kept: {b}"
    );

    let plain = RecordingTransport::new(reply);
    let _ = ClaudeEngine::new(
        chat_req(Protocol::AnthropicMessages, "claude-sonnet"),
        plain.clone(),
    )
    .run()
    .await
    .unwrap();
    let b = plain.body_json();
    assert_eq!(b["system"], "be brief");
    assert_eq!(b["messages"][0]["content"], "hello");
}

const CLAUDE_OK: &str = r#"{"model":"claude-test","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#;
const OPENAI_OK: &str = r#"{"model":"gpt","choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#;

fn reasoning_req(mt: Protocol, name: &str, reasoning: gw_models::ReasoningParam) -> GatewayRequest {
    let mut req = chat_req(mt, name);
    if let Some(p) = req.model_param_v2.as_mut() {
        p.typed = Some(TypedParams::Chat(ChatParams {
            temperature: Some(0.2),
            top_p: Some(0.5),
            reasoning: Some(Box::new(reasoning)),
            ..Default::default()
        }));
        p.raw = serde_json::json!({"top_k": 5});
    }
    req
}

#[tokio::test]
async fn anthropic_effort_maps_by_model_generation_and_owns_the_conflicting_knobs() {
    let effort = |level: &str| gw_models::ReasoningParam {
        effort: Some(level.to_owned()),
        ..Default::default()
    };
    let t = RecordingTransport::new(CLAUDE_OK);
    let req = reasoning_req(
        Protocol::AnthropicMessages,
        "claude-haiku-4-5",
        effort("high"),
    );
    let _ = ClaudeEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(
        b["thinking"],
        serde_json::json!({"type":"enabled","budget_tokens":16384})
    );
    assert!(b.get("output_config").is_none());
    assert_eq!(b["max_tokens"], 16384 + 1024, "answer budget on top");
    for knob in ["temperature", "top_p", "top_k"] {
        assert!(b.get(knob).is_none(), "{knob} conflicts with thinking");
    }

    let t = RecordingTransport::new(CLAUDE_OK);
    let mut req = reasoning_req(
        Protocol::AnthropicMessages,
        "claude-fable-5",
        effort("minimal"),
    );
    if let Some(TypedParams::Chat(p)) = req.model_param_v2.as_mut().and_then(|p| p.typed.as_mut()) {
        p.max_tokens = Some(8192);
    }
    let _ = ClaudeEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(
        b["thinking"],
        serde_json::json!({"type":"adaptive","display":"summarized"})
    );
    assert_eq!(b["output_config"], serde_json::json!({"effort":"low"}));
    assert_eq!(b["max_tokens"], 8192, "client cap above the budget stays");

    let t = RecordingTransport::new(CLAUDE_OK);
    let req = reasoning_req(
        Protocol::AnthropicMessages,
        "claude-fable-5",
        effort("none"),
    );
    let _ = ClaudeEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert!(b.get("thinking").is_none() && b.get("output_config").is_none());
    assert_eq!(b["temperature"], 0.2);
    assert_eq!(b["top_k"], 5);
}

#[tokio::test]
async fn anthropic_budget_and_client_thinking_precedence() {
    let t = RecordingTransport::new(CLAUDE_OK);
    let req = reasoning_req(
        Protocol::AnthropicMessages,
        "claude-sonnet-5",
        gw_models::ReasoningParam {
            budget_tokens: Some(12000),
            ..Default::default()
        },
    );
    let _ = ClaudeEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(b["output_config"], serde_json::json!({"effort":"high"}));
    assert_eq!(b["max_tokens"], 12000 + 1024);

    // the chat surface's own `thinking` passthrough wins over an effort
    let t = RecordingTransport::new(CLAUDE_OK);
    let mut req = reasoning_req(
        Protocol::AnthropicMessages,
        "claude-haiku-4-5",
        gw_models::ReasoningParam {
            effort: Some("high".to_owned()),
            ..Default::default()
        },
    );
    req.model_param_v2.as_mut().unwrap().raw =
        serde_json::json!({"thinking":{"type":"enabled","budget_tokens":2000},"top_k":5});
    let _ = ClaudeEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(b["thinking"]["budget_tokens"], 2000);
    assert!(b.get("output_config").is_none());
    assert_eq!(b["temperature"], 0.2, "client thinking is verbatim");
    assert_eq!(b["top_k"], 5);
    assert_eq!(b["max_tokens"], 1024);

    // the native surface's typed thinking + output_config go through verbatim
    let t = RecordingTransport::new(CLAUDE_OK);
    let req = reasoning_req(
        Protocol::AnthropicMessages,
        "claude-fable-5",
        gw_models::ReasoningParam {
            thinking: Some(serde_json::json!({"type":"adaptive","display":"summarized"})),
            output_config: Some(serde_json::json!({"effort":"max"})),
            ..Default::default()
        },
    );
    let _ = ClaudeEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(b["thinking"]["display"], "summarized");
    assert_eq!(b["output_config"]["effort"], "max");
    assert_eq!(b["temperature"], 0.2);
}

#[tokio::test]
async fn anthropic_replays_signed_reasoning_details_ahead_of_the_turn() {
    let t = RecordingTransport::new(CLAUDE_OK);
    let mut req = chat_req(Protocol::AnthropicMessages, "claude-fable-5");
    let mut assistant = ChatMsg::text("assistant", "calling");
    assistant.reasoning_content = Some("unsigned prose is not replayable".to_owned());
    assistant.reasoning_details = Some(serde_json::json!([
        {"type":"reasoning.text","text":"signed","signature":"sig","format":"anthropic-claude-v1","index":0},
        {"type":"reasoning.encrypted","data":"blob","format":"anthropic-claude-v1","index":1},
        {"type":"reasoning.text","text":"unsigned","format":"anthropic-claude-v1","index":2}
    ]));
    assistant.tool_calls = Some(serde_json::json!([
        {"id":"call_1","type":"function","function":{"name":"now","arguments":"{}"}}
    ]));
    let mut tool = ChatMsg::text("tool", "12:00");
    tool.tool_call_id = Some("call_1".to_owned());
    req.message.extend([assistant, tool]);
    let _ = ClaudeEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    let turn = &b["messages"][1];
    assert_eq!(turn["role"], "assistant");
    assert_eq!(
        turn["content"],
        serde_json::json!([
            {"type":"thinking","thinking":"signed","signature":"sig"},
            {"type":"redacted_thinking","data":"blob"},
            {"type":"text","text":"calling"},
            {"type":"tool_use","id":"call_1","name":"now","input":{}}
        ])
    );
    assert!(turn.get("reasoning_content").is_none());
}

#[tokio::test]
async fn openai_reasoning_effort_and_thinking_dialects() {
    let t = RecordingTransport::new(OPENAI_OK);
    let mut req = reasoning_req(
        Protocol::OpenaiChat,
        "gpt-5",
        gw_models::ReasoningParam {
            effort: Some("high".to_owned()),
            ..Default::default()
        },
    );
    if let Some(TypedParams::Chat(p)) = req.model_param_v2.as_mut().and_then(|p| p.typed.as_mut()) {
        p.max_tokens = Some(700);
    }
    let _ = OpenAiEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert_eq!(b["reasoning_effort"], "high");
    assert_eq!(b["max_completion_tokens"], 700);
    assert!(b.get("max_tokens").is_none());
    assert_eq!(b["temperature"], 0.2, "the OpenAI wire keeps its own knobs");

    // the native surface's thinking budget becomes an effort; output_config wins
    for (reasoning, want) in [
        (
            gw_models::ReasoningParam {
                thinking: Some(serde_json::json!({"type":"enabled","budget_tokens":6000})),
                ..Default::default()
            },
            "medium",
        ),
        (
            gw_models::ReasoningParam {
                thinking: Some(serde_json::json!({"type":"adaptive"})),
                output_config: Some(serde_json::json!({"effort":"xhigh"})),
                ..Default::default()
            },
            "xhigh",
        ),
        (
            gw_models::ReasoningParam {
                budget_tokens: Some(1024),
                ..Default::default()
            },
            "low",
        ),
    ] {
        let t = RecordingTransport::new(OPENAI_OK);
        let req = reasoning_req(Protocol::OpenaiChat, "gpt-5", reasoning);
        let _ = OpenAiEngine::new(req, t.clone()).run().await.unwrap();
        let b = t.body_json();
        assert_eq!(b["reasoning_effort"], want);
        assert!(b.get("thinking").is_none() && b.get("output_config").is_none());
    }

    let t = RecordingTransport::new(OPENAI_OK);
    let req = reasoning_req(
        Protocol::OpenaiChat,
        "gpt-5",
        gw_models::ReasoningParam {
            thinking: Some(serde_json::json!({"type":"disabled"})),
            ..Default::default()
        },
    );
    let _ = OpenAiEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    assert!(b.get("reasoning_effort").is_none());
    assert!(b.get("max_completion_tokens").is_none());
}

#[tokio::test]
async fn openai_history_replays_reasoning_and_folds_native_thinking_blocks() {
    let t = RecordingTransport::new(OPENAI_OK);
    let mut req = chat_req(Protocol::OpenaiChat, "deepseek-v4");
    let mut assistant = ChatMsg::text("assistant", "sure");
    assistant.reasoning_content = Some("thought it through".to_owned());
    assistant.reasoning_details = Some(serde_json::json!([{"type":"reasoning.text","text":"t"}]));
    let mut native = ChatMsg::text("assistant", String::new());
    native.parts = Some(serde_json::json!([
        {"type":"thinking","thinking":"native prose","signature":"sig"},
        {"type":"redacted_thinking","data":"blob"},
        {"type":"text","text":"answer"}
    ]));
    req.message
        .extend([assistant, ChatMsg::text("user", "and?"), native]);
    let _ = OpenAiEngine::new(req, t.clone()).run().await.unwrap();
    let b = t.body_json();
    let messages = b["messages"].as_array().unwrap();
    assert_eq!(messages[2]["reasoning_content"], "thought it through");
    assert_eq!(messages[2]["reasoning_details"][0]["text"], "t");
    assert_eq!(messages[4]["reasoning_content"], "native prose");
    assert_eq!(
        messages[4]["content"],
        serde_json::json!([{"type":"text","text":"answer"}])
    );
}
