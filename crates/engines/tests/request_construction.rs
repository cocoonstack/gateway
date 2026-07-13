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

/// Captures the request the engine built, replies with a minimal valid body.
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
            body: UpstreamBody::Json(self.reply.clone()),
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
    req.account = Some(Account {
        name: "live-anthropic".into(),
        endpoint: "https://api.anthropic.com".into(),
        ..Default::default()
    });
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
    req2.account = Some(Account {
        name: "live-gemini".into(),
        endpoint: "https://generativelanguage.googleapis.com".into(),
        ..Default::default()
    });
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
    req.account = Some(Account {
        name: "live-bedrock".into(),
        endpoint: "https://bedrock-runtime.eu-west-1.amazonaws.com".into(),
        api_key_env: "GW_TEST_AWS_AK".into(),
        secret_key_env: "GW_TEST_AWS_SK".into(),
        ..Default::default()
    });
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
    req.account = Some(Account {
        name: "live-dashscope".into(),
        endpoint: "https://dashscope.aliyuncs.com".into(),
        ..Default::default()
    });
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
