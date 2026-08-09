//! Golden byte-level alignment against REAL recorded vendor responses: each
//! fixture is a recorded upstream body fed through the Rust engines and diffed
//! against expected values — the offline half of byte-level vendor alignment
//! (the online half needs credentials + egress).

use std::sync::Arc;

use async_trait::async_trait;
use gw_consts::Protocol;
use gw_engines::transport::{Transport, UpstreamBody, UpstreamRequest, UpstreamResponse};
use gw_engines::{ClaudeEngine, ModelEngine, OpenAiEngine, VertexEngine, extract_common_usage};
use gw_models::{ChatMsg, GResult, GatewayRequest, ModelParamV2};

#[derive(Debug)]
struct FixtureTransport {
    status: u16,
    sse: bool,
    bytes: Vec<u8>,
}

#[async_trait]
impl Transport for FixtureTransport {
    async fn send(&self, _req: UpstreamRequest) -> GResult<UpstreamResponse> {
        Ok(UpstreamResponse {
            status: self.status,
            body: if self.sse {
                UpstreamBody::Sse(self.bytes.clone())
            } else {
                UpstreamBody::Json(bytes::Bytes::from(self.bytes.clone()))
            },
        })
    }
}

fn openai_req() -> GatewayRequest {
    GatewayRequest {
        message: vec![ChatMsg::text("user", "hi")],
        model_param_v2: Some(ModelParamV2::with_name(Protocol::OpenaiChat, "gpt")),
        ..Default::default()
    }
}

const GO_OPENAI_CHAT: &str = r#"{"id":"chatcmpl-test","object":"chat.completion","model":"test-model","choices":[{"index":0,"message":{"role":"assistant","content":"Hello!"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}}"#;

#[tokio::test]
async fn openai_chat_matches_go_recorded_response() {
    let transport = Arc::new(FixtureTransport {
        status: 200,
        sse: false,
        bytes: GO_OPENAI_CHAT.as_bytes().to_vec(),
    });
    let out = OpenAiEngine::new(openai_req(), transport)
        .run()
        .await
        .unwrap();
    assert_eq!(out.response.message, "Hello!");
    assert_eq!(out.response.model, "test-model");
    assert_eq!(out.response.finish_reason, "stop");
    assert_eq!(out.response.prompt_tokens, 5);
    assert_eq!(out.response.completion_tokens, 3);
    assert_eq!(out.response.total_tokens, 8);
    let usage = out.response.raw_usage.unwrap();
    assert_eq!(
        usage,
        serde_json::json!({"completion_tokens": 3, "prompt_tokens": 5, "total_tokens": 8})
    );
}

const GO_OPENAI_SSE: &str = "data: {\"id\":\"cmpl-7\",\"object\":\"text_completion\",\"created\":1684243313,\"choices\":[{\"text\":\"AL\",\"index\":0,\"finish_reason\":null}],\"model\":\"text-davinci-003\"}\n\ndata: [DONE]\n\n";

#[tokio::test]
async fn openai_sse_decodes_go_recorded_stream() {
    use gw_engines::SseDecoder;
    let (events, done) = SseDecoder::decode_all(GO_OPENAI_SSE.as_bytes()).unwrap();
    assert!(done, "must see [DONE] from the recorded stream");
    assert_eq!(events.len(), 1);
    let v: serde_json::Value = serde_json::from_str(&events[0]).unwrap();
    assert_eq!(v["model"], "text-davinci-003");
    assert_eq!(v["choices"][0]["text"], "AL");
}

#[test]
fn common_usage_matches_go_struct_semantics() {
    let raw = serde_json::json!({"prompt_tokens":10,"completion_tokens":5,"total_tokens":15,
        "prompt_tokens_details":{"cached_tokens":4},
        "completion_tokens_details":{"reasoning_tokens":2}});
    let u = extract_common_usage(&raw, false).unwrap();
    assert_eq!(u.platform_input, 6);
    assert_eq!(u.read_cache, 4);
    assert_eq!(u.write_cache, 0);
    assert_eq!(u.completion, 3);
    assert_eq!(u.reason, 2);
}

#[test]
fn anthropic_common_usage_matches_semantics() {
    let raw = serde_json::json!({"input_tokens":8,"output_tokens":6,"cache_read_input_tokens":1});
    let u = extract_common_usage(&raw, true).unwrap();
    assert_eq!(u.platform_input, 8);
    assert_eq!(u.read_cache, 1);
    assert_eq!(u.completion, 6);
}

fn claude_req() -> GatewayRequest {
    GatewayRequest {
        message: vec![ChatMsg::text("user", "hi")],
        model_param_v2: Some(ModelParamV2::with_name(
            Protocol::AnthropicMessages,
            "claude-test",
        )),
        ..Default::default()
    }
}

async fn run_claude(fixture: &str) -> GResult<gw_engines::EngineOutcome> {
    let transport = Arc::new(FixtureTransport {
        status: 200,
        sse: false,
        bytes: fixture.as_bytes().to_vec(),
    });
    ClaudeEngine::new(claude_req(), transport).run().await
}

const GO_ANTHROPIC_VALID: &str = r#"{"id":"msg_01XFDUDYJgAACzvnptvVoYEL","type":"message","role":"assistant","model":"claude-4-sonnet-20250514","content":[{"type":"text","text":"Hello!"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":25,"output_tokens":150}}"#;

#[tokio::test]
async fn anthropic_valid_matches_go_recorded_response() {
    let out = run_claude(GO_ANTHROPIC_VALID).await.unwrap();
    assert_eq!(out.response.message, "Hello!");
    assert_eq!(out.response.model, "claude-4-sonnet-20250514");
    assert_eq!(out.response.finish_reason, "end_turn");
    assert_eq!(out.response.prompt_tokens, 25);
    assert_eq!(out.response.completion_tokens, 150);
    assert!(out.response.is_messages_protocol);
}

const GO_ANTHROPIC_NO_USAGE: &str = r#"{"id":"msg_01","model":"test","stop_reason":"end_turn"}"#;

#[tokio::test]
async fn anthropic_no_usage_matches_go() {
    let out = run_claude(GO_ANTHROPIC_NO_USAGE).await.unwrap();
    assert_eq!(out.response.model, "test");
    assert_eq!(out.response.finish_reason, "end_turn");
    assert_eq!(out.response.prompt_tokens, 0);
    assert_eq!(out.response.completion_tokens, 0);
}

const GO_ANTHROPIC_NO_STOP: &str =
    r#"{"model":"test","usage":{"input_tokens":10,"output_tokens":5}}"#;

#[tokio::test]
async fn anthropic_no_stop_reason_matches_go() {
    let out = run_claude(GO_ANTHROPIC_NO_STOP).await.unwrap();
    assert_eq!(out.response.finish_reason, "");
    assert_eq!(out.response.prompt_tokens, 10);
    assert_eq!(out.response.completion_tokens, 5);
}

#[test]
fn anthropic_cache_usage_matches_go_recorded() {
    let raw = serde_json::json!({"input_tokens":12,"cache_creation_input_tokens":3,"cache_read_input_tokens":2});
    let u = extract_common_usage(&raw, true).unwrap();
    assert_eq!(u.platform_input, 12);
    assert_eq!(u.read_cache, 2);
    assert_eq!(u.write_cache, 3);
    assert_eq!(u.completion, 0);
}

#[tokio::test]
async fn family_and_bespoke_engines_surface_errors() {
    use gw_engines::{EmbeddingsEngine, ErnieEngine, VertexEngine};
    let err_body = r#"{"error":{"code":"429","message":"vendor rate limited"}}"#;

    async fn expect_err<E: ModelEngine>(mut engine: E) -> u16 {
        engine.run().await.expect_err("error surfaced").http_status
    }

    let v = VertexEngine::new(
        GatewayRequest {
            message: vec![ChatMsg::text("user", "x")],
            model_param_v2: Some(ModelParamV2::with_name(Protocol::Gemini, "g")),
            ..Default::default()
        },
        Arc::new(FixtureTransport {
            status: 200,
            sse: false,
            bytes: err_body.as_bytes().to_vec(),
        }),
    );
    assert_eq!(expect_err(v).await, 429);

    let e = ErnieEngine::new(
        GatewayRequest {
            message: vec![ChatMsg::text("user", "x")],
            model_param_v2: Some(ModelParamV2::with_name(Protocol::Ernie, "e")),
            ..Default::default()
        },
        Arc::new(FixtureTransport {
            status: 200,
            sse: false,
            bytes: err_body.as_bytes().to_vec(),
        }),
    );
    assert_eq!(expect_err(e).await, 429);

    let mut p = ModelParamV2::with_name(Protocol::Embeddings, "emb");
    p.typed = Some(gw_models::TypedParams::Embeddings(
        gw_models::EmbeddingParams {
            input: vec!["a".into()],
            dimensions: None,
        },
    ));
    let em = EmbeddingsEngine::new(
        GatewayRequest {
            model_param_v2: Some(p),
            ..Default::default()
        },
        Arc::new(FixtureTransport {
            status: 200,
            sse: false,
            bytes: err_body.as_bytes().to_vec(),
        }),
    );
    assert_eq!(expect_err(em).await, 429);
}

#[tokio::test]
async fn openai_stream_error_frame_surfaces() {
    let sse = "data: {\"type\":\"error\",\"error\":{\"type\":\"too_many_requests\",\"code\":\"rate_limit_reached\",\"message\":\"Requests have exceeded the throughput limit\"},\"sequence_number\":2}\n\n";
    let err = OpenAiEngine::new(
        openai_req_stream(),
        Arc::new(FixtureTransport {
            status: 200,
            sse: true,
            bytes: sse.as_bytes().to_vec(),
        }),
    )
    .run()
    .await
    .err()
    .unwrap();
    assert!(err.message.contains("exceeded the throughput limit"));
}

#[tokio::test]
async fn openai_stream_error_with_http_code_maps_status() {
    let sse = "data: {\"type\":\"error\",\"error\":{\"type\":\"unprocessable_entity_error\",\"message\":\"output new_sensitive (1027)\",\"http_code\":\"422\"},\"request_id\":\"x\"}\n\n";
    let err = OpenAiEngine::new(
        openai_req_stream(),
        Arc::new(FixtureTransport {
            status: 200,
            sse: true,
            bytes: sse.as_bytes().to_vec(),
        }),
    )
    .run()
    .await
    .err()
    .unwrap();
    assert_eq!(err.http_status, 422);
    assert!(err.message.contains("new_sensitive"));
}

fn openai_req_stream() -> GatewayRequest {
    GatewayRequest {
        stream: true,
        message: vec![ChatMsg::text("user", "hi")],
        model_param_v2: Some(ModelParamV2::with_name(Protocol::OpenaiChat, "gpt")),
        ..Default::default()
    }
}

#[tokio::test]
async fn openai_error_envelope_surfaces() {
    let fixture = r#"{"error":{"type":"rate_limit","code":"429","message":"too many requests"}}"#;
    let transport = Arc::new(FixtureTransport {
        status: 200,
        sse: false,
        bytes: fixture.as_bytes().to_vec(),
    });
    let err = OpenAiEngine::new(openai_req(), transport)
        .run()
        .await
        .err()
        .unwrap();
    assert_eq!(err.http_status, 429);
    assert!(err.message.contains("too many requests"));
}

#[tokio::test]
async fn anthropic_error_envelope_surfaces() {
    let fixture = r#"{"error":{"message":"The request is prohibited due to a violation of provider Terms Of Service","code":403}}"#;
    let err = ClaudeEngine::new(
        claude_req(),
        Arc::new(FixtureTransport {
            status: 200,
            sse: false,
            bytes: fixture.as_bytes().to_vec(),
        }),
    )
    .run()
    .await
    .err()
    .unwrap();
    assert_eq!(err.http_status, 403);
    assert!(err.message.contains("violation of provider Terms"));
}

#[tokio::test]
async fn vendor_http_code_string_maps_the_status() {
    let fixture = r#"{"type":"error","error":{"http_code":"529","message":"cluster overloaded"},"request_id":"t"}"#;
    let err = OpenAiEngine::new(
        openai_req(),
        Arc::new(FixtureTransport {
            status: 200,
            sse: false,
            bytes: fixture.as_bytes().to_vec(),
        }),
    )
    .run()
    .await
    .err()
    .unwrap();
    assert_eq!(err.http_status, 529);
    assert!(err.message.contains("cluster overloaded"));
}

#[tokio::test]
async fn minimax_base_resp_error_surfaces_as_502() {
    let fixture = r#"{"reply":"","base_resp":{"status_code":1002,"status_msg":"rate limit"}}"#;
    let err = gw_engines::MinimaxV1Engine::new(
        GatewayRequest {
            message: vec![ChatMsg::text("user", "hi")],
            model_param_v2: Some(ModelParamV2::with_name(Protocol::MinimaxV1, "abab6.5")),
            ..Default::default()
        },
        Arc::new(FixtureTransport {
            status: 200,
            sse: false,
            bytes: fixture.as_bytes().to_vec(),
        }),
    )
    .run()
    .await
    .expect_err("a non-zero base_resp must not parse as an empty success");
    assert_eq!(err.http_status, 502);
    assert!(err.message.contains("1002"), "{}", err.message);
}

#[tokio::test]
async fn flat_error_bodies_surface_for_bespoke_vendors() {
    let bedrock = r#"{"message":"The security token included in the request is invalid."}"#;
    let err = gw_engines::LlamaEngine::new(
        GatewayRequest {
            message: vec![ChatMsg::text("user", "hi")],
            model_param_v2: Some(ModelParamV2::with_name(Protocol::AwsLlama, "llama3-70b")),
            ..Default::default()
        },
        Arc::new(FixtureTransport {
            status: 403,
            sse: false,
            bytes: bedrock.as_bytes().to_vec(),
        }),
    )
    .run()
    .await
    .expect_err("a flat 403 body must not parse as an empty success");
    assert_eq!(err.http_status, 403);
    assert!(err.message.contains("security token"), "{}", err.message);

    let dashscope =
        r#"{"code":"Throttling.RateQuota","message":"Requests throttled","request_id":"r"}"#;
    let err = gw_engines::DashScopeEngine::new(
        GatewayRequest {
            message: vec![ChatMsg::text("user", "hi")],
            model_param_v2: Some(ModelParamV2::with_name(Protocol::Dashscope, "qwen-max")),
            ..Default::default()
        },
        Arc::new(FixtureTransport {
            status: 429,
            sse: false,
            bytes: dashscope.as_bytes().to_vec(),
        }),
    )
    .run()
    .await
    .expect_err("a flat 429 body must not parse as an empty success");
    assert_eq!(err.http_status, 429);
    assert!(err.message.contains("throttled"), "{}", err.message);
}

const GO_GEMINI: &str = r#"{"candidates":[{"content":{"role":"model","parts":[{"text":"Hi from gemini"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":15,"candidatesTokenCount":10,"totalTokenCount":25}}"#;

#[tokio::test]
async fn gemini_usage_metadata_matches_go_recorded() {
    let transport = Arc::new(FixtureTransport {
        status: 200,
        sse: false,
        bytes: GO_GEMINI.as_bytes().to_vec(),
    });
    let req = GatewayRequest {
        message: vec![ChatMsg::text("user", "hi")],
        model_param_v2: Some(ModelParamV2::with_name(Protocol::Gemini, "gemini-pro")),
        ..Default::default()
    };
    let out = VertexEngine::new(req, transport).run().await.unwrap();
    assert_eq!(out.response.message, "Hi from gemini");
    assert_eq!(out.response.finish_reason, "stop");
    assert_eq!(out.response.prompt_tokens, 15);
    assert_eq!(out.response.completion_tokens, 10);
    assert_eq!(out.response.total_tokens, 25);
}

fn emitted_arguments(out: &gw_engines::EngineOutcome) -> String {
    out.chunks
        .iter()
        .filter_map(|c| c.tool_calls.as_ref()?.as_array())
        .flatten()
        .filter_map(|call| call.pointer("/function/arguments")?.as_str())
        .collect()
}

const YYLX_TOOL_ARGS_SSE: &str = "data: {\"id\":\"msg_011Cdrd2XtxmXszwJB3DqaUj\",\"object\":\"chat.completion.chunk\",\"created\":1786253284,\"model\":\"claude-sonnet-5\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\ndata: {\"id\":\"msg_011Cdrd2XtxmXszwJB3DqaUj\",\"object\":\"chat.completion.chunk\",\"created\":1786253284,\"model\":\"claude-sonnet-5\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"toolu_016Z8ASP2TT9fMRNTiu6dL3S\",\"type\":\"function\",\"function\":{\"name\":\"shell\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}\n\ndata: {\"id\":\"msg_011Cdrd2XtxmXszwJB3DqaUj\",\"object\":\"chat.completion.chunk\",\"created\":1786253284,\"model\":\"claude-sonnet-5\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"\",\"arguments\":\"{\\\"command\\\": \\\"ls -la /tmp\\\"}\"}}]},\"finish_reason\":null}]}\n\ndata: {\"id\":\"msg_011Cdrd2XtxmXszwJB3DqaUj\",\"object\":\"chat.completion.chunk\",\"created\":1786253284,\"model\":\"claude-sonnet-5\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\"},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n";

#[tokio::test]
async fn yylx_recorded_empty_name_continuation_assembles_one_call() {
    let transport = Arc::new(FixtureTransport {
        status: 200,
        sse: true,
        bytes: YYLX_TOOL_ARGS_SSE.as_bytes().to_vec(),
    });
    let out = OpenAiEngine::new(openai_req(), transport)
        .run()
        .await
        .unwrap();
    let calls = out.response.tool_calls.as_ref().expect("tool calls");
    let calls = calls.as_array().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["function"]["name"], "shell");
    assert_eq!(
        calls[0]["function"]["arguments"],
        "{\"command\": \"ls -la /tmp\"}"
    );
    let emitted = emitted_arguments(&out);
    assert_eq!(emitted, "{\"command\": \"ls -la /tmp\"}");
}

const YYLX_NO_ARG_SSE: &str = "data: {\"id\":\"msg_9afb323acd5c466d833909124cb45916\",\"object\":\"chat.completion.chunk\",\"created\":1786253287,\"model\":\"claude-haiku-4-5\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\ndata: {\"id\":\"msg_9afb323acd5c466d833909124cb45916\",\"object\":\"chat.completion.chunk\",\"created\":1786253287,\"model\":\"claude-haiku-4-5\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"tooluse_37bmNjkbDnFcluXowfrJQG\",\"type\":\"function\",\"function\":{\"name\":\"now\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}\n\ndata: {\"id\":\"msg_9afb323acd5c466d833909124cb45916\",\"object\":\"chat.completion.chunk\",\"created\":1786253287,\"model\":\"claude-haiku-4-5\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\"},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n";

#[tokio::test]
async fn yylx_recorded_no_arg_call_completes_to_an_object() {
    let transport = Arc::new(FixtureTransport {
        status: 200,
        sse: true,
        bytes: YYLX_NO_ARG_SSE.as_bytes().to_vec(),
    });
    let out = OpenAiEngine::new(openai_req(), transport)
        .run()
        .await
        .unwrap();
    let calls = out.response.tool_calls.as_ref().expect("tool calls");
    assert_eq!(calls[0]["function"]["arguments"], "{}");
    let emitted = emitted_arguments(&out);
    assert_eq!(emitted, "{}");
}

const YYLX_GPT_NO_ARG_SSE: &str = "data: {\"id\":\"resp_09ab326ce09ca4b3016a7811233af8819a8bba052986e818e1\",\"object\":\"chat.completion.chunk\",\"created\":1786253603,\"model\":\"gpt-5.4-mini\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\ndata: {\"id\":\"resp_09ab326ce09ca4b3016a7811233af8819a8bba052986e818e1\",\"object\":\"chat.completion.chunk\",\"created\":1786253603,\"model\":\"gpt-5.4-mini\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_FzvrVLrIodcfubgWJS0LzLCo\",\"type\":\"function\",\"function\":{\"name\":\"now\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}\n\ndata: {\"id\":\"resp_09ab326ce09ca4b3016a7811233af8819a8bba052986e818e1\",\"object\":\"chat.completion.chunk\",\"created\":1786253603,\"model\":\"gpt-5.4-mini\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"\",\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}\n\ndata: {\"id\":\"resp_09ab326ce09ca4b3016a7811233af8819a8bba052986e818e1\",\"object\":\"chat.completion.chunk\",\"created\":1786253603,\"model\":\"gpt-5.4-mini\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\"},\"finish_reason\":\"tool_calls\"}]}\n\ndata: {\"id\":\"resp_09ab326ce09ca4b3016a7811233af8819a8bba052986e818e1\",\"object\":\"chat.completion.chunk\",\"created\":1786253603,\"model\":\"gpt-5.4-mini\",\"choices\":[],\"usage\":{\"prompt_tokens\":41,\"completion_tokens\":33,\"total_tokens\":74,\"completion_tokens_details\":{\"reasoning_tokens\":18}}}\n\ndata: [DONE]\n\n";

#[tokio::test]
async fn yylx_recorded_empty_name_object_continuation_streams_through() {
    let transport = Arc::new(FixtureTransport {
        status: 200,
        sse: true,
        bytes: YYLX_GPT_NO_ARG_SSE.as_bytes().to_vec(),
    });
    let out = OpenAiEngine::new(openai_req(), transport)
        .run()
        .await
        .unwrap();
    assert_eq!(
        out.response.tool_calls.expect("tool calls")[0]["function"]["arguments"],
        "{}"
    );
    assert!(
        out.chunks.iter().any(|c| {
            c.tool_calls
                .as_ref()
                .and_then(|t| t.pointer("/0/function"))
                .is_some_and(|f| f["name"] == "" && f["arguments"] == "{}")
        }),
        "the {{}} continuation must reach the wire as-is, not be withheld"
    );
    assert_eq!(out.response.completion_tokens, 33);
    let usage = extract_common_usage(out.response.raw_usage.as_ref().unwrap(), false).unwrap();
    assert_eq!(usage.reason, 18);
}
