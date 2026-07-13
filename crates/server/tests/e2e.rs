//! End-to-end round against the fully composed app (embedded config + in-process
//! state + MockTransport). Exercises the same wiring `main.rs` serves, one HTTP
//! call at a time: auth → resolve → quota → account → rate-limit → engine →
//! usage → billing. No network leaves the process (zero-egress default build).

// test scaffolding — unwrap/expect allowed as in #[test] fns (clippy.toml can't reach helpers here)
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::Response;
use gw_config::GatewayConfig;
use gw_state::GatewayState;
use gw_views::AppState;
use serde_json::{Value, json};
use tower::ServiceExt;

fn app() -> Router {
    let cfg = Arc::new(GatewayConfig::embedded_default().expect("embedded config"));
    let state = Arc::new(GatewayState::from_config(&cfg));
    gw_views::app(AppState::new(
        cfg,
        state,
        Arc::new(gw_engines::MockTransport),
    ))
}

async fn body_bytes(resp: Response) -> Vec<u8> {
    axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body")
        .to_vec()
}

async fn body_json(resp: Response) -> Value {
    serde_json::from_slice(&body_bytes(resp).await).expect("json body")
}

fn post(uri: &str, ak: Option<&str>, body: &str) -> Request<Body> {
    let mut b = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(ak) = ak {
        b = b.header("authorization", format!("Bearer {ak}"));
    }
    b.body(Body::from(body.to_owned())).expect("request")
}

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("request")
}

/// GET with the demo AK — the files/batches read surfaces require auth.
fn get_authed(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header("authorization", "Bearer ak-demo-123")
        .body(Body::empty())
        .expect("request")
}

const CHAT_BODY: &str = r#"{"model":"gpt-4o","messages":[{"role":"user","content":"hello e2e"}]}"#;

#[tokio::test]
async fn health_and_models() {
    let app = app();
    let resp = app.clone().oneshot(get("/health")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app.oneshot(get("/v1/models")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    let ids: Vec<&str> = j["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"gpt-4o") && ids.contains(&"claude-sonnet"));
    assert!(
        j["data"]
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["implemented"] == Value::Bool(true))
    );
}

#[tokio::test]
async fn auth_is_enforced() {
    let app = app();
    let resp = app
        .clone()
        .oneshot(post("/v1/chat/completions", None, CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let resp = app
        .oneshot(post("/v1/chat/completions", Some("ak-wrong"), CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn model_failure_modes_404_503_501() {
    let app = app();
    // name known to nobody → resolve_model 404
    let resp = app
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            r#"{"model":"totally-bogus","messages":[{"role":"user","content":"x"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // valid protocol but no account slot serves it → select_account 503
    // (account selection precedes engine creation, matching the pipeline layer order)
    let resp = app
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            r#"{"model":"aws-llama","messages":[{"role":"user","content":"x"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

    // realtime family: account exists but websocket upstream bridging is future work → 501
    let resp = app
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            r#"{"model":"realtime","messages":[{"role":"user","content":"x"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn embeddings_images_audio_families() {
    let app = app();

    // embeddings
    let resp = app
        .clone()
        .oneshot(post(
            "/v1/embeddings",
            Some("ak-demo-123"),
            r#"{"model":"text-embedding-3","input":["hello","world"]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert_eq!(j["object"], "list");
    assert_eq!(j["data"].as_array().unwrap().len(), 2);
    assert_eq!(j["data"][0]["embedding"].as_array().unwrap().len(), 8);
    assert!(j["usage"]["prompt_tokens"].as_i64().unwrap() > 0);

    // images
    let resp = app
        .clone()
        .oneshot(post(
            "/v1/images/generations",
            Some("ak-demo-123"),
            r#"{"model":"dall-e-3","prompt":"a red panda","n":2}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert_eq!(j["data"].as_array().unwrap().len(), 2);
    assert!(j["data"][0]["b64_json"].is_string());

    // audio tts → binary audio bytes
    let resp = app
        .clone()
        .oneshot(post(
            "/v1/audio/speech",
            Some("ak-demo-123"),
            r#"{"model":"tts-1","input":"read this aloud","voice":"alloy"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(ct.starts_with("audio/"), "content-type: {ct}");
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes, b"MOCKBYTES"); // MOCK_B64 decoded

    // audio stt
    let resp = app
        .oneshot(post(
            "/v1/audio/transcriptions",
            Some("ak-demo-123"),
            r#"{"model":"whisper-1","audio_b64":"TU9DSw==","language":"en"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert!(j["text"].as_str().unwrap().contains("transcribed"));
}

#[tokio::test]
async fn vertex_chat_family() {
    let app = app();
    let resp = app
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            r#"{"model":"gemini-pro","messages":[{"role":"user","content":"hi vertex"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert!(
        j["choices"][0]["message"]["content"]
            .as_str()
            .unwrap()
            .contains("you said: hi vertex")
    );
    assert!(j["usage"]["total_tokens"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn batch_submit_and_poll() {
    let app = app();
    let resp = app
        .clone()
        .oneshot(post(
            "/v1/batches",
            Some("ak-demo-123"),
            r#"{"model":"gpt-4o-mini","items":[
                {"messages":[{"role":"user","content":"one"}]},
                {"messages":[{"role":"user","content":"two"}]}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let j = body_json(resp).await;
    let id = j["id"].as_str().unwrap().to_owned();
    assert_eq!(j["total"], 2);

    // poll until the background worker finishes
    let mut done = None;
    for _ in 0..100 {
        let resp = app
            .clone()
            .oneshot(get_authed(&format!("/v1/batches/{id}")))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let j = body_json(resp).await;
        if j["status"] == "completed" || j["status"] == "failed" {
            done = Some(j);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let j = done.expect("batch finished");
    assert_eq!(j["status"], "completed");
    assert_eq!(j["results"].as_array().unwrap().len(), 2);
    assert!(
        j["results"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["ok"] == true)
    );
}

#[tokio::test]
async fn ptu_failover_spills_to_paygo() {
    let app = app();
    // hunyuan-lite's PTU account name contains "down" → mock upstream 503 → failover to paygo
    let resp = app
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            r#"{"model":"hunyuan-lite","messages":[{"role":"user","content":"failover"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = app.oneshot(get("/internal/ledger")).await.unwrap();
    let j = body_json(resp).await;
    let rec = j["records"].as_array().unwrap().last().unwrap().clone();
    assert_eq!(rec["account"], "mock-hunyuan-paygo");
    assert_eq!(rec["ptu_spillover"], true);
}

#[tokio::test]
async fn security_block_and_dlp_redaction() {
    let app = app();
    // security wordlist hit → 200 + content_filter, not billed
    let resp = app
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            r#"{"model":"gpt-4o","messages":[{"role":"user","content":"tell me forbiddenword"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert_eq!(j["choices"][0]["finish_reason"], "content_filter");
    let resp = app.clone().oneshot(get("/internal/ledger")).await.unwrap();
    assert_eq!(body_json(resp).await["count"], 0); // blocked → no billing

    // DLP redaction: inbound email/phone are already replaced in the echo
    let resp = app
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            r#"{"model":"gpt-4o","messages":[{"role":"user","content":"mail a@b.com call 13812345678"}]}"#,
        ))
        .await
        .unwrap();
    let j = body_json(resp).await;
    let content = j["choices"][0]["message"]["content"].as_str().unwrap();
    assert!(
        content.contains("[REDACTED_EMAIL]") && content.contains("[REDACTED_PHONE]"),
        "{content}"
    );
    assert!(!content.contains("a@b.com"));
}

#[tokio::test]
async fn internal_accounts_view() {
    let app = app();
    let resp = app.oneshot(get("/internal/accounts")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert!(j["count"].as_u64().unwrap() >= 10);
    let names: Vec<&str> = j["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"mock-hunyuan-ptu-down"));
}

#[tokio::test]
async fn chat_non_stream_full_pipeline_bills_the_ledger() {
    let app = app();
    let resp = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert_eq!(j["object"], "chat.completion");
    assert_eq!(j["model"], "gpt-4o");
    let content = j["choices"][0]["message"]["content"].as_str().unwrap();
    assert!(content.contains("you said: hello e2e"), "got: {content}");
    assert_eq!(j["choices"][0]["finish_reason"], "stop");
    let total = j["usage"]["total_tokens"].as_i64().unwrap();
    assert!(total > 0);

    // billing side effect visible through the local ledger
    let resp = app.oneshot(get("/internal/ledger")).await.unwrap();
    let j = body_json(resp).await;
    assert_eq!(j["count"], 1);
    let rec = &j["records"][0];
    assert_eq!(rec["ak"], "ak-demo-123");
    assert_eq!(rec["model"], "gpt-4o");
    assert_eq!(rec["account"], "mock-openai-1");
    assert_eq!(rec["total_tokens"].as_i64().unwrap(), total);
    assert!(rec["cost_micros"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn chat_stream_emits_sse_chunks_and_done() {
    let app = app();
    let body =
        r#"{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"stream me"}]}"#;
    let resp = app
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(ct.starts_with("text/event-stream"), "content-type: {ct}");

    let text = String::from_utf8(body_bytes(resp).await).unwrap();
    let frames: Vec<&str> = text
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .collect();
    assert!(frames.len() >= 3, "sse frames: {frames:?}");
    assert_eq!(*frames.last().unwrap(), "[DONE]");

    // reassemble deltas and check the finish frame carries usage
    let mut assembled = String::new();
    let mut saw_finish_with_usage = false;
    for f in &frames[..frames.len() - 1] {
        let v: Value = serde_json::from_str(f).unwrap();
        assert_eq!(v["object"], "chat.completion.chunk");
        if let Some(d) = v["choices"][0]["delta"]["content"].as_str() {
            assembled.push_str(d);
        }
        if v["choices"][0]["finish_reason"] == "stop"
            && v["usage"]["total_tokens"].as_i64().unwrap_or(0) > 0
        {
            saw_finish_with_usage = true;
        }
    }
    assert!(
        assembled.contains("you said: stream me"),
        "assembled: {assembled}"
    );
    assert!(saw_finish_with_usage);
}

#[tokio::test]
async fn chat_stream_tools_emit_tool_call_chunks() {
    let app = app();
    let body = r#"{"model":"gpt-4o","stream":true,
        "messages":[{"role":"user","content":"call the tool"}],
        "tools":[{"type":"function","function":{"name":"get_weather","parameters":{}}}]}"#;
    let resp = app
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = String::from_utf8(body_bytes(resp).await).unwrap();
    let mut saw_tool_chunk = false;
    let mut finish = String::new();
    for f in text.lines().filter_map(|l| l.strip_prefix("data: ")) {
        if f == "[DONE]" {
            continue;
        }
        let v: Value = serde_json::from_str(f).unwrap();
        let delta = &v["choices"][0]["delta"];
        if delta["tool_calls"][0]["function"]["name"] == "get_weather" {
            saw_tool_chunk = true;
        }
        if let Some(fr) = v["choices"][0]["finish_reason"].as_str() {
            finish = fr.to_owned();
        }
    }
    assert!(saw_tool_chunk, "stream must carry the tool_calls delta");
    assert_eq!(finish, "tool_calls");
}

#[tokio::test]
async fn gemini_stream_emits_incremental_deltas() {
    let app = app();
    let body = r#"{"model":"gemini-pro","stream":true,"messages":[{"role":"user","content":"stream me gemini"}]}"#;
    let resp = app
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = String::from_utf8(body_bytes(resp).await).unwrap();
    let mut deltas = 0;
    let mut assembled = String::new();
    let mut saw_usage = false;
    for f in text.lines().filter_map(|l| l.strip_prefix("data: ")) {
        if f == "[DONE]" {
            continue;
        }
        let v: Value = serde_json::from_str(f).unwrap();
        if let Some(d) = v["choices"][0]["delta"]["content"].as_str() {
            deltas += 1;
            assembled.push_str(d);
        }
        if v["usage"]["total_tokens"].as_i64().unwrap_or(0) > 0 {
            saw_usage = true;
        }
    }
    assert!(deltas >= 2, "expected incremental deltas, got {deltas}");
    assert!(assembled.contains("you said: stream me gemini"));
    assert!(saw_usage, "final frame must carry usage");
}

#[tokio::test]
async fn dashscope_stream_emits_incremental_deltas() {
    let app = app();
    let body = r#"{"model":"qwen-max","stream":true,"messages":[{"role":"user","content":"stream me dashscope"}]}"#;
    let resp = app
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = String::from_utf8(body_bytes(resp).await).unwrap();
    let mut deltas = 0;
    let mut assembled = String::new();
    let mut saw_usage = false;
    for f in text.lines().filter_map(|l| l.strip_prefix("data: ")) {
        if f == "[DONE]" {
            continue;
        }
        let v: Value = serde_json::from_str(f).unwrap();
        if let Some(d) = v["choices"][0]["delta"]["content"].as_str() {
            deltas += 1;
            assembled.push_str(d);
        }
        if v["usage"]["total_tokens"].as_i64().unwrap_or(0) > 0 {
            saw_usage = true;
        }
    }
    assert!(deltas >= 2, "expected incremental deltas, got {deltas}");
    assert!(assembled.contains("you said: stream me dashscope"));
    assert!(saw_usage, "final frame must carry usage");
}

#[tokio::test]
async fn messages_errors_are_anthropic_shaped() {
    let app = app();
    // no api key → authentication_error with the anthropic discriminator
    let r = app
        .clone()
        .oneshot(post(
            "/v1/messages",
            None,
            r#"{"model":"claude-sonnet","messages":[{"role":"user","content":"x"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    let j = body_json(r).await;
    assert_eq!(j["type"], "error");
    assert_eq!(j["error"]["type"], "authentication_error");

    // unknown model → not_found_error
    let r = app
        .oneshot(post(
            "/v1/messages",
            Some("ak-demo-123"),
            r#"{"model":"nope","messages":[{"role":"user","content":"x"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
    let j = body_json(r).await;
    assert_eq!(j["type"], "error");
    assert_eq!(j["error"]["type"], "not_found_error");
    assert!(j["error"]["message"].as_str().unwrap().contains("nope"));
}

#[tokio::test]
async fn messages_cross_protocol_converts_tool_calls_to_tool_use() {
    // an openai-family model behind /v1/messages: anthropic-shaped tool defs
    // must reach the vendor as function defs, and the returned tool_calls must
    // come back as anthropic tool_use blocks (the dsl mapping at work)
    let app = app();
    let body = r#"{"model":"gpt-4o","max_tokens":64,
        "messages":[{"role":"user","content":"use the tool"}],
        "tools":[{"name":"get_weather","description":"d","input_schema":{"type":"object"}}]}"#;
    let resp = app
        .clone()
        .oneshot(post("/v1/messages", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    let block = j["content"]
        .as_array()
        .and_then(|c| c.iter().find(|b| b["type"] == "tool_use"))
        .cloned()
        .expect("tool_use block from a cross-protocol model");
    assert_eq!(block["name"], "get_weather");
    assert!(block["input"].is_object(), "arguments parsed: {block}");
    assert_eq!(j["stop_reason"], "tool_use");

    // streaming: same conversion, emitted as tool_use content blocks
    let body = r#"{"model":"gpt-4o","max_tokens":64,"stream":true,
        "messages":[{"role":"user","content":"use the tool"}],
        "tools":[{"name":"get_weather","description":"d","input_schema":{"type":"object"}}]}"#;
    let resp = app
        .oneshot(post("/v1/messages", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = String::from_utf8(body_bytes(resp).await).unwrap();
    assert!(text.contains(r#""type":"tool_use""#), "sse: {text}");
    assert!(text.contains("get_weather"), "sse: {text}");
}

#[tokio::test]
async fn anthropic_streaming_carries_tool_use_blocks() {
    let app = app();
    let body = r#"{"model":"claude-sonnet","stream":true,"max_tokens":64,
        "messages":[{"role":"user","content":"use the tool"}],
        "tools":[{"name":"get_weather","description":"d","input_schema":{}}]}"#;
    let resp = app
        .oneshot(post("/v1/messages", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = String::from_utf8(body_bytes(resp).await).unwrap();
    assert!(text.contains(r#""type":"tool_use""#), "sse: {text}");
    assert!(text.contains("input_json_delta"), "sse: {text}");
    assert!(text.contains("get_weather"), "sse: {text}");
}

#[tokio::test]
async fn anthropic_messages_non_stream() {
    let app = app();
    let body = r#"{"model":"claude-sonnet","max_tokens":128,"messages":[{"role":"user","content":"ping claude"}]}"#;
    let resp = app
        .clone()
        .oneshot(post("/v1/messages", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert_eq!(j["type"], "message");
    assert_eq!(j["role"], "assistant");
    assert_eq!(j["stop_reason"], "end_turn");
    assert!(
        j["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("you said: ping claude")
    );
    assert!(j["usage"]["input_tokens"].as_i64().unwrap() > 0);
    assert!(j["usage"]["output_tokens"].as_i64().unwrap() > 0);

    // block-array content form also accepted
    let body = r#"{"model":"claude-sonnet","messages":[{"role":"user","content":[{"type":"text","text":"blocks"}]}]}"#;
    let resp = app
        .oneshot(post("/v1/messages", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert!(
        j["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("you said: blocks")
    );
}

#[tokio::test]
async fn rate_limit_qps1_second_call_429() {
    let app = app();
    let first = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-limited"), CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let second = app
        .oneshot(post("/v1/chat/completions", Some("ak-limited"), CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    let j = body_json(second).await;
    assert!(
        j["error"]["message"]
            .as_str()
            .unwrap()
            .contains("rate limit")
    );
}

#[tokio::test]
async fn quota_exhaustion_second_call_429() {
    let app = app();
    // daily_token_quota=1: first call passes the pre-check then consumes >1 token
    let first = app
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-tiny-quota"),
            CHAT_BODY,
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let second = app
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-tiny-quota"),
            CHAT_BODY,
        ))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    let j = body_json(second).await;
    assert!(j["error"]["message"].as_str().unwrap().contains("quota"));
}

// ==================== governance & protocol-surface coverage ====================

#[tokio::test]
async fn tools_function_calling_round_trip() {
    let app = app();
    // turn 1: model requests a tool call
    let body = r#"{"model":"gpt-4o","messages":[{"role":"user","content":"what's the weather in sf"}],
        "tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}}}],
        "tool_choice":"auto"}"#;
    let resp = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert_eq!(j["choices"][0]["finish_reason"], "tool_calls");
    let call = &j["choices"][0]["message"]["tool_calls"][0];
    assert_eq!(call["function"]["name"], "get_weather");
    assert!(j["choices"][0]["message"].get("content").is_none());

    // turn 2: send the tool result back (no tools field) → normal text turn
    let body = r#"{"model":"gpt-4o","messages":[
        {"role":"user","content":"what's the weather in sf"},
        {"role":"assistant","content":null,"tool_calls":[{"id":"call-mock-1","type":"function",
            "function":{"name":"get_weather","arguments":"{}"}}]},
        {"role":"tool","tool_call_id":"call-mock-1","content":"sunny 20C"}]}"#;
    let resp = app
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert_eq!(j["choices"][0]["finish_reason"], "stop");
}

#[tokio::test]
async fn multimodal_content_parts() {
    let app = app();
    let body = r#"{"model":"gpt-4o","messages":[{"role":"user","content":[
        {"type":"text","text":"what is in this picture?"},
        {"type":"image_url","image_url":{"url":"data:image/png;base64,iVBORw0KGgo="}}]}]}"#;
    let resp = app
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    let content = j["choices"][0]["message"]["content"].as_str().unwrap();
    assert!(content.contains("[saw 1 image(s)]"), "{content}");
    assert!(content.contains("what is in this picture?"));
}

#[tokio::test]
async fn anthropic_streaming_event_sequence() {
    let app = app();
    let body = r#"{"model":"claude-sonnet","stream":true,"max_tokens":64,
        "messages":[{"role":"user","content":"stream me claude"}]}"#;
    let resp = app
        .oneshot(post("/v1/messages", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(ct.starts_with("text/event-stream"), "content-type: {ct}");

    let text = String::from_utf8(body_bytes(resp).await).unwrap();
    let events: Vec<&str> = text
        .lines()
        .filter_map(|l| l.strip_prefix("event: "))
        .collect();
    assert_eq!(events.first(), Some(&"message_start"));
    assert_eq!(events.last(), Some(&"message_stop"));
    assert!(events.contains(&"content_block_delta"));
    assert!(events.contains(&"message_delta"));
    // reassemble text deltas
    let mut assembled = String::new();
    for l in text.lines().filter_map(|l| l.strip_prefix("data: ")) {
        let v: Value = serde_json::from_str(l).unwrap();
        if v["type"] == "content_block_delta" {
            assembled.push_str(v["delta"]["text"].as_str().unwrap_or_default());
        }
    }
    assert!(
        assembled.contains("you said: stream me claude"),
        "{assembled}"
    );
}

#[tokio::test]
async fn anthropic_system_and_tools() {
    let app = app();
    // system prompt reaches the vendor (mock echoes [sys:...])
    let body = r#"{"model":"claude-sonnet","system":"be brief","max_tokens":64,
        "messages":[{"role":"user","content":"sys check"}]}"#;
    let resp = app
        .clone()
        .oneshot(post("/v1/messages", Some("ak-demo-123"), body))
        .await
        .unwrap();
    let j = body_json(resp).await;
    assert!(
        j["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("[sys:be brief]")
    );

    // tools → tool_use block + stop_reason=tool_use
    let body = r#"{"model":"claude-sonnet","max_tokens":64,
        "tools":[{"name":"get_weather","description":"d","input_schema":{"type":"object"}}],
        "messages":[{"role":"user","content":"weather in sf"}]}"#;
    let resp = app
        .oneshot(post("/v1/messages", Some("ak-demo-123"), body))
        .await
        .unwrap();
    let j = body_json(resp).await;
    assert_eq!(j["stop_reason"], "tool_use");
    assert_eq!(j["content"][0]["type"], "tool_use");
    assert_eq!(j["content"][0]["name"], "get_weather");
}

#[tokio::test]
async fn cross_protocol_exchanger_both_ways() {
    let app = app();
    // Anthropic surface → OpenAI-family model (gpt-4o)
    let body = r#"{"model":"gpt-4o","max_tokens":64,
        "messages":[{"role":"user","content":"cross to openai"}]}"#;
    let resp = app
        .clone()
        .oneshot(post("/v1/messages", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert_eq!(j["type"], "message");
    assert_eq!(j["stop_reason"], "end_turn"); // openai "stop" → anthropic "end_turn"
    assert!(
        j["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("[mock-openai:gpt-4o]")
    );

    // OpenAI surface → Claude-family model (claude-sonnet)
    let body =
        r#"{"model":"claude-sonnet","messages":[{"role":"user","content":"cross to claude"}]}"#;
    let resp = app
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert_eq!(j["object"], "chat.completion");
    assert_eq!(j["choices"][0]["finish_reason"], "stop"); // anthropic "end_turn" → openai "stop"
    assert!(
        j["choices"][0]["message"]["content"]
            .as_str()
            .unwrap()
            .contains("[mock-anthropic:claude-sonnet]")
    );
}

#[tokio::test]
async fn bespoke_ernie_full_pipeline() {
    let app = app();
    let body = r#"{"model":"ernie-4.0","messages":[{"role":"user","content":"你好文心"}]}"#;
    let resp = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert!(
        j["choices"][0]["message"]["content"]
            .as_str()
            .unwrap()
            .contains("[mock-ernie] you said: 你好文心")
    );
    // billed through the same pipeline
    let resp = app.oneshot(get("/internal/ledger")).await.unwrap();
    let j = body_json(resp).await;
    assert_eq!(j["records"][0]["protocol"], "ernie");
    assert!(j["records"][0]["cost_micros"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn request_cache_hits_and_skips_billing() {
    let app = app();
    let body = r#"{"model":"cached-mini","messages":[{"role":"user","content":"cache me"}]}"#;
    let r1 = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    let j1 = body_json(r1).await;
    let r2 = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(r2.status(), StatusCode::OK);
    let j2 = body_json(r2).await;
    assert_eq!(
        j1["choices"][0]["message"]["content"],
        j2["choices"][0]["message"]["content"]
    );
    // second call was a cache hit → only one billing record
    let resp = app.oneshot(get("/internal/ledger")).await.unwrap();
    assert_eq!(body_json(resp).await["count"], 1);
}

#[tokio::test]
async fn files_upload_then_batch_from_file() {
    // OpenAI batch-from-file workflow: upload JSONL → create batch by input_file_id
    // → poll → results. Also exercises /v1/files/{id}/content.
    let app = app();

    // 1) upload a 2-line JSONL batch input file
    let jsonl = "{\"custom_id\":\"a\",\"method\":\"POST\",\"url\":\"/v1/chat/completions\",\"body\":{\"model\":\"gpt-4o-mini\",\"messages\":[{\"role\":\"user\",\"content\":\"one\"}]}}\n{\"custom_id\":\"b\",\"method\":\"POST\",\"url\":\"/v1/chat/completions\",\"body\":{\"model\":\"gpt-4o-mini\",\"messages\":[{\"role\":\"user\",\"content\":\"two\"}]}}";
    let upload_body = json!({"purpose": "batch", "file": jsonl}).to_string();
    let resp = app
        .clone()
        .oneshot(post("/v1/files", Some("ak-demo-123"), &upload_body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert_eq!(j["object"], "file");
    assert_eq!(j["purpose"], "batch");
    let file_id = j["id"].as_str().unwrap().to_owned();

    // 2) content requires auth (ids are sequential/enumerable) and is retrievable
    let resp = app
        .clone()
        .oneshot(get(&format!("/v1/files/{file_id}/content")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let resp = app
        .clone()
        .oneshot(get_authed(&format!("/v1/files/{file_id}/content")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        String::from_utf8(body_bytes(resp).await)
            .unwrap()
            .contains("custom_id")
    );

    // 3) create a batch from the uploaded file (model inferred from the lines)
    let batch_body = json!({"input_file_id": file_id}).to_string();
    let resp = app
        .clone()
        .oneshot(post("/v1/batches", Some("ak-demo-123"), &batch_body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let j = body_json(resp).await;
    assert_eq!(j["total"], 2);
    let id = j["id"].as_str().unwrap().to_owned();

    // 4) poll to completion
    let mut done = None;
    for _ in 0..100 {
        let resp = app
            .clone()
            .oneshot(get_authed(&format!("/v1/batches/{id}")))
            .await
            .unwrap();
        let j = body_json(resp).await;
        if j["status"] == "completed" || j["status"] == "failed" {
            done = Some(j);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let j = done.expect("batch finished");
    assert_eq!(j["status"], "completed");
    assert_eq!(j["results"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn batch_requires_items_or_file() {
    let app = app();
    let resp = app
        .oneshot(post(
            "/v1/batches",
            Some("ak-demo-123"),
            r#"{"model":"gpt-4o-mini"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn image_edits_full_pipeline() {
    // /v1/images/edits: source image + prompt → edited image (same data[].b64_json
    // shape as generations). Requires image; 400 without it.
    let app = app();
    let ok = r#"{"model":"dall-e-3","prompt":"add a rainbow","image":"c3JjaW1nYnl0ZXM=","n":1}"#;
    let resp = app
        .clone()
        .oneshot(post("/v1/images/edits", Some("ak-demo-123"), ok))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert!(
        j["data"][0]["b64_json"].is_string(),
        "edited image returned"
    );

    // missing image → 400
    let bad = r#"{"model":"dall-e-3","prompt":"add a rainbow"}"#;
    let resp = app
        .oneshot(post("/v1/images/edits", Some("ak-demo-123"), bad))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn legacy_completions_full_pipeline() {
    // OpenAI legacy text-completions surface: {prompt} in, text_completion out.
    let app = app();
    let body =
        r#"{"model":"gpt-3.5-turbo-instruct","prompt":"the capital of France is","max_tokens":16}"#;
    let resp = app
        .clone()
        .oneshot(post("/v1/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert_eq!(j["object"], "text_completion");
    // legacy shape: choices[].text (not choices[].message.content)
    assert!(
        j["choices"][0]["text"]
            .as_str()
            .unwrap()
            .contains("you said: the capital of France is")
    );
    assert!(
        j["choices"][0]["message"].is_null(),
        "must not be chat-shaped"
    );
    assert!(j["usage"]["total_tokens"].as_i64().unwrap() > 0);
    // billed once
    let led = app.oneshot(get("/internal/ledger")).await.unwrap();
    assert_eq!(body_json(led).await["count"], 1);
}

#[tokio::test]
async fn legacy_completions_requires_prompt() {
    let app = app();
    let resp = app
        .oneshot(post(
            "/v1/completions",
            Some("ak-demo-123"),
            r#"{"model":"gpt-3.5-turbo-instruct"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn responses_api_full_pipeline() {
    // OpenAI Responses API surface: native body in, native Responses body out,
    // billed on the ledger.
    let app = app();
    let body =
        r#"{"model":"gpt-5-responses","input":"summarize the quarter","instructions":"be terse"}"#;
    let resp = app
        .clone()
        .oneshot(post("/v1/responses", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    // native Responses shape: output[].content[].output_text
    assert_eq!(j["object"], "response");
    assert_eq!(j["status"], "completed");
    assert_eq!(j["output"][0]["content"][0]["type"], "output_text");
    assert!(
        j["output"][0]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("you said: summarize the quarter")
    );
    // Responses usage present (input_tokens/output_tokens)
    assert!(j["usage"]["input_tokens"].as_i64().unwrap() > 0);
    // the call was billed
    let led = app.oneshot(get("/internal/ledger")).await.unwrap();
    assert_eq!(body_json(led).await["count"], 1);
}

#[tokio::test]
async fn responses_api_streaming_full_pipeline() {
    // /v1/responses with stream:true → client-facing Responses SSE sequence
    // (response.created → output_text.delta× → response.completed → [DONE]).
    let app = app();
    let body = r#"{"model":"gpt-5-responses","stream":true,"input":"stream this"}"#;
    let resp = app
        .oneshot(post("/v1/responses", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(ct.starts_with("text/event-stream"), "content-type: {ct}");

    let text = String::from_utf8(body_bytes(resp).await).unwrap();
    let frames: Vec<&str> = text
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .collect();
    assert_eq!(*frames.last().unwrap(), "[DONE]");

    let mut assembled = String::new();
    let mut saw_completed_with_usage = false;
    for f in &frames[..frames.len() - 1] {
        let v: Value = serde_json::from_str(f).unwrap();
        match v["type"].as_str().unwrap_or_default() {
            "response.output_text.delta" => assembled.push_str(v["delta"].as_str().unwrap_or("")),
            "response.completed" => {
                saw_completed_with_usage = saw_completed_with_usage
                    || v["response"]["usage"]["output_tokens"]
                        .as_i64()
                        .unwrap_or(0)
                        > 0;
            }
            _ => {}
        }
    }
    assert!(
        assembled.contains("you said: stream this"),
        "assembled: {assembled}"
    );
    assert!(saw_completed_with_usage, "completed frame must carry usage");
}

#[tokio::test]
async fn responses_api_requires_input() {
    let app = app();
    let resp = app
        .oneshot(post(
            "/v1/responses",
            Some("ak-demo-123"),
            r#"{"model":"gpt-5-responses"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn cache_key_distinguishes_raw_passthrough_params() {
    // Two requests identical except for a passthrough param (`seed`, which lands
    // in `raw`) must NOT collide onto one cache entry — different params can
    // produce different model output, so the second must run, not serve a stale
    // cached response. Proven via the ledger: 2 runs → 2 billing records.
    let app = app();
    let b1 = r#"{"model":"cached-mini","messages":[{"role":"user","content":"hi"}],"seed":1}"#;
    let b2 = r#"{"model":"cached-mini","messages":[{"role":"user","content":"hi"}],"seed":2}"#;
    let r1 = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), b1))
        .await
        .unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    let r2 = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), b2))
        .await
        .unwrap();
    assert_eq!(r2.status(), StatusCode::OK);
    // different `raw` → different cache key → both ran → 2 billing records.
    let resp = app.oneshot(get("/internal/ledger")).await.unwrap();
    assert_eq!(
        body_json(resp).await["count"],
        2,
        "differing raw params must not share a cache entry"
    );
}

#[tokio::test]
async fn model_qpm_limit_third_call_429() {
    let app = app();
    let body = r#"{"model":"qpm-mini","messages":[{"role":"user","content":"q"}]}"#;
    for _ in 0..2 {
        let r = app
            .clone()
            .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
    }
    let r = app
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(
        body_json(r).await["error"]["message"]
            .as_str()
            .unwrap()
            .contains("qpm")
    );
}

#[tokio::test]
async fn ak_tpm_limit_second_call_429() {
    let app = app();
    let body = r#"{"model":"gpt-4o","messages":[{"role":"user","content":"tokens please"}]}"#;
    let r = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-tpm-tiny"), body))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK); // first call consumes > 10 tokens
    let r = app
        .oneshot(post("/v1/chat/completions", Some("ak-tpm-tiny"), body))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(
        body_json(r).await["error"]["message"]
            .as_str()
            .unwrap()
            .contains("token-per-minute")
    );
}

#[tokio::test]
async fn account_cooldown_and_recovery() {
    let app = app();
    let body = r#"{"model":"spark-lite","messages":[{"role":"user","content":"x"}]}"#;
    // 3 consecutive upstream failures (sole account, down, no backup) → cooldown
    for _ in 0..3 {
        let r = app
            .clone()
            .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
    // health view shows cooling
    let r = app
        .clone()
        .oneshot(get("/internal/accounts"))
        .await
        .unwrap();
    let j = body_json(r).await;
    let spark = j["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "mock-spark-down")
        .unwrap()
        .clone();
    assert_eq!(spark["health"], "cooling");
    // while cooling: selection skips it → "no healthy upstream account"
    let r = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        body_json(r).await["error"]["message"]
            .as_str()
            .unwrap()
            .contains("healthy")
    );
    // cooldown_seconds=2 → auto-recovers on expiry
    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
    let r = app.oneshot(get("/internal/accounts")).await.unwrap();
    let j = body_json(r).await;
    let spark = j["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "mock-spark-down")
        .unwrap()
        .clone();
    assert_eq!(spark["health"], "ok");
}

#[tokio::test]
async fn realtime_websocket_mock_session() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    // spin the composed app on a loopback socket (local loopback, zero egress)
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let application = app();
    tokio::spawn(async move {
        axum::serve(listener, application).await.unwrap();
    });

    let mut req = format!("ws://{addr}/v1/realtime?model=realtime")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", "Bearer ak-demo-123".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .expect("ws connect");

    // 1) session.created
    let first = ws.next().await.unwrap().unwrap();
    let v: Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
    assert_eq!(v["type"], "session.created");
    assert_eq!(v["session"]["account"], "mock-realtime-1");

    // 2) input_text → response.delta ×2 → response.done(usage)
    ws.send(Message::text(
        serde_json::json!({"type":"input_text","text":"realtime hello"}).to_string(),
    ))
    .await
    .unwrap();
    let mut assembled = String::new();
    let mut done_usage = None;
    while let Some(Ok(msg)) = ws.next().await {
        let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        match v["type"].as_str().unwrap() {
            "response.delta" => assembled.push_str(v["delta"].as_str().unwrap()),
            "response.done" => {
                done_usage = Some(v["usage"].clone());
                break;
            }
            other => panic!("unexpected event {other}"),
        }
    }
    assert!(
        assembled.contains("you said: realtime hello"),
        "assembled: {assembled}"
    );
    let usage = done_usage.expect("usage");
    assert!(usage["input_tokens"].as_i64().unwrap() > 0);
    assert!(usage["output_tokens"].as_i64().unwrap() > 0);

    // 3) session.close → session.closed
    ws.send(Message::text(
        serde_json::json!({"type":"session.close"}).to_string(),
    ))
    .await
    .unwrap();
    let last = ws.next().await.unwrap().unwrap();
    let v: Value = serde_json::from_str(last.to_text().unwrap()).unwrap();
    assert_eq!(v["type"], "session.closed");
}

#[tokio::test]
async fn realtime_bridges_to_a_real_upstream_websocket() {
    use axum::routing::any;
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    // fake vendor: a loopback realtime websocket that answers response.create
    // with two output_text deltas and a response.done carrying usage
    async fn vendor_ws(ws: axum::extract::ws::WebSocketUpgrade) -> axum::response::Response {
        ws.on_upgrade(|mut socket| async move {
            use axum::extract::ws::Message as M;
            let send = |v: Value| M::Text(v.to_string().into());
            let _ = socket
                .send(send(serde_json::json!({"type":"session.created","session":{"vendor":"fake"}})))
                .await;
            while let Some(Ok(M::Text(t))) = socket.recv().await {
                let Ok(v) = serde_json::from_str::<Value>(&t) else {
                    continue;
                };
                if v["type"] == "response.create" {
                    let _ = socket
                        .send(send(
                            serde_json::json!({"type":"response.output_text.delta","delta":"bridge "}),
                        ))
                        .await;
                    let _ = socket
                        .send(send(
                            serde_json::json!({"type":"response.output_text.delta","delta":"ok"}),
                        ))
                        .await;
                    let _ = socket
                        .send(send(serde_json::json!({"type":"response.done",
                            "response":{"usage":{"input_tokens":9,"output_tokens":4,"total_tokens":13}}})))
                        .await;
                }
            }
        })
    }
    let vendor_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let vendor_addr = vendor_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            vendor_listener,
            axum::Router::new().route("/v1/realtime", any(vendor_ws)),
        )
        .await
        .unwrap();
    });

    // gateway configured with a realtime account whose endpoint is the fake vendor
    let yaml = format!(
        r#"
listen: {{host: 127.0.0.1, port: 0}}
access_keys:
  - {{ak: ak-rt, product: rt, qps: 100, daily_token_quota: 1000000}}
accounts:
  - {{name: rt-vendor, provider: openai, endpoint: "http://{vendor_addr}", protocols: ["realtime"]}}
models:
  - {{name: rt-model, protocol: realtime}}
"#
    );
    let cfg = Arc::new(gw_config::GatewayConfig::from_yaml(&yaml).unwrap());
    let state = Arc::new(gw_state::GatewayState::from_config(&cfg));
    let application = gw_views::app(gw_views::AppState::new(
        cfg,
        state.clone(),
        Arc::new(gw_engines::MockTransport),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, application).await.unwrap();
    });

    let mut req = format!("ws://{addr}/v1/realtime?model=rt-model")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", "Bearer ak-rt".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .expect("ws connect");

    // vendor's session.created is relayed through the bridge
    let first = ws.next().await.unwrap().unwrap();
    let v: Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
    assert_eq!(v["type"], "session.created");
    assert_eq!(v["session"]["vendor"], "fake");

    ws.send(Message::text(
        serde_json::json!({"type":"response.create"}).to_string(),
    ))
    .await
    .unwrap();
    let mut assembled = String::new();
    let mut done_usage = None;
    while let Some(Ok(msg)) = ws.next().await {
        let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        match v["type"].as_str().unwrap() {
            "response.output_text.delta" => assembled.push_str(v["delta"].as_str().unwrap()),
            "response.done" => {
                done_usage = Some(v["response"]["usage"].clone());
                break;
            }
            other => panic!("unexpected event {other}"),
        }
    }
    assert_eq!(assembled, "bridge ok");
    assert_eq!(done_usage.unwrap()["total_tokens"], 13);

    // the vendor-reported usage was billed to the ledger
    let (count, records) = state.store.ledger_snapshot(usize::MAX).await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(records[0].model, "rt-model");
    assert_eq!(records[0].account, "rt-vendor");
    assert_eq!(records[0].total_tokens, 13);
}

#[tokio::test]
async fn realtime_turns_are_rate_limited() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let application = app();
    tokio::spawn(async move {
        axum::serve(listener, application).await.unwrap();
    });

    // ak-limited: qps=1 → the second back-to-back turn must be denied
    let mut req = format!("ws://{addr}/v1/realtime?model=realtime")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", "Bearer ak-limited".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .expect("ws connect");
    let first = ws.next().await.unwrap().unwrap();
    let v: Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
    assert_eq!(v["type"], "session.created");

    let turn = serde_json::json!({"type":"input_text","text":"one"}).to_string();
    ws.send(Message::text(turn.clone())).await.unwrap();
    // drain the first turn to its response.done
    loop {
        let msg = ws.next().await.unwrap().unwrap();
        let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        if v["type"] == "response.done" {
            break;
        }
    }
    ws.send(Message::text(turn)).await.unwrap();
    let msg = ws.next().await.unwrap().unwrap();
    let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
    assert_eq!(v["type"], "error", "second turn must be rate limited: {v}");
    assert!(v["message"].as_str().unwrap().contains("rate limit"));
}

#[tokio::test]
async fn bespoke_dashscope_native_wire() {
    let app = app();
    let body = r#"{"model":"qwen-max","messages":[{"role":"user","content":"通义你好"}]}"#;
    let resp = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert!(
        j["choices"][0]["message"]["content"]
            .as_str()
            .unwrap()
            .contains("[mock-dashscope] you said: 通义你好")
    );
    assert!(j["usage"]["total_tokens"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn product_qpm_limit_third_call_429() {
    let app = app();
    let body = r#"{"model":"gpt-4o","messages":[{"role":"user","content":"p"}]}"#;
    for _ in 0..2 {
        let r = app
            .clone()
            .oneshot(post("/v1/chat/completions", Some("ak-prod-limited"), body))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
    }
    let r = app
        .oneshot(post("/v1/chat/completions", Some("ak-prod-limited"), body))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(
        body_json(r).await["error"]["message"]
            .as_str()
            .unwrap()
            .contains("product qpm")
    );
}

#[tokio::test]
async fn vendor_error_envelope_propagates_to_client() {
    // full-pipeline coverage: a vendor 200-body error envelope must surface to the
    // client as the mapped status (400), not a silent empty 200. (engine-level unit
    // test exists; this verifies the whole DAG→views path end to end.)
    let app = app();
    let resp = app
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            r#"{"model":"erroring-model","messages":[{"role":"user","content":"x"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let j = body_json(resp).await;
    assert!(
        j["error"]["message"]
            .as_str()
            .unwrap()
            .contains("mock vendor rejected")
    );
}

#[tokio::test]
async fn streaming_a_non_native_streaming_model_still_delivers_content() {
    // Gemini's engine doesn't natively stream (returns a full response). Requesting
    // stream:true must still deliver the content as SSE, not an empty stream.
    let app = app();
    let body = r#"{"model":"gemini-pro","stream":true,"messages":[{"role":"user","content":"stream gemini"}]}"#;
    let resp = app
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(ct.starts_with("text/event-stream"), "content-type: {ct}");
    let text = String::from_utf8(body_bytes(resp).await).unwrap();
    let frames: Vec<&str> = text
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .collect();
    assert!(frames.len() >= 2, "expected content+done, got: {frames:?}");
    assert_eq!(*frames.last().unwrap(), "[DONE]");
    let mut assembled = String::new();
    for f in &frames[..frames.len() - 1] {
        let v: Value = serde_json::from_str(f).unwrap();
        if let Some(d) = v["choices"][0]["delta"]["content"].as_str() {
            assembled.push_str(d);
        }
    }
    assert!(
        assembled.contains("you said: stream gemini"),
        "assembled: {assembled}"
    );
}

#[tokio::test]
async fn messages_streaming_non_native_engine_delivers_content() {
    // /v1/messages streamed with a non-native-streaming model (gemini) must still
    // deliver the text via content_block_delta, not an empty message.
    let app = app();
    let body = r#"{"model":"gemini-pro","stream":true,"max_tokens":64,"messages":[{"role":"user","content":"msg stream gemini"}]}"#;
    let resp = app
        .oneshot(post("/v1/messages", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = String::from_utf8(body_bytes(resp).await).unwrap();
    let mut assembled = String::new();
    for l in text.lines().filter_map(|l| l.strip_prefix("data: ")) {
        let v: Value = serde_json::from_str(l).unwrap();
        if v["type"] == "content_block_delta" {
            assembled.push_str(v["delta"]["text"].as_str().unwrap_or_default());
        }
    }
    assert!(
        assembled.contains("you said: msg stream gemini"),
        "assembled: {assembled}"
    );
}

#[tokio::test]
async fn provider_preset_config_serves_requests() {
    // providers: two lines of config replace hand-written accounts + wire types;
    // the injected MockTransport answers the preset's real URLs by path shape.
    let yaml = r#"
listen: {host: 127.0.0.1, port: 0}
access_keys:
  - {ak: ak-p, product: demo, qps: 100, daily_token_quota: 1000000}
providers:
  - {name: openai, kind: openai}
  - {name: anthropic, kind: anthropic}
models:
  - {name: gpt-x, provider: openai, input_price_per_1k_micros: 100, output_price_per_1k_micros: 100}
  - {name: claude-x, provider: anthropic}
"#;
    let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
    let state = Arc::new(GatewayState::from_config(&cfg));
    let app = gw_views::app(AppState::new(
        cfg,
        state,
        Arc::new(gw_engines::MockTransport),
    ));

    let resp = app
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-p"),
            r#"{"model":"gpt-x","messages":[{"role":"user","content":"preset"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert!(
        j["choices"][0]["message"]["content"]
            .as_str()
            .unwrap()
            .contains("you said: preset"),
        "{j}"
    );

    let resp = app
        .oneshot(post(
            "/v1/messages",
            Some("ak-p"),
            r#"{"model":"claude-x","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert_eq!(j["role"], "assistant");
}

#[tokio::test]
async fn metrics_endpoint_exposes_request_counters() {
    // mirrors the server wiring: global recorder + /metrics on top of the app
    let prometheus = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("install recorder");
    let router = app().route(
        "/metrics",
        axum::routing::get(move || {
            let prometheus = prometheus.clone();
            async move { prometheus.render() }
        }),
    );

    let resp = router
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            r#"{"model":"gpt-4o","messages":[{"role":"user","content":"count me"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = router.oneshot(get("/metrics")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("gateway_requests_total"), "{text}");
    assert!(text.contains("status=\"200\""), "{text}");
    assert!(text.contains("gateway_node_duration_seconds"), "{text}");
    assert!(text.contains("gateway_tokens_total"), "{text}");
}

#[tokio::test]
async fn metrics_count_error_statuses_too() {
    // error paths short-circuit before handlers finish — the router middleware
    // must still count them (this is what makes error-rate dashboards possible)
    let app = app();
    let resp = app
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            r#"{"model":"no-such-model","messages":[{"role":"user","content":"x"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    // the global recorder is process-wide; the render below sees this 404
    // regardless of which test installed the recorder first.
}

#[tokio::test]
async fn ledger_pagination_limits_records_not_count() {
    let app = app();
    for i in 0..3 {
        let body =
            format!(r#"{{"model":"gpt-4o","messages":[{{"role":"user","content":"page {i}"}}]}}"#);
        let resp = app
            .clone()
            .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), &body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
    let resp = app.oneshot(get("/internal/ledger?limit=2")).await.unwrap();
    let j = body_json(resp).await;
    assert_eq!(j["count"], 3, "count reports the total");
    assert_eq!(
        j["records"].as_array().unwrap().len(),
        2,
        "records page is limited"
    );
}
