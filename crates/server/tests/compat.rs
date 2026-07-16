//! Protocol-compatibility diff against the upstream OpenAI/Anthropic wire:
//! canonical samples deserialize into our structs (inbound) and our live
//! responses diff against the canonical key sets (outbound). Fully offline.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

mod common;
use common::app;

fn keys(v: &Value) -> BTreeSet<String> {
    v.as_object()
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default()
}

const OPENAI_CHAT_CANONICAL: &str = r#"{
  "id": "chatcmpl-abc123",
  "object": "chat.completion",
  "created": 1700000000,
  "model": "gpt-4o",
  "choices": [{
    "index": 0,
    "message": {"role": "assistant", "content": "Hello there!"},
    "finish_reason": "stop"
  }],
  "usage": {"prompt_tokens": 9, "completion_tokens": 12, "total_tokens": 21}
}"#;

const ANTHROPIC_MSG_CANONICAL: &str = r#"{
  "id": "msg_01XFDUDYJgAACzvnptvVoYEL",
  "type": "message",
  "role": "assistant",
  "model": "claude-sonnet",
  "content": [{"type": "text", "text": "Hello!"}],
  "stop_reason": "end_turn",
  "usage": {"input_tokens": 12, "output_tokens": 6,
            "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0}
}"#;

#[test]
fn canonical_openai_parses_into_protocol_structs() {
    let resp: gw_protocol::openai::ChatCompletionResponse =
        serde_json::from_str(OPENAI_CHAT_CANONICAL).expect("canonical openai parses");
    assert_eq!(resp.object, "chat.completion");
    assert_eq!(resp.choices[0].message.role, "assistant");
    assert_eq!(resp.usage.total_tokens, 21);
}

#[test]
fn canonical_anthropic_parses_into_protocol_structs() {
    let resp: gw_protocol::anthropic::MessagesResponse =
        serde_json::from_str(ANTHROPIC_MSG_CANONICAL).expect("canonical anthropic parses");
    assert_eq!(resp.kind, "message");
    assert!(matches!(
        &resp.content[0],
        gw_protocol::anthropic::ContentBlock::Text { text } if text == "Hello!"
    ));
    assert_eq!(resp.usage.input_tokens, 12);
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&bytes).expect("json")
}

fn post(uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .header("authorization", "Bearer ak-demo-123")
        .body(Body::from(body.to_owned()))
        .expect("request")
}

#[tokio::test]
async fn live_chat_response_matches_canonical_key_sets() {
    let resp = app()
        .oneshot(post(
            "/v1/chat/completions",
            r#"{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ours = body_json(resp).await;
    let canon: Value = serde_json::from_str(OPENAI_CHAT_CANONICAL).unwrap();

    assert_eq!(keys(&ours), keys(&canon), "top-level keys diverge");
    assert_eq!(
        keys(&ours["choices"][0]),
        keys(&canon["choices"][0]),
        "choice keys diverge"
    );
    assert_eq!(
        keys(&ours["choices"][0]["message"]),
        keys(&canon["choices"][0]["message"]),
        "message keys diverge"
    );
    assert_eq!(
        keys(&ours["usage"]),
        keys(&canon["usage"]),
        "usage keys diverge"
    );
}

#[tokio::test]
async fn live_messages_response_matches_canonical_key_sets() {
    let resp = app()
        .oneshot(post(
            "/v1/messages",
            r#"{"model":"claude-sonnet","messages":[{"role":"user","content":"hi"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ours = body_json(resp).await;
    let canon: Value = serde_json::from_str(ANTHROPIC_MSG_CANONICAL).unwrap();

    assert_eq!(keys(&ours), keys(&canon), "top-level keys diverge");
    assert_eq!(
        keys(&ours["content"][0]),
        keys(&canon["content"][0]),
        "content keys diverge"
    );
    assert_eq!(
        keys(&ours["usage"]),
        keys(&canon["usage"]),
        "usage keys diverge"
    );
}

#[tokio::test]
async fn live_embeddings_response_matches_openai_shape() {
    let resp = app()
        .oneshot(post(
            "/v1/embeddings",
            r#"{"model":"text-embedding-3","input":"hi"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ours = body_json(resp).await;
    assert_eq!(
        keys(&ours),
        ["data", "model", "object", "usage"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    );
    assert_eq!(
        keys(&ours["data"][0]),
        ["embedding", "index", "object"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    );
}
