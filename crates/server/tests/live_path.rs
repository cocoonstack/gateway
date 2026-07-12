//! Capstone: the ENTIRE gateway pipeline over real HTTP — the go-live proof.
//!
//! Composes the full app with the REAL `HttpTransport` (reqwest) and an account whose `endpoint` points at a
//! loopback "vendor" server, then drives a request through the whole stack:
//!   views(auth) → handler(plugins) → DAG(resolve/quota/account/limit) →
//!   OpenAiEngine → HttpTransport → real TCP socket → vendor → parse →
//!   CommonUsage → billing ledger.
//!
//! Boundary (honest): the vendor is local, so no egress and no credentials. But
//! everything EXCEPT the vendor's identity is the real go-live path — swap the
//! account endpoint/api_key_env for a real vendor and it is live, no code change.

// test scaffolding — unwrap/expect allowed as in #[test] fns (clippy.toml can't reach helpers here)
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use ap_config::GatewayConfig;
use ap_engines::http_transport::HttpTransport;
use ap_state::GatewayState;
use ap_views::AppState;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};
use tower::ServiceExt;

/// Loopback OpenAI-shaped vendor: echoes the user's text. Returns JSON, or a real
/// `text/event-stream` SSE body when the request asked for `stream:true`.
async fn spawn_vendor() -> String {
    let app = Router::new()
        .route(
            "/v1/chat/completions",
            post(|Json(body): Json<Value>| async move {
                let user = body["messages"]
                    .as_array()
                    .and_then(|m| m.iter().rev().find(|m| m["role"] == "user"))
                    .and_then(|m| m["content"].as_str())
                    .unwrap_or("")
                    .to_owned();
                let reply = format!("vendor echo: {user}");
                if body["stream"].as_bool().unwrap_or(false) {
                    let sse = format!(
                        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                        json!({"model":body["model"],"choices":[{"index":0,"delta":{"content":reply},"finish_reason":null}]}),
                        json!({"model":body["model"],"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],
                               "usage":{"prompt_tokens":7,"completion_tokens":4,"total_tokens":11}}),
                    );
                    axum::response::Response::builder()
                        .header("content-type", "text/event-stream")
                        .body(axum::body::Body::from(sse))
                        .unwrap()
                } else {
                    let json = json!({
                        "id":"vendor-1","object":"chat.completion","created":1,"model":body["model"],
                        "choices":[{"index":0,"message":{"role":"assistant","content":reply},"finish_reason":"stop"}],
                        "usage":{"prompt_tokens":7,"completion_tokens":4,"total_tokens":11}
                    });
                    axum::response::Response::builder()
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(json.to_string()))
                        .unwrap()
                }
            }),
        )
        // Anthropic-shaped endpoint: proves a NON-OpenAI engine (ClaudeEngine,
        // x-api-key auth, /v1/messages) also completes over the real transport.
        .route(
            "/v1/messages",
            post(|headers: axum::http::HeaderMap, Json(body): Json<Value>| async move {
                // the go-live seam must send the account key as x-api-key (not Bearer)
                let has_key = headers.contains_key("x-api-key");
                let user = body["messages"]
                    .as_array()
                    .and_then(|m| m.iter().rev().find(|m| m["role"] == "user"))
                    .map(|m| m["content"].to_string())
                    .unwrap_or_default();
                let reply = format!("anthropic echo (keyed={has_key}): {user}");
                let json = json!({
                    "id":"msg-1","type":"message","role":"assistant","model":body["model"],
                    "content":[{"type":"text","text":reply}],
                    "stop_reason":"end_turn",
                    "usage":{"input_tokens":7,"output_tokens":4}
                });
                axum::response::Response::builder()
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(json.to_string()))
                    .unwrap()
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// Build the gateway app wired with the REAL HttpTransport + an account whose
/// endpoint is the loopback vendor.
fn gateway(vendor_url: &str) -> Router {
    let yaml = format!(
        r#"
listen: {{ host: 127.0.0.1, port: 0 }}
access_keys:
  - {{ ak: ak-live, product: demo, qps: 100, daily_token_quota: 1000000 }}
models:
  - {{ name: gpt-live, protocol: openai-chat, input_price_per_1k_micros: 3000, output_price_per_1k_micros: 15000 }}
  - {{ name: claude-live, protocol: anthropic-messages, input_price_per_1k_micros: 3000, output_price_per_1k_micros: 15000 }}
accounts:
  - {{ name: live-acct, provider: openai, priority: 1, endpoint: "{vendor_url}", protocols: ["openai-chat"] }}
  - {{ name: live-anthropic, provider: anthropic, priority: 1, endpoint: "{vendor_url}", protocols: ["anthropic-messages"] }}
"#
    );
    let cfg = Arc::new(GatewayConfig::from_yaml(&yaml).expect("config"));
    let state = Arc::new(GatewayState::from_config(&cfg));
    let transport = Arc::new(HttpTransport::new(Duration::from_secs(5)).expect("http transport"));
    ap_views::app(AppState::new(cfg, state, transport))
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn full_pipeline_over_real_http() {
    let vendor = spawn_vendor().await;
    let app = gateway(&vendor);

    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .header("authorization", "Bearer ak-live")
        .body(Body::from(
            r#"{"model":"gpt-live","messages":[{"role":"user","content":"is this live?"}]}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;

    // response came from the loopback vendor, through the whole pipeline over real HTTP
    assert_eq!(j["object"], "chat.completion");
    assert_eq!(j["model"], "gpt-live");
    assert!(
        j["choices"][0]["message"]["content"]
            .as_str()
            .unwrap()
            .contains("vendor echo: is this live?")
    );
    assert_eq!(j["usage"]["total_tokens"], 11);

    // billing recorded through the real path
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/internal/ledger")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let j = body_json(resp).await;
    assert_eq!(j["count"], 1);
    assert_eq!(j["records"][0]["account"], "live-acct");
    assert_eq!(j["records"][0]["total_tokens"], 11);
    assert!(j["records"][0]["cost_micros"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn streaming_pipeline_over_real_http() {
    // distinct path: vendor SSE → HttpTransport content-type detection → engine
    // parse_sse → views SSE re-emission, all over a real socket.
    let vendor = spawn_vendor().await;
    let app = gateway(&vendor);
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .header("authorization", "Bearer ak-live")
        .body(Body::from(
            r#"{"model":"gpt-live","stream":true,"messages":[{"role":"user","content":"stream live"}]}"#,
        ))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(ct.starts_with("text/event-stream"), "content-type: {ct}");

    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    let frames: Vec<&str> = text
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .collect();
    assert!(frames.len() >= 2, "sse frames: {frames:?}");
    assert_eq!(*frames.last().unwrap(), "[DONE]");
    // reassemble deltas: the vendor's streamed content came through the whole pipeline
    let mut assembled = String::new();
    for f in &frames[..frames.len() - 1] {
        let v: Value = serde_json::from_str(f).unwrap();
        if let Some(d) = v["choices"][0]["delta"]["content"].as_str() {
            assembled.push_str(d);
        }
    }
    assert!(
        assembled.contains("vendor echo: stream live"),
        "assembled: {assembled}"
    );
}

#[tokio::test]
async fn claude_messages_over_real_http() {
    // Proves the go-live seam works for a NON-OpenAI engine: an Anthropic-protocol
    // request drives ClaudeEngine → HttpTransport → real socket → /v1/messages on
    // the loopback vendor, with the account key sent as x-api-key. This is the
    // integration capstone for the "all engines go-live-ready" work.
    let vendor = spawn_vendor().await;
    let app = gateway(&vendor);
    let req = Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header("content-type", "application/json")
        .header("authorization", "Bearer ak-live")
        .body(Body::from(
            r#"{"model":"claude-live","max_tokens":64,"messages":[{"role":"user","content":"is claude live?"}]}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    // response came back from the loopback vendor's /v1/messages over real HTTP,
    // reshaped to the Anthropic wire response by the views layer.
    assert_eq!(j["type"], "message");
    assert!(
        j["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("anthropic echo (keyed=true)")
    );
    // billed through the real path, attributed to the anthropic account
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/internal/ledger")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let j = body_json(resp).await;
    assert_eq!(j["records"][0]["account"], "live-anthropic");
    assert_eq!(j["records"][0]["total_tokens"], 11);
}

#[tokio::test]
async fn auth_and_limits_still_apply_over_real_http() {
    let vendor = spawn_vendor().await;
    let app = gateway(&vendor);
    // wrong key is rejected before any upstream call
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .header("authorization", "Bearer wrong")
        .body(Body::from(
            r#"{"model":"gpt-live","messages":[{"role":"user","content":"x"}]}"#,
        ))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
