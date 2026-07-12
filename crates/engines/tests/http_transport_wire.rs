//! Real-socket wire verification for `HttpTransport` — a record/replay style test.
//!
//! Stands up a REAL axum HTTP server on a loopback port and drives `HttpTransport` (the
//! reqwest client) against it — proving the transport works over a real TCP
//! socket with real HTTP framing (JSON + SSE), not just that it compiles.
//!
//! Boundary (honest): this is a LOCAL server, not a real vendor. It verifies the
//! live transport machinery; byte-level alignment with real vendors
//! still needs real endpoints + credentials.

// test scaffolding — unwrap/expect allowed as in #[test] fns (clippy.toml can't reach helpers here)
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use ap_consts::Protocol;
use ap_engines::http_transport::{DispatchTransport, HttpTransport};
use ap_engines::transport::{Transport, UpstreamBody, UpstreamRequest};
use ap_engines::{ModelEngine, OpenAiEngine};
use ap_models::{ChatMsg, GatewayRequest, ModelParamV2};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};

/// Loopback server that answers OpenAI-shaped chat completions (JSON + SSE).
async fn spawn_vendor() -> String {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|Json(body): Json<Value>| async move {
            let stream = body["stream"].as_bool().unwrap_or(false);
            let user = body["messages"]
                .as_array()
                .and_then(|m| m.last())
                .and_then(|m| m["content"].as_str())
                .unwrap_or("")
                .to_owned();
            if stream {
                let sse = format!(
                    "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                    json!({"model":"srv","choices":[{"index":0,"delta":{"content":format!("srv:{user}")},"finish_reason":null}]}),
                    json!({"model":"srv","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],
                           "usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}),
                );
                axum::response::Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(axum::body::Body::from(sse))
                    .unwrap()
            } else {
                let payload = json!({
                    "id":"srv-1","object":"chat.completion","model":"srv",
                    "choices":[{"index":0,"message":{"role":"assistant","content":format!("srv:{user}")},"finish_reason":"stop"}],
                    "usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}
                });
                axum::response::Response::builder()
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(payload.to_string()))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn http_transport_json_over_real_socket() {
    let base = spawn_vendor().await;
    let transport = HttpTransport::new(Duration::from_secs(5)).unwrap();
    let req = UpstreamRequest {
        protocol: Protocol::OpenaiChat,
        method: "POST".into(),
        url: format!("{base}/v1/chat/completions"),
        headers: vec![("content-type".into(), "application/json".into())],
        body: json!({"model":"srv","messages":[{"role":"user","content":"wire"}]})
            .to_string()
            .into_bytes(),
        stream: false,
        account: "real-local".into(),
    };
    let resp = transport.send(req).await.expect("real http round trip");
    assert_eq!(resp.status, 200);
    let bytes = match resp.body {
        UpstreamBody::Json(b) => b,
        UpstreamBody::Sse(_) => panic!("expected json"),
    };
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["choices"][0]["message"]["content"], "srv:wire");
}

#[tokio::test]
async fn http_transport_sse_over_real_socket() {
    let base = spawn_vendor().await;
    let transport = HttpTransport::new(Duration::from_secs(5)).unwrap();
    let req = UpstreamRequest {
        protocol: Protocol::OpenaiChat,
        method: "POST".into(),
        url: format!("{base}/v1/chat/completions"),
        headers: vec![("content-type".into(), "application/json".into())],
        body: json!({"model":"srv","stream":true,"messages":[{"role":"user","content":"wire"}]})
            .to_string()
            .into_bytes(),
        stream: true,
        account: "real-local".into(),
    };
    let resp = transport.send(req).await.expect("real http round trip");
    assert_eq!(resp.status, 200);
    match resp.body {
        UpstreamBody::Sse(b) => {
            let text = String::from_utf8(b).unwrap();
            assert!(text.contains("data: "));
            assert!(text.contains("[DONE]"));
        }
        UpstreamBody::Json(_) => panic!("expected sse (content-type text/event-stream)"),
    }
}

#[tokio::test]
async fn dispatch_routes_mock_scheme_in_process_and_real_urls_over_http() {
    let base = spawn_vendor().await;
    let transport = DispatchTransport::new(Duration::from_secs(5)).unwrap();

    let req = |url: String| UpstreamRequest {
        protocol: Protocol::OpenaiChat,
        method: "POST".into(),
        url,
        headers: vec![("content-type".into(), "application/json".into())],
        body: json!({"model":"srv","messages":[{"role":"user","content":"route"}]})
            .to_string()
            .into_bytes(),
        stream: false,
        account: "dispatch-test".into(),
    };

    let resp = transport
        .send(req("mock://openai/v1/chat/completions".into()))
        .await
        .unwrap();
    let UpstreamBody::Json(b) = resp.body else {
        panic!("expected json")
    };
    let v: Value = serde_json::from_slice(&b).unwrap();
    assert!(
        v["choices"][0]["message"]["content"]
            .as_str()
            .unwrap()
            .starts_with("[mock-openai:"),
        "mock:// must stay in-process: {v}"
    );

    let resp = transport
        .send(req(format!("{base}/v1/chat/completions")))
        .await
        .unwrap();
    let UpstreamBody::Json(b) = resp.body else {
        panic!("expected json")
    };
    let v: Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["choices"][0]["message"]["content"], "srv:route");
}

#[tokio::test]
async fn engine_through_real_http_transport_end_to_end() {
    // GENUINE full path: OpenAiEngine builds the request → routes to the account's
    // configured endpoint (the local server) → HttpTransport sends over a real
    // socket → engine parses the reply. This is exactly the go-live path; the only
    // thing separating it from a real vendor is the endpoint URL + credentials in
    // account config.
    let base = spawn_vendor().await;
    let transport: ap_engines::SharedTransport =
        Arc::new(HttpTransport::new(Duration::from_secs(5)).unwrap());

    let account = ap_models::Account {
        name: "real-local".into(),
        provider: "local".into(),
        priority: 1,
        endpoint: base, // the one config field that makes it "real"
        ..Default::default()
    };
    let request = GatewayRequest {
        account: Some(account),
        message: vec![ChatMsg::text("user", "over the wire")],
        model_param_v2: Some(ModelParamV2::with_name(Protocol::OpenaiChat, "srv")),
        ..Default::default()
    };
    let engine = OpenAiEngine::new(request, transport);
    let out = engine.run().await.expect("engine over real http");
    assert_eq!(out.http_code, 200);
    assert_eq!(out.response.message, "srv:over the wire");
    assert_eq!(out.response.total_tokens, 7);
}
