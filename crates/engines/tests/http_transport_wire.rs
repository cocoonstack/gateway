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

use axum::routing::post;
use axum::{Json, Router};
use gw_consts::Protocol;
use gw_engines::http_transport::{DispatchTransport, HttpTransport};
use gw_engines::transport::{Transport, UpstreamBody, UpstreamRequest};
use gw_engines::{ModelEngine, OpenAiEngine};
use gw_models::{ChatMsg, GatewayRequest, ModelParamV2};
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
    let UpstreamBody::Json(bytes) = resp.body else {
        panic!("expected json")
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
    // live SSE arrives as a stream now; buffering it exercises the drain path
    assert!(matches!(resp.body, UpstreamBody::SseStream(_)));
    let resp = resp.buffered().await.expect("drain live sse");
    let UpstreamBody::Sse(b) = resp.body else {
        panic!("expected buffered sse")
    };
    let text = String::from_utf8(b).unwrap();
    assert!(text.contains("data: "));
    assert!(text.contains("[DONE]"));
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
    let transport: gw_engines::SharedTransport =
        Arc::new(HttpTransport::new(Duration::from_secs(5)).unwrap());

    let account = gw_models::Account {
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

#[tokio::test]
async fn per_account_policy_and_connect_retry() {
    use std::collections::HashMap;

    use gw_engines::http_transport::UpstreamPolicy;

    let mut per_account = HashMap::new();
    per_account.insert(
        "tight".to_owned(),
        UpstreamPolicy {
            timeout: Duration::from_secs(5),
            connect_retries: 2,
        },
    );
    let transport = HttpTransport::with_policies(UpstreamPolicy::default(), per_account).unwrap();
    assert_eq!(transport.policy_for("tight").connect_retries, 2);
    assert_eq!(transport.policy_for("other").connect_retries, 1);

    // grab a port and close it: connect must fail, retry twice (100+200ms
    // backoff), then surface a 502 — an in-flight request is never replayed.
    let closed = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    let started = std::time::Instant::now();
    let err = transport
        .send(UpstreamRequest {
            protocol: Protocol::OpenaiChat,
            method: "POST".into(),
            url: format!("http://{closed}/v1/chat/completions"),
            headers: vec![],
            body: b"{}".to_vec(),
            stream: false,
            account: "tight".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.http_status, 502);
    assert!(
        started.elapsed() >= Duration::from_millis(300),
        "two backoffs must have elapsed: {:?}",
        started.elapsed()
    );
}

/// A client that disconnects mid-stream must surface as 499 (below the 5xx
/// failover threshold) so the DAG never re-bills or faults the account.
#[tokio::test]
async fn client_disconnect_midstream_is_499_not_500() {
    use futures::StreamExt;
    use gw_engines::transport::UpstreamResponse;

    #[derive(Debug)]
    struct StreamTransport;
    #[async_trait::async_trait]
    impl Transport for StreamTransport {
        async fn send(&self, _req: UpstreamRequest) -> gw_models::GResult<UpstreamResponse> {
            let frames = vec![
                Ok(bytes::Bytes::from(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
                )),
                Ok(bytes::Bytes::from(
                    "data: {\"choices\":[{\"delta\":{\"content\":\" there\"}}]}\n\n",
                )),
            ];
            Ok(UpstreamResponse {
                status: 200,
                body: UpstreamBody::SseStream(futures::stream::iter(frames).boxed()),
            })
        }
    }

    // a stream_tx whose receiver is dropped immediately = a gone client
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    drop(rx);
    let mut request = GatewayRequest {
        message: vec![ChatMsg::text("user", "hi")],
        model_param_v2: Some(ModelParamV2::with_name(Protocol::OpenaiChat, "gpt")),
        stream: true,
        ..Default::default()
    };
    request.stream_tx = Some(tx);

    let err = OpenAiEngine::new(request, Arc::new(StreamTransport))
        .run()
        .await
        .unwrap_err();
    assert_eq!(err.http_status, 499, "disconnect must not look like a 5xx");
}

/// A transport error AFTER the first chunk reached the client aborts (committed,
/// billed from delivered content) so failover never splices a second generation.
#[tokio::test]
async fn midstream_upstream_error_after_send_aborts_without_failover() {
    use futures::StreamExt;
    use gw_engines::transport::UpstreamResponse;

    #[derive(Debug)]
    struct FlakyStream;
    #[async_trait::async_trait]
    impl Transport for FlakyStream {
        async fn send(&self, _req: UpstreamRequest) -> gw_models::GResult<UpstreamResponse> {
            let frames: Vec<Result<bytes::Bytes, String>> = vec![
                Ok(bytes::Bytes::from(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
                )),
                Err("connection reset".to_string()),
            ];
            Ok(UpstreamResponse {
                status: 200,
                body: UpstreamBody::SseStream(futures::stream::iter(frames).boxed()),
            })
        }
    }

    // a live receiver that stays open (client still connected)
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let mut request = GatewayRequest {
        message: vec![ChatMsg::text("user", "hi")],
        model_param_v2: Some(ModelParamV2::with_name(Protocol::OpenaiChat, "gpt")),
        stream: true,
        ..Default::default()
    };
    request.stream_tx = Some(tx);

    // committed response: no error, no failover — the delivered part is kept
    // (and billed from) as an aborted outcome
    let out = OpenAiEngine::new(request, Arc::new(FlakyStream))
        .run()
        .await
        .unwrap();
    assert!(out.response.aborted, "post-send break must mark aborted");
    assert!(out.streamed_live);
    assert_eq!(out.response.message, "partial");
}

/// A well-formed vendor error frame (Ok bytes, not a transport error) AFTER a
/// chunk reached the client must also abort without failover — otherwise a
/// retry splices a second generation onto the committed stream.
#[tokio::test]
async fn vendor_error_frame_after_send_aborts_without_failover() {
    use futures::StreamExt;
    use gw_engines::transport::UpstreamResponse;

    #[derive(Debug)]
    struct ErrorFrameStream;
    #[async_trait::async_trait]
    impl Transport for ErrorFrameStream {
        async fn send(&self, _req: UpstreamRequest) -> gw_models::GResult<UpstreamResponse> {
            let frames: Vec<Result<bytes::Bytes, String>> = vec![
                Ok(bytes::Bytes::from(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
                )),
                // a vendor overload error delivered as a normal SSE frame
                Ok(bytes::Bytes::from(
                    "data: {\"error\":{\"message\":\"overloaded\"}}\n\n",
                )),
            ];
            Ok(UpstreamResponse {
                status: 200,
                body: UpstreamBody::SseStream(futures::stream::iter(frames).boxed()),
            })
        }
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let mut request = GatewayRequest {
        message: vec![ChatMsg::text("user", "hi")],
        model_param_v2: Some(ModelParamV2::with_name(Protocol::OpenaiChat, "gpt")),
        stream: true,
        ..Default::default()
    };
    request.stream_tx = Some(tx);

    let out = OpenAiEngine::new(request, Arc::new(ErrorFrameStream))
        .run()
        .await
        .expect("committed vendor error must not be a failover-eligible Err");
    assert!(out.response.aborted, "vendor error after send must abort");
    assert!(out.streamed_live);
    assert_eq!(out.response.message, "partial");
}
