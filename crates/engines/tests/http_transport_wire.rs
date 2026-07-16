//! Real-socket wire verification for `HttpTransport`: a REAL axum server on a
//! loopback port, driven over real TCP with real HTTP framing (JSON + SSE).
//! Boundary: a local server, not a real vendor — byte-level vendor alignment
//! still needs real endpoints + credentials.

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
    let base = spawn_vendor().await;
    let transport: gw_engines::SharedTransport =
        Arc::new(HttpTransport::new(Duration::from_secs(5)).unwrap());

    let account = gw_models::Account {
        name: "real-local".into(),
        provider: "local".into(),
        priority: 1,
        endpoint: base,
        ..Default::default()
    };
    let request = GatewayRequest {
        account: Some(Arc::new(account)),
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

    let mut reloaded = HashMap::new();
    reloaded.insert(
        "tight".to_owned(),
        UpstreamPolicy {
            timeout: Duration::from_secs(9),
            connect_retries: 5,
        },
    );
    Transport::reload_policies(&transport, UpstreamPolicy::default(), reloaded);
    assert_eq!(transport.policy_for("tight").connect_retries, 5);
    assert_eq!(
        transport.policy_for("tight").timeout,
        Duration::from_secs(9)
    );
    assert_eq!(
        transport.policy_for("other").connect_retries,
        1,
        "an account dropped from the reload falls back to the default"
    );

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

async fn spawn_paced_vendor(frames: usize, gap: Duration) -> String {
    let app = Router::new().route(
        "/paced",
        post(move || async move {
            let sse = futures::stream::unfold(0usize, move |i| async move {
                if i > frames {
                    return None;
                }
                tokio::time::sleep(gap).await;
                let frame = if i == frames {
                    "data: [DONE]\n\n".to_owned()
                } else {
                    format!("data: {{\"n\":{i}}}\n\n")
                };
                Some((Ok::<_, std::convert::Infallible>(frame), i + 1))
            });
            axum::response::Response::builder()
                .header("content-type", "text/event-stream")
                .body(axum::body::Body::from_stream(sse))
                .unwrap()
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}/paced")
}

fn paced_req(url: String) -> UpstreamRequest {
    UpstreamRequest {
        protocol: Protocol::OpenaiChat,
        method: "POST".into(),
        url,
        headers: vec![("content-type".into(), "application/json".into())],
        body: b"{}".to_vec(),
        stream: true,
        account: "paced".into(),
    }
}

#[tokio::test]
async fn slow_stream_outlives_the_total_policy_timeout() {
    let url = spawn_paced_vendor(5, Duration::from_millis(300)).await;
    let transport = HttpTransport::new(Duration::from_secs(1)).unwrap();
    let resp = transport.send(paced_req(url)).await.unwrap();
    let resp = resp
        .buffered()
        .await
        .expect("a 1.8s stream must survive a 1s policy timeout");
    let UpstreamBody::Sse(b) = resp.body else {
        panic!("expected buffered sse")
    };
    let text = String::from_utf8(b).unwrap();
    assert!(
        text.contains(r#"{"n":4}"#) && text.contains("[DONE]"),
        "{text}"
    );
}

#[tokio::test]
async fn stalled_stream_errors_at_the_idle_gap_instead_of_hanging() {
    let url = spawn_paced_vendor(2, Duration::from_secs(20)).await;
    let transport = HttpTransport::new(Duration::from_millis(300)).unwrap();
    let started = std::time::Instant::now();
    let err = match transport.send(paced_req(url)).await {
        Ok(resp) => resp
            .buffered()
            .await
            .expect_err("stalled stream must error"),
        Err(e) => e,
    };
    assert_eq!(err.http_status, 502);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "must fail at the idle gap, not wait out the vendor: {:?}",
        started.elapsed()
    );
}

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

    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let mut request = GatewayRequest {
        message: vec![ChatMsg::text("user", "hi")],
        model_param_v2: Some(ModelParamV2::with_name(Protocol::OpenaiChat, "gpt")),
        stream: true,
        ..Default::default()
    };
    request.stream_tx = Some(tx);

    let out = OpenAiEngine::new(request, Arc::new(FlakyStream))
        .run()
        .await
        .unwrap();
    assert!(out.response.aborted, "post-send break must mark aborted");
    assert!(out.streamed_live);
    assert_eq!(out.response.message, "partial");
}

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
