//! End-to-end round against the fully composed app (embedded config + in-process
//! state + MockTransport). Exercises the same wiring `main.rs` serves, one HTTP
//! call at a time: auth → resolve → quota → account → rate-limit → engine →
//! usage → billing. No network leaves the process (zero-egress default build).

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

mod common;
use common::app;

#[tokio::test]
async fn admin_audit_freezes_source_ip_before_a_trust_flip() {
    const V1: &str = r#"
listen: {host: 127.0.0.1, port: 0}
admin: {token_env: GW_TEST_ADMIN_TOKEN_TRUST}
trust_proxy_headers: false
access_keys: [{ak: ak-x, product: demo, qps: 100, daily_token_quota: 1000000}]
models: [{name: gpt-4o, protocol: openai-chat}]
accounts: [{name: mock-openai-1, provider: openai, protocols: ["openai-chat"]}]
"#;
    // SAFETY: unique var name for this test; no concurrent reader of it.
    unsafe { std::env::set_var("GW_TEST_ADMIN_TOKEN_TRUST", "s3cret") };
    let v1 = GatewayConfig::from_yaml(V1).unwrap();
    let v2_yaml = V1.replace("trust_proxy_headers: false", "trust_proxy_headers: true");
    let loader: gw_views::ConfigLoader = Arc::new(move || {
        let yaml = v2_yaml.clone();
        Box::pin(async move { GatewayConfig::from_yaml(&yaml).map_err(|e| e.to_string()) })
            as gw_views::ConfigFuture
    });
    let state = Arc::new(GatewayState::from_config(&v1));
    let shared = gw_state::SharedConfig::new(Arc::new(v1), state);
    let app = gw_views::app(gw_views::AppState::with_config(
        shared,
        Arc::new(gw_engines::MockTransport),
        Some(loader),
    ));

    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/reload")
                .header("authorization", "Bearer s3cret")
                .header("x-real-ip", "9.9.9.9")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    let ops = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/audit/ops")
                .header("authorization", "Bearer s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let j = body_json(ops).await;
    let reload = j["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["action"] == "reload")
        .expect("reload audited");
    assert_ne!(
        reload["source_ip"], "9.9.9.9",
        "the trust-enabling op must not trust its own forged header"
    );
}

#[tokio::test]
async fn admin_reload_is_gated_and_swaps_keys_live() {
    // an admin-less config keeps the whole admin surface indistinguishable
    // from a missing route (the shared app() now names an admin token env)
    let no_admin = GatewayConfig::from_yaml(
        "listen: {host: h, port: 0}\nmodels: [{name: m, protocol: openai-chat}]\naccounts: [{name: a, provider: openai, protocols: [openai-chat]}]\naccess_keys: [{ak: k, product: p, qps: 1, daily_token_quota: 1}]",
    )
    .unwrap();
    let no_admin_state = Arc::new(GatewayState::from_config(&no_admin));
    let r = gw_views::app(gw_views::AppState::new(
        Arc::new(no_admin),
        no_admin_state,
        Arc::new(gw_engines::MockTransport),
    ))
    .oneshot(
        Request::builder()
            .method("POST")
            .uri("/admin/reload")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);

    const V1: &str = r#"
listen: {host: 127.0.0.1, port: 0}
admin: {token_env: GW_TEST_ADMIN_TOKEN_E2E}
access_keys: [{ak: ak-v1, product: demo, qps: 100, daily_token_quota: 1000000}]
models: [{name: gpt-4o, protocol: openai-chat}]
accounts: [{name: mock-openai-1, provider: openai, protocols: ["openai-chat"]}]
"#;
    // SAFETY: unique var name for this test; no concurrent reader of it.
    unsafe { std::env::set_var("GW_TEST_ADMIN_TOKEN_E2E", "s3cret") };
    let v1 = GatewayConfig::from_yaml(V1).unwrap();
    let v2_yaml = V1.replace("ak-v1", "ak-v2");
    let loader: gw_views::ConfigLoader = Arc::new(move || {
        let yaml = v2_yaml.clone();
        Box::pin(async move { GatewayConfig::from_yaml(&yaml).map_err(|e| e.to_string()) })
            as gw_views::ConfigFuture
    });
    let state = Arc::new(GatewayState::from_config(&v1));
    let shared = gw_state::SharedConfig::new(Arc::new(v1), state);
    let app = gw_views::app(gw_views::AppState::with_config(
        shared,
        Arc::new(gw_engines::MockTransport),
        Some(loader),
    ));

    let chat = |ak: &str| {
        Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {ak}"))
            .body(Body::from(
                r#"{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}"#,
            ))
            .unwrap()
    };
    let reload = |token: Option<&str>| {
        let mut b = Request::builder().method("POST").uri("/admin/reload");
        if let Some(t) = token {
            b = b.header("authorization", format!("Bearer {t}"));
        }
        b.body(Body::empty()).unwrap()
    };

    assert_eq!(
        app.clone().oneshot(chat("ak-v1")).await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone().oneshot(chat("ak-v2")).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone().oneshot(reload(None)).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(reload(Some("wrong")))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(reload(Some("s3cret")))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone().oneshot(chat("ak-v2")).await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone().oneshot(chat("ak-v1")).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );

    let r = app
        .clone()
        .oneshot(admin(
            "POST",
            "/admin/keys",
            Some("s3cret"),
            Some(r#"{"ak":"ak-admin","product":"demo","qps":100,"daily_token_quota":1000000}"#),
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CREATED);
    assert_eq!(
        app.clone()
            .oneshot(chat("ak-admin"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK,
        "admin-created key works immediately"
    );
    assert_eq!(
        app.clone()
            .oneshot(admin(
                "POST",
                "/admin/keys",
                None,
                Some(r#"{"ak":"x","product":"y"}"#)
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );

    assert_eq!(
        app.clone()
            .oneshot(reload(Some("s3cret")))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone()
            .oneshot(chat("ak-admin"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK,
        "admin key survives a config reload"
    );

    let r = app
        .clone()
        .oneshot(admin(
            "DELETE",
            "/admin/keys/ak-admin",
            Some("s3cret"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(
        app.clone()
            .oneshot(chat("ak-admin"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED,
        "revoked key is rejected"
    );
    assert_eq!(
        app.clone()
            .oneshot(admin(
                "PATCH",
                "/admin/keys/ak-admin",
                Some("s3cret"),
                Some(r#"{"qps":5}"#),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );

    app.clone()
        .oneshot(admin(
            "POST",
            "/admin/keys",
            Some("s3cret"),
            Some(r#"{"ak":"ak-tpm","product":"demo","qps":100,"daily_token_quota":1000000,"tokens_per_minute":50}"#),
        ))
        .await
        .unwrap();
    let r = app
        .oneshot(admin(
            "PATCH",
            "/admin/keys/ak-tpm",
            Some("s3cret"),
            Some(r#"{"tokens_per_minute":5.5}"#),
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let j = body_json(r).await;
    assert_eq!(
        j["tokens_per_minute"], 50,
        "malformed tpm must leave the cap unchanged, not clear it"
    );
}

async fn body_bytes(resp: Response) -> Vec<u8> {
    axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body")
        .to_vec()
}

/// Serve `application` on an ephemeral local port; the bound address.
async fn serve_app(application: Router) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, application).await.unwrap();
    });
    addr
}

async fn body_json(resp: Response) -> Value {
    serde_json::from_slice(&body_bytes(resp).await).expect("json body")
}

#[tokio::test]
async fn anthropic_thinking_signature_exact_passes_tamper_is_local_400_and_miss_passes() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct ThinkingFixture {
        hits: Arc<AtomicUsize>,
        cross_protocol: Arc<std::sync::Mutex<Option<Value>>>,
    }

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for ThinkingFixture {
        async fn send(
            &self,
            request: gw_engines::transport::UpstreamRequest,
        ) -> gw_models::GResult<gw_engines::transport::UpstreamResponse> {
            self.hits.fetch_add(1, Ordering::Relaxed);
            let request: Value = serde_json::from_slice(&request.body).unwrap();
            if request.get("reasoning_effort").is_some() {
                *self.cross_protocol.lock().unwrap() = Some(request);
                return Ok(gw_engines::transport::UpstreamResponse {
                    status: 200,
                    body: gw_engines::transport::UpstreamBody::Json(
                        serde_json::to_vec(&json!({
                            "id":"chatcmpl-1","object":"chat.completion","model":"gpt-test",
                            "choices":[{"index":0,"message":{"role":"assistant","content":"ok","reasoning_content":"weighing"},"finish_reason":"stop"}],
                            "usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5,"completion_tokens_details":{"reasoning_tokens":1}}
                        }))
                        .unwrap()
                        .into(),
                    ),
                    headers: Default::default(),
                });
            }
            let is_seed = request["messages"]
                .as_array()
                .is_some_and(|messages| messages.len() == 1);
            if request["stream"] == true {
                let sse = concat!(
                    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-stream-seed\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-test\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n",
                    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\",\"signature\":\"\"}}\n\n",
                    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"sig-stream\"}}\n\n",
                    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                    "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool-stream\",\"name\":\"probe\",\"input\":{}}}\n\n",
                    "data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
                    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":8}}\n\n",
                    "data: {\"type\":\"message_stop\"}\n\n",
                );
                return Ok(gw_engines::transport::UpstreamResponse {
                    status: 200,
                    body: gw_engines::transport::UpstreamBody::Sse(sse.as_bytes().to_vec()),
                    headers: Default::default(),
                });
            }
            let response = if is_seed {
                json!({
                    "id":"msg-thinking-seed",
                    "type":"message",
                    "role":"assistant",
                    "model":"claude-test",
                    "content":[
                        {"type":"thinking","thinking":"","signature":"sig-original"},
                        {"type":"redacted_thinking","data":"opaque-redacted"},
                        {"type":"tool_use","id":"tool-known","name":"probe","input":{}}
                    ],
                    "stop_reason":"tool_use",
                    "stop_sequence":null,
                    "usage":{"input_tokens":10,"output_tokens":8}
                })
            } else {
                json!({
                    "id":"msg-thinking-followup",
                    "type":"message",
                    "role":"assistant",
                    "model":"claude-test",
                    "content":[{"type":"text","text":"ok"}],
                    "stop_reason":"end_turn",
                    "stop_sequence":null,
                    "usage":{"input_tokens":12,"output_tokens":1}
                })
            };
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::Json(
                    serde_json::to_vec(&response).unwrap().into(),
                ),
                headers: Default::default(),
            })
        }
    }

    let yaml = r#"
listen: {host: 127.0.0.1, port: 0}
security: {dlp_redact: false, detect_secrets: false}
access_keys: [{ak: ak-thinking, product: demo, qps: 100, daily_token_quota: 1000000}]
models: [{name: claude-test, protocol: anthropic-messages}, {name: gpt-test, protocol: openai-chat}]
accounts: [{name: anthropic, provider: anthropic, protocols: ["anthropic-messages"]}, {name: openai, provider: openai, protocols: ["openai-chat"]}]
"#;
    let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
    let state = Arc::new(GatewayState::from_config(&cfg));
    let hits = Arc::new(AtomicUsize::new(0));
    let cross_protocol = Arc::new(std::sync::Mutex::new(None));
    let app = gw_views::app(AppState::new(
        cfg,
        state,
        Arc::new(ThinkingFixture {
            hits: hits.clone(),
            cross_protocol: cross_protocol.clone(),
        }),
    ));

    let cross = json!({
        "model":"gpt-test",
        "max_tokens":128,
        "thinking":{"type":"enabled","budget_tokens":1024},
        "messages":[{"role":"user","content":"use the tool"}]
    });
    let cross_response = app
        .clone()
        .oneshot(post(
            "/v1/messages",
            Some("ak-thinking"),
            &cross.to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(cross_response.status(), StatusCode::OK);
    let body = body_json(cross_response).await;
    assert_eq!(
        body["content"],
        json!([
            {"type":"thinking","thinking":"weighing","signature":""},
            {"type":"text","text":"ok"}
        ])
    );
    let upstream = cross_protocol.lock().unwrap().take().unwrap();
    assert_eq!(upstream["reasoning_effort"], "low");
    assert_eq!(
        upstream["max_tokens"], 128,
        "not an OpenAI reasoning family"
    );
    assert!(upstream.get("thinking").is_none() && upstream.get("output_config").is_none());
    assert_eq!(hits.swap(0, Ordering::Relaxed), 1);

    let seed = json!({
        "model":"claude-test",
        "max_tokens":128,
        "messages":[{"role":"user","content":"use the tool"}]
    });
    let seed_response = app
        .clone()
        .oneshot(post("/v1/messages", Some("ak-thinking"), &seed.to_string()))
        .await
        .unwrap();
    assert_eq!(seed_response.status(), StatusCode::OK);
    let seed_response = body_json(seed_response).await;
    let original_content = seed_response["content"].clone();
    assert_eq!(original_content[0]["signature"], "sig-original");
    assert_eq!(original_content[1]["data"], "opaque-redacted");
    assert_eq!(hits.load(Ordering::Relaxed), 1);

    let followup = |content: Value, result_id: &str| {
        json!({
            "model":"claude-test",
            "max_tokens":128,
            "messages":[
                {"role":"user","content":"use the tool"},
                {"role":"assistant","content":content},
                {"role":"user","content":[
                    {"type":"tool_result","tool_use_id":result_id,"content":"ready"}
                ]}
            ]
        })
    };
    let exact = followup(original_content.clone(), "tool-known");
    let exact_response = app
        .clone()
        .oneshot(post(
            "/v1/messages",
            Some("ak-thinking"),
            &exact.to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(exact_response.status(), StatusCode::OK);
    assert_eq!(hits.load(Ordering::Relaxed), 2);

    let mut tampered = original_content.clone();
    tampered[0]["signature"] = "sig-tampered".into();
    let tampered = followup(tampered, "tool-known");
    let tampered_response = app
        .clone()
        .oneshot(post(
            "/v1/messages",
            Some("ak-thinking"),
            &tampered.to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(tampered_response.status(), StatusCode::BAD_REQUEST);
    let error = body_json(tampered_response).await;
    assert_eq!(error["error"]["type"], "invalid_request_error");
    assert_eq!(
        hits.load(Ordering::Relaxed),
        2,
        "mismatch is rejected locally"
    );

    let nonstandard_role = json!({
        "model":"claude-test",
        "max_tokens":128,
        "messages":[
            {"role":"assistant","content":[
                {"type":"thinking","thinking":"","signature":"sig-tampered"},
                {"type":"tool_use","id":"tool-known","name":"probe","input":{}}
            ]},
            {"role":"tool","content":[
                {"type":"tool_result","tool_use_id":"tool-known","content":"ready"}
            ]}
        ]
    });
    let nonstandard_role_response = app
        .clone()
        .oneshot(post(
            "/v1/messages",
            Some("ak-thinking"),
            &nonstandard_role.to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(
        nonstandard_role_response.status(),
        StatusCode::OK,
        "non-standard roles forward as before; without a trailing user turn the audit fails open"
    );
    assert_eq!(hits.load(Ordering::Relaxed), 3);

    let mut unknown = original_content;
    unknown[0]["signature"] = "sig-unknown".into();
    unknown[2]["id"] = "tool-unknown".into();
    let unknown = followup(unknown, "tool-unknown");
    let unknown_response = app
        .clone()
        .oneshot(post(
            "/v1/messages",
            Some("ak-thinking"),
            &unknown.to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(unknown_response.status(), StatusCode::OK);
    assert_eq!(hits.load(Ordering::Relaxed), 4, "unknown anchor fails open");

    let stream_seed = json!({
        "model":"claude-test",
        "max_tokens":128,
        "stream":true,
        "messages":[{"role":"user","content":"stream a tool call"}]
    });
    let stream_response = app
        .clone()
        .oneshot(post(
            "/v1/messages",
            Some("ak-thinking"),
            &stream_seed.to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(stream_response.status(), StatusCode::OK);
    let stream_body = String::from_utf8(body_bytes(stream_response).await).unwrap();
    assert!(stream_body.contains("signature_delta"), "{stream_body}");
    assert!(stream_body.contains("sig-stream"), "{stream_body}");
    assert_eq!(hits.load(Ordering::Relaxed), 5);

    let stream_content = json!([
        {"type":"thinking","thinking":"","signature":"sig-stream"},
        {"type":"tool_use","id":"tool-stream","name":"probe","input":{}}
    ]);
    let stream_exact = followup(stream_content.clone(), "tool-stream");
    let stream_exact_response = app
        .clone()
        .oneshot(post(
            "/v1/messages",
            Some("ak-thinking"),
            &stream_exact.to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(stream_exact_response.status(), StatusCode::OK);
    assert_eq!(hits.load(Ordering::Relaxed), 6);

    let mut stream_tampered = stream_content;
    stream_tampered[0]["signature"] = "sig-stream-tampered".into();
    let stream_tampered = followup(stream_tampered, "tool-stream");
    let stream_tampered_response = app
        .oneshot(post(
            "/v1/messages",
            Some("ak-thinking"),
            &stream_tampered.to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(stream_tampered_response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        hits.load(Ordering::Relaxed),
        6,
        "stream-captured mismatch is rejected locally"
    );
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

fn admin(method: &str, uri: &str, token: Option<&str>, body: Option<&str>) -> Request<Body> {
    let mut b = Request::builder().method(method).uri(uri);
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    match body {
        Some(j) => b
            .header("content-type", "application/json")
            .body(Body::from(j.to_owned()))
            .expect("request"),
        None => b.body(Body::empty()).expect("request"),
    }
}

fn get_authed(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header("authorization", "Bearer ak-demo-123")
        .body(Body::empty())
        .expect("request")
}

/// Operator read against the global-token-gated /internal/* surface.
fn internal_get(uri: &str) -> Request<Body> {
    static TOKEN: std::sync::LazyLock<&str> = std::sync::LazyLock::new(|| {
        // SAFETY: written once with a process-constant value; every later
        // reader observes the same string, so no concurrent-write hazard.
        unsafe { std::env::set_var("GW_ADMIN_TOKEN", "e2e-operator-token") };
        "e2e-operator-token"
    });
    Request::builder()
        .uri(uri)
        .header("authorization", format!("Bearer {}", *TOKEN))
        .body(Body::empty())
        .expect("request")
}

const CHAT_BODY: &str = r#"{"model":"gpt-4o","messages":[{"role":"user","content":"hello e2e"}]}"#;

#[tokio::test]
async fn health_and_models() {
    let app = app();
    let resp = app.clone().oneshot(get("/health")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app.clone().oneshot(get("/v1/models")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let resp = app.oneshot(get_authed("/v1/models")).await.unwrap();
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
async fn banned_and_expired_keys_get_distinct_403s() {
    let app = app();
    let resp = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-banned"), CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let j = body_json(resp).await;
    assert!(j["error"]["message"].as_str().unwrap().contains("banned"));

    let resp = app
        .oneshot(post("/v1/chat/completions", Some("ak-expired"), CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let j = body_json(resp).await;
    assert!(j["error"]["message"].as_str().unwrap().contains("expired"));
}

#[tokio::test]
async fn tenant_entitlement_gates_models_and_catalog() {
    let app = app();
    let resp = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-acme-1"), CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-acme-1"),
            r#"{"model":"gpt-4o-mini","messages":[{"role":"user","content":"x"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let j = body_json(resp).await;
    assert!(j["error"]["message"].as_str().unwrap().contains("entitled"));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .header("authorization", "Bearer ak-acme-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    let ids: Vec<&str> = j["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["gpt-4o"]);
}

#[tokio::test]
async fn tenant_price_override_and_vendor_cost_reach_the_ledger() {
    let app = app();
    let r = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-beta-1"), CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let r = app.oneshot(internal_get("/internal/ledger")).await.unwrap();
    let j = body_json(r).await;
    let rec = j["records"]
        .as_array()
        .and_then(|a| a.iter().rev().find(|x| x["ak"] == "ak-beta-1"))
        .expect("beta record");
    let (p, c) = (
        rec["prompt_tokens"].as_i64().unwrap(),
        rec["completion_tokens"].as_i64().unwrap(),
    );
    assert_eq!(
        rec["cost_micros"].as_i64().unwrap(),
        p * 5000 / 1000 + c * 20000 / 1000,
        "tenant override price charged, not the list price"
    );
    assert_eq!(
        rec["vendor_cost_micros"].as_i64().unwrap(),
        p * 100 / 1000 + c * 400 / 1000,
        "serving account's vendor cost recorded"
    );
}

#[tokio::test]
async fn concurrent_requests_cannot_blow_past_quota() {
    let app = app();
    let mut handles = Vec::new();
    for _ in 0..10 {
        let app = app.clone();
        handles.push(tokio::spawn(async move {
            app.oneshot(post(
                "/v1/chat/completions",
                Some("ak-tiny-quota"),
                CHAT_BODY,
            ))
            .await
            .unwrap()
            .status()
        }));
    }
    let mut ok = 0;
    let mut exhausted = 0;
    for h in handles {
        match h.await.unwrap() {
            StatusCode::OK => ok += 1,
            StatusCode::BAD_REQUEST => exhausted += 1,
            other => panic!("unexpected status {other}"),
        }
    }
    assert_eq!(
        (ok, exhausted),
        (1, 9),
        "reservation admits exactly one in-flight request on a quota of 1"
    );
}

#[tokio::test]
async fn failed_request_refunds_its_reservation() {
    let app = app();
    let r = app
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-tiny-quota"),
            r#"{"model":"erroring-model","messages":[{"role":"user","content":"x"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::FAILED_DEPENDENCY,
        "vendor error surfaces as 424 ModelError"
    );
    let r = app
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-tiny-quota"),
            CHAT_BODY,
        ))
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::OK,
        "the failed call's reservation was refunded, budget intact"
    );
}

#[tokio::test]
async fn tenant_scoped_admin() {
    const YAML: &str = r#"
listen: {host: 127.0.0.1, port: 0}
admin: {token_env: GW_TEST_GLOBAL_ADMIN_TSA}
models: [{name: gpt-4o, protocol: openai-chat}]
accounts: [{name: mock-openai-1, provider: openai, protocols: ["openai-chat"]}]
tenants:
  - {name: acme, admin_token_env: GW_TEST_ACME_ADMIN_TSA}
  - {name: beta}
access_keys:
  - {ak: ak-beta-key, tenant: beta, product: demo, qps: 100, daily_token_quota: 1000000}
"#;
    // SAFETY: unique var names for this test; no concurrent reader of them.
    unsafe {
        std::env::set_var("GW_TEST_GLOBAL_ADMIN_TSA", "g-secret");
        std::env::set_var("GW_TEST_ACME_ADMIN_TSA", "t-secret");
    }
    let cfg = Arc::new(GatewayConfig::from_yaml(YAML).unwrap());
    let state = Arc::new(GatewayState::from_config(&cfg));
    let app = gw_views::app(AppState::new(
        cfg,
        state,
        Arc::new(gw_engines::MockTransport),
    ));

    let r = app
        .clone()
        .oneshot(admin(
            "POST",
            "/admin/keys",
            Some("t-secret"),
            Some(r#"{"ak":"ak-acme-new","product":"demo","qps":100,"daily_token_quota":1000}"#),
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CREATED);
    let r = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-acme-new"), CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK, "tenant-created key serves");

    let r = app
        .clone()
        .oneshot(admin(
            "POST",
            "/admin/keys",
            Some("t-secret"),
            Some(r#"{"ak":"x","product":"p","tenant":"beta"}"#),
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    for (method, uri) in [
        ("PATCH", "/admin/keys/ak-beta-key"),
        ("DELETE", "/admin/keys/ak-beta-key"),
    ] {
        let r = app
            .clone()
            .oneshot(admin(method, uri, Some("t-secret"), Some(r#"{"qps":1}"#)))
            .await
            .unwrap();
        assert_eq!(
            r.status(),
            StatusCode::NOT_FOUND,
            "{method} on another tenant's key must not leak its existence"
        );
    }

    let r = app
        .clone()
        .oneshot(admin("POST", "/admin/reload", Some("t-secret"), None))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::FORBIDDEN);

    let r = app
        .clone()
        .oneshot(admin(
            "POST",
            "/admin/keys",
            Some("g-secret"),
            Some(r#"{"ak":"x","product":"p","tenant":"acmee"}"#),
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);

    let r = app
        .clone()
        .oneshot(admin(
            "PUT",
            "/admin/config",
            Some("t-secret"),
            Some("x: 1"),
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    let r = app
        .clone()
        .oneshot(admin(
            "PUT",
            "/admin/config",
            Some("g-secret"),
            Some("x: 1"),
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST, "no config store wired");

    let r = app
        .clone()
        .oneshot(admin("GET", "/admin/keys", Some("t-secret"), None))
        .await
        .unwrap();
    let j = body_json(r).await;
    assert_eq!(j["count"], 1);
    assert_eq!(j["keys"][0]["ak"], "ak-acme-new");
    let r = app
        .clone()
        .oneshot(admin("GET", "/admin/keys", Some("g-secret"), None))
        .await
        .unwrap();
    let j = body_json(r).await;
    assert_eq!(j["count"], 2);

    let r = app
        .clone()
        .oneshot(admin(
            "PATCH",
            "/admin/keys/ak-acme-new",
            Some("t-secret"),
            Some(r#"{"banned":true}"#),
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let r = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-acme-new"), CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::FORBIDDEN);

    let r = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-beta-key"), CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let r = app
        .clone()
        .oneshot(admin("GET", "/admin/usage", Some("t-secret"), None))
        .await
        .unwrap();
    let j = body_json(r).await;
    let tenants: Vec<&str> = j["usage"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["tenant"].as_str().unwrap())
        .collect();
    assert_eq!(tenants, vec!["acme"], "usage is tenant-scoped");
    let r = app
        .oneshot(admin("GET", "/admin/usage", Some("g-secret"), None))
        .await
        .unwrap();
    let j = body_json(r).await;
    let tenants: Vec<&str> = j["usage"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["tenant"].as_str().unwrap())
        .collect();
    assert_eq!(tenants, vec!["acme", "beta"], "global usage sees all");
}

#[tokio::test]
async fn admin_config_publish_reloads_from_store() {
    let Ok(url) = std::env::var("GW_TEST_PG_URL") else {
        return;
    };
    const BOOT: &str = r#"
listen: {host: 127.0.0.1, port: 0}
admin: {token_env: GW_TEST_ADMIN_TOKEN_CFGPUB}
models: [{name: gpt-4o, protocol: openai-chat}]
accounts: [{name: mock-openai-1, provider: openai, protocols: ["openai-chat"]}]
access_keys: [{ak: ak-boot, product: demo, qps: 100, daily_token_quota: 1000000}]
"#;
    // SAFETY: unique var name for this test; no concurrent reader of it.
    unsafe { std::env::set_var("GW_TEST_ADMIN_TOKEN_CFGPUB", "cfg-secret") };
    let store = Arc::new(
        gw_state::PostgresConfigStore::connect(&url)
            .await
            .expect("config store"),
    );
    store.publish(BOOT).await.expect("seed");
    let cfg = Arc::new(GatewayConfig::from_yaml(BOOT).unwrap());
    let state = Arc::new(GatewayState::from_config(&cfg));
    let loader: gw_views::ConfigLoader = {
        let store = store.clone();
        Arc::new(move || {
            let store = store.clone();
            Box::pin(async move {
                match store.load_latest().await.map_err(|e| e.to_string())? {
                    Some((_, yaml)) => GatewayConfig::from_yaml(&yaml).map_err(|e| e.to_string()),
                    None => Err("empty store".to_owned()),
                }
            }) as gw_views::ConfigFuture
        })
    };
    let app = gw_views::app(
        AppState::with_config(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
            Some(loader),
        )
        .with_config_store(store),
    );
    let put = |body: &str| admin("PUT", "/admin/config", Some("cfg-secret"), Some(body));

    let r = app
        .clone()
        .oneshot(put("models: [{name: x}]"))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);

    let v2 = BOOT.replace("ak-boot", "ak-pushed");
    let r = app.clone().oneshot(put(&v2)).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert!(body_json(r).await["version"].as_i64().unwrap() >= 2);
    let r = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-pushed"), CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::OK,
        "published key live after reload"
    );
    let r = app
        .oneshot(post("/v1/chat/completions", Some("ak-boot"), CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::UNAUTHORIZED,
        "old config key dropped"
    );
}

#[tokio::test]
async fn model_quota_degrades_to_fallback() {
    let app = app();
    for i in 1..=2 {
        let resp = app
            .clone()
            .oneshot(post("/v1/chat/completions", Some("ak-beta-1"), CHAT_BODY))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "call {i} under quota");
        let j = body_json(resp).await;
        assert_eq!(j["model"], "gpt-4o");
        assert!(
            j["choices"][0]["message"]["content"]
                .as_str()
                .unwrap()
                .contains("mock-openai:gpt-4o]"),
            "under-quota calls serve the requested model"
        );
    }
    let resp = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-beta-1"), CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert_eq!(j["model"], "gpt-4o", "response echoes the requested model");
    assert!(
        j["choices"][0]["message"]["content"]
            .as_str()
            .unwrap()
            .contains("mock-openai:gpt-4o-mini"),
        "over-quota call is served by the fallback model"
    );

    let resp = app.oneshot(internal_get("/internal/ledger")).await.unwrap();
    let j = body_json(resp).await;
    let last = j["records"]
        .as_array()
        .and_then(|r| {
            r.iter()
                .rev()
                .find(|rec| rec["ak"] == "ak-beta-1" && rec["served_model"] == "gpt-4o-mini")
        })
        .expect("degraded call recorded in the ledger");
    assert_eq!(last["model"], "gpt-4o");
    assert_eq!(last["tenant"], "beta");
}

#[tokio::test]
async fn tenant_rate_limit_pools_across_keys() {
    let app = app();
    for ak in ["ak-acme-1", "ak-acme-2"] {
        let resp = app
            .clone()
            .oneshot(post("/v1/chat/completions", Some(ak), CHAT_BODY))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "warm-up call for {ak}");
    }
    let resp = app
        .oneshot(post("/v1/chat/completions", Some("ak-acme-2"), CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    let j = body_json(resp).await;
    assert!(
        j["error"]["message"]
            .as_str()
            .unwrap()
            .contains("tenant rate limit"),
        "pooled limit must fire at the tenant tier, not per key"
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
async fn model_failure_modes_404_503_unsupported() {
    let app = app();
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

    let resp = app
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            r#"{"model":"realtime","messages":[{"role":"user","content":"x"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        body_json(resp).await["error"]["code"],
        "validation_exception",
        "wrong-surface model must classify as validation"
    );
}

#[tokio::test]
async fn embeddings_images_audio_families() {
    let app = app();

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
    assert_eq!(bytes, b"MOCKBYTES");

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
async fn pricing_dimensions_batch_discount_long_context_tier_and_per_image() {
    let app = app();
    // batch items at the model's batch_discount
    let resp = app
        .clone()
        .oneshot(post(
            "/v1/batches",
            Some("ak-demo-123"),
            r#"{"model":"gpt-4o-mini","items":[{"messages":[{"role":"user","content":"discounted item"}]}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let id = body_json(resp).await["id"].as_str().unwrap().to_owned();
    for _ in 0..100 {
        let j = body_json(
            app.clone()
                .oneshot(get_authed(&format!("/v1/batches/{id}")))
                .await
                .unwrap(),
        )
        .await;
        if j["status"] == "completed" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    // the same prompt online, at list price
    let resp = app
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            r#"{"model":"gpt-4o-mini","messages":[{"role":"user","content":"discounted item"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // long-context tier: a prompt past the threshold bills 2x / 1.5x
    for content in ["hi", "this prompt is long enough to cross the tier"] {
        let resp = app
            .clone()
            .oneshot(post(
                "/v1/messages",
                Some("ak-demo-123"),
                &format!(
                    r#"{{"model":"claude-longctx","max_tokens":32,"messages":[{{"role":"user","content":"{content}"}}]}}"#
                ),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
    // images bill per image
    let resp = app
        .clone()
        .oneshot(post(
            "/v1/images/generations",
            Some("ak-demo-123"),
            r#"{"model":"dall-e-3","prompt":"two pandas","n":2}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app.oneshot(internal_get("/internal/ledger")).await.unwrap();
    let j = body_json(resp).await;
    let rows: Vec<&Value> = j["records"].as_array().unwrap().iter().collect();
    let mini: Vec<&&Value> = rows
        .iter()
        .filter(|r| r["model"] == "gpt-4o-mini")
        .collect();
    let (batch, online) = (mini[0], mini[1]);
    assert_eq!(batch["prompt_tokens"], online["prompt_tokens"]);
    let half = |v: &Value| (v["cost_micros"].as_f64().unwrap() * 0.5).round() as i64;
    assert_eq!(
        batch["cost_micros"],
        half(online),
        "batch item at half price: {batch} vs {online}"
    );
    assert_eq!(
        batch["vendor_cost_micros"],
        (online["vendor_cost_micros"].as_f64().unwrap() * 0.5).round() as i64
    );
    let long: Vec<&&Value> = rows
        .iter()
        .filter(|r| r["model"] == "claude-longctx")
        .collect();
    let (short, tiered) = (long[0], long[1]);
    assert_eq!(
        short["total_tokens"],
        short["prompt_tokens"].as_i64().unwrap() + short["completion_tokens"].as_i64().unwrap(),
        "under the threshold the weighted total is the plain sum"
    );
    let (p, c) = (
        tiered["prompt_tokens"].as_i64().unwrap(),
        tiered["completion_tokens"].as_i64().unwrap(),
    );
    assert!(p > 8, "prompt {p} must cross the tier");
    assert_eq!(
        tiered["total_tokens"].as_i64().unwrap(),
        p * 2 + (c as f64 * 1.5).round() as i64,
        "past the threshold both sides scale: {tiered}"
    );
    let image = rows.iter().find(|r| r["model"] == "dall-e-3").unwrap();
    assert_eq!(image["billed_units"], 2);
    assert_eq!(image["cost_micros"], 80_000);
}

#[tokio::test]
async fn unit_priced_surfaces_bill_characters_and_seconds() {
    let app = app();
    for body in [
        r#"{"model":"tts-1","input":"read this aloud","voice":"alloy"}"#,
        r#"{"model":"whisper-1","audio_b64":"TU9DSw=="}"#,
    ] {
        let path = if body.contains("tts-1") {
            "/v1/audio/speech"
        } else {
            "/v1/audio/transcriptions"
        };
        let resp = app
            .clone()
            .oneshot(post(path, Some("ak-demo-123"), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
    let resp = app.oneshot(internal_get("/internal/ledger")).await.unwrap();
    let j = body_json(resp).await;
    let rows = j["records"].as_array().unwrap();
    let row = |model: &str| rows.iter().find(|r| r["model"] == model).unwrap();
    // "read this aloud" = 15 characters × 15 micros; the mock transcribes 5 s × 100 micros
    assert_eq!(row("tts-1")["billed_units"], 15);
    assert_eq!(row("tts-1")["cost_micros"], 225);
    assert_eq!(row("whisper-1")["billed_units"], 5);
    assert_eq!(row("whisper-1")["cost_micros"], 500);
    assert_eq!(row("whisper-1")["total_tokens"], 0);
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
    let resp = app.oneshot(internal_get("/internal/ledger")).await.unwrap();
    let j = body_json(resp).await;
    let rec = j["records"].as_array().unwrap().last().unwrap();
    assert_eq!(rec["account"], "mock-hunyuan-paygo");
    assert_eq!(rec["ptu_spillover"], true);
}

#[tokio::test]
async fn security_block_and_dlp_redaction() {
    let app = app();
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
    let resp = app
        .clone()
        .oneshot(internal_get("/internal/ledger"))
        .await
        .unwrap();
    assert_eq!(body_json(resp).await["count"], 0, "blocked is not billed");

    let resp = app
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            r#"{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"tell me forbiddenword"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let stream = String::from_utf8(body_bytes(resp).await).unwrap();
    assert!(stream.contains("content_filter"), "{stream}");
    let resp = app
        .clone()
        .oneshot(internal_get("/internal/ledger"))
        .await
        .unwrap();
    assert_eq!(
        body_json(resp).await["count"],
        0,
        "streaming block is not billed"
    );

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
    let resp = app
        .oneshot(internal_get("/internal/accounts"))
        .await
        .unwrap();
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

    let resp = app.oneshot(internal_get("/internal/ledger")).await.unwrap();
    let j = body_json(resp).await;
    assert_eq!(j["count"], 1);
    let rec = &j["records"][0];
    assert_eq!(rec["ak"], "ak-demo-123");
    assert_eq!(rec["model"], "gpt-4o");
    assert_eq!(rec["account"], "mock-openai-1");
    // equal only while gpt-4o sets no token_rate: a weighted ledger total diverges from raw wire usage
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

async fn assert_incremental_stream(model: &str, content: &str) {
    let yaml = gw_config::DEFAULT_YAML.replace("dlp_redact: true", "dlp_redact: false");
    let cfg = Arc::new(GatewayConfig::from_yaml(&yaml).unwrap());
    let state = Arc::new(GatewayState::from_config(&cfg));
    let app = gw_views::app(AppState::new(
        cfg,
        state,
        Arc::new(gw_engines::MockTransport),
    ));

    let body = format!(
        r#"{{"model":"{model}","stream":true,"messages":[{{"role":"user","content":"{content}"}}]}}"#
    );
    let resp = app
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), &body))
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
    assert!(assembled.contains(&format!("you said: {content}")));
    assert!(saw_usage, "final frame must carry usage");
}

#[tokio::test]
async fn gemini_stream_emits_incremental_deltas() {
    assert_incremental_stream("gemini-pro", "stream me gemini").await;
}

#[tokio::test]
async fn dashscope_stream_emits_incremental_deltas() {
    assert_incremental_stream("qwen-max", "stream me dashscope").await;
}

#[tokio::test]
async fn messages_errors_are_anthropic_shaped() {
    let app = app();
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
        .expect("tool_use block from a cross-protocol model");
    assert_eq!(block["name"], "get_weather");
    assert!(block["input"].is_object(), "arguments parsed: {block}");
    assert_eq!(j["stop_reason"], "tool_use");

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
async fn quota_exhaustion_second_call_is_non_retryable_400() {
    let app = app();
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
    assert_eq!(second.status(), StatusCode::BAD_REQUEST);
    let j = body_json(second).await;
    assert!(j["error"]["message"].as_str().unwrap().contains("quota"));
    assert_eq!(j["error"]["code"], "service_quota_exceeded_exception");
}

#[tokio::test]
async fn tools_function_calling_round_trip() {
    let app = app();
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
    assert_eq!(j["stop_reason"], "end_turn");
    assert!(
        j["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("[mock-openai:gpt-4o]")
    );

    let body =
        r#"{"model":"claude-sonnet","messages":[{"role":"user","content":"cross to claude"}]}"#;
    let resp = app
        .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let j = body_json(resp).await;
    assert_eq!(j["object"], "chat.completion");
    assert_eq!(j["choices"][0]["finish_reason"], "stop");
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
    let resp = app.oneshot(internal_get("/internal/ledger")).await.unwrap();
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
    let resp = app.oneshot(internal_get("/internal/ledger")).await.unwrap();
    assert_eq!(body_json(resp).await["count"], 1);
}

#[tokio::test]
async fn files_upload_then_batch_from_file() {
    let app = app();

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
async fn files_and_batches_are_tenant_isolated() {
    let app = app();
    let get_as = |uri: &str, ak: &str| {
        Request::builder()
            .uri(uri)
            .header("authorization", format!("Bearer {ak}"))
            .body(Body::empty())
            .unwrap()
    };

    let upload = json!({"purpose": "batch", "file": "secret default-tenant content"}).to_string();
    let resp = app
        .clone()
        .oneshot(post("/v1/files", Some("ak-demo-123"), &upload))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let file_id = body_json(resp).await["id"].as_str().unwrap().to_owned();

    for uri in [
        format!("/v1/files/{file_id}"),
        format!("/v1/files/{file_id}/content"),
    ] {
        let resp = app
            .clone()
            .oneshot(get_as(&uri, "ak-acme-1"))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "cross-tenant file access must 404: {uri}"
        );
    }
    let steal = json!({"input_file_id": file_id, "model": "gpt-4o"}).to_string();
    let resp = app
        .clone()
        .oneshot(post("/v1/batches", Some("ak-acme-1"), &steal))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "cross-tenant input_file_id must 404"
    );

    let resp = app
        .clone()
        .oneshot(get_as(&format!("/v1/files/{file_id}"), "ak-demo-123"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let submit = json!({"model":"gpt-4o-mini","items":[
        {"messages":[{"role":"user","content":"one"}]}]})
    .to_string();
    let resp = app
        .clone()
        .oneshot(post("/v1/batches", Some("ak-demo-123"), &submit))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let batch_id = body_json(resp).await["id"].as_str().unwrap().to_owned();
    let resp = app
        .clone()
        .oneshot(get_as(&format!("/v1/batches/{batch_id}"), "ak-acme-1"))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "cross-tenant batch access must 404"
    );
    let resp = app
        .oneshot(get_as(&format!("/v1/batches/{batch_id}"), "ak-demo-123"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn realtime_entitlement_blocks_unentitled_tenant() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let addr = serve_app(app()).await;

    let mut req = format!("ws://{addr}/v1/realtime?model=realtime")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", "Bearer ak-acme-1".parse().unwrap());
    assert!(
        tokio_tungstenite::connect_async(req).await.is_err(),
        "unentitled tenant must not open a realtime session"
    );

    let mut ok = format!("ws://{addr}/v1/realtime?model=realtime")
        .into_client_request()
        .unwrap();
    ok.headers_mut()
        .insert("authorization", "Bearer ak-demo-123".parse().unwrap());
    assert!(tokio_tungstenite::connect_async(ok).await.is_ok());
}

#[tokio::test]
async fn realtime_variant_session_serves_canary_bills_public_name() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let yaml = r#"
listen: {host: 127.0.0.1, port: 0}
access_keys:
  - {ak: ak-rt, product: rt, qps: 100, daily_token_quota: 1000000}
accounts:
  - {name: rt-acc, provider: openai, protocols: ["realtime"]}
models:
  - {name: rt-pub, protocol: realtime, variants: [{model: rt-canary, weight: 1}]}
  - {name: rt-canary, protocol: realtime}
"#;
    let cfg = Arc::new(gw_config::GatewayConfig::from_yaml(yaml).unwrap());
    let state = Arc::new(gw_state::GatewayState::from_config(&cfg));
    let application = gw_views::app(gw_views::AppState::new(
        cfg,
        state.clone(),
        Arc::new(gw_engines::MockTransport),
    ));
    let addr = serve_app(application).await;

    let mut req = format!("ws://{addr}/v1/realtime?model=rt-pub")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", "Bearer ak-rt".parse().unwrap());
    req.headers_mut()
        .insert("x-gw-user", "alice".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .expect("ws connect");

    let first = ws.next().await.unwrap().unwrap();
    let v: Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
    assert_eq!(
        v["session"]["model"], "rt-pub",
        "clients see the public name"
    );

    ws.send(Message::text(
        serde_json::json!({"type":"input_text","text":"canary hello"}).to_string(),
    ))
    .await
    .unwrap();
    let mut assembled = String::new();
    while let Some(Ok(msg)) = ws.next().await {
        let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        match v["type"].as_str().unwrap() {
            "response.delta" => assembled.push_str(v["delta"].as_str().unwrap()),
            "response.done" => break,
            other => panic!("unexpected event {other}"),
        }
    }
    assert!(
        assembled.contains("[mock-realtime:rt-canary]"),
        "the variant serves the session: {assembled}"
    );

    let rec = 'ledger: {
        for _ in 0..100 {
            let (_, ledger) = state.store.ledger_snapshot(usize::MAX).await.unwrap();
            if let Some(rec) = ledger.into_iter().next() {
                break 'ledger rec;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("turn was never billed");
    };
    assert_eq!(
        rec.model, "rt-pub",
        "quota/billing rows carry the public name"
    );
    assert_eq!(rec.served_model, "rt-canary");
    state.avail.flush().await;
    let minute = gw_state::epoch_secs() / 60;
    assert_eq!(
        state.avail.window("rt-pub", minute - 5, minute).await,
        (1, 0),
        "the turn samples availability under the public name"
    );
}

#[tokio::test]
async fn realtime_refuses_ungovernable_provider() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let yaml = r#"
listen: {host: 127.0.0.1, port: 0}
access_keys:
  - {ak: ak-rt, product: rt, qps: 100, daily_token_quota: 1000000}
accounts:
  - {name: gem-rt, provider: gemini, endpoint: "http://127.0.0.1:1", protocols: ["realtime"]}
models:
  - {name: rt-model, protocol: realtime}
"#;
    let cfg = Arc::new(gw_config::GatewayConfig::from_yaml(yaml).unwrap());
    let state = Arc::new(gw_state::GatewayState::from_config(&cfg));
    let application = gw_views::app(gw_views::AppState::new(
        cfg,
        state,
        Arc::new(gw_engines::MockTransport),
    ));
    let addr = serve_app(application).await;

    let mut req = format!("ws://{addr}/v1/realtime?model=rt-model")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", "Bearer ak-rt".parse().unwrap());
    assert!(
        tokio_tungstenite::connect_async(req).await.is_err(),
        "realtime must refuse a provider it cannot gate before generation"
    );
}

#[tokio::test]
async fn dlp_redacts_streaming_output_from_the_vendor() {
    use futures::StreamExt;

    #[derive(Debug)]
    struct PiiStream;
    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for PiiStream {
        async fn send(
            &self,
            _req: gw_engines::transport::UpstreamRequest,
        ) -> gw_models::GResult<gw_engines::transport::UpstreamResponse> {
            let frames: Vec<Result<bytes::Bytes, gw_engines::transport::StreamFault>> = vec![
                Ok(bytes::Bytes::from(
                    "data: {\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"reach me at leak@evil.com now\"},\"finish_reason\":null}]}\n\n",
                )),
                Ok(bytes::Bytes::from(
                    "data: {\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":5,\"total_tokens\":8}}\n\n",
                )),
                Ok(bytes::Bytes::from("data: [DONE]\n\n")),
            ];
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::SseStream(
                    futures::stream::iter(frames).boxed(),
                ),
                headers: Default::default(),
            })
        }
    }

    let yaml = r#"
listen: {host: 127.0.0.1, port: 0}
security: {dlp_redact: true}
access_keys: [{ak: ak-dlp, product: demo, qps: 100, daily_token_quota: 1000000}]
models: [{name: gpt-4o, protocol: openai-chat}]
accounts: [{name: a, provider: openai, protocols: ["openai-chat"]}]
"#;
    let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
    let state = Arc::new(GatewayState::from_config(&cfg));
    let app = gw_views::app(AppState::new(cfg, state.clone(), Arc::new(PiiStream)));

    let body = r#"{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let resp = app
        .oneshot(post("/v1/chat/completions", Some("ak-dlp"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = String::from_utf8(body_bytes(resp).await).unwrap();
    assert!(
        text.contains("[REDACTED_EMAIL]"),
        "streamed output must be redacted: {text}"
    );
    assert!(
        !text.contains("leak@evil.com"),
        "raw PII must never reach the client over the stream: {text}"
    );
    let (_, ledger) = state.store.ledger_snapshot(10).await.unwrap();
    assert_eq!(
        ledger.len(),
        1,
        "a fully replayed DLP stream is billed once"
    );
}

#[tokio::test]
async fn batch_response_never_leaks_the_owning_key() {
    let app = app();
    let submit = json!({"model":"gpt-4o-mini","items":[
        {"messages":[{"role":"user","content":"one"}]}]})
    .to_string();
    let resp = app
        .clone()
        .oneshot(post("/v1/batches", Some("ak-demo-123"), &submit))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let id = body_json(resp).await["id"].as_str().unwrap().to_owned();
    let resp = app
        .oneshot(get_authed(&format!("/v1/batches/{id}")))
        .await
        .unwrap();
    let text = String::from_utf8(body_bytes(resp).await).unwrap();
    assert!(
        !text.contains("ak-demo-123") && !text.contains("\"ak\""),
        "batch response must not expose the owning bearer key: {text}"
    );
}

#[tokio::test]
async fn blocklist_covers_the_responses_body() {
    let yaml = r#"
listen: {host: 127.0.0.1, port: 0}
security: {blocklist: ["forbiddenword"]}
access_keys: [{ak: ak-b, product: demo, qps: 100, daily_token_quota: 1000000}]
models: [{name: gpt-5-responses, protocol: responses}]
accounts: [{name: a, provider: openai, protocols: ["responses"]}]
"#;
    let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
    let state = Arc::new(GatewayState::from_config(&cfg));
    let app = gw_views::app(AppState::new(
        cfg,
        state,
        Arc::new(gw_engines::MockTransport),
    ));
    let body = r#"{"model":"gpt-5-responses","input":"please say forbiddenword"}"#;
    let resp = app
        .oneshot(post("/v1/responses", Some("ak-b"), body))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "a blocked Responses request answers a graceful 400, not a 500"
    );
    let j = body_json(resp).await;
    assert_eq!(
        j["error"]["message"], "this content cannot be answered, please try a different request",
        "{j}"
    );
}

#[tokio::test]
async fn outbound_dlp_redacts_the_responses_body() {
    #[derive(Debug)]
    struct PiiResponses;
    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for PiiResponses {
        async fn send(
            &self,
            _req: gw_engines::transport::UpstreamRequest,
        ) -> gw_models::GResult<gw_engines::transport::UpstreamResponse> {
            let body = json!({
                "id":"resp_x","object":"response","model":"gpt-5","status":"completed",
                "output":[{"type":"message","role":"assistant",
                    "content":[{"type":"output_text","text":"write to leak@evil.com"}]}],
                "usage":{"input_tokens":3,"output_tokens":5,"total_tokens":8}
            });
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::Json(body.to_string().into()),
                headers: Default::default(),
            })
        }
    }
    let yaml = r#"
listen: {host: 127.0.0.1, port: 0}
security: {dlp_redact: true}
access_keys: [{ak: ak-d, product: demo, qps: 100, daily_token_quota: 1000000}]
models: [{name: gpt-5-responses, protocol: responses}]
accounts: [{name: a, provider: openai, protocols: ["responses"]}]
"#;
    let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
    let state = Arc::new(GatewayState::from_config(&cfg));
    let app = gw_views::app(AppState::new(cfg, state, Arc::new(PiiResponses)));
    let body = r#"{"model":"gpt-5-responses","input":"hi"}"#;
    let resp = app
        .oneshot(post("/v1/responses", Some("ak-d"), body))
        .await
        .unwrap();
    let text = String::from_utf8(body_bytes(resp).await).unwrap();
    assert!(
        text.contains("[REDACTED_EMAIL]"),
        "response_v2 must be redacted: {text}"
    );
    assert!(
        !text.contains("leak@evil.com"),
        "raw PII must not leak: {text}"
    );
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

    let bad = r#"{"model":"dall-e-3","prompt":"add a rainbow"}"#;
    let resp = app
        .oneshot(post("/v1/images/edits", Some("ak-demo-123"), bad))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn legacy_completions_full_pipeline() {
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
    let led = app.oneshot(internal_get("/internal/ledger")).await.unwrap();
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
    assert_eq!(j["object"], "response");
    assert_eq!(j["status"], "completed");
    assert_eq!(j["output"][0]["content"][0]["type"], "output_text");
    assert!(
        j["output"][0]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("you said: summarize the quarter")
    );
    assert!(j["usage"]["input_tokens"].as_i64().unwrap() > 0);
    let led = app.oneshot(internal_get("/internal/ledger")).await.unwrap();
    assert_eq!(body_json(led).await["count"], 1);
}

#[tokio::test]
async fn responses_api_streaming_full_pipeline() {
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
async fn chat_surface_reasoning_round_trips_through_claude() {
    #[derive(Debug)]
    struct ClaudeReasoning {
        seen: Arc<std::sync::Mutex<Vec<Value>>>,
    }

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for ClaudeReasoning {
        async fn send(
            &self,
            request: gw_engines::transport::UpstreamRequest,
        ) -> gw_models::GResult<gw_engines::transport::UpstreamResponse> {
            let stream = request.stream;
            self.seen
                .lock()
                .unwrap()
                .push(serde_json::from_slice(&request.body).unwrap());
            if stream {
                let sse = concat!(
                    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_s\",\"model\":\"claude-fable-5\",\"usage\":{\"input_tokens\":10}}}\n\n",
                    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\",\"signature\":\"\"}}\n\n",
                    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"weigh\"}}\n\n",
                    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"ing\"}}\n\n",
                    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"sig-s\"}}\n\n",
                    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                    "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                    "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"done\"}}\n\n",
                    "data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
                    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":6}}\n\n",
                    "data: {\"type\":\"message_stop\"}\n\n",
                );
                return Ok(gw_engines::transport::UpstreamResponse {
                    status: 200,
                    body: gw_engines::transport::UpstreamBody::Sse(sse.as_bytes().to_vec()),
                    headers: Default::default(),
                });
            }
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::Json(
                    serde_json::to_vec(&json!({
                        "id":"msg_1","type":"message","role":"assistant","model":"claude-fable-5",
                        "content":[
                            {"type":"thinking","thinking":"weighing","signature":"sig-1"},
                            {"type":"redacted_thinking","data":"blob"},
                            {"type":"tool_use","id":"toolu_1","name":"now","input":{}}
                        ],
                        "stop_reason":"tool_use",
                        "usage":{"input_tokens":10,"output_tokens":8}
                    }))
                    .unwrap()
                    .into(),
                ),
                headers: Default::default(),
            })
        }
    }

    let yaml = r#"
listen: {host: 127.0.0.1, port: 0}
security: {dlp_redact: false, detect_secrets: false}
access_keys: [{ak: ak-reason, product: demo, qps: 100, daily_token_quota: 1000000}]
models: [{name: claude-fable-5, protocol: anthropic-messages}]
accounts: [{name: anthropic, provider: anthropic, protocols: ["anthropic-messages"]}]
"#;
    let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
    let state = Arc::new(GatewayState::from_config(&cfg));
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let app = gw_views::app(AppState::new(
        cfg,
        state,
        Arc::new(ClaudeReasoning { seen: seen.clone() }),
    ));

    let first = json!({
        "model":"claude-fable-5",
        "reasoning_effort":"high",
        "temperature":0,
        "tools":[{"type":"function","function":{"name":"now","parameters":{"type":"object","properties":{}}}}],
        "messages":[{"role":"user","content":"what time is it?"}]
    });
    let resp = app
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-reason"),
            &first.to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let message = &body["choices"][0]["message"];
    assert_eq!(message["reasoning_content"], "weighing");
    assert_eq!(
        message["reasoning_details"],
        json!([
            {"type":"reasoning.text","text":"weighing","signature":"sig-1","format":"anthropic-claude-v1","index":0},
            {"type":"reasoning.encrypted","data":"blob","format":"anthropic-claude-v1","index":1}
        ])
    );
    assert_eq!(message["tool_calls"][0]["function"]["name"], "now");
    assert_eq!(body["choices"][0]["finish_reason"], "tool_calls");
    {
        let upstream = &seen.lock().unwrap()[0];
        assert_eq!(
            upstream["thinking"],
            json!({"type":"adaptive","display":"summarized"})
        );
        assert_eq!(upstream["output_config"], json!({"effort":"high"}));
        assert!(upstream.get("temperature").is_none());
        assert!(upstream.get("reasoning_effort").is_none());
    }

    let mut history = first["messages"].as_array().unwrap().clone();
    let mut assistant = message.clone();
    assistant["role"] = "assistant".into();
    history.push(assistant);
    history.push(json!({"role":"tool","tool_call_id":"toolu_1","content":"12:00"}));
    let second = json!({
        "model":"claude-fable-5",
        "reasoning_effort":"high",
        "stream":true,
        "tools":first["tools"],
        "messages":history
    });
    let resp = app
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-reason"),
            &second.to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = String::from_utf8(body_bytes(resp).await).unwrap();
    let deltas: Vec<Value> = text
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .filter(|f| *f != "[DONE]")
        .map(|f| serde_json::from_str::<Value>(f).unwrap()["choices"][0]["delta"].clone())
        .collect();
    let reasoning: String = deltas
        .iter()
        .filter_map(|d| d["reasoning_content"].as_str())
        .collect();
    assert_eq!(reasoning, "weighing");
    let details: Vec<Value> = deltas
        .iter()
        .filter_map(|d| d["reasoning_details"].as_array())
        .flatten()
        .cloned()
        .collect();
    assert_eq!(
        details,
        [
            json!({"type":"reasoning.text","text":"weighing","signature":"sig-s","format":"anthropic-claude-v1","index":0})
        ]
    );
    let content: String = deltas
        .iter()
        .filter_map(|d| d["content"].as_str())
        .collect();
    assert_eq!(content, "done");
    {
        let upstream = &seen.lock().unwrap()[1];
        assert_eq!(
            upstream["messages"][1]["content"],
            json!([
                {"type":"thinking","thinking":"weighing","signature":"sig-1"},
                {"type":"redacted_thinking","data":"blob"},
                {"type":"tool_use","id":"toolu_1","name":"now","input":{}}
            ])
        );
        assert_eq!(upstream["messages"][2]["role"], "user");
        assert_eq!(upstream["messages"][2]["content"][0]["type"], "tool_result");
    }
}

#[tokio::test]
async fn responses_stream_forwards_native_events_with_names_and_bills() {
    #[derive(Debug)]
    struct ReasoningFixture;

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for ReasoningFixture {
        async fn send(
            &self,
            _request: gw_engines::transport::UpstreamRequest,
        ) -> gw_models::GResult<gw_engines::transport::UpstreamResponse> {
            let sse = concat!(
                "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5-responses\",\"status\":\"in_progress\"}}\n\n",
                "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"id\":\"rs_1\",\"summary\":[],\"encrypted_content\":\"opaque\"}}\n\n",
                "event: response.reasoning_summary_text.delta\ndata: {\"type\":\"response.reasoning_summary_text.delta\",\"output_index\":0,\"summary_index\":0,\"delta\":\"weighing options\"}\n\n",
                "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"now\",\"arguments\":\"{}\"}}\n\n",
                "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"output_index\":2,\"delta\":\"done\"}\n\n",
                "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5-responses\",\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":9,\"output_tokens_details\":{\"reasoning_tokens\":4}}}}\n\n",
            );
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::Sse(sse.as_bytes().to_vec()),
                headers: Default::default(),
            })
        }
    }

    let yaml = gw_config::DEFAULT_YAML.replace("dlp_redact: true", "dlp_redact: false");
    let cfg = Arc::new(GatewayConfig::from_yaml(&yaml).unwrap());
    let state = Arc::new(GatewayState::from_config(&cfg));
    let app = gw_views::app(AppState::new(cfg, state, Arc::new(ReasoningFixture)));

    let body =
        r#"{"model":"gpt-5-responses","stream":true,"input":"think","reasoning":{"effort":"low"}}"#;
    let resp = app
        .clone()
        .oneshot(post("/v1/responses", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = String::from_utf8(body_bytes(resp).await).unwrap();
    let names: Vec<&str> = text
        .lines()
        .filter_map(|l| l.strip_prefix("event: "))
        .collect();
    assert_eq!(
        names,
        [
            "response.created",
            "response.output_item.added",
            "response.reasoning_summary_text.delta",
            "response.output_item.added",
            "response.output_text.delta",
            "response.completed",
        ]
    );
    let frames: Vec<Value> = text
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .filter(|f| *f != "[DONE]")
        .map(|f| serde_json::from_str(f).unwrap())
        .collect();
    assert_eq!(frames.len(), 6, "one data frame per upstream event");
    assert_eq!(frames[1]["item"]["encrypted_content"], "opaque");
    assert_eq!(frames[3]["item"]["arguments"], "{}");
    assert_eq!(
        frames[5]["response"]["usage"]["output_tokens_details"]["reasoning_tokens"],
        4
    );
    assert!(text.trim_end().ends_with("data: [DONE]"));

    let led = app.oneshot(internal_get("/internal/ledger")).await.unwrap();
    let led = body_json(led).await;
    assert_eq!(led["count"], 1);
    assert_eq!(led["records"][0]["completion_tokens"], 9);
}

#[tokio::test]
async fn responses_stream_is_incremental_with_dlp_off() {
    let yaml = gw_config::DEFAULT_YAML.replace("dlp_redact: true", "dlp_redact: false");
    let cfg = Arc::new(GatewayConfig::from_yaml(&yaml).unwrap());
    let state = Arc::new(GatewayState::from_config(&cfg));
    let app = gw_views::app(AppState::new(
        cfg,
        state,
        Arc::new(gw_engines::MockTransport),
    ));

    let body = r#"{"model":"gpt-5-responses","stream":true,"input":"stream this"}"#;
    let resp = app
        .oneshot(post("/v1/responses", Some("ak-demo-123"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = String::from_utf8(body_bytes(resp).await).unwrap();
    let deltas = text
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .filter(|f| *f != "[DONE]")
        .filter_map(|f| serde_json::from_str::<Value>(f).ok())
        .filter(|v| v["type"] == "response.output_text.delta")
        .count();
    assert!(deltas >= 2, "expected incremental deltas, got {deltas}");
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
    let resp = app.oneshot(internal_get("/internal/ledger")).await.unwrap();
    assert_eq!(
        body_json(resp).await["count"],
        2,
        "differing raw params must not share a cache entry"
    );
}

#[tokio::test]
async fn reload_invalidates_the_response_cache() {
    const YAML: &str = r#"
listen: {host: 127.0.0.1, port: 0}
admin: {token_env: GW_TEST_ADMIN_CACHEGEN}
access_keys: [{ak: ak-c, product: demo, qps: 100, daily_token_quota: 1000000}]
models: [{name: cachem, protocol: openai-chat, cache_ttl_seconds: 300}]
accounts: [{name: mock-openai-1, provider: openai, protocols: ["openai-chat"]}]
"#;
    const YAML2: &str = r#"
listen: {host: 127.0.0.1, port: 0}
admin: {token_env: GW_TEST_ADMIN_CACHEGEN}
access_keys: [{ak: ak-c, product: demo, qps: 100, daily_token_quota: 1000000}]
models:
  - {name: cachem, protocol: openai-chat, cache_ttl_seconds: 300}
  - {name: other, protocol: openai-chat}
accounts: [{name: mock-openai-1, provider: openai, protocols: ["openai-chat"]}]
"#;
    // SAFETY: unique var name for this test; no concurrent reader.
    unsafe { std::env::set_var("GW_TEST_ADMIN_CACHEGEN", "cg-secret") };
    let cfg = Arc::new(GatewayConfig::from_yaml(YAML).unwrap());
    let state = Arc::new(GatewayState::from_config(&cfg));
    let loader: gw_views::ConfigLoader = Arc::new(|| {
        Box::pin(async { GatewayConfig::from_yaml(YAML2).map_err(|e| e.to_string()) })
            as gw_views::ConfigFuture
    });
    let app = gw_views::app(gw_views::AppState::with_config(
        gw_state::SharedConfig::new(cfg, state),
        Arc::new(gw_engines::MockTransport),
        Some(loader),
    ));

    let body = r#"{"model":"cachem","messages":[{"role":"user","content":"cache me"}]}"#;
    let count = |app: Router| async move {
        let ledger = admin("GET", "/internal/ledger", Some("cg-secret"), None);
        body_json(app.oneshot(ledger).await.unwrap()).await["count"]
            .as_i64()
            .unwrap()
    };

    for _ in 0..2 {
        let r = app
            .clone()
            .oneshot(post("/v1/chat/completions", Some("ak-c"), body))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
    }
    assert_eq!(count(app.clone()).await, 1, "second call was a cache hit");

    let r = app
        .clone()
        .oneshot(admin("POST", "/admin/reload", Some("cg-secret"), None))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    let r = app
        .clone()
        .oneshot(post("/v1/chat/completions", Some("ak-c"), body))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(
        count(app).await,
        2,
        "the same request misses the cache after a reload and bills again"
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
    assert_eq!(r.status(), StatusCode::OK);
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
    for _ in 0..3 {
        let r = app
            .clone()
            .oneshot(post("/v1/chat/completions", Some("ak-demo-123"), body))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
    let spark_health = async |app: &Router| {
        let r = app
            .clone()
            .oneshot(internal_get("/internal/accounts"))
            .await
            .unwrap();
        let j = body_json(r).await;
        j["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["name"] == "mock-spark-down")
            .unwrap()["health"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    assert_eq!(spark_health(&app).await, "cooling");
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
    let mut recovered = false;
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        if spark_health(&app).await == "ok" {
            recovered = true;
            break;
        }
    }
    assert!(recovered, "cooldown must auto-recover");
}

#[tokio::test]
async fn realtime_applies_blocklist_and_dlp() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let addr = serve_app(app()).await;

    let mut req = format!("ws://{addr}/v1/realtime?model=realtime")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", "Bearer ak-demo-123".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .expect("ws connect");
    let first = ws.next().await.unwrap().unwrap();
    let v: Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
    assert_eq!(v["type"], "session.created");

    ws.send(Message::text(
        serde_json::json!({"type":"input_text","text":"say ForbiddenWord now"}).to_string(),
    ))
    .await
    .unwrap();
    let ev = ws.next().await.unwrap().unwrap();
    let v: Value = serde_json::from_str(ev.to_text().unwrap()).unwrap();
    assert_eq!(v["type"], "error", "blocklisted turn must be refused: {v}");

    ws.send(Message::text(
        serde_json::json!({"type":"input_text","text":"mail me at jane@corp.com"}).to_string(),
    ))
    .await
    .unwrap();
    let mut assembled = String::new();
    while let Some(Ok(msg)) = ws.next().await {
        let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        match v["type"].as_str().unwrap() {
            "response.delta" => assembled.push_str(v["delta"].as_str().unwrap()),
            "response.done" => break,
            other => panic!("unexpected event {other}"),
        }
    }
    assert!(
        assembled.contains("[REDACTED_EMAIL]") && !assembled.contains("jane@corp.com"),
        "PII must be redacted on the realtime surface: {assembled}"
    );
}

#[tokio::test]
async fn realtime_websocket_mock_session() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let addr = serve_app(app()).await;

    let mut req = format!("ws://{addr}/v1/realtime?model=realtime")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", "Bearer ak-demo-123".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .expect("ws connect");

    let first = ws.next().await.unwrap().unwrap();
    let v: Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
    assert_eq!(v["type"], "session.created");
    assert_eq!(v["session"]["account"], "mock-realtime-1");

    ws.send(Message::text("not json")).await.unwrap();
    let err = ws.next().await.unwrap().unwrap();
    let v: Value = serde_json::from_str(err.to_text().unwrap()).unwrap();
    assert_eq!(v["type"], "error");
    assert_eq!(v["error"]["code"], "validation_exception");
    assert_eq!(v["error"]["type"], "invalid_request_error");

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

    async fn vendor_ws(ws: axum::extract::ws::WebSocketUpgrade) -> axum::response::Response {
        ws.on_upgrade(|mut socket| async move {
            use axum::extract::ws::Message as M;
            let send = |v: Value| M::Text(v.to_string().into());
            let _ = socket
                .send(send(serde_json::json!({"type":"session.created","session":{"vendor":"fake"}})))
                .await;
            let mut turn = 0;
            while let Some(Ok(M::Text(t))) = socket.recv().await {
                let Ok(v) = serde_json::from_str::<Value>(&t) else {
                    continue;
                };
                if v["type"] == "response.create" {
                    turn += 1;
                    let _ = socket
                        .send(send(serde_json::json!({
                            "type":"response.output_text.delta",
                            "delta": if turn == 1 { "bridge " } else { "partial output" },
                        })))
                        .await;
                    if turn == 1 {
                        let _ = socket
                            .send(send(serde_json::json!({
                                "type":"response.output_text.delta",
                                "delta":"ok",
                            })))
                            .await;
                        let _ = socket
                            .send(send(serde_json::json!({"type":"response.done",
                                "response":{"usage":{"input_tokens":9,"output_tokens":4,"total_tokens":13}}})))
                            .await;
                    }
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
    let addr = serve_app(application).await;

    let mut req = format!("ws://{addr}/v1/realtime?model=rt-model")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", "Bearer ak-rt".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .expect("ws connect");

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

    let (count, records) = state.store.ledger_snapshot(usize::MAX).await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(records[0].model, "rt-model");
    assert_eq!(records[0].account, "rt-vendor");
    assert_eq!(records[0].total_tokens, 13);

    ws.send(Message::text(
        serde_json::json!({"type":"response.create"}).to_string(),
    ))
    .await
    .unwrap();
    let partial = ws.next().await.unwrap().unwrap();
    let partial: Value = serde_json::from_str(partial.to_text().unwrap()).unwrap();
    assert_eq!(partial["type"], "response.output_text.delta");
    assert_eq!(partial["delta"], "partial output");
    ws.close(None).await.unwrap();

    let (count, records) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let snapshot = state.store.ledger_snapshot(usize::MAX).await.unwrap();
            if snapshot.0 == 2 {
                break snapshot;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("aborted turn was never settled");
    assert_eq!(count, 2);
    let aborted = &records[1];
    assert!(aborted.estimated);
    assert_eq!(aborted.prompt_tokens, 0);
    assert!(aborted.completion_tokens > 0);
    assert_ne!(aborted.request_id, records[0].request_id);
    assert_eq!(
        state.governance.quota_used("ak-rt").await,
        records
            .iter()
            .map(|record| record.total_tokens)
            .sum::<i64>(),
        "the delivered partial output must not be refunded"
    );
}

#[tokio::test]
async fn realtime_second_create_during_a_turn_cannot_desync_billing() {
    use axum::routing::any;
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    async fn vendor_ws(ws: axum::extract::ws::WebSocketUpgrade) -> axum::response::Response {
        ws.on_upgrade(|mut socket| async move {
            use axum::extract::ws::Message as M;
            let send = |v: Value| M::Text(v.to_string().into());
            let turn_frames = || {
                (
                    serde_json::json!({"type":"response.output_text.delta","delta":"turn"}),
                    serde_json::json!({"type":"response.done",
                        "response":{"usage":{"input_tokens":9,"output_tokens":4,"total_tokens":13}}}),
                )
            };
            let _ = socket
                .send(send(serde_json::json!({"type":"session.created"})))
                .await;
            let mut active = false;
            while let Some(Ok(M::Text(t))) = socket.recv().await {
                let Ok(v) = serde_json::from_str::<Value>(&t) else {
                    continue;
                };
                match v["type"].as_str() {
                    Some("response.create") if !active => active = true,
                    Some("response.create") => {
                        let _ = socket
                            .send(send(serde_json::json!({"type":"error",
                                "error":{"code":"conversation_already_has_active_response"}})))
                            .await;
                        let (a, b) = turn_frames();
                        let _ = socket.send(send(a)).await;
                        let _ = socket.send(send(b)).await;
                        active = false;
                    }
                    Some("input_text") if active => {
                        let (a, b) = turn_frames();
                        let _ = socket.send(send(a)).await;
                        let _ = socket.send(send(b)).await;
                        active = false;
                    }
                    _ => {}
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

    let yaml = format!(
        r#"
listen: {{host: 127.0.0.1, port: 0}}
access_keys:
  - {{ak: ak-dup, product: rt, qps: 100, daily_token_quota: 1000000}}
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
    let addr = serve_app(application).await;

    let mut req = format!("ws://{addr}/v1/realtime?model=rt-model")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", "Bearer ak-dup".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .expect("ws connect");
    let _ = ws.next().await.unwrap().unwrap();

    let create = || Message::text(serde_json::json!({"type":"response.create"}).to_string());
    ws.send(create()).await.unwrap();
    ws.send(create()).await.unwrap();
    let mut saw_error = false;
    while let Some(Ok(msg)) = ws.next().await {
        let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        match v["type"].as_str() {
            Some("error") => saw_error = true,
            Some("response.done") => break,
            _ => {}
        }
    }
    assert!(saw_error, "the duplicate create is rejected upstream");

    ws.send(create()).await.unwrap();
    ws.send(Message::text(
        serde_json::json!({"type":"input_text","text":"go"}).to_string(),
    ))
    .await
    .unwrap();
    while let Some(Ok(msg)) = ws.next().await {
        let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        if v["type"] == "response.done" {
            break;
        }
    }

    assert_eq!(
        state.governance.quota_used("ak-dup").await,
        26,
        "both turns settled to actuals with no reserve left dangling"
    );
    let (count, records) = state.store.ledger_snapshot(usize::MAX).await.unwrap();
    assert_eq!(count, 2, "exactly the two real turns billed");
    assert!(records.iter().all(|r| r.total_tokens == 13));
    assert_ne!(
        records[0].request_id, records[1].request_id,
        "each turn billed under its own admission"
    );
}

#[tokio::test]
async fn realtime_bridge_gates_server_vad_turns() {
    use axum::routing::any;
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    async fn vendor_ws(ws: axum::extract::ws::WebSocketUpgrade) -> axum::response::Response {
        ws.on_upgrade(|mut socket| async move {
            use axum::extract::ws::Message as M;
            let send = |v: Value| M::Text(v.to_string().into());
            let _ = socket
                .send(send(serde_json::json!({"type":"session.created"})))
                .await;
            while let Some(Ok(M::Text(t))) = socket.recv().await {
                let Ok(v) = serde_json::from_str::<Value>(&t) else {
                    continue;
                };
                if v["type"] == "input_audio_buffer.append" {
                    let _ = socket
                        .send(send(serde_json::json!({"type":"response.created"})))
                        .await;
                    tokio::select! {
                        m = socket.recv() => {
                            if let Some(Ok(M::Text(c))) = m
                                && serde_json::from_str::<Value>(&c)
                                    .map(|c| c["type"] == "response.cancel")
                                    .unwrap_or(false)
                            {
                                let _ = socket
                                    .send(send(serde_json::json!({"type":"response.done",
                                        "response":{"status":"cancelled","usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}})))
                                    .await;
                            }
                        }
                        _ = tokio::time::sleep(std::time::Duration::from_millis(150)) => {
                            let _ = socket
                                .send(send(serde_json::json!({"type":"response.output_text.delta","delta":"vad"})))
                                .await;
                            let _ = socket
                                .send(send(serde_json::json!({"type":"response.done",
                                    "response":{"usage":{"input_tokens":9,"output_tokens":4,"total_tokens":13}}})))
                                .await;
                        }
                    }
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

    let yaml = format!(
        r#"
listen: {{host: 127.0.0.1, port: 0}}
access_keys:
  - {{ak: ak-vad, product: rt, qps: 100, daily_token_quota: 10}}
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
    let addr = serve_app(application).await;

    let mut req = format!("ws://{addr}/v1/realtime?model=rt-model")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", "Bearer ak-vad".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .expect("ws connect");
    let _ = ws.next().await.unwrap().unwrap();

    let append = || {
        Message::text(
            serde_json::json!({"type":"input_audio_buffer.append","audio":"aGk="}).to_string(),
        )
    };

    ws.send(append()).await.unwrap();
    let mut done1 = None;
    while let Some(Ok(msg)) = ws.next().await {
        let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        if v["type"] == "response.done" {
            done1 = Some(v);
            break;
        }
    }
    assert_eq!(
        done1.unwrap()["response"]["usage"]["total_tokens"],
        13,
        "first turn admitted and billed"
    );

    ws.send(append()).await.unwrap();
    let mut saw_error = false;
    let mut leaked_output = false;
    while let Ok(Some(Ok(msg))) =
        tokio::time::timeout(std::time::Duration::from_millis(400), ws.next()).await
    {
        let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        match v["type"].as_str() {
            Some("error") => saw_error = true,
            Some("response.output_text.delta") | Some("response.done") => leaked_output = true,
            _ => {}
        }
    }
    assert!(saw_error, "an over-quota server-VAD turn must be denied");
    assert!(
        !leaked_output,
        "a denied turn's output must not reach the client"
    );

    let (count, records) = state.store.ledger_snapshot(usize::MAX).await.unwrap();
    assert_eq!(
        count, 1,
        "only the admitted turn bills; the denied one does not"
    );
    assert_eq!(records[0].total_tokens, 13);
}

#[tokio::test]
async fn realtime_authenticates_via_ws_subprotocol() {
    use futures::StreamExt;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let addr = serve_app(app()).await;

    let mut req = format!("ws://{addr}/v1/realtime?model=realtime")
        .into_client_request()
        .unwrap();
    req.headers_mut().insert(
        "sec-websocket-protocol",
        "realtime, gw-api-key.ak-demo-123".parse().unwrap(),
    );
    let (mut ws, resp) = tokio_tungstenite::connect_async(req)
        .await
        .expect("ws connect via subprotocol auth");
    assert_eq!(
        resp.headers()
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok()),
        Some("realtime")
    );
    let first = ws.next().await.unwrap().unwrap();
    let v: Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
    assert_eq!(v["type"], "session.created");

    let mut bad = format!("ws://{addr}/v1/realtime?model=realtime")
        .into_client_request()
        .unwrap();
    bad.headers_mut().insert(
        "sec-websocket-protocol",
        "realtime, gw-api-key.nope".parse().unwrap(),
    );
    assert!(
        tokio_tungstenite::connect_async(bad).await.is_err(),
        "invalid subprotocol AK must be rejected"
    );
}

#[tokio::test]
async fn realtime_turns_are_rate_limited() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let addr = serve_app(app()).await;

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
    assert_eq!(v["error"]["code"], "throttling_exception");
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("rate limit")
    );
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
    let app = app();
    let resp = app
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            r#"{"model":"erroring-model","messages":[{"role":"user","content":"x"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FAILED_DEPENDENCY);
    assert_eq!(
        resp.headers()
            .get("x-amzn-errortype")
            .and_then(|v| v.to_str().ok()),
        Some("ModelErrorException")
    );
    let j = body_json(resp).await;
    assert!(
        j["error"]["message"]
            .as_str()
            .unwrap()
            .contains("mock vendor rejected")
    );
    assert_eq!(j["error"]["code"], "model_error_exception");
    assert_eq!(j["error"]["type"], "model_error");
    assert_eq!(j["error"]["original_status_code"], 400);
    assert_eq!(j["error"]["resource_name"], "erroring-model");
}

#[tokio::test]
async fn error_contract_machine_channel() {
    let app = app();
    let r = app
        .clone()
        .oneshot(post("/v1/chat/completions", None, CHAT_BODY))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        r.headers()
            .get("x-amzn-errortype")
            .and_then(|v| v.to_str().ok()),
        Some("UnrecognizedClientException")
    );
    assert_eq!(
        r.headers()
            .get("access-control-expose-headers")
            .and_then(|v| v.to_str().ok()),
        Some("x-amzn-errortype, retry-after")
    );
    let j = body_json(r).await;
    assert_eq!(j["error"]["code"], "unrecognized_client_exception");
    assert_eq!(j["error"]["type"], "authentication_error");

    let r = app
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            "{not json",
        ))
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::BAD_REQUEST,
        "rejection stays in the envelope"
    );
    let j = body_json(r).await;
    assert_eq!(j["error"]["code"], "validation_exception");

    let r = app.clone().oneshot(get("/v1/nope")).await.unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
    let j = body_json(r).await;
    assert_eq!(j["error"]["code"], "resource_not_found_exception");

    let r = app
        .clone()
        .oneshot(get("/v1/chat/completions"))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST, "405 normalizes to 400");
    let j = body_json(r).await;
    assert_eq!(j["error"]["code"], "validation_exception");

    let r = app
        .clone()
        .oneshot(post("/v1/messages", None, r#"{"model":"m","messages":[]}"#))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    let j = body_json(r).await;
    assert_eq!(j["type"], "error");
    assert_eq!(j["error"]["type"], "authentication_error");
    assert_eq!(j["error"]["code"], "unrecognized_client_exception");

    let oversized = format!(
        r#"{{"model":"m-chat","messages":[{{"role":"user","content":"{}"}}]}}"#,
        "x".repeat(3 * 1024 * 1024)
    );
    let r = app
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            &oversized,
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let j = body_json(r).await;
    assert_eq!(j["error"]["code"], "request_entity_too_large_exception");
}

#[tokio::test]
async fn streaming_a_non_native_streaming_model_still_delivers_content() {
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
async fn erasure_round_trip_over_http() {
    // SAFETY: unique var name for this test; no concurrent reader of it.
    unsafe { std::env::set_var("GW_E2E_ERASE_ADMIN", "root-tok") };
    let yaml = r#"
listen: {host: 127.0.0.1, port: 0}
admin: {token_env: GW_E2E_ERASE_ADMIN}
models: [{name: gpt-4o, protocol: openai-chat}]
accounts: [{name: a1, provider: openai, protocols: ['openai-chat']}]
tenants: [{name: t1, retention: {content: redacted, days: 1}}]
access_keys: [{ak: k-erase, tenant: t1, product: p, qps: 100, daily_token_quota: 100000}]
"#;
    let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
    let state = Arc::new(GatewayState::from_config(&cfg));
    let app = gw_views::app(AppState::new(
        cfg,
        state.clone(),
        Arc::new(gw_engines::MockTransport),
    ));

    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .header("authorization", "Bearer k-erase")
        .header("x-gw-user", "erase-me")
        .body(Body::from(
            r#"{"model":"gpt-4o","messages":[{"role":"user","content":"remember this"}]}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let (_, rows) = state.store.ledger_snapshot(1).await.unwrap();
    let rid = rows[0].request_id.clone();

    let admin_get = |uri: String| {
        Request::builder()
            .method("GET")
            .uri(uri)
            .header("authorization", "Bearer root-tok")
            .body(Body::empty())
            .unwrap()
    };
    let resp = app
        .clone()
        .oneshot(admin_get(format!("/admin/audit/content/{rid}")))
        .await
        .unwrap();
    let j = body_json(resp).await;
    assert_eq!(
        j["entries"].as_array().unwrap().len(),
        3,
        "prompt, response, and terminal retained: {j}"
    );

    let resp = app
        .clone()
        .oneshot(admin_get("/admin/usage/users?user=erase-me".into()))
        .await
        .unwrap();
    let j = body_json(resp).await;
    assert_eq!(j["usage"][0]["user_id"], "erase-me", "{j}");
    assert!(j["usage"][0]["total_tokens"].as_i64().unwrap() > 0);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/admin/audit/content?user=erase-me")
                .header("authorization", "Bearer root-tok")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(body_json(resp).await["deleted"], 3);

    let resp = app
        .clone()
        .oneshot(admin_get(format!("/admin/audit/content/{rid}")))
        .await
        .unwrap();
    let j = body_json(resp).await;
    assert!(
        j["entries"].as_array().unwrap().is_empty(),
        "every retained row is gone: {j}"
    );

    let resp = app
        .oneshot(admin_get("/admin/audit/ops".into()))
        .await
        .unwrap();
    let j = body_json(resp).await;
    assert!(
        j["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["action"] == "content_erase" && e["target"] == "erase-me"),
        "the erasure is audited: {j}"
    );
}

#[tokio::test]
async fn metrics_endpoint_exposes_request_counters() {
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
    let resp = router
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-demo-123"),
            r#"{"model":"no-such-model","messages":[{"role":"user","content":"x"}]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let resp = router.oneshot(get("/metrics")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("gateway_requests_total"), "{text}");
    assert!(text.contains("status=\"200\""), "{text}");
    assert!(text.contains("status=\"404\""), "{text}");
    assert!(text.contains("gateway_node_duration_seconds"), "{text}");
    assert!(text.contains("gateway_tokens_total"), "{text}");
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
    let resp = app
        .oneshot(internal_get("/internal/ledger?limit=2"))
        .await
        .unwrap();
    let j = body_json(resp).await;
    assert_eq!(j["count"], 3, "count reports the total");
    assert_eq!(
        j["records"].as_array().unwrap().len(),
        2,
        "records page is limited"
    );
}

#[tokio::test]
async fn chat_surface_renders_anthropic_tool_use_as_tool_calls() {
    #[derive(Debug)]
    struct ToolUseFixture;

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for ToolUseFixture {
        async fn send(
            &self,
            request: gw_engines::transport::UpstreamRequest,
        ) -> gw_models::GResult<gw_engines::transport::UpstreamResponse> {
            let request: Value = serde_json::from_slice(&request.body).unwrap();
            if request["stream"] == true {
                let sse = concat!(
                    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-s\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-test\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n",
                    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Listing.\"}}\n\n",
                    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                    "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool-s\",\"name\":\"shell\",\"input\":{}}}\n\n",
                    "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"command\\\":\\\"ls\\\"}\"}}\n\n",
                    "data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
                    "data: {\"type\":\"content_block_start\",\"index\":2,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool-s2\",\"name\":\"pwd\",\"input\":{}}}\n\n",
                    "data: {\"type\":\"content_block_stop\",\"index\":2}\n\n",
                    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":8}}\n\n",
                    "data: {\"type\":\"message_stop\"}\n\n",
                );
                return Ok(gw_engines::transport::UpstreamResponse {
                    status: 200,
                    body: gw_engines::transport::UpstreamBody::Sse(sse.as_bytes().to_vec()),
                    headers: Default::default(),
                });
            }
            let mut tool_use =
                json!({"type":"tool_use","id":"tool-1","name":"shell","input":{"command":"ls"}});
            if request["messages"][0]["content"] == "broken" {
                tool_use["id"] = Value::Null;
            }
            let response = json!({
                "id":"msg-1","type":"message","role":"assistant","model":"claude-test",
                "content":[{"type":"text","text":"I'll list them."}, tool_use],
                "stop_reason":"tool_use","stop_sequence":null,
                "usage":{"input_tokens":10,"output_tokens":8}
            });
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::Json(
                    serde_json::to_vec(&response).unwrap().into(),
                ),
                headers: Default::default(),
            })
        }
    }

    async fn streamed_tool_calls(
        app: Router,
        body: &Value,
    ) -> (Vec<Value>, Option<String>, String) {
        let resp = app
            .oneshot(post(
                "/v1/chat/completions",
                Some("ak-tools"),
                &body.to_string(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let text = String::from_utf8(body_bytes(resp).await).unwrap();
        let mut calls = Vec::new();
        let mut finish = None;
        for l in text.lines().filter_map(|l| l.strip_prefix("data: ")) {
            if l == "[DONE]" {
                break;
            }
            let v: Value = serde_json::from_str(l).unwrap();
            if let Some(c) = v["choices"][0]["delta"]["tool_calls"].as_array() {
                calls.extend(c.iter().cloned());
            }
            if let Some(fr) = v["choices"][0]["finish_reason"].as_str() {
                finish = Some(fr.to_owned());
            }
        }
        (calls, finish, text)
    }

    fn app_with(dlp_redact: bool) -> Router {
        let yaml = format!(
            r#"
listen: {{host: 127.0.0.1, port: 0}}
security: {{dlp_redact: {dlp_redact}, detect_secrets: false}}
access_keys: [{{ak: ak-tools, product: demo, qps: 100, daily_token_quota: 1000000}}]
models: [{{name: claude-test, protocol: anthropic-messages}}]
accounts: [{{name: anthropic, provider: anthropic, protocols: ["anthropic-messages"]}}]
"#
        );
        let cfg = Arc::new(GatewayConfig::from_yaml(&yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        gw_views::app(AppState::new(cfg, state, Arc::new(ToolUseFixture)))
    }

    let app = app_with(false);
    let tools = json!([{"type":"function","function":{"name":"shell","description":"run",
        "parameters":{"type":"object","properties":{"command":{"type":"string"}}}}}]);

    let body = json!({"model":"claude-test","max_tokens":64,"tools":tools,
        "messages":[{"role":"user","content":"list files"}]});
    let resp = app
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-tools"),
            &body.to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = body_json(resp).await;
    let msg = &v["choices"][0]["message"];
    assert_eq!(v["choices"][0]["finish_reason"], "tool_calls");
    assert_eq!(msg["content"], "I'll list them.");
    assert_eq!(msg["tool_calls"][0]["id"], "tool-1");
    assert_eq!(msg["tool_calls"][0]["type"], "function");
    assert_eq!(msg["tool_calls"][0]["function"]["name"], "shell");
    assert_eq!(
        msg["tool_calls"][0]["function"]["arguments"],
        "{\"command\":\"ls\"}"
    );

    let body = json!({"model":"claude-test","max_tokens":64,"tools":tools,
        "messages":[{"role":"user","content":"broken"}]});
    let resp = app
        .clone()
        .oneshot(post(
            "/v1/chat/completions",
            Some("ak-tools"),
            &body.to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let v = body_json(resp).await;
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("do not render as OpenAI tool_calls"),
        "{v}"
    );

    let body = json!({"model":"claude-test","max_tokens":64,"stream":true,"tools":tools,
        "messages":[{"role":"user","content":"list files"}]});
    for buffered in [false, true] {
        let (calls, finish, text) = streamed_tool_calls(app_with(buffered), &body).await;
        assert_eq!(
            finish.as_deref(),
            Some("tool_calls"),
            "buffered={buffered}: {text}"
        );
        assert_eq!(calls.len(), 2, "buffered={buffered}: {text}");
        assert_eq!(calls[0]["index"], 0);
        assert_eq!(calls[0]["id"], "tool-s");
        assert_eq!(calls[0]["type"], "function");
        assert_eq!(calls[0]["function"]["name"], "shell");
        assert_eq!(calls[0]["function"]["arguments"], "{\"command\":\"ls\"}");
        assert_eq!(calls[1]["index"], 1);
        assert_eq!(calls[1]["function"]["name"], "pwd");
        assert_eq!(calls[1]["function"]["arguments"], "{}");
    }
}

#[tokio::test]
async fn model_prompt_cache_knob_reaches_the_anthropic_wire() {
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct CaptureFixture {
        body: Mutex<Option<Value>>,
    }

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for CaptureFixture {
        async fn send(
            &self,
            request: gw_engines::transport::UpstreamRequest,
        ) -> gw_models::GResult<gw_engines::transport::UpstreamResponse> {
            *self.body.lock().unwrap() = Some(serde_json::from_slice(&request.body).unwrap());
            let response = json!({
                "id":"msg-1","type":"message","role":"assistant","model":"claude-test",
                "content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn",
                "usage":{"input_tokens":10,"output_tokens":1}
            });
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::Json(
                    serde_json::to_vec(&response).unwrap().into(),
                ),
                headers: Default::default(),
            })
        }
    }

    let yaml = r#"
listen: {host: 127.0.0.1, port: 0}
security: {dlp_redact: false, detect_secrets: false}
access_keys: [{ak: ak-cache, product: demo, qps: 100, daily_token_quota: 1000000}]
models:
  - {name: claude-cached, protocol: anthropic-messages, prompt_cache: true}
  - {name: claude-plain, protocol: anthropic-messages}
  - {name: claude-split, protocol: anthropic-messages, variants: [{model: claude-cached, weight: 1}]}
accounts: [{name: anthropic, provider: anthropic, protocols: ["anthropic-messages"]}]
"#;
    let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
    let state = Arc::new(GatewayState::from_config(&cfg));
    let fixture = Arc::new(CaptureFixture::default());
    let app = gw_views::app(AppState::new(cfg, state, fixture.clone()));

    for (model, cached) in [
        ("claude-cached", true),
        ("claude-plain", false),
        ("claude-split", true), // the served variant's knob, not the public name's
    ] {
        let body = json!({"model":model,"max_tokens":32,
            "messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hello"}]});
        let resp = app
            .clone()
            .oneshot(post(
                "/v1/chat/completions",
                Some("ak-cache"),
                &body.to_string(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let sent = fixture.body.lock().unwrap().take().expect("upstream body");
        let marked = sent["system"][0]["cache_control"]["type"] == "ephemeral"
            && sent["messages"][0]["content"][0]["cache_control"]["type"] == "ephemeral";
        assert_eq!(marked, cached, "{model}: {sent}");
    }
}
