use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use gw_config::{ConfigError, GatewayConfig};
use gw_state::{GatewayState, KeyStatus};
use gw_views::AppState;
use serde_json::{Value, json};
use tower::ServiceExt;

fn app(qps: f64) -> Result<(Router, Arc<GatewayState>), ConfigError> {
    let yaml = format!(
        "listen: {{host: h, port: 1}}
abuse: {{tiers: [{{rejects: 2, suspend_hours: 1}}]}}
tenants: [{{name: t, qps: {qps}}}]
access_keys:
  - {{ak: flooder, tenant: t, product: p, qps: {qps}, daily_token_quota: 100000}}
  - {{ak: bystander, tenant: t, product: p, qps: 100, daily_token_quota: 100000}}
models: [{{name: m, protocol: openai-chat}}]
accounts: [{{name: a, provider: openai, protocols: [openai-chat]}}]"
    );
    let cfg = Arc::new(GatewayConfig::from_yaml(&yaml)?);
    let state = Arc::new(GatewayState::from_config(&cfg));
    let app = gw_views::app(AppState::new(
        cfg,
        Arc::clone(&state),
        Arc::new(gw_engines::MockTransport),
    ));
    Ok((app, state))
}

fn request(path: &str, key: &str) -> Result<Request<Body>, axum::http::Error> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {key}"))
        .body(Body::from(
            r#"{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":8}"#,
        ))
}

#[tokio::test]
async fn pooled_and_key_denials_preserve_http_throttling_envelopes() {
    let (app, _) = app(0.0).expect("denial config");
    for path in ["/v1/chat/completions", "/v1/messages"] {
        for (key, reason) in [
            ("flooder", "rate limit exceeded for key"),
            ("bystander", "tenant rate limit"),
        ] {
            let response = app
                .clone()
                .oneshot(request(path, key).expect("request"))
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(
                response.headers()["x-amzn-errortype"],
                "ThrottlingException"
            );
            assert_eq!(response.headers()["content-type"], "application/json");
            let body = to_bytes(response.into_body(), 4096)
                .await
                .expect("response body");
            let body: Value = serde_json::from_slice(&body).expect("JSON envelope");
            let message = body["error"]["message"].as_str().expect("error message");
            assert!(message.contains(reason), "{message}");
            let expected = if path == "/v1/messages" {
                json!({
                    "type": "error",
                    "error": {
                        "type": "rate_limit_error",
                        "code": "throttling_exception",
                        "message": message,
                    },
                })
            } else {
                json!({"error": {
                    "type": "rate_limit_error",
                    "code": "throttling_exception",
                    "message": message,
                    "param": null,
                }})
            };
            assert_eq!(body, expected);
        }
    }
}

#[tokio::test]
async fn tenant_flooding_suspends_only_the_offending_key() {
    let (app, state) = app(0.001).expect("shared tenant config");
    let response = app
        .clone()
        .oneshot(request("/v1/chat/completions", "flooder").expect("first request"))
        .await
        .expect("first response");
    assert_eq!(response.status(), StatusCode::OK);

    for key in ["bystander", "flooder", "bystander", "flooder", "bystander"] {
        let response = app
            .clone()
            .oneshot(request("/v1/chat/completions", key).expect("request"))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS, "{key}");
    }
    let response = app
        .oneshot(request("/v1/chat/completions", "flooder").expect("suspended request"))
        .await
        .expect("suspended response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    for (key, rejects, status) in [
        ("flooder", 2, KeyStatus::Suspended),
        ("bystander", 0, KeyStatus::Active),
    ] {
        assert_eq!(
            state.governance.quota_used(&format!("abuse:{key}")).await,
            rejects
        );
        let fresh = state.auth.authenticate(key).await.expect("fresh key");
        assert_eq!(fresh.status_at(gw_state::epoch_secs()), status);
    }
}
