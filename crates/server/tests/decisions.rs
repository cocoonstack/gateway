//! The System One decision surfaces (`/v1/decisions`, `/v1/systemone`) against a recording OpenRouter stand-in.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use gw_config::GatewayConfig;
use gw_engines::transport::{
    HeaderMap, Transport, UpstreamBody, UpstreamRequest, UpstreamResponse,
};
use gw_state::GatewayState;
use serde_json::{Value, json};
use tokio::sync::{Notify, Semaphore};
use tower::ServiceExt;

mod common;
use common::body_json;

const CONFIG: &str = r#"
listen: {host: 127.0.0.1, port: 0}
providers:
  - {name: openrouter, kind: openrouter, api_key_env: GW_TEST_JEV_UPSTREAM_KEY}
models:
  - {name: typesafe/jev-1.13, protocol: decisions, provider: openrouter, input_price_per_1k_micros: 900000}
  - {name: jev-1.13, protocol: decisions, provider: openrouter}
  - {name: gpt-4o, provider: openrouter}
tenants:
  - {name: boxes, models: [typesafe/jev-1.13, jev-1.13, gpt-4o], user_daily_cost_quota_micros: 20}
  - {name: other, models: [gpt-4o]}
access_keys:
  - {ak: ak-box, product: p, tenant: boxes, owner: alice, qps: 100, daily_token_quota: 1000000}
  - {ak: ak-other, product: p, tenant: other, owner: bob, qps: 100, daily_token_quota: 1000000}
"#;

const DECISION: &str = r#"{
  "model": "typesafe/jev-1.13",
  "state": {"tree": "[button] Save  [button] Cancel"},
  "questions": {"save": {"type": "choice", "instructions": "Which element saves?",
                          "criteria": {"b1": "first button", "b2": "second button"}}},
  "session_id": "s-1"
}"#;

const UPSTREAM_REPLY: &str = r#"{
  "id": "gen-dec-1790015143-AIaTutprXsJ5EwohRSjb",
  "model": "typesafe/jev-1.13-20260917",
  "provider": "TypeSafe",
  "answers": {"save": {"type": "choice", "choice": "b1", "confidence": 0.9,
                        "probabilities": {"b1": 0.95, "b2": 0.05}}},
  "usage": {"input_tokens": 476, "output_tokens": 70, "cost": 0.000019992}
}"#;

#[derive(Debug, Default)]
struct OpenRouter {
    seen: Mutex<Vec<UpstreamRequest>>,
    started: Notify,
    release: Option<Semaphore>,
}

impl OpenRouter {
    fn last(&self) -> (String, String, Value) {
        let seen = self.seen.lock().unwrap();
        let req = seen.last().expect("an upstream call");
        let auth = req
            .headers
            .iter()
            .find(|(k, _)| *k == "authorization")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        (
            req.url.clone(),
            auth,
            serde_json::from_slice(&req.body).unwrap(),
        )
    }

    fn calls(&self) -> usize {
        self.seen.lock().unwrap().len()
    }
}

#[async_trait::async_trait]
impl Transport for OpenRouter {
    async fn send(&self, req: UpstreamRequest) -> gw_models::GResult<UpstreamResponse> {
        self.seen.lock().unwrap().push(req);
        if let Some(release) = &self.release {
            self.started.notify_one();
            release.acquire().await.unwrap().forget();
        }
        Ok(UpstreamResponse {
            status: 200,
            body: UpstreamBody::Json(bytes::Bytes::from_static(UPSTREAM_REPLY.as_bytes())),
            headers: HeaderMap::new(),
        })
    }
}

fn gateway() -> (Router, Arc<GatewayState>, Arc<OpenRouter>) {
    gateway_with(CONFIG)
}

fn gateway_with(yaml: &str) -> (Router, Arc<GatewayState>, Arc<OpenRouter>) {
    // SAFETY: one process-constant value under a name only this file reads.
    unsafe { std::env::set_var("GW_TEST_JEV_UPSTREAM_KEY", "sk-or-upstream") };
    let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
    let state = Arc::new(GatewayState::from_config(&cfg));
    let vendor = Arc::new(OpenRouter::default());
    let app = gw_views::app(gw_views::AppState::new(cfg, state.clone(), vendor.clone()));
    (app, state, vendor)
}

fn post(uri: &str, ak: Option<&str>, body: &str) -> Request<Body> {
    let mut b = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(ak) = ak {
        b = b.header("authorization", format!("Bearer {ak}"));
    }
    b.body(Body::from(body.to_owned())).unwrap()
}

#[tokio::test]
async fn a_decision_needs_a_gateway_key() {
    let (app, _, vendor) = gateway();
    for ak in [None, Some("ak-unknown")] {
        let resp = app
            .clone()
            .oneshot(post("/v1/decisions", ak, DECISION))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{ak:?}");
    }
    assert_eq!(vendor.calls(), 0);
}

#[tokio::test]
async fn an_unentitled_tenant_is_refused_before_the_vendor() {
    let (app, state, vendor) = gateway();
    let resp = app
        .oneshot(post("/v1/decisions", Some("ak-other"), DECISION))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(vendor.calls(), 0);
    assert_eq!(state.store.ledger_snapshot(10).await.unwrap().0, 0);
}

#[tokio::test]
async fn a_chat_model_is_not_a_decisions_model() {
    let (app, _, vendor) = gateway();
    let resp = app
        .oneshot(post(
            "/v1/decisions",
            Some("ak-box"),
            &DECISION.replace("typesafe/jev-1.13", "gpt-4o"),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(vendor.calls(), 0);
}

#[tokio::test]
async fn a_decision_without_questions_is_a_400() {
    let (app, _, vendor) = gateway();
    let resp = app
        .oneshot(post(
            "/v1/decisions",
            Some("ak-box"),
            r#"{"model":"typesafe/jev-1.13","state":"x"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(vendor.calls(), 0);
}

#[tokio::test]
async fn a_decision_forwards_with_the_upstream_key_and_passes_the_reply_through() {
    let (app, _, vendor) = gateway();
    let resp = app
        .oneshot(post("/v1/decisions", Some("ak-box"), DECISION))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = body_json(resp).await;
    let want: Value = serde_json::from_str(UPSTREAM_REPLY).unwrap();
    assert_eq!(
        got, want,
        "the vendor reply, id included, reaches the client as is"
    );

    let (url, auth, body) = vendor.last();
    assert_eq!(url, "https://openrouter.ai/api/alpha/decisions");
    assert_eq!(auth, "Bearer sk-or-upstream");
    let sent: Value = serde_json::from_str(DECISION).unwrap();
    assert_eq!(body, sent, "every client field reaches the vendor");
}

#[tokio::test]
async fn systemone_forwards_to_the_typesafe_sdk_path() {
    let (app, _, vendor) = gateway();
    let body = DECISION.replace("typesafe/jev-1.13", "jev-1.13");
    let resp = app
        .oneshot(post("/v1/systemone", Some("ak-box"), &body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let (url, _, sent) = vendor.last();
    assert_eq!(url, "https://openrouter.ai/api/v1/systemone");
    assert_eq!(sent["model"], "jev-1.13");
}

#[tokio::test]
async fn a_decision_bills_the_vendor_cost_to_the_user() {
    let (app, state, _) = gateway();
    let resp = app
        .oneshot(post("/v1/decisions", Some("ak-box"), DECISION))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let (count, rows) = state.store.ledger_snapshot(10).await.unwrap();
    assert_eq!(count, 1);
    let row = &rows[0];
    assert_eq!(row.protocol, "decisions");
    assert_eq!(row.tenant, "boxes");
    assert_eq!(row.user_id, "alice");
    assert_eq!(row.model, "typesafe/jev-1.13");
    assert_eq!(row.prompt_tokens, 476);
    assert_eq!(row.completion_tokens, 70);
    assert_eq!(
        row.cost_micros, 20,
        "usage.cost wins over the 900000 per 1k list price"
    );
    assert_eq!(row.vendor_cost_micros, 20);
}

#[tokio::test]
async fn a_versioned_openrouter_endpoint_keeps_both_decision_paths() {
    let (app, _, vendor) = gateway_with(
        &CONFIG
            .replace(
                "kind: openrouter,",
                "kind: openrouter, endpoint: \"https://openrouter.ai/api/v1\",",
            )
            .replace(", user_daily_cost_quota_micros: 20", ""),
    );
    for (uri, want) in [
        ("/v1/decisions", "https://openrouter.ai/api/alpha/decisions"),
        ("/v1/systemone", "https://openrouter.ai/api/v1/systemone"),
    ] {
        let resp = app
            .clone()
            .oneshot(post(uri, Some("ak-box"), DECISION))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(vendor.last().0, want);
    }
}

#[tokio::test]
async fn a_tenant_price_override_wins_over_the_vendor_cost() {
    let (app, state, _) = gateway_with(&CONFIG.replace(
        "user_daily_cost_quota_micros: 20}",
        "model_prices: {typesafe/jev-1.13: {input_price_per_1k_micros: 1000}}}",
    ));
    let resp = app
        .oneshot(post("/v1/decisions", Some("ak-box"), DECISION))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let rows = state.store.ledger_snapshot(10).await.unwrap().1;
    assert_eq!(rows[0].cost_micros, 476);
    assert_eq!(rows[0].vendor_cost_micros, 20);
}

#[tokio::test]
async fn a_variant_served_decision_echoes_the_requested_name() {
    let (app, _, vendor) = gateway_with(
        &CONFIG
            .replace(
                "  - {name: gpt-4o, provider: openrouter}",
                "  - {name: gpt-4o, provider: openrouter}\n  - {name: jev, protocol: decisions, provider: openrouter, variants: [{model: jev-1.13, weight: 1}]}",
            )
            .replace("models: [typesafe/jev-1.13,", "models: [jev, typesafe/jev-1.13,"),
    );
    let body = DECISION.replace("typesafe/jev-1.13", "jev");
    let resp = app
        .oneshot(post("/v1/decisions", Some("ak-box"), &body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(vendor.last().2["model"], "jev-1.13");
    assert_eq!(body_json(resp).await["model"], "jev");
}

#[tokio::test]
async fn an_exhausted_user_budget_refuses_the_next_decision() {
    let (app, state, vendor) = gateway();
    let resp = app
        .clone()
        .oneshot(post("/v1/decisions", Some("ak-box"), DECISION))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = app
        .oneshot(post("/v1/decisions", Some("ak-box"), DECISION))
        .await
        .unwrap();
    assert_ne!(resp.status(), StatusCode::OK);
    assert_eq!(
        body_json(resp).await["error"]["code"],
        "service_quota_exceeded_exception"
    );
    assert_eq!(
        vendor.calls(),
        1,
        "the refused call never reaches the vendor"
    );
    assert_eq!(state.store.ledger_snapshot(10).await.unwrap().0, 1);
}

#[tokio::test]
async fn an_endpoint_less_decisions_account_answers_from_the_mock() {
    let cfg = Arc::new(
        GatewayConfig::from_yaml(
            "listen: {host: h, port: 0}\nmodels: [{name: jev, protocol: decisions}]\naccounts: [{name: a, provider: p, protocols: [decisions]}]\naccess_keys: [{ak: k, product: p, qps: 10, daily_token_quota: 100000}]",
        )
        .unwrap(),
    );
    let state = Arc::new(GatewayState::from_config(&cfg));
    let app = gw_views::app(gw_views::AppState::new(
        cfg,
        state.clone(),
        Arc::new(gw_engines::MockTransport),
    ));
    let resp = app
        .oneshot(post(
            "/v1/decisions",
            Some("k"),
            &json!({"model": "jev", "state": "x", "questions": {
                "q": {"type": "noul", "instructions": "y?"},
                "score": {"type": "score", "instructions": "How urgent?",
                          "criteria": ["Low", {"severity": "Medium"}, ["High"]]}
            }})
            .to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = body_json(resp).await;
    assert_eq!(got["answers"]["q"]["type"], "noul");
    assert_eq!(
        got["answers"]["score"],
        json!({
            "type": "score", "score": 0.0, "confidence": 1.0,
            "legend": {"0": "Low", "1": {"severity": "Medium"}, "2": ["High"]},
            "probabilities": {"0": 1.0, "1": 0.0, "2": 0.0}
        })
    );
    let rows = state.store.ledger_snapshot(10).await.unwrap().1;
    assert_eq!(rows[0].cost_micros, rows[0].vendor_cost_micros);
    assert!(rows[0].cost_micros > 0, "{:?}", rows[0]);
}

#[tokio::test]
async fn large_decisions_reserve_daily_quota_and_tpm_while_in_flight() {
    for uri in ["/v1/decisions", "/v1/systemone"] {
        for field in ["state", "questions"] {
            for (daily, tpm, status) in [
                (1024, None, StatusCode::BAD_REQUEST),
                (1_000_000, Some(1024), StatusCode::TOO_MANY_REQUESTS),
            ] {
                let mut cfg = GatewayConfig::from_yaml(
                    &CONFIG.replace(", user_daily_cost_quota_micros: 20", ""),
                )
                .unwrap();
                cfg.access_keys[0].daily_token_quota = daily;
                cfg.access_keys[0].tokens_per_minute = tpm;
                let state = Arc::new(GatewayState::from_config(&cfg));
                let vendor = Arc::new(OpenRouter {
                    release: Some(Semaphore::new(0)),
                    ..Default::default()
                });
                let app = gw_views::app(gw_views::AppState::new(
                    Arc::new(cfg),
                    state.clone(),
                    vendor.clone(),
                ));
                let mut body: Value = serde_json::from_str(DECISION).unwrap();
                if field == "state" {
                    body["state"] = json!({"tree": [{"text": "word ".repeat(4096)}]});
                } else {
                    body["questions"]["save"]["instructions"] =
                        json!({"context": ["word ".repeat(4096)]});
                }
                let body = body.to_string();
                let first = tokio::spawn(app.clone().oneshot(post(uri, Some("ak-box"), &body)));
                tokio::time::timeout(Duration::from_secs(2), vendor.started.notified())
                    .await
                    .expect("first request reached the upstream");
                let second = tokio::time::timeout(
                    Duration::from_secs(2),
                    app.oneshot(post(uri, Some("ak-box"), &body)),
                )
                .await;
                vendor.release.as_ref().unwrap().add_permits(2);
                assert_eq!(first.await.unwrap().unwrap().status(), StatusCode::OK);
                let response = second
                    .expect("second request must be refused before the upstream")
                    .unwrap();
                assert_eq!(response.status(), status, "{uri} {field} {tpm:?}");
                assert_eq!(vendor.calls(), 1);
                assert_eq!(state.store.ledger_snapshot(10).await.unwrap().0, 1);
                assert_eq!(
                    state
                        .governance
                        .quota_used(&gw_config::access_key_id("ak-box"))
                        .await,
                    546
                );
            }
        }
    }
}
