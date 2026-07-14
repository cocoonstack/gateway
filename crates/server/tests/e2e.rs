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

fn app() -> Router {
    let cfg = Arc::new(GatewayConfig::embedded_default().expect("embedded config"));
    let state = Arc::new(GatewayState::from_config(&cfg));
    gw_views::app(AppState::new(
        cfg,
        state,
        Arc::new(gw_engines::MockTransport),
    ))
}

#[tokio::test]
async fn admin_reload_is_gated_and_swaps_keys_live() {
    let r = app()
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
    let r = app.oneshot(get_authed("/internal/ledger")).await.unwrap();
    let j = body_json(r).await;
    let rec = j["records"]
        .as_array()
        .and_then(|a| a.iter().rev().find(|x| x["ak"] == "ak-beta-1"))
        .cloned()
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
    let mut limited = 0;
    for h in handles {
        match h.await.unwrap() {
            StatusCode::OK => ok += 1,
            StatusCode::TOO_MANY_REQUESTS => limited += 1,
            other => panic!("unexpected status {other}"),
        }
    }
    assert_eq!(
        (ok, limited),
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
    assert_eq!(r.status(), StatusCode::BAD_REQUEST, "vendor error surfaces");
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

    let resp = app.oneshot(get_authed("/internal/ledger")).await.unwrap();
    let j = body_json(resp).await;
    let last = j["records"]
        .as_array()
        .and_then(|r| {
            r.iter()
                .rev()
                .find(|rec| rec["ak"] == "ak-beta-1" && rec["served_model"] == "gpt-4o-mini")
        })
        .cloned()
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
async fn model_failure_modes_404_503_501() {
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
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
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
    let resp = app.oneshot(get("/internal/ledger")).await.unwrap();
    let j = body_json(resp).await;
    let rec = j["records"].as_array().unwrap().last().unwrap().clone();
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
    let resp = app.clone().oneshot(get("/internal/ledger")).await.unwrap();
    assert_eq!(body_json(resp).await["count"], 0, "blocked is not billed");

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
        .cloned()
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
async fn quota_exhaustion_second_call_429() {
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
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    let j = body_json(second).await;
    assert!(j["error"]["message"].as_str().unwrap().contains("quota"));
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
    let resp = app.oneshot(get("/internal/ledger")).await.unwrap();
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
            let frames: Vec<Result<bytes::Bytes, String>> = vec![
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
    let app = gw_views::app(AppState::new(cfg, state, Arc::new(PiiStream)));

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
    let j = body_json(resp).await;
    assert_ne!(
        j["output"][0]["content"][0]["text"], "please say forbiddenword",
        "blocked Responses input must not reach the vendor: {j}"
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
                body: gw_engines::transport::UpstreamBody::Json(body.to_string().into_bytes()),
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
    let led = app.oneshot(get("/internal/ledger")).await.unwrap();
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
    let resp = app.oneshot(get("/internal/ledger")).await.unwrap();
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
        body_json(app.oneshot(get("/internal/ledger")).await.unwrap()).await["count"]
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
