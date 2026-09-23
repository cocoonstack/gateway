use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use gw_config::{ConfigError, GatewayConfig};
use gw_consts::{ErrCode, Protocol};
use gw_engines::transport::{Transport, UpstreamBody, UpstreamRequest, UpstreamResponse};
use gw_handler::OnlineHandler;
use gw_models::{ChatMsg, GResult, GatewayError, GatewayRequest, ModelParamV2};
use gw_state::{GatewayState, SharedConfig, admission};
use serde_json::Value;

#[derive(Debug, Default)]
struct Vendor(AtomicUsize);

impl Vendor {
    fn calls(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl Transport for Vendor {
    async fn send(&self, req: UpstreamRequest) -> GResult<UpstreamResponse> {
        self.0.fetch_add(1, Ordering::Relaxed);
        let body: Value = serde_json::from_slice(&req.body)
            .map_err(|e| GatewayError::internal(format!("decode upstream request JSON: {e}")))?;
        let (status, body): (u16, &'static [u8]) = match body["model"].as_str() {
            Some("broken") => (503, br#"{"error":{"message":"vendor down"}}"#),
            Some("throttled") => (429, br#"{"error":{"message":"rate limited"}}"#),
            Some("proxied") => (502, b"<html><body>502 Bad Gateway</body></html>"),
            Some("oversized") => (413, b"Request Entity Too Large"),
            Some("healthy") => (
                200,
                br#"{"model":"healthy","choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#,
            ),
            model => panic!("unexpected upstream model: {model:?}"),
        };
        Ok(UpstreamResponse {
            status,
            body: UpstreamBody::Json(Bytes::from_static(body)),
            headers: Default::default(),
        })
    }
}

fn handler(
    key_qps: f64,
    limits: &str,
    model_qpm: u32,
) -> Result<(OnlineHandler, Arc<Vendor>), ConfigError> {
    let yaml = format!(
        "listen: {{host: h, port: 1}}
{limits}
access_keys: [{{ak: k, tenant: t, product: p, qps: {key_qps}, daily_token_quota: 100000, tokens_per_minute: 1000}}]
models:
  - {{name: unavailable, protocol: openai-chat, provider: absent, fallback_models: [broken, throttled, healthy]}}
  - {{name: broken, protocol: openai-chat, fallback_models: [throttled, healthy]}}
  - {{name: throttled, protocol: openai-chat}}
  - {{name: proxied, protocol: openai-chat, fallback_models: [healthy]}}
  - {{name: oversized, protocol: openai-chat, fallback_models: [healthy]}}
  - {{name: healthy, protocol: openai-chat, qpm: {model_qpm}}}
accounts: [{{name: a, provider: p, protocols: [openai-chat]}}]"
    );
    let cfg = Arc::new(GatewayConfig::from_yaml(&yaml)?);
    let state = Arc::new(GatewayState::from_config(&cfg));
    let vendor = Arc::new(Vendor::default());
    let transport = Arc::clone(&vendor);
    Ok((
        OnlineHandler::new(SharedConfig::new(cfg, state), transport),
        vendor,
    ))
}

fn request(model: &str, online: bool) -> GatewayRequest {
    GatewayRequest {
        is_online: online,
        message: vec![ChatMsg::text("user", "hi")],
        model_param_v2: Some(ModelParamV2::with_name(Protocol::OpenaiChat, model)),
        ..Default::default()
    }
}

#[tokio::test]
async fn upstream_failures_fall_back_along_the_chain() {
    let (h, vendor) = handler(100.0, "tenants: [{name: t}]", 100).expect("fallback config");
    let ak = h.state().auth.authenticate("k").await.expect("access key");
    let ctx = h
        .run(request("broken", true), ak)
        .await
        .expect("the chain recovers");
    assert_eq!(vendor.calls(), 3);
    let outcome = ctx.outcome.as_ref().expect("outcome");
    assert_eq!(
        outcome.response.model, "broken",
        "the caller sees the requested name"
    );
    let trail = ctx.decisions_line();
    assert!(
        trail.contains("fallback: broken -> throttled: vendor down")
            && trail.contains("fallback: throttled -> healthy: rate limited"),
        "{trail}"
    );
    assert!(
        trail.contains("resolve_model: healthy -> openai-chat"),
        "{trail}"
    );
    let (_, rows) = h.state().store.ledger_snapshot(10).await.expect("ledger");
    assert_eq!(
        (rows[0].model.as_str(), rows[0].served_model.as_str()),
        ("broken", "healthy")
    );
}

#[tokio::test]
async fn fallback_skips_unentitled_models_and_gateway_denials_never_fall_back() {
    let (h, vendor) = handler(
        100.0,
        "tenants: [{name: t, models: [broken, healthy]}]",
        100,
    )
    .expect("fallback config");
    let ak = h.state().auth.authenticate("k").await.expect("access key");
    let ctx = h
        .run(request("broken", true), ak)
        .await
        .expect("the chain recovers");
    assert_eq!(vendor.calls(), 2, "throttled is skipped unserved");
    let trail = ctx.decisions_line();
    assert!(
        trail.contains("fallback: broken -> healthy: vendor down"),
        "{trail}"
    );

    let ak = h.state().auth.authenticate("k").await.expect("access key");
    let err = h
        .run(request("throttled", true), ak)
        .await
        .err()
        .expect("an unentitled model is a gateway denial");
    assert_eq!(err.http_status, 403);
    assert_eq!(vendor.calls(), 2, "a gateway denial reaches no vendor");
}

#[tokio::test]
async fn a_served_fallback_samples_one_success_for_the_requested_model() {
    let (h, _) = handler(
        100.0,
        "tenants: [{name: t, models: [broken, healthy]}]",
        100,
    )
    .expect("fallback config");
    let ak = h.state().auth.authenticate("k").await.expect("access key");
    h.run(request("broken", true), ak)
        .await
        .expect("the chain recovers");
    let avail = &h.state().avail;
    avail.flush().await;
    let minute = gw_state::epoch_secs() / 60;
    assert_eq!(
        avail.window("broken", minute - 5, minute).await,
        (1, 0),
        "the client saw one success; the failed first attempt is not a sample"
    );
}

#[tokio::test]
async fn an_exhausted_chain_reports_the_last_upstream_error() {
    let (h, vendor) = handler(
        100.0,
        "tenants: [{name: t, models: [broken, throttled]}]",
        100,
    )
    .expect("fallback config");
    let ak = h.state().auth.authenticate("k").await.expect("access key");
    let err = h
        .run(request("broken", true), ak)
        .await
        .err()
        .expect("the chain ends in the throttled vendor");
    assert_eq!(vendor.calls(), 2);
    assert_eq!(err.original_status(), Some(429));
    assert!(err.message.contains("rate limited"), "{}", err.message);
}

#[tokio::test]
async fn a_non_json_error_body_keeps_the_vendor_status() {
    let (h, vendor) = handler(100.0, "tenants: [{name: t}]", 100).expect("fallback config");
    let ak = h.state().auth.authenticate("k").await.expect("access key");
    let ctx = h
        .run(request("proxied", true), ak.clone())
        .await
        .expect("a proxy's 502 page falls back");
    let trail = ctx.decisions_line();
    assert!(trail.contains("fallback: proxied -> healthy"), "{trail}");
    let err = h
        .run(request("oversized", true), ak)
        .await
        .err()
        .expect("a 413 page is the vendor's refusal");
    assert_eq!((err.http_status, err.original_status()), (413, Some(413)));
    assert_eq!(
        vendor.calls(),
        3,
        "a 4xx page neither fails over nor falls back"
    );
}

#[tokio::test]
async fn fallback_consumes_each_request_limit_once() {
    for (qps, limits, denial) in [
        (0.01, "tenants: [{name: t}]", "rate limit exceeded for key"),
        (
            100.0,
            "tenants: [{name: t, qps: 0.01}]",
            "tenant rate limit",
        ),
        (
            100.0,
            "tenants: [{name: t}]\nproducts: [{name: p, qpm: 1}]",
            "product qpm limit",
        ),
    ] {
        for (start, online) in [("broken", true), ("unavailable", true), ("broken", false)] {
            let (h, vendor) = handler(qps, limits, 100).expect("fallback config");
            let state = h.state();
            let ak = state.auth.authenticate("k").await.expect("access key");
            let ctx = h
                .run(request(start, online), ak)
                .await
                .unwrap_or_else(|e| panic!("{start}, {denial}: {e}"));
            assert_eq!(ctx.outcome.as_ref().expect("outcome").response.model, start);
            assert_eq!(vendor.calls(), 3);
            let ak = state.auth.authenticate("k").await.expect("access key");
            let err = h
                .run(request("healthy", online), ak)
                .await
                .err()
                .expect("a new request must consume its own permit");
            assert!(err.message.contains(denial), "{err}");
            assert_eq!(vendor.calls(), 3);
            assert_eq!(state.governance.quota_used("k").await, 2);
            assert!(
                state
                    .governance
                    .token_window_reserve("k", 1, 3, gw_consts::MINUTE)
                    .await
                    .is_some()
            );
            assert!(
                state
                    .governance
                    .token_window_reserve("k", 1, 3, gw_consts::MINUTE)
                    .await
                    .is_none()
            );
            let (_, rows) = state.store.ledger_snapshot(10).await.expect("ledger");
            assert_eq!(rows.len(), 1);
            assert_eq!(
                (rows[0].model.as_str(), rows[0].served_model.as_str()),
                (start, "healthy")
            );
        }
    }
}

#[tokio::test]
async fn no_account_fallback_does_not_bypass_initial_request_limits() {
    for (qps, limits, code, denial) in [
        (
            0.0,
            "tenants: [{name: t}]",
            ErrCode::STOP_LIMIT_MSG,
            "rate limit exceeded for key",
        ),
        (
            100.0,
            "tenants: [{name: t, qps: 0}]",
            ErrCode::POOLED_LIMIT_MSG,
            "tenant rate limit",
        ),
        (
            100.0,
            "tenants: [{name: t}]\nproducts: [{name: p, qpm: 0}]",
            ErrCode::POOLED_LIMIT_MSG,
            "product qpm limit",
        ),
    ] {
        let (h, vendor) = handler(qps, limits, 100).expect("fallback config");
        let state = h.state();
        let ak = state.auth.authenticate("k").await.expect("access key");
        let err = h
            .run(request("unavailable", true), ak)
            .await
            .err()
            .expect("fallback must still pass initial admission");
        assert_eq!(err.code, code);
        assert!(err.message.contains(denial), "{err}");
        assert_eq!(vendor.calls(), 0);
        assert_eq!(state.governance.quota_used("k").await, 0);
    }
}

#[tokio::test]
async fn fallback_keeps_model_qpm_and_refunds_token_reservations() {
    let (h, vendor) = handler(100.0, "tenants: [{name: t}]", 0).expect("fallback config");
    let state = h.state();
    let ak = state.auth.authenticate("k").await.expect("access key");
    let err = h
        .run(request("broken", true), ak)
        .await
        .err()
        .expect("the fallback model's QPM is exhausted");
    assert!(err.message.contains("model qpm limit"), "{err}");
    assert_eq!(vendor.calls(), 2);
    assert_eq!(state.governance.quota_used("k").await, 0);
    assert!(
        state
            .governance
            .token_window_reserve("k", 1, 1, gw_consts::MINUTE)
            .await
            .is_some()
    );
    assert_eq!(state.store.ledger_snapshot(10).await.expect("ledger").0, 0);
}

#[tokio::test]
async fn quota_degradation_preserves_the_requested_model_and_fallback_chain() {
    for (tenant_fallback, degraded_serves) in [("healthy", true), ("unavailable", false)] {
        let tenants = format!(
            "tenants: [{{name: t, fallback_model: {tenant_fallback}, model_quotas: {{throttled: 1}}}}]"
        );
        let (h, vendor) = handler(100.0, &tenants, 100).expect("fallback config");
        let state = h.state();
        let quota_key = admission::model_quota_key("k", "throttled");
        state.governance.quota_consume(&quota_key, 1).await;
        let ak = state.auth.authenticate("k").await.expect("access key");
        let ctx = h
            .run(request("broken", true), ak)
            .await
            .expect("fallback must recover");
        assert_eq!(
            ctx.outcome.as_ref().expect("outcome").response.model,
            "broken"
        );
        assert_eq!(vendor.calls(), 2);
        let trail = ctx.decisions_line();
        assert!(trail.contains("fallback: broken -> throttled:"), "{trail}");
        assert!(
            trail.contains(&format!(
                "model_quota: throttled over 1, serving {tenant_fallback}"
            )),
            "{trail}"
        );
        assert_eq!(
            trail.contains("fallback: unavailable -> healthy:"),
            !degraded_serves,
            "{trail}"
        );
        let (count, rows) = state.store.ledger_snapshot(10).await.expect("ledger");
        assert_eq!((count, rows.len()), (1, 1));
        let row = &rows[0];
        assert_eq!(
            (row.model.as_str(), row.served_model.as_str()),
            ("broken", "healthy")
        );
        assert_eq!(state.governance.quota_used("k").await, row.total_tokens);
        let accrued = if degraded_serves { row.total_tokens } else { 0 };
        assert_eq!(state.governance.quota_used(&quota_key).await, 1 + accrued);
    }
}
