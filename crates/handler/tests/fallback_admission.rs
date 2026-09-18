use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use gw_config::{ConfigError, GatewayConfig};
use gw_consts::{ErrCode, Protocol};
use gw_engines::transport::{Transport, UpstreamBody, UpstreamRequest, UpstreamResponse};
use gw_handler::OnlineHandler;
use gw_models::{ChatMsg, GResult, GatewayError, GatewayRequest, ModelParamV2};
use gw_state::{GatewayState, SharedConfig};
use serde_json::Value;

#[derive(Debug, Default)]
struct Vendor(AtomicUsize);

#[async_trait::async_trait]
impl Transport for Vendor {
    async fn send(&self, req: UpstreamRequest) -> GResult<UpstreamResponse> {
        self.0.fetch_add(1, Ordering::Relaxed);
        let body: Value = serde_json::from_slice(&req.body)
            .map_err(|e| GatewayError::internal(format!("decode upstream request JSON: {e}")))?;
        let (status, body): (u16, &'static [u8]) = match body["model"].as_str() {
            Some("broken") => (503, br#"{"error":{"message":"vendor down"}}"#),
            Some("throttled") => (429, br#"{"error":{"message":"rate limited"}}"#),
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
            assert_eq!(vendor.0.load(Ordering::Relaxed), 3);
            let ak = state.auth.authenticate("k").await.expect("access key");
            let err = h
                .run(request("healthy", online), ak)
                .await
                .err()
                .expect("a new request must consume its own permit");
            assert!(err.message.contains(denial), "{err}");
            assert_eq!(vendor.0.load(Ordering::Relaxed), 3);
            assert_eq!(state.governance.quota_used("k").await, 2);
            assert!(
                state
                    .governance
                    .token_window_reserve("k", 1, 3, gw_consts::MINUTE)
                    .await
            );
            assert!(
                !state
                    .governance
                    .token_window_reserve("k", 1, 3, gw_consts::MINUTE)
                    .await
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
    for (qps, limits, denial) in [
        (0.0, "tenants: [{name: t}]", "rate limit exceeded for key"),
        (100.0, "tenants: [{name: t, qps: 0}]", "tenant rate limit"),
        (
            100.0,
            "tenants: [{name: t}]\nproducts: [{name: p, qpm: 0}]",
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
        assert_eq!(err.code, ErrCode::STOP_LIMIT_MSG);
        assert!(err.message.contains(denial), "{err}");
        assert_eq!(vendor.0.load(Ordering::Relaxed), 0);
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
    assert_eq!(vendor.0.load(Ordering::Relaxed), 2);
    assert_eq!(state.governance.quota_used("k").await, 0);
    assert!(
        state
            .governance
            .token_window_reserve("k", 1, 1, gw_consts::MINUTE)
            .await
    );
    assert_eq!(state.store.ledger_snapshot(10).await.expect("ledger").0, 0);
}
