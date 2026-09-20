use std::sync::Arc;

use gw_config::{ConfigError, GatewayConfig};
use gw_consts::{ErrCode, Protocol};
use gw_handler::OnlineHandler;
use gw_models::{ChatMsg, GatewayRequest, ModelParamV2};
use gw_state::{GatewayState, KeyStatus, SharedConfig};

fn handler(qps: f64, tpm: i64, pools: &str, model_qpm: u32) -> Result<OnlineHandler, ConfigError> {
    let yaml = format!(
        "listen: {{host: h, port: 1}}
abuse: {{tiers: [{{rejects: 2, suspend_hours: 1}}]}}
{pools}
access_keys:
  - {{ak: flooder, tenant: t, product: p, qps: {qps}, tokens_per_minute: {tpm}, daily_token_quota: 100000}}
  - {{ak: bystander, tenant: t, product: p, qps: 100, daily_token_quota: 100000}}
models: [{{name: m, protocol: openai-chat, qpm: {model_qpm}}}]
accounts: [{{name: a, provider: openai, protocols: [openai-chat]}}]"
    );
    let cfg = Arc::new(GatewayConfig::from_yaml(&yaml)?);
    let state = Arc::new(GatewayState::from_config(&cfg));
    Ok(OnlineHandler::new(
        SharedConfig::new(cfg, state),
        Arc::new(gw_engines::MockTransport),
    ))
}

fn request() -> GatewayRequest {
    GatewayRequest {
        is_online: true,
        message: vec![ChatMsg::text("user", "hi")],
        model_param_v2: Some(ModelParamV2::with_name(Protocol::OpenaiChat, "m")),
        ..Default::default()
    }
}

#[tokio::test]
async fn pooled_admission_denials_never_accrue_key_abuse() {
    for (pools, model_qpm, reason) in [
        ("tenants: [{name: t, qps: 0}]", 100, "tenant rate limit"),
        (
            "tenants: [{name: t}]\nproducts: [{name: p, qpm: 0}]",
            100,
            "product qpm limit",
        ),
        ("tenants: [{name: t}]", 0, "model qpm limit"),
    ] {
        let h = handler(100.0, 100000, pools, model_qpm).expect("pooled limit config");
        let state = h.state();
        for _ in 0..3 {
            let key = state
                .auth
                .authenticate("flooder")
                .await
                .expect("access key");
            let err = h.run(request(), key).await.err().expect("pooled denial");
            assert_eq!(
                (err.code, err.http_status),
                (ErrCode::POOLED_LIMIT_MSG, 429)
            );
            assert!(err.message.contains(reason), "{err}");
            assert_eq!(state.governance.quota_used("abuse:flooder").await, 0);
            let fresh = state.auth.authenticate("flooder").await.expect("fresh key");
            assert_eq!(fresh.status_at(gw_state::epoch_secs()), KeyStatus::Active);
        }
    }
}

#[tokio::test]
async fn key_admission_denials_accrue_abuse_and_suspend() {
    for (qps, tpm, reason) in [
        (0.0, 100000, "rate limit exceeded for key"),
        (100.0, 0, "token-per-minute limit"),
    ] {
        let h = handler(qps, tpm, "tenants: [{name: t}]", 100).expect("key limit config");
        let state = h.state();
        for (rejects, status) in [(1, KeyStatus::Active), (2, KeyStatus::Suspended)] {
            let key = state
                .auth
                .authenticate("flooder")
                .await
                .expect("access key");
            let err = h.run(request(), key).await.err().expect("key denial");
            assert_eq!((err.code, err.http_status), (ErrCode::STOP_LIMIT_MSG, 429));
            assert!(err.message.contains(reason), "{err}");
            assert_eq!(state.governance.quota_used("abuse:flooder").await, rejects);
            let fresh = state.auth.authenticate("flooder").await.expect("fresh key");
            assert_eq!(fresh.status_at(gw_state::epoch_secs()), status);
        }
    }
}

#[tokio::test]
async fn tenant_exhaustion_suspends_the_flooder_without_penalizing_a_bystander() {
    let h = handler(0.001, 100000, "tenants: [{name: t, qps: 0.001}]", 100)
        .expect("shared tenant config");
    let state = h.state();
    let key = state
        .auth
        .authenticate("flooder")
        .await
        .expect("access key");
    h.run(request(), key)
        .await
        .expect("first request consumes both permits");
    for rejects in 1..=2 {
        let key = state
            .auth
            .authenticate("bystander")
            .await
            .expect("bystander key");
        let err = h.run(request(), key).await.err().expect("pooled denial");
        assert_eq!(err.code, ErrCode::POOLED_LIMIT_MSG);
        assert!(err.message.contains("tenant rate limit"), "{err}");
        assert_eq!(state.governance.quota_used("abuse:bystander").await, 0);
        let fresh = state
            .auth
            .authenticate("bystander")
            .await
            .expect("fresh bystander");
        assert_eq!(fresh.status_at(gw_state::epoch_secs()), KeyStatus::Active);

        let key = state
            .auth
            .authenticate("flooder")
            .await
            .expect("flooder key");
        let err = h.run(request(), key).await.err().expect("key denial");
        assert_eq!(err.code, ErrCode::STOP_LIMIT_MSG);
        assert!(err.message.contains("rate limit exceeded for key"), "{err}");
        assert_eq!(state.governance.quota_used("abuse:flooder").await, rejects);
    }
    let fresh = state
        .auth
        .authenticate("flooder")
        .await
        .expect("fresh flooder");
    assert_eq!(
        fresh.status_at(gw_state::epoch_secs()),
        KeyStatus::Suspended
    );
    let key = state
        .auth
        .authenticate("bystander")
        .await
        .expect("bystander key");
    let err = h
        .run(request(), key)
        .await
        .err()
        .expect("pool remains exhausted");
    assert_eq!(err.code, ErrCode::POOLED_LIMIT_MSG);
    assert_eq!(state.governance.quota_used("abuse:bystander").await, 0);
    let fresh = state
        .auth
        .authenticate("bystander")
        .await
        .expect("fresh bystander");
    assert_eq!(fresh.status_at(gw_state::epoch_secs()), KeyStatus::Active);
}
