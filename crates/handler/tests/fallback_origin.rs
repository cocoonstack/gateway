use std::sync::Arc;

use gw_config::GatewayConfig;
use gw_consts::Protocol;
use gw_engines::MockTransport;
use gw_handler::OnlineHandler;
use gw_models::{ChatMsg, GatewayRequest, ModelParamV2};
use gw_state::{GatewayState, SharedConfig, admission};

#[tokio::test]
async fn quota_degradation_preserves_the_requested_model_and_fallback_chain() {
    for (degraded_account, served) in [("tenant-ok", "degraded"), ("tenant-down", "healthy")] {
        let yaml = format!(
            "listen: {{host: h, port: 1}}
tenants: [{{name: t, fallback_model: degraded, model_quotas: {{limited: 1}}}}]
access_keys: [{{ak: k, tenant: t, product: p, qps: 100, daily_token_quota: 100000}}]
models:
  - {{name: source, protocol: openai-chat, provider: primary, fallback_models: [limited, healthy]}}
  - {{name: limited, protocol: openai-chat}}
  - {{name: healthy, protocol: openai-chat, provider: healthy}}
  - {{name: degraded, protocol: openai-chat, provider: degraded}}
accounts:
  - {{name: primary-down, provider: primary, protocols: [openai-chat]}}
  - {{name: {degraded_account}, provider: degraded, protocols: [openai-chat]}}
  - {{name: healthy, provider: healthy, protocols: [openai-chat]}}"
        );
        let cfg = Arc::new(GatewayConfig::from_yaml(&yaml).expect("fallback config"));
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(SharedConfig::new(cfg, state), Arc::new(MockTransport));
        let state = h.state();
        let quota_key = admission::model_quota_key("k", "limited");
        state.governance.quota_consume(&quota_key, 1).await;
        let ak = state.auth.authenticate("k").await.expect("access key");
        let request = GatewayRequest {
            is_online: true,
            message: vec![ChatMsg::text("user", "hi")],
            model_param_v2: Some(ModelParamV2::with_name(Protocol::OpenaiChat, "source")),
            ..Default::default()
        };
        let ctx = h.run(request, ak).await.expect("fallback must recover");
        assert_eq!(
            ctx.outcome.as_ref().expect("outcome").response.model,
            "source"
        );
        let trail = ctx.decisions_line();
        assert!(trail.contains("fallback: source -> limited:"), "{trail}");
        assert!(
            trail.contains("model_quota: limited over 1, serving degraded"),
            "{trail}"
        );
        assert_eq!(
            trail.contains("fallback: degraded -> healthy:"),
            served == "healthy",
            "{trail}"
        );
        let (count, rows) = state.store.ledger_snapshot(10).await.expect("ledger");
        assert_eq!(count, 1);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(
            (row.model.as_str(), row.served_model.as_str()),
            ("source", served)
        );
        assert_eq!(state.governance.quota_used("k").await, row.total_tokens);
        let limited_tokens = if served == "degraded" {
            row.total_tokens
        } else {
            0
        };
        assert_eq!(
            state.governance.quota_used(&quota_key).await,
            1 + limited_tokens
        );
    }
}
