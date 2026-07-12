//! Request orchestration (online/offline; plugin chain inlined).
//!
//! Layer L4: the seam between HTTP views and the DAG. `OnlineHandler` runs the
//! plugin pre-stage (security block / DLP), the four DAG layers, then the plugin
//! post-stage. `OfflineHandler` (offline.rs) reuses the same chain for batches.
//! realtime orchestration (websocket upstream) is not implemented yet.

pub mod offline;
pub mod plugins;

use std::sync::Arc;

use ap_config::GatewayConfig;
use ap_dag::DagContext;
use ap_engines::{EngineOutcome, SharedTransport};
use ap_models::{GResult, GatewayRequest, GatewayResponse};
use ap_state::{AkInfo, GatewayState};

pub use offline::{BatchItem, OfflineHandler};

/// Runs one request through the plugin pre-stage, the DAG, and the plugin post-stage.
#[derive(Clone)]
pub struct OnlineHandler {
    pub cfg: Arc<GatewayConfig>,
    pub state: Arc<GatewayState>,
    pub transport: SharedTransport,
}

impl OnlineHandler {
    pub fn new(
        cfg: Arc<GatewayConfig>,
        state: Arc<GatewayState>,
        transport: SharedTransport,
    ) -> Self {
        Self {
            cfg,
            state,
            transport,
        }
    }

    /// Run one request: plugin pre → DAG (4 layers) → plugin post.
    /// The returned context carries the outcome, decision log, billing effects.
    pub async fn run(&self, mut request: GatewayRequest, ak: AkInfo) -> GResult<DagContext> {
        // --- plugin pre-stage ---
        let redacted = plugins::dlp_redact_request(&self.cfg.security, &mut request);
        if let Some(block) = plugins::security_check(&self.cfg.security, &request) {
            // security block hit: skips the engine and billing, returns the block message
            let mut ctx = DagContext::new(
                self.cfg.clone(),
                self.state.clone(),
                self.transport.clone(),
                request,
                ak,
            );
            ctx.decide(
                "security_check",
                format!("blocked (code {})", block.err_code),
            );
            let response = GatewayResponse {
                message: block.message.clone(),
                finish_reason: "content_filter".to_owned(),
                http_code: 200,
                ..Default::default()
            };
            ctx.outcome = Some(EngineOutcome {
                response,
                http_code: 200,
                block,
                chunks: vec![],
            });
            return Ok(ctx);
        }

        let mut ctx = DagContext::new(
            self.cfg.clone(),
            self.state.clone(),
            self.transport.clone(),
            request,
            ak,
        );
        if redacted > 0 {
            ctx.decide("dlp", format!("redacted {redacted} span(s) inbound"));
        }

        let layers = ap_dag::default_layers();
        ap_dag::run(&layers, &mut ctx).await?;

        // --- plugin post-stage: outbound redaction ---
        if let Some(outcome) = ctx.outcome.as_mut() {
            let n = plugins::dlp_redact_response(&self.cfg.security, &mut outcome.response);
            if n > 0 {
                ctx.decisions
                    .push(format!("dlp: redacted {n} span(s) outbound"));
            }
        }
        Ok(ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ap_consts::Protocol;
    use ap_models::{ChatMsg, ModelParamV2};

    fn handler() -> OnlineHandler {
        let cfg = Arc::new(GatewayConfig::embedded_default().unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        OnlineHandler::new(cfg, state, Arc::new(ap_engines::MockTransport))
    }

    fn ak(h: &OnlineHandler) -> AkInfo {
        h.state.auth.authenticate("ak-demo-123").unwrap()
    }

    fn chat_req(name: &str, content: &str) -> GatewayRequest {
        GatewayRequest {
            is_online: true,
            message: vec![ChatMsg::text("user", content)],
            model_param_v2: Some(ModelParamV2::with_name(Protocol::OpenaiChat, name)),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn full_pipeline_openai() {
        let h = handler();
        let ctx = h.run(chat_req("gpt-4o", "hi there"), ak(&h)).await.unwrap();
        let out = ctx.outcome.expect("outcome");
        assert!(out.response.message.contains("you said: hi there"));
        assert!(out.response.common_usage.is_some());
        let ledger = h.state.store.ledger_snapshot().await.unwrap();
        assert_eq!(ledger.len(), 1);
        assert!(ledger[0].cost_micros > 0);
        assert_eq!(ledger[0].account, "mock-openai-1");
        assert!(ctx.decisions.iter().any(|d| d.starts_with("resolve_model")));
        assert!(ctx.decisions.iter().any(|d| d.starts_with("cost_calc")));
    }

    #[tokio::test]
    async fn unknown_model_404() {
        let h = handler();
        let err = h.run(chat_req("bogus", "x"), ak(&h)).await.err().unwrap();
        assert_eq!(err.http_status, 404);
    }

    #[tokio::test]
    async fn security_block_short_circuits() {
        let h = handler();
        let ctx = h
            .run(chat_req("gpt-4o", "please say forbiddenword"), ak(&h))
            .await
            .unwrap();
        let out = ctx.outcome.expect("outcome");
        assert!(out.block.block);
        assert_eq!(out.response.finish_reason, "content_filter");
        assert!(h.state.store.ledger_snapshot().await.unwrap().is_empty()); // not billed
    }

    #[tokio::test]
    async fn dlp_redacts_round_trip() {
        let h = handler();
        let ctx = h
            .run(
                chat_req("gpt-4o", "mail me at a@b.com and call 13812345678"),
                ak(&h),
            )
            .await
            .unwrap();
        let msg = ctx.outcome.unwrap().response.message;
        assert!(msg.contains("[REDACTED_EMAIL]"), "{msg}");
        assert!(msg.contains("[REDACTED_PHONE]"), "{msg}");
        assert!(!msg.contains("a@b.com"));
    }

    #[tokio::test]
    async fn ptu_failover_spills_to_paygo() {
        let h = handler();
        // hunyuan-lite: PTU account name contains "down" -> mock 503 -> fails over to paygo
        let ctx = h
            .run(chat_req("hunyuan-lite", "failover please"), ak(&h))
            .await
            .unwrap();
        let out = ctx.outcome.expect("outcome");
        assert!(out.response.ptu_spillover);
        assert!(out.response.message.contains("you said: failover please"));
        let ledger = h.state.store.ledger_snapshot().await.unwrap();
        assert_eq!(ledger.last().unwrap().account, "mock-hunyuan-paygo");
        assert!(ledger.last().unwrap().ptu_spillover);
        assert!(ctx.decisions.iter().any(|d| d.contains("failover")));
    }

    #[tokio::test]
    async fn batch_runs_all_items() {
        let h = handler();
        let off = OfflineHandler::new(h.clone());
        let job = off
            .submit(
                ak(&h),
                "gpt-4o-mini".into(),
                vec![
                    BatchItem {
                        messages: vec![ChatMsg::text("user", "one")],
                    },
                    BatchItem {
                        messages: vec![ChatMsg::text("user", "two")],
                    },
                ],
            )
            .await
            .unwrap();
        // poll until the background task finishes
        for _ in 0..100 {
            if let Some(j) = h.state.store.batch_get(&job.id).await.unwrap()
                && matches!(
                    j.status,
                    ap_state::BatchStatus::Completed | ap_state::BatchStatus::Failed
                )
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let j = h.state.store.batch_get(&job.id).await.unwrap().unwrap();
        assert_eq!(j.status, ap_state::BatchStatus::Completed);
        assert_eq!(j.results.len(), 2);
        assert!(j.results.iter().all(|r| r.ok && r.total_tokens > 0));
        assert_eq!(h.state.store.ledger_snapshot().await.unwrap().len(), 2);
    }
}
