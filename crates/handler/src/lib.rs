//! Request orchestration (online/offline; plugin chain inlined).
//!
//! Layer L4: the seam between HTTP views and the DAG. `OnlineHandler` runs the
//! plugin pre-stage (security block / DLP), the four DAG layers, then the plugin
//! post-stage. `OfflineHandler` (offline.rs) reuses the same chain for batches.
//! realtime orchestration (websocket upstream) is not implemented yet.

pub mod offline;
pub mod plugins;

use std::sync::Arc;

use gw_config::GatewayConfig;
use gw_dag::DagContext;
use gw_engines::{EngineOutcome, SharedTransport};
use gw_models::{GResult, GatewayRequest, GatewayResponse};
use gw_state::{AkInfo, GatewayState, SharedConfig};

pub use offline::{BatchItem, OfflineHandler};

/// Runs one request through the plugin pre-stage, the DAG, and the plugin post-stage.
#[derive(Clone)]
pub struct OnlineHandler {
    pub config: SharedConfig,
    pub transport: SharedTransport,
    plan: Arc<gw_dag::Plan>,
}

impl OnlineHandler {
    /// Panics only if the static DAG topology has a cycle — a build-time bug,
    /// caught by tests.
    pub fn new(config: SharedConfig, transport: SharedTransport) -> Self {
        #[allow(clippy::expect_used)]
        let plan =
            Arc::new(gw_dag::Plan::build(gw_dag::default_layers()).expect("static dag topology"));
        Self {
            config,
            transport,
            plan,
        }
    }

    /// The live config snapshot (cheap atomic load). Introspection surfaces read
    /// through this so a runtime reload takes effect immediately.
    pub fn cfg(&self) -> Arc<GatewayConfig> {
        self.config.load().cfg.clone()
    }

    pub fn state(&self) -> Arc<GatewayState> {
        self.config.load().state.clone()
    }

    /// Run one request: plugin pre → DAG (4 layers) → plugin post.
    /// The returned context carries the outcome, decision log, billing effects.
    pub async fn run(&self, mut request: GatewayRequest, ak: AkInfo) -> GResult<DagContext> {
        // one consistent snapshot for the whole request
        let snap = self.config.load();
        // Outbound DLP is a response-buffering boundary: no engine may stream raw
        // deltas, since a masked span can straddle deltas and a live delta leaves
        // before the post-stage scrubs it. Enforce it here at the security
        // boundary so no caller (any view or future surface) can opt out.
        let dlp = snap.cfg.security.dlp_redact;
        if dlp {
            request.stream_tx = None;
        }
        let redacted = plugins::dlp_redact_request(&snap.cfg.security, &mut request);
        if let Some(block) = plugins::security_check(&snap.cfg.security, &request) {
            let mut ctx = DagContext::new(
                snap.cfg.clone(),
                snap.state.clone(),
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
                ..Default::default()
            };
            ctx.outcome = Some(EngineOutcome {
                response,
                http_code: 200,
                block,
                ..Default::default()
            });
            return Ok(ctx);
        }

        let mut ctx = DagContext::new(
            snap.cfg.clone(),
            snap.state.clone(),
            self.transport.clone(),
            request,
            ak,
        );
        if redacted > 0 {
            ctx.decide("dlp", format!("redacted {redacted} span(s) inbound"));
        }

        if let Err(e) = gw_dag::run(&self.plan, &mut ctx).await {
            // a failed pipeline refunds its admission reservations whole, on the
            // day bucket the reserve used
            if let Some(est) = ctx.quota_reserved.take() {
                ctx.state
                    .governance
                    .quota_settle(&ctx.ak.ak, -est, ctx.quota_at)
                    .await;
            }
            if let Some(est) = ctx.tpm_reserved.take() {
                ctx.state
                    .governance
                    .token_window_settle(&ctx.ak.ak, -est, gw_consts::MINUTE)
                    .await;
            }
            return Err(e);
        }

        // a quota fallback served a different model; every surface echoes the
        // requested name (the ledger keeps both)
        if let Some(requested) = ctx
            .request
            .model_param_v2
            .as_ref()
            .and_then(|p| p.fallback_from.clone())
            && let Some(outcome) = ctx.outcome.as_mut()
        {
            outcome.response.model = requested;
        }

        let redacted_out = if let Some(outcome) = ctx.outcome.as_mut() {
            let n = plugins::dlp_redact_response(&snap.cfg.security, &mut outcome.response);
            // raw decoded deltas are pre-redaction; drop them so no downstream
            // reconstruction can replay unmasked text past the boundary
            if dlp {
                outcome.chunks.clear();
            }
            n
        } else {
            0
        };
        if redacted_out > 0 {
            ctx.decide("dlp", format!("redacted {redacted_out} span(s) outbound"));
        }
        Ok(ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gw_consts::Protocol;
    use gw_models::{ChatMsg, ModelParamV2};

    fn handler() -> OnlineHandler {
        let cfg = Arc::new(GatewayConfig::embedded_default().unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        )
    }

    async fn ak(h: &OnlineHandler) -> AkInfo {
        h.state().auth.authenticate("ak-demo-123").await.unwrap()
    }

    async fn wait_terminal(h: &OnlineHandler, id: &str) {
        for _ in 0..100 {
            if let Some(j) = h.state().store.batch_get(id).await.unwrap()
                && matches!(
                    j.status,
                    gw_state::BatchStatus::Completed | gw_state::BatchStatus::Failed
                )
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
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
        let ctx = h
            .run(chat_req("gpt-4o", "hi there"), ak(&h).await)
            .await
            .unwrap();
        let out = ctx.outcome.expect("outcome");
        assert!(out.response.message.contains("you said: hi there"));
        assert!(out.response.common_usage.is_some());
        let (_, ledger) = h.state().store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(ledger.len(), 1);
        assert!(ledger[0].cost_micros > 0);
        assert_eq!(ledger[0].account, "mock-openai-1");
        assert!(ctx.decisions.iter().any(|(n, _)| *n == "resolve_model"));
        assert!(ctx.decisions.iter().any(|(n, _)| *n == "cost_calc"));
    }

    #[tokio::test]
    async fn unknown_model_404() {
        let h = handler();
        let err = h
            .run(chat_req("bogus", "x"), ak(&h).await)
            .await
            .err()
            .unwrap();
        assert_eq!(err.http_status, 404);
    }

    #[tokio::test]
    async fn security_block_short_circuits() {
        let h = handler();
        let ctx = h
            .run(chat_req("gpt-4o", "please say forbiddenword"), ak(&h).await)
            .await
            .unwrap();
        let out = ctx.outcome.expect("outcome");
        assert!(out.block.block);
        assert_eq!(out.response.finish_reason, "content_filter");
        assert!(
            h.state()
                .store
                .ledger_snapshot(usize::MAX)
                .await
                .unwrap()
                .1
                .is_empty()
        );
    }

    #[tokio::test]
    async fn dlp_redacts_round_trip() {
        let h = handler();
        let ctx = h
            .run(
                chat_req("gpt-4o", "mail me at a@b.com and call 13812345678"),
                ak(&h).await,
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
        let ctx = h
            .run(chat_req("hunyuan-lite", "failover please"), ak(&h).await)
            .await
            .unwrap();
        let out = ctx.outcome.expect("outcome");
        assert!(out.response.ptu_spillover);
        assert!(out.response.message.contains("you said: failover please"));
        let (_, ledger) = h.state().store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(ledger.last().unwrap().account, "mock-hunyuan-paygo");
        assert!(ledger.last().unwrap().ptu_spillover);
        assert!(ctx.decisions.iter().any(|(_, w)| w.contains("failover")));
    }

    #[derive(Debug)]
    struct BreakingStream;

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for BreakingStream {
        async fn send(
            &self,
            _req: gw_engines::transport::UpstreamRequest,
        ) -> GResult<gw_engines::transport::UpstreamResponse> {
            use futures::StreamExt;
            let frames: Vec<Result<bytes::Bytes, String>> = vec![
                Ok(bytes::Bytes::from(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"partial answer\"}}]}\n\n",
                )),
                Err("connection reset".to_owned()),
            ];
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::SseStream(
                    futures::stream::iter(frames).boxed(),
                ),
            })
        }
    }

    #[tokio::test]
    async fn aborted_stream_bills_estimated_delivered_tokens() {
        // salvage-partial is a live-streaming behavior; DLP forces buffering, so
        // disable it to exercise the abort path
        let mut cfg = GatewayConfig::embedded_default().unwrap();
        cfg.security.dlp_redact = false;
        let cfg = Arc::new(cfg);
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(BreakingStream),
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let mut req = chat_req("gpt-4o", "please stream something long");
        req.stream = true;
        req.stream_tx = Some(tx);
        let ctx = h.run(req, ak(&h).await).await.unwrap();
        let out = ctx.outcome.expect("outcome");
        assert!(out.response.aborted, "mid-stream break must mark aborted");
        assert_eq!(out.response.message, "partial answer");
        let (_, ledger) = h.state().store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(ledger.len(), 1, "aborted stream must still bill");
        assert!(
            ledger[0].prompt_tokens > 0 && ledger[0].completion_tokens > 0,
            "estimated tokens, not zero: {:?}",
            ledger[0]
        );
    }

    #[derive(Debug)]
    struct BreakingAnthropicStream;

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for BreakingAnthropicStream {
        async fn send(
            &self,
            _req: gw_engines::transport::UpstreamRequest,
        ) -> GResult<gw_engines::transport::UpstreamResponse> {
            use futures::StreamExt;
            let frames: Vec<Result<bytes::Bytes, String>> = vec![
                Ok(bytes::Bytes::from(
                    "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-sonnet\",\"usage\":{\"input_tokens\":100}}}\n\n",
                )),
                Ok(bytes::Bytes::from(
                    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"delivered words here\"}}\n\n",
                )),
                Err("connection reset".to_owned()),
            ];
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::SseStream(
                    futures::stream::iter(frames).boxed(),
                ),
            })
        }
    }

    #[tokio::test]
    async fn aborted_anthropic_stream_bills_delivered_completion() {
        let mut cfg = GatewayConfig::embedded_default().unwrap();
        cfg.security.dlp_redact = false;
        let cfg = Arc::new(cfg);
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(BreakingAnthropicStream),
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let mut req = chat_req("claude-sonnet", "please stream something");
        req.stream = true;
        req.stream_tx = Some(tx);
        let ctx = h.run(req, ak(&h).await).await.unwrap();
        let out = ctx.outcome.expect("outcome");
        assert!(out.response.aborted);
        assert_eq!(out.response.message, "delivered words here");
        let (_, ledger) = h.state().store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(ledger.len(), 1);
        assert_eq!(ledger[0].prompt_tokens, 100);
        assert!(
            ledger[0].completion_tokens > 0,
            "delivered completion must not bill zero: {:?}",
            ledger[0]
        );
    }

    #[derive(Debug)]
    struct PiiStream;

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for PiiStream {
        async fn send(
            &self,
            _req: gw_engines::transport::UpstreamRequest,
        ) -> GResult<gw_engines::transport::UpstreamResponse> {
            use futures::StreamExt;
            let frames: Vec<Result<bytes::Bytes, String>> = vec![
                Ok(bytes::Bytes::from(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"reach me at jane@corp.com now\"}}]}\n\n",
                )),
                Ok(bytes::Bytes::from(
                    "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
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

    #[tokio::test]
    async fn dlp_buffers_stream_and_drops_raw_chunks() {
        // #9: the handler is the DLP boundary — a caller that sets stream_tx
        // under DLP still gets buffered redaction, and the raw pre-redaction
        // chunks are cleared so nothing downstream can replay unmasked text.
        let h = OnlineHandler::new(handler().config.clone(), Arc::new(PiiStream));
        assert!(h.cfg().security.dlp_redact, "default config has DLP on");
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let mut req = chat_req("gpt-4o", "hello");
        req.stream = true;
        req.stream_tx = Some(tx);
        // run() consumes the request; under DLP it clears stream_tx before the
        // engine runs, so the sender is dropped and the live channel stays empty
        let ctx = h.run(req, ak(&h).await).await.unwrap();
        let mut live: Vec<String> = Vec::new();
        while let Ok(chunk) = rx.try_recv() {
            live.push(chunk.delta);
        }
        assert!(
            live.iter().all(|d| !d.contains("jane@corp.com")),
            "no raw delta may reach the live channel under DLP: {live:?}"
        );
        let out = ctx.outcome.expect("outcome");
        assert!(
            out.chunks.is_empty(),
            "raw chunks must be cleared under DLP: {:?}",
            out.chunks
        );
        assert!(
            !out.response.message.contains("jane@corp.com"),
            "email must be redacted: {}",
            out.response.message
        );
    }

    #[tokio::test]
    async fn batch_items_bypass_the_cache_and_bill_each() {
        let h = handler();
        let off = OfflineHandler::new(h.clone());
        let items = vec![
            BatchItem {
                messages: vec![ChatMsg::text("user", "same prompt")],
            },
            BatchItem {
                messages: vec![ChatMsg::text("user", "same prompt")],
            },
        ];
        let job = off
            .submit(ak(&h).await, "cached-mini".into(), items)
            .await
            .unwrap();
        wait_terminal(&h, &job.id).await;
        let (count, _) = h.state().store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(count, 2, "every batch item bills, cache hits or not");
    }

    #[tokio::test]
    async fn batch_runs_all_items() {
        let h = handler();
        let off = OfflineHandler::new(h.clone());
        let job = off
            .submit(
                ak(&h).await,
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
        wait_terminal(&h, &job.id).await;
        let j = h.state().store.batch_get(&job.id).await.unwrap().unwrap();
        assert_eq!(j.status, gw_state::BatchStatus::Completed);
        assert_eq!(j.results.len(), 2);
        assert!(j.results.iter().all(|r| r.ok && r.total_tokens > 0));
        assert_eq!(
            h.state().store.ledger_snapshot(usize::MAX).await.unwrap().0,
            2
        );
    }

    /// Set GW_TEST_PG_URL to run: a batch submitted on one handler is claimed,
    /// executed, and billed by a separate drain loop (the distributed path).
    #[tokio::test]
    async fn distributed_batch_drained_by_a_separate_handler() {
        let Ok(url) = std::env::var("GW_TEST_PG_URL") else {
            return;
        };
        let cfg = Arc::new(GatewayConfig::embedded_default().unwrap());
        let mut st = GatewayState::from_config(&cfg);
        st.store = Arc::new(
            gw_state::PostgresStore::connect(&url)
                .await
                .expect("pg store"),
        );
        let state = Arc::new(st);
        let online = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state.clone()),
            Arc::new(gw_engines::MockTransport),
        );
        let submitter = OfflineHandler::new(online.clone());
        let ak = state.auth.authenticate("ak-demo-123").await.unwrap();

        assert!(state.store.distributed_batches());
        let job = submitter
            .submit(
                ak,
                "gpt-4o-mini".into(),
                vec![
                    BatchItem {
                        messages: vec![ChatMsg::text("user", "alpha")],
                    },
                    BatchItem {
                        messages: vec![ChatMsg::text("user", "beta")],
                    },
                ],
            )
            .await
            .unwrap();
        let pending = state.store.batch_get(&job.id).await.unwrap().unwrap();
        assert_eq!(pending.status, gw_state::BatchStatus::Pending);

        let drainer = OfflineHandler::new(online);
        let drain = tokio::spawn(async move {
            drainer
                .drain_forever(120, std::time::Duration::from_millis(50))
                .await
        });
        let mut completed = None;
        for _ in 0..200 {
            if let Some(j) = state.store.batch_get(&job.id).await.unwrap()
                && matches!(j.status, gw_state::BatchStatus::Completed)
            {
                completed = Some(j);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        drain.abort();
        let j = completed.expect("drain completed the batch");
        assert_eq!(j.results.len(), 2, "both items executed exactly once");
        assert!(j.results.iter().all(|r| r.ok && r.total_tokens > 0));
    }
}
