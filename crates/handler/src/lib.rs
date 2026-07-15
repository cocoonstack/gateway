//! Request orchestration (L4): the seam between HTTP views and the DAG.
//! `OnlineHandler` runs the plugin pre-stage (security block / DLP), the four
//! DAG layers, then the plugin post-stage; `OfflineHandler` reuses the same
//! chain for batches.

pub mod offline;
pub mod plugins;

use std::collections::HashMap;
use std::sync::Arc;

use futures::FutureExt;
use gw_config::GatewayConfig;
use gw_dag::DagContext;
use gw_engines::http_transport::UpstreamPolicy;
use gw_engines::{EngineOutcome, SharedTransport};
use gw_models::{GResult, GatewayError, GatewayRequest, GatewayResponse};
use gw_state::{AkInfo, GatewayState, SharedConfig};

pub use offline::{BatchItem, OfflineHandler};

use std::sync::atomic::{AtomicU64, Ordering};

static REQ_SEQ: AtomicU64 = AtomicU64::new(0);

/// Record a content-safety outcome (no prompt text) against this request's
/// key/user/tenant. Best-effort: a store failure is logged, never fails the
/// request. Surface = the request protocol, or "batch" for offline items.
async fn emit_security_event(ctx: &DagContext, rule: &str, action: &str, hits: i64) {
    let user_id = ctx
        .ak
        .owner
        .as_deref()
        .or(ctx.request.user_id.as_deref())
        .unwrap_or_default()
        .to_owned();
    let surface = if ctx.request.is_online {
        ctx.request
            .model_param_v2
            .as_ref()
            .map(|p| p.protocol.as_str())
            .unwrap_or_default()
            .to_owned()
    } else {
        "batch".to_owned()
    };
    let event = gw_state::SecurityEvent {
        created_at_epoch_secs: gw_state::epoch_secs(),
        request_id: ctx.request.request_id.clone(),
        ak: ctx.ak.ak.clone(),
        user_id,
        tenant: ctx.ak.tenant.clone(),
        surface,
        rule: rule.to_owned(),
        action: action.to_owned(),
        hits,
    };
    if let Err(e) = ctx.state.store.security_event_add(&event).await {
        tracing::warn!(error = %e, "security event write failed");
    }
}

/// A per-request correlation id: `req-<epoch_ms>-<seq>`, time-sortable and
/// unique within the process (the seq disambiguates same-millisecond requests).
pub fn new_request_id() -> String {
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("req-{ms}-{}", REQ_SEQ.fetch_add(1, Ordering::Relaxed))
}

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
        let handler = Self {
            config,
            transport,
            plan,
        };
        handler.push_policies(&handler.cfg());
        handler
    }

    /// The live config snapshot (cheap atomic load). Introspection surfaces read
    /// through this so a runtime reload takes effect immediately.
    pub fn cfg(&self) -> Arc<GatewayConfig> {
        self.config.load().cfg.clone()
    }

    pub fn state(&self) -> Arc<GatewayState> {
        self.config.load().state.clone()
    }

    /// Swap in a new config and push the transport policies derived from it as
    /// one step, so config and upstream policy can never desync. On error the
    /// old snapshot (and its policies) stay live.
    pub async fn reload(&self, cfg: GatewayConfig) -> GResult<()> {
        let handoff = self.config.reload(cfg).await;
        if handoff.is_ok() {
            self.push_policies(&self.cfg());
        }
        handoff
    }

    /// Run one request: plugin pre → DAG (4 layers) → plugin post.
    /// The returned context carries the outcome, decision log, billing effects.
    pub async fn run(&self, mut request: GatewayRequest, ak: AkInfo) -> GResult<DagContext> {
        if request.request_id.is_empty() {
            request.request_id = new_request_id();
        }
        // one consistent snapshot for the whole request
        let snap = self.config.load();
        // outbound DLP is a response-buffering boundary: a masked span can
        // straddle deltas, so no engine may stream raw ones — enforced here so
        // no caller can opt out
        let dlp = snap.cfg.security.dlp_redact;
        if dlp {
            request.stream_tx = None;
        }
        // blocklist on the ORIGINAL content, before DLP — else a blocklisted term
        // inside a redacted span (a domain in an email) is masked out and slips
        if let Some(block) = plugins::security_check(&snap.cfg.security, &mut request) {
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
            emit_security_event(&ctx, "blocklist", "block", 1).await;
            return Ok(ctx);
        }
        let redacted = plugins::dlp_redact_request(&snap.cfg.security, &mut request);

        let mut ctx = DagContext::new(
            snap.cfg.clone(),
            snap.state.clone(),
            self.transport.clone(),
            request,
            ak,
        );
        if redacted > 0 {
            ctx.decide("dlp", format!("redacted {redacted} span(s) inbound"));
            emit_security_event(&ctx, "dlp", "redact", redacted as i64).await;
        }

        // a panicking node must refund too, not leak the reserves; unwind-safe —
        // the refund reads only plain ctx fields (ak, state, quota_at, the
        // reserves), each written whole by its node, so a panic can't tear them
        let ran = std::panic::AssertUnwindSafe(gw_dag::run(&self.plan, &mut ctx))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(GatewayError::internal("pipeline panicked")));
        if let Err(e) = ran {
            // a failed pipeline refunds its reservations whole, on the reserve's day bucket
            ctx.state
                .governance
                .refund_reserves(
                    &ctx.ak.ak,
                    ctx.quota_reserved.take().unwrap_or(0),
                    ctx.tpm_reserved.take(),
                    ctx.quota_at,
                )
                .await;
            return Err(e);
        }

        // a fallback served a different model; surfaces echo the requested name
        if let Some(requested) = ctx
            .request
            .model_param_v2
            .as_mut()
            .and_then(|p| p.fallback_from.take())
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
            emit_security_event(&ctx, "dlp", "redact_out", redacted_out as i64).await;
        }
        Ok(ctx)
    }

    /// Derive the upstream policies (timeouts/connect-retries) from `cfg` and
    /// apply them to the transport live.
    fn push_policies(&self, cfg: &GatewayConfig) {
        let default = UpstreamPolicy::default();
        let per_account: HashMap<String, UpstreamPolicy> = cfg
            .accounts
            .iter()
            .filter(|a| a.timeout_seconds.is_some() || a.connect_retries.is_some())
            .map(|a| {
                (
                    a.name.clone(),
                    UpstreamPolicy {
                        timeout: a
                            .timeout_seconds
                            .map(std::time::Duration::from_secs)
                            .unwrap_or(default.timeout),
                        connect_retries: a.connect_retries.unwrap_or(default.connect_retries),
                    },
                )
            })
            .collect();
        self.transport.reload_policies(default, per_account);
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
        let h = OnlineHandler::new(handler().config.clone(), Arc::new(PiiStream));
        assert!(h.cfg().security.dlp_redact, "default config has DLP on");
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let mut req = chat_req("gpt-4o", "hello");
        req.stream = true;
        req.stream_tx = Some(tx);
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
    async fn blocklist_runs_before_dlp_redaction() {
        let mut cfg = GatewayConfig::embedded_default().unwrap();
        cfg.security.dlp_redact = true;
        cfg.security.blocklist = vec!["example.com".into()];
        let cfg = Arc::new(cfg);
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        );
        let ctx = h
            .run(
                chat_req("gpt-4o", "reach me at ops@example.com"),
                ak(&h).await,
            )
            .await
            .unwrap();
        let out = ctx.outcome.expect("outcome");
        assert_eq!(
            out.response.finish_reason, "content_filter",
            "blocklist must catch a term inside a redactable span"
        );
    }

    #[derive(Debug)]
    struct PanickingTransport;

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for PanickingTransport {
        async fn send(
            &self,
            _req: gw_engines::transport::UpstreamRequest,
        ) -> GResult<gw_engines::transport::UpstreamResponse> {
            panic!("mock upstream panicked");
        }
    }

    #[tokio::test]
    async fn panicking_pipeline_refunds_reserves() {
        let cfg = Arc::new(GatewayConfig::embedded_default().unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(PanickingTransport),
        );
        let ak = ak(&h).await;
        let err = h
            .run(chat_req("gpt-4o", "boom"), ak.clone())
            .await
            .err()
            .expect("panic must surface as an error");
        assert_eq!(err.http_status, 500);
        assert_eq!(
            h.state().governance.quota_used(&ak.ak).await,
            0,
            "reserves refunded after a panicking pipeline"
        );
    }

    #[tokio::test]
    async fn admin_key_orphaned_by_tenant_removal_fails_closed() {
        let with_t1 = GatewayConfig::from_yaml(
            "listen: {host: h, port: 1}\nmodels: [{name: m1, protocol: openai-chat}]\naccounts: [{name: a1, provider: openai, protocols: ['openai-chat']}]\ntenants: [{name: t1, models: [m1]}]",
        )
        .unwrap();
        let cfg = Arc::new(with_t1);
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        );
        let key = gw_state::AkInfo {
            ak: "ak-t1".into(),
            product: "p".into(),
            tenant: "t1".into(),
            owner: None,
            qps: 10.0,
            daily_token_quota: 100_000,
            tokens_per_minute: None,
            expires_at_epoch_secs: None,
            banned: false,
            model_quotas: Default::default(),
        };
        h.state()
            .auth
            .put(key.clone(), gw_state::KeySource::Admin)
            .await
            .unwrap();
        assert!(
            h.run(chat_req("m1", "hi"), key.clone()).await.is_ok(),
            "entitled while t1 is declared"
        );
        let without_t1 = GatewayConfig::from_yaml(
            "listen: {host: h, port: 1}\nmodels: [{name: m1, protocol: openai-chat}]\naccounts: [{name: a1, provider: openai, protocols: ['openai-chat']}]",
        )
        .unwrap();
        h.reload(without_t1).await.unwrap();
        assert!(
            h.state().auth.authenticate("ak-t1").await.is_some(),
            "admin key survives the reload"
        );
        let err = h
            .run(chat_req("m1", "hi"), key)
            .await
            .err()
            .expect("orphaned tenant must fail closed, not become unrestricted");
        assert_eq!(err.http_status, 403);
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
