//! Request orchestration (L4): the seam between HTTP views and the DAG.
//! `OnlineHandler` runs the plugin pre-stage (security block / DLP), the four
//! DAG layers, then the plugin post-stage; `OfflineHandler` reuses the same
//! chain for batches.

pub mod moderation;
pub mod offline;
pub mod plugins;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::FutureExt;
use gw_config::GatewayConfig;
use gw_dag::DagContext;
use gw_engines::http_transport::UpstreamPolicy;
use gw_engines::{EngineOutcome, SharedTransport};
use gw_models::{Block, GResult, GatewayError, GatewayRequest, GatewayResponse};
use gw_state::{AkInfo, GatewayState, SharedConfig};

pub use gw_models::BatchItem;
pub use offline::OfflineHandler;

const MODERATION_UNAVAILABLE: &str = "content moderation is unavailable";

static REQ_SEQ: AtomicU64 = AtomicU64::new(0);

/// Runs one request through the plugin pre-stage, the DAG, and the plugin post-stage.
#[derive(Clone)]
pub struct OnlineHandler {
    pub config: SharedConfig,
    pub transport: SharedTransport,
    plan: Arc<gw_dag::Plan>,
    moderator: Arc<dyn moderation::Moderator>,
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
            moderator: moderation::default_moderator(),
        };
        handler.push_policies(&handler.cfg());
        handler
    }

    /// Plug an external content moderator into the pre-stage (enable per tenant
    /// via `security.moderate`).
    pub fn with_moderator(mut self, moderator: Arc<dyn moderation::Moderator>) -> Self {
        self.moderator = moderator;
        self
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
        let sec = snap.cfg.security_for(&ak.tenant);
        // outbound redaction (PII or secrets) is a response-buffering boundary:
        // a masked span can straddle deltas, so no engine may stream raw ones —
        // enforced here so no caller can opt out
        let dlp = sec.redacts_output();
        if dlp {
            request.stream_tx = None;
        }
        // scan the ORIGINAL content pre-DLP: a blocklisted term inside a redacted span would slip
        let scan = plugins::security_check(sec, &mut request);

        let mut ctx = DagContext::new(
            snap.cfg.clone(),
            snap.state.clone(),
            self.transport.clone(),
            request,
            ak,
        );
        // every fired rule is recorded (block/flag/shadow alike); only a block-action hit denies
        for hit in &scan.hits {
            emit_security_event(&ctx, &hit.rule, hit.action.as_str(), hit.count).await;
        }
        if let Some(block) = scan.block {
            ctx.decide(
                "security_check",
                format!("blocked (code {})", block.err_code),
            );
            ctx.outcome = Some(content_filter_outcome(block));
            return Ok(ctx);
        }

        // pre-DLP text, computed once for moderation and the retained prompt
        let retention = snap
            .cfg
            .retention_for(&ctx.ak.tenant)
            .copied()
            .filter(|r| r.content != gw_config::ContentLevel::None);
        let inbound =
            (sec.moderate || retention.is_some()).then(|| plugins::inbound_text(&mut ctx.request));

        if sec.moderate
            && let Some(block) = self
                .moderate(&ctx, sec, inbound.as_deref().unwrap_or_default())
                .await
        {
            ctx.decide("moderation", "denied");
            ctx.outcome = Some(content_filter_outcome(block));
            return Ok(ctx);
        }

        let redacted = plugins::dlp_redact_request(sec, &mut ctx.request);
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

        // raw response pre-outbound-DLP, only when full retention can store it (key present)
        let capture_raw = matches!(retention, Some(r) if r.content == gw_config::ContentLevel::Full)
            && gw_state::sealing_available();
        let raw_response = capture_raw
            .then(|| ctx.outcome.as_ref().map(|o| o.response.message.clone()))
            .flatten();

        let redacted_out = if let Some(outcome) = ctx.outcome.as_mut() {
            let n = plugins::dlp_redact_response(sec, &mut outcome.response);
            // raw decoded deltas are pre-redaction; drop them so no downstream
            // reconstruction can replay unmasked text past the boundary
            if dlp {
                outcome.chunks.clear();
            }
            n
        } else {
            0
        };
        if let Some(r) = retention {
            persist_content(
                &ctx,
                r,
                capture_raw,
                inbound.unwrap_or_default(),
                raw_response,
            )
            .await;
        }
        if redacted_out > 0 {
            ctx.decide("dlp", format!("redacted {redacted_out} span(s) outbound"));
            emit_security_event(&ctx, "dlp", "redact_out", redacted_out as i64).await;
        }
        Ok(ctx)
    }

    /// Run the wired moderator over raw text; `Some(reason)` to deny, `None` to
    /// allow. The seam the realtime surface uses (it has no `DagContext`); the
    /// caller records the security event on its own surface.
    pub async fn moderate_text(&self, sec: &gw_config::SecurityConf, text: &str) -> Option<String> {
        match self.moderation(sec, text).await {
            Moderation::Allow => None,
            Moderation::Deny(reason) => Some(reason),
            Moderation::Unavailable => Some(MODERATION_UNAVAILABLE.to_owned()),
        }
    }

    /// Run the wired moderator over the request's pre-DLP inbound `text`;
    /// `Some(Block)` to deny. Records a security event on a moderator deny.
    async fn moderate(
        &self,
        ctx: &DagContext,
        sec: &gw_config::SecurityConf,
        text: &str,
    ) -> Option<Block> {
        match self.moderation(sec, text).await {
            Moderation::Allow => None,
            Moderation::Deny(reason) => {
                emit_security_event(ctx, "moderation", "block", 1).await;
                Some(Block::blocked(
                    reason,
                    gw_consts::ErrCode::EMPTY_RESP.value() as i32,
                ))
            }
            Moderation::Unavailable => Some(Block::blocked(
                MODERATION_UNAVAILABLE,
                gw_consts::ErrCode::SYSTEM_ERROR.value() as i32,
            )),
        }
    }

    /// The one moderator-verdict resolution every surface shares, so the
    /// fail-open posture can't drift between REST and realtime.
    async fn moderation(&self, sec: &gw_config::SecurityConf, text: &str) -> Moderation {
        match self.moderator.review(text).await {
            Ok(moderation::Verdict::Allow) => Moderation::Allow,
            Ok(moderation::Verdict::Deny(reason)) => Moderation::Deny(reason),
            Err(e) => {
                tracing::warn!(error = %e, fail_open = sec.moderation_fail_open, "moderator error");
                if sec.moderation_fail_open {
                    Moderation::Allow
                } else {
                    Moderation::Unavailable
                }
            }
        }
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

/// One resolved moderator verdict: `Unavailable` is a moderator error under a
/// fail-closed posture (fail-open resolves to `Allow`).
enum Moderation {
    Allow,
    Deny(String),
    Unavailable,
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

/// The 200-with-`content_filter` outcome every pre-stage denial returns.
fn content_filter_outcome(block: Block) -> EngineOutcome {
    EngineOutcome {
        response: GatewayResponse {
            message: block.message.clone(),
            finish_reason: "content_filter".to_owned(),
            ..Default::default()
        },
        http_code: 200,
        block,
        ..Default::default()
    }
}

/// Record a content-safety outcome (no prompt text) against this request's
/// key/user/tenant. Best-effort. Surface = the request protocol, or "batch"
/// for offline items.
async fn emit_security_event(ctx: &DagContext, rule: &str, action: &str, hits: i64) {
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
    gw_state::SecurityEvent {
        created_at_epoch_secs: gw_state::epoch_secs(),
        request_id: ctx.request.request_id.clone(),
        ak: ctx.ak.ak.clone(),
        user_id: ctx.effective_user_id().to_owned(),
        tenant: ctx.ak.tenant.clone(),
        surface,
        rule: rule.to_owned(),
        action: action.to_owned(),
        hits,
    }
    .record(ctx.state.store.as_ref())
    .await;
}

/// Persist this request's prompt and response per the tenant's retention policy.
/// `inbound` is the pre-DLP prompt text; `store_full` (resolved once by the
/// caller: full level AND a content key) stores it sealed, else it is stored
/// PII/secret-stripped — retention owns that redaction, so a row can't hold
/// raw content even with DLP off. Best-effort — a store failure is logged.
async fn persist_content(
    ctx: &DagContext,
    retention: gw_config::RetentionConf,
    store_full: bool,
    inbound: String,
    raw_response: Option<String>,
) {
    if retention.content == gw_config::ContentLevel::Full && !store_full {
        tracing::warn!("full retention configured but GW_CONTENT_KEY unset; storing redacted text");
    }

    let now = gw_state::epoch_secs();
    let expires = if retention.days > 0 {
        now + retention.days as i64 * 86_400
    } else {
        0
    };
    let redacted_response = || {
        plugins::redact_retained(
            ctx.outcome
                .as_ref()
                .map(|o| o.response.message.as_str())
                .unwrap_or_default(),
        )
    };
    let prompt = if store_full {
        inbound
    } else {
        plugins::redact_retained(&inbound)
    };
    let response = if store_full {
        raw_response.unwrap_or_else(redacted_response)
    } else {
        redacted_response()
    };

    let writes = [("prompt", prompt), ("response", response)]
        .into_iter()
        .filter(|(_, text)| !text.is_empty())
        .map(|(kind, text)| {
            // seal whenever a key exists (defense in depth even for redacted text)
            let (content, sealed) = match gw_state::content::seal(&text) {
                Some(ct) => (ct, true),
                None => (text, false),
            };
            let record = gw_state::ContentRecord {
                created_at_epoch_secs: now,
                request_id: ctx.request.request_id.clone(),
                ak: ctx.ak.ak.clone(),
                user_id: ctx.effective_user_id().to_owned(),
                tenant: ctx.ak.tenant.clone(),
                kind: kind.to_owned(),
                content,
                sealed,
                expires_at_epoch_secs: expires,
            };
            async move {
                if let Err(e) = ctx.state.store.content_add(&record).await {
                    tracing::warn!(error = %e, kind, "content retention write failed");
                }
            }
        });
    futures::future::join_all(writes).await;
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

    /// MockTransport with the usage rewritten to 100 prompt tokens, 80 cached.
    #[derive(Debug)]
    struct CachedUsageTransport;

    #[async_trait::async_trait]
    impl gw_engines::Transport for CachedUsageTransport {
        async fn send(
            &self,
            req: gw_engines::UpstreamRequest,
        ) -> GResult<gw_engines::UpstreamResponse> {
            let mut resp = gw_engines::MockTransport.send(req).await?;
            if let gw_engines::UpstreamBody::Json(b) = &mut resp.body {
                let mut v: serde_json::Value = serde_json::from_slice(b).unwrap();
                v["usage"]["prompt_tokens"] = 100.into();
                v["usage"]["prompt_tokens_details"] = serde_json::json!({"cached_tokens": 80});
                *b = serde_json::to_vec(&v).unwrap();
            }
            Ok(resp)
        }
    }

    #[tokio::test]
    async fn token_rate_discounts_cached_prompt_cost() {
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: m-cache, protocol: openai-chat, input_price_per_1k_micros: 1000, token_rate: {read_cache: 0.1}}]\naccounts: [{name: a1, provider: openai, protocols: ['openai-chat']}]\naccess_keys: [{ak: k1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(CachedUsageTransport),
        );
        let key = h.state().auth.authenticate("k1").await.unwrap();
        h.run(chat_req("m-cache", "hi"), key).await.unwrap();
        let (_, ledger) = h.state().store.ledger_snapshot(usize::MAX).await.unwrap();
        let rec = &ledger[0];
        assert_eq!(rec.prompt_tokens, 100, "raw column: 20 fresh + 80 cached");
        assert_eq!(
            rec.cost_micros, 28,
            "20 + 80*0.1 billable at 1000 micros/1k"
        );
        assert_eq!(rec.total_tokens, 28 + rec.completion_tokens);
    }

    #[tokio::test]
    async fn failover_recovery_records_one_availability_success() {
        // a-down (priority 1) always 503s, a-up recovers via failover; the
        // model must sample exactly one success, no error
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: m, protocol: openai-chat, provider: p}]\naccounts: [{name: a-down, provider: p, priority: 1, protocols: ['openai-chat']}, {name: a-up, provider: p, priority: 2, protocols: ['openai-chat']}]\nstability: {failure_threshold: 100}\naccess_keys: [{ak: k1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        );
        let key = h.state().auth.authenticate("k1").await.unwrap();
        let ctx = h.run(chat_req("m", "hi"), key).await.unwrap();
        assert!(
            ctx.decisions
                .iter()
                .any(|(_, w)| w.contains("failover a-down -> a-up")),
            "request must have gone through failover: {:?}",
            ctx.decisions
        );
        let avail = &h.state().avail;
        avail.flush().await;
        let minute = gw_state::epoch_secs() / 60;
        assert_eq!(avail.window("m", minute - 5, minute).await, (1, 0));
    }

    #[tokio::test]
    async fn variant_split_bills_requested_serves_target() {
        // tenant entitled only to the public name: entitlement precedes the swap
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: pub-m, protocol: openai-chat, variants: [{model: canary-m, weight: 1}]}, {name: canary-m, protocol: openai-chat}]\naccounts: [{name: a1, provider: openai, protocols: ['openai-chat']}]\ntenants: [{name: t1, models: [pub-m]}]\naccess_keys: [{ak: k1, tenant: t1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        );
        let key = h.state().auth.authenticate("k1").await.unwrap();
        let ctx = h.run(chat_req("pub-m", "hi"), key).await.unwrap();
        assert_eq!(
            ctx.outcome.expect("outcome").response.model,
            "pub-m",
            "response echoes the requested public name"
        );
        let (_, ledger) = h.state().store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(ledger[0].model, "pub-m");
        assert_eq!(ledger[0].served_model, "canary-m");
    }

    #[derive(Debug)]
    struct DenyModerator;

    #[async_trait::async_trait]
    impl moderation::Moderator for DenyModerator {
        async fn review(&self, _text: &str) -> Result<moderation::Verdict, String> {
            Ok(moderation::Verdict::Deny("nope".into()))
        }
    }

    #[tokio::test]
    async fn moderator_denies_when_tenant_enables_it() {
        let mut cfg = GatewayConfig::embedded_default().unwrap();
        cfg.security.moderate = true;
        let cfg = Arc::new(cfg);
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        )
        .with_moderator(Arc::new(DenyModerator));
        let ctx = h
            .run(chat_req("gpt-4o", "hello"), ak(&h).await)
            .await
            .unwrap();
        let out = ctx.outcome.expect("outcome");
        assert!(out.block.block);
        assert_eq!(out.response.finish_reason, "content_filter");
        assert!(
            h.state()
                .store
                .ledger_snapshot(1)
                .await
                .unwrap()
                .1
                .is_empty(),
            "a moderated deny bills nothing"
        );
    }

    #[derive(Debug)]
    struct ErrModerator;

    #[async_trait::async_trait]
    impl moderation::Moderator for ErrModerator {
        async fn review(&self, _text: &str) -> Result<moderation::Verdict, String> {
            Err("moderator upstream down".into())
        }
    }

    #[tokio::test]
    async fn moderate_text_allow_deny_and_failure_posture() {
        let cfg = Arc::new(GatewayConfig::embedded_default().unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let base = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        );
        let mut sec = gw_config::SecurityConf::default();
        assert_eq!(base.moderate_text(&sec, "x").await, None, "default allows");
        let deny = base.clone().with_moderator(Arc::new(DenyModerator));
        assert_eq!(deny.moderate_text(&sec, "x").await.as_deref(), Some("nope"));
        let err = base.with_moderator(Arc::new(ErrModerator));
        sec.moderation_fail_open = true;
        assert_eq!(
            err.moderate_text(&sec, "x").await,
            None,
            "error + fail-open allows"
        );
        sec.moderation_fail_open = false;
        assert!(
            err.moderate_text(&sec, "x").await.is_some(),
            "error + fail-closed denies"
        );
    }

    #[tokio::test]
    async fn per_user_budget_denies_over_the_cap() {
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: gpt-4o, protocol: openai-chat}]\naccounts: [{name: a1, provider: openai, protocols: ['openai-chat']}]\ntenants: [{name: t1, user_daily_token_quota: 5}]\naccess_keys: [{ak: k1, tenant: t1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        );
        let key = h.state().auth.authenticate("k1").await.unwrap();
        let with_user = |content: &str| GatewayRequest {
            is_online: true,
            message: vec![ChatMsg::text("user", content)],
            model_param_v2: Some(ModelParamV2::with_name(Protocol::OpenaiChat, "gpt-4o")),
            user_id: Some("u1".into()),
            ..Default::default()
        };
        h.run(with_user("first burns the budget"), key.clone())
            .await
            .unwrap();
        let err = h
            .run(with_user("second is over"), key)
            .await
            .err()
            .expect("second denied by the per-user budget");
        assert_eq!(err.http_status, 429);
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

    #[derive(Debug)]
    struct SecretStream;

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for SecretStream {
        async fn send(
            &self,
            _req: gw_engines::transport::UpstreamRequest,
        ) -> GResult<gw_engines::transport::UpstreamResponse> {
            use futures::StreamExt;
            let frames: Vec<Result<bytes::Bytes, String>> = vec![
                Ok(bytes::Bytes::from(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"key sk-abcdefghijklmnopqrstuvwxyz012345 ok\"}}]}\n\n",
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
    async fn detect_secrets_alone_buffers_and_masks_the_stream() {
        let mut cfg = GatewayConfig::embedded_default().unwrap();
        cfg.security.dlp_redact = false;
        cfg.security.detect_secrets = true;
        let cfg = Arc::new(cfg);
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(SecretStream),
        );
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
            live.iter().all(|d| !d.contains("sk-abc")),
            "no raw secret delta may reach the live channel: {live:?}"
        );
        let out = ctx.outcome.expect("outcome");
        assert!(
            out.chunks.is_empty(),
            "raw chunks cleared: {:?}",
            out.chunks
        );
        assert!(
            out.response.message.contains("[REDACTED_SECRET]")
                && !out.response.message.contains("sk-abc"),
            "{}",
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
                user: String::new(),
            },
            BatchItem {
                messages: vec![ChatMsg::text("user", "same prompt")],
                user: String::new(),
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
                        user: String::new(),
                    },
                    BatchItem {
                        messages: vec![ChatMsg::text("user", "two")],
                        user: String::new(),
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
    async fn batch_submitted_after_an_erasure_still_runs() {
        let h = handler();
        let off = OfflineHandler::new(h.clone());
        let key = ak(&h).await;
        let audit = gw_state::AdminAudit {
            created_at_epoch_secs: 1,
            actor: "global".into(),
            scope: "global".into(),
            action: "content_erase".into(),
            target: "user-42".into(),
            summary: String::new(),
            source_ip: String::new(),
        };
        h.state()
            .store
            .content_erase_user(None, "user-42", audit)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let job = off
            .submit(
                key,
                "gpt-4o".into(),
                vec![BatchItem {
                    messages: vec![ChatMsg::text("user", "new content after erasure")],
                    user: "user-42".into(),
                }],
            )
            .await
            .unwrap();
        wait_terminal(&h, &job.id).await;
        let done = h.state().store.batch_get(&job.id).await.unwrap().unwrap();
        assert!(
            done.results[0].ok,
            "a past erasure must not fail the user's future batches: {}",
            done.results[0].message
        );
    }

    #[tokio::test]
    async fn erased_batch_item_fails_instead_of_running() {
        let h = handler();
        let off = OfflineHandler::new(h.clone());
        let key = ak(&h).await;
        let job = off
            .submit(
                key,
                "gpt-4o".into(),
                vec![BatchItem {
                    messages: Vec::new(),
                    user: "u1".into(),
                }],
            )
            .await
            .unwrap();
        wait_terminal(&h, &job.id).await;
        let done = h.state().store.batch_get(&job.id).await.unwrap().unwrap();
        assert!(!done.results[0].ok, "an erased item must not execute");
        assert_eq!(done.results[0].message, "item content erased");
        assert_eq!(done.results[0].total_tokens, 0, "nothing billed");
    }

    #[tokio::test]
    async fn batch_attributes_each_item_to_its_user() {
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: gpt-4o, protocol: openai-chat}]\naccounts: [{name: a1, provider: openai, protocols: ['openai-chat']}]\naccess_keys: [{ak: k1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        );
        let off = OfflineHandler::new(h.clone());
        let key = h.state().auth.authenticate("k1").await.unwrap();
        let job = off
            .submit(
                key,
                "gpt-4o".into(),
                vec![
                    BatchItem {
                        messages: vec![ChatMsg::text("user", "for alice")],
                        user: "alice".into(),
                    },
                    BatchItem {
                        messages: vec![ChatMsg::text("user", "for bob")],
                        user: "bob".into(),
                    },
                ],
            )
            .await
            .unwrap();
        wait_terminal(&h, &job.id).await;
        let (_, ledger) = h.state().store.ledger_snapshot(usize::MAX).await.unwrap();
        let users: std::collections::HashSet<&str> =
            ledger.iter().map(|r| r.user_id.as_str()).collect();
        assert!(
            users.contains("alice") && users.contains("bob"),
            "each shared-key batch item bills to its own user: {users:?}"
        );
        let results = h
            .state()
            .store
            .batch_get(&job.id)
            .await
            .unwrap()
            .unwrap()
            .results;
        let owners: std::collections::HashSet<&str> =
            results.iter().map(|r| r.user.as_str()).collect();
        assert!(
            owners.contains("alice") && owners.contains("bob"),
            "each generated result carries its owner: {owners:?}"
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
                        user: "alice".into(),
                    },
                    BatchItem {
                        messages: vec![ChatMsg::text("user", "beta")],
                        user: "bob".into(),
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
        let (_, ledger) = state.store.ledger_snapshot(usize::MAX).await.unwrap();
        let users: std::collections::HashSet<&str> =
            ledger.iter().map(|r| r.user_id.as_str()).collect();
        assert!(
            users.contains("alice") && users.contains("bob"),
            "distributed batch preserved per-item user attribution: {users:?}"
        );
        assert!(
            state
                .store
                .batch_load_items(&job.id)
                .await
                .unwrap()
                .is_empty(),
            "a terminal batch's input rows are pruned"
        );
    }
}
