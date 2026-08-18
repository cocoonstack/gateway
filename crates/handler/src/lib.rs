//! Request orchestration (L4): the seam between HTTP views and the DAG.
//! `OnlineHandler` runs the plugin pre-stage (security block / DLP), the four
//! DAG layers, then the plugin post-stage; `OfflineHandler` reuses the same
//! chain for batches.

pub mod moderation;
pub mod offline;
pub mod plugins;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use futures::FutureExt;
use gw_config::GatewayConfig;
use gw_dag::DagContext;
use gw_engines::http_transport::UpstreamPolicy;
use gw_engines::{EngineOutcome, SharedTransport};
use gw_models::{Block, GResult, GatewayError, GatewayRequest, GatewayResponse};
use gw_state::admission;
use gw_state::{AkInfo, GatewayState, SharedConfig};
use uuid::Uuid;

pub use gw_models::BatchItem;
pub use offline::OfflineHandler;

const MODERATION_UNAVAILABLE: &str = "content moderation is unavailable";

static REQ_INSTANCE: LazyLock<String> = LazyLock::new(|| Uuid::new_v4().simple().to_string());
static REQ_SEQ: AtomicU64 = AtomicU64::new(0);

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

    /// Swap in a new config, then refresh its timeout/connect policies. Status
    /// replay permission stays on each request's selected account snapshot.
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
        let retention = active_retention(&snap.cfg, &ak.tenant);
        let terminal = retention.map(|retention| (retention, TerminalSubject::new(&ak, &request)));
        let result = self.run_inner(request, ak, &snap, retention).await;
        let settled_here = match result.as_ref() {
            Err(_) => true,
            Ok(ctx) => !ctx.request.buffered_online() && !ctx.billing_deferred,
        };
        if settled_here && let Some((retention, subject)) = terminal {
            let body = terminal_result_body(result.as_ref());
            persist_terminal(snap.state.store.as_ref(), &subject, retention, body).await;
        }
        result
    }

    async fn run_inner(
        &self,
        mut request: GatewayRequest,
        ak: AkInfo,
        snap: &gw_state::Snapshot,
        retention: Option<gw_config::RetentionConf>,
    ) -> GResult<DagContext> {
        let sec = snap.cfg.security_for(&ak.tenant);
        // outbound redaction buffers the response: a masked span can straddle deltas
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
        ctx.billing_deferred = dlp && ctx.request.is_online && ctx.request.stream;
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
        let inbound =
            (sec.moderate || retention.is_some()).then(|| plugins::inbound_text(&mut ctx.request));

        if sec.moderate {
            match self
                .moderation(sec, inbound.as_deref().unwrap_or_default())
                .await
            {
                Moderation::Allow => {}
                Moderation::Mask(spans) => {
                    let masked = match plugins::apply_moderation_mask(&mut ctx.request, &spans) {
                        Ok(masked) => masked,
                        Err(protected) => {
                            emit_security_event(
                                &ctx,
                                "moderation",
                                "block_protected",
                                protected as i64,
                            )
                            .await;
                            return Err(GatewayError::bad_request(
                                "moderation cannot rewrite signed thinking content",
                            ));
                        }
                    };
                    if masked > 0 {
                        ctx.decide("moderation", format!("masked {masked} span(s)"));
                        emit_security_event(&ctx, "moderation", "mask", masked as i64).await;
                    }
                }
                Moderation::Degrade => {
                    if ctx.request.pins_reasoning_route() {
                        emit_security_event(&ctx, "moderation", "block_pinned_thinking", 1).await;
                        deny_moderation(
                            &mut ctx,
                            "degrade would change a pinned thinking model: denied",
                            "content requires degraded serving, but signed thinking pins the requested model",
                            gw_consts::ErrCode::EMPTY_RESP,
                        );
                        return Ok(ctx);
                    }
                    let tenant = &ctx.ak.tenant;
                    let swapped = ctx
                        .request
                        .model_param_v2
                        .as_mut()
                        .map(|p| admission::swap_to_fallback(&snap.cfg, tenant, p));
                    match swapped {
                        Some(admission::FallbackSwap::Swapped(from, fb)) => {
                            ctx.decide("moderation", format!("degraded {from} -> {fb}"));
                            emit_security_event(&ctx, "moderation", "degrade", 1).await;
                        }
                        Some(admission::FallbackSwap::AlreadyServing) => {
                            ctx.decide("moderation", "already serving the fallback");
                            emit_security_event(&ctx, "moderation", "degrade", 1).await;
                        }
                        _ => {
                            emit_security_event(&ctx, "moderation", "block", 1).await;
                            deny_moderation(
                                &mut ctx,
                                "degrade without a fallback: denied",
                                "content requires degraded serving; no fallback model configured",
                                gw_consts::ErrCode::EMPTY_RESP,
                            );
                            return Ok(ctx);
                        }
                    }
                }
                Moderation::Deny(reason) => {
                    emit_security_event(&ctx, "moderation", "block", 1).await;
                    deny_moderation(&mut ctx, "denied", reason, gw_consts::ErrCode::EMPTY_RESP);
                    return Ok(ctx);
                }
                Moderation::Unavailable => {
                    deny_moderation(
                        &mut ctx,
                        "moderator unavailable: denied",
                        MODERATION_UNAVAILABLE,
                        gw_consts::ErrCode::SYSTEM_ERROR,
                    );
                    return Ok(ctx);
                }
            }
        }

        let protected_dlp = plugins::protected_thinking_dlp_hits(sec, &mut ctx.request);
        if protected_dlp > 0 {
            ctx.decide(
                "dlp",
                format!("rejected {protected_dlp} protected thinking span(s)"),
            );
            emit_security_event(&ctx, "dlp", "block_protected", protected_dlp as i64).await;
            return Err(GatewayError::bad_request(
                "signed thinking contains sensitive data and cannot be safely redacted",
            ));
        }

        let redacted = plugins::dlp_redact_request(sec, &mut ctx.request);
        if redacted > 0 {
            ctx.decide("dlp", format!("redacted {redacted} span(s) inbound"));
            emit_security_event(&ctx, "dlp", "redact", redacted as i64).await;
        }

        // a panicking node must refund too; unwind-safe: the refund reads only plain ctx
        // fields each written whole by its node
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
            if e.code == gw_consts::ErrCode::STOP_LIMIT_MSG {
                note_abuse(&ctx).await;
            }
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

        let stripped = ctx
            .outcome
            .as_mut()
            .map(|outcome| {
                let stripped =
                    plugins::strip_unservable_thinking(sec, &mut outcome.response, &outcome.chunks);
                if stripped > 0 {
                    // the buffered raw events still carry the unservable prose
                    outcome.chunks.clear();
                }
                stripped
            })
            .unwrap_or(0);
        if stripped > 0 {
            ctx.decide(
                "dlp",
                format!("stripped signed thinking ({stripped} hit(s))"),
            );
            emit_security_event(&ctx, "dlp", "strip_protected_out", stripped as i64).await;
        }

        let redacted_out = if let Some(outcome) = ctx.outcome.as_mut() {
            let native_event_hits = plugins::native_event_dlp_hits(sec, &mut outcome.chunks);
            let n = plugins::dlp_redact_response(sec, &mut outcome.response);
            // raw deltas are pre-redaction: drop them when anything hit DLP, keeping only the
            // signed reasoning units the strip pass kept (the chat surface carries them in chunks)
            if dlp && (n > 0 || native_event_hits > 0) {
                for chunk in outcome.chunks.drain(..) {
                    if let Some(units) = chunk.reasoning_details {
                        outcome
                            .response
                            .reasoning_details
                            .get_or_insert_default()
                            .extend(units);
                    }
                }
            }
            n.max(native_event_hits)
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

    /// Run the wired moderator over raw text; the seam the realtime surface
    /// uses (it has no `DagContext`), so the caller records the security event
    /// on its own surface. [`Self::moderation`] narrowed to this surface: a
    /// live session can't switch models, so `Degrade` denies here instead.
    pub async fn moderate_rt(&self, sec: &gw_config::SecurityConf, text: &str) -> RtModeration {
        match self.moderation(sec, text).await {
            Moderation::Allow => RtModeration::Allow,
            Moderation::Mask(spans) => RtModeration::Mask(spans),
            Moderation::Degrade => RtModeration::Deny(
                "content requires degraded serving; not available on a live session".to_owned(),
            ),
            Moderation::Deny(reason) => RtModeration::Deny(reason),
            Moderation::Unavailable => RtModeration::Deny(MODERATION_UNAVAILABLE.to_owned()),
        }
    }

    /// The one moderator-verdict resolution every surface shares, so the
    /// fail-open posture can't drift between REST and realtime.
    async fn moderation(&self, sec: &gw_config::SecurityConf, text: &str) -> Moderation {
        match self.moderator.review(text).await {
            Ok(moderation::Verdict::Allow) => Moderation::Allow,
            Ok(moderation::Verdict::Mask(spans)) => Moderation::Mask(spans),
            Ok(moderation::Verdict::Degrade) => Moderation::Degrade,
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

    /// Derive the per-account upstream policies from `cfg` and apply them to
    /// the transport live.
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
    Mask(Vec<std::ops::Range<usize>>),
    Degrade,
    Deny(String),
    Unavailable,
}

/// The realtime-surface subset of [`Moderation`]: no degrade mid-session.
pub enum RtModeration {
    Allow,
    Mask(Vec<std::ops::Range<usize>>),
    Deny(String),
}

/// A fleet-unique request id, `req-<process instance>-<seq>`: one relaxed atomic
/// add per id, the random instance tag drawn once per process.
pub fn new_request_id() -> String {
    format!(
        "req-{}-{}",
        REQ_INSTANCE.as_str(),
        REQ_SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// Persist the final HTTP result for an online non-streaming request after its
/// view has finished rendering the response.
pub async fn persist_terminal_response(ctx: &DagContext, http_status: u16) {
    persist_ctx_terminal(ctx, || terminal_status_body(http_status)).await;
}

/// Settle billing and terminal retention after an outbound-DLP buffered stream
/// has been replayed by the HTTP view.
pub async fn complete_buffered_stream(
    ctx: &mut DagContext,
    delivery: gw_dag::StreamDelivery,
) -> GResult<()> {
    let terminal = match &delivery {
        gw_dag::StreamDelivery::Complete => terminal_plain_body("success", 200, false),
        gw_dag::StreamDelivery::Partial(_) => terminal_plain_body("client_closed", 499, true),
        gw_dag::StreamDelivery::None => terminal_plain_body("client_closed", 499, false),
    };
    gw_dag::settle_deferred_stream(ctx, delivery).await?;
    persist_ctx_terminal(ctx, || terminal).await;
    Ok(())
}

async fn persist_ctx_terminal(ctx: &DagContext, body: impl FnOnce() -> serde_json::Value) {
    let Some(retention) = active_retention(&ctx.cfg, &ctx.ak.tenant) else {
        return;
    };
    let subject = TerminalSubject::new(&ctx.ak, &ctx.request);
    persist_terminal(ctx.state.store.as_ref(), &subject, retention, body()).await;
}

/// Count one admission rejection against the key's fleet-shared daily abuse
/// counter and suspend it when a configured tier trips (deny path only).
async fn note_abuse(ctx: &DagContext) {
    let tiers = &ctx.cfg.abuse.tiers;
    if tiers.is_empty() {
        return;
    }
    let now = gw_state::epoch_secs();
    // fresh read: a concurrent rejection may have suspended the key already
    match ctx.state.auth.authenticate(&ctx.ak.ak).await {
        Some(fresh) if fresh.status_at(now) != gw_state::KeyStatus::Suspended => {}
        _ => return,
    }
    // ':' is banned in ak names, so the prefix cannot collide with a real key
    let counter = format!("abuse:{}", ctx.ak.ak);
    ctx.state.governance.quota_consume(&counter, 1).await;
    let rejects = ctx.state.governance.quota_used(&counter).await;
    let Some(tier) = tiers
        .iter()
        .filter(|t| rejects >= t.rejects)
        .max_by_key(|t| t.rejects)
    else {
        return;
    };
    let until = now + (tier.suspend_hours * 3600) as i64;
    let patch = gw_state::KeyPatch {
        suspended_until_epoch_secs: Some(Some(until)),
        ..Default::default()
    };
    if let Err(e) = ctx.state.auth.patch(&ctx.ak.ak, &patch).await {
        let ak_id = gw_state::access_key_fingerprint(&ctx.ak.ak);
        tracing::warn!(error = %e, ak_id, "abuse suspension patch failed");
        return;
    }
    let summary = format!(
        "{rejects} admission rejects today; suspended {}h",
        tier.suspend_hours
    );
    ctx.state
        .store
        .admin_audit_add(&gw_state::AdminAudit {
            created_at_epoch_secs: now,
            actor: "system".to_owned(),
            scope: "global".to_owned(),
            action: "abuse_suspend".to_owned(),
            target: ctx.ak.ak.clone(),
            summary: summary.clone(),
            source_ip: String::new(),
        })
        .await
        .unwrap_or_else(|e| tracing::warn!(error = %e, "abuse audit write failed"));
    ctx.state
        .alerts
        .emit("abuse_suspend", ctx.ak.ak.clone(), summary);
}

/// Record a moderation denial: decision line + content-filter outcome. Event
/// emission stays at the call sites (`Unavailable` deliberately records none).
fn deny_moderation(
    ctx: &mut DagContext,
    decision: &str,
    reason: impl Into<String>,
    code: gw_consts::ErrCode,
) {
    ctx.decide("moderation", decision);
    ctx.outcome = Some(content_filter_outcome(Block::blocked(
        reason,
        code.value() as i32,
    )));
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
/// key/user/tenant; best-effort.
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

struct TerminalSubject {
    request_id: String,
    ak: String,
    user_id: String,
    tenant: String,
}

impl TerminalSubject {
    fn new(ak: &AkInfo, request: &GatewayRequest) -> Self {
        Self {
            request_id: request.request_id.clone(),
            ak: ak.ak.clone(),
            user_id: ak
                .attributed_user(request.user_id.as_deref().unwrap_or_default())
                .to_owned(),
            tenant: ak.tenant.clone(),
        }
    }
}

/// Persist one lifecycle result row (no provider message or user content).
async fn persist_terminal(
    store: &dyn gw_state::Store,
    subject: &TerminalSubject,
    retention: gw_config::RetentionConf,
    body: serde_json::Value,
) {
    let now = gw_state::epoch_secs();
    let record = gw_state::ContentRecord {
        created_at_epoch_secs: now,
        request_id: subject.request_id.clone(),
        ak: subject.ak.clone(),
        user_id: subject.user_id.clone(),
        tenant: subject.tenant.clone(),
        kind: "terminal".to_owned(),
        content: body.to_string(),
        sealed: false,
        expires_at_epoch_secs: retention_expiry(retention, now),
    };
    if let Err(error) = store.content_terminal_put(&record).await {
        tracing::warn!(error = %error, request_id = %subject.request_id, "terminal retention write failed");
    }
}

fn terminal_result_body(result: Result<&DagContext, &GatewayError>) -> serde_json::Value {
    match result {
        Err(error) => match gw_consts::ErrClass::classify(error.code, error.http_status) {
            Some(class) => terminal_error_body(class, error.original_status(), false),
            None => terminal_plain_body("client_closed", error.http_status, false),
        },
        Ok(ctx) => match ctx.outcome.as_ref() {
            Some(outcome) => match outcome.terminal_error.as_ref() {
                Some(error) => terminal_error_body(error.class, error.original_status, true),
                None if outcome.response.aborted => terminal_plain_body("client_closed", 499, true),
                None => terminal_plain_body("success", outcome.http_code, false),
            },
            None => terminal_error_body(gw_consts::ErrClass::InternalServer, None, false),
        },
    }
}

fn terminal_status_body(http_status: u16) -> serde_json::Value {
    if (200..300).contains(&http_status) {
        return terminal_plain_body("success", http_status, false);
    }
    match gw_consts::ErrClass::from_status(http_status) {
        Some(class) => terminal_error_body(class, None, false),
        None => terminal_plain_body("client_closed", http_status, false),
    }
}

fn terminal_plain_body(state: &str, http_status: u16, stream_committed: bool) -> serde_json::Value {
    serde_json::json!({
        "state": state,
        "http_status": http_status,
        "stream_committed": stream_committed,
    })
}

fn terminal_error_body(
    class: gw_consts::ErrClass,
    original_status: Option<u16>,
    stream_committed: bool,
) -> serde_json::Value {
    let mut body = serde_json::json!({
        "state": "error",
        "code": class.code(),
        "http_status": class.status(),
        "stream_committed": stream_committed,
    });
    if let Some(status) = original_status {
        body["original_status_code"] = status.into();
    }
    body
}

/// The tenant's retention policy when it actually retains content.
fn active_retention(cfg: &GatewayConfig, tenant: &str) -> Option<gw_config::RetentionConf> {
    cfg.retention_for(tenant)
        .copied()
        .filter(|r| r.content != gw_config::ContentLevel::None)
}

fn retention_expiry(retention: gw_config::RetentionConf, now: i64) -> i64 {
    if retention.days > 0 {
        now + retention.days as i64 * 86_400
    } else {
        0
    }
}

/// Persist the pre-DLP prompt and the response per the tenant's retention:
/// sealed when `store_full` (full level AND a content key), else PII/secret-stripped; best-effort.
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
    let expires = retention_expiry(retention, now);
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

    #[tokio::test]
    async fn a_retry_status_only_account_reaches_the_upstream_request() {
        #[derive(Debug, Default)]
        struct Recording(std::sync::Mutex<Option<Arc<gw_models::Account>>>);
        #[async_trait::async_trait]
        impl gw_engines::transport::Transport for Recording {
            async fn send(
                &self,
                req: gw_engines::transport::UpstreamRequest,
            ) -> GResult<gw_engines::transport::UpstreamResponse> {
                *self.0.lock().unwrap() = req.replay_account.as_ref().map(Arc::clone);
                gw_engines::MockTransport.send(req).await
            }
        }
        let yaml = "listen: {host: h, port: 1}\naccess_keys: [{ak: test, product: p, qps: 10, daily_token_quota: 1000}]\nmodels: [{name: m, protocol: openai-chat}]\naccounts: [{name: a1, provider: p, protocols: [openai-chat], retry_status: [429, 502]}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let recording = Arc::new(Recording::default());
        let transport = Arc::clone(&recording);
        let h = OnlineHandler::new(gw_state::SharedConfig::new(cfg, state), transport);
        let ak = h.state().auth.authenticate("test").await.unwrap();
        h.run(chat_req("m", "hi"), ak).await.unwrap();
        let recorded = recording.0.lock().unwrap();
        let replay = recorded
            .as_ref()
            .expect("selected account must carry its replay policy");
        assert_eq!(replay.retry_status, [429, 502]);
        assert_eq!(
            replay
                .connect_retries
                .unwrap_or(UpstreamPolicy::default().connect_retries),
            UpstreamPolicy::default().connect_retries
        );
    }

    #[tokio::test]
    async fn an_inflight_request_keeps_its_vendors_replay_permission_across_reload() {
        use std::sync::atomic::{AtomicU32, Ordering};

        #[derive(Debug)]
        struct GateModerator {
            entered: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
        }

        #[async_trait::async_trait]
        impl moderation::Moderator for GateModerator {
            async fn review(&self, _text: &str) -> Result<moderation::Verdict, String> {
                self.entered.notify_one();
                self.release.notified().await;
                Ok(moderation::Verdict::Allow)
            }
        }

        async fn flaky_vendor() -> (String, Arc<AtomicU32>) {
            let hits = Arc::new(AtomicU32::new(0));
            let seen = Arc::clone(&hits);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                loop {
                    let Ok((mut socket, _)) = listener.accept().await else {
                        return;
                    };
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut request = [0u8; 1024];
                    let _ = socket.read(&mut request).await;
                    let first = seen.fetch_add(1, Ordering::SeqCst) == 0;
                    let (status, body) = if first {
                        (502, r#"{"error":{"message":"old vendor failed"}}"#)
                    } else {
                        (
                            200,
                            r#"{"model":"m","choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#,
                        )
                    };
                    let response = format!(
                        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                }
            });
            (format!("http://{addr}"), hits)
        }

        let (old_endpoint, hits) = flaky_vendor().await;
        let config = |endpoint: &str, retry_status: &str| {
            GatewayConfig::from_yaml(&format!(
                "listen: {{host: h, port: 1}}\nsecurity: {{moderate: true}}\naccess_keys: [{{ak: test, product: p, qps: 100, daily_token_quota: 100000}}]\nmodels: [{{name: m, protocol: openai-chat}}]\naccounts: [{{name: a1, provider: p, endpoint: '{endpoint}', protocols: [openai-chat], connect_retries: 1, retry_status: {retry_status}}}]"
            ))
            .unwrap()
        };
        let old_cfg = Arc::new(config(&old_endpoint, "[]"));
        let state = Arc::new(GatewayState::from_config(&old_cfg));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(old_cfg, state),
            Arc::new(
                gw_engines::http_transport::HttpTransport::new(std::time::Duration::from_secs(5))
                    .unwrap(),
            ),
        )
        .with_moderator(Arc::new(GateModerator {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        }));
        let ak = h.state().auth.authenticate("test").await.unwrap();
        let running = tokio::spawn({
            let h = h.clone();
            async move { h.run(chat_req("m", "hi"), ak).await }
        });
        entered.notified().await;
        h.reload(config("http://127.0.0.1:9", "[502]"))
            .await
            .unwrap();
        release.notify_one();
        assert!(
            running.await.unwrap().is_err(),
            "old 502 must not be replayed"
        );
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "new vendor policy must not replay an old vendor request"
        );
    }

    async fn ak(h: &OnlineHandler) -> AkInfo {
        h.state().auth.authenticate("ak-demo-123").await.unwrap()
    }

    fn retained_handler(transport: SharedTransport) -> OnlineHandler {
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: gpt-4o, protocol: openai-chat}]\naccounts: [{name: a1, provider: openai, protocols: [openai-chat]}]\ntenants: [{name: t1, retention: {content: redacted, days: 1}}]\naccess_keys: [{ak: retained-ak, tenant: t1, owner: attempt-1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        OnlineHandler::new(gw_state::SharedConfig::new(cfg, state), transport)
    }

    async fn retained_ak(h: &OnlineHandler) -> AkInfo {
        h.state().auth.authenticate("retained-ak").await.unwrap()
    }

    async fn terminal_body(h: &OnlineHandler, request_id: &str) -> serde_json::Value {
        let rows = h.state().store.content_for(request_id).await.unwrap();
        let row = rows
            .iter()
            .find(|row| row.kind == "terminal")
            .expect("terminal row");
        serde_json::from_str(&row.content).unwrap()
    }

    #[test]
    fn generated_request_ids_are_fleet_unique() {
        let (a, b) = (new_request_id(), new_request_id());
        let (instance, seq) = a
            .strip_prefix("req-")
            .expect("request id prefix")
            .rsplit_once('-')
            .expect("instance-seq shape");
        Uuid::parse_str(instance).expect("random instance tag");
        seq.parse::<u64>().expect("sequence tail");
        assert_ne!(a, b, "generated request ids must not repeat");
        assert!(
            b.starts_with(&format!("req-{instance}-")),
            "one instance tag per process"
        );
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

    fn drained_stream_req(name: &str, content: &str) -> GatewayRequest {
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let mut request = chat_req(name, content);
        request.stream = true;
        request.stream_tx = Some(tx);
        request
    }

    fn thinking_req(name: &str, content: &str, thinking_type: &str) -> GatewayRequest {
        let mut request = chat_req(name, content);
        request.preserve_anthropic_wire = true;
        if let Some(param) = request.model_param_v2.as_mut() {
            let thinking = if thinking_type == "enabled" {
                serde_json::json!({"type":"enabled","budget_tokens":1024})
            } else {
                serde_json::json!({"type":thinking_type})
            };
            param.raw = serde_json::json!({
                "thinking":thinking
            });
        }
        request
    }

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
                *b = serde_json::to_vec(&v).unwrap().into();
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
        let avail = &h.state().avail;
        avail.flush().await;
        let minute = gw_state::epoch_secs() / 60;
        assert_eq!(
            avail.window("pub-m", minute - 5, minute).await,
            (1, 0),
            "availability attributes to the requested public name"
        );
        assert_eq!(avail.window("canary-m", minute - 5, minute).await, (0, 0));
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

    #[derive(Debug)]
    struct MaskModerator;

    #[async_trait::async_trait]
    impl moderation::Moderator for MaskModerator {
        async fn review(&self, text: &str) -> Result<moderation::Verdict, String> {
            match text.find("secret") {
                Some(i) => Ok(moderation::Verdict::Mask(
                    std::iter::once(i..i + "secret".len()).collect(),
                )),
                None => Ok(moderation::Verdict::Allow),
            }
        }
    }

    #[derive(Debug)]
    struct DegradeModerator;

    #[async_trait::async_trait]
    impl moderation::Moderator for DegradeModerator {
        async fn review(&self, _text: &str) -> Result<moderation::Verdict, String> {
            Ok(moderation::Verdict::Degrade)
        }
    }

    #[tokio::test]
    async fn moderate_rt_maps_verdicts_and_failure_posture() {
        let cfg = Arc::new(GatewayConfig::embedded_default().unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let base = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        );
        let mut sec = gw_config::SecurityConf::default();
        assert!(matches!(
            base.moderate_rt(&sec, "x").await,
            RtModeration::Allow
        ));
        let deny = base.clone().with_moderator(Arc::new(DenyModerator));
        assert!(matches!(deny.moderate_rt(&sec, "x").await, RtModeration::Deny(r) if r == "nope"));
        let mask = base.clone().with_moderator(Arc::new(MaskModerator));
        assert!(matches!(
            mask.moderate_rt(&sec, "a secret").await,
            RtModeration::Mask(spans) if spans.len() == 1 && spans[0] == (2..8)
        ));
        let degrade = base.clone().with_moderator(Arc::new(DegradeModerator));
        assert!(
            matches!(degrade.moderate_rt(&sec, "x").await, RtModeration::Deny(r) if r.contains("live session")),
            "degrade maps to deny on the realtime surface"
        );
        let err = base.with_moderator(Arc::new(ErrModerator));
        sec.moderation_fail_open = true;
        assert!(matches!(
            err.moderate_rt(&sec, "x").await,
            RtModeration::Allow
        ));
        sec.moderation_fail_open = false;
        assert!(matches!(
            err.moderate_rt(&sec, "x").await,
            RtModeration::Deny(_)
        ));
    }

    #[tokio::test]
    async fn moderation_mask_redacts_before_the_engine() {
        let yaml = "listen: {host: h, port: 1}\nsecurity: {moderate: true}\nmodels: [{name: gpt-4o, protocol: openai-chat}]\naccounts: [{name: a1, provider: openai, protocols: ['openai-chat']}]\naccess_keys: [{ak: k1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        )
        .with_moderator(Arc::new(MaskModerator));
        let key = h.state().auth.authenticate("k1").await.unwrap();
        let ctx = h
            .run(chat_req("gpt-4o", "tell secret now"), key)
            .await
            .unwrap();
        assert!(
            ctx.outcome
                .expect("outcome")
                .response
                .message
                .contains("you said: tell [MASKED] now"),
            "the engine saw the masked text"
        );
        let events = h.state().store.security_events(None, 10).await.unwrap();
        assert!(
            events
                .iter()
                .any(|e| e.rule == "moderation" && e.action == "mask")
        );
    }

    #[tokio::test]
    async fn moderation_degrade_swaps_to_tenant_fallback() {
        let yaml = "listen: {host: h, port: 1}\nsecurity: {moderate: true}\nmodels: [{name: pub-m, protocol: openai-chat}, {name: fb-m, protocol: openai-chat}]\naccounts: [{name: a1, provider: openai, protocols: ['openai-chat']}]\ntenants: [{name: t1, models: [pub-m, fb-m], fallback_model: fb-m}]\naccess_keys: [{ak: k1, tenant: t1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        )
        .with_moderator(Arc::new(DegradeModerator));
        let key = h.state().auth.authenticate("k1").await.unwrap();
        let ctx = h.run(chat_req("pub-m", "hi"), key).await.unwrap();
        assert_eq!(
            ctx.outcome.expect("outcome").response.model,
            "pub-m",
            "response echoes the requested name"
        );
        let (_, ledger) = h.state().store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(ledger[0].model, "pub-m");
        assert_eq!(ledger[0].served_model, "fb-m");
        let events = h.state().store.security_events(None, 10).await.unwrap();
        assert!(
            events
                .iter()
                .any(|e| e.rule == "moderation" && e.action == "degrade")
        );
    }

    #[tokio::test]
    async fn moderation_degrade_denies_instead_of_moving_a_thinking_request() {
        let yaml = "listen: {host: h, port: 1}\nsecurity: {moderate: true}\nmodels: [{name: pub-m, protocol: anthropic-messages}, {name: fb-m, protocol: anthropic-messages}]\naccounts: [{name: a1, provider: anthropic, protocols: ['anthropic-messages']}]\ntenants: [{name: t1, models: [pub-m, fb-m], fallback_model: fb-m}]\naccess_keys: [{ak: k1, tenant: t1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        )
        .with_moderator(Arc::new(DegradeModerator));
        let key = h.state().auth.authenticate("k1").await.unwrap();
        let ctx = h
            .run(thinking_req("pub-m", "hi", "enabled"), key)
            .await
            .unwrap();
        let out = ctx.outcome.expect("moderation denial outcome");

        assert_eq!(out.response.finish_reason, "content_filter");
        assert!(out.response.message.contains("pins the requested model"));
        assert!(
            h.state()
                .store
                .ledger_snapshot(usize::MAX)
                .await
                .unwrap()
                .1
                .is_empty(),
            "the pinned request must not reach a fallback model"
        );
    }

    #[tokio::test]
    async fn moderation_degrade_serves_when_already_on_the_fallback() {
        let yaml = "listen: {host: h, port: 1}\nsecurity: {moderate: true}\nmodels: [{name: pub-m, protocol: openai-chat}, {name: fb-m, protocol: openai-chat}]\naccounts: [{name: a1, provider: openai, protocols: ['openai-chat']}]\ntenants: [{name: t1, models: [pub-m, fb-m], fallback_model: fb-m}]\naccess_keys: [{ak: k1, tenant: t1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        )
        .with_moderator(Arc::new(DegradeModerator));
        let key = h.state().auth.authenticate("k1").await.unwrap();
        let ctx = h.run(chat_req("fb-m", "hi"), key).await.unwrap();
        let out = ctx.outcome.expect("outcome");
        assert_eq!(
            out.response.finish_reason, "stop",
            "a request already on the fallback serves, not denies: {}",
            out.response.message
        );
        assert!(
            ctx.decisions
                .iter()
                .any(|(n, w)| *n == "moderation" && w.contains("already serving")),
            "{:?}",
            ctx.decisions
        );
    }

    #[tokio::test]
    async fn moderation_degrade_without_fallback_denies() {
        let yaml = "listen: {host: h, port: 1}\nsecurity: {moderate: true}\nmodels: [{name: pub-m, protocol: openai-chat}]\naccounts: [{name: a1, provider: openai, protocols: ['openai-chat']}]\naccess_keys: [{ak: k1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        )
        .with_moderator(Arc::new(DegradeModerator));
        let key = h.state().auth.authenticate("k1").await.unwrap();
        let ctx = h.run(chat_req("pub-m", "hi"), key).await.unwrap();
        let out = ctx.outcome.expect("outcome");
        assert_eq!(out.response.finish_reason, "content_filter");
        assert!(
            out.response
                .message
                .contains("no fallback model configured")
        );
    }

    #[tokio::test]
    async fn abuse_tiers_suspend_after_repeated_rejections() {
        let yaml = "listen: {host: h, port: 1}\nabuse: {tiers: [{rejects: 2, suspend_hours: 2}]}\nmodels: [{name: gpt-4o, protocol: openai-chat}]\naccounts: [{name: a1, provider: openai, protocols: ['openai-chat']}]\naccess_keys: [{ak: k1, product: p, qps: 0, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        );
        let mut alerts = h.state().alerts.take_receiver().expect("receiver");
        let key = h.state().auth.authenticate("k1").await.unwrap();
        let reject = |k: AkInfo| h.run(chat_req("gpt-4o", "hi"), k);
        assert_eq!(
            reject(key.clone()).await.err().map(|e| e.http_status),
            Some(429)
        );
        let now = gw_state::epoch_secs();
        let fresh = h.state().auth.authenticate("k1").await.unwrap();
        assert_eq!(
            fresh.status_at(now),
            gw_state::KeyStatus::Active,
            "one reject is under the tier"
        );
        assert_eq!(reject(key).await.err().map(|e| e.http_status), Some(429));
        let fresh = h.state().auth.authenticate("k1").await.unwrap();
        assert_eq!(
            fresh.status_at(now),
            gw_state::KeyStatus::Suspended,
            "second reject trips the 2-reject tier"
        );
        let until = fresh.suspended_until_epoch_secs.expect("deadline set");
        assert!(
            (until - now - 2 * 3600).abs() < 60,
            "2h suspension: {until}"
        );
        let trail = h.state().store.admin_audit_list(10).await.unwrap();
        assert!(
            trail
                .iter()
                .any(|e| e.action == "abuse_suspend" && e.actor == "system" && e.target == "k1"),
            "audit row: {trail:?}"
        );
        let ev = alerts.recv().await.expect("alert emitted");
        assert_eq!((ev.kind, ev.subject.as_str()), ("abuse_suspend", "k1"));
    }

    #[tokio::test]
    async fn total_only_vendor_usage_meters_instead_of_zeroing() {
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: abab, protocol: minimax-v1}]\naccounts: [{name: a1, provider: minimax, protocols: ['minimax-v1']}]\naccess_keys: [{ak: k1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        );
        let key = h.state().auth.authenticate("k1").await.unwrap();
        let ctx = h.run(chat_req("abab", "hello minimax"), key).await.unwrap();
        let out = ctx.outcome.expect("outcome");
        assert!(out.response.total_tokens > 0, "engine carries the total");
        assert!(
            out.response.common_usage.is_none(),
            "a total-only usage must not fabricate a zeroed parts view: {:?}",
            out.response.common_usage
        );
        let (_, ledger) = h.state().store.ledger_snapshot(usize::MAX).await.unwrap();
        assert!(
            ledger[0].total_tokens > 0,
            "total-only vendor usage must meter, not bill zero: {:?}",
            ledger[0]
        );
        assert!(ledger[0].cost_micros >= 0);
    }

    #[tokio::test]
    async fn hard_quota_rejection_does_not_trip_abuse_tier() {
        let yaml = "listen: {host: h, port: 1}\nabuse: {tiers: [{rejects: 1, suspend_hours: 2}]}\nmodels: [{name: gpt-4o, protocol: openai-chat}]\naccounts: [{name: a1, provider: openai, protocols: ['openai-chat']}]\naccess_keys: [{ak: k1, product: p, qps: 100, daily_token_quota: 1}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        );
        let key = h.state().auth.authenticate("k1").await.unwrap();
        assert!(h.run(chat_req("gpt-4o", "hi"), key.clone()).await.is_ok());
        let err = h
            .run(chat_req("gpt-4o", "hi"), key)
            .await
            .err()
            .expect("hard quota must reject");
        assert_eq!(err.code, gw_consts::ErrCode::QUOTA_EXHAUSTED);
        let fresh = h.state().auth.authenticate("k1").await.unwrap();
        assert_eq!(
            fresh.status_at(gw_state::epoch_secs()),
            gw_state::KeyStatus::Active
        );
    }

    #[tokio::test]
    async fn quota_fallback_skips_variant_select() {
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: pub-m, protocol: openai-chat, variants: [{model: canary-m, weight: 1}]}, {name: canary-m, protocol: openai-chat}, {name: fb-m, protocol: openai-chat}]\naccounts: [{name: a1, provider: openai, protocols: ['openai-chat']}]\ntenants: [{name: t1, models: [pub-m, canary-m, fb-m], fallback_model: fb-m, model_quotas: {pub-m: 1}}]\naccess_keys: [{ak: k1, tenant: t1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        );
        let key = h.state().auth.authenticate("k1").await.unwrap();
        h.run(chat_req("pub-m", "burn the tiny quota"), key.clone())
            .await
            .unwrap();
        let ctx = h
            .run(chat_req("pub-m", "over quota now"), key)
            .await
            .unwrap();
        assert!(
            ctx.decisions
                .iter()
                .any(|(n, w)| *n == "model_quota" && w.contains("serving fb-m")),
            "quota gate degraded the request: {:?}",
            ctx.decisions
        );
        assert!(
            !ctx.decisions.iter().any(|(n, _)| *n == "variant_select"),
            "an already-degraded request skips the variant split"
        );
        let (_, ledger) = h.state().store.ledger_snapshot(usize::MAX).await.unwrap();
        let rec = ledger.last().expect("two billed requests");
        assert_eq!(rec.model, "pub-m");
        assert_eq!(rec.served_model, "fb-m");
    }

    async fn assert_thinking_route_pinned(thinking_type: &str) {
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: pub-m, protocol: anthropic-messages, variants: [{model: canary-m, weight: 1}]}, {name: canary-m, protocol: anthropic-messages}, {name: fb-m, protocol: anthropic-messages}]\naccounts: [{name: a1, provider: anthropic, protocols: ['anthropic-messages']}]\ntenants: [{name: t1, models: [pub-m, canary-m, fb-m], fallback_model: fb-m, model_quotas: {pub-m: 1}}]\naccess_keys: [{ak: k1, tenant: t1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        );
        let key = h.state().auth.authenticate("k1").await.unwrap();

        let seed = h
            .run(
                thinking_req("pub-m", "start thinking", thinking_type),
                key.clone(),
            )
            .await
            .unwrap();
        assert!(
            !seed
                .decisions
                .iter()
                .any(|(node, _)| *node == "variant_select"),
            "{thinking_type} thinking must stay on the requested model"
        );

        let mut assistant = ChatMsg::text("assistant", String::new());
        assistant.parts = Some(serde_json::json!([
            {"type":"thinking","thinking":"summary","signature":"opaque"},
            {"type":"tool_use","id":"tool-1","name":"probe","input":{}}
        ]));
        let mut tool_result = ChatMsg::text("user", String::new());
        tool_result.parts = Some(serde_json::json!([
            {"type":"tool_result","tool_use_id":"tool-1","content":"done"}
        ]));
        let mut continuation = chat_req("pub-m", "");
        continuation.preserve_anthropic_wire = true;
        continuation.message = vec![assistant, tool_result];
        let continued = h.run(continuation, key).await.unwrap();
        assert!(
            continued.decisions.iter().any(|(node, decision)| {
                *node == "model_quota" && decision.contains("reasoning route pinned")
            }),
            "over-quota continuation must not fall back: {:?}",
            continued.decisions
        );
        assert!(
            !continued
                .decisions
                .iter()
                .any(|(node, _)| *node == "variant_select")
        );

        let (_, ledger) = h.state().store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(ledger.len(), 2);
        assert!(ledger.iter().all(|record| record.served_model == "pub-m"));
    }

    #[tokio::test]
    async fn thinking_modes_stay_off_variants_and_quota_fallbacks() {
        for thinking_type in ["enabled", "adaptive"] {
            assert_thinking_route_pinned(thinking_type).await;
        }
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
        assert_eq!(err.http_status, 400);
        assert_eq!(err.code, gw_consts::ErrCode::QUOTA_EXHAUSTED);
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
    struct ProviderFailure;

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for ProviderFailure {
        async fn send(
            &self,
            _req: gw_engines::transport::UpstreamRequest,
        ) -> GResult<gw_engines::transport::UpstreamResponse> {
            Ok(gw_engines::transport::UpstreamResponse {
                status: 502,
                body: gw_engines::transport::UpstreamBody::Json(bytes::Bytes::from_static(
                    br#"{"error":{"message":"provider failed"}}"#,
                )),
                headers: Default::default(),
            })
        }
    }

    #[tokio::test]
    async fn retained_provider_error_has_external_terminal_result() {
        let h = retained_handler(Arc::new(ProviderFailure));
        let mut request = chat_req("gpt-4o", "hello");
        request.request_id = "req-provider-error".into();
        let error = h
            .run(request, retained_ak(&h).await)
            .await
            .err()
            .expect("provider error");
        assert_eq!(error.http_status, 502);

        let terminal = terminal_body(&h, "req-provider-error").await;
        assert_eq!(terminal["state"], "error");
        assert_eq!(terminal["code"], "model_error_exception");
        assert_eq!(terminal["http_status"], 424);
        assert_eq!(terminal["original_status_code"], 502);
        assert_eq!(terminal["stream_committed"], false);
    }

    #[derive(Debug)]
    struct ToolOnlyReply;

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for ToolOnlyReply {
        async fn send(
            &self,
            _req: gw_engines::transport::UpstreamRequest,
        ) -> GResult<gw_engines::transport::UpstreamResponse> {
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::Json(bytes::Bytes::from_static(
                    br#"{"model":"gpt-4o","choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call-1","type":"function","function":{"name":"probe","arguments":"{}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}"#,
                )),
                headers: Default::default(),
            })
        }
    }

    #[tokio::test]
    async fn retained_online_success_waits_for_view_result() {
        let h = retained_handler(Arc::new(ToolOnlyReply));
        let mut request = chat_req("gpt-4o", "use a tool");
        request.request_id = "req-tool-only".into();
        let ctx = h.run(request, retained_ak(&h).await).await.unwrap();
        let outcome = ctx.outcome.as_ref().expect("outcome");
        assert!(outcome.response.message.is_empty());
        assert!(outcome.response.tool_calls.is_some());
        let before = h.state().store.content_for("req-tool-only").await.unwrap();
        assert!(
            before.iter().all(|row| row.kind != "terminal"),
            "the handler cannot know the final non-streaming view status"
        );

        persist_terminal_response(&ctx, 200).await;

        let terminal = terminal_body(&h, "req-tool-only").await;
        assert_eq!(terminal["state"], "success");
        assert_eq!(terminal["http_status"], 200);
        assert_eq!(terminal["stream_committed"], false);
        assert!(terminal.get("code").is_none());
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
            let frames: Vec<Result<bytes::Bytes, gw_engines::transport::StreamFault>> = vec![
                Ok(bytes::Bytes::from(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"partial answer\"}}]}\n\n",
                )),
                Err(gw_engines::transport::StreamFault {
                    timeout: false,
                    message: "connection reset".to_owned(),
                }),
            ];
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::SseStream(
                    futures::stream::iter(frames).boxed(),
                ),
                headers: Default::default(),
            })
        }
    }

    #[derive(Debug)]
    struct FailoverBreakingStream;

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for FailoverBreakingStream {
        async fn send(
            &self,
            req: gw_engines::transport::UpstreamRequest,
        ) -> GResult<gw_engines::transport::UpstreamResponse> {
            if req.account == "a-down" {
                return gw_engines::MockTransport.send(req).await;
            }
            BreakingStream.send(req).await
        }
    }

    #[derive(Debug)]
    struct PausedStream {
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for PausedStream {
        async fn send(
            &self,
            _req: gw_engines::transport::UpstreamRequest,
        ) -> GResult<gw_engines::transport::UpstreamResponse> {
            use futures::StreamExt;
            let first = futures::stream::once(async {
                Ok::<_, gw_engines::transport::StreamFault>(bytes::Bytes::from_static(
                    b"data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n",
                ))
            });
            let release = Arc::clone(&self.release);
            let second = futures::stream::once(async move {
                release.notified().await;
                Ok::<_, gw_engines::transport::StreamFault>(bytes::Bytes::from_static(
                    b"data: {\"choices\":[{\"delta\":{\"content\":\"second\"}}]}\n\n",
                ))
            });
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::SseStream(first.chain(second).boxed()),
                headers: Default::default(),
            })
        }
    }

    #[tokio::test]
    async fn committed_stream_error_bills_and_records_failure() {
        let h = retained_handler(Arc::new(BreakingStream));
        let mut req = drained_stream_req("gpt-4o", "please stream something long");
        req.request_id = "req-committed-error".into();
        let ctx = h.run(req, retained_ak(&h).await).await.unwrap();
        let out = ctx.outcome.expect("outcome");
        assert!(out.response.aborted, "mid-stream break must mark aborted");
        assert_eq!(
            out.terminal_error.as_ref().map(|error| error.class),
            Some(gw_consts::ErrClass::ModelStreamError)
        );
        assert_eq!(out.response.message, "partial answer");
        let terminal = terminal_body(&h, "req-committed-error").await;
        assert_eq!(terminal["state"], "error");
        assert_eq!(terminal["code"], "model_stream_error_exception");
        assert_eq!(terminal["http_status"], 424);
        assert_eq!(terminal["stream_committed"], true);
        let (_, ledger) = h.state().store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(ledger.len(), 1, "aborted stream must still bill");
        assert!(
            ledger[0].prompt_tokens > 0 && ledger[0].completion_tokens > 0,
            "estimated tokens, not zero: {:?}",
            ledger[0]
        );
        let avail = &h.state().avail;
        avail.flush().await;
        let minute = gw_state::epoch_secs() / 60;
        assert_eq!(
            avail.window("gpt-4o", minute - 5, minute).await,
            (0, 1),
            "a committed provider error is an availability failure"
        );
    }

    #[tokio::test]
    async fn committed_retry_error_records_both_account_failures() {
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: m, protocol: openai-chat, provider: p}]\naccounts: [{name: a-down, provider: p, priority: 1, protocols: [openai-chat]}, {name: a-up, provider: p, priority: 2, protocols: [openai-chat]}]\nstability: {failure_threshold: 1, cooldown_seconds: 30}\naccess_keys: [{ak: k1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(FailoverBreakingStream),
        );
        let mut alerts = h.state().alerts.take_receiver().expect("receiver");
        let request = drained_stream_req("m", "please stream something long");

        let key = h.state().auth.authenticate("k1").await.unwrap();
        let ctx = h.run(request, key).await.unwrap();
        assert_eq!(
            ctx.outcome
                .as_ref()
                .and_then(|outcome| outcome.terminal_error.as_ref())
                .map(|error| error.class),
            Some(gw_consts::ErrClass::ModelStreamError)
        );
        assert!(!h.state().health.available("a-down").await);
        assert!(!h.state().health.available("a-up").await);
        let first = alerts.try_recv().expect("first account cooldown alert");
        let second = alerts.try_recv().expect("retry account cooldown alert");
        assert_eq!(
            (first.kind, first.subject.as_str()),
            ("account_cooldown", "a-down")
        );
        assert_eq!(
            (second.kind, second.subject.as_str()),
            ("account_cooldown", "a-up")
        );
        assert!(alerts.try_recv().is_err(), "one alert per failed account");
    }

    #[tokio::test]
    async fn client_disconnect_after_commit_stays_health_neutral() {
        let yaml = "listen: {host: h, port: 1}\nsecurity: {dlp_redact: false}\nmodels: [{name: m, protocol: openai-chat}]\naccounts: [{name: a1, provider: p, protocols: [openai-chat]}]\nstability: {failure_threshold: 1, cooldown_seconds: 30}\naccess_keys: [{ak: k1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let release = Arc::new(tokio::sync::Notify::new());
        let h = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(PausedStream {
                release: Arc::clone(&release),
            }),
        );
        let mut alerts = h.state().alerts.take_receiver().expect("receiver");
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut request = chat_req("m", "please stream something long");
        request.stream = true;
        request.stream_tx = Some(tx);
        let key = h.state().auth.authenticate("k1").await.unwrap();
        let running = tokio::spawn({
            let h = h.clone();
            async move { h.run(request, key).await }
        });

        assert_eq!(rx.recv().await.expect("first chunk").delta, "first");
        drop(rx);
        release.notify_one();
        let ctx = running.await.unwrap().unwrap();
        let outcome = ctx.outcome.expect("outcome");
        assert!(outcome.response.aborted);
        assert!(outcome.terminal_error.is_none());
        let avail = &h.state().avail;
        avail.flush().await;
        let minute = gw_state::epoch_secs() / 60;
        assert_eq!(avail.window("m", minute - 5, minute).await, (0, 0));
        assert!(h.state().health.available("a1").await);
        assert!(alerts.try_recv().is_err());
    }

    #[derive(Debug)]
    struct MalformedCommittedStream;

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for MalformedCommittedStream {
        async fn send(
            &self,
            _req: gw_engines::transport::UpstreamRequest,
        ) -> GResult<gw_engines::transport::UpstreamResponse> {
            use futures::StreamExt;
            let frames: Vec<Result<bytes::Bytes, gw_engines::transport::StreamFault>> = vec![
                Ok(bytes::Bytes::from(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"partial answer\"}}]}\n\n",
                )),
                Ok(bytes::Bytes::from("data: not-json\n\n")),
            ];
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::SseStream(
                    futures::stream::iter(frames).boxed(),
                ),
                headers: Default::default(),
            })
        }
    }

    #[tokio::test]
    async fn malformed_committed_stream_persists_terminal_error() {
        let h = retained_handler(Arc::new(MalformedCommittedStream));
        let mut request = drained_stream_req("gpt-4o", "please stream something long");
        request.request_id = "req-committed-parse-error".into();

        let ctx = h.run(request, retained_ak(&h).await).await.unwrap();
        let outcome = ctx.outcome.expect("outcome");
        assert!(outcome.response.aborted);
        assert_eq!(outcome.response.message, "partial answer");
        assert_eq!(
            outcome.terminal_error.as_ref().map(|error| error.class),
            Some(gw_consts::ErrClass::InternalServer)
        );
        let terminal = terminal_body(&h, "req-committed-parse-error").await;
        assert_eq!(terminal["state"], "error");
        assert_eq!(terminal["code"], "internal_server_exception");
        assert_eq!(terminal["http_status"], 500);
        assert_eq!(terminal["stream_committed"], true);
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
            let frames: Vec<Result<bytes::Bytes, gw_engines::transport::StreamFault>> = vec![
                Ok(bytes::Bytes::from(
                    "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-sonnet\",\"usage\":{\"input_tokens\":100}}}\n\n",
                )),
                Ok(bytes::Bytes::from(
                    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"delivered words here\"}}\n\n",
                )),
                Err(gw_engines::transport::StreamFault {
                    timeout: false,
                    message: "connection reset".to_owned(),
                }),
            ];
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::SseStream(
                    futures::stream::iter(frames).boxed(),
                ),
                headers: Default::default(),
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
        let req = drained_stream_req("claude-sonnet", "please stream something");
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
            let frames: Vec<Result<bytes::Bytes, gw_engines::transport::StreamFault>> = vec![
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
                headers: Default::default(),
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
    async fn dlp_partial_replay_bills_only_delivered_text() {
        let h = OnlineHandler::new(handler().config.clone(), Arc::new(PiiStream));
        let mut request = chat_req("gpt-4o", "hello");
        request.stream = true;
        let mut ctx = h.run(request, ak(&h).await).await.unwrap();
        assert_eq!(
            h.state().store.ledger_snapshot(10).await.unwrap().0,
            0,
            "billing waits for the buffered replay"
        );

        complete_buffered_stream(&mut ctx, gw_dag::StreamDelivery::Partial(2))
            .await
            .unwrap();
        let (_, ledger) = h.state().store.ledger_snapshot(10).await.unwrap();
        assert_eq!(ledger.len(), 1);
        assert!(ledger[0].estimated);
        assert!(ledger[0].completion_tokens > 0);
        assert!(ctx.outcome.as_ref().unwrap().response.aborted);
    }

    #[tokio::test]
    async fn dlp_buffer_keeps_clean_native_chunks_for_lossless_replay() {
        let h = handler();
        assert!(h.cfg().security.dlp_redact, "default config has DLP on");
        let mut request = chat_req("claude-sonnet", "clean input");
        request.stream = true;
        request.preserve_anthropic_wire = true;
        let context = h.run(request, ak(&h).await).await.unwrap();
        let outcome = context.outcome.expect("outcome");

        assert!(
            !outcome.chunks.is_empty(),
            "clean buffered events must survive outbound DLP"
        );
        assert!(
            outcome
                .chunks
                .iter()
                .any(|chunk| chunk.native_event.is_some()),
            "native Anthropic events must remain available to the view"
        );
    }

    #[derive(Debug)]
    struct ClaudePiiStream;

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for ClaudePiiStream {
        async fn send(
            &self,
            _req: gw_engines::transport::UpstreamRequest,
        ) -> GResult<gw_engines::transport::UpstreamResponse> {
            let sse = concat!(
                "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-sonnet\",\"usage\":{\"input_tokens\":5}}}\n\n",
                "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"clean\",\"signature\":\"opaque\"}}\n\n",
                "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"mail jane@corp.com\"}}\n\n",
                "data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
                "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n",
                "data: {\"type\":\"message_stop\"}\n\n",
            );
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::Sse(sse.as_bytes().to_vec()),
                headers: Default::default(),
            })
        }
    }

    #[derive(Debug)]
    struct ClaudePiiThinkingStream;

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for ClaudePiiThinkingStream {
        async fn send(
            &self,
            _req: gw_engines::transport::UpstreamRequest,
        ) -> GResult<gw_engines::transport::UpstreamResponse> {
            let sse = concat!(
                "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-sonnet\",\"usage\":{\"input_tokens\":5}}}\n\n",
                "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"mail jane@corp.com\",\"signature\":\"opaque\"}}\n\n",
                "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\",\"text\":\"answer\"}}\n\n",
                "data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
                "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n",
                "data: {\"type\":\"message_stop\"}\n\n",
            );
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::Sse(sse.as_bytes().to_vec()),
                headers: Default::default(),
            })
        }
    }

    #[derive(Debug)]
    struct ClaudeCleanThinkingPiiAnswerStream;

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for ClaudeCleanThinkingPiiAnswerStream {
        async fn send(
            &self,
            _req: gw_engines::transport::UpstreamRequest,
        ) -> GResult<gw_engines::transport::UpstreamResponse> {
            let sse = concat!(
                "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-sonnet\",\"usage\":{\"input_tokens\":5}}}\n\n",
                "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\",\"signature\":\"\"}}\n\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"clean\"}}\n\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"opaque\"}}\n\n",
                "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"mail jane@corp.com\"}}\n\n",
                "data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
                "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n",
                "data: {\"type\":\"message_stop\"}\n\n",
            );
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::Sse(sse.as_bytes().to_vec()),
                headers: Default::default(),
            })
        }
    }

    #[tokio::test]
    async fn chat_surface_dlp_buffered_stream_keeps_clean_signed_reasoning() {
        let base = handler();
        let h = OnlineHandler::new(
            base.config.clone(),
            Arc::new(ClaudeCleanThinkingPiiAnswerStream),
        );
        let mut request = chat_req("claude-sonnet", "clean input");
        request.stream = true;
        let context = h.run(request, ak(&h).await).await.unwrap();
        let outcome = context.outcome.expect("a served outcome, not a failure");

        assert!(outcome.chunks.is_empty(), "raw deltas must be dropped");
        assert_eq!(outcome.response.message, "mail [REDACTED_EMAIL]");
        assert_eq!(outcome.response.reasoning, "clean");
        assert_eq!(
            outcome.response.reasoning_details,
            Some(vec![serde_json::json!({
                "type":"reasoning.text","text":"clean","signature":"opaque",
                "format":"anthropic-claude-v1","index":0
            })])
        );
    }

    #[tokio::test]
    async fn pii_inside_thinking_prose_strips_the_thinking_and_still_serves() {
        let base = handler();
        let h = OnlineHandler::new(base.config.clone(), Arc::new(ClaudePiiThinkingStream));
        let mut request = chat_req("claude-sonnet", "clean input");
        request.stream = true;
        request.preserve_anthropic_wire = true;
        let context = h.run(request, ak(&h).await).await.unwrap();
        let outcome = context.outcome.expect("a served outcome, not a failure");

        assert!(outcome.chunks.is_empty(), "raw events must be dropped");
        let content = outcome.response.anthropic_content.unwrap();
        assert!(
            content
                .as_array()
                .unwrap()
                .iter()
                .all(|block| block["type"] != "thinking"),
            "unservable thinking is stripped, not failed: {content}"
        );
        assert_eq!(content[0]["text"], "answer");
    }

    #[tokio::test]
    async fn dlp_redacts_native_text_without_dropping_thinking_proof() {
        let base = handler();
        let h = OnlineHandler::new(base.config.clone(), Arc::new(ClaudePiiStream));
        let mut request = chat_req("claude-sonnet", "clean input");
        request.stream = true;
        request.preserve_anthropic_wire = true;
        let context = h.run(request, ak(&h).await).await.unwrap();
        let outcome = context.outcome.expect("outcome");

        assert!(outcome.chunks.is_empty(), "raw events must be dropped");
        let content = outcome.response.anthropic_content.unwrap();
        assert_eq!(content[0]["signature"], "opaque");
        assert!(
            content[1]["text"]
                .as_str()
                .unwrap()
                .contains("[REDACTED_EMAIL]")
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
            let frames: Vec<Result<bytes::Bytes, gw_engines::transport::StreamFault>> = vec![
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
                headers: Default::default(),
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
            suspended_until_epoch_secs: None,
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
        let mut cfg = GatewayConfig::embedded_default().unwrap();
        cfg.storage.postgres_url = url;
        let cfg = Arc::new(cfg);
        let state = Arc::new(GatewayState::build(&cfg).await.expect("pg state"));
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
