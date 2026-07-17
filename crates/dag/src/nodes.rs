//! The default node set for the online chat pipeline.
//! Metrics-reporting nodes are dropped by design.

use gw_consts::{ErrCode, Protocol};
use gw_models::{GResult, GatewayError};
use gw_state::admission;

use crate::context::DagContext;
use crate::executor::{DagNode, Layer};

/// Completion tokens reserved when the caller sets no max_tokens; settle
/// corrects to actuals, so the estimate only needs to be monotone.
const DEFAULT_COMPLETION_RESERVE: i64 = 256;
/// Cap on the reservation regardless of a caller's `max_tokens`, so a hostile
/// `max_tokens: i64::MAX` can't overflow the estimate or corrupt the counter.
const MAX_RESERVE: i64 = 1_000_000;

/// A shared admission denial as the wire error every limit answers with.
fn limit_denied(msg: String) -> GatewayError {
    GatewayError::new(ErrCode::STOP_LIMIT_MSG, 429, msg)
}

/// preprocess/model_quota: per-(AK, model) daily token cap — AK override, else
/// tenant default, else unmetered (the per-AK daily cap backstops). Over-quota
/// degrades to the tenant's fallback model when one is configured. Runs before
/// resolve_model so a swap re-routes protocol, entitlement, and cache.
/// A soft cap like the user budget: check-then-consume, so concurrent in-flight
/// requests can overshoot before the counter accrues — the routing trigger
/// doesn't warrant reserve/settle; the reserved AK daily quota hard-caps spend.
pub struct ModelQuotaGate;

#[async_trait::async_trait]
impl DagNode for ModelQuotaGate {
    fn name(&self) -> &'static str {
        "model_quota"
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        let Some(p) = ctx
            .request
            .model_param_v2
            .as_ref()
            .filter(|p| !p.model_name.is_empty())
        else {
            return Ok(());
        };
        let Some(limit) = admission::model_quota_limit(&ctx.cfg, &ctx.ak, &p.model_name) else {
            return Ok(());
        };
        // clone only on the metered path — the common unmetered case stays allocation-free
        let requested = p.model_name.clone();
        let key = admission::model_quota_key(&ctx.ak.ak, &requested);
        let under = ctx.state.governance.quota_check(&key, limit).await;
        // usage accrues to the requested name either way: a fallback period ends at the daily reset
        ctx.model_quota_key = Some(key);
        if under {
            return Ok(());
        }
        let fallback = ctx
            .cfg
            .find_tenant(&ctx.ak.tenant)
            .and_then(|t| t.fallback_model.clone());
        match fallback {
            Some(fb) if fb != requested => {
                ctx.decide(
                    "model_quota",
                    format!("{requested} over {limit}, serving {fb}"),
                );
                if let Some(param) = ctx.request.model_param_v2.as_mut() {
                    param.fallback_from = Some(requested);
                    param.model_name = fb;
                }
            }
            _ => ctx.decide(
                "model_quota",
                format!("{requested} over {limit}, no fallback"),
            ),
        }
        Ok(())
    }
}

/// preprocess/resolve_model: public model name -> Protocol.
pub struct ResolveModel;

#[async_trait::async_trait]
impl DagNode for ResolveModel {
    fn name(&self) -> &'static str {
        "resolve_model"
    }
    fn deps(&self) -> &'static [&'static str] {
        &["model_quota"]
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        let param = ctx
            .request
            .model_param_v2
            .as_mut()
            .ok_or_else(|| GatewayError::bad_request("request missing model param"))?;
        let name = &param.model_name;
        let mt = if let Some(conf) = ctx.cfg.find_model(name) {
            conf.protocol().ok_or_else(|| {
                GatewayError::internal(format!("config maps `{name}` to unknown type"))
            })?
        } else if let Some(direct) = Protocol::from_wire(name) {
            direct // callers may address a wire model type directly
        } else {
            return Err(GatewayError::new(
                ErrCode::REQ_PARAM,
                404,
                format!("unknown model: {name}"),
            ));
        };
        let decision = format!("{name} -> {mt}");
        param.protocol = mt;
        ctx.decide("resolve_model", decision);
        Ok(())
    }
}

/// preprocess/tenant_entitlement: per-tenant model allowlist. Runs before the
/// cache so an unentitled model can't be served from another tenant's entry.
pub struct TenantEntitlement;

#[async_trait::async_trait]
impl DagNode for TenantEntitlement {
    fn name(&self) -> &'static str {
        "tenant_entitlement"
    }
    fn deps(&self) -> &'static [&'static str] {
        &["resolve_model"]
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        let name = ctx
            .request
            .model_param_v2
            .as_ref()
            .map(|p| p.model_name.as_str())
            .unwrap_or_default();
        if !ctx.cfg.tenant_allows_model(&ctx.ak.tenant, name) {
            return Err(GatewayError::new(
                ErrCode::PERMISSION_CHECK,
                403,
                format!(
                    "model `{name}` is not entitled for tenant `{}`",
                    ctx.ak.tenant
                ),
            ));
        }
        Ok(())
    }
}

/// preprocess/cache_lookup: request-level TTL cache. On a hit the outcome is
/// produced directly and the downstream nodes all short-circuit.
pub struct CacheLookup;

#[async_trait::async_trait]
impl DagNode for CacheLookup {
    fn name(&self) -> &'static str {
        "cache_lookup"
    }
    fn deps(&self) -> &'static [&'static str] {
        &["tenant_entitlement"]
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        let param = match ctx.request.model_param_v2.as_ref() {
            Some(p) => p,
            None => return Ok(()),
        };
        // online non-streaming cache_ttl models only; batch items bypass —
        // a hit is free (unbilled) and batches promise per-item billing
        let ttl = ctx
            .cfg
            .find_model(&param.model_name)
            .and_then(|m| m.cache_ttl_seconds);
        let Some(ttl) = ttl else { return Ok(()) };
        if ctx.request.stream || !ctx.request.is_online {
            return Ok(());
        }
        let Some(key) = cache_key_of(ctx) else {
            return Ok(());
        };
        if let Some(cached) = ctx.state.cache.get(&key).await {
            ctx.decide("cache_lookup", format!("hit ttl={ttl}s"));
            metrics::counter!("gateway_cache_hits_total").increment(1);
            ctx.cache_hit = true;
            ctx.outcome = Some(gw_engines::EngineOutcome::ok(cached));
        } else {
            ctx.decide("cache_lookup", "miss".to_owned());
        }
        ctx.cache_key = Some(key);
        Ok(())
    }
}

/// Cache key: sha256 of model name + messages + typed params + passthrough
/// params. Not keyed by tenant: entitlement gates before the cache, and a
/// per-tenant split would only shrink the hit rate.
fn cache_key_of(ctx: &DagContext) -> Option<String> {
    use sha2::{Digest, Sha256};
    let param = ctx.request.model_param_v2.as_ref()?;
    let mut h = Sha256::new();
    // generation: a reload may have remapped the model — a pre-reload entry must not match
    h.update(ctx.cfg.generation().to_le_bytes());
    h.update(param.model_name.as_bytes());
    // serialize straight into the hasher — no throwaway buffers for a multi-KB history
    serde_json::to_writer(&mut h, &ctx.request.message).ok()?;
    if let Some(t) = &param.typed {
        serde_json::to_writer(&mut h, t).ok()?;
    }
    // raw params (seed, vendor extras) change the output — omitting them would collide entries
    if !param.raw.is_null() {
        serde_json::to_writer(&mut h, &param.raw).ok()?;
    }
    Some(hex::encode(h.finalize()))
}

/// Cheap admission estimate: ~chars/4 prompt heuristic + requested max_tokens,
/// saturating and capped so caller-controlled input can't wrap the counters.
fn reserve_estimate(req: &gw_models::GatewayRequest) -> i64 {
    let prompt: usize = req.message.iter().map(|m| m.content.len()).sum();
    let max_out = req
        .model_param_v2
        .as_ref()
        .and_then(|p| p.typed.as_ref())
        .and_then(|t| match t {
            gw_models::TypedParams::Chat(c) => c.max_tokens,
            _ => None,
        })
        .unwrap_or(DEFAULT_COMPLETION_RESERVE)
        .clamp(0, MAX_RESERVE);
    ((prompt as i64 / 4).max(1))
        .saturating_add(max_out)
        .min(MAX_RESERVE)
}

/// preprocess/quota_check: AK daily-quota admission. Reserves the estimate
/// atomically so concurrent in-flight requests count against the budget;
/// billing settles to actuals and a failed pipeline refunds in the handler.
pub struct QuotaCheck;

#[async_trait::async_trait]
impl DagNode for QuotaCheck {
    fn name(&self) -> &'static str {
        "quota_check"
    }
    fn deps(&self) -> &'static [&'static str] {
        &["cache_lookup"]
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        let est = reserve_estimate(&ctx.request);
        let at = gw_state::epoch_secs();
        admission::reserve_daily(ctx.state.governance.as_ref(), &ctx.ak, est, at)
            .await
            .map_err(limit_denied)?;
        ctx.quota_reserved = Some(est);
        ctx.quota_at = at;
        ctx.decide("quota_check", format!("reserved {est}"));
        Ok(())
    }
}

/// account_select/select_account: PTU preferred + priority + round-robin + health filter.
pub struct SelectAccount;

#[async_trait::async_trait]
impl DagNode for SelectAccount {
    fn name(&self) -> &'static str {
        "select_account"
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        let mt = ctx
            .request
            .protocol()
            .ok_or_else(|| GatewayError::internal("select_account before resolve_model"))?;
        let provider = model_provider(ctx);
        let account = ctx
            .state
            .pool
            .select_healthy(mt, provider, &[], ctx.state.health.as_ref())
            .await
            .ok_or_else(|| {
                GatewayError::new(
                    ErrCode::SYSTEM_ERROR,
                    503,
                    format!("no healthy upstream account serves model type `{mt}`"),
                )
            })?;
        ctx.decide("select_account", account.name.clone());
        ctx.request.account = Some(account);
        Ok(())
    }
}

/// model_access/tenant_rate: pooled tenant QPS — all of a tenant's keys share
/// one bucket, checked ahead of the per-AK limit.
pub struct TenantRateLimit;

#[async_trait::async_trait]
impl DagNode for TenantRateLimit {
    fn name(&self) -> &'static str {
        "tenant_rate"
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        admission::check_tenant_rate(ctx.state.governance.as_ref(), &ctx.cfg, &ctx.ak.tenant)
            .await
            .map_err(limit_denied)
    }
}

/// model_access/rate_limit: AK-level QPS limiting.
pub struct RateLimit;

#[async_trait::async_trait]
impl DagNode for RateLimit {
    fn name(&self) -> &'static str {
        "rate_limit"
    }
    fn deps(&self) -> &'static [&'static str] {
        &["tenant_rate"]
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        admission::check_ak_rate(ctx.state.governance.as_ref(), &ctx.ak)
            .await
            .map_err(limit_denied)
    }
}

/// model_access/product_qpm: product-level requests-per-minute limiting.
pub struct ProductQpmLimit;

#[async_trait::async_trait]
impl DagNode for ProductQpmLimit {
    fn name(&self) -> &'static str {
        "product_qpm"
    }
    fn deps(&self) -> &'static [&'static str] {
        &["rate_limit"]
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        admission::check_product_qpm(ctx.state.governance.as_ref(), &ctx.cfg, &ctx.ak.product)
            .await
            .map_err(limit_denied)
    }
}

/// model_access/model_qpm: model-level requests-per-minute limiting.
pub struct ModelQpmLimit;

#[async_trait::async_trait]
impl DagNode for ModelQpmLimit {
    fn name(&self) -> &'static str {
        "model_qpm"
    }
    fn deps(&self) -> &'static [&'static str] {
        &["product_qpm"]
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        // fail loud: silently skipping would waive the limit on a broken plan
        let param = ctx
            .request
            .model_param_v2
            .as_ref()
            .ok_or_else(|| GatewayError::internal("model_qpm before resolve_model"))?;
        admission::check_model_qpm(ctx.state.governance.as_ref(), &ctx.cfg, &param.model_name)
            .await
            .map_err(limit_denied)
    }
}

/// model_access/ak_tpm: AK-level tokens-per-minute limiting.
pub struct AkTpmLimit;

#[async_trait::async_trait]
impl DagNode for AkTpmLimit {
    fn name(&self) -> &'static str {
        "ak_tpm"
    }
    fn deps(&self) -> &'static [&'static str] {
        &["model_qpm"]
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        let est = ctx
            .quota_reserved
            .unwrap_or_else(|| reserve_estimate(&ctx.request));
        ctx.tpm_reserved = admission::reserve_tpm(ctx.state.governance.as_ref(), &ctx.ak, est)
            .await
            .map_err(limit_denied)?;
        Ok(())
    }
}

/// model_access/user_budget: per-user daily token budget (soft cap), keyed by
/// the effective end user. No-op without a tenant limit or a user attribution.
pub struct UserBudgetGate;

#[async_trait::async_trait]
impl DagNode for UserBudgetGate {
    fn name(&self) -> &'static str {
        "user_budget"
    }
    fn deps(&self) -> &'static [&'static str] {
        &["ak_tpm"]
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        admission::check_user_budget(
            ctx.state.governance.as_ref(),
            &ctx.cfg,
            &ctx.ak.tenant,
            ctx.effective_user_id(),
        )
        .await
        .map_err(limit_denied)
    }
}

/// model_access/call_engine: factory dispatch + engine execution + failover.
/// On an upstream 5xx the failed account is excluded and reselected once (a
/// PTU → paygo spill sets `ptu_spillover`); a second failure propagates as-is.
pub struct CallEngine;

#[async_trait::async_trait]
impl DagNode for CallEngine {
    fn name(&self) -> &'static str {
        "call_engine"
    }
    fn deps(&self) -> &'static [&'static str] {
        &["user_budget"]
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        let threshold = ctx.cfg.stability.failure_threshold;
        let cooldown = std::time::Duration::from_secs(ctx.cfg.stability.cooldown_seconds);
        let engine = gw_engines::get_engine(ctx.request.clone(), ctx.transport.clone())?;
        match engine.run().await {
            Ok(outcome) => {
                // an aborted stream is neither a success nor an account fault
                if !outcome.response.aborted {
                    ctx.state.avail.record(served_model(ctx), true);
                    if let Some(a) = ctx.request.account.as_ref() {
                        ctx.state.health.record_success(&a.name).await;
                    }
                }
                ctx.decide(
                    "call_engine",
                    format!(
                        "model={} http={}",
                        outcome.response.model, outcome.http_code
                    ),
                );
                ctx.outcome = Some(outcome);
                Ok(())
            }
            Err(first_err) if first_err.http_status >= 500 => {
                ctx.state.avail.record(served_model(ctx), false);
                let mt = ctx
                    .request
                    .protocol()
                    .ok_or_else(|| GatewayError::internal("call_engine without model type"))?;
                // fail loud: a default account would corrupt health bookkeeping
                let failed = ctx
                    .request
                    .account
                    .clone()
                    .ok_or_else(|| GatewayError::internal("call_engine without an account"))?;
                if ctx
                    .state
                    .health
                    .record_failure(&failed.name, threshold, cooldown)
                    .await
                {
                    ctx.decide(
                        "account_health",
                        format!(
                            "{} entered cooldown ({}s)",
                            failed.name, ctx.cfg.stability.cooldown_seconds
                        ),
                    );
                }
                let provider = model_provider(ctx);
                let next = ctx
                    .state
                    .pool
                    .select_healthy(
                        mt,
                        provider,
                        std::slice::from_ref(&failed.name),
                        ctx.state.health.as_ref(),
                    )
                    .await;
                let Some(next) = next else {
                    return Err(first_err);
                };
                let spillover = failed.is_ptu() && !next.is_ptu();
                ctx.decide(
                    "call_engine",
                    format!(
                        "failover {} -> {} (spillover={spillover}): {}",
                        failed.name, next.name, first_err.message
                    ),
                );
                ctx.request.account = Some(next.clone());
                let retry = gw_engines::get_engine(ctx.request.clone(), ctx.transport.clone())?;
                match retry.run().await {
                    Ok(mut outcome) => {
                        ctx.state.avail.record(served_model(ctx), true);
                        ctx.state.health.record_success(&next.name).await;
                        outcome.response.ptu_spillover = spillover;
                        ctx.outcome = Some(outcome);
                        Ok(())
                    }
                    Err(e) => {
                        if e.http_status >= 500 {
                            ctx.state.avail.record(served_model(ctx), false);
                        }
                        ctx.state
                            .health
                            .record_failure(&next.name, threshold, cooldown)
                            .await;
                        Err(e)
                    }
                }
            }
            Err(e) => Err(e),
        }
    }
}

/// post_process/common_usage: RawUsageJSON -> CommonUsage.
pub struct CommonUsageNode;

#[async_trait::async_trait]
impl DagNode for CommonUsageNode {
    fn name(&self) -> &'static str {
        "common_usage"
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        if let Some(outcome) = ctx.outcome.as_mut() {
            let resp = &mut outcome.response;
            resp.common_usage =
                gw_engines::extract_common_usage(&resp.raw_usage_json, resp.is_messages_protocol);
        }
        Ok(())
    }
}

/// post_process/cost_calc: local billing + quota consumption + ledger.
pub struct CostCalc;

#[async_trait::async_trait]
impl DagNode for CostCalc {
    fn name(&self) -> &'static str {
        "cost_calc"
    }
    fn deps(&self) -> &'static [&'static str] {
        &["common_usage"]
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        // fail loud: silently skipping here would serve a response unbilled
        let outcome = ctx
            .outcome
            .as_ref()
            .ok_or_else(|| GatewayError::internal("cost_calc without an engine outcome"))?;
        let resp = &outcome.response;
        // An aborted stream delivered text but never the final usage frame — bill it.
        // Gate on completion_tokens==0, not total: Anthropic reports input up front,
        // output only in the final message_delta the break skipped.
        if resp.aborted && resp.completion_tokens == 0 {
            let enc = gw_models::token_estimate::default_encoder();
            let param = ctx.request.model_param_v2.as_ref();
            let tools = param.and_then(|p| p.typed.as_ref()).and_then(|t| match t {
                gw_models::TypedParams::Chat(c) => c.tools.as_ref(),
                _ => None,
            });
            let model_name = param.map(|p| p.model_name.as_str()).unwrap_or_default();
            let pt = if resp.prompt_tokens > 0 {
                resp.prompt_tokens
            } else {
                gw_models::estimate_prompt_tokens(&ctx.request.message, tools, model_name, enc)
            };
            let ct = enc.encode_len(&resp.message) as i64;
            ctx.decide("cost_calc", format!("aborted stream, billed {pt}+{ct}"));
            return bill(ctx, BillTokens::flat(pt, ct), true).await;
        }
        // saturating sums so a malformed usage subtree can't overflow the totals
        let tokens = match &resp.common_usage {
            Some(u) => {
                let ti = gw_models::TokenInput {
                    prompt: u.platform_input,
                    read_cache: u.read_cache,
                    write_cache: u.write_cache,
                    completion: u.completion,
                    reasoning: u.reason,
                };
                let served = ctx
                    .request
                    .model_param_v2
                    .as_ref()
                    .map(|p| p.model_name.as_str())
                    .unwrap_or_default();
                let rate = token_rate(&ctx.cfg, served);
                BillTokens {
                    prompt: u.prompt_total(),
                    completion: u.completion_total(),
                    billable_prompt: gw_models::weighted_prompt(&ti, &rate),
                    billable_completion: gw_models::weighted_completion(&ti, &rate),
                }
            }
            None => BillTokens::flat(resp.prompt_tokens, resp.completion_tokens),
        };
        bill(ctx, tokens, false).await
    }
}

/// The served model's billing weights; identity when unconfigured. The
/// `prompt_includes_cache` normalization already happened at usage extraction.
fn token_rate(cfg: &gw_config::GatewayConfig, model: &str) -> gw_models::TokenRate {
    match cfg.find_model(model).and_then(|m| m.token_rate) {
        Some(r) => gw_models::TokenRate {
            prompt_includes_cache: false,
            prompt_weight: r.prompt,
            read_cache_weight: r.read_cache,
            write_cache_weight: r.write_cache,
            completion_weight: r.completion,
            reasoning_weight: r.reasoning,
        },
        None => gw_models::TokenRate::default(),
    }
}

/// Token counts for one bill: vendor-reported sides plus the weighted billable
/// sides; the platform total is always the billable sum, so quota consumption
/// and cost cannot drift.
struct BillTokens {
    prompt: i64,
    completion: i64,
    billable_prompt: i64,
    billable_completion: i64,
}

impl BillTokens {
    /// Unweighted counts: billable equals reported.
    fn flat(prompt: i64, completion: i64) -> Self {
        Self {
            prompt,
            completion,
            billable_prompt: prompt,
            billable_completion: completion,
        }
    }

    fn total(&self) -> i64 {
        self.billable_prompt
            .saturating_add(self.billable_completion)
    }
}

/// Settle reserves and write the ledger for one served request via the shared
/// [`admission::settle_and_bill`] orchestration. `estimated` marks a bill from
/// an aborted stream's estimated counts rather than a vendor usage payload.
async fn bill(ctx: &mut DagContext, tokens: BillTokens, estimated: bool) -> GResult<()> {
    let ptu_spillover = ctx
        .outcome
        .as_ref()
        .map(|o| o.response.ptu_spillover)
        .unwrap_or(false);
    let param = ctx.request.model_param_v2.as_ref();
    // cost bills at the served (post-fallback) model's price; the (AK, model)
    // counter accrues against the requested name
    let served = param.map(|p| p.model_name.as_str()).unwrap_or_default();
    let requested = param
        .and_then(|p| p.fallback_from.as_deref())
        .unwrap_or(served);
    // locals first: the whole-ctx borrow `effective_user_id` needs can't overlap these takes
    let (reserved, tpm_reserved, model_quota_key) = (
        ctx.quota_reserved.take().unwrap_or(0),
        ctx.tpm_reserved.take(),
        ctx.model_quota_key.take(),
    );
    let record = admission::settle_and_bill(
        ctx.state.governance.as_ref(),
        ctx.state.store.as_ref(),
        &ctx.cfg,
        admission::SettleInput {
            billing: gw_state::BillingInput {
                ak: &ctx.ak.ak,
                product: &ctx.ak.product,
                tenant: &ctx.ak.tenant,
                user_id: ctx.effective_user_id(),
                request_id: &ctx.request.request_id,
                requested_model: requested,
                served_model: served,
                protocol: param.map(|p| p.protocol.as_str()).unwrap_or_default(),
                account: ctx.request.account_name(),
                prompt: tokens.prompt,
                completion: tokens.completion,
                billable_prompt: tokens.billable_prompt,
                billable_completion: tokens.billable_completion,
                total: tokens.total(),
                ptu_spillover,
                estimated,
            },
            reserved,
            tpm_reserved,
            reserved_at: ctx.quota_at,
            model_quota_key,
        },
    )
    .await;
    admission::consume_user_budget(
        ctx.state.governance.as_ref(),
        &ctx.cfg,
        &ctx.ak.tenant,
        ctx.effective_user_id(),
        record.total_tokens,
    )
    .await;
    ctx.decide(
        "cost_calc",
        format!(
            "tokens={} cost_micros={}",
            record.total_tokens, record.cost_micros
        ),
    );
    Ok(())
}

/// post_process/cache_store: successful non-streaming responses are written to the TTL cache.
pub struct CacheStore;

#[async_trait::async_trait]
impl DagNode for CacheStore {
    fn name(&self) -> &'static str {
        "cache_store"
    }
    fn deps(&self) -> &'static [&'static str] {
        &["cost_calc"]
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        if ctx.request.stream || !ctx.request.is_online {
            return Ok(());
        }
        let Some(outcome) = ctx.outcome.as_ref() else {
            return Ok(());
        };
        let Some(param) = ctx.request.model_param_v2.as_ref() else {
            return Ok(());
        };
        let Some(ttl) = ctx
            .cfg
            .find_model(&param.model_name)
            .and_then(|m| m.cache_ttl_seconds)
        else {
            return Ok(());
        };
        if outcome.http_code == 200
            && !outcome.block.block
            && !outcome.response.aborted
            && let Some(key) = ctx.cache_key.take()
        {
            ctx.state
                .cache
                .put(
                    key,
                    outcome.response.clone(),
                    std::time::Duration::from_secs(ttl),
                )
                .await;
            ctx.decide("cache_store", format!("stored ttl={ttl}s"));
        }
        Ok(())
    }
}

/// The standard online pipeline: 4 layers, run in a fixed order.
pub fn default_layers() -> Vec<Layer> {
    vec![
        Layer {
            name: "preprocess",
            nodes: vec![
                Box::new(ModelQuotaGate),
                Box::new(ResolveModel),
                Box::new(TenantEntitlement),
                Box::new(CacheLookup),
                Box::new(QuotaCheck),
            ],
        },
        Layer {
            name: "account_select",
            nodes: vec![Box::new(SelectAccount)],
        },
        Layer {
            name: "model_access",
            nodes: vec![
                Box::new(TenantRateLimit),
                Box::new(RateLimit),
                Box::new(ProductQpmLimit),
                Box::new(ModelQpmLimit),
                Box::new(AkTpmLimit),
                Box::new(UserBudgetGate),
                Box::new(CallEngine),
            ],
        },
        Layer {
            name: "post_process",
            nodes: vec![
                Box::new(CommonUsageNode),
                Box::new(CostCalc),
                Box::new(CacheStore),
            ],
        },
    ]
}

/// The provider a model is bound to in config, if any.
fn model_provider(ctx: &DagContext) -> Option<&str> {
    let name = &ctx.request.model_param_v2.as_ref()?.model_name;
    ctx.cfg.find_model(name).and_then(|m| m.provider.as_deref())
}

/// The model name the request currently targets (post-fallback).
fn served_model(ctx: &DagContext) -> &str {
    ctx.request
        .model_param_v2
        .as_ref()
        .map(|p| p.model_name.as_str())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    #[test]
    fn token_rate_maps_config_weights() {
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: m, protocol: openai-chat, token_rate: {read_cache: 0.1, write_cache: 1.25}}]";
        let cfg = gw_config::GatewayConfig::from_yaml(yaml).unwrap();
        let rate = super::token_rate(&cfg, "m");
        assert_eq!(
            (rate.read_cache_weight, rate.write_cache_weight),
            (0.1, 1.25)
        );
        assert!(!rate.prompt_includes_cache);
        assert_eq!(
            super::token_rate(&cfg, "absent"),
            gw_models::TokenRate::default()
        );
    }

    #[test]
    fn reserve_estimate_saturates_on_hostile_max_tokens() {
        use gw_models::params::ChatParams;
        use gw_models::{ChatMsg, GatewayRequest, ModelParamV2, TypedParams};
        let mut param = ModelParamV2::with_name(gw_consts::Protocol::OpenaiChat, "m");
        param.typed = Some(TypedParams::Chat(ChatParams {
            max_tokens: Some(i64::MAX),
            ..Default::default()
        }));
        let req = GatewayRequest {
            message: vec![ChatMsg::text("user", "hello")],
            model_param_v2: Some(param),
            ..Default::default()
        };
        let est = super::reserve_estimate(&req);
        assert!(est > 0 && est <= super::MAX_RESERVE);
    }
}
