//! The default node set for the online chat pipeline.
//! Metrics-reporting nodes are dropped by design.

use gw_consts::{ErrCode, Protocol};
use gw_models::{GResult, GatewayError};
use gw_state::BillingRecord;

use crate::context::DagContext;
use crate::executor::{DagNode, Layer};

/// preprocess/model_quota: per-(AK, model) daily token cap — AK override, else
/// tenant default, else unmetered (no counter is ever touched). Over-quota
/// degrades to the tenant's fallback model when one is configured; otherwise
/// the request passes and the per-AK daily cap stays the hard backstop.
/// Runs before resolve_model so a swap re-routes protocol, entitlement, and
/// cache to the served model.
pub struct ModelQuotaGate;

#[async_trait::async_trait]
impl DagNode for ModelQuotaGate {
    fn name(&self) -> &'static str {
        "model_quota"
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        let requested = match ctx.request.model_param_v2.as_ref() {
            Some(p) if !p.model_name.is_empty() => p.model_name.clone(),
            _ => return Ok(()),
        };
        let limit = ctx.ak.model_quotas.get(&requested).copied().or_else(|| {
            ctx.cfg
                .find_tenant(&ctx.ak.tenant)
                .and_then(|t| t.model_quotas.get(&requested).copied())
        });
        let Some(limit) = limit else {
            return Ok(());
        };
        let key = format!("{}|{requested}", ctx.ak.ak);
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
        let name = param.model_name.clone();
        let mt = if let Some(conf) = ctx.cfg.find_model(&name) {
            conf.protocol().ok_or_else(|| {
                GatewayError::internal(format!("config maps `{name}` to unknown type"))
            })?
        } else if let Some(direct) = Protocol::from_wire(&name) {
            direct // callers may address a wire model type directly
        } else {
            return Err(GatewayError::new(
                ErrCode::REQ_PARAM,
                404,
                format!("unknown model: {name}"),
            ));
        };
        param.protocol = mt;
        ctx.decide("resolve_model", format!("{name} -> {mt}"));
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

/// preprocess/cache_lookup: request-level cache lookup (in-memory, TTL-based).
/// On a hit, outcome is produced directly and the downstream account/rate-limit/
/// engine/billing nodes all short-circuit.
pub struct CacheLookup;

/// Cache key: sha256 of model name + messages + typed params + passthrough
/// params. Not keyed by tenant: entitlement gates before the cache, and a
/// per-tenant split would only shrink the hit rate.
fn cache_key_of(ctx: &DagContext) -> Option<String> {
    use sha2::{Digest, Sha256};
    let param = ctx.request.model_param_v2.as_ref()?;
    let mut h = Sha256::new();
    h.update(param.model_name.as_bytes());
    h.update(serde_json::to_vec(&ctx.request.message).ok()?);
    if let Some(t) = &param.typed {
        h.update(serde_json::to_vec(t).ok()?);
    }
    // `raw` params (seed, response_format, vendor extras) change the output;
    // omitting them would collide distinct requests onto one entry
    if !param.raw.is_null() {
        h.update(serde_json::to_vec(&param.raw).ok()?);
    }
    Some(hex::encode(h.finalize()))
}

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
        if let Some(cached) = ctx.state.cache.get(&key) {
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

/// Completion tokens reserved when the caller sets no max_tokens; settle
/// corrects to actuals, so the estimate only needs to be monotone.
const DEFAULT_COMPLETION_RESERVE: i64 = 256;

/// Cheap admission estimate: ~chars/4 prompt heuristic + requested max_tokens.
fn reserve_estimate(req: &gw_models::GatewayRequest) -> i64 {
    let prompt: usize = req.message.iter().map(|m| m.content.len()).sum();
    let max_out = req
        .model_param_v2
        .as_ref()
        .and_then(|p| p.typed.as_ref())
        .and_then(|t| match t {
            gw_models::TypedParams::Chat(c) => c.max_tokens,
            _ => None,
        });
    (prompt as i64 / 4).max(1) + max_out.unwrap_or(DEFAULT_COMPLETION_RESERVE)
}

/// preprocess/quota_check: AK daily-quota admission. Reserves the estimate
/// atomically (admitted while spent-before < limit), so concurrent in-flight
/// requests count against the budget instead of all passing a stale check;
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
        if ctx.cache_hit {
            return Ok(()); // cache hit doesn't consume quota
        }
        let est = reserve_estimate(&ctx.request);
        if !ctx
            .state
            .governance
            .quota_reserve(&ctx.ak.ak, est, ctx.ak.daily_token_quota)
            .await
        {
            return Err(GatewayError::new(
                ErrCode::STOP_LIMIT_MSG,
                429,
                format!("daily token quota exhausted for ak {}", ctx.ak.ak),
            ));
        }
        ctx.quota_reserved = Some(est);
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
        if ctx.cache_hit {
            return Ok(());
        }
        let mt = ctx
            .request
            .protocol()
            .ok_or_else(|| GatewayError::internal("select_account before resolve_model"))?;
        let provider = model_provider(ctx);
        let account = ctx
            .state
            .pool
            .select_healthy(mt, provider.as_deref(), &[], ctx.state.health.as_ref())
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
        if ctx.cache_hit {
            return Ok(());
        }
        let Some(qps) = ctx.cfg.find_tenant(&ctx.ak.tenant).and_then(|t| t.qps) else {
            return Ok(());
        };
        if !ctx
            .state
            .governance
            .rate_allow(&format!("tenant:{}", ctx.ak.tenant), qps)
            .await
        {
            return Err(GatewayError::new(
                ErrCode::STOP_LIMIT_MSG,
                429,
                format!(
                    "tenant rate limit exceeded for `{}` (qps {qps})",
                    ctx.ak.tenant
                ),
            ));
        }
        Ok(())
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
        if ctx.cache_hit {
            return Ok(());
        }
        if !ctx
            .state
            .governance
            .rate_allow(&ctx.ak.ak, ctx.ak.qps)
            .await
        {
            return Err(GatewayError::new(
                ErrCode::STOP_LIMIT_MSG,
                429,
                format!(
                    "rate limit exceeded for ak {} (qps {})",
                    ctx.ak.ak, ctx.ak.qps
                ),
            ));
        }
        Ok(())
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
        if ctx.cache_hit {
            return Ok(());
        }
        let Some(qpm) = ctx.cfg.find_product(&ctx.ak.product).and_then(|p| p.qpm) else {
            return Ok(());
        };
        let window = std::time::Duration::from_secs(60);
        if !ctx
            .state
            .governance
            .window_allow(&format!("product:{}", ctx.ak.product), qpm, window)
            .await
        {
            return Err(GatewayError::new(
                ErrCode::STOP_LIMIT_MSG,
                429,
                format!(
                    "product qpm limit exceeded for `{}` (qpm {qpm})",
                    ctx.ak.product
                ),
            ));
        }
        Ok(())
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
        if ctx.cache_hit {
            return Ok(());
        }
        let Some(param) = ctx.request.model_param_v2.as_ref() else {
            return Ok(());
        };
        let Some(qpm) = ctx.cfg.find_model(&param.model_name).and_then(|m| m.qpm) else {
            return Ok(());
        };
        let window = std::time::Duration::from_secs(60);
        if !ctx
            .state
            .governance
            .window_allow(&format!("model:{}", param.model_name), qpm, window)
            .await
        {
            return Err(GatewayError::new(
                ErrCode::STOP_LIMIT_MSG,
                429,
                format!(
                    "model qpm limit exceeded for `{}` (qpm {qpm})",
                    param.model_name
                ),
            ));
        }
        Ok(())
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
        if ctx.cache_hit {
            return Ok(());
        }
        let Some(tpm) = ctx.ak.tokens_per_minute else {
            return Ok(());
        };
        let window = std::time::Duration::from_secs(60);
        let est = ctx
            .quota_reserved
            .unwrap_or_else(|| reserve_estimate(&ctx.request));
        if !ctx
            .state
            .governance
            .token_window_reserve(&ctx.ak.ak, est, tpm, window)
            .await
        {
            return Err(GatewayError::new(
                ErrCode::STOP_LIMIT_MSG,
                429,
                format!(
                    "token-per-minute limit exceeded for ak {} (tpm {tpm})",
                    ctx.ak.ak
                ),
            ));
        }
        ctx.tpm_reserved = Some(est);
        Ok(())
    }
}

/// model_access/call_engine: factory dispatch + engine execution + failover.
///
/// On an upstream 5xx, the failed account is excluded and reselected once (a
/// PTU -> paygo spill sets `ptu_spillover`); a second failure is propagated
/// as-is.
pub struct CallEngine;

#[async_trait::async_trait]
impl DagNode for CallEngine {
    fn name(&self) -> &'static str {
        "call_engine"
    }
    fn deps(&self) -> &'static [&'static str] {
        &["ak_tpm"]
    }
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()> {
        if ctx.cache_hit {
            return Ok(());
        }
        let threshold = ctx.cfg.stability.failure_threshold;
        let cooldown = std::time::Duration::from_secs(ctx.cfg.stability.cooldown_seconds);
        let engine = gw_engines::get_engine(ctx.request.clone(), ctx.transport.clone())?;
        match engine.run().await {
            Ok(outcome) => {
                // an aborted stream is neither a success nor an account fault
                if !outcome.response.aborted
                    && let Some(a) = ctx.request.account.as_ref()
                {
                    ctx.state.health.record_success(&a.name).await;
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
                let mt = ctx
                    .request
                    .protocol()
                    .ok_or_else(|| GatewayError::internal("call_engine without model type"))?;
                let failed = ctx.request.account.clone().unwrap_or_default();
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
                        provider.as_deref(),
                        std::slice::from_ref(&failed.name),
                        ctx.state.health.as_ref(),
                    )
                    .await;
                let Some(next) = next else {
                    return Err(first_err); // no backup account available, propagate the original error
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
                        ctx.state.health.record_success(&next.name).await;
                        outcome.response.ptu_spillover = spillover;
                        ctx.outcome = Some(outcome);
                        Ok(())
                    }
                    Err(e) => {
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
        if ctx.cache_hit {
            return Ok(());
        }
        if let Some(outcome) = ctx.outcome.as_mut() {
            let resp = &mut outcome.response;
            resp.common_usage =
                gw_engines::extract_common_usage(&resp.raw_usage_json, resp.is_messages_protocol);
        }
        Ok(())
    }
}

/// post_process/cost_calc: local billing + quota consumption + ledger (metrics
/// reporting dropped by design).
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
        if ctx.cache_hit {
            return Ok(()); // cache hit is not billed and doesn't consume quota
        }
        let Some(outcome) = ctx.outcome.as_ref() else {
            return Ok(()); // nothing to bill
        };
        let resp = &outcome.response;
        // An aborted stream delivered text but never the final usage frame — bill it.
        // Gate on completion_tokens==0, not total: Anthropic reports input up front,
        // output only in the final message_delta the break skipped.
        if resp.aborted && resp.completion_tokens == 0 {
            let enc = gw_models::token_estimate::default_encoder();
            let param = ctx.request.model_param_v2.as_ref();
            let tools = param.and_then(|p| p.typed.as_ref()).and_then(|t| match t {
                gw_models::TypedParams::Chat(c) => c.tools.clone(),
                _ => None,
            });
            let model_name = param.map(|p| p.model_name.as_str()).unwrap_or_default();
            let pt = if resp.prompt_tokens > 0 {
                resp.prompt_tokens
            } else {
                gw_models::estimate_prompt_tokens(
                    &ctx.request.message,
                    tools.as_ref(),
                    model_name,
                    enc,
                )
            };
            let ct = enc.encode_len(&resp.message) as i64;
            ctx.decide("cost_calc", format!("aborted stream, billed {pt}+{ct}"));
            return bill(ctx, pt, ct, pt + ct).await;
        }
        // default rate is 1:1 (total == prompt+completion); the formula carries future weighted rates
        let (prompt, completion, total) = match &resp.common_usage {
            Some(u) => {
                let ti = gw_models::TokenInput {
                    prompt: u.platform_input,
                    read_cache: u.read_cache,
                    write_cache: u.write_cache,
                    completion: u.completion,
                    reasoning: u.reason,
                };
                let rate = gw_models::TokenRate::default();
                (
                    u.platform_input + u.read_cache + u.write_cache,
                    u.completion + u.reason,
                    gw_models::platform_total(&ti, &rate),
                )
            }
            None => (
                resp.prompt_tokens,
                resp.completion_tokens,
                resp.prompt_tokens + resp.completion_tokens,
            ),
        };
        bill(ctx, prompt, completion, total).await
    }
}

/// Consume quota/TPM and write the ledger record for one served request.
async fn bill(ctx: &mut DagContext, prompt: i64, completion: i64, total: i64) -> GResult<()> {
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
    let (p_in, p_out) = ctx.cfg.prices_for_tenant(&ctx.ak.tenant, served);
    let cost = prompt * p_in / 1000 + completion * p_out / 1000;
    let vendor_cost = ctx
        .request
        .account
        .as_ref()
        .map(|a| {
            prompt * a.cost_input_price_per_1k_micros / 1000
                + completion * a.cost_output_price_per_1k_micros / 1000
        })
        .unwrap_or(0);
    // settle reservations to actuals (the model-quota counter stays soft
    // post-hoc by design); independent keys run as one concurrent round-trip
    let quota_delta = total - ctx.quota_reserved.take().unwrap_or(0);
    match &ctx.model_quota_key {
        Some(key) => {
            tokio::join!(
                ctx.state.governance.quota_settle(&ctx.ak.ak, quota_delta),
                ctx.state.governance.quota_consume(key, total)
            );
        }
        None => {
            ctx.state
                .governance
                .quota_settle(&ctx.ak.ak, quota_delta)
                .await
        }
    }
    let window = std::time::Duration::from_secs(60);
    match ctx.tpm_reserved.take() {
        Some(est) => {
            ctx.state
                .governance
                .token_window_settle(&ctx.ak.ak, total - est, window)
                .await
        }
        None => {
            ctx.state
                .governance
                .token_window_add(&ctx.ak.ak, total, window)
                .await
        }
    }
    let record = BillingRecord {
        ak: ctx.ak.ak.clone(),
        product: ctx.ak.product.clone(),
        tenant: ctx.ak.tenant.clone(),
        model: requested.to_owned(),
        served_model: served.to_owned(),
        protocol: param
            .map(|p| p.protocol.as_str().to_owned())
            .unwrap_or_default(),
        account: ctx
            .request
            .account
            .as_ref()
            .map(|a| a.name.clone())
            .unwrap_or_default(),
        prompt_tokens: prompt,
        completion_tokens: completion,
        total_tokens: total,
        cost_micros: cost,
        vendor_cost_micros: vendor_cost,
        ptu_spillover,
    };
    // a ledger write failure must not fail an already-served response
    if let Err(e) = ctx.state.store.ledger_add(record).await {
        metrics::counter!("gateway_ledger_write_failures_total").increment(1);
        tracing::error!(error = %e, "billing ledger write failed");
    }
    ctx.decide("cost_calc", format!("tokens={total} cost_micros={cost}"));
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
        if ctx.cache_hit || ctx.request.stream || !ctx.request.is_online {
            return Ok(());
        }
        let (Some(key), Some(outcome)) = (ctx.cache_key.as_ref(), ctx.outcome.as_ref()) else {
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
        if outcome.http_code == 200 && !outcome.block.block && !outcome.response.aborted {
            ctx.state.cache.put(
                key.clone(),
                outcome.response.clone(),
                std::time::Duration::from_secs(ttl),
            );
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
fn model_provider(ctx: &DagContext) -> Option<String> {
    let name = ctx
        .request
        .model_param_v2
        .as_ref()
        .map(|p| p.model_name.clone())?;
    ctx.cfg.find_model(&name).and_then(|m| m.provider.clone())
}
