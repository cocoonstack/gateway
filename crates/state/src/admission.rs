//! Shared admission checks and billing settlement — the one home for
//! governance key formats, limit-lookup semantics, and the reserve/settle
//! orchestration, called by both the DAG nodes and the realtime surface so the
//! two admission paths cannot drift. Denials carry the user-facing message;
//! callers wrap it in their own wire shape.

use gw_config::GatewayConfig;

use crate::store::{BillingInput, BillingRecord, Store, billing_record};
use crate::{AkInfo, Governance, clamp_tokens};

pub fn tenant_rate_key(tenant: &str) -> String {
    format!("tenant:{tenant}")
}

pub fn product_qpm_key(product: &str) -> String {
    format!("product:{product}")
}

pub fn model_qpm_key(model: &str) -> String {
    format!("model:{model}")
}

pub fn model_quota_key(ak: &str, model: &str) -> String {
    format!("{ak}|{model}")
}

/// The per-(AK, model) daily cap: AK override, else tenant default, else none.
pub fn model_quota_limit(cfg: &GatewayConfig, ak: &AkInfo, model: &str) -> Option<i64> {
    ak.model_quotas.get(model).copied().or_else(|| {
        cfg.find_tenant(&ak.tenant)
            .and_then(|t| t.model_quotas.get(model).copied())
    })
}

/// Pooled tenant QPS, when the tenant configures one.
pub async fn check_tenant_rate(
    gov: &dyn Governance,
    cfg: &GatewayConfig,
    tenant: &str,
) -> Result<(), String> {
    let Some(qps) = cfg.find_tenant(tenant).and_then(|t| t.qps) else {
        return Ok(());
    };
    if gov.rate_allow(&tenant_rate_key(tenant), qps).await {
        Ok(())
    } else {
        Err(format!(
            "tenant rate limit exceeded for `{tenant}` (qps {qps})"
        ))
    }
}

/// Per-AK QPS.
pub async fn check_ak_rate(gov: &dyn Governance, ak: &AkInfo) -> Result<(), String> {
    if gov.rate_allow(&ak.ak, ak.qps).await {
        Ok(())
    } else {
        Err(format!(
            "rate limit exceeded for ak {} (qps {})",
            ak.ak, ak.qps
        ))
    }
}

/// Product-level QPM, when the product configures one.
pub async fn check_product_qpm(
    gov: &dyn Governance,
    cfg: &GatewayConfig,
    product: &str,
) -> Result<(), String> {
    let Some(qpm) = cfg.find_product(product).and_then(|p| p.qpm) else {
        return Ok(());
    };
    if gov
        .window_allow(&product_qpm_key(product), qpm, gw_consts::MINUTE)
        .await
    {
        Ok(())
    } else {
        Err(format!(
            "product qpm limit exceeded for `{product}` (qpm {qpm})"
        ))
    }
}

/// Model-level QPM, when the model configures one.
pub async fn check_model_qpm(
    gov: &dyn Governance,
    cfg: &GatewayConfig,
    model: &str,
) -> Result<(), String> {
    let Some(qpm) = cfg.find_model(model).and_then(|m| m.qpm) else {
        return Ok(());
    };
    if gov
        .window_allow(&model_qpm_key(model), qpm, gw_consts::MINUTE)
        .await
    {
        Ok(())
    } else {
        Err(format!(
            "model qpm limit exceeded for `{model}` (qpm {qpm})"
        ))
    }
}

/// Reserve `amount` against the AK daily quota on the `at` day bucket.
pub async fn reserve_daily(
    gov: &dyn Governance,
    ak: &AkInfo,
    amount: i64,
    at: i64,
) -> Result<(), String> {
    if gov
        .quota_reserve(&ak.ak, amount, ak.daily_token_quota, at)
        .await
    {
        Ok(())
    } else {
        Err(format!("daily token quota exhausted for ak {}", ak.ak))
    }
}

/// Reserve `amount` in the AK TPM window; `Ok(None)` when the key has no TPM cap.
pub async fn reserve_tpm(
    gov: &dyn Governance,
    ak: &AkInfo,
    amount: i64,
) -> Result<Option<i64>, String> {
    let Some(tpm) = ak.tokens_per_minute else {
        return Ok(None);
    };
    if gov
        .token_window_reserve(&ak.ak, amount, tpm, gw_consts::MINUTE)
        .await
    {
        Ok(Some(amount))
    } else {
        Err(format!(
            "token-per-minute limit exceeded for ak {} (tpm {tpm})",
            ak.ak
        ))
    }
}

/// One settled call: identity + reserves to close.
pub struct SettleInput<'a> {
    pub billing: BillingInput<'a>,
    /// Tokens reserved against the daily quota at admission; 0 = unreserved
    /// (the settle degenerates to a plain add).
    pub reserved: i64,
    /// Tokens reserved in the TPM window; `None` = no TPM cap at admission.
    pub tpm_reserved: Option<i64>,
    /// Admission day bucket, so the settle lands where the reserve did.
    pub reserved_at: i64,
    /// Per-(AK, model) counter to accrue; `None` = no cap configured.
    pub model_quota_key: Option<String>,
}

/// Settle admission reserves to actuals, accrue the per-(AK, model) counter,
/// and write the ledger — one round of concurrent independent ops. Token
/// counts are clamped before metering; a ledger write failure is logged and
/// counted, never surfaced (the response was already served).
pub async fn settle_and_bill(
    gov: &dyn Governance,
    store: &dyn Store,
    cfg: &GatewayConfig,
    s: SettleInput<'_>,
) -> BillingRecord {
    let total = clamp_tokens(s.billing.total);
    let record = billing_record(cfg, &s.billing);
    let settle_daily = gov.quota_settle(s.billing.ak, total - s.reserved, s.reserved_at);
    let consume_model = async {
        if let Some(key) = &s.model_quota_key {
            gov.quota_consume(key, total).await;
        }
    };
    let settle_tpm = async {
        match s.tpm_reserved {
            Some(est) => {
                gov.token_window_settle(s.billing.ak, total - est, gw_consts::MINUTE)
                    .await
            }
            None if total > 0 => {
                gov.token_window_add(s.billing.ak, total, gw_consts::MINUTE)
                    .await
            }
            None => {}
        }
    };
    let write_ledger = async {
        if let Err(e) = store.ledger_add(&record).await {
            metrics::counter!("gateway_ledger_write_failures_total").increment(1);
            tracing::error!(error = %e, "billing ledger write failed");
        }
    };
    tokio::join!(settle_daily, consume_model, settle_tpm, write_ledger);
    record
}
