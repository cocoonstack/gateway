//! Shared admission checks and billing settlement — the one home for
//! governance key formats, limit-lookup semantics, and the reserve/settle
//! orchestration, called by both the DAG nodes and the realtime surface so the
//! two admission paths cannot drift. Denials carry the user-facing message;
//! callers wrap it in their own wire shape.

use std::sync::Arc;
use std::time::Duration;

use gw_config::GatewayConfig;
use tokio::sync::{mpsc, oneshot};

use crate::store::{BillingInput, BillingRecord, Store, billing_record};
use crate::{AkInfo, GatewayState, Governance, clamp_tokens};

const LEDGER_QUEUE_CAPACITY: usize = 4_096;
const LEDGER_BATCH_MAX: usize = 256;
const LEDGER_RETRY_INITIAL: Duration = Duration::from_millis(100);
const LEDGER_RETRY_MAX: Duration = Duration::from_secs(30);

/// Outcome of a tenant-fallback swap; `AlreadyServing` IS degraded (nowhere
/// further to go) and must not be denied as `Unconfigured`.
pub enum FallbackSwap {
    /// Swapped; carries `(requested, fallback)` for the decision trail.
    Swapped(String, String),
    AlreadyServing,
    Unconfigured,
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

#[derive(Debug, Clone)]
pub(crate) struct BillingLedger {
    store: Arc<dyn Store>,
    queue: Option<mpsc::Sender<LedgerWrite>>,
    deferred: bool,
}

impl BillingLedger {
    pub(crate) fn direct(store: Arc<dyn Store>) -> Self {
        Self {
            store,
            queue: None,
            deferred: false,
        }
    }

    pub(crate) fn repairing(store: Arc<dyn Store>) -> Self {
        let (queue, mut pending) = mpsc::channel::<LedgerWrite>(LEDGER_QUEUE_CAPACITY);
        let deferred = store.deferred_ledger_writes();
        let worker_store = store.clone();
        // The bounded worker owns accepted rows through caller cancellation.
        tokio::spawn(async move {
            let mut batch = Vec::with_capacity(LEDGER_BATCH_MAX);
            let mut ack = None;
            while let Some(msg) = pending.recv().await {
                msg.take(&mut batch, &mut ack);
                while ack.is_none() && batch.len() < LEDGER_BATCH_MAX {
                    let Ok(next) = pending.try_recv() else {
                        break;
                    };
                    next.take(&mut batch, &mut ack);
                }
                Self::commit(&worker_store, &mut batch).await;
                if let Some(tx) = ack.take() {
                    let _ = tx.send(());
                }
            }
        });
        Self {
            store,
            queue: Some(queue),
            deferred,
        }
    }

    /// Commit what the writer still holds; false means the worker stopped first.
    pub(crate) async fn flush(&self) -> bool {
        let Some(queue) = &self.queue else {
            return true;
        };
        let (tx, rx) = oneshot::channel();
        if queue.send(LedgerWrite::Flush(tx)).await.is_err() {
            return false;
        }
        rx.await.is_ok()
    }

    // deferred on SQL backends: a SIGKILL between the response and the next flush loses the queue plus the batch in flight (4352 rows at the defaults)
    async fn write(&self, record: &BillingRecord) {
        if self.deferred
            && let Some(queue) = &self.queue
            && queue.try_send(LedgerWrite::Row(record.clone())).is_ok()
        {
            return;
        }
        let Err(e) = self.store.ledger_add(record).await else {
            return;
        };
        metrics::counter!("gateway_ledger_write_failures_total").increment(1);
        let Some(queue) = &self.queue else {
            tracing::error!(error = %e, "billing ledger write failed");
            return;
        };
        tracing::error!(error = %e, "billing ledger write failed; queued for repair");
        if queue.send(LedgerWrite::Row(record.clone())).await.is_err() {
            tracing::error!("billing ledger repair worker stopped");
        }
    }

    async fn commit(store: &Arc<dyn Store>, batch: &mut Vec<BillingRecord>) {
        if batch.is_empty() {
            return;
        }
        let mut delay = LEDGER_RETRY_INITIAL;
        loop {
            let Err(e) = store.ledger_add_batch(batch).await else {
                batch.clear();
                return;
            };
            metrics::counter!("gateway_ledger_write_failures_total").increment(batch.len() as u64);
            tracing::error!(error = %e, rows = batch.len(), "billing ledger write failed; retrying");
            tokio::time::sleep(delay).await;
            delay = delay.saturating_mul(2).min(LEDGER_RETRY_MAX);
        }
    }
}

// the row is the common variant; boxing it would add an allocation per request
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum LedgerWrite {
    Row(BillingRecord),
    Flush(oneshot::Sender<()>),
}

impl LedgerWrite {
    fn take(self, batch: &mut Vec<BillingRecord>, ack: &mut Option<oneshot::Sender<()>>) {
        match self {
            LedgerWrite::Row(r) => batch.push(r),
            LedgerWrite::Flush(tx) => *ack = Some(tx),
        }
    }
}

/// Commit billing rows the batching writer still holds, before the process exits.
pub async fn flush_billing(state: &GatewayState) {
    if !state.billing.flush().await {
        tracing::error!("billing ledger repair worker stopped before shutdown flush");
    }
}

/// Per-user daily token budget (soft cap): admit while under the tenant's limit;
/// skipped without a limit or user attribution, consumed via [`consume_user_budget`].
pub async fn check_user_budget(
    gov: &dyn Governance,
    cfg: &GatewayConfig,
    tenant: &str,
    user: &str,
) -> Result<(), String> {
    if user.is_empty() {
        return Ok(());
    }
    let Some(limit) = user_budget_limit(cfg, tenant) else {
        return Ok(());
    };
    admit(
        gov.quota_check(&user_budget_key(tenant, user), limit).await,
        || format!("daily token budget exhausted for user `{user}`"),
    )
}

/// Accrue actual usage to the per-user daily budget (no-op without a limit or
/// user). Soft cap: check-then-consume, so a burst can overshoot by one turn.
pub async fn consume_user_budget(
    gov: &dyn Governance,
    cfg: &GatewayConfig,
    tenant: &str,
    user: &str,
    total: i64,
) {
    if user.is_empty() || total <= 0 {
        return;
    }
    if user_budget_limit(cfg, tenant).is_some() {
        gov.quota_consume(&user_budget_key(tenant, user), total)
            .await;
    }
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

/// Swap `param` to the tenant's fallback model, threading `fallback_from` for
/// billing/echo; shared by the quota gate and the moderation degrade.
pub fn swap_to_fallback(
    cfg: &GatewayConfig,
    tenant: &str,
    param: &mut gw_models::ModelParamV2,
) -> FallbackSwap {
    let Some(fb) = cfg
        .find_tenant(tenant)
        .and_then(|t| t.fallback_model.as_deref())
    else {
        return FallbackSwap::Unconfigured;
    };
    if fb == param.model_name {
        return FallbackSwap::AlreadyServing;
    }
    let from = std::mem::replace(&mut param.model_name, fb.to_owned());
    param.fallback_from = Some(from.clone());
    FallbackSwap::Swapped(from, param.model_name.clone())
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
    admit(gov.rate_allow(&tenant_rate_key(tenant), qps).await, || {
        format!("tenant rate limit exceeded for `{tenant}` (qps {qps})")
    })
}

/// Per-AK QPS.
pub async fn check_ak_rate(gov: &dyn Governance, ak: &AkInfo) -> Result<(), String> {
    admit(gov.rate_allow(&ak.ak, ak.qps).await, || {
        format!("rate limit exceeded for ak {} (qps {})", ak.ak, ak.qps)
    })
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
    admit(
        gov.window_allow(&product_qpm_key(product), qpm, gw_consts::MINUTE)
            .await,
        || format!("product qpm limit exceeded for `{product}` (qpm {qpm})"),
    )
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
    admit(
        gov.window_allow(&model_qpm_key(model), qpm, gw_consts::MINUTE)
            .await,
        || format!("model qpm limit exceeded for `{model}` (qpm {qpm})"),
    )
}

/// Reserve `amount` against the AK daily quota on the `at` day bucket.
pub async fn reserve_daily(
    gov: &dyn Governance,
    ak: &AkInfo,
    amount: i64,
    at: i64,
) -> Result<(), String> {
    admit(
        gov.quota_reserve(&ak.ak, amount, ak.daily_token_quota, at)
            .await,
        || format!("daily token quota exhausted for ak {}", ak.ak),
    )
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

/// Settle reserves to actuals, accrue the per-(AK, model) counter and write the
/// ledger concurrently; a transient ledger failure goes to the bounded repair queue.
pub async fn settle_and_bill(
    state: &GatewayState,
    cfg: &GatewayConfig,
    s: SettleInput<'_>,
) -> BillingRecord {
    let gov = state.governance.as_ref();
    let total = clamp_tokens(s.billing.total);
    let record = billing_record(cfg, &s.billing);
    let settle_daily = gov.quota_settle(s.billing.ak, total - s.reserved, s.reserved_at);
    let consume_model = async {
        if let Some(key) = &s.model_quota_key {
            // accrues to the CURRENT day: this counter has no paired reserve on the admission
            // bucket
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
    let write_ledger = state.billing.write(&record);
    tokio::join!(settle_daily, consume_model, settle_tpm, write_ledger);
    record
}

fn admit(ok: bool, deny: impl FnOnce() -> String) -> Result<(), String> {
    if ok { Ok(()) } else { Err(deny()) }
}

fn tenant_rate_key(tenant: &str) -> String {
    format!("tenant:{tenant}")
}

fn product_qpm_key(product: &str) -> String {
    format!("product:{product}")
}

fn model_qpm_key(model: &str) -> String {
    format!("model:{model}")
}

// namespaced by tenant so the same user id under two tenants meters separately
fn user_budget_key(tenant: &str, user: &str) -> String {
    format!("ub:{tenant}:{user}")
}

fn user_budget_limit(cfg: &GatewayConfig, tenant: &str) -> Option<i64> {
    cfg.find_tenant(tenant)
        .and_then(|t| t.user_daily_token_quota)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(request_id: impl Into<String>) -> BillingRecord {
        BillingRecord {
            ak: "ak".into(),
            product: "p".into(),
            tenant: "default".into(),
            user_id: "u".into(),
            request_id: request_id.into(),
            created_at_epoch_secs: 1,
            model: "m".into(),
            served_model: "m".into(),
            protocol: "openai-chat".into(),
            account: "a".into(),
            prompt_tokens: 1,
            completion_tokens: 1,
            total_tokens: 2,
            cost_micros: 0,
            vendor_cost_micros: 0,
            billed_units: 0,
            ptu_spillover: false,
            estimated: false,
        }
    }

    #[tokio::test]
    async fn billing_ledger_repairs_failed_writes() {
        let store = Arc::new(crate::MemoryStore::default());
        store.fail_next_ledger_writes(2);
        let ledger = BillingLedger::repairing(store.clone());
        let record = record("req-repair");
        ledger.write(&record).await;
        let (count, rows) = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = store.ledger_snapshot(usize::MAX).await.unwrap();
                if snapshot.0 == 1 {
                    break snapshot;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("ledger repair did not finish");
        assert_eq!(count, 1);
        assert_eq!(rows[0].request_id, "req-repair");
    }

    #[tokio::test]
    async fn billing_ledger_backpressures_at_capacity_then_repairs_every_row() {
        let store = Arc::new(crate::MemoryStore::default());
        store.fail_next_ledger_writes(usize::MAX);
        let ledger = BillingLedger::repairing(store.clone());
        let queue = ledger.queue.as_ref().unwrap();

        ledger.write(&record("req-0")).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while queue.capacity() != LEDGER_QUEUE_CAPACITY {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("repair worker did not take the first row");

        for i in 1..=LEDGER_QUEUE_CAPACITY {
            ledger.write(&record(format!("req-{i}"))).await;
        }
        assert_eq!(queue.capacity(), 0);

        let blocked_record = record(format!("req-{}", LEDGER_QUEUE_CAPACITY + 1));
        let mut blocked = Box::pin(ledger.write(&blocked_record));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), blocked.as_mut())
                .await
                .is_err(),
            "a full queue must apply backpressure"
        );

        store.fail_next_ledger_writes(0);
        tokio::time::timeout(Duration::from_secs(2), blocked)
            .await
            .expect("queue did not resume after store recovery");

        let expected = LEDGER_QUEUE_CAPACITY + 2;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if store.ledger_snapshot(usize::MAX).await.unwrap().0 == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queued ledger rows were not repaired");
    }

    #[tokio::test]
    async fn flush_returns_only_after_queued_rows_commit() {
        let store = Arc::new(crate::MemoryStore::default());
        store.fail_next_ledger_writes(1);
        let ledger = BillingLedger::repairing(store.clone());
        ledger.write(&record("req-flush")).await;
        assert!(ledger.flush().await);
        let (count, rows) = store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(count, 1);
        assert_eq!(rows[0].request_id, "req-flush");
    }

    #[tokio::test(start_paused = true)]
    async fn billing_ledger_retries_until_store_recovers() {
        let store = Arc::new(crate::MemoryStore::default());
        store.fail_next_ledger_writes(10);
        let ledger = BillingLedger::repairing(store.clone());

        ledger.write(&record("req-eventual-repair")).await;
        assert!(ledger.flush().await);

        let (count, rows) = store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(count, 1);
        assert_eq!(rows[0].request_id, "req-eventual-repair");
    }
}
