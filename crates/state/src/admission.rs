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
// a store that refuses writes must drain the queue rather than wedge it
const LEDGER_RETRY_ATTEMPTS: usize = 8;
// a month's counter must outlive the next month's rollover read
const MONTH_COUNTER_TTL: Duration = Duration::from_secs(62 * 86_400);

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

/// One budget: its window, governance counter and cap.
struct Budget {
    scope: BudgetScope,
    window: Window,
    key: String,
    limit: i64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Window {
    Day,
    Month,
}

impl Window {
    fn label(self) -> &'static str {
        match self {
            Self::Day => "daily",
            Self::Month => "monthly",
        }
    }
}

#[derive(Clone, Copy)]
enum BudgetScope {
    UserTokens,
    TenantCost,
    KeyCost,
    UserCost,
}

impl BudgetScope {
    fn is_per_user(self) -> bool {
        matches!(self, Self::UserTokens | Self::UserCost)
    }

    fn charges_cost(self) -> bool {
        !matches!(self, Self::UserTokens)
    }

    fn unit(self) -> &'static str {
        if self.charges_cost() { "cost" } else { "token" }
    }

    /// The governance counter: user scopes carry the tenant, month counters their calendar month.
    fn key(self, month: Option<(i64, u32)>, ak: &AkInfo, user: &str) -> String {
        let prefix = month.map_or(String::new(), month_prefix);
        match self {
            Self::UserTokens => format!("{prefix}ub:{}:{user}", ak.tenant),
            Self::TenantCost => format!("{prefix}cb:tenant:{}", ak.tenant),
            Self::KeyCost => format!("{prefix}cb:ak:{}", ak.ak_id),
            Self::UserCost => format!("{prefix}cb:user:{}:{user}", ak.tenant),
        }
    }

    /// The alert subject; the key is named by its fingerprint, never the credential.
    fn subject(self, ak: &AkInfo, user: &str) -> String {
        match self {
            Self::UserTokens | Self::UserCost => format!("user:{}/{user}", ak.tenant),
            Self::TenantCost => format!("tenant:{}", ak.tenant),
            Self::KeyCost => format!("key:{}", ak.ak_id),
        }
    }
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
        let deferred = store.defers_ledger_writes();
        let worker_store = store.clone();
        // the bounded worker owns accepted rows through caller cancellation
        tokio::spawn(async move {
            let mut batch = Vec::with_capacity(LEDGER_BATCH_MAX);
            let mut row_acks = Vec::with_capacity(LEDGER_BATCH_MAX);
            let mut ack = None;
            let mut dropped = 0u64;
            while let Some(msg) = pending.recv().await {
                msg.take(&mut batch, &mut row_acks, &mut ack);
                while ack.is_none() && batch.len() < LEDGER_BATCH_MAX {
                    let Ok(next) = pending.try_recv() else {
                        break;
                    };
                    next.take(&mut batch, &mut row_acks, &mut ack);
                }
                let unacked = (batch.len() - row_acks.len()) as u64;
                let committed = Self::commit(&worker_store, &mut batch).await;
                if !committed {
                    dropped += unacked;
                }
                for tx in row_acks.drain(..) {
                    if tx.send(committed).is_err() && !committed {
                        dropped += 1;
                    }
                }
                if let Some(tx) = ack.take() {
                    let _ = tx.send(std::mem::take(&mut dropped));
                }
            }
        });
        Self {
            store,
            queue: Some(queue),
            deferred,
        }
    }

    /// Commit what the writer still holds and return the rows it dropped since the last flush.
    pub(crate) async fn flush(&self) -> u64 {
        let Some(queue) = &self.queue else {
            return 0;
        };
        let (tx, rx) = oneshot::channel();
        if queue.send(LedgerWrite::Flush(tx)).await.is_err() {
            return 0;
        }
        rx.await.unwrap_or(0)
    }

    async fn write(&self, record: BillingRecord) {
        let record = if self.deferred
            && let Some(queue) = &self.queue
        {
            if record.user_id.is_empty() {
                match queue.try_send(LedgerWrite::Row(record, None)) {
                    Ok(()) => return,
                    Err(refused) => match refused.into_inner() {
                        LedgerWrite::Row(record, _) => record,
                        LedgerWrite::Flush(_) => return,
                    },
                }
            } else if Self::queue_attributed(queue, &record).await {
                return;
            } else {
                record
            }
        } else {
            record
        };
        let Err(e) = self.store.ledger_add(&record).await else {
            return;
        };
        metrics::counter!("gateway_ledger_write_failures_total").increment(1);
        let Some(queue) = &self.queue else {
            tracing::error!(error = %e, "billing ledger write failed");
            return;
        };
        tracing::error!(error = %e, "billing ledger write failed; queued for repair");
        if queue.send(LedgerWrite::Row(record, None)).await.is_err() {
            tracing::error!("billing ledger repair worker stopped");
        }
    }

    async fn queue_attributed(queue: &mpsc::Sender<LedgerWrite>, record: &BillingRecord) -> bool {
        let (tx, rx) = oneshot::channel();
        if queue
            .send(LedgerWrite::Row(record.clone(), Some(tx)))
            .await
            .is_err()
        {
            return false;
        }
        rx.await.unwrap_or(false)
    }

    async fn commit(store: &Arc<dyn Store>, batch: &mut Vec<BillingRecord>) -> bool {
        if batch.is_empty() {
            return true;
        }
        let mut delay = LEDGER_RETRY_INITIAL;
        let mut committed = false;
        for attempt in 1..=LEDGER_RETRY_ATTEMPTS {
            let Err(e) = store.ledger_add_batch(batch).await else {
                committed = true;
                break;
            };
            metrics::counter!("gateway_ledger_write_failures_total").increment(batch.len() as u64);
            if attempt == LEDGER_RETRY_ATTEMPTS {
                tracing::error!(error = %e, rows = batch.len(), "billing ledger retries exhausted; rows dropped");
                break;
            }
            tracing::error!(error = %e, rows = batch.len(), "billing ledger write failed; retrying");
            tokio::time::sleep(delay).await;
            delay = delay.saturating_mul(2).min(LEDGER_RETRY_MAX);
        }
        batch.clear();
        committed
    }
}

// the row is the common variant; boxing it would add an allocation per request
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum LedgerWrite {
    Row(BillingRecord, Option<oneshot::Sender<bool>>),
    Flush(oneshot::Sender<u64>),
}

impl LedgerWrite {
    fn take(
        self,
        batch: &mut Vec<BillingRecord>,
        row_acks: &mut Vec<oneshot::Sender<bool>>,
        ack: &mut Option<oneshot::Sender<u64>>,
    ) {
        match self {
            LedgerWrite::Row(r, tx) => {
                batch.push(r);
                if let Some(tx) = tx {
                    row_acks.push(tx);
                }
            }
            LedgerWrite::Flush(tx) => *ack = Some(tx),
        }
    }
}

/// Commit billing rows the batching writer still holds, before the process exits.
pub async fn flush_billing(state: &GatewayState) {
    let dropped = state.billing.flush().await;
    if dropped > 0 {
        tracing::error!(rows = dropped, "billing rows dropped before shutdown");
    }
}

/// Daily and monthly budgets (soft caps: check-then-consume, so concurrent
/// turns can overshoot by one): admit while every configured scope is under its cap.
pub async fn check_budgets(
    gov: &dyn Governance,
    cfg: &GatewayConfig,
    ak: &AkInfo,
    user: &str,
) -> Result<(), String> {
    for b in budgets(gov, cfg, ak, user).await {
        let under = match b.window {
            Window::Day => gov.quota_check(&b.key, b.limit).await,
            Window::Month => gov.counter_get(&b.key).await < b.limit,
        };
        if !under {
            return Err(format!(
                "{} {} budget exhausted for {}",
                b.window.label(),
                b.scope.unit(),
                b.scope.subject(ak, user)
            ));
        }
    }
    Ok(())
}

/// Accrue actual usage to the budgets; a scope that reaches its cap raises a
/// `budget_exhausted` alert (the bus dedups repeats).
pub async fn consume_budgets(
    state: &GatewayState,
    cfg: &GatewayConfig,
    ak: &AkInfo,
    user: &str,
    tokens: i64,
    cost_micros: i64,
) {
    let gov = state.governance.as_ref();
    for b in budgets(gov, cfg, ak, user).await {
        let amount = if b.scope.charges_cost() {
            cost_micros
        } else {
            tokens
        };
        if amount <= 0 {
            continue;
        }
        let used = match b.window {
            Window::Day => gov.quota_consume(&b.key, amount).await,
            Window::Month => gov.counter_add(&b.key, amount, MONTH_COUNTER_TTL).await,
        };
        if used >= b.limit {
            state.alerts.emit(
                "budget_exhausted",
                b.scope.subject(ak, user),
                format!(
                    "{} {} budget: {used} of {}",
                    b.window.label(),
                    b.scope.unit(),
                    b.limit
                ),
            );
        }
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
        format!("rate limit exceeded for key {} (qps {})", ak.ak_id, ak.qps)
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
        || format!("daily token quota exhausted for key {}", ak.ak_id),
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
            "token-per-minute limit exceeded for key {} (tpm {tpm})",
            ak.ak_id
        ))
    }
}

/// What a settled request cost, for the budgets and the decision trail.
pub struct Settled {
    pub total_tokens: i64,
    pub cost_micros: i64,
}

/// Settle reserves to actuals, accrue the per-(AK, model) counter and write the
/// ledger concurrently; a transient ledger failure goes to the bounded repair queue.
pub async fn settle_and_bill(
    state: &GatewayState,
    cfg: &GatewayConfig,
    s: SettleInput<'_>,
) -> Settled {
    let gov = state.governance.as_ref();
    let total = clamp_tokens(s.billing.total);
    let record = billing_record(cfg, &s.billing);
    let settled = Settled {
        total_tokens: record.total_tokens,
        cost_micros: record.cost_micros,
    };
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
    let write_ledger = state.billing.write(record);
    tokio::join!(settle_daily, consume_model, settle_tpm, write_ledger);
    settled
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

/// The tenant's budgets that apply to `ak` and `user`; empty when none is
/// configured. With rollover on, a month's cap grows by what the previous
/// month left unspent, at most one month's cap (one counter read per scope).
async fn budgets(
    gov: &dyn Governance,
    cfg: &GatewayConfig,
    ak: &AkInfo,
    user: &str,
) -> Vec<Budget> {
    let Some(t) = cfg.find_tenant(&ak.tenant) else {
        return Vec::new();
    };
    let scopes = [
        (
            BudgetScope::UserTokens,
            Window::Day,
            t.user_daily_token_quota,
        ),
        (
            BudgetScope::TenantCost,
            Window::Day,
            t.daily_cost_quota_micros,
        ),
        (
            BudgetScope::KeyCost,
            Window::Day,
            t.key_daily_cost_quota_micros,
        ),
        (
            BudgetScope::UserCost,
            Window::Day,
            t.user_daily_cost_quota_micros,
        ),
        (
            BudgetScope::TenantCost,
            Window::Month,
            t.monthly_cost_quota_micros,
        ),
        (
            BudgetScope::KeyCost,
            Window::Month,
            t.key_monthly_cost_quota_micros,
        ),
        (
            BudgetScope::UserCost,
            Window::Month,
            t.user_monthly_cost_quota_micros,
        ),
    ];
    let month = civil_month(crate::epoch_secs());
    let mut out = Vec::new();
    for (scope, window, limit) in scopes {
        let Some(mut limit) = limit else {
            continue;
        };
        if scope.is_per_user() && user.is_empty() {
            continue;
        }
        let key = match window {
            Window::Day => scope.key(None, ak, user),
            Window::Month => scope.key(Some(month), ak, user),
        };
        if window == Window::Month && t.monthly_cost_rollover {
            let spent = gov
                .counter_get(&scope.key(Some(previous_month(month)), ak, user))
                .await;
            limit = limit.saturating_add((limit - spent).clamp(0, limit));
        }
        out.push(Budget {
            scope,
            window,
            key,
            limit,
        });
    }
    out
}

/// The proleptic-Gregorian (year, month) of a UTC epoch second.
fn civil_month(epoch_secs: i64) -> (i64, u32) {
    let z = epoch_secs.div_euclid(86_400) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m as u32)
}

fn previous_month((y, m): (i64, u32)) -> (i64, u32) {
    if m == 1 { (y - 1, 12) } else { (y, m - 1) }
}

fn month_prefix((y, m): (i64, u32)) -> String {
    format!("m:{y}{m:02}:")
}

/// The counter prefixes of the current and previous month: everything the rollover still reads.
pub fn month_prefixes() -> [String; 2] {
    let month = civil_month(crate::epoch_secs());
    [month_prefix(month), month_prefix(previous_month(month))]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(request_id: impl Into<String>) -> BillingRecord {
        BillingRecord {
            ak: "ak".into(),
            product: "p".into(),
            tenant: "default".into(),
            user_id: String::new(),
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
        ledger.write(record.clone()).await;
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
    async fn attributed_write_returns_after_the_batch_commits() {
        let store = Arc::new(crate::MemoryStore::default());
        let ledger = BillingLedger {
            deferred: true,
            ..BillingLedger::repairing(store.clone())
        };
        let mut row = record("req-attributed");
        row.user_id = "user-42".into();

        ledger.write(row.clone()).await;

        let (count, rows) = store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(count, 1);
        assert_eq!(rows[0].request_id, "req-attributed");
    }

    #[tokio::test(start_paused = true)]
    async fn attributed_write_falls_back_to_a_direct_write_when_the_batch_is_dropped() {
        let store = Arc::new(crate::MemoryStore::default());
        store.fail_next_ledger_writes(LEDGER_RETRY_ATTEMPTS);
        let ledger = BillingLedger {
            deferred: true,
            ..BillingLedger::repairing(store.clone())
        };
        let mut row = record("req-fallback");
        row.user_id = "user-42".into();

        ledger.write(row.clone()).await;

        let (count, rows) = store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(count, 1);
        assert_eq!(rows[0].request_id, "req-fallback");
    }

    #[tokio::test(start_paused = true)]
    async fn canceled_attributed_write_is_reported_as_dropped() {
        let store = Arc::new(crate::MemoryStore::default());
        store.fail_next_ledger_writes(LEDGER_RETRY_ATTEMPTS);
        let ledger = BillingLedger::repairing(store);
        let (tx, rx) = oneshot::channel();
        drop(rx);

        ledger
            .queue
            .as_ref()
            .unwrap()
            .send(LedgerWrite::Row(record("req-canceled"), Some(tx)))
            .await
            .unwrap();

        assert_eq!(ledger.flush().await, 1);
    }

    #[tokio::test]
    async fn billing_ledger_backpressures_at_capacity_then_repairs_every_row() {
        let store = Arc::new(crate::MemoryStore::default());
        store.fail_next_ledger_writes(usize::MAX);
        let ledger = BillingLedger::repairing(store.clone());
        let queue = ledger.queue.as_ref().unwrap();

        ledger.write(record("req-0")).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while queue.capacity() != LEDGER_QUEUE_CAPACITY {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("repair worker did not take the first row");

        for i in 1..=LEDGER_QUEUE_CAPACITY {
            ledger.write(record(format!("req-{i}"))).await;
        }
        assert_eq!(queue.capacity(), 0);

        let blocked_record = record(format!("req-{}", LEDGER_QUEUE_CAPACITY + 1));
        let mut blocked = Box::pin(ledger.write(blocked_record.clone()));
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
        ledger.write(record("req-flush")).await;
        ledger.flush().await;
        let (count, rows) = store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(count, 1);
        assert_eq!(rows[0].request_id, "req-flush");
    }

    #[tokio::test(start_paused = true)]
    async fn billing_ledger_drains_a_store_that_never_accepts() {
        let store = Arc::new(crate::MemoryStore::default());
        store.fail_next_ledger_writes(usize::MAX);
        let ledger = BillingLedger::repairing(store.clone());

        tokio::time::timeout(Duration::from_secs(3_600), async {
            for i in 0..=LEDGER_QUEUE_CAPACITY {
                ledger.write(record(format!("req-{i}"))).await;
            }
        })
        .await
        .expect("write must not wedge on a permanently failing store");

        let queue = ledger.queue.as_ref().unwrap();
        tokio::time::timeout(Duration::from_secs(3_600), async {
            while queue.capacity() != LEDGER_QUEUE_CAPACITY {
                tokio::time::sleep(LEDGER_RETRY_INITIAL).await;
            }
        })
        .await
        .expect("the queue must drain once the retries are exhausted");
        assert_eq!(store.ledger_snapshot(usize::MAX).await.unwrap().0, 0);
        assert!(
            ledger.flush().await > 0,
            "flush reports the rows the exhausted retries dropped"
        );
    }

    #[test]
    fn civil_month_follows_the_gregorian_calendar() {
        assert_eq!(civil_month(0), (1970, 1));
        assert_eq!(civil_month(-1), (1969, 12));
        assert_eq!(civil_month(1_788_825_600), (2026, 9));
        assert_eq!(civil_month(1_767_225_599), (2025, 12));
        assert_eq!(civil_month(1_767_225_600), (2026, 1));
        assert_eq!(civil_month(951_782_400), (2000, 2));
        assert_eq!(civil_month(954_547_200), (2000, 4));
        assert_eq!(previous_month((2026, 1)), (2025, 12));
        assert_eq!(previous_month((2026, 9)), (2026, 8));
    }

    async fn monthly_fixture(
        rollover: bool,
    ) -> (Arc<GatewayConfig>, Arc<GatewayState>, Arc<AkInfo>) {
        let yaml = format!(
            "listen: {{host: h, port: 1}}\ntenants: [{{name: t1, key_monthly_cost_quota_micros: 3, monthly_cost_rollover: {rollover}}}]\naccess_keys: [{{ak: k1, tenant: t1, product: p, qps: 1, daily_token_quota: 1}}]"
        );
        let cfg = Arc::new(GatewayConfig::from_yaml(&yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let ak = state.auth.authenticate("k1").await.unwrap();
        (cfg, state, ak)
    }

    #[tokio::test]
    async fn monthly_keys_carry_their_month_and_rollover_carries_the_remainder() {
        let (cfg, state, ak) = monthly_fixture(true).await;
        let gov = state.governance.as_ref();
        let month = civil_month(crate::epoch_secs());
        let (y, m) = month;
        let prev = BudgetScope::KeyCost.key(Some(previous_month(month)), &ak, "");
        assert_eq!(
            BudgetScope::KeyCost.key(Some(month), &ak, ""),
            format!("m:{y}{m:02}:cb:ak:{}", ak.ak_id)
        );
        assert_eq!(
            BudgetScope::KeyCost.key(None, &ak, ""),
            format!("cb:ak:{}", ak.ak_id)
        );

        let untouched = budgets(gov, &cfg, &ak, "").await;
        assert_eq!(untouched.len(), 1);
        assert!(untouched[0].window == Window::Month);
        assert_eq!(
            untouched[0].limit, 6,
            "an unspent month carries its whole cap"
        );

        gov.counter_add(&prev, 1, MONTH_COUNTER_TTL).await;
        assert_eq!(budgets(gov, &cfg, &ak, "").await[0].limit, 5);
        gov.counter_add(&prev, 10, MONTH_COUNTER_TTL).await;
        assert_eq!(
            budgets(gov, &cfg, &ak, "").await[0].limit,
            3,
            "an overspent month carries nothing"
        );

        let (cfg, state, ak) = monthly_fixture(false).await;
        assert_eq!(
            budgets(state.governance.as_ref(), &cfg, &ak, "").await[0].limit,
            3
        );
    }

    #[tokio::test]
    async fn monthly_budget_denies_at_the_cap_without_touching_daily_counters() {
        let (cfg, state, ak) = monthly_fixture(false).await;
        let gov = state.governance.as_ref();
        check_budgets(gov, &cfg, &ak, "").await.unwrap();
        consume_budgets(&state, &cfg, &ak, "", 10, 3).await;
        let err = check_budgets(gov, &cfg, &ak, "").await.unwrap_err();
        assert!(
            err.starts_with("monthly cost budget exhausted for key:"),
            "{err}"
        );
        assert_eq!(gov.quota_used(&format!("cb:ak:{}", ak.ak_id)).await, 0);
        gov.quota_reset_all().await;
        assert!(
            check_budgets(gov, &cfg, &ak, "").await.is_err(),
            "the daily reset leaves month counters alone"
        );
    }
}
