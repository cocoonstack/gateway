//! Shared admission checks and billing settlement — the one home for
//! governance key formats, limit-lookup semantics, and the reserve/settle
//! orchestration, called by both the DAG nodes and the realtime surface so the
//! two admission paths cannot drift. Denials carry the user-facing message;
//! callers wrap it in their own wire shape.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use gw_config::{GatewayConfig, TenantConf};
use gw_models::GResult;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use tokio_util::task::{TaskTracker, task_tracker::TaskTrackerToken};

use crate::store::{BillingInput, BillingRecord, Store, billing_record};
use crate::{AkInfo, GatewayState, Governance, UserBudget, clamp_tokens};

const LEDGER_QUEUE_CAPACITY: usize = 4_096;
const LEDGER_BATCH_MAX: usize = 256;
const LEDGER_RETRY_INITIAL: Duration = Duration::from_millis(100);
const LEDGER_RETRY_MAX: Duration = Duration::from_secs(30);
// a store that refuses writes must drain the queue rather than wedge it
const LEDGER_RETRY_ATTEMPTS: usize = 8;
// with a spool, rows leave the queue sooner, so an outage does not back up into requests
const LEDGER_SPOOL_AFTER_ATTEMPTS: usize = 3;
const LEDGER_REPLAY_INTERVAL: Duration = Duration::from_secs(30);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TpmReserve {
    pub est: i64,
    pub window: Option<i64>,
}

/// Tokens and cost a call charges the budgets: the admission estimate, or the settled actuals.
#[derive(Debug, Clone, Copy, Default)]
pub struct BudgetSpend {
    pub tokens: i64,
    pub cost_micros: i64,
}

impl BudgetSpend {
    fn of(self, scope: BudgetScope) -> i64 {
        if scope.charges_cost() {
            self.cost_micros
        } else {
            self.tokens
        }
    }
}

/// Day-budget amounts held at admission, so concurrent calls count before they settle.
#[derive(Debug, Default)]
pub struct BudgetHold {
    held: Vec<(String, i64)>,
    at: i64,
}

impl BudgetHold {
    /// Return every held amount, for a call that never reached billing.
    pub async fn refund(self, gov: &dyn Governance) {
        if self.held.is_empty() {
            return;
        }
        let refunds: Vec<_> = self
            .held
            .iter()
            .map(|(key, amount)| (key.as_str(), -amount))
            .collect();
        gov.quota_settle_all(&refunds, self.at).await;
    }

    fn take(&mut self, key: &str) -> Option<i64> {
        let i = self.held.iter().position(|(k, _)| k == key)?;
        Some(self.held.swap_remove(i).1)
    }
}

/// One settled call: identity + reserves to close.
pub struct SettleInput<'a> {
    pub billing: BillingInput<'a>,
    /// Tokens reserved against the daily quota at admission; 0 = unreserved
    /// (the settle degenerates to a plain add).
    pub reserved: i64,
    /// Tokens reserved in the TPM window; `None` = no TPM cap at admission.
    pub tpm_reserved: Option<TpmReserve>,
    /// Admission day bucket, so the settle lands where the reserve did.
    pub reserved_at: i64,
    /// Per-(AK, model) counter to accrue; `None` = no cap configured.
    pub model_quota_key: Option<String>,
    /// The key whose budgets the call charges, and the user override admission resolved.
    pub budget_ak: &'a AkInfo,
    pub user_budget: Option<UserBudget>,
    /// Day-budget amounts admission held; the settle charges the difference.
    pub budget_hold: BudgetHold,
}

/// A settle in progress: shutdown waits for it to drop and fires `closing` so it bills without waiting.
pub struct SettleGuard {
    _tracked: TaskTrackerToken,
    closing: CancellationToken,
}

impl SettleGuard {
    pub fn closing(&self) -> WaitForCancellationFuture<'_> {
        self.closing.cancelled()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BillingLedger {
    store: Arc<dyn Store>,
    queue: Option<mpsc::Sender<LedgerWrite>>,
    deferred: bool,
    settles: TaskTracker,
    closing: CancellationToken,
}

impl BillingLedger {
    pub(crate) fn direct(store: Arc<dyn Store>) -> Self {
        Self {
            store,
            queue: None,
            deferred: false,
            settles: TaskTracker::new(),
            closing: CancellationToken::new(),
        }
    }

    /// A batching writer; with `spool_path`, rows the store keeps refusing wait there instead of being dropped.
    pub(crate) fn repairing(store: Arc<dyn Store>, spool_path: &str) -> Self {
        let (queue, mut pending) = mpsc::channel::<LedgerWrite>(LEDGER_QUEUE_CAPACITY);
        let deferred = store.defers_ledger_writes();
        let worker_store = store.clone();
        let spool = (!spool_path.is_empty()).then(|| Arc::new(LedgerSpool::new(spool_path)));
        if let Some(spool) = spool.clone() {
            let replay_store = store.clone();
            // detached: replays the spool for the life of the process
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(LEDGER_REPLAY_INTERVAL);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    spool.replay(&replay_store).await;
                }
            });
        }
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
                let committed = Self::commit(&worker_store, spool.as_deref(), &mut batch).await;
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
            queue: Some(queue),
            deferred,
            ..Self::direct(store)
        }
    }

    pub(crate) fn begin_settle(&self) -> SettleGuard {
        SettleGuard {
            _tracked: self.settles.token(),
            closing: self.closing.clone(),
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

    async fn commit(
        store: &Arc<dyn Store>,
        spool: Option<&LedgerSpool>,
        batch: &mut Vec<BillingRecord>,
    ) -> bool {
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
            if let Some(spool) = spool
                && attempt >= LEDGER_SPOOL_AFTER_ATTEMPTS
            {
                match spool.append(batch).await {
                    Ok(()) => {
                        tracing::warn!(error = %e, rows = batch.len(), "billing ledger write failed; rows spooled for replay");
                        committed = true;
                        break;
                    }
                    Err(spool_err) => {
                        tracing::error!(error = %spool_err, rows = batch.len(), "billing spool write failed")
                    }
                }
            }
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

struct LedgerSpool {
    path: PathBuf,
    replaying: PathBuf,
    replayed: PathBuf,
    lock: Mutex<()>,
}

impl LedgerSpool {
    fn new(path: &str) -> Self {
        let path = PathBuf::from(path);
        Self {
            replaying: path.with_extension("replaying"),
            replayed: path.with_extension("replayed"),
            path,
            lock: Mutex::new(()),
        }
    }

    async fn append(&self, rows: &[BillingRecord]) -> std::io::Result<()> {
        // a leading newline ends a row a crash tore
        let mut lines = vec![b'\n'];
        for r in rows {
            serde_json::to_writer(&mut lines, r)?;
            lines.push(b'\n');
        }
        let path = self.path.clone();
        let _held = self.lock.lock().await;
        run_blocking(move || {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let mut f = OpenOptions::new().create(true).append(true).open(&path)?;
            f.write_all(&lines)?;
            f.sync_data()
        })
        .await
    }

    /// Resumes after the last batch it saved; `request_id` uniqueness covers one committed but not saved.
    async fn replay(&self, store: &Arc<dyn Store>) {
        let claimed = {
            let (path, replaying, replayed) = (
                self.path.clone(),
                self.replaying.clone(),
                self.replayed.clone(),
            );
            let _held = self.lock.lock().await;
            run_blocking(move || {
                if replaying.exists() {
                    let text = std::fs::read_to_string(&replayed).unwrap_or_default();
                    return Ok(Some(text.trim().parse().unwrap_or(0)));
                }
                if !path.exists() {
                    return Ok(None);
                }
                remove_if_present(&replayed)?;
                std::fs::rename(&path, &replaying).map(|()| Some(0))
            })
            .await
        };
        let mut offset = match claimed {
            Ok(Some(offset)) => offset,
            Ok(None) => return,
            Err(e) => {
                tracing::error!(error = %e, path = %self.path.display(), "billing spool claim failed");
                return;
            }
        };
        let mut total = 0;
        loop {
            let replaying = self.replaying.clone();
            let (rows, next) = match run_blocking(move || read_spool_batch(&replaying, offset))
                .await
            {
                Ok(batch) => batch,
                Err(e) => {
                    tracing::error!(error = %e, path = %self.replaying.display(), "billing spool read failed");
                    return;
                }
            };
            if next == offset {
                break;
            }
            if let Err(e) = store.ledger_replay_batch(&rows).await {
                tracing::warn!(error = %e, rows = rows.len(), "billing spool replay deferred");
                return;
            }
            total += rows.len();
            offset = next;
            let offset_file = self.replayed.clone();
            if let Err(e) =
                run_blocking(move || std::fs::write(offset_file, offset.to_string())).await
            {
                tracing::warn!(error = %e, "billing spool offset save failed");
            }
        }
        let (replaying, offset_file) = (self.replaying.clone(), self.replayed.clone());
        if let Err(e) = run_blocking(move || {
            std::fs::remove_file(replaying).and_then(|()| remove_if_present(&offset_file))
        })
        .await
        {
            tracing::error!(error = %e, path = %self.replaying.display(), "billing spool remove failed");
            return;
        }
        tracing::info!(rows = total, "billing spool replayed");
    }
}

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

    /// The alert subject; the key is named by its `ak_id`, never the credential.
    fn subject(self, ak: &AkInfo, user: &str) -> String {
        match self {
            Self::UserTokens | Self::UserCost => format!("user:{}/{user}", ak.tenant),
            Self::TenantCost => format!("tenant:{}", ak.tenant),
            Self::KeyCost => format!("key:{}", ak.ak_id),
        }
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

/// Finish in-flight settles, then commit the rows the batching writer holds, before the process exits.
pub async fn flush_billing(state: &GatewayState) {
    state.billing.closing.cancel();
    state.billing.settles.close();
    state.billing.settles.wait().await;
    let dropped = state.billing.flush().await;
    if dropped > 0 {
        tracing::error!(rows = dropped, "billing rows dropped before shutdown");
    }
}

/// Hold the call's estimate against each day budget while spent plus held stays under its cap; month budgets only check.
pub async fn reserve_budgets(
    state: &GatewayState,
    cfg: &GatewayConfig,
    ak: &AkInfo,
    user: &str,
    est: impl FnOnce() -> BudgetSpend,
    at: i64,
) -> GResult<Result<(UserBudget, BudgetHold), String>> {
    let over = user_budget(state, ak, user).await?;
    let gov = state.governance.as_ref();
    let mut budgets = budgets(gov, cfg, ak, user, over).await;
    if budgets.is_empty() {
        return Ok(Ok((
            over,
            BudgetHold {
                held: Vec::new(),
                at,
            },
        )));
    }
    let est = if budgets.iter().any(|b| b.window == Window::Day) {
        est()
    } else {
        BudgetSpend::default()
    };
    let holds = |b: &Budget| b.window == Window::Day && est.of(b.scope) > 0;
    let hold_args: Vec<_> = budgets
        .iter()
        .filter(|b| holds(b))
        .map(|b| (b.key.as_str(), est.of(b.scope), b.limit))
        .collect();
    let (reserved, checked) = tokio::join!(
        async {
            if hold_args.is_empty() {
                Ok(true)
            } else {
                gov.quota_reserve_all(&hold_args, at).await
            }
        },
        futures::future::join_all(budgets.iter().map(|b| async move {
            match b.window {
                _ if holds(b) => true,
                Window::Day => gov.quota_check(&b.key, b.limit).await,
                Window::Month => gov.counter_get(&b.key).await < b.limit,
            }
        })),
    );
    let refused = reserved
        .err()
        .and_then(|n| budgets.iter().enumerate().filter(|(_, b)| holds(b)).nth(n))
        .map(|(i, _)| i);
    let Some(i) = checked
        .iter()
        .position(|ok| !ok)
        .into_iter()
        .chain(refused)
        .min()
    else {
        let held = if reserved == Ok(true) {
            budgets
                .iter_mut()
                .filter(|b| holds(b))
                .map(|b| (std::mem::take(&mut b.key), est.of(b.scope)))
                .collect()
        } else {
            Vec::new()
        };
        return Ok(Ok((over, BudgetHold { held, at })));
    };
    if reserved == Ok(true) {
        let refunds: Vec<_> = hold_args
            .iter()
            .map(|&(key, amount, _)| (key, -amount))
            .collect();
        gov.quota_settle_all(&refunds, at).await;
    }
    let b = &budgets[i];
    Ok(Err(format!(
        "{} {} budget exhausted for {}",
        b.window.label(),
        b.scope.unit(),
        b.scope.subject(ak, user)
    )))
}

pub fn model_quota_key(ak_id: &str, model: &str) -> String {
    format!("{ak_id}|{model}")
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
    let chat = matches!(param.typed, Some(gw_models::TypedParams::Chat(_)));
    let Some(fb) = cfg
        .find_tenant(tenant)
        .and_then(|t| t.fallback_model.as_deref())
        .filter(|fb| {
            chat || cfg
                .find_model(fb)
                .and_then(|m| m.protocol())
                .is_some_and(|p| p.serves(param.protocol))
        })
    else {
        return FallbackSwap::Unconfigured;
    };
    if fb == param.model_name {
        return FallbackSwap::AlreadyServing;
    }
    let from = std::mem::replace(&mut param.model_name, fb.to_owned());
    if param.fallback_from.is_none() {
        param.fallback_from = Some(from.clone());
    }
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
    admit(
        gov.rate_allow(&format!("tenant:{tenant}"), qps).await,
        || format!("tenant rate limit exceeded for `{tenant}` (qps {qps})"),
    )
}

/// Per-AK QPS.
pub async fn check_ak_rate(gov: &dyn Governance, ak: &AkInfo) -> Result<(), String> {
    admit(gov.rate_allow(&ak.ak_id, ak.qps).await, || {
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
        gov.window_allow(&format!("product:{product}"), qpm, gw_consts::MINUTE)
            .await,
        || format!("product qpm limit exceeded for `{product}` (qpm {qpm})"),
    )
}

/// Key QPS, tenant QPS, then product QPM: the limits a request spends once.
pub async fn check_request_rates(
    gov: &dyn Governance,
    cfg: &GatewayConfig,
    ak: &AkInfo,
) -> Result<(), String> {
    check_ak_rate(gov, ak).await?;
    check_tenant_rate(gov, cfg, &ak.tenant).await?;
    check_product_qpm(gov, cfg, &ak.product).await
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
        gov.window_allow(&format!("model:{model}"), qpm, gw_consts::MINUTE)
            .await,
        || format!("model qpm limit exceeded for `{model}` (qpm {qpm})"),
    )
}

/// Reserve `amount` against the AK daily quota on the `at` day bucket; yields the amount recorded.
pub async fn reserve_daily(
    gov: &dyn Governance,
    ak: &AkInfo,
    amount: i64,
    at: i64,
) -> Result<i64, String> {
    gov.quota_reserve(&ak.ak_id, amount, ak.daily_token_quota, at)
        .await
        .ok_or_else(|| format!("daily token quota exhausted for key {}", ak.ak_id))
}

/// Reserve `amount` in the AK TPM window; `Ok(None)` when the key has no TPM cap.
pub async fn reserve_tpm(
    gov: &dyn Governance,
    ak: &AkInfo,
    amount: i64,
) -> Result<Option<TpmReserve>, String> {
    let Some(tpm) = ak.tokens_per_minute else {
        return Ok(None);
    };
    match gov
        .token_window_reserve(&ak.ak_id, amount, tpm, gw_consts::MINUTE)
        .await
    {
        Some(window) => Ok(Some(TpmReserve {
            est: amount,
            window: Some(window),
        })),
        None => Err(format!(
            "token-per-minute limit exceeded for key {} (tpm {tpm})",
            ak.ak_id
        )),
    }
}

/// Settle reserves to actuals, accrue the per-(AK, model) counter and write the
/// ledger concurrently; a transient ledger failure goes to the bounded repair queue.
pub async fn settle_and_bill(
    state: &GatewayState,
    cfg: &GatewayConfig,
    s: SettleInput<'_>,
) -> BudgetSpend {
    let gov = state.governance.as_ref();
    let total = clamp_tokens(s.billing.total);
    let record = billing_record(cfg, &s.billing);
    let settled = BudgetSpend {
        tokens: record.total_tokens,
        cost_micros: record.cost_micros,
    };
    let settle_daily = gov.quota_settle(s.billing.ak, total - s.reserved, s.reserved_at);
    let consume_model = async {
        if let Some(key) = &s.model_quota_key {
            // accrues to the current day: no reserve pins this counter to the admission bucket
            gov.quota_consume(key, total).await;
        }
    };
    let settle_tpm = async {
        if let Some(tpm) = s.tpm_reserved {
            gov.token_window_settle(s.billing.ak, total - tpm.est, gw_consts::MINUTE, tpm.window)
                .await;
        }
    };
    let charge_budgets = consume_budgets(
        state,
        cfg,
        s.budget_ak,
        s.billing.user_id,
        s.user_budget,
        s.budget_hold,
        settled,
    );
    let write_ledger = state.billing.write(record);
    tokio::join!(
        settle_daily,
        consume_model,
        settle_tpm,
        charge_budgets,
        write_ledger
    );
    settled
}

/// The counter prefixes of the current and previous month: everything the rollover still reads.
pub fn month_prefixes() -> [String; 2] {
    let month = civil_month(crate::epoch_secs());
    [month_prefix(month), month_prefix(previous_month(month))]
}

/// Accrue usage under `over` (`None` resolves it now) and settle `hold`; a scope at its cap alerts.
async fn consume_budgets(
    state: &GatewayState,
    cfg: &GatewayConfig,
    ak: &AkInfo,
    user: &str,
    over: Option<UserBudget>,
    mut hold: BudgetHold,
    spent: BudgetSpend,
) {
    let gov = state.governance.as_ref();
    if spent.tokens <= 0 && spent.cost_micros <= 0 {
        hold.refund(gov).await;
        return;
    }
    let over = match over {
        Some(over) => over,
        None => user_budget(state, ak, user).await.unwrap_or_else(|e| {
            tracing::warn!(error = %e, "user budget lookup failed at settlement; charging tenant defaults");
            UserBudget::default()
        }),
    };
    let budgets = budgets(gov, cfg, ak, user, over).await;
    if budgets.is_empty() {
        return;
    }
    let (mut settles, mut deltas, mut accrues) = (Vec::new(), Vec::new(), Vec::new());
    for (i, b) in budgets.iter().enumerate() {
        let amount = spent.of(b.scope);
        match (b.window, hold.take(&b.key)) {
            (Window::Day, Some(held)) => {
                settles.push(i);
                deltas.push((b.key.as_str(), amount - held));
            }
            _ if amount <= 0 => {}
            _ => accrues.push(i),
        }
    }
    let (settled, accrued) = tokio::join!(
        async {
            if deltas.is_empty() {
                Vec::new()
            } else {
                gov.quota_settle_all(&deltas, hold.at).await
            }
        },
        futures::future::join_all(accrues.iter().map(|&i| {
            let b = &budgets[i];
            let amount = spent.of(b.scope);
            async move {
                match b.window {
                    Window::Day => gov.quota_consume(&b.key, amount).await,
                    Window::Month => gov.counter_add(&b.key, amount, MONTH_COUNTER_TTL).await,
                }
            }
        })),
    );
    let totals = settles
        .iter()
        .zip(settled)
        .chain(accrues.iter().zip(accrued));
    for (&i, used) in totals {
        let b = &budgets[i];
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

fn admit(ok: bool, deny: impl FnOnce() -> String) -> Result<(), String> {
    if ok { Ok(()) } else { Err(deny()) }
}

/// `user`'s override in `ak`'s tenant; all-inherit without a user or an override.
async fn user_budget(state: &GatewayState, ak: &AkInfo, user: &str) -> GResult<UserBudget> {
    if user.is_empty() {
        return Ok(UserBudget::default());
    }
    Ok(state
        .user_budgets
        .get(&ak.tenant, user)
        .await?
        .unwrap_or_default())
}

/// The tenant's caps for `ak` and `user`, `over` replacing its per-user defaults.
/// With rollover on, a month's cap grows by what the previous month left
/// unspent, at most one month's cap (one counter read per scope).
async fn budgets(
    gov: &dyn Governance,
    cfg: &GatewayConfig,
    ak: &AkInfo,
    user: &str,
    over: UserBudget,
) -> Vec<Budget> {
    use BudgetScope::{KeyCost, TenantCost, UserCost, UserTokens};
    use Window::{Day, Month};
    let t = cfg.find_tenant(&ak.tenant);
    let tv = |f: fn(&TenantConf) -> Option<i64>| t.and_then(f);
    let user_tokens = over
        .daily_token_quota
        .resolve(tv(|t| t.user_daily_token_quota));
    let user_day = over
        .daily_cost_quota_micros
        .resolve(tv(|t| t.user_daily_cost_quota_micros));
    let user_month = over
        .monthly_cost_quota_micros
        .resolve(tv(|t| t.user_monthly_cost_quota_micros));
    let scopes = [
        (UserTokens, Day, user_tokens),
        (TenantCost, Day, tv(|t| t.daily_cost_quota_micros)),
        (KeyCost, Day, tv(|t| t.key_daily_cost_quota_micros)),
        (UserCost, Day, user_day),
        (TenantCost, Month, tv(|t| t.monthly_cost_quota_micros)),
        (KeyCost, Month, tv(|t| t.key_monthly_cost_quota_micros)),
        (UserCost, Month, user_month),
    ];
    let month = civil_month(crate::epoch_secs());
    let mut out = Vec::new();
    for (scope, window, limit) in scopes {
        let Some(limit) = limit else {
            continue;
        };
        if scope.is_per_user() && user.is_empty() {
            continue;
        }
        let key = match window {
            Window::Day => scope.key(None, ak, user),
            Window::Month => scope.key(Some(month), ak, user),
        };
        out.push(Budget {
            scope,
            window,
            key,
            limit,
        });
    }
    if t.is_some_and(|t| t.monthly_cost_rollover) {
        let months = out.iter().filter(|b| b.window == Window::Month);
        let spent = futures::future::join_all(months.map(|b| {
            let key = b.scope.key(Some(previous_month(month)), ak, user);
            async move { gov.counter_get(&key).await }
        }))
        .await;
        for (b, spent) in out
            .iter_mut()
            .filter(|b| b.window == Window::Month)
            .zip(spent)
        {
            b.limit = b.limit.saturating_add((b.limit - spent).clamp(0, b.limit));
        }
    }
    out
}

/// The proleptic-Gregorian (year, month) of a UTC epoch second.
fn civil_month(epoch_secs: i64) -> (i64, u32) {
    let (y, m, _) = gw_models::civil_from_days(epoch_secs.div_euclid(86_400));
    (y, m)
}

fn previous_month((y, m): (i64, u32)) -> (i64, u32) {
    if m == 1 { (y - 1, 12) } else { (y, m - 1) }
}

fn month_prefix((y, m): (i64, u32)) -> String {
    format!("m:{y}{m:02}:")
}

/// Up to `LEDGER_BATCH_MAX` rows from `offset`, and the offset after them; equal offsets mean the end.
fn read_spool_batch(path: &Path, offset: u64) -> std::io::Result<(Vec<BillingRecord>, u64)> {
    let mut reader = BufReader::new(File::open(path)?);
    reader.seek(SeekFrom::Start(offset))?;
    let (mut rows, mut next, mut line) = (Vec::new(), offset, Vec::new());
    while rows.len() < LEDGER_BATCH_MAX {
        line.clear();
        let n = reader.read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        next += n as u64;
        if line.trim_ascii().is_empty() {
            continue;
        }
        match serde_json::from_slice(&line) {
            Ok(row) => rows.push(row),
            Err(e) => tracing::error!(error = %e, "skipping a torn billing spool line"),
        }
    }
    Ok((rows, next))
}

fn remove_if_present(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

async fn run_blocking<T, F>(f: F) -> std::io::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> std::io::Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .unwrap_or_else(|e| Err(std::io::Error::other(e)))
}

#[cfg(test)]
mod tests {
    use gw_config::access_key_id;

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

    #[test]
    fn a_typed_family_never_degrades_to_a_model_of_another_protocol() {
        let cfg = GatewayConfig::from_yaml(
            "listen: {host: h, port: 1}\nmodels: [{name: chat, protocol: openai-chat}, {name: other-chat, protocol: anthropic-messages}, {name: emb, protocol: embeddings}]\ntenants: [{name: t, models: [chat, other-chat, emb], fallback_model: chat}]",
        )
        .unwrap();
        let mut family = gw_models::ModelParamV2::with_name(gw_consts::Protocol::Embeddings, "emb");
        family.typed = Some(gw_models::TypedParams::Embeddings(Default::default()));
        assert!(matches!(
            swap_to_fallback(&cfg, "t", &mut family),
            FallbackSwap::Unconfigured
        ));
        assert_eq!(family.model_name, "emb");
        let mut native = gw_models::ModelParamV2::with_name(gw_consts::Protocol::Responses, "emb");
        assert!(matches!(
            swap_to_fallback(&cfg, "t", &mut native),
            FallbackSwap::Unconfigured
        ));
        let mut chat = gw_models::ModelParamV2::with_name(
            gw_consts::Protocol::AnthropicMessages,
            "other-chat",
        );
        chat.typed = Some(gw_models::TypedParams::Chat(Default::default()));
        assert!(matches!(
            swap_to_fallback(&cfg, "t", &mut chat),
            FallbackSwap::Swapped(..)
        ));
    }

    #[tokio::test]
    async fn billing_ledger_repairs_failed_writes() {
        let store = Arc::new(crate::MemoryStore::default());
        store.fail_next_ledger_writes(2);
        let ledger = BillingLedger::repairing(store.clone(), "");
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
            ..BillingLedger::repairing(store.clone(), "")
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
            ..BillingLedger::repairing(store.clone(), "")
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
        let ledger = BillingLedger::repairing(store, "");
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
        let ledger = BillingLedger::repairing(store.clone(), "");
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
        let ledger = BillingLedger::repairing(store.clone(), "");
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
        let ledger = BillingLedger::repairing(store.clone(), "");

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

    #[tokio::test]
    async fn rows_the_store_keeps_refusing_are_spooled_not_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ledger-spool.jsonl");
        let store = Arc::new(crate::MemoryStore::default());
        store.fail_next_ledger_writes(usize::MAX);
        let ledger = BillingLedger::repairing(store.clone(), path.to_str().unwrap());

        ledger.write(record("req-spooled")).await;

        assert_eq!(
            ledger.flush().await,
            0,
            "a spooled row is not a dropped row"
        );
        let spooled = std::fs::read_to_string(&path).unwrap();
        assert_eq!(spooled.lines().filter(|l| !l.is_empty()).count(), 1);
        assert!(spooled.contains("req-spooled"));
    }

    #[tokio::test]
    async fn spool_replays_into_the_store_once_it_accepts_and_retries_otherwise() {
        let dir = tempfile::tempdir().unwrap();
        let spool = LedgerSpool::new(dir.path().join("ledger-spool.jsonl").to_str().unwrap());
        let store = Arc::new(crate::MemoryStore::default());
        let as_store: Arc<dyn Store> = store.clone();
        spool
            .append(&[record("req-a"), record("req-b")])
            .await
            .unwrap();

        store.fail_next_ledger_writes(1);
        spool.replay(&as_store).await;
        assert_eq!(store.ledger_snapshot(usize::MAX).await.unwrap().0, 0);
        assert!(spool.replaying.exists(), "a refused replay keeps its rows");
        spool.append(&[record("req-c")]).await.unwrap();

        spool.replay(&as_store).await;
        spool.replay(&as_store).await;
        let (count, rows) = store.ledger_snapshot(usize::MAX).await.unwrap();
        let mut ids: Vec<_> = rows.iter().map(|r| r.request_id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!((count, ids), (3, vec!["req-a", "req-b", "req-c"]));
        assert!(!spool.replaying.exists() && !spool.path.exists());
    }

    #[tokio::test]
    async fn a_torn_spool_row_never_swallows_the_next_one() {
        let dir = tempfile::tempdir().unwrap();
        let spool = LedgerSpool::new(dir.path().join("ledger-spool.jsonl").to_str().unwrap());
        std::fs::write(&spool.path, b"{\"ak\":\"torn").unwrap();
        spool.append(&[record("req-after-crash")]).await.unwrap();
        let store = Arc::new(crate::MemoryStore::default());
        let as_store: Arc<dyn Store> = store.clone();

        spool.replay(&as_store).await;

        let (count, rows) = store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(count, 1);
        assert_eq!(rows[0].request_id, "req-after-crash");
    }

    #[tokio::test]
    async fn an_unwritable_spool_keeps_retrying_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("not-a-dir");
        std::fs::write(&blocker, b"").unwrap();
        let store = Arc::new(crate::MemoryStore::default());
        store.fail_next_ledger_writes(LEDGER_SPOOL_AFTER_ATTEMPTS + 1);
        let path = blocker.join("ledger-spool.jsonl");
        let ledger = BillingLedger::repairing(store.clone(), path.to_str().unwrap());

        ledger.write(record("req-unspooled")).await;

        assert_eq!(ledger.flush().await, 0);
        let (count, rows) = store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!((count, rows[0].request_id.as_str()), (1, "req-unspooled"));
    }

    #[tokio::test]
    async fn a_spool_dir_that_does_not_exist_yet_is_created() {
        let dir = tempfile::tempdir().unwrap();
        let spool = LedgerSpool::new(dir.path().join("gw/ledger/spool.jsonl").to_str().unwrap());
        spool.append(&[record("req-nested")]).await.unwrap();
        assert!(
            std::fs::read_to_string(&spool.path)
                .unwrap()
                .contains("req-nested")
        );
    }

    #[tokio::test]
    async fn replay_streams_batches_and_rolls_up_rows_billed_before_the_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let spool = LedgerSpool::new(dir.path().join("ledger-spool.jsonl").to_str().unwrap());
        let store = Arc::new(crate::MemoryStore::default());
        let as_store: Arc<dyn Store> = store.clone();
        let now = crate::epoch_secs();
        let mut late = record("req-current");
        late.created_at_epoch_secs = now - 600;
        store.ledger_add(&late).await.unwrap();
        store.usage_rollup_advance(now).await.unwrap();
        let billed_at = now - 7_200;
        let rows: Vec<_> = (0..=LEDGER_BATCH_MAX)
            .map(|i| BillingRecord {
                created_at_epoch_secs: billed_at,
                ..record(format!("req-late-{i}"))
            })
            .collect();
        spool.append(&rows).await.unwrap();

        spool.replay(&as_store).await;

        assert_eq!(
            store.ledger_snapshot(usize::MAX).await.unwrap().0,
            LEDGER_BATCH_MAX + 2
        );
        let usage = store
            .usage_by_user(None, None, billed_at, billed_at + 60)
            .await
            .unwrap();
        assert_eq!(
            usage.iter().map(|u| u.requests).sum::<i64>(),
            LEDGER_BATCH_MAX as i64 + 1,
            "{usage:?}"
        );
        assert!(!spool.replaying.exists());
    }

    #[tokio::test]
    async fn replay_resumes_after_the_last_saved_batch() {
        let dir = tempfile::tempdir().unwrap();
        let spool = LedgerSpool::new(dir.path().join("ledger-spool.jsonl").to_str().unwrap());
        let mut lines = Vec::new();
        for id in ["req-committed", "req-pending"] {
            serde_json::to_writer(&mut lines, &record(id)).unwrap();
            lines.push(b'\n');
        }
        let committed = lines.iter().position(|&b| b == b'\n').unwrap() + 1;
        std::fs::write(&spool.replaying, &lines).unwrap();
        std::fs::write(&spool.replayed, committed.to_string()).unwrap();
        let store = Arc::new(crate::MemoryStore::default());
        let as_store: Arc<dyn Store> = store.clone();

        spool.replay(&as_store).await;

        let (count, rows) = store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!((count, rows[0].request_id.as_str()), (1, "req-pending"));
        assert!(!spool.replaying.exists() && !spool.replayed.exists());
    }

    #[tokio::test]
    async fn a_spool_row_from_an_older_release_still_replays() {
        let dir = tempfile::tempdir().unwrap();
        let spool = LedgerSpool::new(dir.path().join("ledger-spool.jsonl").to_str().unwrap());
        std::fs::write(
            &spool.path,
            br#"{"ak":"ak","tenant":"default","request_id":"req-old","created_at_epoch_secs":1,"model":"m","total_tokens":2}"#,
        )
        .unwrap();
        let store = Arc::new(crate::MemoryStore::default());
        let as_store: Arc<dyn Store> = store.clone();

        spool.replay(&as_store).await;

        let (count, rows) = store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!((count, rows[0].request_id.as_str()), (1, "req-old"));
        assert_eq!(rows[0].total_tokens, 2);
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

    async fn daily_fixture() -> (Arc<GatewayConfig>, Arc<GatewayState>, Arc<AkInfo>) {
        let yaml = "listen: {host: h, port: 1}\ntenants: [{name: t1, key_daily_cost_quota_micros: 100}]\naccess_keys: [{ak: k1, tenant: t1, product: p, qps: 1, daily_token_quota: 1000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let ak = state.auth.get(&access_key_id("k1")).await.unwrap();
        (cfg, state, ak)
    }

    async fn monthly_fixture(
        rollover: bool,
    ) -> (Arc<GatewayConfig>, Arc<GatewayState>, Arc<AkInfo>) {
        let yaml = format!(
            "listen: {{host: h, port: 1}}\ntenants: [{{name: t1, key_monthly_cost_quota_micros: 3, monthly_cost_rollover: {rollover}}}]\naccess_keys: [{{ak: k1, tenant: t1, product: p, qps: 1, daily_token_quota: 1}}]"
        );
        let cfg = Arc::new(GatewayConfig::from_yaml(&yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let ak = state.auth.get(&access_key_id("k1")).await.unwrap();
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

        let untouched = budgets(gov, &cfg, &ak, "", UserBudget::default()).await;
        assert_eq!(untouched.len(), 1);
        assert!(untouched[0].window == Window::Month);
        assert_eq!(
            untouched[0].limit, 6,
            "an unspent month carries its whole cap"
        );

        gov.counter_add(&prev, 1, MONTH_COUNTER_TTL).await;
        assert_eq!(
            budgets(gov, &cfg, &ak, "", UserBudget::default()).await[0].limit,
            5
        );
        gov.counter_add(&prev, 10, MONTH_COUNTER_TTL).await;
        assert_eq!(
            budgets(gov, &cfg, &ak, "", UserBudget::default()).await[0].limit,
            3,
            "an overspent month carries nothing"
        );

        let (cfg, state, ak) = monthly_fixture(false).await;
        let gov = state.governance.as_ref();
        assert_eq!(
            budgets(gov, &cfg, &ak, "", UserBudget::default()).await[0].limit,
            3
        );
    }

    #[tokio::test]
    async fn daily_budgets_hold_in_flight_calls_until_they_settle() {
        let (cfg, state, ak) = daily_fixture().await;
        let gov = state.governance.as_ref();
        let key = format!("cb:ak:{}", ak.ak_id);
        let est = BudgetSpend {
            tokens: 1,
            cost_micros: 60,
        };
        let now = crate::epoch_secs();
        let reserve = || reserve_budgets(&state, &cfg, &ak, "", || est, now);

        let (_, first) = reserve().await.unwrap().unwrap();
        let (_, second) = reserve().await.unwrap().unwrap();
        assert_eq!(gov.quota_used(&key).await, 120, "both calls are in flight");
        let denied = reserve().await.unwrap().unwrap_err();
        assert!(
            denied.starts_with("daily cost budget exhausted for key:"),
            "{denied}"
        );
        assert_eq!(gov.quota_used(&key).await, 120, "a denial holds nothing");

        let spent = BudgetSpend {
            tokens: 1,
            cost_micros: 10,
        };
        consume_budgets(&state, &cfg, &ak, "", None, first, spent).await;
        assert_eq!(gov.quota_used(&key).await, 70, "the settle charges actuals");
        second.refund(gov).await;
        assert_eq!(gov.quota_used(&key).await, 10, "a refund returns the hold");
        let (_, third) = reserve().await.unwrap().unwrap();
        consume_budgets(&state, &cfg, &ak, "", None, third, BudgetSpend::default()).await;
        assert_eq!(
            gov.quota_used(&key).await,
            10,
            "a zero-cost settle refunds the hold"
        );
    }

    #[tokio::test]
    async fn a_settle_that_matches_its_hold_still_alerts_at_the_cap() {
        let (cfg, state, ak) = daily_fixture().await;
        let mut alerts = state.alerts.take_receiver().unwrap();
        let spend = BudgetSpend {
            tokens: 1,
            cost_micros: 100,
        };
        let (_, hold) = reserve_budgets(&state, &cfg, &ak, "", || spend, crate::epoch_secs())
            .await
            .unwrap()
            .unwrap();

        consume_budgets(&state, &cfg, &ak, "", None, hold, spend).await;

        let alert = alerts.try_recv().expect("budget_exhausted alert");
        assert_eq!(alert.kind, "budget_exhausted");
    }

    #[tokio::test]
    async fn without_a_day_budget_admission_never_prices_the_estimate() {
        let (cfg, state, ak) = monthly_fixture(false).await;
        reserve_budgets(&state, &cfg, &ak, "", || unreachable!("no day budget"), 0)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn monthly_budget_denies_at_the_cap_without_touching_daily_counters() {
        let (cfg, state, ak) = monthly_fixture(false).await;
        let gov = state.governance.as_ref();
        reserve_budgets(&state, &cfg, &ak, "", BudgetSpend::default, 0)
            .await
            .unwrap()
            .unwrap();
        consume_budgets(
            &state,
            &cfg,
            &ak,
            "",
            None,
            BudgetHold::default(),
            BudgetSpend {
                tokens: 10,
                cost_micros: 3,
            },
        )
        .await;
        let err = reserve_budgets(&state, &cfg, &ak, "", BudgetSpend::default, 0)
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            err.starts_with("monthly cost budget exhausted for key:"),
            "{err}"
        );
        assert_eq!(gov.quota_used(&format!("cb:ak:{}", ak.ak_id)).await, 0);
        gov.quota_reset_all().await;
        assert!(
            reserve_budgets(&state, &cfg, &ak, "", BudgetSpend::default, 0)
                .await
                .unwrap()
                .is_err(),
            "the daily reset leaves month counters alone"
        );
    }
}
