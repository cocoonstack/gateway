//! Durable gateway records: billing ledger, uploaded files, batch jobs.
//!
//! Three backends behind the [`Store`] trait: [`MemoryStore`] (the default),
//! [`SqliteStore`] (`storage.sqlite_path`, one durable node), and
//! [`PostgresStore`] (`storage.postgres_url`, shared across a fleet).

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use gw_models::GResult;
use sqlx::Row;

/// Consuming the sequence explicitly keeps id and n on one atomic value —
/// concurrent PG writers would race a MAX(n)+1 subselect.
const PG_INSERT_BATCH: &str = "INSERT INTO batches (n, id, ak, tenant, model, status, total)
     SELECT v, 'batch-' || v, $1, $2, $3, $4, $5
     FROM nextval(pg_get_serial_sequence('batches', 'n')) AS v
     RETURNING id";

/// Per-call token ceiling: usage is floored at 0 upstream but not capped, so
/// clamping keeps a hostile count from overflowing downstream accumulators.
/// Far above any real response, so real traffic is never clamped.
pub const MAX_METERED_TOKENS: i64 = 1_000_000_000;

/// Prune the SQL ledger every Nth insert instead of per write (the cap becomes
/// approximate by at most this many rows, saving a round-trip per billing).
/// Pruning spares rows the rollup hasn't folded yet (created at or past the
/// watermark), so a burst can exceed `ledger_max_rows` briefly rather than
/// lose usage — billing integrity outranks a strict cap.
const LEDGER_PRUNE_EVERY: usize = 64;

/// Usage-rollup bucket width.
const ROLLUP_BUCKET_SECS: i64 = 60;

/// Each rollup advance recomputes at least this trailing window (more when the
/// watermark trails it — first run rolls the whole ledger, a stalled task
/// catches up whole), so late rows and missed ticks self-heal.
const ROLLUP_BACKFILL_SECS: i64 = 20 * 60;

/// A minute is rolled only once it has been closed for at least this long, so
/// an in-flight billing write (or a replica whose clock trails by less than a
/// minute) can never land a row in an already-rolled minute — which is what
/// makes the max-upsert sound: a rolled minute's source set can only shrink
/// (pruning), never grow.
const ROLLUP_SETTLE_SECS: i64 = ROLLUP_BUCKET_SECS;

/// Postgres advisory-lock key serializing the fleet's rollup: one replica
/// advances per tick, the rest skip (the upsert is idempotent either way —
/// the lock only avoids N replicas repeating the same scan).
const ROLLUP_LOCK_KEY: i64 = 0x6777_726f_6c6c;

/// One minute past the newest rolled bucket — where the raw ledger tail
/// starts. Valid in both SQL dialects.
const ROLLUP_WATERMARK_SQL: &str = "SELECT COALESCE(MAX(minute_epoch), -60) + 60 FROM usage_rollup";

/// One billing entry (recorded locally only; no reporting upstream).
#[derive(Debug, Clone, serde::Serialize)]
pub struct BillingRecord {
    pub ak: String,
    pub product: String,
    pub tenant: String,
    /// Effective end user: the key's `owner` if set, else request metadata; empty
    /// when neither is present. The precise per-user billing dimension.
    #[serde(default)]
    pub user_id: String,
    /// Ingress correlation id, joins this row to the access log and audit events.
    #[serde(default)]
    pub request_id: String,
    /// Unix seconds the call was billed — the billing-period axis.
    #[serde(default)]
    pub created_at_epoch_secs: i64,
    /// Public model the caller requested.
    pub model: String,
    /// Model that actually served (differs from `model` after a quota fallback).
    pub served_model: String,
    pub protocol: String,
    pub account: String,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    /// Weighted platform total (what quota metering consumed); equals
    /// prompt + completion only when the served model has no `token_rate`.
    pub total_tokens: i64,
    pub cost_micros: i64,
    /// What the serving account's vendor charged us (zero = untracked).
    #[serde(default)]
    pub vendor_cost_micros: i64,
    /// PTU spilled over to a paygo account (a failover occurred).
    #[serde(default)]
    pub ptu_spillover: bool,
    /// Token counts were estimated (an aborted stream billed from delivered
    /// text), not read from a vendor usage payload.
    #[serde(default)]
    pub estimated: bool,
}

/// Offline batch job status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl BatchStatus {
    fn as_str(self) -> &'static str {
        match self {
            BatchStatus::Pending => "pending",
            BatchStatus::Running => "running",
            BatchStatus::Completed => "completed",
            BatchStatus::Failed => "failed",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "pending" => BatchStatus::Pending,
            "running" => BatchStatus::Running,
            "completed" => BatchStatus::Completed,
            "failed" => BatchStatus::Failed,
            _ => return None,
        })
    }
}

/// One item's result inside a batch.
#[derive(Debug, Clone, serde::Serialize)]
pub struct BatchItemResult {
    pub index: usize,
    pub ok: bool,
    pub message: String,
    pub total_tokens: i64,
    /// Effective end user the item billed to; ties the generated `message`
    /// to an owner so user erasure can reach it.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub user: String,
}

/// One offline batch job.
#[derive(Debug, Clone, serde::Serialize)]
pub struct BatchJob {
    pub id: String,
    /// Owning key — a bearer credential, so it's never serialized into a response.
    #[serde(skip)]
    pub ak: String,
    /// Owning tenant; reads are gated on it. Internal routing, not client-facing.
    #[serde(skip)]
    pub tenant: String,
    pub model: String,
    pub status: BatchStatus,
    pub total: usize,
    pub results: Vec<BatchItemResult>,
}

/// A stored file (batch input JSONL, etc.).
#[derive(Debug, Clone, serde::Serialize)]
pub struct StoredFile {
    pub id: String,
    /// Owning tenant; reads are gated on it.
    pub tenant: String,
    pub bytes: usize,
    pub purpose: String,
    /// raw content (not serialized in metadata views; fetched via /content).
    #[serde(skip)]
    pub content: String,
}

/// Identity + token counts for one billed call, priced into a [`BillingRecord`].
pub struct BillingInput<'a> {
    pub ak: &'a str,
    pub product: &'a str,
    pub tenant: &'a str,
    /// Effective end user (key owner else request metadata); empty when neither.
    pub user_id: &'a str,
    /// Ingress correlation id for this request.
    pub request_id: &'a str,
    /// Public model the caller requested (accrues the per-(AK, model) counter).
    pub requested_model: &'a str,
    /// Model that actually served — charged at its price (may differ on fallback).
    pub served_model: &'a str,
    pub protocol: &'a str,
    pub account: &'a str,
    pub prompt: i64,
    pub completion: i64,
    /// Weighted billable sides (the served model's token_rate applied); equal
    /// to `prompt`/`completion` when the model has no rate configured.
    pub billable_prompt: i64,
    pub billable_completion: i64,
    pub total: i64,
    pub ptu_spillover: bool,
    /// Counts are estimated (aborted stream), not vendor-reported.
    pub estimated: bool,
}

/// Clamp a metered token count into `[0, MAX_METERED_TOKENS]`.
pub fn clamp_tokens(n: i64) -> i64 {
    n.clamp(0, MAX_METERED_TOKENS)
}

/// The served model's billing weights; identity when unconfigured. The
/// `prompt_includes_cache` normalization already happened at usage extraction.
pub fn model_token_rate(cfg: &gw_config::GatewayConfig, model: &str) -> gw_models::TokenRate {
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

/// Price one call into a [`BillingRecord`]: the tenant's price for the served
/// model, vendor cost from the serving account. Shared by the request pipeline
/// and the realtime surface so the two can't drift; token counts are clamped.
/// Cost multiplies the weighted billable sides; the prompt/completion columns
/// keep the vendor-reported counts, while `total_tokens` is the weighted
/// platform total that quota metering consumed.
pub fn billing_record(cfg: &gw_config::GatewayConfig, b: &BillingInput) -> BillingRecord {
    let (prompt, completion, total) = (
        clamp_tokens(b.prompt),
        clamp_tokens(b.completion),
        clamp_tokens(b.total),
    );
    let (billable_prompt, billable_completion) = (
        clamp_tokens(b.billable_prompt),
        clamp_tokens(b.billable_completion),
    );
    let charged = cfg.prices_for_tenant(b.tenant, b.served_model);
    let vendor = cfg
        .accounts
        .iter()
        .find(|a| a.name == b.account)
        .map(|a| {
            (
                a.cost_input_price_per_1k_micros,
                a.cost_output_price_per_1k_micros,
            )
        })
        .unwrap_or((0, 0));
    BillingRecord {
        ak: b.ak.to_owned(),
        product: b.product.to_owned(),
        tenant: b.tenant.to_owned(),
        user_id: b.user_id.to_owned(),
        request_id: b.request_id.to_owned(),
        created_at_epoch_secs: crate::epoch_secs(),
        model: b.requested_model.to_owned(),
        served_model: b.served_model.to_owned(),
        protocol: b.protocol.to_owned(),
        account: b.account.to_owned(),
        prompt_tokens: prompt,
        completion_tokens: completion,
        total_tokens: total,
        cost_micros: gw_models::cost_micros(billable_prompt, billable_completion, charged),
        vendor_cost_micros: gw_models::cost_micros(billable_prompt, billable_completion, vendor),
        ptu_spillover: b.ptu_spillover,
        estimated: b.estimated,
    }
}

/// One row of the per-(tenant, model) usage rollup.
#[derive(Debug, Clone, serde::Serialize)]
pub struct UsageRow {
    pub tenant: String,
    pub model: String,
    pub requests: i64,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
    pub cost_micros: i64,
    pub vendor_cost_micros: i64,
}

/// One row of the per-(user, model) usage rollup over a billing period.
#[derive(Debug, Clone, serde::Serialize)]
pub struct UserUsageRow {
    pub user_id: String,
    pub model: String,
    pub requests: i64,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
    pub cost_micros: i64,
    pub vendor_cost_micros: i64,
}

impl UserUsageRow {
    pub fn zero(user_id: String, model: String) -> Self {
        Self {
            user_id,
            model,
            requests: 0,
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
            cost_micros: 0,
            vendor_cost_micros: 0,
        }
    }

    /// Fold `o`'s counters into self (saturating).
    pub fn absorb(&mut self, o: &UserUsageRow) {
        self.requests = self.requests.saturating_add(o.requests);
        self.prompt_tokens = self.prompt_tokens.saturating_add(o.prompt_tokens);
        self.completion_tokens = self.completion_tokens.saturating_add(o.completion_tokens);
        self.total_tokens = self.total_tokens.saturating_add(o.total_tokens);
        self.cost_micros = self.cost_micros.saturating_add(o.cost_micros);
        self.vendor_cost_micros = self.vendor_cost_micros.saturating_add(o.vendor_cost_micros);
    }

    /// Fold one raw ledger row's counters into self (saturating).
    fn add_record(&mut self, r: &BillingRecord) {
        self.requests = self.requests.saturating_add(1);
        self.prompt_tokens = self.prompt_tokens.saturating_add(r.prompt_tokens);
        self.completion_tokens = self.completion_tokens.saturating_add(r.completion_tokens);
        self.total_tokens = self.total_tokens.saturating_add(r.total_tokens);
        self.cost_micros = self.cost_micros.saturating_add(r.cost_micros);
        self.vendor_cost_micros = self.vendor_cost_micros.saturating_add(r.vendor_cost_micros);
    }

    /// Keep the larger of each counter. A bucket's source rows are append-only
    /// and only ever shrink via ledger pruning, so per-column max always picks
    /// the more complete snapshot — a recompute from a partially pruned ledger
    /// can never shrink a bucket.
    fn keep_max(&mut self, o: &UserUsageRow) {
        self.requests = self.requests.max(o.requests);
        self.prompt_tokens = self.prompt_tokens.max(o.prompt_tokens);
        self.completion_tokens = self.completion_tokens.max(o.completion_tokens);
        self.total_tokens = self.total_tokens.max(o.total_tokens);
        self.cost_micros = self.cost_micros.max(o.cost_micros);
        self.vendor_cost_micros = self.vendor_cost_micros.max(o.vendor_cost_micros);
    }
}

/// One raw ledger row as a single-request usage line.
fn usage_of(r: &BillingRecord) -> UserUsageRow {
    UserUsageRow {
        user_id: r.user_id.clone(),
        model: r.model.clone(),
        requests: 1,
        prompt_tokens: r.prompt_tokens,
        completion_tokens: r.completion_tokens,
        total_tokens: r.total_tokens,
        cost_micros: r.cost_micros,
        vendor_cost_micros: r.vendor_cost_micros,
    }
}

/// Fold grouped usage rows into `map` by (user, model).
fn fold_user_usage(
    map: &mut std::collections::BTreeMap<(String, String), UserUsageRow>,
    rows: impl IntoIterator<Item = UserUsageRow>,
) {
    for r in rows {
        map.entry((r.user_id.clone(), r.model.clone()))
            .and_modify(|e| e.absorb(&r))
            .or_insert(r);
    }
}

fn fold_series(
    map: &mut std::collections::BTreeMap<i64, UserUsageRow>,
    rows: impl IntoIterator<Item = (i64, UserUsageRow)>,
) {
    for (start, r) in rows {
        map.entry(start).and_modify(|e| e.absorb(&r)).or_insert(r);
    }
}

fn series_row<'r, R>(row: &'r R) -> (i64, UserUsageRow)
where
    R: sqlx::Row,
    usize: sqlx::ColumnIndex<R>,
    i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    (
        row.get(0),
        UserUsageRow {
            user_id: String::new(),
            model: String::new(),
            requests: row.get(1),
            prompt_tokens: row.get(2),
            completion_tokens: row.get(3),
            total_tokens: row.get(4),
            cost_micros: row.get(5),
            vendor_cost_micros: row.get(6),
        },
    )
}

fn bucket_floor(ts: i64) -> i64 {
    ts - ts.rem_euclid(ROLLUP_BUCKET_SECS)
}

/// Minute-align a query window: identical semantics whether a minute is served
/// from its bucket or still raw, so a repeated query can't drift as the
/// watermark advances past its bounds.
fn align_bounds(since: i64, until: i64) -> (i64, i64) {
    (
        bucket_floor(since),
        bucket_floor(until).saturating_add(ROLLUP_BUCKET_SECS - 1),
    )
}

/// A content-safety outcome, recorded WITHOUT the offending text — only which
/// key/user/rule fired and what the gateway did, so hits are queryable per
/// ak/tenant without retaining prompt content.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SecurityEvent {
    pub created_at_epoch_secs: i64,
    pub request_id: String,
    pub ak: String,
    pub user_id: String,
    pub tenant: String,
    /// Which surface: chat/messages/responses/realtime/…
    pub surface: String,
    /// The rule family that fired: "blocklist" | "dlp" | a recognizer name.
    pub rule: String,
    /// What the gateway did: "block" | "redact" | "flag".
    pub action: String,
    pub hits: i64,
}

impl SecurityEvent {
    /// Append to `store`. Best-effort: a write failure is logged, never fails
    /// the request being audited.
    pub async fn record(&self, store: &dyn Store) {
        if let Err(e) = store.security_event_add(self).await {
            tracing::warn!(error = %e, rule = %self.rule, "security event write failed");
        }
    }
}

/// One admin-plane mutation, recorded with who/what/when for compliance.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AdminAudit {
    pub created_at_epoch_secs: i64,
    /// The admin identity: "global" or the tenant name whose token was used.
    pub actor: String,
    /// The presented scope: "global" | "tenant".
    pub scope: String,
    /// The mutation: "key_create" | "key_patch" | "key_delete" |
    /// "config_publish" | "reload" | "content_erase".
    pub action: String,
    /// The object acted on (an ak, a config version, …).
    pub target: String,
    pub summary: String,
    pub source_ip: String,
}

#[async_trait::async_trait]
pub trait Store: Send + Sync + std::fmt::Debug {
    async fn ledger_add(&self, r: &BillingRecord) -> GResult<()>;
    /// Total count plus the most recent `limit` records in chronological order;
    /// the ledger is append-only, so count/page skew is at most one fresh record.
    async fn ledger_snapshot(&self, limit: usize) -> GResult<(usize, Vec<BillingRecord>)>;
    /// Usage rolled up by (tenant, requested model), sorted.
    async fn ledger_usage(&self, tenant: Option<&str>) -> GResult<Vec<UsageRow>>;
    /// Per-user cost over `[since, until]` (unix secs), grouped by (user,
    /// requested model); optional tenant/user filter. The billing-period query
    /// behind per-user invoicing. Served from the minute rollup for rolled
    /// minutes (where the bounds are minute-aligned) plus the raw ledger tail,
    /// so the result survives `ledger_max_rows` pruning.
    async fn usage_by_user(
        &self,
        tenant: Option<&str>,
        user: Option<&str>,
        since: i64,
        until: i64,
    ) -> GResult<Vec<UserUsageRow>>;
    /// Bucketed usage totals over `[since, until]`: one `(bucket_start,
    /// totals)` per `bucket_secs` bucket with traffic, ascending. Same
    /// rollup-plus-tail sourcing and filters as [`Store::usage_by_user`];
    /// the `user_id`/`model` fields of the totals are empty.
    async fn usage_series(
        &self,
        tenant: Option<&str>,
        user: Option<&str>,
        since: i64,
        until: i64,
        bucket_secs: i64,
    ) -> GResult<Vec<(i64, UserUsageRow)>>;
    /// Roll completed minutes of the ledger into durable `usage_rollup`
    /// buckets: every bucket in the trailing backfill window is recomputed
    /// from the raw rows and upserted — never deleted — so the periodic task
    /// is idempotent, self-heals missed ticks, and a bucket outlives the raw
    /// rows it summarizes. Returns the buckets written.
    async fn usage_rollup_advance(&self, now_epoch_secs: i64) -> GResult<u64>;

    /// Append a content-safety event (no prompt text retained).
    async fn security_event_add(&self, e: &SecurityEvent) -> GResult<()>;
    /// The most recent `limit` security events, newest first; optional tenant filter.
    async fn security_events(
        &self,
        tenant: Option<&str>,
        limit: usize,
    ) -> GResult<Vec<SecurityEvent>>;

    async fn admin_audit_add(&self, e: &AdminAudit) -> GResult<()>;
    /// The most recent `limit` admin-audit entries, newest first.
    async fn admin_audit_list(&self, limit: usize) -> GResult<Vec<AdminAudit>>;

    /// Store one retained prompt/response record (per-tenant retention policy).
    async fn content_add(&self, r: &crate::ContentRecord) -> GResult<()>;
    /// Delete content whose `expires_at_epoch_secs` is in `(0, now]`; returns the
    /// number deleted. Rows with `expires_at = 0` are kept until manual purge.
    async fn content_purge(&self, now_epoch_secs: i64) -> GResult<u64>;
    /// Erase every retained trace of `user`'s content, optionally confined to
    /// one tenant (a tenant admin's scope): retained prompt/response rows,
    /// generated batch-result messages, and still-queued batch inputs (a
    /// terminal batch's inputs are already pruned at its status write).
    /// `audit` (its `summary` set to the erased-row count here) is written in
    /// the same transaction on the SQL backends, so a recorded success can't
    /// separate from the deletion. Returns rows erased. Ledger rows and
    /// security events carry no content and are kept.
    async fn content_erase_user(
        &self,
        tenant: Option<&str>,
        user: &str,
        audit: AdminAudit,
    ) -> GResult<u64>;
    /// The retained content for one request (both prompt and response rows).
    async fn content_for(&self, request_id: &str) -> GResult<Vec<crate::ContentRecord>>;

    /// Store `content` under a fresh id owned by `tenant`; returns the metadata.
    async fn file_put(&self, tenant: &str, purpose: &str, content: String) -> GResult<StoredFile>;
    async fn file_get(&self, id: &str) -> GResult<Option<StoredFile>>;
    /// Delete `tenant`'s uploaded file in one guarded statement (no
    /// check-then-delete window); whether it existed under that tenant.
    async fn file_delete(&self, id: &str, tenant: &str) -> GResult<bool>;

    async fn batch_create(
        &self,
        ak: &str,
        tenant: &str,
        model: &str,
        total: usize,
    ) -> GResult<BatchJob>;
    async fn batch_get(&self, id: &str) -> GResult<Option<BatchJob>>;
    async fn batch_set_status(&self, id: &str, status: BatchStatus) -> GResult<()>;
    /// Record one item's result: first-writer-wins, and rejected once the batch
    /// is terminal (so a stale executor can neither overwrite nor append late).
    async fn batch_push_result(&self, id: &str, result: BatchItemResult) -> GResult<()>;
    /// Set status only if `claim` still matches the batch's fence token (see
    /// [`Store::batch_claim_pending`]); returns whether the write applied.
    /// Unfenced backends apply unconditionally and return `true`.
    async fn batch_set_status_owned(
        &self,
        id: &str,
        status: BatchStatus,
        _claim: i64,
    ) -> GResult<bool> {
        self.batch_set_status(id, status).await.map(|()| true)
    }
    /// Set a running batch's terminal status derived from the persisted results
    /// (Completed iff all items succeeded), if `claim` still owns it; `None` if
    /// not. The fenced backend serializes with result writes via the row lock.
    async fn batch_finalize(&self, id: &str, claim: i64) -> GResult<Option<BatchStatus>> {
        let Some(job) = self.batch_get(id).await? else {
            return Ok(None);
        };
        let done = if job.results.len() == job.total && job.results.iter().all(|r| r.ok) {
            BatchStatus::Completed
        } else {
            BatchStatus::Failed
        };
        Ok(self
            .batch_set_status_owned(id, done, claim)
            .await?
            .then_some(done))
    }

    /// Whether this backend runs a fleet work queue; local backends execute on
    /// the submitting instance.
    fn distributed_batches(&self) -> bool {
        false
    }
    /// Atomically enqueue a batch and its items so a partial save never leaves
    /// a claimable job with missing items; local stores fall back to a create.
    async fn batch_enqueue(
        &self,
        ak: &str,
        tenant: &str,
        model: &str,
        items: &[gw_models::BatchItem],
    ) -> GResult<BatchJob> {
        self.batch_create(ak, tenant, model, items.len()).await
    }
    /// Load a batch's input items for execution.
    async fn batch_load_items(&self, _id: &str) -> GResult<Vec<gw_models::BatchItem>> {
        Ok(Vec::new())
    }
    /// The current stored copy of one queued item, on backends that persist
    /// items (`None` otherwise). Executors re-read this immediately before
    /// dispatch, so an erasure landing while the batch sat queued stops the
    /// item instead of letting the pre-load snapshot run.
    async fn batch_item_snapshot(
        &self,
        _id: &str,
        _idx: usize,
    ) -> GResult<Option<gw_models::BatchItem>> {
        Ok(None)
    }
    /// Whether `user`'s content was erased at or after `since` (unix MILLIS —
    /// second granularity would misjudge an erase-then-resubmit in the same
    /// second). Local executors check this before dispatching an item captured
    /// before the erasure, so a long sequential batch can't keep running an
    /// erased user's prompts; a batch submitted after the erasure passes.
    /// Item-persisting backends re-read rows at dispatch instead and keep the
    /// default `false`.
    async fn user_erased_since(&self, _tenant: &str, _user: &str, _since: i64) -> GResult<bool> {
        Ok(false)
    }
    /// Claim one pending batch (requeuing stale running ones first); `None` =
    /// nothing to run. The returned fence token (>= 1, bumped per claim) rides
    /// [`Store::batch_touch`] / [`Store::batch_set_status_owned`] so a reclaimed
    /// executor detects it lost ownership; the in-process path passes 0.
    async fn batch_claim_pending(&self, _stale_secs: i64) -> GResult<Option<(BatchJob, i64)>> {
        Ok(None)
    }
    /// Heartbeat a running batch; `false` = the fence no longer matches (the
    /// batch was reclaimed) and this executor must stop.
    async fn batch_touch(&self, _id: &str, _claim: i64) -> GResult<bool> {
        Ok(true)
    }
}

/// In-process store: append-only ledger, DashMap-backed files and batches.
#[derive(Debug, Default)]
pub struct MemoryStore {
    records: Mutex<Vec<BillingRecord>>,
    /// Minute buckets keyed by (minute, tenant, user, model); see
    /// [`Store::usage_rollup_advance`].
    rollup: Mutex<std::collections::BTreeMap<(i64, String, String, String), UserUsageRow>>,
    sec_events: Mutex<Vec<SecurityEvent>>,
    audit: Mutex<Vec<AdminAudit>>,
    content: Mutex<Vec<crate::ContentRecord>>,
    /// Latest erasure instant per (tenant, user), backing
    /// [`Store::user_erased_since`] — one entry per pair, not a log.
    erasures: Mutex<std::collections::HashMap<(String, String), i64>>,
    files: DashMap<String, StoredFile>,
    jobs: DashMap<String, BatchJob>,
    seq: AtomicUsize,
    /// oldest records beyond this are pruned on write; 0 = unlimited.
    ledger_max_rows: usize,
}

impl MemoryStore {
    pub fn with_ledger_cap(max_rows: usize) -> Self {
        Self {
            ledger_max_rows: max_rows,
            ..Self::default()
        }
    }
}

#[async_trait::async_trait]
impl Store for MemoryStore {
    async fn ledger_add(&self, r: &BillingRecord) -> GResult<()> {
        // watermark first: rollup-then-records is the lock order advance uses
        let watermark = self
            .rollup
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .next_back()
            .map_or(0, |k| k.0 + ROLLUP_BUCKET_SECS);
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        records.push(r.clone());
        if self.ledger_max_rows > 0 && records.len() > self.ledger_max_rows {
            // spare rows the rollup hasn't folded yet — lost here means lost
            // from usage forever; the cap yields to billing integrity. A full
            // scan, not a prefix cut: completion order can wedge a young row
            // ahead of prunable ones, which must not defeat the cap.
            let mut excess = records.len() - self.ledger_max_rows;
            records.retain(|r| {
                if excess > 0 && r.created_at_epoch_secs < watermark {
                    excess -= 1;
                    false
                } else {
                    true
                }
            });
        }
        Ok(())
    }

    async fn ledger_snapshot(&self, limit: usize) -> GResult<(usize, Vec<BillingRecord>)> {
        let records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let total = records.len();
        let page = records[total.saturating_sub(limit)..].to_vec();
        Ok((total, page))
    }

    async fn ledger_usage(&self, tenant: Option<&str>) -> GResult<Vec<UsageRow>> {
        let records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut rollup: std::collections::BTreeMap<(String, String), UsageRow> =
            std::collections::BTreeMap::new();
        for r in records.iter() {
            if tenant.is_some_and(|t| t != r.tenant) {
                continue;
            }
            let e = rollup
                .entry((r.tenant.clone(), r.model.clone()))
                .or_insert_with(|| UsageRow {
                    tenant: r.tenant.clone(),
                    model: r.model.clone(),
                    requests: 0,
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                    cost_micros: 0,
                    vendor_cost_micros: 0,
                });
            // saturating: a hostile upstream can drive a single record's counts
            // to i64::MAX (usage is floored, not capped), so the rollup sum must
            // not overflow across records
            e.requests += 1;
            e.prompt_tokens = e.prompt_tokens.saturating_add(r.prompt_tokens);
            e.completion_tokens = e.completion_tokens.saturating_add(r.completion_tokens);
            e.total_tokens = e.total_tokens.saturating_add(r.total_tokens);
            e.cost_micros = e.cost_micros.saturating_add(r.cost_micros);
            e.vendor_cost_micros = e.vendor_cost_micros.saturating_add(r.vendor_cost_micros);
        }
        Ok(rollup.into_values().collect())
    }

    async fn usage_by_user(
        &self,
        tenant: Option<&str>,
        user: Option<&str>,
        since: i64,
        until: i64,
    ) -> GResult<Vec<UserUsageRow>> {
        let (since, until) = align_bounds(since, until);
        let mut map = std::collections::BTreeMap::new();
        let watermark = {
            let rollup = self
                .rollup
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for ((minute, t, u, _), row) in rollup.iter() {
                if *minute >= bucket_floor(since)
                    && *minute <= bucket_floor(until)
                    && tenant.is_none_or(|f| f == t)
                    && user.is_none_or(|f| f == u)
                {
                    map.entry((row.user_id.clone(), row.model.clone()))
                        .and_modify(|e: &mut UserUsageRow| e.absorb(row))
                        .or_insert_with(|| row.clone());
                }
            }
            rollup
                .keys()
                .next_back()
                .map_or(0, |k| k.0 + ROLLUP_BUCKET_SECS)
        };
        let records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let tail = records.iter().filter(|r| {
            r.created_at_epoch_secs >= since.max(watermark)
                && r.created_at_epoch_secs <= until
                && tenant.is_none_or(|t| t == r.tenant)
                && user.is_none_or(|u| u == r.user_id)
        });
        fold_user_usage(&mut map, tail.map(usage_of));
        Ok(map.into_values().collect())
    }

    async fn usage_series(
        &self,
        tenant: Option<&str>,
        user: Option<&str>,
        since: i64,
        until: i64,
        bucket_secs: i64,
    ) -> GResult<Vec<(i64, UserUsageRow)>> {
        let bucket = |t: i64| t - t.rem_euclid(bucket_secs);
        let (since, until) = align_bounds(since, until);
        let mut map: std::collections::BTreeMap<i64, UserUsageRow> =
            std::collections::BTreeMap::new();
        let watermark = {
            let rollup = self
                .rollup
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for ((minute, t, u, _), row) in rollup.iter() {
                if *minute >= bucket_floor(since)
                    && *minute <= bucket_floor(until)
                    && tenant.is_none_or(|f| f == t)
                    && user.is_none_or(|f| f == u)
                {
                    map.entry(bucket(*minute))
                        .or_insert_with(|| UserUsageRow::zero(String::new(), String::new()))
                        .absorb(row);
                }
            }
            rollup
                .keys()
                .next_back()
                .map_or(0, |k| k.0 + ROLLUP_BUCKET_SECS)
        };
        let records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for r in records.iter().filter(|r| {
            r.created_at_epoch_secs >= since.max(watermark)
                && r.created_at_epoch_secs <= until
                && tenant.is_none_or(|t| t == r.tenant)
                && user.is_none_or(|u| u == r.user_id)
        }) {
            map.entry(bucket(r.created_at_epoch_secs))
                .or_insert_with(|| UserUsageRow::zero(String::new(), String::new()))
                .add_record(r);
        }
        Ok(map.into_iter().collect())
    }

    async fn usage_rollup_advance(&self, now: i64) -> GResult<u64> {
        let hi = bucket_floor(now - ROLLUP_SETTLE_SECS);
        let mut rollup = self
            .rollup
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let watermark = rollup
            .keys()
            .next_back()
            .map_or(0, |k| k.0 + ROLLUP_BUCKET_SECS);
        let lo = (hi - ROLLUP_BACKFILL_SECS).min(watermark);
        let mut fresh = std::collections::BTreeMap::new();
        {
            let records = self
                .records
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for r in records
                .iter()
                .filter(|r| r.created_at_epoch_secs >= lo && r.created_at_epoch_secs < hi)
            {
                fresh
                    .entry((
                        bucket_floor(r.created_at_epoch_secs),
                        r.tenant.clone(),
                        r.user_id.clone(),
                        r.model.clone(),
                    ))
                    .or_insert_with(|| UserUsageRow::zero(r.user_id.clone(), r.model.clone()))
                    .add_record(r);
            }
        }
        let written = fresh.len() as u64;
        for (k, v) in fresh {
            rollup.entry(k).and_modify(|e| e.keep_max(&v)).or_insert(v);
        }
        Ok(written)
    }

    async fn security_event_add(&self, e: &SecurityEvent) -> GResult<()> {
        self.sec_events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(e.clone());
        Ok(())
    }

    async fn security_events(
        &self,
        tenant: Option<&str>,
        limit: usize,
    ) -> GResult<Vec<SecurityEvent>> {
        let events = self
            .sec_events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(events
            .iter()
            .rev()
            .filter(|e| tenant.is_none_or(|t| t == e.tenant))
            .take(limit)
            .cloned()
            .collect())
    }

    async fn admin_audit_add(&self, e: &AdminAudit) -> GResult<()> {
        self.audit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(e.clone());
        Ok(())
    }

    async fn admin_audit_list(&self, limit: usize) -> GResult<Vec<AdminAudit>> {
        let audit = self
            .audit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(audit.iter().rev().take(limit).cloned().collect())
    }

    async fn content_add(&self, r: &crate::ContentRecord) -> GResult<()> {
        self.content
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(r.clone());
        Ok(())
    }

    async fn content_purge(&self, now: i64) -> GResult<u64> {
        let mut content = self
            .content
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let before = content.len();
        content.retain(|r| r.expires_at_epoch_secs == 0 || r.expires_at_epoch_secs > now);
        Ok((before - content.len()) as u64)
    }

    async fn content_erase_user(
        &self,
        tenant: Option<&str>,
        user: &str,
        mut audit: AdminAudit,
    ) -> GResult<u64> {
        let mut erased = {
            let mut content = self
                .content
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let before = content.len();
            content.retain(|r| r.user_id != user || tenant.is_some_and(|t| t != r.tenant));
            (before - content.len()) as u64
        };
        for mut job in self.jobs.iter_mut() {
            if tenant.is_some_and(|t| t != job.tenant) {
                continue;
            }
            for r in job.results.iter_mut() {
                if r.user == user && !r.message.is_empty() {
                    r.message = String::new();
                    erased += 1;
                }
            }
        }
        self.erasures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                (tenant.unwrap_or_default().to_owned(), user.to_owned()),
                crate::epoch_millis(),
            );
        audit.summary = format!("rows={erased}");
        self.audit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(audit);
        Ok(erased)
    }

    async fn user_erased_since(&self, tenant: &str, user: &str, since: i64) -> GResult<bool> {
        let erasures = self
            .erasures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let hit = |t: &str| {
            erasures
                .get(&(t.to_owned(), user.to_owned()))
                .is_some_and(|at| *at >= since)
        };
        Ok(hit("") || hit(tenant))
    }

    async fn content_for(&self, request_id: &str) -> GResult<Vec<crate::ContentRecord>> {
        let content = self
            .content
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(content
            .iter()
            .filter(|r| r.request_id == request_id)
            .cloned()
            .collect())
    }

    async fn file_put(&self, tenant: &str, purpose: &str, content: String) -> GResult<StoredFile> {
        let id = format!(
            "file-local-{}",
            self.seq.fetch_add(1, Ordering::Relaxed) + 1
        );
        let meta = StoredFile {
            id: id.clone(),
            tenant: tenant.to_owned(),
            bytes: content.len(),
            purpose: purpose.to_owned(),
            content: String::new(),
        };
        let mut stored = meta.clone();
        stored.content = content;
        self.files.insert(id, stored);
        Ok(meta)
    }

    async fn file_get(&self, id: &str) -> GResult<Option<StoredFile>> {
        Ok(self.files.get(id).map(|f| f.value().clone()))
    }

    async fn file_delete(&self, id: &str, tenant: &str) -> GResult<bool> {
        Ok(self
            .files
            .remove_if(id, |_, f| f.tenant == tenant)
            .is_some())
    }

    async fn batch_create(
        &self,
        ak: &str,
        tenant: &str,
        model: &str,
        total: usize,
    ) -> GResult<BatchJob> {
        let id = format!(
            "batch-local-{}",
            self.seq.fetch_add(1, Ordering::Relaxed) + 1
        );
        let job = BatchJob {
            id: id.clone(),
            ak: ak.to_owned(),
            tenant: tenant.to_owned(),
            model: model.to_owned(),
            status: BatchStatus::Pending,
            total,
            results: Vec::new(),
        };
        self.jobs.insert(id, job.clone());
        Ok(job)
    }

    async fn batch_get(&self, id: &str) -> GResult<Option<BatchJob>> {
        Ok(self.jobs.get(id).map(|j| j.value().clone()))
    }

    async fn batch_set_status(&self, id: &str, status: BatchStatus) -> GResult<()> {
        if let Some(mut j) = self.jobs.get_mut(id) {
            j.status = status;
        }
        Ok(())
    }

    async fn batch_push_result(&self, id: &str, result: BatchItemResult) -> GResult<()> {
        if let Some(mut j) = self.jobs.get_mut(id)
            && !matches!(j.status, BatchStatus::Completed | BatchStatus::Failed)
            && !j.results.iter().any(|r| r.index == result.index)
        {
            j.results.push(result);
        }
        Ok(())
    }
}

/// Positional row → record mappers shared by the SQL backends: fields decode
/// in the SELECT's column order. One macro so the sqlx trait-bound boilerplate
/// lives once.
macro_rules! row_mapper {
    ($name:ident -> $ty:path { $($field:ident),+ $(,)? }) => {
        fn $name<'r, R>(row: &'r R) -> $ty
        where
            R: sqlx::Row,
            usize: sqlx::ColumnIndex<R>,
            String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
            i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
            bool: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
        {
            let mut col = 0usize;
            $(let $field = row.get(next_col(&mut col));)+
            $ty { $($field),+ }
        }
    };
}

fn next_col(col: &mut usize) -> usize {
    let i = *col;
    *col += 1;
    i
}

row_mapper!(row_to_billing -> BillingRecord {
    ak, product, tenant, model, served_model, protocol, account,
    prompt_tokens, completion_tokens, total_tokens, cost_micros,
    vendor_cost_micros, ptu_spillover, user_id, request_id,
    created_at_epoch_secs, estimated,
});

row_mapper!(usage_row -> UsageRow {
    tenant, model, requests, prompt_tokens, completion_tokens, total_tokens,
    cost_micros, vendor_cost_micros,
});

row_mapper!(user_usage_row -> UserUsageRow {
    user_id, model, requests, prompt_tokens, completion_tokens, total_tokens,
    cost_micros, vendor_cost_micros,
});

row_mapper!(security_event_row -> SecurityEvent {
    created_at_epoch_secs, request_id, ak, user_id, tenant, surface, rule,
    action, hits,
});

row_mapper!(admin_audit_row -> AdminAudit {
    created_at_epoch_secs, actor, scope, action, target, summary, source_ip,
});

row_mapper!(content_row -> crate::ContentRecord {
    created_at_epoch_secs, request_id, ak, user_id, tenant, kind, content,
    sealed, expires_at_epoch_secs,
});

fn batch_item_row<'r, R>(row: &'r R) -> BatchItemResult
where
    R: sqlx::Row,
    usize: sqlx::ColumnIndex<R>,
    String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    bool: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    BatchItemResult {
        index: row.get::<i64, _>(0) as usize,
        ok: row.get(1),
        message: row.get(2),
        total_tokens: row.get(3),
        user: row.get(4),
    }
}

/// SQLite-backed store (WAL): ledger, files, and batch jobs in one database
/// file; ids derive from rowids so they stay unique across restarts.
#[derive(Debug)]
pub struct SqliteStore {
    pool: sqlx::SqlitePool,
    ledger_max_rows: u64,
    prune_seq: AtomicUsize,
}

impl SqliteStore {
    /// Open (creating if missing) the database at `path` and ensure the schema.
    pub async fn open(path: &str) -> GResult<Self> {
        Self::open_with_cap(path, 0).await
    }

    /// `ledger_max_rows` > 0 prunes the oldest billing rows past the cap on write.
    pub async fn open_with_cap(path: &str, ledger_max_rows: u64) -> GResult<Self> {
        let opts = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(std::time::Duration::from_secs(5));
        let pool = sqlx::SqlitePool::connect_with(opts)
            .await
            .map_err(|e| crate::sqlx_err("open sqlite store", e))?;
        for ddl in [
            "CREATE TABLE IF NOT EXISTS billing (
                n INTEGER PRIMARY KEY AUTOINCREMENT,
                ak TEXT NOT NULL, product TEXT NOT NULL,
                tenant TEXT NOT NULL DEFAULT 'default', model TEXT NOT NULL,
                served_model TEXT NOT NULL DEFAULT '',
                protocol TEXT NOT NULL, account TEXT NOT NULL,
                prompt_tokens INTEGER NOT NULL, completion_tokens INTEGER NOT NULL,
                total_tokens INTEGER NOT NULL, cost_micros INTEGER NOT NULL,
                vendor_cost_micros INTEGER NOT NULL DEFAULT 0,
                ptu_spillover INTEGER NOT NULL DEFAULT 0,
                user_id TEXT NOT NULL DEFAULT '', request_id TEXT NOT NULL DEFAULT '',
                created_at_epoch_secs INTEGER NOT NULL DEFAULT 0,
                estimated INTEGER NOT NULL DEFAULT 0)",
            "CREATE INDEX IF NOT EXISTS billing_created_idx ON billing (created_at_epoch_secs)",
            "CREATE TABLE IF NOT EXISTS erasures (
                tenant TEXT NOT NULL, user_id TEXT NOT NULL,
                erased_at_epoch_millis INTEGER NOT NULL,
                PRIMARY KEY (tenant, user_id))",
            "CREATE TABLE IF NOT EXISTS usage_rollup (
                minute_epoch INTEGER NOT NULL, tenant TEXT NOT NULL, user_id TEXT NOT NULL,
                model TEXT NOT NULL, requests INTEGER NOT NULL,
                prompt_tokens INTEGER NOT NULL, completion_tokens INTEGER NOT NULL,
                total_tokens INTEGER NOT NULL, cost_micros INTEGER NOT NULL,
                vendor_cost_micros INTEGER NOT NULL,
                PRIMARY KEY (minute_epoch, tenant, user_id, model))",
            "CREATE TABLE IF NOT EXISTS files (
                n INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL,
                tenant TEXT NOT NULL DEFAULT 'default',
                purpose TEXT NOT NULL, bytes INTEGER NOT NULL, content TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS batches (
                n INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL,
                ak TEXT NOT NULL, tenant TEXT NOT NULL DEFAULT 'default', model TEXT NOT NULL,
                status TEXT NOT NULL, total INTEGER NOT NULL)",
            "CREATE TABLE IF NOT EXISTS batch_results (
                batch_id TEXT NOT NULL, idx INTEGER NOT NULL, ok INTEGER NOT NULL,
                message TEXT NOT NULL, total_tokens INTEGER NOT NULL,
                user_id TEXT NOT NULL DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS security_events (
                n INTEGER PRIMARY KEY AUTOINCREMENT, created_at_epoch_secs INTEGER NOT NULL,
                request_id TEXT NOT NULL DEFAULT '', ak TEXT NOT NULL DEFAULT '',
                user_id TEXT NOT NULL DEFAULT '', tenant TEXT NOT NULL DEFAULT '',
                surface TEXT NOT NULL DEFAULT '', rule TEXT NOT NULL DEFAULT '',
                action TEXT NOT NULL DEFAULT '', hits INTEGER NOT NULL DEFAULT 0)",
            "CREATE TABLE IF NOT EXISTS admin_audit (
                n INTEGER PRIMARY KEY AUTOINCREMENT, created_at_epoch_secs INTEGER NOT NULL,
                actor TEXT NOT NULL DEFAULT '', scope TEXT NOT NULL DEFAULT '',
                action TEXT NOT NULL DEFAULT '', target TEXT NOT NULL DEFAULT '',
                summary TEXT NOT NULL DEFAULT '', source_ip TEXT NOT NULL DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS request_content (
                n INTEGER PRIMARY KEY AUTOINCREMENT, created_at_epoch_secs INTEGER NOT NULL,
                request_id TEXT NOT NULL DEFAULT '', ak TEXT NOT NULL DEFAULT '',
                user_id TEXT NOT NULL DEFAULT '', tenant TEXT NOT NULL DEFAULT '',
                kind TEXT NOT NULL DEFAULT '', content TEXT NOT NULL DEFAULT '',
                sealed INTEGER NOT NULL DEFAULT 0, expires_at_epoch_secs INTEGER NOT NULL DEFAULT 0)",
            "CREATE INDEX IF NOT EXISTS content_expiry_idx ON request_content (expires_at_epoch_secs)",
            "CREATE INDEX IF NOT EXISTS content_request_idx ON request_content (request_id)",
        ] {
            sqlx::query(ddl)
                .execute(&pool)
                .await
                .map_err(|e| crate::sqlx_err("create schema", e))?;
        }
        // migrations: "duplicate column name" from an already-migrated db is ignored
        for ddl in [
            "ALTER TABLE billing ADD COLUMN tenant TEXT NOT NULL DEFAULT 'default'",
            "ALTER TABLE billing ADD COLUMN served_model TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE billing ADD COLUMN vendor_cost_micros INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE billing ADD COLUMN user_id TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE billing ADD COLUMN request_id TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE billing ADD COLUMN created_at_epoch_secs INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE billing ADD COLUMN estimated INTEGER NOT NULL DEFAULT 0",
            // back-fill pre-tenant rows to an unmatchable '' tenant (fail closed)
            "ALTER TABLE files ADD COLUMN tenant TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE batches ADD COLUMN tenant TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE batch_results ADD COLUMN user_id TEXT NOT NULL DEFAULT ''",
        ] {
            if let Err(e) = sqlx::query(ddl).execute(&pool).await
                && !e.to_string().contains("duplicate column name")
            {
                return Err(crate::sqlx_err("migrate billing schema", e));
            }
        }
        // a dead process's jobs can never progress single-instance — fail them, don't let clients poll forever
        sqlx::query("UPDATE batches SET status = 'failed' WHERE status IN ('pending', 'running')")
            .execute(&pool)
            .await
            .map_err(|e| crate::sqlx_err("sweep orphaned batches", e))?;
        Ok(Self {
            pool,
            ledger_max_rows,
            prune_seq: AtomicUsize::new(0),
        })
    }
}

/// Rewrite `?` placeholders to Postgres `$1..$N`. Only used on the shared
/// queries below, none of which carries a literal `?`.
fn pg_numbered(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() + 8);
    let mut n = 0;
    for ch in sql.chars() {
        if ch == '?' {
            n += 1;
            out.push('$');
            out.push_str(&n.to_string());
        } else {
            out.push(ch);
        }
    }
    out
}

/// The shared query text for one backend: SQLite takes the `?`-placeholder
/// literal as-is; Postgres rewrites it to `$N` once and caches it.
macro_rules! dialect_sql {
    (sqlite, $s:expr) => {
        $s
    };
    (postgres, $s:expr) => {{
        static SQL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| pg_numbered($s));
        SQL.as_str()
    }};
}

/// One `impl Store` per SQL backend. The methods in the macro body are
/// dialect-independent (placeholder syntax aside) and expand once per backend
/// from this single source; `$specific` carries the genuinely dialect-bound
/// methods (id generation, rollup locking, batch fencing, erasure shape,
/// NULL-parameter and SUM casts).
macro_rules! sql_store_impl {
    ($T:ty, $dialect:ident, { $($specific:item)* }) => {
        #[async_trait::async_trait]
        impl Store for $T {
            $($specific)*

            async fn ledger_add(&self, r: &BillingRecord) -> GResult<()> {
                sqlx::query(dialect_sql!(
                    $dialect,
                    "INSERT INTO billing (ak, product, tenant, model, served_model, protocol, account,
                     prompt_tokens, completion_tokens, total_tokens, cost_micros,
                     vendor_cost_micros, ptu_spillover, user_id, request_id, created_at_epoch_secs,
                     estimated)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                ))
                .bind(&r.ak)
                .bind(&r.product)
                .bind(&r.tenant)
                .bind(&r.model)
                .bind(&r.served_model)
                .bind(&r.protocol)
                .bind(&r.account)
                .bind(r.prompt_tokens)
                .bind(r.completion_tokens)
                .bind(r.total_tokens)
                .bind(r.cost_micros)
                .bind(r.vendor_cost_micros)
                .bind(r.ptu_spillover)
                .bind(&r.user_id)
                .bind(&r.request_id)
                .bind(r.created_at_epoch_secs)
                .bind(r.estimated)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::sqlx_err("insert billing record", e))?;
                if self.ledger_max_rows > 0
                    && self
                        .prune_seq
                        .fetch_add(1, Ordering::Relaxed)
                        .is_multiple_of(LEDGER_PRUNE_EVERY)
                {
                    sqlx::query(dialect_sql!(
                        $dialect,
                        "DELETE FROM billing WHERE n <= (SELECT MAX(n) FROM billing) - ?
                         AND created_at_epoch_secs <
                          (SELECT COALESCE(MAX(minute_epoch), -60) + 60 FROM usage_rollup)"
                    ))
                    .bind(self.ledger_max_rows as i64)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| crate::sqlx_err("prune billing records", e))?;
                }
                Ok(())
            }

            async fn ledger_snapshot(&self, limit: usize) -> GResult<(usize, Vec<BillingRecord>)> {
                let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM billing")
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| crate::sqlx_err("count billing records", e))?;
                let mut rows = sqlx::query(dialect_sql!(
                    $dialect,
                    "SELECT ak, product, tenant, model, served_model, protocol, account,
                     prompt_tokens, completion_tokens, total_tokens, cost_micros,
                     vendor_cost_micros, ptu_spillover, user_id, request_id, created_at_epoch_secs,
                     estimated
                     FROM billing ORDER BY n DESC LIMIT ?"
                ))
                .bind(limit.min(i64::MAX as usize) as i64)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| crate::sqlx_err("read billing records", e))?;
                rows.reverse();
                Ok((total as usize, rows.iter().map(row_to_billing).collect()))
            }

            async fn security_event_add(&self, e: &SecurityEvent) -> GResult<()> {
                sqlx::query(dialect_sql!(
                    $dialect,
                    "INSERT INTO security_events (created_at_epoch_secs, request_id, ak, user_id,
                     tenant, surface, rule, action, hits) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                ))
                .bind(e.created_at_epoch_secs)
                .bind(&e.request_id)
                .bind(&e.ak)
                .bind(&e.user_id)
                .bind(&e.tenant)
                .bind(&e.surface)
                .bind(&e.rule)
                .bind(&e.action)
                .bind(e.hits)
                .execute(&self.pool)
                .await
                .map_err(|err| crate::sqlx_err("insert security event", err))?;
                Ok(())
            }

            async fn admin_audit_add(&self, e: &AdminAudit) -> GResult<()> {
                sqlx::query(dialect_sql!(
                    $dialect,
                    "INSERT INTO admin_audit (created_at_epoch_secs, actor, scope, action, target,
                     summary, source_ip) VALUES (?, ?, ?, ?, ?, ?, ?)"
                ))
                .bind(e.created_at_epoch_secs)
                .bind(&e.actor)
                .bind(&e.scope)
                .bind(&e.action)
                .bind(&e.target)
                .bind(&e.summary)
                .bind(&e.source_ip)
                .execute(&self.pool)
                .await
                .map_err(|err| crate::sqlx_err("insert admin audit", err))?;
                Ok(())
            }

            async fn admin_audit_list(&self, limit: usize) -> GResult<Vec<AdminAudit>> {
                let rows = sqlx::query(dialect_sql!(
                    $dialect,
                    "SELECT created_at_epoch_secs, actor, scope, action, target, summary, source_ip
                     FROM admin_audit ORDER BY n DESC LIMIT ?"
                ))
                .bind(limit.min(i64::MAX as usize) as i64)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| crate::sqlx_err("read admin audit", e))?;
                Ok(rows.iter().map(admin_audit_row).collect())
            }

            async fn content_add(&self, r: &crate::ContentRecord) -> GResult<()> {
                sqlx::query(dialect_sql!(
                    $dialect,
                    "INSERT INTO request_content (created_at_epoch_secs, request_id, ak, user_id,
                     tenant, kind, content, sealed, expires_at_epoch_secs)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                ))
                .bind(r.created_at_epoch_secs)
                .bind(&r.request_id)
                .bind(&r.ak)
                .bind(&r.user_id)
                .bind(&r.tenant)
                .bind(&r.kind)
                .bind(&r.content)
                .bind(r.sealed)
                .bind(r.expires_at_epoch_secs)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::sqlx_err("insert content", e))?;
                Ok(())
            }

            async fn content_purge(&self, now: i64) -> GResult<u64> {
                let r = sqlx::query(dialect_sql!(
                    $dialect,
                    "DELETE FROM request_content
                     WHERE expires_at_epoch_secs > 0 AND expires_at_epoch_secs <= ?"
                ))
                .bind(now)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::sqlx_err("purge content", e))?;
                Ok(r.rows_affected())
            }

            async fn content_for(&self, request_id: &str) -> GResult<Vec<crate::ContentRecord>> {
                let rows = sqlx::query(dialect_sql!(
                    $dialect,
                    "SELECT created_at_epoch_secs, request_id, ak, user_id, tenant, kind, content,
                     sealed, expires_at_epoch_secs FROM request_content WHERE request_id = ? ORDER BY n"
                ))
                .bind(request_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| crate::sqlx_err("read content", e))?;
                Ok(rows.iter().map(content_row).collect())
            }

            async fn file_get(&self, id: &str) -> GResult<Option<StoredFile>> {
                let row = sqlx::query(dialect_sql!(
                    $dialect,
                    "SELECT id, tenant, purpose, bytes, content FROM files WHERE id = ?"
                ))
                .bind(id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| crate::sqlx_err("read file", e))?;
                Ok(row.map(|row| StoredFile {
                    id: row.get(0),
                    tenant: row.get(1),
                    purpose: row.get(2),
                    bytes: row.get::<i64, _>(3) as usize,
                    content: row.get(4),
                }))
            }

            async fn file_delete(&self, id: &str, tenant: &str) -> GResult<bool> {
                let r = sqlx::query(dialect_sql!(
                    $dialect,
                    "DELETE FROM files WHERE id = ? AND tenant = ?"
                ))
                .bind(id)
                .bind(tenant)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::sqlx_err("delete file", e))?;
                Ok(r.rows_affected() > 0)
            }

            async fn batch_get(&self, id: &str) -> GResult<Option<BatchJob>> {
                let row = sqlx::query(dialect_sql!(
                    $dialect,
                    "SELECT id, ak, tenant, model, status, total FROM batches WHERE id = ?"
                ))
                .bind(id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| crate::sqlx_err("read batch", e))?;
                let Some(row) = row else { return Ok(None) };
                let results = sqlx::query(dialect_sql!(
                    $dialect,
                    "SELECT idx, ok, message, total_tokens, user_id FROM batch_results
                     WHERE batch_id = ? ORDER BY idx"
                ))
                .bind(id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| crate::sqlx_err("read batch results", e))?;
                let status_text: String = row.get(4);
                Ok(Some(BatchJob {
                    id: row.get(0),
                    ak: row.get(1),
                    tenant: row.get(2),
                    model: row.get(3),
                    status: BatchStatus::parse(&status_text).unwrap_or(BatchStatus::Failed),
                    total: row.get::<i64, _>(5) as usize,
                    results: results.iter().map(batch_item_row).collect(),
                }))
            }
        }
    };
}

sql_store_impl!(SqliteStore, sqlite, {
    async fn ledger_usage(&self, tenant: Option<&str>) -> GResult<Vec<UsageRow>> {
        // sqlx's SqlSafeStr guard wants static SQL, so the two variants stay spelled out
        let rows =
            match tenant {
                Some(t) => sqlx::query(
                    "SELECT tenant, model, COUNT(*), SUM(prompt_tokens), SUM(completion_tokens),
                     SUM(total_tokens), SUM(cost_micros), SUM(vendor_cost_micros)
                     FROM billing WHERE tenant = ?
                     GROUP BY tenant, model ORDER BY tenant, model",
                )
                .bind(t)
                .fetch_all(&self.pool)
                .await,
                None => sqlx::query(
                    "SELECT tenant, model, COUNT(*), SUM(prompt_tokens), SUM(completion_tokens),
                     SUM(total_tokens), SUM(cost_micros), SUM(vendor_cost_micros)
                     FROM billing
                     GROUP BY tenant, model ORDER BY tenant, model",
                )
                .fetch_all(&self.pool)
                .await,
            }
            .map_err(|e| crate::sqlx_err("roll up usage", e))?;
        Ok(rows.iter().map(usage_row).collect())
    }

    async fn usage_by_user(
        &self,
        tenant: Option<&str>,
        user: Option<&str>,
        since: i64,
        until: i64,
    ) -> GResult<Vec<UserUsageRow>> {
        let (since, until) = align_bounds(since, until);
        let watermark: i64 = sqlx::query_scalar(ROLLUP_WATERMARK_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("read rollup watermark", e))?;
        let rolled = sqlx::query(
            "SELECT user_id, model, SUM(requests), SUM(prompt_tokens), SUM(completion_tokens),
             SUM(total_tokens), SUM(cost_micros), SUM(vendor_cost_micros)
             FROM usage_rollup
             WHERE (?1 IS NULL OR tenant = ?1) AND (?2 IS NULL OR user_id = ?2)
               AND minute_epoch BETWEEN ?3 AND ?4
             GROUP BY user_id, model",
        )
        .bind(tenant)
        .bind(user)
        .bind(since)
        .bind(until)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("read rolled usage", e))?;
        let raw = sqlx::query(
            "SELECT user_id, model, COUNT(*), SUM(prompt_tokens), SUM(completion_tokens),
             SUM(total_tokens), SUM(cost_micros), SUM(vendor_cost_micros)
             FROM billing
             WHERE (?1 IS NULL OR tenant = ?1) AND (?2 IS NULL OR user_id = ?2)
               AND created_at_epoch_secs BETWEEN MAX(?3, ?5) AND ?4
             GROUP BY user_id, model",
        )
        .bind(tenant)
        .bind(user)
        .bind(since)
        .bind(until)
        .bind(watermark)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("roll up user usage", e))?;
        let mut map = std::collections::BTreeMap::new();
        fold_user_usage(&mut map, rolled.iter().map(user_usage_row));
        fold_user_usage(&mut map, raw.iter().map(user_usage_row));
        Ok(map.into_values().collect())
    }

    async fn usage_series(
        &self,
        tenant: Option<&str>,
        user: Option<&str>,
        since: i64,
        until: i64,
        bucket_secs: i64,
    ) -> GResult<Vec<(i64, UserUsageRow)>> {
        let (since, until) = align_bounds(since, until);
        let watermark: i64 = sqlx::query_scalar(ROLLUP_WATERMARK_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("read rollup watermark", e))?;
        let rolled = sqlx::query(
            "SELECT minute_epoch - (minute_epoch % ?5), SUM(requests),
             SUM(prompt_tokens), SUM(completion_tokens),
             SUM(total_tokens), SUM(cost_micros), SUM(vendor_cost_micros)
             FROM usage_rollup
             WHERE (?1 IS NULL OR tenant = ?1) AND (?2 IS NULL OR user_id = ?2)
               AND minute_epoch BETWEEN ?3 AND ?4
             GROUP BY 1",
        )
        .bind(tenant)
        .bind(user)
        .bind(since)
        .bind(until)
        .bind(bucket_secs)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("read rolled series", e))?;
        let raw = sqlx::query(
            "SELECT created_at_epoch_secs - (created_at_epoch_secs % ?6), COUNT(*),
             SUM(prompt_tokens), SUM(completion_tokens), SUM(total_tokens),
             SUM(cost_micros), SUM(vendor_cost_micros)
             FROM billing
             WHERE (?1 IS NULL OR tenant = ?1) AND (?2 IS NULL OR user_id = ?2)
               AND created_at_epoch_secs BETWEEN MAX(?3, ?5) AND ?4
             GROUP BY 1",
        )
        .bind(tenant)
        .bind(user)
        .bind(since)
        .bind(until)
        .bind(watermark)
        .bind(bucket_secs)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("roll up series tail", e))?;
        let mut map = std::collections::BTreeMap::new();
        fold_series(&mut map, rolled.iter().map(series_row));
        fold_series(&mut map, raw.iter().map(series_row));
        Ok(map.into_iter().collect())
    }

    async fn usage_rollup_advance(&self, now: i64) -> GResult<u64> {
        let hi = bucket_floor(now - ROLLUP_SETTLE_SECS);
        let watermark: i64 = sqlx::query_scalar(ROLLUP_WATERMARK_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("read rollup watermark", e))?;
        let r = sqlx::query(
            "INSERT INTO usage_rollup (minute_epoch, tenant, user_id, model, requests,
              prompt_tokens, completion_tokens, total_tokens, cost_micros, vendor_cost_micros)
             SELECT (created_at_epoch_secs/60)*60, tenant, user_id, model, COUNT(*),
              SUM(prompt_tokens), SUM(completion_tokens), SUM(total_tokens),
              SUM(cost_micros), SUM(vendor_cost_micros)
             FROM billing WHERE created_at_epoch_secs >= ?1 AND created_at_epoch_secs < ?2
             GROUP BY 1, 2, 3, 4
             ON CONFLICT (minute_epoch, tenant, user_id, model) DO UPDATE SET
              requests = MAX(usage_rollup.requests, excluded.requests),
              prompt_tokens = MAX(usage_rollup.prompt_tokens, excluded.prompt_tokens),
              completion_tokens = MAX(usage_rollup.completion_tokens, excluded.completion_tokens),
              total_tokens = MAX(usage_rollup.total_tokens, excluded.total_tokens),
              cost_micros = MAX(usage_rollup.cost_micros, excluded.cost_micros),
              vendor_cost_micros = MAX(usage_rollup.vendor_cost_micros, excluded.vendor_cost_micros)",
        )
        .bind((hi - ROLLUP_BACKFILL_SECS).min(watermark))
        .bind(hi)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("advance usage rollup", e))?;
        Ok(r.rows_affected())
    }

    async fn security_events(
        &self,
        tenant: Option<&str>,
        limit: usize,
    ) -> GResult<Vec<SecurityEvent>> {
        let rows = sqlx::query(
            "SELECT created_at_epoch_secs, request_id, ak, user_id, tenant, surface, rule,
             action, hits FROM security_events
             WHERE (?1 IS NULL OR tenant = ?1) ORDER BY n DESC LIMIT ?2",
        )
        .bind(tenant)
        .bind(limit.min(i64::MAX as usize) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("read security events", e))?;
        Ok(rows.iter().map(security_event_row).collect())
    }

    async fn content_erase_user(
        &self,
        tenant: Option<&str>,
        user: &str,
        audit: AdminAudit,
    ) -> GResult<u64> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin erase", e))?;
        let c = sqlx::query(
            "DELETE FROM request_content WHERE user_id = ?1 AND (?2 IS NULL OR tenant = ?2)",
        )
        .bind(user)
        .bind(tenant)
        .execute(&mut *tx)
        .await
        .map_err(|e| crate::sqlx_err("erase user content", e))?;
        let m = sqlx::query(
            "UPDATE batch_results SET message = '' WHERE user_id = ?1 AND message <> ''
              AND batch_id IN (SELECT id FROM batches WHERE ?2 IS NULL OR tenant = ?2)",
        )
        .bind(user)
        .bind(tenant)
        .execute(&mut *tx)
        .await
        .map_err(|e| crate::sqlx_err("erase batch results", e))?;
        let erased = c.rows_affected() + m.rows_affected();
        sqlx::query(
            "INSERT INTO erasures (tenant, user_id, erased_at_epoch_millis) VALUES (?, ?, ?)
             ON CONFLICT (tenant, user_id) DO UPDATE SET
              erased_at_epoch_millis = excluded.erased_at_epoch_millis",
        )
        .bind(tenant.unwrap_or_default())
        .bind(user)
        .bind(crate::epoch_millis())
        .execute(&mut *tx)
        .await
        .map_err(|e| crate::sqlx_err("record erasure", e))?;
        sqlx::query(
            "INSERT INTO admin_audit (created_at_epoch_secs, actor, scope, action, target,
             summary, source_ip) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(audit.created_at_epoch_secs)
        .bind(&audit.actor)
        .bind(&audit.scope)
        .bind(&audit.action)
        .bind(&audit.target)
        .bind(format!("rows={erased}"))
        .bind(&audit.source_ip)
        .execute(&mut *tx)
        .await
        .map_err(|e| crate::sqlx_err("audit erase", e))?;
        tx.commit()
            .await
            .map_err(|e| crate::sqlx_err("commit erase", e))?;
        Ok(erased)
    }

    async fn file_put(&self, tenant: &str, purpose: &str, content: String) -> GResult<StoredFile> {
        let bytes = content.len();
        // ids derive from the AUTOINCREMENT sequence (sqlite_sequence), which a
        // delete never rewinds — MAX(n)+1 would recycle a deleted file's id
        let id: String = sqlx::query_scalar(
            "INSERT INTO files (id, tenant, purpose, bytes, content)
             VALUES ('file-' || (SELECT COALESCE(
                       (SELECT seq FROM sqlite_sequence WHERE name = 'files'), 0) + 1),
                     ?, ?, ?, ?)
             RETURNING id",
        )
        .bind(tenant)
        .bind(purpose)
        .bind(bytes as i64)
        .bind(&content)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("insert file", e))?;
        Ok(StoredFile {
            id,
            tenant: tenant.to_owned(),
            bytes,
            purpose: purpose.to_owned(),
            content,
        })
    }

    async fn user_erased_since(&self, tenant: &str, user: &str, since: i64) -> GResult<bool> {
        let hit: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM erasures
              WHERE user_id = ?1 AND erased_at_epoch_millis >= ?2
                AND (tenant = '' OR tenant = ?3))",
        )
        .bind(user)
        .bind(since)
        .bind(tenant)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("read erasure marker", e))?;
        Ok(hit)
    }

    async fn batch_create(
        &self,
        ak: &str,
        tenant: &str,
        model: &str,
        total: usize,
    ) -> GResult<BatchJob> {
        let id: String = sqlx::query_scalar(
            "INSERT INTO batches (id, ak, tenant, model, status, total)
             VALUES ('batch-' || (SELECT COALESCE(MAX(n), 0) + 1 FROM batches), ?, ?, ?, ?, ?)
             RETURNING id",
        )
        .bind(ak)
        .bind(tenant)
        .bind(model)
        .bind(BatchStatus::Pending.as_str())
        .bind(total as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("insert batch", e))?;
        Ok(BatchJob {
            id,
            ak: ak.to_owned(),
            tenant: tenant.to_owned(),
            model: model.to_owned(),
            status: BatchStatus::Pending,
            total,
            results: Vec::new(),
        })
    }

    async fn batch_set_status(&self, id: &str, status: BatchStatus) -> GResult<()> {
        sqlx::query("UPDATE batches SET status = ? WHERE id = ?")
            .bind(status.as_str())
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("update batch status", e))?;
        Ok(())
    }

    async fn batch_push_result(&self, id: &str, result: BatchItemResult) -> GResult<()> {
        // reject inserts into a terminal batch (single-node, so no writer race)
        sqlx::query(
            "INSERT INTO batch_results (batch_id, idx, ok, message, total_tokens, user_id)
             SELECT ?, ?, ?, ?, ?, ?
             WHERE EXISTS (SELECT 1 FROM batches
                           WHERE id = ? AND status NOT IN ('completed', 'failed'))",
        )
        .bind(id)
        .bind(result.index as i64)
        .bind(result.ok)
        .bind(&result.message)
        .bind(result.total_tokens)
        .bind(&result.user)
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("insert batch result", e))?;
        Ok(())
    }
});

/// A terminal batch's input rows have served their purpose — delete them in
/// the same transaction as the status write, so submitted prompt text cannot
/// outlive the run even across a crash between statements.
async fn prune_terminal_items(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: &str,
    status: BatchStatus,
) -> GResult<()> {
    if !matches!(status, BatchStatus::Completed | BatchStatus::Failed) {
        return Ok(());
    }
    sqlx::query("DELETE FROM batch_items WHERE batch_id = $1")
        .bind(id)
        .execute(&mut **tx)
        .await
        .map_err(|e| crate::sqlx_err("prune batch items", e))?;
    Ok(())
}

/// Postgres-backed store shared across a fleet. Unlike [`SqliteStore`] there
/// is no orphan sweep on open — a starting instance must not fail batches
/// another live instance is still executing.
#[derive(Debug)]
pub struct PostgresStore {
    pool: sqlx::PgPool,
    ledger_max_rows: u64,
    prune_seq: AtomicUsize,
}

impl PostgresStore {
    pub async fn connect(url: &str) -> GResult<Self> {
        Self::connect_with_cap(url, 0).await
    }

    /// `ledger_max_rows` > 0 prunes the oldest billing rows past the cap on write.
    pub async fn connect_with_cap(url: &str, ledger_max_rows: u64) -> GResult<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(10)
            .connect(url)
            .await
            .map_err(|e| crate::sqlx_err("connect postgres store", e))?;
        let ddls = [
            "CREATE TABLE IF NOT EXISTS billing (
                n BIGSERIAL PRIMARY KEY,
                ak TEXT NOT NULL, product TEXT NOT NULL,
                tenant TEXT NOT NULL DEFAULT 'default', model TEXT NOT NULL,
                served_model TEXT NOT NULL DEFAULT '',
                protocol TEXT NOT NULL, account TEXT NOT NULL,
                prompt_tokens BIGINT NOT NULL, completion_tokens BIGINT NOT NULL,
                total_tokens BIGINT NOT NULL, cost_micros BIGINT NOT NULL,
                vendor_cost_micros BIGINT NOT NULL DEFAULT 0,
                ptu_spillover BOOLEAN NOT NULL DEFAULT FALSE,
                user_id TEXT NOT NULL DEFAULT '', request_id TEXT NOT NULL DEFAULT '',
                created_at_epoch_secs BIGINT NOT NULL DEFAULT 0,
                estimated BOOLEAN NOT NULL DEFAULT FALSE)",
            "CREATE INDEX IF NOT EXISTS billing_created_idx ON billing (created_at_epoch_secs)",
            "CREATE TABLE IF NOT EXISTS usage_rollup (
                minute_epoch BIGINT NOT NULL, tenant TEXT NOT NULL, user_id TEXT NOT NULL,
                model TEXT NOT NULL, requests BIGINT NOT NULL,
                prompt_tokens BIGINT NOT NULL, completion_tokens BIGINT NOT NULL,
                total_tokens BIGINT NOT NULL, cost_micros BIGINT NOT NULL,
                vendor_cost_micros BIGINT NOT NULL,
                PRIMARY KEY (minute_epoch, tenant, user_id, model))",
            "CREATE TABLE IF NOT EXISTS security_events (
                n BIGSERIAL PRIMARY KEY, created_at_epoch_secs BIGINT NOT NULL,
                request_id TEXT NOT NULL DEFAULT '', ak TEXT NOT NULL DEFAULT '',
                user_id TEXT NOT NULL DEFAULT '', tenant TEXT NOT NULL DEFAULT '',
                surface TEXT NOT NULL DEFAULT '', rule TEXT NOT NULL DEFAULT '',
                action TEXT NOT NULL DEFAULT '', hits BIGINT NOT NULL DEFAULT 0)",
            "CREATE TABLE IF NOT EXISTS admin_audit (
                n BIGSERIAL PRIMARY KEY, created_at_epoch_secs BIGINT NOT NULL,
                actor TEXT NOT NULL DEFAULT '', scope TEXT NOT NULL DEFAULT '',
                action TEXT NOT NULL DEFAULT '', target TEXT NOT NULL DEFAULT '',
                summary TEXT NOT NULL DEFAULT '', source_ip TEXT NOT NULL DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS request_content (
                n BIGSERIAL PRIMARY KEY, created_at_epoch_secs BIGINT NOT NULL,
                request_id TEXT NOT NULL DEFAULT '', ak TEXT NOT NULL DEFAULT '',
                user_id TEXT NOT NULL DEFAULT '', tenant TEXT NOT NULL DEFAULT '',
                kind TEXT NOT NULL DEFAULT '', content TEXT NOT NULL DEFAULT '',
                sealed BOOLEAN NOT NULL DEFAULT FALSE, expires_at_epoch_secs BIGINT NOT NULL DEFAULT 0)",
            "CREATE INDEX IF NOT EXISTS content_expiry_idx ON request_content (expires_at_epoch_secs)",
            "CREATE INDEX IF NOT EXISTS content_request_idx ON request_content (request_id)",
            "CREATE TABLE IF NOT EXISTS files (
                n BIGSERIAL PRIMARY KEY, id TEXT UNIQUE NOT NULL,
                tenant TEXT NOT NULL DEFAULT 'default',
                purpose TEXT NOT NULL, bytes BIGINT NOT NULL, content TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS batches (
                n BIGSERIAL PRIMARY KEY, id TEXT UNIQUE NOT NULL,
                ak TEXT NOT NULL, tenant TEXT NOT NULL DEFAULT 'default', model TEXT NOT NULL,
                status TEXT NOT NULL, total BIGINT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS batch_results (
                batch_id TEXT NOT NULL, idx BIGINT NOT NULL, ok BOOLEAN NOT NULL,
                message TEXT NOT NULL, total_tokens BIGINT NOT NULL,
                user_id TEXT NOT NULL DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS batch_items (
                batch_id TEXT NOT NULL, idx BIGINT NOT NULL, messages TEXT NOT NULL,
                PRIMARY KEY (batch_id, idx))",
            // per-item end-user attribution so a fleet drainer still bills/budgets it
            "ALTER TABLE batch_items ADD COLUMN IF NOT EXISTS user_id TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE batch_results ADD COLUMN IF NOT EXISTS user_id TEXT NOT NULL DEFAULT ''",
            // pre-upgrade results predate the user_id column; their items rows
            // (pruned only at terminal status from this version on) still carry
            // the owner — backfill so erasure reaches history. Idempotent.
            "UPDATE batch_results r SET user_id = i.user_id FROM batch_items i
             WHERE r.batch_id = i.batch_id AND r.idx = i.idx
               AND r.user_id = '' AND i.user_id <> ''",
            "ALTER TABLE batches ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ",
            // fence token: bumped on every claim so a reclaimed executor's fenced writes no-op
            "ALTER TABLE batches ADD COLUMN IF NOT EXISTS claim_seq BIGINT NOT NULL DEFAULT 0",
            // back-fill pre-tenant rows to an unmatchable '' tenant (fail closed)
            "ALTER TABLE files ADD COLUMN IF NOT EXISTS tenant TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE batches ADD COLUMN IF NOT EXISTS tenant TEXT NOT NULL DEFAULT ''",
            // dedup rows the pre-fix plain-INSERT could have left, so the unique index builds
            "DELETE FROM batch_results a USING batch_results b
             WHERE a.ctid < b.ctid AND a.batch_id = b.batch_id AND a.idx = b.idx",
            "CREATE UNIQUE INDEX IF NOT EXISTS batch_results_uidx
             ON batch_results (batch_id, idx)",
            "ALTER TABLE billing ADD COLUMN IF NOT EXISTS vendor_cost_micros BIGINT NOT NULL DEFAULT 0",
            "ALTER TABLE billing ADD COLUMN IF NOT EXISTS user_id TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE billing ADD COLUMN IF NOT EXISTS request_id TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE billing ADD COLUMN IF NOT EXISTS created_at_epoch_secs BIGINT NOT NULL DEFAULT 0",
            "ALTER TABLE billing ADD COLUMN IF NOT EXISTS estimated BOOLEAN NOT NULL DEFAULT FALSE",
        ];
        crate::setup_schema(&pool, "postgres", &ddls).await?;
        Ok(Self {
            pool,
            ledger_max_rows,
            prune_seq: AtomicUsize::new(0),
        })
    }
}

sql_store_impl!(PostgresStore, postgres, {
    async fn ledger_usage(&self, tenant: Option<&str>) -> GResult<Vec<UsageRow>> {
        // sqlx's SqlSafeStr guard wants static SQL, so the two variants stay spelled out
        let rows = match tenant {
            Some(t) => {
                sqlx::query(
                    "SELECT tenant, model, COUNT(*),
                     SUM(prompt_tokens)::BIGINT, SUM(completion_tokens)::BIGINT,
                     SUM(total_tokens)::BIGINT, SUM(cost_micros)::BIGINT,
                     SUM(vendor_cost_micros)::BIGINT
                     FROM billing WHERE tenant = $1
                     GROUP BY tenant, model ORDER BY tenant, model",
                )
                .bind(t)
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query(
                    "SELECT tenant, model, COUNT(*),
                     SUM(prompt_tokens)::BIGINT, SUM(completion_tokens)::BIGINT,
                     SUM(total_tokens)::BIGINT, SUM(cost_micros)::BIGINT,
                     SUM(vendor_cost_micros)::BIGINT
                     FROM billing
                     GROUP BY tenant, model ORDER BY tenant, model",
                )
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(|e| crate::sqlx_err("roll up usage", e))?;
        Ok(rows.iter().map(usage_row).collect())
    }

    async fn usage_by_user(
        &self,
        tenant: Option<&str>,
        user: Option<&str>,
        since: i64,
        until: i64,
    ) -> GResult<Vec<UserUsageRow>> {
        let (since, until) = align_bounds(since, until);
        let watermark: i64 = sqlx::query_scalar(ROLLUP_WATERMARK_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("read rollup watermark", e))?;
        let rolled = sqlx::query(
            "SELECT user_id, model, SUM(requests)::BIGINT,
             SUM(prompt_tokens)::BIGINT, SUM(completion_tokens)::BIGINT,
             SUM(total_tokens)::BIGINT, SUM(cost_micros)::BIGINT, SUM(vendor_cost_micros)::BIGINT
             FROM usage_rollup
             WHERE ($1::text IS NULL OR tenant = $1) AND ($2::text IS NULL OR user_id = $2)
               AND minute_epoch BETWEEN $3 AND $4
             GROUP BY user_id, model",
        )
        .bind(tenant)
        .bind(user)
        .bind(since)
        .bind(until)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("read rolled usage", e))?;
        let raw = sqlx::query(
            "SELECT user_id, model, COUNT(*),
             SUM(prompt_tokens)::BIGINT, SUM(completion_tokens)::BIGINT,
             SUM(total_tokens)::BIGINT, SUM(cost_micros)::BIGINT, SUM(vendor_cost_micros)::BIGINT
             FROM billing
             WHERE ($1::text IS NULL OR tenant = $1) AND ($2::text IS NULL OR user_id = $2)
               AND created_at_epoch_secs BETWEEN GREATEST($3, $5) AND $4
             GROUP BY user_id, model",
        )
        .bind(tenant)
        .bind(user)
        .bind(since)
        .bind(until)
        .bind(watermark)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("roll up user usage", e))?;
        let mut map = std::collections::BTreeMap::new();
        fold_user_usage(&mut map, rolled.iter().map(user_usage_row));
        fold_user_usage(&mut map, raw.iter().map(user_usage_row));
        Ok(map.into_values().collect())
    }

    async fn usage_series(
        &self,
        tenant: Option<&str>,
        user: Option<&str>,
        since: i64,
        until: i64,
        bucket_secs: i64,
    ) -> GResult<Vec<(i64, UserUsageRow)>> {
        let (since, until) = align_bounds(since, until);
        let watermark: i64 = sqlx::query_scalar(ROLLUP_WATERMARK_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("read rollup watermark", e))?;
        let rolled = sqlx::query(
            "SELECT minute_epoch - (minute_epoch % $5), SUM(requests)::BIGINT,
             SUM(prompt_tokens)::BIGINT, SUM(completion_tokens)::BIGINT,
             SUM(total_tokens)::BIGINT, SUM(cost_micros)::BIGINT, SUM(vendor_cost_micros)::BIGINT
             FROM usage_rollup
             WHERE ($1::text IS NULL OR tenant = $1) AND ($2::text IS NULL OR user_id = $2)
               AND minute_epoch BETWEEN $3 AND $4
             GROUP BY 1",
        )
        .bind(tenant)
        .bind(user)
        .bind(since)
        .bind(until)
        .bind(bucket_secs)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("read rolled series", e))?;
        let raw = sqlx::query(
            "SELECT created_at_epoch_secs - (created_at_epoch_secs % $6), COUNT(*),
             SUM(prompt_tokens)::BIGINT, SUM(completion_tokens)::BIGINT, SUM(total_tokens)::BIGINT,
             SUM(cost_micros)::BIGINT, SUM(vendor_cost_micros)::BIGINT
             FROM billing
             WHERE ($1::text IS NULL OR tenant = $1) AND ($2::text IS NULL OR user_id = $2)
               AND created_at_epoch_secs BETWEEN GREATEST($3, $5) AND $4
             GROUP BY 1",
        )
        .bind(tenant)
        .bind(user)
        .bind(since)
        .bind(until)
        .bind(watermark)
        .bind(bucket_secs)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("roll up series tail", e))?;
        let mut map = std::collections::BTreeMap::new();
        fold_series(&mut map, rolled.iter().map(series_row));
        fold_series(&mut map, raw.iter().map(series_row));
        Ok(map.into_iter().collect())
    }

    async fn usage_rollup_advance(&self, now: i64) -> GResult<u64> {
        let hi = bucket_floor(now - ROLLUP_SETTLE_SECS);
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin rollup", e))?;
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
            .bind(ROLLUP_LOCK_KEY)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| crate::sqlx_err("acquire rollup lock", e))?;
        if !locked {
            return Ok(0);
        }
        let watermark: i64 = sqlx::query_scalar(ROLLUP_WATERMARK_SQL)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| crate::sqlx_err("read rollup watermark", e))?;
        let r = sqlx::query(
            "INSERT INTO usage_rollup (minute_epoch, tenant, user_id, model, requests,
              prompt_tokens, completion_tokens, total_tokens, cost_micros, vendor_cost_micros)
             SELECT (created_at_epoch_secs/60)*60, tenant, user_id, model, COUNT(*),
              SUM(prompt_tokens)::BIGINT, SUM(completion_tokens)::BIGINT,
              SUM(total_tokens)::BIGINT, SUM(cost_micros)::BIGINT,
              SUM(vendor_cost_micros)::BIGINT
             FROM billing WHERE created_at_epoch_secs >= $1 AND created_at_epoch_secs < $2
             GROUP BY 1, 2, 3, 4
             ON CONFLICT (minute_epoch, tenant, user_id, model) DO UPDATE SET
              requests = GREATEST(usage_rollup.requests, excluded.requests),
              prompt_tokens = GREATEST(usage_rollup.prompt_tokens, excluded.prompt_tokens),
              completion_tokens =
               GREATEST(usage_rollup.completion_tokens, excluded.completion_tokens),
              total_tokens = GREATEST(usage_rollup.total_tokens, excluded.total_tokens),
              cost_micros = GREATEST(usage_rollup.cost_micros, excluded.cost_micros),
              vendor_cost_micros =
               GREATEST(usage_rollup.vendor_cost_micros, excluded.vendor_cost_micros)",
        )
        .bind((hi - ROLLUP_BACKFILL_SECS).min(watermark))
        .bind(hi)
        .execute(&mut *tx)
        .await
        .map_err(|e| crate::sqlx_err("advance usage rollup", e))?;
        tx.commit()
            .await
            .map_err(|e| crate::sqlx_err("commit rollup", e))?;
        Ok(r.rows_affected())
    }

    async fn security_events(
        &self,
        tenant: Option<&str>,
        limit: usize,
    ) -> GResult<Vec<SecurityEvent>> {
        let rows = sqlx::query(
            "SELECT created_at_epoch_secs, request_id, ak, user_id, tenant, surface, rule,
             action, hits FROM security_events
             WHERE ($1::text IS NULL OR tenant = $1) ORDER BY n DESC LIMIT $2",
        )
        .bind(tenant)
        .bind(limit.min(i64::MAX as usize) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("read security events", e))?;
        Ok(rows.iter().map(security_event_row).collect())
    }

    async fn content_erase_user(
        &self,
        tenant: Option<&str>,
        user: &str,
        audit: AdminAudit,
    ) -> GResult<u64> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin erase", e))?;
        let c = sqlx::query(
            "DELETE FROM request_content
             WHERE user_id = $1 AND ($2::text IS NULL OR tenant = $2)",
        )
        .bind(user)
        .bind(tenant)
        .execute(&mut *tx)
        .await
        .map_err(|e| crate::sqlx_err("erase user content", e))?;
        let m = sqlx::query(
            "UPDATE batch_results r SET message = '' FROM batches b
             WHERE r.batch_id = b.id AND r.user_id = $1 AND r.message <> ''
               AND ($2::text IS NULL OR b.tenant = $2)",
        )
        .bind(user)
        .bind(tenant)
        .execute(&mut *tx)
        .await
        .map_err(|e| crate::sqlx_err("erase batch results", e))?;
        // pending/running batches included: the emptied item fails at execution
        // instead of running erased content (terminal batches already pruned)
        let i = sqlx::query(
            "UPDATE batch_items i SET messages = '[]' FROM batches b
             WHERE i.batch_id = b.id AND i.user_id = $1 AND i.messages <> '[]'
               AND ($2::text IS NULL OR b.tenant = $2)",
        )
        .bind(user)
        .bind(tenant)
        .execute(&mut *tx)
        .await
        .map_err(|e| crate::sqlx_err("erase batch items", e))?;
        let erased = c.rows_affected() + m.rows_affected() + i.rows_affected();
        sqlx::query(
            "INSERT INTO admin_audit (created_at_epoch_secs, actor, scope, action, target,
             summary, source_ip) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(audit.created_at_epoch_secs)
        .bind(&audit.actor)
        .bind(&audit.scope)
        .bind(&audit.action)
        .bind(&audit.target)
        .bind(format!("rows={erased}"))
        .bind(&audit.source_ip)
        .execute(&mut *tx)
        .await
        .map_err(|e| crate::sqlx_err("audit erase", e))?;
        tx.commit()
            .await
            .map_err(|e| crate::sqlx_err("commit erase", e))?;
        Ok(erased)
    }

    async fn file_put(&self, tenant: &str, purpose: &str, content: String) -> GResult<StoredFile> {
        let bytes = content.len();
        // consume the sequence explicitly — concurrent writers race a MAX(n)+1 subselect
        let id: String = sqlx::query_scalar(
            "INSERT INTO files (n, id, tenant, purpose, bytes, content)
             SELECT v, 'file-' || v, $1, $2, $3, $4
             FROM nextval(pg_get_serial_sequence('files', 'n')) AS v
             RETURNING id",
        )
        .bind(tenant)
        .bind(purpose)
        .bind(bytes as i64)
        .bind(&content)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("insert file", e))?;
        Ok(StoredFile {
            id,
            tenant: tenant.to_owned(),
            bytes,
            purpose: purpose.to_owned(),
            content,
        })
    }

    async fn batch_create(
        &self,
        ak: &str,
        tenant: &str,
        model: &str,
        total: usize,
    ) -> GResult<BatchJob> {
        let id: String = sqlx::query_scalar(PG_INSERT_BATCH)
            .bind(ak)
            .bind(tenant)
            .bind(model)
            .bind(BatchStatus::Pending.as_str())
            .bind(total as i64)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("insert batch", e))?;
        Ok(BatchJob {
            id,
            ak: ak.to_owned(),
            tenant: tenant.to_owned(),
            model: model.to_owned(),
            status: BatchStatus::Pending,
            total,
            results: Vec::new(),
        })
    }

    async fn batch_set_status(&self, id: &str, status: BatchStatus) -> GResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin status", e))?;
        sqlx::query("UPDATE batches SET status = $1 WHERE id = $2")
            .bind(status.as_str())
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(|e| crate::sqlx_err("update batch status", e))?;
        prune_terminal_items(&mut tx, id, status).await?;
        tx.commit()
            .await
            .map_err(|e| crate::sqlx_err("commit status", e))
    }

    async fn batch_set_status_owned(
        &self,
        id: &str,
        status: BatchStatus,
        claim: i64,
    ) -> GResult<bool> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin status", e))?;
        let r = sqlx::query("UPDATE batches SET status = $1 WHERE id = $2 AND claim_seq = $3")
            .bind(status.as_str())
            .bind(id)
            .bind(claim)
            .execute(&mut *tx)
            .await
            .map_err(|e| crate::sqlx_err("update batch status (fenced)", e))?;
        let applied = r.rows_affected() > 0;
        if applied {
            prune_terminal_items(&mut tx, id, status).await?;
        }
        tx.commit()
            .await
            .map_err(|e| crate::sqlx_err("commit status", e))?;
        Ok(applied)
    }

    async fn batch_push_result(&self, id: &str, result: BatchItemResult) -> GResult<()> {
        // DO NOTHING (first-writer-wins) + non-terminal guard; the FOR UPDATE row
        // lock serializes with batch_finalize so no result lands after finalize.
        sqlx::query(
            "INSERT INTO batch_results (batch_id, idx, ok, message, total_tokens, user_id)
             SELECT $1, $2, $3, $4, $5, $6
             WHERE EXISTS (SELECT 1 FROM batches
                           WHERE id = $1 AND status NOT IN ('completed', 'failed') FOR UPDATE)
             ON CONFLICT (batch_id, idx) DO NOTHING",
        )
        .bind(id)
        .bind(result.index as i64)
        .bind(result.ok)
        .bind(&result.message)
        .bind(result.total_tokens)
        .bind(&result.user)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("insert batch result", e))?;
        Ok(())
    }

    async fn batch_finalize(&self, id: &str, claim: i64) -> GResult<Option<BatchStatus>> {
        // Lock the row, THEN aggregate separately: a single UPDATE reads its
        // subquery on the statement-start snapshot and would miss a result that
        // commits while it waits for the lock, wrongly reporting Failed.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("finalize begin", e))?;
        let locked = sqlx::query(
            "SELECT total FROM batches
             WHERE id = $1 AND claim_seq = $2 AND status = 'running' FOR UPDATE",
        )
        .bind(id)
        .bind(claim)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| crate::sqlx_err("finalize lock", e))?;
        let Some(row) = locked else {
            return Ok(None); // not owned or already terminal; tx rolls back on drop
        };
        let total: i64 = row.get(0);
        let agg = sqlx::query(
            "SELECT count(*), count(*) FILTER (WHERE NOT ok) FROM batch_results
             WHERE batch_id = $1",
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| crate::sqlx_err("finalize count", e))?;
        let (n, failed): (i64, i64) = (agg.get(0), agg.get(1));
        let done = if n == total && failed == 0 {
            BatchStatus::Completed
        } else {
            BatchStatus::Failed
        };
        sqlx::query("UPDATE batches SET status = $1 WHERE id = $2")
            .bind(done.as_str())
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(|e| crate::sqlx_err("finalize write", e))?;
        prune_terminal_items(&mut tx, id, done).await?;
        tx.commit()
            .await
            .map_err(|e| crate::sqlx_err("finalize commit", e))?;
        Ok(Some(done))
    }

    fn distributed_batches(&self) -> bool {
        true
    }

    async fn batch_enqueue(
        &self,
        ak: &str,
        tenant: &str,
        model: &str,
        items: &[gw_models::BatchItem],
    ) -> GResult<BatchJob> {
        // the batch becomes claimable only once all its items are committed
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin batch enqueue", e))?;
        let id: String = sqlx::query_scalar(PG_INSERT_BATCH)
            .bind(ak)
            .bind(tenant)
            .bind(model)
            .bind(BatchStatus::Pending.as_str())
            .bind(items.len() as i64)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| crate::sqlx_err("insert batch", e))?;
        for (idx, item) in items.iter().enumerate() {
            let json = serde_json::to_string(&item.messages).unwrap_or_else(|_| "[]".into());
            sqlx::query(
                "INSERT INTO batch_items (batch_id, idx, messages, user_id) VALUES ($1, $2, $3, $4)",
            )
            .bind(&id)
            .bind(idx as i64)
            .bind(json)
            .bind(&item.user)
            .execute(&mut *tx)
            .await
            .map_err(|e| crate::sqlx_err("save batch item", e))?;
        }
        tx.commit()
            .await
            .map_err(|e| crate::sqlx_err("commit batch enqueue", e))?;
        Ok(BatchJob {
            id,
            ak: ak.to_owned(),
            tenant: tenant.to_owned(),
            model: model.to_owned(),
            status: BatchStatus::Pending,
            total: items.len(),
            results: Vec::new(),
        })
    }

    async fn batch_item_snapshot(
        &self,
        id: &str,
        idx: usize,
    ) -> GResult<Option<gw_models::BatchItem>> {
        let row = sqlx::query(
            "SELECT messages, user_id FROM batch_items WHERE batch_id = $1 AND idx = $2",
        )
        .bind(id)
        .bind(idx as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("read batch item", e))?;
        Ok(row.map(|r| gw_models::BatchItem {
            messages: serde_json::from_str(r.get::<&str, _>(0)).unwrap_or_default(),
            user: r.get(1),
        }))
    }

    async fn batch_load_items(&self, id: &str) -> GResult<Vec<gw_models::BatchItem>> {
        let rows = sqlx::query(
            "SELECT messages, user_id FROM batch_items WHERE batch_id = $1 ORDER BY idx",
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("load batch items", e))?;
        Ok(rows
            .iter()
            .map(|r| gw_models::BatchItem {
                messages: serde_json::from_str(r.get::<&str, _>(0)).unwrap_or_default(),
                user: r.get::<String, _>(1),
            })
            .collect())
    }

    async fn batch_claim_pending(&self, stale_secs: i64) -> GResult<Option<(BatchJob, i64)>> {
        // requeue batches whose executor stopped heartbeating before claiming
        sqlx::query(
            "UPDATE batches SET status = 'pending', claimed_at = NULL
             WHERE status = 'running'
               AND claimed_at < now() - make_interval(secs => $1)",
        )
        .bind(stale_secs as f64)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("requeue stale batches", e))?;
        // bump claim_seq so any prior (stalled) executor's fenced writes no-op
        let row = sqlx::query(
            "UPDATE batches SET status = 'running', claimed_at = now(),
                    claim_seq = claim_seq + 1
             WHERE id = (SELECT id FROM batches WHERE status = 'pending'
                         ORDER BY n FOR UPDATE SKIP LOCKED LIMIT 1)
             RETURNING id, ak, tenant, model, total, claim_seq",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("claim batch", e))?;
        Ok(row.map(|r| {
            (
                BatchJob {
                    id: r.get(0),
                    ak: r.get(1),
                    tenant: r.get(2),
                    model: r.get(3),
                    status: BatchStatus::Running,
                    total: r.get::<i64, _>(4) as usize,
                    results: Vec::new(),
                },
                r.get::<i64, _>(5),
            )
        }))
    }

    async fn batch_touch(&self, id: &str, claim: i64) -> GResult<bool> {
        let r =
            sqlx::query("UPDATE batches SET claimed_at = now() WHERE id = $1 AND claim_seq = $2")
                .bind(id)
                .bind(claim)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::sqlx_err("heartbeat batch", e))?;
        Ok(r.rows_affected() > 0)
    }
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_numbered_rewrites_placeholders_in_order() {
        assert_eq!(
            pg_numbered("a = ? AND b IN (?, ?)"),
            "a = $1 AND b IN ($2, $3)"
        );
        assert_eq!(pg_numbered("no placeholders"), "no placeholders");
    }

    #[test]
    fn model_token_rate_maps_config_weights() {
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: m, protocol: openai-chat, token_rate: {read_cache: 0.1, write_cache: 1.25}}]";
        let cfg = gw_config::GatewayConfig::from_yaml(yaml).unwrap();
        let rate = model_token_rate(&cfg, "m");
        assert_eq!(
            (rate.read_cache_weight, rate.write_cache_weight),
            (0.1, 1.25)
        );
        assert!(!rate.prompt_includes_cache);
        assert_eq!(
            model_token_rate(&cfg, "absent"),
            gw_models::TokenRate::default()
        );
    }

    #[test]
    fn billing_record_bills_weighted_sides_keeps_raw_columns() {
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: m, protocol: openai-chat, input_price_per_1k_micros: 1000, output_price_per_1k_micros: 2000}]";
        let cfg = gw_config::GatewayConfig::from_yaml(yaml).unwrap();
        let rec = billing_record(
            &cfg,
            &BillingInput {
                ak: "k",
                product: "demo",
                tenant: "default",
                user_id: "u1",
                request_id: "req-1",
                requested_model: "m",
                served_model: "m",
                protocol: "openai-chat",
                account: "acc",
                prompt: 1140,
                completion: 60,
                billable_prompt: 250,
                billable_completion: 60,
                total: 310,
                ptu_spillover: false,
                estimated: false,
            },
        );
        assert_eq!(rec.prompt_tokens, 1140);
        assert_eq!(rec.completion_tokens, 60);
        assert_eq!(rec.total_tokens, 310);
        assert_eq!(rec.cost_micros, 250 + 120);
    }

    #[test]
    fn billing_record_clamps_hostile_usage() {
        let cfg = gw_config::GatewayConfig::embedded_default().unwrap();
        let rec = billing_record(
            &cfg,
            &BillingInput {
                ak: "k",
                product: "demo",
                tenant: "default",
                user_id: "u1",
                request_id: "req-1",
                requested_model: "gpt-4o",
                served_model: "gpt-4o",
                protocol: "openai-chat",
                account: "acc",
                prompt: i64::MAX,
                completion: i64::MAX,
                billable_prompt: i64::MAX,
                billable_completion: i64::MAX,
                total: i64::MAX,
                ptu_spillover: false,
                estimated: false,
            },
        );
        assert_eq!(rec.prompt_tokens, MAX_METERED_TOKENS);
        assert_eq!(rec.completion_tokens, MAX_METERED_TOKENS);
        assert_eq!(rec.total_tokens, MAX_METERED_TOKENS);
        assert!(rec.cost_micros >= 0, "cost must not overflow negative");
    }

    fn record(model: &str) -> BillingRecord {
        BillingRecord {
            ak: "ak-t".into(),
            product: "p".into(),
            tenant: "default".into(),
            user_id: "u1".into(),
            request_id: "req-1".into(),
            created_at_epoch_secs: 1_000,
            model: model.into(),
            served_model: model.into(),
            protocol: "openai-chat".into(),
            account: "acc".into(),
            prompt_tokens: 3,
            completion_tokens: 5,
            total_tokens: 8,
            cost_micros: 42,
            vendor_cost_micros: 7,
            ptu_spillover: false,
            estimated: false,
        }
    }

    async fn exercise(store: &dyn Store) {
        store.ledger_add(&record("m1")).await.unwrap();
        store.ledger_add(&record("m2")).await.unwrap();
        let (total, snap) = store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(total, 2);
        assert_eq!(snap[0].model, "m1");
        assert_eq!(snap[1].total_tokens, 8);
        let (total, page) = store.ledger_snapshot(1).await.unwrap();
        assert_eq!(total, 2);
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].model, "m2");

        let f = store
            .file_put("default", "batch", "line1\nline2".into())
            .await
            .unwrap();
        assert_eq!(f.bytes, 11);
        let got = store.file_get(&f.id).await.unwrap().unwrap();
        assert_eq!(got.content, "line1\nline2");
        assert_eq!(got.tenant, "default");
        assert!(store.file_get("file-nope").await.unwrap().is_none());

        let job = store
            .batch_create("ak-t", "default", "m1", 2)
            .await
            .unwrap();
        assert_eq!(job.status, BatchStatus::Pending);
        assert_eq!(job.tenant, "default");
        store
            .batch_set_status(&job.id, BatchStatus::Running)
            .await
            .unwrap();
        store
            .batch_push_result(
                &job.id,
                BatchItemResult {
                    index: 0,
                    ok: true,
                    message: "ok".into(),
                    total_tokens: 8,
                    user: String::new(),
                },
            )
            .await
            .unwrap();
        store
            .batch_set_status(&job.id, BatchStatus::Completed)
            .await
            .unwrap();
        let got = store.batch_get(&job.id).await.unwrap().unwrap();
        assert_eq!(got.status, BatchStatus::Completed);
        assert_eq!(got.results.len(), 1);
        assert_eq!(got.results[0].message, "ok");
    }

    #[tokio::test]
    async fn memory_store_roundtrip() {
        exercise(&MemoryStore::default()).await;
    }

    async fn exercise_audit(store: &dyn Store) {
        let mut a = record("m1");
        a.user_id = "alice".into();
        a.created_at_epoch_secs = 500;
        let mut b = record("m1");
        b.user_id = "bob".into();
        b.created_at_epoch_secs = 1_500;
        store.ledger_add(&a).await.unwrap();
        store.ledger_add(&b).await.unwrap();

        let all = store.usage_by_user(None, None, 0, i64::MAX).await.unwrap();
        assert_eq!(all.len(), 2, "two users");
        let alice = store
            .usage_by_user(Some("default"), Some("alice"), 0, i64::MAX)
            .await
            .unwrap();
        assert_eq!(alice.len(), 1);
        assert_eq!(alice[0].user_id, "alice");
        assert_eq!(alice[0].total_tokens, 8);
        let windowed = store
            .usage_by_user(None, None, 1_000, i64::MAX)
            .await
            .unwrap();
        assert_eq!(windowed.len(), 1, "only bob is in the window");
        assert_eq!(windowed[0].user_id, "bob");

        store
            .security_event_add(&SecurityEvent {
                created_at_epoch_secs: 10,
                request_id: "req-9".into(),
                ak: "ak-t".into(),
                user_id: "alice".into(),
                tenant: "default".into(),
                surface: "openai-chat".into(),
                rule: "blocklist".into(),
                action: "block".into(),
                hits: 1,
            })
            .await
            .unwrap();
        let events = store.security_events(Some("default"), 10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].rule, "blocklist");
        assert!(
            store
                .security_events(Some("ghost"), 10)
                .await
                .unwrap()
                .is_empty(),
            "tenant filter excludes others"
        );

        store
            .admin_audit_add(&AdminAudit {
                created_at_epoch_secs: 20,
                actor: "global".into(),
                scope: "global".into(),
                action: "key_create".into(),
                target: "ak-new".into(),
                summary: "tenant=t1".into(),
                source_ip: "10.0.0.1".into(),
            })
            .await
            .unwrap();
        let audit = store.admin_audit_list(10).await.unwrap();
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].action, "key_create");
        assert_eq!(audit[0].target, "ak-new");
    }

    #[tokio::test]
    async fn memory_audit_roundtrip() {
        exercise_audit(&MemoryStore::default()).await;
    }

    #[tokio::test]
    async fn content_retention_stores_and_purges() {
        let store = MemoryStore::default();
        let rec = |kind: &str, expires: i64| crate::ContentRecord {
            created_at_epoch_secs: 100,
            request_id: "req-1".into(),
            ak: "ak".into(),
            user_id: "u".into(),
            tenant: "default".into(),
            kind: kind.into(),
            content: "hello".into(),
            sealed: false,
            expires_at_epoch_secs: expires,
        };
        store.content_add(&rec("prompt", 200)).await.unwrap();
        store.content_add(&rec("response", 0)).await.unwrap();
        let got = store.content_for("req-1").await.unwrap();
        assert_eq!(got.len(), 2);

        assert_eq!(
            store.content_purge(150).await.unwrap(),
            0,
            "not yet expired"
        );
        assert_eq!(store.content_purge(250).await.unwrap(), 1, "prompt expired");
        let kept = store.content_for("req-1").await.unwrap();
        assert_eq!(kept.len(), 1, "the keep-forever response survives");
        assert_eq!(kept[0].kind, "response");
    }

    #[tokio::test]
    async fn sqlite_audit_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(dir.path().join("audit.db").to_str().unwrap())
            .await
            .unwrap();
        exercise_audit(&store).await;
    }

    /// `ns` isolates shared-database (PG) runs from parallel tests.
    async fn exercise_rollup(store: &dyn Store, ns: &str) {
        let tenant = format!("tr{ns}");
        let mut a = record("m1");
        a.tenant = tenant.clone();
        a.user_id = "alice".into();
        a.created_at_epoch_secs = 400;
        let mut b = record("m1");
        b.tenant = tenant.clone();
        b.user_id = "bob".into();
        b.created_at_epoch_secs = 460;
        let mut tail = record("m1");
        tail.tenant = tenant.clone();
        tail.user_id = "carol".into();
        tail.created_at_epoch_secs = 1_520;
        for r in [&a, &b, &tail] {
            store.ledger_add(r).await.unwrap();
        }
        let baseline = store
            .usage_by_user(Some(&tenant), None, 0, i64::MAX)
            .await
            .unwrap();
        assert_eq!(baseline.len(), 3, "raw-only result before any rollup");

        store.usage_rollup_advance(1_500).await.unwrap();
        store.usage_rollup_advance(1_500).await.unwrap();
        let after = store
            .usage_by_user(Some(&tenant), None, 0, i64::MAX)
            .await
            .unwrap();
        assert_eq!(
            format!("{baseline:?}"),
            format!("{after:?}"),
            "rollup + tail union matches raw and advancing twice is idempotent"
        );

        let windowed = store
            .usage_by_user(Some(&tenant), None, 450, i64::MAX)
            .await
            .unwrap();
        assert!(
            windowed.iter().all(|r| r.user_id != "alice"),
            "window excludes alice's earlier bucket"
        );
        assert!(
            windowed.iter().any(|r| r.user_id == "bob")
                && windowed.iter().any(|r| r.user_id == "carol"),
            "window keeps the rolled bucket and the raw tail"
        );

        let series = store
            .usage_series(Some(&tenant), None, 0, i64::MAX, 600)
            .await
            .unwrap();
        let counts: Vec<(i64, i64)> = series.iter().map(|(b, r)| (*b, r.requests)).collect();
        assert_eq!(
            counts,
            vec![(0, 2), (1_200, 1)],
            "rolled buckets and the raw tail group by bucket start"
        );
    }

    #[tokio::test]
    async fn memory_rollup_roundtrip() {
        exercise_rollup(&MemoryStore::default(), "").await;
    }

    #[tokio::test]
    async fn sqlite_rollup_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(dir.path().join("rollup.db").to_str().unwrap())
            .await
            .unwrap();
        exercise_rollup(&store, "").await;
    }

    #[tokio::test]
    async fn rollup_survives_ledger_prune() {
        let store = MemoryStore::with_ledger_cap(1);
        let mut a = record("m1");
        a.user_id = "alice".into();
        a.created_at_epoch_secs = 9_000;
        store.ledger_add(&a).await.unwrap();
        store.usage_rollup_advance(10_000).await.unwrap();

        let mut b = record("m1");
        b.user_id = "bob".into();
        b.created_at_epoch_secs = 9_970;
        store.ledger_add(&b).await.unwrap();
        let (total, _) = store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(total, 1, "the cap pruned alice's raw row");

        store.usage_rollup_advance(10_000).await.unwrap();
        let usage = store.usage_by_user(None, None, 0, i64::MAX).await.unwrap();
        let alice = usage.iter().find(|r| r.user_id == "alice");
        assert_eq!(
            alice.map(|r| r.total_tokens),
            Some(8),
            "pruned row still billed via its bucket (re-advance never deletes)"
        );
        assert!(
            usage.iter().any(|r| r.user_id == "bob"),
            "unrolled tail served from the raw ledger"
        );
    }

    #[tokio::test]
    async fn rollup_backfills_pre_existing_ledger_on_first_run() {
        let store = MemoryStore::default();
        let mut a = record("m1");
        a.user_id = "alice".into();
        a.created_at_epoch_secs = 400;
        store.ledger_add(&a).await.unwrap();
        store.usage_rollup_advance(400 + 3 * 20 * 60).await.unwrap();
        let usage = store.usage_by_user(None, None, 0, i64::MAX).await.unwrap();
        assert_eq!(
            usage
                .iter()
                .find(|r| r.user_id == "alice")
                .map(|r| r.total_tokens),
            Some(8),
            "pre-existing ledger rows are rolled on the first run, not orphaned below the watermark"
        );
    }

    #[tokio::test]
    async fn recompute_from_pruned_ledger_never_shrinks_a_bucket() {
        let store = MemoryStore::with_ledger_cap(2);
        for ts in [100, 110] {
            let mut r = record("m1");
            r.user_id = "alice".into();
            r.created_at_epoch_secs = ts;
            store.ledger_add(&r).await.unwrap();
        }
        store.usage_rollup_advance(1_260).await.unwrap();

        let mut b = record("m1");
        b.user_id = "bob".into();
        b.created_at_epoch_secs = 1_250;
        store.ledger_add(&b).await.unwrap();
        store.usage_rollup_advance(1_260).await.unwrap();
        let usage = store.usage_by_user(None, None, 0, i64::MAX).await.unwrap();
        assert_eq!(
            usage
                .iter()
                .find(|r| r.user_id == "alice")
                .map(|r| r.total_tokens),
            Some(16),
            "a recompute over the pruned ledger keeps the more complete bucket"
        );
    }

    async fn exercise_erase(store: &dyn Store, ns: &str) {
        let (t1, t2) = (format!("t1{ns}"), format!("t2{ns}"));
        let (u1, u2) = (format!("u1{ns}"), format!("u2{ns}"));
        let (t1, t2, u1, u2) = (t1.as_str(), t2.as_str(), u1.as_str(), u2.as_str());
        let rec = |req: &str, user: &str, tenant: &str| crate::ContentRecord {
            created_at_epoch_secs: 100,
            request_id: req.into(),
            ak: "ak".into(),
            user_id: user.into(),
            tenant: tenant.into(),
            kind: "prompt".into(),
            content: "hello".into(),
            sealed: false,
            expires_at_epoch_secs: 0,
        };
        let (r1, r2, r3) = (format!("r1{ns}"), format!("r2{ns}"), format!("r3{ns}"));
        store.content_add(&rec(&r1, u1, t1)).await.unwrap();
        store.content_add(&rec(&r2, u1, t2)).await.unwrap();
        store.content_add(&rec(&r3, u2, t1)).await.unwrap();
        let job = store.batch_create("ak", t1, "m", 1).await.unwrap();
        store
            .batch_push_result(
                &job.id,
                BatchItemResult {
                    index: 0,
                    ok: true,
                    message: "generated for u1".into(),
                    total_tokens: 3,
                    user: u1.into(),
                },
            )
            .await
            .unwrap();

        let audit = || AdminAudit {
            created_at_epoch_secs: 20,
            actor: "global".into(),
            scope: "global".into(),
            action: "content_erase".into(),
            target: u1.into(),
            summary: String::new(),
            source_ip: "10.0.0.1".into(),
        };
        assert_eq!(
            store
                .content_erase_user(Some(t1), u1, audit())
                .await
                .unwrap(),
            2,
            "tenant-scoped erase: the content row plus the batch-result message"
        );
        assert_eq!(store.content_for(&r2).await.unwrap().len(), 1);
        let results = store.batch_get(&job.id).await.unwrap().unwrap().results;
        assert_eq!(results[0].message, "", "generated output erased");
        assert_eq!(results[0].user, u1, "attribution survives for billing");
        assert_eq!(
            store.content_erase_user(None, u1, audit()).await.unwrap(),
            1,
            "global erase removes the remaining tenant's row"
        );
        assert_eq!(
            store.content_for(&r3).await.unwrap().len(),
            1,
            "other users' content is untouched"
        );
        let trail = store.admin_audit_list(1_000).await.unwrap();
        let erases: Vec<_> = trail
            .iter()
            .filter(|e| e.action == "content_erase" && e.target == u1)
            .collect();
        assert_eq!(erases.len(), 2, "each erasure committed its audit entry");
        let summaries: Vec<&str> = erases.iter().map(|e| e.summary.as_str()).collect();
        assert_eq!(
            summaries,
            ["rows=1", "rows=2"],
            "newest first; each summary carries its erased count"
        );
    }

    /// Local backends only: item-persisting stores re-read rows at dispatch.
    async fn exercise_erasure_markers(store: &dyn Store, ns: &str) {
        let (t1, u1, u2) = (format!("t1{ns}"), format!("u1{ns}"), format!("u2{ns}"));
        let now = crate::epoch_millis();
        // wide margin: a 5ms one flaked under parallel-suite load
        assert!(
            store
                .user_erased_since(&t1, &u1, now - 60_000)
                .await
                .unwrap(),
            "marker visible to a batch started before the erasure"
        );
        assert!(
            !store.user_erased_since(&t1, &u1, now + 5).await.unwrap(),
            "a batch started after the erasure is unaffected"
        );
        assert!(
            !store.user_erased_since(&t1, &u2, now - 5).await.unwrap(),
            "other users unaffected"
        );
    }

    #[tokio::test]
    async fn memory_erase_roundtrip() {
        let store = MemoryStore::default();
        exercise_erase(&store, "").await;
        exercise_erasure_markers(&store, "").await;
    }

    #[tokio::test]
    async fn pg_rollup_and_erase_roundtrip() {
        let Ok(url) = std::env::var("GW_TEST_PG_URL") else {
            return;
        };
        // a private database: the watermark is global per db, so shared-db leftovers would shadow the fixtures
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db = format!("gwtest_{nonce}");
        let admin = sqlx::PgPool::connect(&url).await.expect("pg admin");
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {db}")))
            .execute(&admin)
            .await
            .expect("create test db");
        let own_url = match url.rfind('/') {
            Some(i) => format!("{}/{db}", &url[..i]),
            None => url.clone(),
        };
        let store = PostgresStore::connect(&own_url).await.expect("pg connect");
        let ns = format!("-{nonce}");
        exercise_rollup(&store, &ns).await;
        exercise_erase(&store, &ns).await;

        let (t1, erika) = (format!("t1{ns}"), format!("erika{ns}"));
        let items = vec![
            gw_models::BatchItem {
                messages: vec![gw_models::ChatMsg::text("user", "erase me")],
                user: erika.clone(),
            },
            gw_models::BatchItem {
                messages: vec![gw_models::ChatMsg::text("user", "keep me")],
                user: format!("other{ns}"),
            },
        ];
        let pending = store
            .batch_enqueue("ak", &t1, "gpt-4o", &items)
            .await
            .unwrap();
        let audit2 = AdminAudit {
            created_at_epoch_secs: 21,
            actor: "global".into(),
            scope: "global".into(),
            action: "content_erase".into(),
            target: erika.clone(),
            summary: String::new(),
            source_ip: "10.0.0.1".into(),
        };
        assert_eq!(
            store
                .content_erase_user(Some(&t1), &erika, audit2)
                .await
                .unwrap(),
            1,
            "the pending item's prompt is blanked"
        );
        let loaded = store.batch_load_items(&pending.id).await.unwrap();
        assert!(
            loaded[0].messages.is_empty(),
            "queued prompt erased before any drainer ran"
        );
        assert_eq!(
            loaded[1].messages[0].content, "keep me",
            "other users' queued items untouched"
        );
        let fresh = store
            .batch_item_snapshot(&pending.id, 0)
            .await
            .unwrap()
            .expect("item row still present");
        assert!(fresh.messages.is_empty(), "pre-dispatch snapshot sees it");
    }

    #[tokio::test]
    async fn sqlite_erase_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(dir.path().join("erase.db").to_str().unwrap())
            .await
            .unwrap();
        exercise_erase(&store, "").await;
        exercise_erasure_markers(&store, "").await;
    }

    #[tokio::test]
    async fn sqlite_file_ids_never_recycle_after_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(dir.path().join("files.db").to_str().unwrap())
            .await
            .unwrap();
        let a = store.file_put("t1", "batch", "one".into()).await.unwrap();
        assert!(store.file_delete(&a.id, "t1").await.unwrap());
        let b = store.file_put("t1", "batch", "two".into()).await.unwrap();
        assert_ne!(
            a.id, b.id,
            "a deleted file's id must not be handed to a new upload"
        );
        assert!(
            !store.file_delete(&b.id, "t2").await.unwrap(),
            "a foreign tenant's delete is a guarded no-op"
        );
        assert!(store.file_get(&b.id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn batch_result_is_first_writer_wins() {
        let store = MemoryStore::default();
        let job = store.batch_create("ak", "default", "m", 1).await.unwrap();
        let push = |ok, msg: &str| {
            store.batch_push_result(
                &job.id,
                BatchItemResult {
                    index: 0,
                    ok,
                    message: msg.into(),
                    total_tokens: 1,
                    user: String::new(),
                },
            )
        };
        push(true, "owner").await.unwrap();
        push(false, "stale").await.unwrap();
        let got = store.batch_get(&job.id).await.unwrap().unwrap();
        assert_eq!(got.results.len(), 1);
        assert!(got.results[0].ok);
        assert_eq!(got.results[0].message, "owner", "first write wins");
    }

    #[tokio::test]
    async fn batch_result_rejected_after_terminal_and_finalize_derives() {
        let store = MemoryStore::default();
        let res = |index, ok| BatchItemResult {
            index,
            ok,
            message: String::new(),
            total_tokens: 0,
            user: String::new(),
        };
        let job = store.batch_create("ak", "default", "m", 2).await.unwrap();
        store
            .batch_push_result(&job.id, res(0, true))
            .await
            .unwrap();

        assert_eq!(
            store.batch_finalize(&job.id, 0).await.unwrap(),
            Some(BatchStatus::Failed),
            "missing item 1 → Failed"
        );
        store
            .batch_push_result(&job.id, res(1, true))
            .await
            .unwrap();
        let got = store.batch_get(&job.id).await.unwrap().unwrap();
        assert_eq!(got.results.len(), 1, "no result added to a terminal batch");
        assert_eq!(got.status, BatchStatus::Failed);

        let ok = store.batch_create("ak", "default", "m", 1).await.unwrap();
        store.batch_push_result(&ok.id, res(0, true)).await.unwrap();
        assert_eq!(
            store.batch_finalize(&ok.id, 0).await.unwrap(),
            Some(BatchStatus::Completed)
        );
    }

    #[tokio::test]
    async fn ledger_retention_caps_both_stores() {
        let mem = MemoryStore::with_ledger_cap(2);
        for m in ["a", "b"] {
            mem.ledger_add(&record(m)).await.unwrap();
        }
        mem.ledger_add(&record("unrolled")).await.unwrap();
        let (total, _) = mem.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(total, 3, "unrolled rows are spared from the cap");
        mem.usage_rollup_advance(1_000 + 3 * 60).await.unwrap();
        let mut c = record("c");
        c.created_at_epoch_secs = 1_200;
        mem.ledger_add(&c).await.unwrap();
        let (total, page) = mem.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(total, 2, "rolled rows prune to the cap");
        assert_eq!(page[0].model, "unrolled");
        let usage = mem
            .usage_by_user(Some("default"), None, 0, i64::MAX)
            .await
            .unwrap();
        let total_requests: i64 = usage.iter().map(|r| r.requests).sum();
        assert_eq!(
            total_requests, 4,
            "pruned rows stay billed via their buckets"
        );

        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open_with_cap(dir.path().join("r.db").to_str().unwrap(), 2)
            .await
            .unwrap();
        for i in 0..=LEDGER_PRUNE_EVERY {
            store.ledger_add(&record(&format!("m{i}"))).await.unwrap();
        }
        let (total, _) = store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(
            total,
            LEDGER_PRUNE_EVERY + 1,
            "nothing prunes before the rollup folds the rows"
        );
        store.usage_rollup_advance(1_000 + 3 * 60).await.unwrap();
        for i in 0..LEDGER_PRUNE_EVERY {
            store.ledger_add(&record(&format!("n{i}"))).await.unwrap();
        }
        let (total, _) = store.ledger_snapshot(usize::MAX).await.unwrap();
        assert!(
            total <= 2 + LEDGER_PRUNE_EVERY,
            "prune cycle enforces the cap on rolled rows (got {total})"
        );
    }

    #[tokio::test]
    async fn ledger_prune_survives_an_out_of_order_young_row() {
        let store = MemoryStore::with_ledger_cap(2);
        let at = |ts: i64| {
            let mut r = record("m");
            r.created_at_epoch_secs = ts;
            r
        };
        for ts in [1_000, 1_000] {
            store.ledger_add(&at(ts)).await.unwrap();
        }
        store.usage_rollup_advance(1_180).await.unwrap();
        store.ledger_add(&at(5_000)).await.unwrap();
        store.ledger_add(&at(1_010)).await.unwrap();
        store.ledger_add(&at(1_010)).await.unwrap();
        let (total, page) = store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(
            total, 2,
            "a young row wedged ahead of rolled rows must not defeat the cap"
        );
        assert!(
            page.iter().any(|r| r.created_at_epoch_secs == 5_000),
            "the unrolled young row is spared"
        );
    }

    #[tokio::test]
    async fn sqlite_concurrent_creates_get_unique_ids() {
        let dir = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(
            SqliteStore::open(dir.path().join("c.db").to_str().unwrap())
                .await
                .unwrap(),
        );
        let mut handles = Vec::new();
        for i in 0..10 {
            let s = store.clone();
            handles.push(tokio::spawn(async move {
                s.file_put("default", "batch", format!("content-{i}"))
                    .await
                    .unwrap()
                    .id
            }));
        }
        let mut ids = Vec::new();
        for h in handles {
            ids.push(h.await.unwrap());
        }
        ids.sort();
        ids.dedup();
        assert_eq!(
            ids.len(),
            10,
            "concurrent puts must all succeed with distinct ids"
        );
    }

    #[tokio::test]
    async fn sqlite_open_sweeps_orphaned_jobs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("o.db");
        let path = path.to_str().unwrap();
        {
            let store = SqliteStore::open(path).await.unwrap();
            let job = store.batch_create("ak", "default", "m", 1).await.unwrap();
            store
                .batch_set_status(&job.id, BatchStatus::Running)
                .await
                .unwrap();
        }
        let store = SqliteStore::open(path).await.unwrap();
        let job = store.batch_get("batch-1").await.unwrap().unwrap();
        assert_eq!(job.status, BatchStatus::Failed);
    }

    #[tokio::test]
    async fn sqlite_store_roundtrip_and_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let path = path.to_str().unwrap();
        {
            let store = SqliteStore::open(path).await.unwrap();
            exercise(&store).await;
        }
        let store = SqliteStore::open(path).await.unwrap();
        assert_eq!(store.ledger_snapshot(usize::MAX).await.unwrap().0, 2);
        let usage = store.ledger_usage(Some("default")).await.unwrap();
        assert_eq!(usage.len(), 2);
        assert_eq!(usage[0].requests, 1);
        assert_eq!(usage[0].vendor_cost_micros, 7);
        assert!(store.ledger_usage(Some("ghost")).await.unwrap().is_empty());
        let job = store.batch_get("batch-1").await.unwrap().unwrap();
        assert_eq!(job.status, BatchStatus::Completed);
    }

    #[tokio::test]
    async fn postgres_store_roundtrip() {
        let Ok(url) = std::env::var("GW_TEST_PG_URL") else {
            return;
        };
        let store = PostgresStore::connect(&url).await.expect("pg connect");
        store.ledger_add(&record("gpt-4o")).await.unwrap();
        let (total, page) = store.ledger_snapshot(5).await.unwrap();
        assert!(total >= 1);
        assert_eq!(page.last().unwrap().model, "gpt-4o");
        let usage = store.ledger_usage(Some("default")).await.unwrap();
        assert!(usage.iter().any(|u| u.model == "gpt-4o" && u.requests >= 1));

        let f = store
            .file_put("default", "batch", "hello pg".into())
            .await
            .unwrap();
        assert!(f.id.starts_with("file-"));
        let got = store.file_get(&f.id).await.unwrap().unwrap();
        assert_eq!(got.content, "hello pg");

        let b = store
            .batch_create("ak-t", "default", "gpt-4o", 2)
            .await
            .unwrap();
        assert!(b.id.starts_with("batch-"));
        store
            .batch_set_status(&b.id, BatchStatus::Running)
            .await
            .unwrap();
        store
            .batch_push_result(
                &b.id,
                BatchItemResult {
                    index: 0,
                    ok: true,
                    message: "ok".into(),
                    total_tokens: 5,
                    user: String::new(),
                },
            )
            .await
            .unwrap();
        let got = store.batch_get(&b.id).await.unwrap().unwrap();
        assert_eq!(got.status, BatchStatus::Running);
        assert_eq!(got.results.len(), 1);
        store
            .batch_push_result(
                &b.id,
                BatchItemResult {
                    index: 0,
                    ok: false,
                    message: "stale".into(),
                    total_tokens: 0,
                    user: String::new(),
                },
            )
            .await
            .unwrap();
        let got = store.batch_get(&b.id).await.unwrap().unwrap();
        assert_eq!(got.results.len(), 1);
        assert!(
            got.results[0].ok && got.results[0].message == "ok",
            "first write wins"
        );
        assert_eq!(
            store.batch_finalize(&b.id, 0).await.unwrap(),
            Some(BatchStatus::Failed)
        );
        store
            .batch_push_result(
                &b.id,
                BatchItemResult {
                    index: 1,
                    ok: true,
                    message: "late".into(),
                    total_tokens: 0,
                    user: String::new(),
                },
            )
            .await
            .unwrap();
        let got = store.batch_get(&b.id).await.unwrap().unwrap();
        assert_eq!(
            got.results.len(),
            1,
            "no result added to a terminal PG batch"
        );
        assert_eq!(got.status, BatchStatus::Failed);

        assert!(store.distributed_batches());
        let qmsgs = vec![
            gw_models::BatchItem {
                messages: vec![gw_models::ChatMsg::text("user", "one")],
                user: "u-one".into(),
            },
            gw_models::BatchItem {
                messages: vec![gw_models::ChatMsg::text("user", "two")],
                user: "u-two".into(),
            },
        ];
        let qjob = store
            .batch_enqueue("ak-b", "default", "gpt-4o", &qmsgs)
            .await
            .unwrap();
        assert_eq!(qjob.total, 2);
        let loaded = store.batch_load_items(&qjob.id).await.unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[1].messages[0].content, "two");
        assert_eq!(
            loaded[1].user, "u-two",
            "per-item user round-trips through pg"
        );
        loop {
            let (c, _claim) = store
                .batch_claim_pending(120)
                .await
                .unwrap()
                .expect("claim");
            let mine = c.id == qjob.id;
            if mine {
                assert_eq!(c.total, 2);
            }
            store
                .batch_set_status(&c.id, BatchStatus::Completed)
                .await
                .unwrap();
            if mine {
                break;
            }
        }
        assert!(
            store.batch_load_items(&qjob.id).await.unwrap().is_empty(),
            "terminal status pruned the batch inputs"
        );

        let fjob = store
            .batch_enqueue("ak-f", "default", "gpt-4o", &qmsgs)
            .await
            .unwrap();
        let t1 = loop {
            let (c, t) = store
                .batch_claim_pending(120)
                .await
                .unwrap()
                .expect("claim");
            if c.id == fjob.id {
                break t;
            }
            store
                .batch_set_status(&c.id, BatchStatus::Completed)
                .await
                .unwrap();
        };
        assert!(
            store.batch_touch(&fjob.id, t1).await.unwrap(),
            "holder heartbeats"
        );
        let t2 = loop {
            let (c, t) = store
                .batch_claim_pending(0)
                .await
                .unwrap()
                .expect("reclaim");
            if c.id == fjob.id {
                break t;
            }
            store
                .batch_set_status(&c.id, BatchStatus::Completed)
                .await
                .unwrap();
        };
        assert_ne!(t1, t2, "reclaim bumps the fence token");
        assert!(
            !store.batch_touch(&fjob.id, t1).await.unwrap(),
            "stale token loses the claim"
        );
        assert!(
            store.batch_touch(&fjob.id, t2).await.unwrap(),
            "new token holds the claim"
        );
        store
            .batch_set_status(&fjob.id, BatchStatus::Completed)
            .await
            .unwrap();

        while let Some((c, _claim)) = store.batch_claim_pending(0).await.unwrap() {
            assert_ne!(c.id, qjob.id, "completed batch must stay terminal");
            store
                .batch_set_status(&c.id, BatchStatus::Completed)
                .await
                .unwrap();
        }

        let store = std::sync::Arc::new(store);
        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = store.clone();
            handles.push(tokio::spawn(async move {
                s.file_put("default", "x", "y".into()).await
            }));
        }
        let mut ids = std::collections::HashSet::new();
        for h in handles {
            assert!(ids.insert(h.await.unwrap().unwrap().id));
        }
    }

    #[tokio::test]
    async fn postgres_finalize_sees_result_committed_during_lock_wait() {
        let Ok(url) = std::env::var("GW_TEST_PG_URL") else {
            return;
        };
        let store = std::sync::Arc::new(PostgresStore::connect(&url).await.expect("pg connect"));
        let job = store
            .batch_create("ak-race", "default", "m", 1)
            .await
            .unwrap();
        store
            .batch_set_status(&job.id, BatchStatus::Running)
            .await
            .unwrap();

        let mut txa = store.pool.begin().await.unwrap();
        sqlx::query(
            "INSERT INTO batch_results (batch_id, idx, ok, message, total_tokens)
             SELECT $1, 0, true, '', 1
             WHERE EXISTS (SELECT 1 FROM batches
                           WHERE id = $1 AND status NOT IN ('completed', 'failed') FOR UPDATE)
             ON CONFLICT (batch_id, idx) DO NOTHING",
        )
        .bind(&job.id)
        .execute(&mut *txa)
        .await
        .unwrap();

        let s2 = store.clone();
        let jid = job.id.clone();
        let fin = tokio::spawn(async move { s2.batch_finalize(&jid, 0).await });
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        txa.commit().await.unwrap();
        assert_eq!(
            fin.await.unwrap().unwrap(),
            Some(BatchStatus::Completed),
            "finalize must see the result committed during its lock wait"
        );
        assert_eq!(
            store.batch_get(&job.id).await.unwrap().unwrap().status,
            BatchStatus::Completed
        );

        sqlx::query("DELETE FROM batch_results WHERE batch_id = $1")
            .bind(&job.id)
            .execute(&store.pool)
            .await
            .ok();
        sqlx::query("DELETE FROM batches WHERE id = $1")
            .bind(&job.id)
            .execute(&store.pool)
            .await
            .ok();
    }
}
