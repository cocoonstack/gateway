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

/// One billing entry (recorded locally only; no reporting upstream).
#[derive(Debug, Clone, serde::Serialize)]
pub struct BillingRecord {
    pub ak: String,
    pub product: String,
    pub tenant: String,
    /// Public model the caller requested.
    pub model: String,
    /// Model that actually served (differs from `model` after a quota fallback).
    pub served_model: String,
    pub protocol: String,
    pub account: String,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
    pub cost_micros: i64,
    /// What the serving account's vendor charged us (zero = untracked);
    /// margin = cost_micros - vendor_cost_micros.
    #[serde(default)]
    pub vendor_cost_micros: i64,
    /// PTU spilled over to a paygo account (a failover occurred).
    #[serde(default)]
    pub ptu_spillover: bool,
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
}

/// One offline batch job.
#[derive(Debug, Clone, serde::Serialize)]
pub struct BatchJob {
    pub id: String,
    pub ak: String,
    pub model: String,
    pub status: BatchStatus,
    pub total: usize,
    pub results: Vec<BatchItemResult>,
}

/// A stored file (batch input JSONL, etc.).
#[derive(Debug, Clone, serde::Serialize)]
pub struct StoredFile {
    pub id: String,
    pub bytes: usize,
    pub purpose: String,
    /// raw content (not serialized in metadata views; fetched via /content).
    #[serde(skip)]
    pub content: String,
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

#[async_trait::async_trait]
pub trait Store: Send + Sync + std::fmt::Debug {
    async fn ledger_add(&self, r: BillingRecord) -> GResult<()>;
    /// Total record count plus the most recent `limit` records in
    /// chronological order. Count and page may be read without a shared
    /// transaction; the ledger is append-only, so the skew is at most a
    /// just-appended record and self-heals on the next read.
    async fn ledger_snapshot(&self, limit: usize) -> GResult<(usize, Vec<BillingRecord>)>;
    /// Usage rolled up by (tenant, requested model), sorted; the SQL backends
    /// aggregate server-side instead of paging the whole ledger out.
    async fn ledger_usage(&self, tenant: Option<&str>) -> GResult<Vec<UsageRow>>;

    /// Store `content` under a fresh id; returns the file metadata.
    async fn file_put(&self, purpose: &str, content: String) -> GResult<StoredFile>;
    async fn file_get(&self, id: &str) -> GResult<Option<StoredFile>>;

    async fn batch_create(&self, ak: &str, model: &str, total: usize) -> GResult<BatchJob>;
    async fn batch_get(&self, id: &str) -> GResult<Option<BatchJob>>;
    async fn batch_set_status(&self, id: &str, status: BatchStatus) -> GResult<()>;
    async fn batch_push_result(&self, id: &str, result: BatchItemResult) -> GResult<()>;

    /// Whether this backend runs a fleet work queue (any instance drains
    /// submitted batches). Local backends execute on the submitting instance.
    fn distributed_batches(&self) -> bool {
        false
    }
    /// Atomically enqueue a batch and its items for the fleet drain loop, so a
    /// partial save never leaves a claimable job with missing items. Local
    /// stores fall back to a plain create (items execute in-process).
    async fn batch_enqueue(
        &self,
        ak: &str,
        model: &str,
        items: &[Vec<gw_models::ChatMsg>],
    ) -> GResult<BatchJob> {
        self.batch_create(ak, model, items.len()).await
    }
    /// Load a batch's input items for execution.
    async fn batch_load_items(&self, _id: &str) -> GResult<Vec<Vec<gw_models::ChatMsg>>> {
        Ok(Vec::new())
    }
    /// Claim one pending batch (requeuing any running batch whose executor went
    /// stale first). `None` = nothing to run. Only the distributed backend claims.
    async fn batch_claim_pending(&self, _stale_secs: i64) -> GResult<Option<BatchJob>> {
        Ok(None)
    }
    /// Heartbeat a running batch so its executor isn't judged stale.
    async fn batch_touch(&self, _id: &str) -> GResult<()> {
        Ok(())
    }
}

/// In-process store: append-only ledger, DashMap-backed files and batches.
#[derive(Debug, Default)]
pub struct MemoryStore {
    records: Mutex<Vec<BillingRecord>>,
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
    async fn ledger_add(&self, r: BillingRecord) -> GResult<()> {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        records.push(r);
        if self.ledger_max_rows > 0 && records.len() > self.ledger_max_rows {
            let excess = records.len() - self.ledger_max_rows;
            records.drain(..excess);
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
            e.requests += 1;
            e.prompt_tokens += r.prompt_tokens;
            e.completion_tokens += r.completion_tokens;
            e.total_tokens += r.total_tokens;
            e.cost_micros += r.cost_micros;
            e.vendor_cost_micros += r.vendor_cost_micros;
        }
        Ok(rollup.into_values().collect())
    }

    async fn file_put(&self, purpose: &str, content: String) -> GResult<StoredFile> {
        let id = format!(
            "file-local-{}",
            self.seq.fetch_add(1, Ordering::Relaxed) + 1
        );
        let f = StoredFile {
            id: id.clone(),
            bytes: content.len(),
            purpose: purpose.to_owned(),
            content,
        };
        self.files.insert(id, f.clone());
        Ok(f)
    }

    async fn file_get(&self, id: &str) -> GResult<Option<StoredFile>> {
        Ok(self.files.get(id).map(|f| f.value().clone()))
    }

    async fn batch_create(&self, ak: &str, model: &str, total: usize) -> GResult<BatchJob> {
        let id = format!(
            "batch-local-{}",
            self.seq.fetch_add(1, Ordering::Relaxed) + 1
        );
        let job = BatchJob {
            id: id.clone(),
            ak: ak.to_owned(),
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
        if let Some(mut j) = self.jobs.get_mut(id) {
            j.results.push(result);
        }
        Ok(())
    }
}

/// Positional row → record mapping shared by the SQL backends (their SELECT
/// column order is identical).
fn row_to_billing<'r, R>(row: &'r R) -> BillingRecord
where
    R: sqlx::Row,
    usize: sqlx::ColumnIndex<R>,
    String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    bool: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    BillingRecord {
        ak: row.get(0),
        product: row.get(1),
        tenant: row.get(2),
        model: row.get(3),
        served_model: row.get(4),
        protocol: row.get(5),
        account: row.get(6),
        prompt_tokens: row.get(7),
        completion_tokens: row.get(8),
        total_tokens: row.get(9),
        cost_micros: row.get(10),
        vendor_cost_micros: row.get(11),
        ptu_spillover: row.get(12),
    }
}

fn usage_row<'r, R>(row: &'r R) -> UsageRow
where
    R: sqlx::Row,
    usize: sqlx::ColumnIndex<R>,
    String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    UsageRow {
        tenant: row.get(0),
        model: row.get(1),
        requests: row.get(2),
        prompt_tokens: row.get(3),
        completion_tokens: row.get(4),
        total_tokens: row.get(5),
        cost_micros: row.get(6),
        vendor_cost_micros: row.get(7),
    }
}

/// SQLite-backed store (sqlx, WAL). One database file holds the billing
/// ledger, uploaded files, and batch jobs; ids are derived from rowids so they
/// stay unique across restarts.
#[derive(Debug)]
pub struct SqliteStore {
    pool: sqlx::SqlitePool,
    ledger_max_rows: u64,
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
                ptu_spillover INTEGER NOT NULL DEFAULT 0)",
            "CREATE TABLE IF NOT EXISTS files (
                n INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL,
                purpose TEXT NOT NULL, bytes INTEGER NOT NULL, content TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS batches (
                n INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL,
                ak TEXT NOT NULL, model TEXT NOT NULL,
                status TEXT NOT NULL, total INTEGER NOT NULL)",
            "CREATE TABLE IF NOT EXISTS batch_results (
                batch_id TEXT NOT NULL, idx INTEGER NOT NULL, ok INTEGER NOT NULL,
                message TEXT NOT NULL, total_tokens INTEGER NOT NULL)",
        ] {
            sqlx::query(ddl)
                .execute(&pool)
                .await
                .map_err(|e| crate::sqlx_err("create schema", e))?;
        }
        // Pre-existing databases lack these columns; the ALTER fails with
        // "duplicate column name" on every later boot, so that error is ignored.
        for ddl in [
            "ALTER TABLE billing ADD COLUMN tenant TEXT NOT NULL DEFAULT 'default'",
            "ALTER TABLE billing ADD COLUMN served_model TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE billing ADD COLUMN vendor_cost_micros INTEGER NOT NULL DEFAULT 0",
        ] {
            if let Err(e) = sqlx::query(ddl).execute(&pool).await
                && !e.to_string().contains("duplicate column name")
            {
                return Err(crate::sqlx_err("migrate billing schema", e));
            }
        }
        // a dead process's pending/running jobs can never progress on a
        // single-instance store — fail them instead of letting clients poll forever
        sqlx::query("UPDATE batches SET status = 'failed' WHERE status IN ('pending', 'running')")
            .execute(&pool)
            .await
            .map_err(|e| crate::sqlx_err("sweep orphaned batches", e))?;
        Ok(Self {
            pool,
            ledger_max_rows,
        })
    }
}

#[async_trait::async_trait]
impl Store for SqliteStore {
    async fn ledger_add(&self, r: BillingRecord) -> GResult<()> {
        sqlx::query(
            "INSERT INTO billing (ak, product, tenant, model, served_model, protocol, account,
             prompt_tokens, completion_tokens, total_tokens, cost_micros,
             vendor_cost_micros, ptu_spillover)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
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
        .execute(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("insert billing record", e))?;
        if self.ledger_max_rows > 0 {
            sqlx::query("DELETE FROM billing WHERE n <= (SELECT MAX(n) FROM billing) - ?")
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
        let mut rows = sqlx::query(
            "SELECT ak, product, tenant, model, served_model, protocol, account,
             prompt_tokens, completion_tokens, total_tokens, cost_micros,
             vendor_cost_micros, ptu_spillover
             FROM billing ORDER BY n DESC LIMIT ?",
        )
        .bind(limit.min(i64::MAX as usize) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("read billing records", e))?;
        rows.reverse();
        Ok((total as usize, rows.iter().map(row_to_billing).collect()))
    }

    async fn ledger_usage(&self, tenant: Option<&str>) -> GResult<Vec<UsageRow>> {
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

    async fn file_put(&self, purpose: &str, content: String) -> GResult<StoredFile> {
        let bytes = content.len();
        // SQLite serializes writers, so the MAX(n)+1 subselect is atomic with the insert
        let id: String = sqlx::query_scalar(
            "INSERT INTO files (id, purpose, bytes, content)
             VALUES ('file-' || (SELECT COALESCE(MAX(n), 0) + 1 FROM files), ?, ?, ?)
             RETURNING id",
        )
        .bind(purpose)
        .bind(bytes as i64)
        .bind(&content)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("insert file", e))?;
        Ok(StoredFile {
            id,
            bytes,
            purpose: purpose.to_owned(),
            content,
        })
    }

    async fn file_get(&self, id: &str) -> GResult<Option<StoredFile>> {
        let row = sqlx::query("SELECT id, purpose, bytes, content FROM files WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("read file", e))?;
        Ok(row.map(|row| StoredFile {
            id: row.get(0),
            purpose: row.get(1),
            bytes: row.get::<i64, _>(2) as usize,
            content: row.get(3),
        }))
    }

    async fn batch_create(&self, ak: &str, model: &str, total: usize) -> GResult<BatchJob> {
        let id: String = sqlx::query_scalar(
            "INSERT INTO batches (id, ak, model, status, total)
             VALUES ('batch-' || (SELECT COALESCE(MAX(n), 0) + 1 FROM batches), ?, ?, ?, ?)
             RETURNING id",
        )
        .bind(ak)
        .bind(model)
        .bind(BatchStatus::Pending.as_str())
        .bind(total as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("insert batch", e))?;
        Ok(BatchJob {
            id,
            ak: ak.to_owned(),
            model: model.to_owned(),
            status: BatchStatus::Pending,
            total,
            results: Vec::new(),
        })
    }

    async fn batch_get(&self, id: &str) -> GResult<Option<BatchJob>> {
        let row = sqlx::query("SELECT id, ak, model, status, total FROM batches WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("read batch", e))?;
        let Some(row) = row else { return Ok(None) };
        let results = sqlx::query(
            "SELECT idx, ok, message, total_tokens FROM batch_results
             WHERE batch_id = ? ORDER BY idx",
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("read batch results", e))?;
        let status_text: String = row.get(3);
        Ok(Some(BatchJob {
            id: row.get(0),
            ak: row.get(1),
            model: row.get(2),
            status: BatchStatus::parse(&status_text).unwrap_or(BatchStatus::Failed),
            total: row.get::<i64, _>(4) as usize,
            results: results
                .iter()
                .map(|r| BatchItemResult {
                    index: r.get::<i64, _>(0) as usize,
                    ok: r.get(1),
                    message: r.get(2),
                    total_tokens: r.get(3),
                })
                .collect(),
        }))
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
        sqlx::query(
            "INSERT INTO batch_results (batch_id, idx, ok, message, total_tokens)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(result.index as i64)
        .bind(result.ok)
        .bind(&result.message)
        .bind(result.total_tokens)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("insert batch result", e))?;
        Ok(())
    }
}

/// Postgres-backed store for a multi-instance fleet: ledger, files, and
/// batches are shared, so any instance can serve reads for work submitted on
/// another. Unlike [`SqliteStore`] there is no orphan sweep on open — a
/// starting instance must not fail batches another live instance is still
/// executing (a distributed executor is the M9 follow-up).
#[derive(Debug)]
pub struct PostgresStore {
    pool: sqlx::PgPool,
    ledger_max_rows: u64,
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
        for ddl in [
            "CREATE TABLE IF NOT EXISTS billing (
                n BIGSERIAL PRIMARY KEY,
                ak TEXT NOT NULL, product TEXT NOT NULL,
                tenant TEXT NOT NULL DEFAULT 'default', model TEXT NOT NULL,
                served_model TEXT NOT NULL DEFAULT '',
                protocol TEXT NOT NULL, account TEXT NOT NULL,
                prompt_tokens BIGINT NOT NULL, completion_tokens BIGINT NOT NULL,
                total_tokens BIGINT NOT NULL, cost_micros BIGINT NOT NULL,
                vendor_cost_micros BIGINT NOT NULL DEFAULT 0,
                ptu_spillover BOOLEAN NOT NULL DEFAULT FALSE)",
            "CREATE TABLE IF NOT EXISTS files (
                n BIGSERIAL PRIMARY KEY, id TEXT UNIQUE NOT NULL,
                purpose TEXT NOT NULL, bytes BIGINT NOT NULL, content TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS batches (
                n BIGSERIAL PRIMARY KEY, id TEXT UNIQUE NOT NULL,
                ak TEXT NOT NULL, model TEXT NOT NULL,
                status TEXT NOT NULL, total BIGINT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS batch_results (
                batch_id TEXT NOT NULL, idx BIGINT NOT NULL, ok BOOLEAN NOT NULL,
                message TEXT NOT NULL, total_tokens BIGINT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS batch_items (
                batch_id TEXT NOT NULL, idx BIGINT NOT NULL, messages TEXT NOT NULL,
                PRIMARY KEY (batch_id, idx))",
            "ALTER TABLE batches ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ",
            // dedup any (batch_id, idx) rows the pre-fix plain-INSERT could have
            // left, so the unique index below builds on an already-upgraded fleet
            "DELETE FROM batch_results a USING batch_results b
             WHERE a.ctid < b.ctid AND a.batch_id = b.batch_id AND a.idx = b.idx",
            "CREATE UNIQUE INDEX IF NOT EXISTS batch_results_uidx
             ON batch_results (batch_id, idx)",
        ] {
            sqlx::query(ddl)
                .execute(&pool)
                .await
                .map_err(|e| crate::sqlx_err("create postgres schema", e))?;
        }
        sqlx::query(
            "ALTER TABLE billing ADD COLUMN IF NOT EXISTS
             vendor_cost_micros BIGINT NOT NULL DEFAULT 0",
        )
        .execute(&pool)
        .await
        .map_err(|e| crate::sqlx_err("migrate postgres billing schema", e))?;
        Ok(Self {
            pool,
            ledger_max_rows,
        })
    }
}

#[async_trait::async_trait]
impl Store for PostgresStore {
    async fn ledger_add(&self, r: BillingRecord) -> GResult<()> {
        sqlx::query(
            "INSERT INTO billing (ak, product, tenant, model, served_model, protocol, account,
             prompt_tokens, completion_tokens, total_tokens, cost_micros,
             vendor_cost_micros, ptu_spillover)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
        )
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
        .execute(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("insert billing record", e))?;
        if self.ledger_max_rows > 0 {
            sqlx::query("DELETE FROM billing WHERE n <= (SELECT MAX(n) FROM billing) - $1")
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
        let mut rows = sqlx::query(
            "SELECT ak, product, tenant, model, served_model, protocol, account,
             prompt_tokens, completion_tokens, total_tokens, cost_micros,
             vendor_cost_micros, ptu_spillover
             FROM billing ORDER BY n DESC LIMIT $1",
        )
        .bind(limit.min(i64::MAX as usize) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("read billing records", e))?;
        rows.reverse();
        Ok((total as usize, rows.iter().map(row_to_billing).collect()))
    }

    async fn ledger_usage(&self, tenant: Option<&str>) -> GResult<Vec<UsageRow>> {
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

    async fn file_put(&self, purpose: &str, content: String) -> GResult<StoredFile> {
        let bytes = content.len();
        // concurrent PG writers race a MAX(n)+1 subselect; consume the
        // sequence explicitly so id and n share one atomic value
        let id: String = sqlx::query_scalar(
            "INSERT INTO files (n, id, purpose, bytes, content)
             SELECT v, 'file-' || v, $1, $2, $3
             FROM nextval(pg_get_serial_sequence('files', 'n')) AS v
             RETURNING id",
        )
        .bind(purpose)
        .bind(bytes as i64)
        .bind(&content)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("insert file", e))?;
        Ok(StoredFile {
            id,
            bytes,
            purpose: purpose.to_owned(),
            content,
        })
    }

    async fn file_get(&self, id: &str) -> GResult<Option<StoredFile>> {
        let row = sqlx::query("SELECT id, purpose, bytes, content FROM files WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("read file", e))?;
        Ok(row.map(|row| StoredFile {
            id: row.get(0),
            purpose: row.get(1),
            bytes: row.get::<i64, _>(2) as usize,
            content: row.get(3),
        }))
    }

    async fn batch_create(&self, ak: &str, model: &str, total: usize) -> GResult<BatchJob> {
        let id: String = sqlx::query_scalar(
            "INSERT INTO batches (n, id, ak, model, status, total)
             SELECT v, 'batch-' || v, $1, $2, $3, $4
             FROM nextval(pg_get_serial_sequence('batches', 'n')) AS v
             RETURNING id",
        )
        .bind(ak)
        .bind(model)
        .bind(BatchStatus::Pending.as_str())
        .bind(total as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("insert batch", e))?;
        Ok(BatchJob {
            id,
            ak: ak.to_owned(),
            model: model.to_owned(),
            status: BatchStatus::Pending,
            total,
            results: Vec::new(),
        })
    }

    async fn batch_get(&self, id: &str) -> GResult<Option<BatchJob>> {
        let row = sqlx::query("SELECT id, ak, model, status, total FROM batches WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("read batch", e))?;
        let Some(row) = row else { return Ok(None) };
        let results = sqlx::query(
            "SELECT idx, ok, message, total_tokens FROM batch_results
             WHERE batch_id = $1 ORDER BY idx",
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("read batch results", e))?;
        let status_text: String = row.get(3);
        Ok(Some(BatchJob {
            id: row.get(0),
            ak: row.get(1),
            model: row.get(2),
            status: BatchStatus::parse(&status_text).unwrap_or(BatchStatus::Failed),
            total: row.get::<i64, _>(4) as usize,
            results: results
                .iter()
                .map(|r| BatchItemResult {
                    index: r.get::<i64, _>(0) as usize,
                    ok: r.get(1),
                    message: r.get(2),
                    total_tokens: r.get(3),
                })
                .collect(),
        }))
    }

    async fn batch_set_status(&self, id: &str, status: BatchStatus) -> GResult<()> {
        sqlx::query("UPDATE batches SET status = $1 WHERE id = $2")
            .bind(status.as_str())
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("update batch status", e))?;
        Ok(())
    }

    async fn batch_push_result(&self, id: &str, result: BatchItemResult) -> GResult<()> {
        sqlx::query(
            "INSERT INTO batch_results (batch_id, idx, ok, message, total_tokens)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (batch_id, idx) DO UPDATE SET
               ok = EXCLUDED.ok, message = EXCLUDED.message,
               total_tokens = EXCLUDED.total_tokens",
        )
        .bind(id)
        .bind(result.index as i64)
        .bind(result.ok)
        .bind(&result.message)
        .bind(result.total_tokens)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("insert batch result", e))?;
        Ok(())
    }

    fn distributed_batches(&self) -> bool {
        true
    }

    async fn batch_enqueue(
        &self,
        ak: &str,
        model: &str,
        items: &[Vec<gw_models::ChatMsg>],
    ) -> GResult<BatchJob> {
        // one transaction: the batch becomes claimable (pending) only once all
        // its items are committed, so a partial save can't orphan a job.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin batch enqueue", e))?;
        let id: String = sqlx::query_scalar(
            "INSERT INTO batches (n, id, ak, model, status, total)
             SELECT v, 'batch-' || v, $1, $2, $3, $4
             FROM nextval(pg_get_serial_sequence('batches', 'n')) AS v
             RETURNING id",
        )
        .bind(ak)
        .bind(model)
        .bind(BatchStatus::Pending.as_str())
        .bind(items.len() as i64)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| crate::sqlx_err("insert batch", e))?;
        for (idx, msgs) in items.iter().enumerate() {
            let json = serde_json::to_string(msgs).unwrap_or_else(|_| "[]".into());
            sqlx::query("INSERT INTO batch_items (batch_id, idx, messages) VALUES ($1, $2, $3)")
                .bind(&id)
                .bind(idx as i64)
                .bind(json)
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
            model: model.to_owned(),
            status: BatchStatus::Pending,
            total: items.len(),
            results: Vec::new(),
        })
    }

    async fn batch_load_items(&self, id: &str) -> GResult<Vec<Vec<gw_models::ChatMsg>>> {
        let rows = sqlx::query("SELECT messages FROM batch_items WHERE batch_id = $1 ORDER BY idx")
            .bind(id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("load batch items", e))?;
        Ok(rows
            .iter()
            .map(|r| serde_json::from_str(r.get::<&str, _>(0)).unwrap_or_default())
            .collect())
    }

    async fn batch_claim_pending(&self, stale_secs: i64) -> GResult<Option<BatchJob>> {
        // requeue batches whose executor stopped heartbeating, then claim one
        // pending batch — SKIP LOCKED so concurrent instances never collide.
        sqlx::query(
            "UPDATE batches SET status = 'pending', claimed_at = NULL
             WHERE status = 'running'
               AND claimed_at < now() - make_interval(secs => $1)",
        )
        .bind(stale_secs as f64)
        .execute(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("requeue stale batches", e))?;
        let row = sqlx::query(
            "UPDATE batches SET status = 'running', claimed_at = now()
             WHERE id = (SELECT id FROM batches WHERE status = 'pending'
                         ORDER BY n FOR UPDATE SKIP LOCKED LIMIT 1)
             RETURNING id, ak, model, total",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("claim batch", e))?;
        Ok(row.map(|r| BatchJob {
            id: r.get(0),
            ak: r.get(1),
            model: r.get(2),
            status: BatchStatus::Running,
            total: r.get::<i64, _>(3) as usize,
            results: Vec::new(),
        }))
    }

    async fn batch_touch(&self, id: &str) -> GResult<()> {
        sqlx::query("UPDATE batches SET claimed_at = now() WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("heartbeat batch", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(model: &str) -> BillingRecord {
        BillingRecord {
            ak: "ak-t".into(),
            product: "p".into(),
            tenant: "default".into(),
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
        }
    }

    async fn exercise(store: &dyn Store) {
        store.ledger_add(record("m1")).await.unwrap();
        store.ledger_add(record("m2")).await.unwrap();
        let (total, snap) = store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(total, 2);
        assert_eq!(snap[0].model, "m1");
        assert_eq!(snap[1].total_tokens, 8);
        let (total, page) = store.ledger_snapshot(1).await.unwrap();
        assert_eq!(total, 2);
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].model, "m2");

        let f = store
            .file_put("batch", "line1\nline2".into())
            .await
            .unwrap();
        assert_eq!(f.bytes, 11);
        let got = store.file_get(&f.id).await.unwrap().unwrap();
        assert_eq!(got.content, "line1\nline2");
        assert!(store.file_get("file-nope").await.unwrap().is_none());

        let job = store.batch_create("ak-t", "m1", 2).await.unwrap();
        assert_eq!(job.status, BatchStatus::Pending);
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

    #[tokio::test]
    async fn ledger_retention_caps_both_stores() {
        let mem = MemoryStore::with_ledger_cap(2);
        for m in ["a", "b", "c"] {
            mem.ledger_add(record(m)).await.unwrap();
        }
        let (total, page) = mem.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(total, 2);
        assert_eq!(page[0].model, "b"); // oldest pruned first

        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open_with_cap(dir.path().join("r.db").to_str().unwrap(), 2)
            .await
            .unwrap();
        for m in ["a", "b", "c"] {
            store.ledger_add(record(m)).await.unwrap();
        }
        let (total, page) = store.ledger_snapshot(usize::MAX).await.unwrap();
        assert_eq!(total, 2);
        assert_eq!(page[0].model, "b");
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
                s.file_put("batch", format!("content-{i}"))
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
            let job = store.batch_create("ak", "m", 1).await.unwrap();
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

    /// Set GW_TEST_PG_URL (e.g. postgres://postgres:gwtest@127.0.0.1:15432/gw)
    /// to run this.
    #[tokio::test]
    async fn postgres_store_roundtrip() {
        let Ok(url) = std::env::var("GW_TEST_PG_URL") else {
            return;
        };
        let store = PostgresStore::connect(&url).await.expect("pg connect");
        store.ledger_add(record("gpt-4o")).await.unwrap();
        let (total, page) = store.ledger_snapshot(5).await.unwrap();
        assert!(total >= 1);
        assert_eq!(page.last().unwrap().model, "gpt-4o");
        let usage = store.ledger_usage(Some("default")).await.unwrap();
        assert!(usage.iter().any(|u| u.model == "gpt-4o" && u.requests >= 1));

        let f = store.file_put("batch", "hello pg".into()).await.unwrap();
        assert!(f.id.starts_with("file-"));
        let got = store.file_get(&f.id).await.unwrap().unwrap();
        assert_eq!(got.content, "hello pg");

        let b = store.batch_create("ak-t", "gpt-4o", 2).await.unwrap();
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
                },
            )
            .await
            .unwrap();
        let got = store.batch_get(&b.id).await.unwrap().unwrap();
        assert_eq!(got.status, BatchStatus::Running);
        assert_eq!(got.results.len(), 1);

        // distributed queue: save items, claim (loop to ours), requeue-on-stale
        assert!(store.distributed_batches());
        let qmsgs = vec![
            vec![gw_models::ChatMsg::text("user", "one")],
            vec![gw_models::ChatMsg::text("user", "two")],
        ];
        let qjob = store.batch_enqueue("ak-b", "gpt-4o", &qmsgs).await.unwrap();
        assert_eq!(qjob.total, 2);
        loop {
            let c = store
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
        let loaded = store.batch_load_items(&qjob.id).await.unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[1][0].content, "two");
        // our completed batch is never re-claimed even at zero staleness
        // (other runs' leftover pending batches may still surface — drain them)
        while let Some(c) = store.batch_claim_pending(0).await.unwrap() {
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
            handles.push(tokio::spawn(
                async move { s.file_put("x", "y".into()).await },
            ));
        }
        let mut ids = std::collections::HashSet::new();
        for h in handles {
            assert!(ids.insert(h.await.unwrap().unwrap().id));
        }
    }
}
