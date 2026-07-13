//! Durable gateway records: billing ledger, uploaded files, batch jobs.
//!
//! Two backends behind the [`Store`] trait: [`MemoryStore`] (the default — and
//! what tests use) and [`SqliteStore`] (sqlx; selected by `storage.sqlite_path`
//! in the config, survives restarts).

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use gw_models::{GResult, GatewayError};
use sqlx::Row;

/// One billing entry (recorded locally only; no reporting upstream).
#[derive(Debug, Clone, serde::Serialize)]
pub struct BillingRecord {
    pub ak: String,
    pub product: String,
    pub tenant: String,
    pub model: String,
    pub protocol: String,
    pub account: String,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
    pub cost_micros: i64,
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

#[async_trait::async_trait]
pub trait Store: Send + Sync + std::fmt::Debug {
    async fn ledger_add(&self, r: BillingRecord) -> GResult<()>;
    /// Total record count plus the most recent `limit` records in
    /// chronological order. Count and page may be read without a shared
    /// transaction; the ledger is append-only, so the skew is at most a
    /// just-appended record and self-heals on the next read.
    async fn ledger_snapshot(&self, limit: usize) -> GResult<(usize, Vec<BillingRecord>)>;

    /// Store `content` under a fresh id; returns the file metadata.
    async fn file_put(&self, purpose: &str, content: String) -> GResult<StoredFile>;
    async fn file_get(&self, id: &str) -> GResult<Option<StoredFile>>;

    async fn batch_create(&self, ak: &str, model: &str, total: usize) -> GResult<BatchJob>;
    async fn batch_get(&self, id: &str) -> GResult<Option<BatchJob>>;
    async fn batch_set_status(&self, id: &str, status: BatchStatus) -> GResult<()>;
    async fn batch_push_result(&self, id: &str, result: BatchItemResult) -> GResult<()>;
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
            .map_err(|e| store_err("open sqlite store", e))?;
        for ddl in [
            "CREATE TABLE IF NOT EXISTS billing (
                n INTEGER PRIMARY KEY AUTOINCREMENT,
                ak TEXT NOT NULL, product TEXT NOT NULL,
                tenant TEXT NOT NULL DEFAULT 'default', model TEXT NOT NULL,
                protocol TEXT NOT NULL, account TEXT NOT NULL,
                prompt_tokens INTEGER NOT NULL, completion_tokens INTEGER NOT NULL,
                total_tokens INTEGER NOT NULL, cost_micros INTEGER NOT NULL,
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
                .map_err(|e| store_err("create schema", e))?;
        }
        // Pre-tenant databases lack the column; the ALTER fails once with
        // "duplicate column name" on every later boot, so that error is ignored.
        if let Err(e) =
            sqlx::query("ALTER TABLE billing ADD COLUMN tenant TEXT NOT NULL DEFAULT 'default'")
                .execute(&pool)
                .await
            && !e.to_string().contains("duplicate column name")
        {
            return Err(store_err("migrate billing schema", e));
        }
        // Jobs left pending/running by a dead process can never progress
        // (single-instance store) — surface them as failed instead of letting
        // clients poll a job that will never finish.
        sqlx::query("UPDATE batches SET status = 'failed' WHERE status IN ('pending', 'running')")
            .execute(&pool)
            .await
            .map_err(|e| store_err("sweep orphaned batches", e))?;
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
            "INSERT INTO billing (ak, product, tenant, model, protocol, account, prompt_tokens,
             completion_tokens, total_tokens, cost_micros, ptu_spillover)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&r.ak)
        .bind(&r.product)
        .bind(&r.tenant)
        .bind(&r.model)
        .bind(&r.protocol)
        .bind(&r.account)
        .bind(r.prompt_tokens)
        .bind(r.completion_tokens)
        .bind(r.total_tokens)
        .bind(r.cost_micros)
        .bind(r.ptu_spillover)
        .execute(&self.pool)
        .await
        .map_err(|e| store_err("insert billing record", e))?;
        if self.ledger_max_rows > 0 {
            sqlx::query("DELETE FROM billing WHERE n <= (SELECT MAX(n) FROM billing) - ?")
                .bind(self.ledger_max_rows as i64)
                .execute(&self.pool)
                .await
                .map_err(|e| store_err("prune billing records", e))?;
        }
        Ok(())
    }

    async fn ledger_snapshot(&self, limit: usize) -> GResult<(usize, Vec<BillingRecord>)> {
        let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM billing")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| store_err("count billing records", e))?;
        let mut rows = sqlx::query(
            "SELECT ak, product, tenant, model, protocol, account, prompt_tokens,
             completion_tokens, total_tokens, cost_micros, ptu_spillover
             FROM billing ORDER BY n DESC LIMIT ?",
        )
        .bind(limit.min(i64::MAX as usize) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| store_err("read billing records", e))?;
        rows.reverse();
        Ok((
            total as usize,
            rows.iter()
                .map(|row| BillingRecord {
                    ak: row.get(0),
                    product: row.get(1),
                    tenant: row.get(2),
                    model: row.get(3),
                    protocol: row.get(4),
                    account: row.get(5),
                    prompt_tokens: row.get(6),
                    completion_tokens: row.get(7),
                    total_tokens: row.get(8),
                    cost_micros: row.get(9),
                    ptu_spillover: row.get(10),
                })
                .collect(),
        ))
    }

    async fn file_put(&self, purpose: &str, content: String) -> GResult<StoredFile> {
        let bytes = content.len();
        // id computed inside the single INSERT: SQLite serializes writers, so the
        // subselect is atomic with the insert (no placeholder row, no second step).
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
        .map_err(|e| store_err("insert file", e))?;
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
            .map_err(|e| store_err("read file", e))?;
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
        .map_err(|e| store_err("insert batch", e))?;
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
            .map_err(|e| store_err("read batch", e))?;
        let Some(row) = row else { return Ok(None) };
        let results = sqlx::query(
            "SELECT idx, ok, message, total_tokens FROM batch_results
             WHERE batch_id = ? ORDER BY idx",
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| store_err("read batch results", e))?;
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
            .map_err(|e| store_err("update batch status", e))?;
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
        .map_err(|e| store_err("insert batch result", e))?;
        Ok(())
    }
}

fn store_err(what: &str, e: sqlx::Error) -> GatewayError {
    GatewayError::internal(what).with_source(e)
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
            protocol: "openai-chat".into(),
            account: "acc".into(),
            prompt_tokens: 3,
            completion_tokens: 5,
            total_tokens: 8,
            cost_micros: 42,
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
        // pagination: latest record only, total still reports everything
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
        // a new process can never resume the dead process's job
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
        // reopen: records survive the process (unlike MemoryStore)
        let store = SqliteStore::open(path).await.unwrap();
        assert_eq!(store.ledger_snapshot(usize::MAX).await.unwrap().0, 2);
        let job = store.batch_get("batch-1").await.unwrap().unwrap();
        assert_eq!(job.status, BatchStatus::Completed);
    }
}
