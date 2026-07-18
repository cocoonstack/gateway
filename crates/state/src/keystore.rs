//! Dynamic access-key storage behind a trait: the in-memory table serves a
//! single node; Postgres shares one key set across a fleet, fronted by a
//! short-TTL cache so the hot auth path stays off the network.

use std::time::Duration;

use async_trait::async_trait;
use gw_models::GResult;
use sqlx::Row;

use crate::{AkInfo, KeyPatch, KeySource};

/// How long an instance may serve a stale key view: the fleet-wide bound on
/// create/revoke propagation (a local write invalidates its own cache at once).
const AUTH_CACHE_TTL: Duration = Duration::from_secs(2);
/// Bounded so unknown-key probing can't grow the negative cache unboundedly.
const AUTH_CACHE_MAX: u64 = 100_000;

/// The live key table. Mirrors [`crate::AkAuth`]'s semantics: config keys are
/// re-applied on reload, admin keys survive it, and config ownership is sticky
/// against admin overwrites.
#[async_trait]
pub trait KeyStore: Send + Sync + std::fmt::Debug {
    /// Resolve a presented key; `None` = unknown, revoked, or (for a networked
    /// backend) unreachable — auth fails closed.
    async fn authenticate(&self, ak: &str) -> Option<AkInfo>;
    /// Insert or replace a key. Config ownership is sticky: an admin write to
    /// a config-declared key updates values but keeps it revocable by config.
    async fn put(&self, info: AkInfo, source: KeySource) -> GResult<()>;
    /// Update quota/lifecycle fields in place; `Ok(None)` if the key doesn't exist.
    async fn patch(&self, ak: &str, patch: &KeyPatch) -> GResult<Option<AkInfo>>;
    /// Remove a key regardless of source; whether it existed.
    async fn revoke(&self, ak: &str) -> GResult<bool>;
    /// A page of keys, sorted by ak, optionally confined to one tenant. The
    /// tenant filter applies before paging, so a scoped page is never emptied
    /// by a later filter; `offset`/`limit` bound the scan.
    async fn list(&self, tenant: Option<&str>, offset: usize, limit: usize)
    -> GResult<Vec<AkInfo>>;
    /// Re-apply the config file's key set, leaving admin-created keys untouched.
    async fn reload_config_keys(&self, keys: &[gw_config::AkConf]) -> GResult<()>;
}

/// Fleet-shared key table in Postgres. Reads go through a short-TTL cache
/// (positive and negative entries); writes invalidate the local cache
/// immediately and reach other instances within [`AUTH_CACHE_TTL`].
#[derive(Debug)]
pub struct PostgresKeyStore {
    pool: sqlx::PgPool,
    cache: moka::sync::Cache<String, Option<AkInfo>>,
    /// Bumped on every write: an authenticate that overlapped a write skips
    /// caching its (possibly pre-write) fetch instead of poisoning the cache.
    /// A write landing between the re-check and the insert still self-heals
    /// within [`AUTH_CACHE_TTL`].
    write_epoch: std::sync::atomic::AtomicU64,
}

impl PostgresKeyStore {
    pub async fn connect(url: &str) -> GResult<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .map_err(|e| crate::sqlx_err("connect postgres key store", e))?;
        let mut schema = pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin access_keys schema", e))?;
        sqlx::query(crate::PG_SCHEMA_LOCK_SQL)
            .execute(&mut *schema)
            .await
            .map_err(|e| crate::sqlx_err("lock access_keys schema", e))?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS access_keys (
                ak TEXT PRIMARY KEY,
                product TEXT NOT NULL,
                tenant TEXT NOT NULL DEFAULT 'default',
                qps DOUBLE PRECISION NOT NULL,
                daily_token_quota BIGINT NOT NULL,
                tokens_per_minute BIGINT,
                expires_at_epoch_secs BIGINT,
                banned BOOLEAN NOT NULL DEFAULT FALSE,
                model_quotas TEXT NOT NULL DEFAULT '{}',
                owner TEXT,
                source TEXT NOT NULL DEFAULT 'admin')",
        )
        .execute(&mut *schema)
        .await
        .map_err(|e| crate::sqlx_err("create access_keys schema", e))?;
        sqlx::query("ALTER TABLE access_keys ADD COLUMN IF NOT EXISTS owner TEXT")
            .execute(&mut *schema)
            .await
            .map_err(|e| crate::sqlx_err("migrate access_keys owner", e))?;
        sqlx::query(
            "ALTER TABLE access_keys ADD COLUMN IF NOT EXISTS suspended_until_epoch_secs BIGINT",
        )
        .execute(&mut *schema)
        .await
        .map_err(|e| crate::sqlx_err("migrate access_keys suspension", e))?;
        schema
            .commit()
            .await
            .map_err(|e| crate::sqlx_err("commit access_keys schema", e))?;
        Ok(Self {
            pool,
            cache: moka::sync::Cache::builder()
                .max_capacity(AUTH_CACHE_MAX)
                .time_to_live(AUTH_CACHE_TTL)
                .build(),
            write_epoch: std::sync::atomic::AtomicU64::new(0),
        })
    }

    fn note_write(&self, ak: &str) {
        self.write_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        self.cache.invalidate(ak);
    }

    async fn fetch(&self, ak: &str) -> Result<Option<AkInfo>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT ak, product, tenant, qps, daily_token_quota, tokens_per_minute,
             expires_at_epoch_secs, banned, model_quotas, owner, suspended_until_epoch_secs FROM access_keys WHERE ak = $1",
        )
        .bind(ak)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(row_to_info))
    }
}

#[async_trait]
impl KeyStore for PostgresKeyStore {
    async fn authenticate(&self, ak: &str) -> Option<AkInfo> {
        if let Some(cached) = self.cache.get(ak) {
            return cached;
        }
        let epoch = self.write_epoch.load(std::sync::atomic::Ordering::Acquire);
        match self.fetch(ak).await {
            Ok(info) => {
                if self.write_epoch.load(std::sync::atomic::Ordering::Acquire) == epoch {
                    self.cache.insert(ak.to_owned(), info.clone());
                }
                info
            }
            Err(e) => {
                // fail closed: a store outage must not admit unknown keys
                tracing::warn!(error = %e, "key store unreachable; auth fails closed");
                None
            }
        }
    }

    async fn put(&self, info: AkInfo, source: KeySource) -> GResult<()> {
        upsert(&self.pool, &info, source)
            .await
            .map_err(|e| crate::sqlx_err("upsert access key", e))?;
        self.note_write(&info.ak);
        Ok(())
    }

    async fn patch(&self, ak: &str, patch: &KeyPatch) -> GResult<Option<AkInfo>> {
        // FOR UPDATE: concurrent patches serialize instead of clobbering fields
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin patch", e))?;
        let row = sqlx::query(
            "SELECT ak, product, tenant, qps, daily_token_quota, tokens_per_minute,
             expires_at_epoch_secs, banned, model_quotas, owner, suspended_until_epoch_secs FROM access_keys
             WHERE ak = $1 FOR UPDATE",
        )
        .bind(ak)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| crate::sqlx_err("read key for patch", e))?;
        let Some(row) = row else { return Ok(None) };
        let mut info = row_to_info(&row);
        info.apply_patch(patch);
        sqlx::query(
            "UPDATE access_keys SET qps = $2, daily_token_quota = $3,
             tokens_per_minute = $4, expires_at_epoch_secs = $5, banned = $6,
             suspended_until_epoch_secs = $7
             WHERE ak = $1",
        )
        .bind(ak)
        .bind(info.qps)
        .bind(info.daily_token_quota)
        .bind(info.tokens_per_minute)
        .bind(info.expires_at_epoch_secs)
        .bind(info.banned)
        .bind(info.suspended_until_epoch_secs)
        .execute(&mut *tx)
        .await
        .map_err(|e| crate::sqlx_err("apply patch", e))?;
        tx.commit()
            .await
            .map_err(|e| crate::sqlx_err("commit patch", e))?;
        self.note_write(ak);
        Ok(Some(info))
    }

    async fn revoke(&self, ak: &str) -> GResult<bool> {
        let n = sqlx::query("DELETE FROM access_keys WHERE ak = $1")
            .bind(ak)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("revoke key", e))?
            .rows_affected();
        self.note_write(ak);
        Ok(n > 0)
    }

    async fn list(
        &self,
        tenant: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> GResult<Vec<AkInfo>> {
        let rows = sqlx::query(
            "SELECT ak, product, tenant, qps, daily_token_quota, tokens_per_minute,
             expires_at_epoch_secs, banned, model_quotas, owner, suspended_until_epoch_secs FROM access_keys
             WHERE ($1::text IS NULL OR tenant = $1) ORDER BY ak LIMIT $2 OFFSET $3",
        )
        .bind(tenant)
        .bind(limit.min(i64::MAX as usize) as i64)
        .bind(offset.min(i64::MAX as usize) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("list keys", e))?;
        Ok(rows.iter().map(row_to_info).collect())
    }

    async fn reload_config_keys(&self, keys: &[gw_config::AkConf]) -> GResult<()> {
        let wanted: Vec<&str> = keys.iter().map(|k| k.ak.as_str()).collect();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin reload", e))?;
        sqlx::query("DELETE FROM access_keys WHERE source = 'config' AND NOT (ak = ANY($1))")
            .bind(&wanted)
            .execute(&mut *tx)
            .await
            .map_err(|e| crate::sqlx_err("drop stale config keys", e))?;
        for k in keys {
            upsert(&mut *tx, &AkInfo::from(k), KeySource::Config)
                .await
                .map_err(|e| crate::sqlx_err("re-apply config key", e))?;
        }
        tx.commit()
            .await
            .map_err(|e| crate::sqlx_err("commit reload", e))?;
        self.write_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        self.cache.invalidate_all();
        Ok(())
    }
}

/// The one INSERT..ON CONFLICT for the key table. The sticky-source CASE is a
/// no-op when `source` is Config, so the reload path shares it unchanged.
/// `suspended_until_epoch_secs` is deliberately absent: an upsert never clears
/// a runtime suspension — only a patch touches it.
async fn upsert(
    exec: impl sqlx::PgExecutor<'_>,
    info: &AkInfo,
    source: KeySource,
) -> Result<(), sqlx::Error> {
    let quotas = serde_json::to_string(&*info.model_quotas).unwrap_or_else(|_| "{}".into());
    sqlx::query(
        "INSERT INTO access_keys (ak, product, tenant, qps, daily_token_quota,
         tokens_per_minute, expires_at_epoch_secs, banned, model_quotas, owner, source)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
         ON CONFLICT (ak) DO UPDATE SET
           product = EXCLUDED.product, tenant = EXCLUDED.tenant,
           qps = EXCLUDED.qps, daily_token_quota = EXCLUDED.daily_token_quota,
           tokens_per_minute = EXCLUDED.tokens_per_minute,
           expires_at_epoch_secs = EXCLUDED.expires_at_epoch_secs,
           banned = EXCLUDED.banned, model_quotas = EXCLUDED.model_quotas,
           owner = EXCLUDED.owner,
           source = CASE WHEN access_keys.source = 'config' AND EXCLUDED.source = 'admin'
                         THEN 'config' ELSE EXCLUDED.source END",
    )
    .bind(&info.ak)
    .bind(&info.product)
    .bind(&info.tenant)
    .bind(info.qps)
    .bind(info.daily_token_quota)
    .bind(info.tokens_per_minute)
    .bind(info.expires_at_epoch_secs)
    .bind(info.banned)
    .bind(&quotas)
    .bind(&info.owner)
    .bind(source_str(source))
    .execute(exec)
    .await
    .map(|_| ())
}

fn row_to_info(row: &sqlx::postgres::PgRow) -> AkInfo {
    AkInfo {
        ak: row.get(0),
        product: row.get(1),
        tenant: row.get(2),
        qps: row.get(3),
        daily_token_quota: row.get(4),
        tokens_per_minute: row.get(5),
        expires_at_epoch_secs: row.get(6),
        banned: row.get(7),
        model_quotas: std::sync::Arc::new(
            serde_json::from_str(row.get::<&str, _>(8)).unwrap_or_default(),
        ),
        owner: row.get(9),
        suspended_until_epoch_secs: row.get(10),
    }
}

fn source_str(s: KeySource) -> &'static str {
    match s {
        KeySource::Config => "config",
        KeySource::Admin => "admin",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(ak: &str, qps: f64) -> AkInfo {
        AkInfo {
            ak: ak.into(),
            product: "p".into(),
            tenant: "default".into(),
            owner: None,
            qps,
            daily_token_quota: 10,
            tokens_per_minute: None,
            expires_at_epoch_secs: None,
            banned: false,
            suspended_until_epoch_secs: None,
            model_quotas: Default::default(),
        }
    }

    #[tokio::test]
    async fn postgres_keystore_semantics_mirror_memory() {
        let Ok(url) = std::env::var("GW_TEST_PG_URL") else {
            return;
        };
        let ks = PostgresKeyStore::connect(&url).await.expect("pg connect");
        sqlx::query("TRUNCATE access_keys")
            .execute(&ks.pool)
            .await
            .unwrap();
        ks.cache.invalidate_all();

        ks.put(info("pk-a", 1.0), KeySource::Admin).await.unwrap();
        assert_eq!(ks.authenticate("pk-a").await.unwrap().qps, 1.0);
        assert!(ks.authenticate("pk-nope").await.is_none());
        assert!(ks.revoke("pk-a").await.unwrap());
        assert!(
            ks.authenticate("pk-a").await.is_none(),
            "local write invalidates the cache immediately"
        );
        assert!(!ks.revoke("pk-a").await.unwrap());

        ks.put(info("pk-b", 2.0), KeySource::Admin).await.unwrap();
        let p = ks
            .patch(
                "pk-b",
                &KeyPatch {
                    qps: Some(9.0),
                    tokens_per_minute: Some(Some(5)),
                    banned: Some(true),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!((p.qps, p.tokens_per_minute, p.banned), (9.0, Some(5), true));
        let p = ks
            .patch(
                "pk-b",
                &KeyPatch {
                    tokens_per_minute: Some(None),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(p.tokens_per_minute, None);
        assert!(p.banned, "untouched fields survive a partial patch");

        let cfg = gw_config::GatewayConfig::from_yaml(
            "listen: {host: h, port: 1}\naccess_keys: [{ak: pk-cfg, product: p, qps: 1, daily_token_quota: 5}]",
        )
        .unwrap();
        ks.reload_config_keys(&cfg.access_keys).await.unwrap();
        assert!(ks.authenticate("pk-cfg").await.is_some());
        assert!(
            ks.authenticate("pk-b").await.is_some(),
            "admin key survives reload"
        );
        ks.put(info("pk-cfg", 3.0), KeySource::Admin).await.unwrap();
        let empty =
            gw_config::GatewayConfig::from_yaml("listen: {host: h, port: 1}\naccess_keys: []")
                .unwrap();
        ks.reload_config_keys(&empty.access_keys).await.unwrap();
        assert!(
            ks.authenticate("pk-cfg").await.is_none(),
            "config ownership is sticky against admin overwrite"
        );
        assert!(ks.authenticate("pk-b").await.is_some());
    }
}
