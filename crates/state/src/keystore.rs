//! Dynamic access-key storage behind a trait: the in-memory table serves a
//! single node; Postgres shares one key set across a fleet, fronted by a
//! cache each write invalidates fleet-wide, so the hot auth path stays off the network.

use std::sync::Arc;

use async_trait::async_trait;
use gw_models::GResult;
use sqlx::Row;

use crate::fleet_cache::{FLUSH_ALL, FleetCache};
use crate::{AkInfo, KeyPatch, KeySource};

/// Bounded so unknown-key probing can't grow the negative cache unboundedly.
const AUTH_CACHE_MAX: u64 = 1_000_000;
const KEYSTORE_MAX_CONNECTIONS: u32 = 5;
const KEY_CHANNEL: &str = "gw_keys";

/// The live key table with [`crate::AkAuth`]'s semantics: config keys re-apply
/// on reload, admin keys survive it, config ownership is sticky. Every key
/// argument is the key's id ([`gw_config::access_key_id`]), never the raw key.
#[async_trait]
pub trait KeyStore: Send + Sync + std::fmt::Debug {
    /// Resolve a key by id; `None` = unknown, revoked, or (for a networked
    /// backend) unreachable — auth fails closed.
    async fn get(&self, ak_id: &str) -> Option<Arc<AkInfo>>;
    /// Insert or replace a key. Config ownership is sticky: an admin write to
    /// a config-declared key updates values but keeps it revocable by config.
    async fn put(&self, info: AkInfo, source: KeySource) -> GResult<()>;
    /// Update quota/lifecycle fields in place; `Ok(None)` if the key doesn't exist.
    async fn patch(&self, ak_id: &str, patch: &KeyPatch) -> GResult<Option<AkInfo>>;
    /// Remove a key regardless of source; whether it existed.
    async fn revoke(&self, ak_id: &str) -> GResult<bool>;
    /// A page of keys, sorted by ak_id, optionally confined to one tenant and one
    /// owner. The filters apply before paging, so a scoped page is never emptied
    /// by a later filter; `offset`/`limit` bound the scan.
    async fn list(
        &self,
        tenant: Option<&str>,
        owner: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> GResult<Vec<AkInfo>>;
    /// Re-apply the config file's key set, leaving admin-created keys untouched.
    async fn reload_config_keys(&self, keys: &[gw_config::AkConf]) -> GResult<()>;
}

/// Fleet-shared key table in Postgres: cached reads (positive and negative),
/// each write NOTIFYs every instance to drop that key.
#[derive(Debug)]
pub struct PostgresKeyStore {
    pool: sqlx::PgPool,
    cache: FleetCache<Option<Arc<AkInfo>>>,
}

impl PostgresKeyStore {
    pub async fn connect(url: &str, max_connections: u32) -> GResult<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(if max_connections == 0 {
                KEYSTORE_MAX_CONNECTIONS
            } else {
                max_connections
            })
            .connect(url)
            .await
            .map_err(|e| crate::sqlx_err("connect postgres key store", e))?;
        crate::setup_schema(
            &pool,
            "access_keys",
            &[
                "CREATE TABLE IF NOT EXISTS access_keys (
                ak_id TEXT PRIMARY KEY,
                product TEXT NOT NULL,
                tenant TEXT NOT NULL DEFAULT 'default',
                qps DOUBLE PRECISION NOT NULL,
                daily_token_quota BIGINT NOT NULL,
                tokens_per_minute BIGINT,
                expires_at_epoch_secs BIGINT,
                banned BOOLEAN NOT NULL DEFAULT FALSE,
                model_quotas TEXT NOT NULL DEFAULT '{}',
                owner TEXT,
                source TEXT NOT NULL DEFAULT 'admin',
                mcp_servers TEXT NOT NULL DEFAULT '[]',
                mcp_tools TEXT NOT NULL DEFAULT '{}')",
                "DO $$ BEGIN
                IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema()
                           AND table_name = 'access_keys' AND column_name = 'ak') THEN
                  ALTER TABLE access_keys RENAME COLUMN ak TO ak_id;
                END IF;
                END $$",
                "UPDATE access_keys
                 SET ak_id = 'sha256:' || left(encode(sha256(convert_to(ak_id, 'UTF8')), 'hex'), 32)
                 WHERE ak_id !~ '^sha256:[0-9a-f]{32}$'",
                "ALTER TABLE access_keys ADD COLUMN IF NOT EXISTS owner TEXT",
                "ALTER TABLE access_keys ADD COLUMN IF NOT EXISTS suspended_until_epoch_secs BIGINT",
                "ALTER TABLE access_keys ADD COLUMN IF NOT EXISTS mcp_servers TEXT NOT NULL DEFAULT '[]'",
                "ALTER TABLE access_keys ADD COLUMN IF NOT EXISTS mcp_tools TEXT NOT NULL DEFAULT '{}'",
                "CREATE INDEX IF NOT EXISTS access_keys_owner_idx ON access_keys (owner, tenant)",
            ],
        )
        .await?;
        let cache = FleetCache::listen(url, KEY_CHANNEL, AUTH_CACHE_MAX).await?;
        Ok(Self { pool, cache })
    }

    /// The key table's pool, shared with the other low-traffic admin tables.
    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    async fn fetch(&self, ak_id: &str) -> Result<Option<Arc<AkInfo>>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT ak_id, product, tenant, qps, daily_token_quota, tokens_per_minute,
             expires_at_epoch_secs, banned, model_quotas, owner, suspended_until_epoch_secs, mcp_servers, mcp_tools FROM access_keys WHERE ak_id = $1",
        )
        .bind(ak_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(|row| Arc::new(row_to_info(row))))
    }
}

#[async_trait]
impl KeyStore for PostgresKeyStore {
    async fn get(&self, ak_id: &str) -> Option<Arc<AkInfo>> {
        match self.cache.get_with(ak_id, self.fetch(ak_id)).await {
            Ok(info) => info,
            Err(e) => {
                // fail closed: a store outage must not admit unknown keys
                tracing::warn!(error = %e, "key store unreachable; auth fails closed");
                None
            }
        }
    }

    async fn put(&self, info: AkInfo, source: KeySource) -> GResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin key upsert", e))?;
        upsert(&mut *tx, &info, source)
            .await
            .map_err(|e| crate::sqlx_err("upsert access key", e))?;
        self.cache
            .commit(tx, &info.ak_id)
            .await
            .map_err(|e| crate::sqlx_err("commit key upsert", e))
    }

    async fn patch(&self, ak_id: &str, patch: &KeyPatch) -> GResult<Option<AkInfo>> {
        // concurrent patches serialize under FOR UPDATE instead of clobbering fields
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin patch", e))?;
        let row = sqlx::query(
            "SELECT ak_id, product, tenant, qps, daily_token_quota, tokens_per_minute,
             expires_at_epoch_secs, banned, model_quotas, owner, suspended_until_epoch_secs, mcp_servers, mcp_tools FROM access_keys
             WHERE ak_id = $1 FOR UPDATE",
        )
        .bind(ak_id)
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
             WHERE ak_id = $1",
        )
        .bind(ak_id)
        .bind(info.qps)
        .bind(info.daily_token_quota)
        .bind(info.tokens_per_minute)
        .bind(info.expires_at_epoch_secs)
        .bind(info.banned)
        .bind(info.suspended_until_epoch_secs)
        .execute(&mut *tx)
        .await
        .map_err(|e| crate::sqlx_err("apply patch", e))?;
        self.cache
            .commit(tx, ak_id)
            .await
            .map_err(|e| crate::sqlx_err("commit patch", e))?;
        Ok(Some(info))
    }

    async fn revoke(&self, ak_id: &str) -> GResult<bool> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin revoke", e))?;
        let n = sqlx::query("DELETE FROM access_keys WHERE ak_id = $1")
            .bind(ak_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| crate::sqlx_err("revoke key", e))?
            .rows_affected();
        self.cache
            .commit(tx, ak_id)
            .await
            .map_err(|e| crate::sqlx_err("commit revoke", e))?;
        Ok(n > 0)
    }

    async fn list(
        &self,
        tenant: Option<&str>,
        owner: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> GResult<Vec<AkInfo>> {
        let rows = sqlx::query(
            "SELECT ak_id, product, tenant, qps, daily_token_quota, tokens_per_minute,
             expires_at_epoch_secs, banned, model_quotas, owner, suspended_until_epoch_secs, mcp_servers, mcp_tools FROM access_keys
             WHERE ($1::text IS NULL OR tenant = $1) AND ($2::text IS NULL OR owner = $2)
             ORDER BY ak_id LIMIT $3 OFFSET $4",
        )
        .bind(tenant)
        .bind(owner)
        .bind(limit.min(i64::MAX as usize) as i64)
        .bind(offset.min(i64::MAX as usize) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("list keys", e))?;
        Ok(rows.iter().map(row_to_info).collect())
    }

    async fn reload_config_keys(&self, keys: &[gw_config::AkConf]) -> GResult<()> {
        let infos: Vec<AkInfo> = keys.iter().map(AkInfo::from).collect();
        let wanted: Vec<&str> = infos.iter().map(|k| &*k.ak_id).collect();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin reload", e))?;
        sqlx::query("DELETE FROM access_keys WHERE source = 'config' AND NOT (ak_id = ANY($1))")
            .bind(&wanted)
            .execute(&mut *tx)
            .await
            .map_err(|e| crate::sqlx_err("drop stale config keys", e))?;
        for info in &infos {
            upsert(&mut *tx, info, KeySource::Config)
                .await
                .map_err(|e| crate::sqlx_err("re-apply config key", e))?;
        }
        self.cache
            .commit(tx, FLUSH_ALL)
            .await
            .map_err(|e| crate::sqlx_err("commit reload", e))
    }
}

/// The one INSERT..ON CONFLICT for the key table; `suspended_until_epoch_secs`
/// is deliberately absent — an upsert never clears a runtime suspension.
async fn upsert(
    exec: impl sqlx::PgExecutor<'_>,
    info: &AkInfo,
    source: KeySource,
) -> Result<(), sqlx::Error> {
    let quotas = serde_json::to_string(&*info.model_quotas).unwrap_or_else(|_| "{}".into());
    let servers = serde_json::to_string(&info.mcp.servers).unwrap_or_else(|_| "[]".into());
    let tools = serde_json::to_string(&info.mcp.tools).unwrap_or_else(|_| "{}".into());
    sqlx::query(
        "INSERT INTO access_keys (ak_id, product, tenant, qps, daily_token_quota,
         tokens_per_minute, expires_at_epoch_secs, banned, model_quotas, owner, source,
         mcp_servers, mcp_tools)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
         ON CONFLICT (ak_id) DO UPDATE SET
           product = EXCLUDED.product, tenant = EXCLUDED.tenant,
           qps = EXCLUDED.qps, daily_token_quota = EXCLUDED.daily_token_quota,
           tokens_per_minute = EXCLUDED.tokens_per_minute,
           expires_at_epoch_secs = EXCLUDED.expires_at_epoch_secs,
           banned = EXCLUDED.banned, model_quotas = EXCLUDED.model_quotas,
           owner = EXCLUDED.owner,
           mcp_servers = EXCLUDED.mcp_servers, mcp_tools = EXCLUDED.mcp_tools,
           source = CASE WHEN access_keys.source = 'config' AND EXCLUDED.source = 'admin'
                         THEN 'config' ELSE EXCLUDED.source END",
    )
    .bind(&*info.ak_id)
    .bind(&info.product)
    .bind(&info.tenant)
    .bind(info.qps)
    .bind(info.daily_token_quota)
    .bind(info.tokens_per_minute)
    .bind(info.expires_at_epoch_secs)
    .bind(info.banned)
    .bind(&quotas)
    .bind(&info.owner)
    .bind(match source {
        KeySource::Config => "config",
        KeySource::Admin => "admin",
    })
    .bind(&servers)
    .bind(&tools)
    .execute(exec)
    .await
    .map(|_| ())
}

fn row_to_info(row: &sqlx::postgres::PgRow) -> AkInfo {
    AkInfo {
        ak_id: row.get::<&str, _>(0).into(),
        product: row.get(1),
        tenant: row.get(2),
        qps: row.get(3),
        daily_token_quota: row.get(4),
        tokens_per_minute: row.get(5),
        expires_at_epoch_secs: row.get(6),
        banned: row.get(7),
        model_quotas: Arc::new(serde_json::from_str(row.get::<&str, _>(8)).unwrap_or_default()),
        owner: row.get(9),
        suspended_until_epoch_secs: row.get(10),
        mcp: Arc::new(crate::McpAccess {
            servers: serde_json::from_str(row.get::<&str, _>(11)).unwrap_or_default(),
            tools: serde_json::from_str(row.get::<&str, _>(12)).unwrap_or_default(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use gw_config::access_key_id;

    use super::*;

    fn info(ak: &str, qps: f64) -> AkInfo {
        AkInfo {
            ak_id: access_key_id(ak).into(),
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
            mcp: Default::default(),
        }
    }

    #[tokio::test]
    async fn postgres_keystore_migrates_raw_keys_to_ids() {
        let Ok(url) = std::env::var("GW_TEST_PG_URL") else {
            return;
        };
        let (own_url, _) = crate::scratch_pg(&url).await;
        let legacy = sqlx::PgPool::connect(&own_url).await.expect("pg legacy");
        sqlx::query(
            "CREATE TABLE access_keys (ak TEXT PRIMARY KEY, product TEXT NOT NULL,
             tenant TEXT NOT NULL DEFAULT 'default', qps DOUBLE PRECISION NOT NULL,
             daily_token_quota BIGINT NOT NULL, tokens_per_minute BIGINT,
             expires_at_epoch_secs BIGINT, banned BOOLEAN NOT NULL DEFAULT FALSE,
             model_quotas TEXT NOT NULL DEFAULT '{}', source TEXT NOT NULL DEFAULT 'admin')",
        )
        .execute(&legacy)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO access_keys (ak, product, qps, daily_token_quota) VALUES ('pk-legacy', 'p', 1, 5)",
        )
        .execute(&legacy)
        .await
        .unwrap();

        for _ in 0..2 {
            let ks = PostgresKeyStore::connect(&own_url, 0)
                .await
                .expect("pg connect");
            let k = ks.get(&access_key_id("pk-legacy")).await.expect("migrated");
            assert_eq!(k.daily_token_quota, 5);
            let stored: Vec<String> = sqlx::query_scalar("SELECT ak_id FROM access_keys")
                .fetch_all(&ks.pool)
                .await
                .unwrap();
            assert_eq!(stored, [access_key_id("pk-legacy")]);
        }
    }

    #[tokio::test]
    async fn postgres_key_writes_reach_a_peer_instance_at_once() {
        let Ok(url) = std::env::var("GW_TEST_PG_URL") else {
            return;
        };
        let (own_url, _) = crate::scratch_pg(&url).await;
        let a = PostgresKeyStore::connect(&own_url, 0).await.expect("a");
        let b = PostgresKeyStore::connect(&own_url, 0).await.expect("b");
        let id = access_key_id("pk-peer");

        assert!(b.get(&id).await.is_none(), "b caches the miss");
        let qps = async || b.get(&id).await.map(|k| k.qps);
        a.put(info("pk-peer", 1.0), KeySource::Admin).await.unwrap();
        crate::eventually("peer sees the put", async || qps().await == Some(1.0)).await;
        let patch = KeyPatch {
            qps: Some(7.0),
            ..Default::default()
        };
        a.patch(&id, &patch).await.unwrap();
        crate::eventually("peer sees the patch", async || qps().await == Some(7.0)).await;
        assert!(a.revoke(&id).await.unwrap());
        crate::eventually("peer sees the revoke", async || qps().await.is_none()).await;
    }

    #[tokio::test]
    async fn postgres_keystore_semantics_mirror_memory() {
        let Ok(url) = std::env::var("GW_TEST_PG_URL") else {
            return;
        };
        let ks = PostgresKeyStore::connect(&url, 0)
            .await
            .expect("pg connect");
        sqlx::query("TRUNCATE access_keys")
            .execute(&ks.pool)
            .await
            .unwrap();
        ks.cache.flush().await;

        ks.put(info("pk-a", 1.0), KeySource::Admin).await.unwrap();
        assert_eq!(ks.get(&access_key_id("pk-a")).await.unwrap().qps, 1.0);
        assert!(ks.get(&access_key_id("pk-nope")).await.is_none());
        assert!(ks.revoke(&access_key_id("pk-a")).await.unwrap());
        assert!(
            ks.get(&access_key_id("pk-a")).await.is_none(),
            "local write invalidates the cache immediately"
        );
        assert!(!ks.revoke(&access_key_id("pk-a")).await.unwrap());

        ks.put(info("pk-b", 2.0), KeySource::Admin).await.unwrap();
        let p = ks
            .patch(
                &access_key_id("pk-b"),
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
                &access_key_id("pk-b"),
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
        assert!(ks.get(&access_key_id("pk-cfg")).await.is_some());
        assert!(
            ks.get(&access_key_id("pk-b")).await.is_some(),
            "admin key survives reload"
        );
        ks.put(info("pk-cfg", 3.0), KeySource::Admin).await.unwrap();
        let empty =
            gw_config::GatewayConfig::from_yaml("listen: {host: h, port: 1}\naccess_keys: []")
                .unwrap();
        ks.reload_config_keys(&empty.access_keys).await.unwrap();
        assert!(
            ks.get(&access_key_id("pk-cfg")).await.is_none(),
            "config ownership is sticky against admin overwrite"
        );
        assert!(ks.get(&access_key_id("pk-b")).await.is_some());

        let cfg = gw_config::GatewayConfig::from_yaml(
            "listen: {host: h, port: 1}\nmcp_servers: [{name: srv, endpoint: http://h/mcp}]\naccess_keys: [{ak: pk-mcp, product: p, qps: 1, daily_token_quota: 5, mcp_servers: [srv], mcp_tools: {srv: [t1]}}]",
        )
        .unwrap();
        ks.reload_config_keys(&cfg.access_keys).await.unwrap();
        let k = ks
            .get(&access_key_id("pk-mcp"))
            .await
            .expect("config key persisted");
        assert_eq!(k.mcp.servers, vec!["srv".to_owned()]);
        assert_eq!(k.mcp.tools["srv"], vec!["t1".to_owned()]);

        let mut admin = info("pk-plain", 1.0);
        admin.mcp = Arc::new(crate::McpAccess {
            servers: vec!["srv".into()],
            tools: Default::default(),
        });
        ks.put(admin, KeySource::Admin).await.unwrap();
        let k = ks
            .get(&access_key_id("pk-plain"))
            .await
            .expect("admin key persisted");
        assert_eq!(k.mcp.servers, vec!["srv".to_owned()]);
        assert!(k.mcp.tools.is_empty());

        let mut owned = info("pk-owned", 1.0);
        owned.owner = Some("m-1".into());
        ks.put(owned, KeySource::Admin).await.unwrap();
        let listed = ks.list(None, Some("m-1"), 0, 10).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(*listed[0].ak_id, access_key_id("pk-owned"));
        assert!(
            ks.list(Some("other"), Some("m-1"), 0, 10)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
