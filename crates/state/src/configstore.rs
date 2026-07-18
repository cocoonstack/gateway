//! Fleet-shared config source of truth: versioned YAML documents in Postgres.
//! A publish inserts a new version and fires a `gw_config` NOTIFY, so every
//! instance's listener reloads without a per-instance SIGHUP.

use gw_models::GResult;
use sqlx::Row;

/// Superseded versions kept for operator inspection/rollback.
const KEEP_VERSIONS: i64 = 20;

/// The Postgres NOTIFY channel a publish fires on (payload = version id).
pub const CONFIG_CHANNEL: &str = "gw_config";

/// Every publish serializes on this advisory lock: a `MAX(id)`-guarded insert
/// is not atomic under READ COMMITTED — two concurrent guarded publishes both
/// see the old head and both insert.
const PG_PUBLISH_LOCK_SQL: &str = "SELECT pg_advisory_xact_lock(hashtext('gw_config_publish'))";

/// Metadata for one retained config document, newest first in list responses.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConfigVersion {
    pub id: i64,
    pub created_at_epoch_secs: i64,
}

/// Versioned gateway config in Postgres — the source of truth when
/// `storage.postgres_url` is set (the local file only seeds an empty store).
#[derive(Debug)]
pub struct PostgresConfigStore {
    pool: sqlx::PgPool,
}

impl PostgresConfigStore {
    pub async fn connect(url: &str) -> GResult<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(3)
            .connect(url)
            .await
            .map_err(|e| crate::sqlx_err("connect postgres config store", e))?;
        crate::setup_schema(
            &pool,
            "gw_config",
            &["CREATE TABLE IF NOT EXISTS gw_config (
                id BIGSERIAL PRIMARY KEY,
                yaml TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now())"],
        )
        .await?;
        Ok(Self { pool })
    }

    /// The latest published config document; `None` on a fresh store.
    pub async fn load_latest(&self) -> GResult<Option<(i64, String)>> {
        let row = sqlx::query("SELECT id, yaml FROM gw_config ORDER BY id DESC LIMIT 1")
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("read latest config", e))?;
        Ok(row.map(|r| (r.get(0), r.get(1))))
    }

    /// Load one retained version by id.
    pub async fn load_version(&self, id: i64) -> GResult<Option<String>> {
        sqlx::query_scalar("SELECT yaml FROM gw_config WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("read config version", e))
    }

    /// List retained versions newest first.
    pub async fn list_versions(&self, limit: usize) -> GResult<Vec<ConfigVersion>> {
        let rows = sqlx::query(
            "SELECT id, EXTRACT(EPOCH FROM created_at)::BIGINT
             FROM gw_config ORDER BY id DESC LIMIT $1",
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("list config versions", e))?;
        Ok(rows
            .iter()
            .map(|r| ConfigVersion {
                id: r.get(0),
                created_at_epoch_secs: r.get(1),
            })
            .collect())
    }

    /// Store a new version and notify every listening instance. The caller
    /// validates the YAML first — the store never holds an unparsable config.
    pub async fn publish(&self, yaml: &str) -> GResult<i64> {
        let mut tx = self.begin_locked().await?;
        let id = Self::insert_notify(&mut tx, yaml).await?;
        tx.commit()
            .await
            .map_err(|e| crate::sqlx_err("commit config publish", e))?;
        self.prune().await?;
        Ok(id)
    }

    /// [`Self::publish`] only if `expected` is still the newest version;
    /// `None` when a concurrent publish moved the head (head checked under
    /// the publish lock).
    pub async fn publish_if(&self, yaml: &str, expected: i64) -> GResult<Option<i64>> {
        let mut tx = self.begin_locked().await?;
        let head: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM gw_config")
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| crate::sqlx_err("read config head", e))?;
        if head != expected {
            return Ok(None);
        }
        let id = Self::insert_notify(&mut tx, yaml).await?;
        tx.commit()
            .await
            .map_err(|e| crate::sqlx_err("commit config publish", e))?;
        self.prune().await?;
        Ok(Some(id))
    }

    async fn begin_locked(&self) -> GResult<sqlx::Transaction<'_, sqlx::Postgres>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin config publish", e))?;
        sqlx::query(PG_PUBLISH_LOCK_SQL)
            .execute(&mut *tx)
            .await
            .map_err(|e| crate::sqlx_err("lock config publish", e))?;
        Ok(tx)
    }

    // NOTIFY is transactional: peers hear the id only after the commit
    async fn insert_notify(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        yaml: &str,
    ) -> GResult<i64> {
        sqlx::query_scalar(
            "WITH ins AS (INSERT INTO gw_config (yaml) VALUES ($1) RETURNING id)
             SELECT id FROM ins, pg_notify($2, ins.id::text)",
        )
        .bind(yaml)
        .bind(CONFIG_CHANNEL)
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| crate::sqlx_err("publish config", e))
    }

    async fn prune(&self) -> GResult<()> {
        sqlx::query("DELETE FROM gw_config WHERE id <= (SELECT MAX(id) FROM gw_config) - $1")
            .bind(KEEP_VERSIONS)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::sqlx_err("prune config versions", e))?;
        Ok(())
    }
}

/// Subscribe to the config change feed: yields each published version id.
/// The channel closes when the Postgres connection drops — the caller loops
/// and re-subscribes (missed versions don't matter; a reload reads latest).
pub async fn subscribe(url: &str) -> GResult<tokio::sync::mpsc::Receiver<i64>> {
    let mut listener = sqlx::postgres::PgListener::connect(url)
        .await
        .map_err(|e| crate::sqlx_err("connect config listener", e))?;
    listener
        .listen(CONFIG_CHANNEL)
        .await
        .map_err(|e| crate::sqlx_err("listen on config channel", e))?;
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    tokio::spawn(async move {
        loop {
            match listener.recv().await {
                Ok(n) => {
                    let version = n.payload().parse().unwrap_or(0);
                    if tx.send(version).await.is_err() {
                        return;
                    }
                }
                Err(_) => return, // closing tx signals resubscribe
            }
        }
    });
    Ok(rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_notifies_and_load_returns_latest() {
        let Ok(url) = std::env::var("GW_TEST_PG_URL") else {
            return;
        };
        let store = PostgresConfigStore::connect(&url).await.expect("connect");

        let mut listener = sqlx::postgres::PgListener::connect(&url)
            .await
            .expect("listener");
        listener.listen(CONFIG_CHANNEL).await.expect("listen");

        let v1 = store.publish("listen: {host: a, port: 1}").await.unwrap();
        let v2 = store.publish("listen: {host: b, port: 2}").await.unwrap();
        assert!(v2 > v1);
        let (id, yaml) = store.load_latest().await.unwrap().expect("latest");
        assert_eq!(id, v2);
        assert!(yaml.contains("host: b"));

        let versions = store.list_versions(20).await.unwrap();
        assert_eq!(versions[0].id, v2);
        assert_eq!(
            store.load_version(v1).await.unwrap().as_deref(),
            Some("listen: {host: a, port: 1}")
        );

        let stale = store
            .publish_if("listen: {host: c, port: 3}", v1)
            .await
            .unwrap();
        assert!(stale.is_none(), "stale expected head must not publish");
        let v3 = store
            .publish_if("listen: {host: a, port: 1}", v2)
            .await
            .unwrap()
            .expect("matching head publishes");
        assert!(v3 > v2);
        let (id, yaml) = store.load_latest().await.unwrap().expect("latest");
        assert_eq!(id, v3);
        assert!(yaml.contains("host: a"));

        let n = listener.recv().await.expect("notify");
        assert_eq!(n.channel(), CONFIG_CHANNEL);
        assert_eq!(n.payload(), v1.to_string());

        let store = &store;
        let contenders = futures::future::join_all((0..8).map(|i| {
            let yaml = format!("listen: {{host: c{i}, port: 4}}");
            async move { store.publish_if(&yaml, v3).await.unwrap() }
        }))
        .await;
        assert_eq!(
            contenders.iter().flatten().count(),
            1,
            "concurrent guarded publishes admit exactly one"
        );
    }
}
