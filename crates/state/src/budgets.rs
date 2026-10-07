//! Per-user budget overrides inside a tenant: a user's own daily cost, monthly
//! cost and daily token caps in place of the tenant's per-user defaults.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Mutex;

use async_trait::async_trait;
use gw_consts::ErrCode;
use gw_models::{GResult, GatewayError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sqlx::Row;

use crate::fleet_cache::FleetCache;

const BUDGET_CACHE_MAX: u64 = 1_000_000;
const BUDGET_CHANNEL: &str = "gw_user_budgets";
const UNLIMITED_COLUMN: i64 = -1;
const UNLIMITED_WIRE: &str = "unlimited";

/// One per-user cap: the tenant's default, a cap of its own, or none.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum UserCap {
    #[default]
    Inherit,
    Limit(i64),
    Unlimited,
}

impl UserCap {
    /// The cap in force given the tenant's default; `None` = no per-user scope.
    pub fn resolve(self, tenant_default: Option<i64>) -> Option<i64> {
        match self {
            UserCap::Inherit => tenant_default,
            UserCap::Limit(n) => Some(n),
            // still counted, so a later cap starts from the real spend
            UserCap::Unlimited => Some(i64::MAX),
        }
    }

    fn from_column(v: Option<i64>) -> Self {
        match v {
            None => UserCap::Inherit,
            Some(UNLIMITED_COLUMN) => UserCap::Unlimited,
            Some(n) => UserCap::Limit(n),
        }
    }

    fn to_column(self) -> Option<i64> {
        match self {
            UserCap::Inherit => None,
            UserCap::Limit(n) => Some(n),
            UserCap::Unlimited => Some(UNLIMITED_COLUMN),
        }
    }
}

impl Serialize for UserCap {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            UserCap::Inherit => s.serialize_none(),
            UserCap::Limit(n) => s.serialize_i64(*n),
            UserCap::Unlimited => s.serialize_str(UNLIMITED_WIRE),
        }
    }
}

impl<'de> Deserialize<'de> for UserCap {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Limit(i64),
            Word(String),
        }
        match Option::<Wire>::deserialize(d)? {
            None => Ok(UserCap::Inherit),
            Some(Wire::Limit(n)) if n >= 0 => Ok(UserCap::Limit(n)),
            Some(Wire::Word(w)) if w == UNLIMITED_WIRE => Ok(UserCap::Unlimited),
            Some(_) => Err(serde::de::Error::custom(
                "a cap is a non-negative integer, \"unlimited\", or null",
            )),
        }
    }
}

/// A user's caps; an absent or null field inherits the tenant's per-user default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UserBudget {
    pub daily_cost_quota_micros: UserCap,
    pub monthly_cost_quota_micros: UserCap,
    pub daily_token_quota: UserCap,
}

/// The override table behind `/admin/tenants/{tenant}/users`.
#[async_trait]
pub trait UserBudgetStore: Send + Sync + std::fmt::Debug {
    /// `user`'s override in `tenant`; `Ok(None)` = none.
    async fn get(&self, tenant: &str, user: &str) -> GResult<Option<UserBudget>>;
    async fn put(&self, tenant: &str, user: &str, budget: UserBudget) -> GResult<()>;
    async fn delete(&self, tenant: &str, user: &str) -> GResult<bool>;
    /// Up to `limit` overrides in `tenant` ordered by user id, after `after`.
    async fn list(
        &self,
        tenant: &str,
        after: &str,
        limit: usize,
    ) -> GResult<Vec<(String, UserBudget)>>;
}

/// Single-node overrides; lost on restart like admin-created keys.
#[derive(Debug, Default)]
pub struct MemoryUserBudgets {
    tenants: Mutex<BTreeMap<String, BTreeMap<String, UserBudget>>>,
}

#[async_trait]
impl UserBudgetStore for MemoryUserBudgets {
    async fn get(&self, tenant: &str, user: &str) -> GResult<Option<UserBudget>> {
        Ok(crate::lock(&self.tenants)
            .get(tenant)
            .and_then(|users| users.get(user))
            .copied())
    }

    async fn put(&self, tenant: &str, user: &str, budget: UserBudget) -> GResult<()> {
        crate::lock(&self.tenants)
            .entry(tenant.to_owned())
            .or_default()
            .insert(user.to_owned(), budget);
        Ok(())
    }

    async fn delete(&self, tenant: &str, user: &str) -> GResult<bool> {
        Ok(crate::lock(&self.tenants)
            .get_mut(tenant)
            .is_some_and(|users| users.remove(user).is_some()))
    }

    async fn list(
        &self,
        tenant: &str,
        after: &str,
        limit: usize,
    ) -> GResult<Vec<(String, UserBudget)>> {
        let tenants = crate::lock(&self.tenants);
        let Some(users) = tenants.get(tenant) else {
            return Ok(Vec::new());
        };
        Ok(users
            .range::<str, _>((Bound::Excluded(after), Bound::Unbounded))
            .take(limit)
            .map(|(u, b)| (u.clone(), *b))
            .collect())
    }
}

/// Fleet-shared overrides in Postgres, cached per instance (absent ones included).
#[derive(Debug)]
pub struct PostgresUserBudgets {
    pool: sqlx::PgPool,
    cache: FleetCache<Option<UserBudget>>,
}

impl PostgresUserBudgets {
    /// Shares `pool` (the key table's): this table only serves cache misses and admin writes.
    pub async fn connect(pool: sqlx::PgPool, url: &str) -> GResult<Self> {
        crate::setup_schema(
            &pool,
            "user_budgets",
            &["CREATE TABLE IF NOT EXISTS user_budgets (
                tenant TEXT NOT NULL,
                user_id TEXT NOT NULL,
                daily_cost_quota_micros BIGINT,
                monthly_cost_quota_micros BIGINT,
                daily_token_quota BIGINT,
                PRIMARY KEY (tenant, user_id))"],
        )
        .await?;
        let cache = FleetCache::listen(url, BUDGET_CHANNEL, BUDGET_CACHE_MAX).await?;
        Ok(Self { pool, cache })
    }

    async fn fetch(&self, tenant: &str, user: &str) -> Result<Option<UserBudget>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT daily_cost_quota_micros, monthly_cost_quota_micros, daily_token_quota
             FROM user_budgets WHERE tenant = $1 AND user_id = $2",
        )
        .bind(tenant)
        .bind(user)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(row_to_budget))
    }
}

#[async_trait]
impl UserBudgetStore for PostgresUserBudgets {
    async fn get(&self, tenant: &str, user: &str) -> GResult<Option<UserBudget>> {
        self.cache
            .get_with(&cache_key(tenant, user), self.fetch(tenant, user))
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "user budget store unreachable; admission fails closed");
                GatewayError::new(ErrCode::DB_READ, 503, "user budget store unavailable")
                    .with_source(e)
            })
    }

    async fn put(&self, tenant: &str, user: &str, budget: UserBudget) -> GResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin user budget upsert", e))?;
        sqlx::query(
            "INSERT INTO user_budgets (tenant, user_id, daily_cost_quota_micros,
             monthly_cost_quota_micros, daily_token_quota) VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (tenant, user_id) DO UPDATE SET
               daily_cost_quota_micros = EXCLUDED.daily_cost_quota_micros,
               monthly_cost_quota_micros = EXCLUDED.monthly_cost_quota_micros,
               daily_token_quota = EXCLUDED.daily_token_quota",
        )
        .bind(tenant)
        .bind(user)
        .bind(budget.daily_cost_quota_micros.to_column())
        .bind(budget.monthly_cost_quota_micros.to_column())
        .bind(budget.daily_token_quota.to_column())
        .execute(&mut *tx)
        .await
        .map_err(|e| crate::sqlx_err("upsert user budget", e))?;
        self.cache
            .commit(tx, &cache_key(tenant, user))
            .await
            .map_err(|e| crate::sqlx_err("commit user budget upsert", e))
    }

    async fn delete(&self, tenant: &str, user: &str) -> GResult<bool> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| crate::sqlx_err("begin user budget delete", e))?;
        let n = sqlx::query("DELETE FROM user_budgets WHERE tenant = $1 AND user_id = $2")
            .bind(tenant)
            .bind(user)
            .execute(&mut *tx)
            .await
            .map_err(|e| crate::sqlx_err("delete user budget", e))?
            .rows_affected();
        self.cache
            .commit(tx, &cache_key(tenant, user))
            .await
            .map_err(|e| crate::sqlx_err("commit user budget delete", e))?;
        Ok(n > 0)
    }

    async fn list(
        &self,
        tenant: &str,
        after: &str,
        limit: usize,
    ) -> GResult<Vec<(String, UserBudget)>> {
        let rows = sqlx::query(
            "SELECT daily_cost_quota_micros, monthly_cost_quota_micros, daily_token_quota, user_id
             FROM user_budgets WHERE tenant = $1 AND user_id > $2 ORDER BY user_id LIMIT $3",
        )
        .bind(tenant)
        .bind(after)
        .bind(limit.min(i64::MAX as usize) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| crate::sqlx_err("list user budgets", e))?;
        Ok(rows.iter().map(|r| (r.get(3), row_to_budget(r))).collect())
    }
}

/// Tenant names never contain ':', so two pairs never share a key.
fn cache_key(tenant: &str, user: &str) -> String {
    format!("{tenant}:{user}")
}

fn row_to_budget(row: &sqlx::postgres::PgRow) -> UserBudget {
    UserBudget {
        daily_cost_quota_micros: UserCap::from_column(row.get(0)),
        monthly_cost_quota_micros: UserCap::from_column(row.get(1)),
        daily_token_quota: UserCap::from_column(row.get(2)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(n: i64) -> UserBudget {
        UserBudget {
            daily_cost_quota_micros: UserCap::Limit(n),
            ..Default::default()
        }
    }

    #[test]
    fn caps_parse_integers_unlimited_and_null() {
        let b: UserBudget = serde_json::from_str(
            r#"{"daily_cost_quota_micros": 5, "monthly_cost_quota_micros": "unlimited", "daily_token_quota": null}"#,
        )
        .unwrap();
        assert_eq!(b.daily_cost_quota_micros, UserCap::Limit(5));
        assert_eq!(b.monthly_cost_quota_micros, UserCap::Unlimited);
        assert_eq!(b.daily_token_quota, UserCap::Inherit);
        assert_eq!(
            serde_json::from_str::<UserBudget>("{}").unwrap(),
            UserBudget::default()
        );
        assert_eq!(
            serde_json::to_value(b).unwrap(),
            serde_json::json!({"daily_cost_quota_micros": 5, "monthly_cost_quota_micros": "unlimited", "daily_token_quota": null})
        );
        for bad in [
            r#"{"daily_cost_quota_micros": -1}"#,
            r#"{"daily_cost_quota_micros": 1.5}"#,
            r#"{"daily_cost_quota_micros": "none"}"#,
            r#"{"daily_cost_quota": 5}"#,
        ] {
            assert!(serde_json::from_str::<UserBudget>(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn caps_resolve_against_the_tenant_default() {
        assert_eq!(UserCap::Inherit.resolve(Some(7)), Some(7));
        assert_eq!(UserCap::Inherit.resolve(None), None);
        assert_eq!(UserCap::Limit(2).resolve(Some(7)), Some(2));
        assert_eq!(UserCap::Limit(2).resolve(None), Some(2));
        assert_eq!(UserCap::Unlimited.resolve(Some(7)), Some(i64::MAX));
        for cap in [
            UserCap::Inherit,
            UserCap::Limit(0),
            UserCap::Limit(9),
            UserCap::Unlimited,
        ] {
            assert_eq!(UserCap::from_column(cap.to_column()), cap);
        }
    }

    #[tokio::test]
    async fn memory_store_pages_by_user_within_a_tenant() {
        let s = MemoryUserBudgets::default();
        for u in ["c", "a", "b"] {
            s.put("t1", u, limit(1)).await.unwrap();
        }
        s.put("t2", "a", limit(9)).await.unwrap();
        assert_eq!(s.get("t1", "a").await.unwrap(), Some(limit(1)));
        assert_eq!(s.get("t2", "a").await.unwrap(), Some(limit(9)));
        assert_eq!(s.get("t1", "z").await.unwrap(), None);
        let page = s.list("t1", "", 2).await.unwrap();
        assert_eq!(
            page.iter().map(|(u, _)| u.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
        let page = s.list("t1", "b", 2).await.unwrap();
        assert_eq!(
            page.iter().map(|(u, _)| u.as_str()).collect::<Vec<_>>(),
            ["c"]
        );
        assert!(s.delete("t1", "a").await.unwrap());
        assert!(!s.delete("t1", "a").await.unwrap());
        assert!(s.list("t3", "", 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn postgres_overrides_reach_a_peer_instance_at_once() {
        let Ok(url) = std::env::var("GW_TEST_PG_URL") else {
            return;
        };
        let (own_url, _) = crate::scratch_pg(&url).await;
        let connect = async || {
            let pool = sqlx::PgPool::connect(&own_url).await.expect("pool");
            PostgresUserBudgets::connect(pool, &own_url)
                .await
                .expect("store")
        };
        let (a, b) = (connect().await, connect().await);

        assert_eq!(
            b.get("t1", "u1").await.unwrap(),
            None,
            "b caches the absence"
        );
        a.put("t1", "u1", limit(5)).await.unwrap();
        crate::eventually("peer sees the put", async || {
            b.get("t1", "u1").await.unwrap() == Some(limit(5))
        })
        .await;

        let unlimited = UserBudget {
            daily_cost_quota_micros: UserCap::Unlimited,
            daily_token_quota: UserCap::Limit(0),
            ..Default::default()
        };
        a.put("t1", "u1", unlimited).await.unwrap();
        crate::eventually("peer sees the replace", async || {
            b.get("t1", "u1").await.unwrap() == Some(unlimited)
        })
        .await;
        assert!(a.delete("t1", "u1").await.unwrap());
        crate::eventually("peer sees the delete", async || {
            b.get("t1", "u1").await.unwrap().is_none()
        })
        .await;

        for u in ["u3", "u1", "u2"] {
            a.put("t1", u, limit(1)).await.unwrap();
        }
        a.put("t2", "u0", limit(1)).await.unwrap();
        let page = b.list("t1", "u1", 10).await.unwrap();
        assert_eq!(
            page.iter().map(|(u, _)| u.as_str()).collect::<Vec<_>>(),
            ["u2", "u3"]
        );
    }
}
