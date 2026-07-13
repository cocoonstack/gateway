//! Account health (cooldown/recovery) behind a trait: in-process for a single
//! node, Redis so a bad upstream account cools down across the whole fleet.

use std::time::Duration;

use async_trait::async_trait;

use crate::AccountHealth;

/// How long a local view of another instance's cooldown may lag. Account
/// selection scans every candidate per request, so reads must stay local;
/// cooldowns are seconds-granular, so a 1s lag is invisible.
const AVAILABLE_CACHE_TTL: Duration = Duration::from_millis(1000);
const AVAILABLE_CACHE_MAX: u64 = 10_000;
/// Failure streaks self-expire (hygiene only — a success deletes them).
const FAILS_TTL_MS: i64 = 3_600_000;

/// Consecutive-failure cooldown with auto-recovery, shared or per-node.
#[async_trait]
pub trait HealthStore: Send + Sync + std::fmt::Debug {
    /// Record a failure; true when this call tripped the account into cooldown.
    async fn record_failure(&self, name: &str, threshold: usize, cooldown: Duration) -> bool;
    async fn record_success(&self, name: &str);
    /// Available = not in an active cooldown (auto-recovers on expiry).
    async fn available(&self, name: &str) -> bool;
    /// Health label for the accounts view: "ok" | "cooling".
    async fn status(&self, name: &str) -> &'static str {
        if self.available(name).await {
            "ok"
        } else {
            "cooling"
        }
    }
}

#[async_trait]
impl HealthStore for AccountHealth {
    async fn record_failure(&self, name: &str, threshold: usize, cooldown: Duration) -> bool {
        AccountHealth::record_failure(self, name, threshold, cooldown)
    }
    async fn record_success(&self, name: &str) {
        AccountHealth::record_success(self, name);
    }
    async fn available(&self, name: &str) -> bool {
        AccountHealth::available(self, name)
    }
}

/// Fleet-wide health in Redis: the failure streak and the cooldown flag live
/// under `gw:health:*`, so an account tripped by one instance is skipped by
/// all. Reads come from a short-TTL local cache; a Redis outage fails open
/// (accounts stay selectable), matching the governance posture.
pub struct RedisHealth {
    conn: redis::aio::ConnectionManager,
    cache: moka::sync::Cache<String, bool>,
}

impl std::fmt::Debug for RedisHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RedisHealth")
    }
}

impl RedisHealth {
    pub async fn connect(url: &str) -> Result<Self, String> {
        let client = redis::Client::open(url).map_err(|e| format!("redis open: {e}"))?;
        let conn = redis::aio::ConnectionManager::new(client)
            .await
            .map_err(|e| format!("redis connect: {e}"))?;
        Ok(Self {
            conn,
            cache: moka::sync::Cache::builder()
                .max_capacity(AVAILABLE_CACHE_MAX)
                .time_to_live(AVAILABLE_CACHE_TTL)
                .build(),
        })
    }
}

#[async_trait]
impl HealthStore for RedisHealth {
    async fn record_failure(&self, name: &str, threshold: usize, cooldown: Duration) -> bool {
        let mut conn = self.conn.clone();
        // Atomic trip; an expired flag no longer EXISTS, so a still-failing
        // account re-arms.
        let script = redis::Script::new(
            "local f = redis.call('INCR', KEYS[1])
             redis.call('PEXPIRE', KEYS[1], ARGV[3])
             if f >= tonumber(ARGV[1]) and redis.call('EXISTS', KEYS[2]) == 0 then
               redis.call('SET', KEYS[2], '1', 'PX', ARGV[2])
               return 1
             end
             return 0",
        );
        let tripped: i64 = match script
            .key(fails_key(name))
            .key(cd_key(name))
            .arg(threshold as i64)
            .arg(cooldown.as_millis() as i64)
            .arg(FAILS_TTL_MS)
            .invoke_async(&mut conn)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, name, "redis health unavailable; failure not recorded");
                return false;
            }
        };
        self.cache.invalidate(name);
        tripped == 1
    }

    async fn record_success(&self, name: &str) {
        let mut conn = self.conn.clone();
        let _ = redis::cmd("DEL")
            .arg(fails_key(name))
            .arg(cd_key(name))
            .query_async::<i64>(&mut conn)
            .await;
        self.cache.invalidate(name);
    }

    async fn available(&self, name: &str) -> bool {
        if let Some(cached) = self.cache.get(name) {
            return cached;
        }
        let mut conn = self.conn.clone();
        let avail = match redis::cmd("EXISTS")
            .arg(cd_key(name))
            .query_async::<i64>(&mut conn)
            .await
        {
            Ok(v) => v == 0,
            Err(e) => {
                tracing::warn!(error = %e, name, "redis health unavailable; treating as healthy");
                true
            }
        };
        self.cache.insert(name.to_owned(), avail);
        avail
    }
}

fn fails_key(name: &str) -> String {
    format!("gw:health:fails:{name}")
}

fn cd_key(name: &str) -> String {
    format!("gw:health:cd:{name}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Set GW_TEST_REDIS_URL (e.g. redis://127.0.0.1:16379) to run this.
    #[tokio::test]
    async fn redis_health_trips_and_recovers() {
        let Ok(url) = std::env::var("GW_TEST_REDIS_URL") else {
            return;
        };
        let h = RedisHealth::connect(&url).await.expect("redis connect");
        let name = format!("acc-{}", std::process::id());
        let cd = Duration::from_millis(300);

        h.record_success(&name).await; // clean slate
        assert!(h.available(&name).await);
        assert!(!h.record_failure(&name, 2, cd).await);
        assert!(h.record_failure(&name, 2, cd).await, "threshold trips");
        assert!(!h.available(&name).await, "tripped account is unavailable");

        tokio::time::sleep(Duration::from_millis(350)).await;
        h.cache.invalidate(&name); // skip the local-cache lag in the test
        assert!(h.available(&name).await, "cooldown auto-recovers");
        assert!(
            h.record_failure(&name, 2, cd).await,
            "expired cooldown re-arms on the next failure"
        );
        h.record_success(&name).await;
        assert!(h.available(&name).await);
    }
}
