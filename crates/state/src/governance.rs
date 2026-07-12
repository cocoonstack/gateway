//! Rate/quota governance behind a trait so a single-node deployment uses
//! in-process counters and a multi-replica deployment shares state in Redis.
//!
//! [`MemoryGovernance`] is the default; [`RedisGovernance`] keeps the same
//! semantics using atomic server-side operations (INCR + EXPIRE windows,
//! token-bucket via a Lua script).

use std::time::Duration;

use async_trait::async_trait;

use crate::{QuotaStore, RateLimiter, TokenWindow, WindowCounter};

/// The governance operations the request pipeline calls.
#[async_trait]
pub trait Governance: Send + Sync + std::fmt::Debug {
    /// Rate limit: take one permit at `qps` for `key`.
    async fn rate_allow(&self, key: &str, qps: f64) -> bool;

    /// Daily quota: is `ak` under `limit`?
    async fn quota_check(&self, ak: &str, limit: i64) -> bool;
    /// Tokens spent today by `ak`.
    async fn quota_used(&self, ak: &str) -> i64;
    /// Add to `ak`'s spent tokens.
    async fn quota_consume(&self, ak: &str, tokens: i64);
    /// Reset every daily counter.
    async fn quota_reset_all(&self);

    /// Fixed-window request limit (QPM): take one permit.
    async fn window_allow(&self, key: &str, limit: i64, window: Duration) -> bool;

    /// Fixed-window token limit (TPM): are spent tokens under `limit`?
    async fn token_window_check(&self, key: &str, limit: i64, window: Duration) -> bool;
    /// Add to the current TPM window.
    async fn token_window_add(&self, key: &str, tokens: i64, window: Duration);
}

/// In-process governance: wraps the local counter structs. The default.
#[derive(Debug, Default)]
pub struct MemoryGovernance {
    rate: RateLimiter,
    quota: QuotaStore,
    qpm: WindowCounter,
    tpm: TokenWindow,
}

#[async_trait]
impl Governance for MemoryGovernance {
    async fn rate_allow(&self, key: &str, qps: f64) -> bool {
        self.rate.allow(key, qps)
    }
    async fn quota_check(&self, ak: &str, limit: i64) -> bool {
        self.quota.check(ak, limit)
    }
    async fn quota_used(&self, ak: &str) -> i64 {
        self.quota.used(ak)
    }
    async fn quota_consume(&self, ak: &str, tokens: i64) {
        self.quota.consume(ak, tokens);
    }
    async fn quota_reset_all(&self) {
        self.quota.reset_all();
    }
    async fn window_allow(&self, key: &str, limit: i64, window: Duration) -> bool {
        self.qpm.allow(key, limit, window)
    }
    async fn token_window_check(&self, key: &str, limit: i64, window: Duration) -> bool {
        self.tpm.check(key, limit, window)
    }
    async fn token_window_add(&self, key: &str, tokens: i64, window: Duration) {
        self.tpm.add(key, tokens, window);
    }
}

/// Redis-backed governance for multi-replica deployments. Keys are namespaced
/// under `ap:`; windows use INCR + EXPIRE so they self-expire.
#[derive(Clone)]
pub struct RedisGovernance {
    conn: redis::aio::ConnectionManager,
}

impl std::fmt::Debug for RedisGovernance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RedisGovernance")
    }
}

impl RedisGovernance {
    pub async fn connect(url: &str) -> Result<Self, String> {
        let client = redis::Client::open(url).map_err(|e| format!("redis open: {e}"))?;
        let conn = redis::aio::ConnectionManager::new(client)
            .await
            .map_err(|e| format!("redis connect: {e}"))?;
        Ok(Self { conn })
    }

    /// Increment `key` and set its TTL on first use; returns the post-increment
    /// count. A failed round-trip returns 0 so limits fail open rather than
    /// wedging the gateway on a Redis blip.
    async fn incr_window(&self, key: &str, by: i64, window: Duration) -> i64 {
        let mut conn = self.conn.clone();
        let script = redis::Script::new(
            "local v = redis.call('INCRBY', KEYS[1], ARGV[1])
             if v == tonumber(ARGV[1]) then redis.call('PEXPIRE', KEYS[1], ARGV[2]) end
             return v",
        );
        script
            .key(key)
            .arg(by)
            .arg(window.as_millis() as i64)
            .invoke_async(&mut conn)
            .await
            .unwrap_or(0)
    }
}

#[async_trait]
impl Governance for RedisGovernance {
    async fn rate_allow(&self, key: &str, qps: f64) -> bool {
        // 1s fixed window approximating qps; burst = ceil(qps).
        let limit = qps.ceil().max(1.0) as i64;
        self.incr_window(&format!("ap:rate:{key}"), 1, Duration::from_secs(1))
            .await
            <= limit
    }
    async fn quota_check(&self, ak: &str, limit: i64) -> bool {
        self.quota_used(ak).await < limit
    }
    async fn quota_used(&self, ak: &str) -> i64 {
        let mut conn = self.conn.clone();
        redis::cmd("GET")
            .arg(format!("ap:quota:{ak}"))
            .query_async::<Option<i64>>(&mut conn)
            .await
            .ok()
            .flatten()
            .unwrap_or(0)
    }
    async fn quota_consume(&self, ak: &str, tokens: i64) {
        let mut conn = self.conn.clone();
        let _ = redis::cmd("INCRBY")
            .arg(format!("ap:quota:{ak}"))
            .arg(tokens)
            .query_async::<i64>(&mut conn)
            .await;
    }
    async fn quota_reset_all(&self) {
        let mut conn = self.conn.clone();
        if let Ok(keys) = redis::cmd("KEYS")
            .arg("ap:quota:*")
            .query_async::<Vec<String>>(&mut conn)
            .await
            && !keys.is_empty()
        {
            let _ = redis::cmd("DEL")
                .arg(keys)
                .query_async::<i64>(&mut conn)
                .await;
        }
    }
    async fn window_allow(&self, key: &str, limit: i64, window: Duration) -> bool {
        self.incr_window(&format!("ap:qpm:{key}"), 1, window).await <= limit
    }
    async fn token_window_check(&self, key: &str, limit: i64, window: Duration) -> bool {
        let mut conn = self.conn.clone();
        let used = redis::cmd("GET")
            .arg(format!("ap:tpm:{key}"))
            .query_async::<Option<i64>>(&mut conn)
            .await
            .ok()
            .flatten()
            .unwrap_or(0);
        let _ = window;
        used < limit
    }
    async fn token_window_add(&self, key: &str, tokens: i64, window: Duration) {
        self.incr_window(&format!("ap:tpm:{key}"), tokens, window)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Set AP_TEST_REDIS_URL (e.g. redis://127.0.0.1:16379) to run this.
    #[tokio::test]
    async fn redis_governance_enforces_limits() {
        let Ok(url) = std::env::var("AP_TEST_REDIS_URL") else {
            return;
        };
        let g = RedisGovernance::connect(&url).await.expect("redis connect");
        let ak = format!("t{}", std::process::id());
        g.quota_reset_all().await;
        assert!(g.quota_check(&ak, 10).await);
        g.quota_consume(&ak, 10).await;
        assert_eq!(g.quota_used(&ak).await, 10);
        assert!(!g.quota_check(&ak, 10).await);

        let mkey = format!("m{}", std::process::id());
        assert!(g.window_allow(&mkey, 1, Duration::from_secs(60)).await);
        assert!(!g.window_allow(&mkey, 1, Duration::from_secs(60)).await);

        assert!(g.token_window_check(&ak, 10, Duration::from_secs(60)).await);
        g.token_window_add(&ak, 10, Duration::from_secs(60)).await;
        assert!(!g.token_window_check(&ak, 10, Duration::from_secs(60)).await);
        g.quota_reset_all().await;
    }

    #[tokio::test]
    async fn memory_governance_enforces_limits() {
        let g = MemoryGovernance::default();
        assert!(g.quota_check("ak", 10).await);
        g.quota_consume("ak", 10).await;
        assert_eq!(g.quota_used("ak").await, 10);
        assert!(!g.quota_check("ak", 10).await);
        g.quota_reset_all().await;
        assert_eq!(g.quota_used("ak").await, 0);

        assert!(g.window_allow("m", 1, Duration::from_secs(60)).await);
        assert!(!g.window_allow("m", 1, Duration::from_secs(60)).await);

        assert!(
            g.token_window_check("ak", 10, Duration::from_secs(60))
                .await
        );
        g.token_window_add("ak", 10, Duration::from_secs(60)).await;
        assert!(
            !g.token_window_check("ak", 10, Duration::from_secs(60))
                .await
        );
    }
}
