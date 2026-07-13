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
    /// Admission with reservation: admit while spent-before < `limit`, and
    /// atomically add `amount` so concurrent in-flight requests count against
    /// the budget. False = rejected (nothing reserved).
    async fn quota_reserve(&self, key: &str, amount: i64, limit: i64) -> bool;
    /// Apply the settle delta (actual - reserved; negative refunds).
    async fn quota_settle(&self, key: &str, delta: i64);
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
    /// Windowed admission with reservation (see [`Governance::quota_reserve`]).
    async fn token_window_reserve(
        &self,
        key: &str,
        amount: i64,
        limit: i64,
        window: Duration,
    ) -> bool;
    /// Apply the settle delta to the current window (negative refunds).
    async fn token_window_settle(&self, key: &str, delta: i64, window: Duration);
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
    async fn quota_reserve(&self, key: &str, amount: i64, limit: i64) -> bool {
        self.quota.reserve(key, amount, limit)
    }
    async fn quota_settle(&self, key: &str, delta: i64) {
        self.quota.settle(key, delta);
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
    async fn token_window_reserve(
        &self,
        key: &str,
        amount: i64,
        limit: i64,
        window: Duration,
    ) -> bool {
        self.tpm.reserve(key, amount, limit, window)
    }
    async fn token_window_settle(&self, key: &str, delta: i64, window: Duration) {
        self.tpm.settle(key, delta, window);
    }
    async fn token_window_add(&self, key: &str, tokens: i64, window: Duration) {
        self.tpm.add(key, tokens, window);
    }
}

/// Redis-backed governance for multi-replica deployments. Keys are namespaced
/// under `gw:`; windows use INCR + EXPIRE so they self-expire.
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
        match script
            .key(key)
            .arg(by)
            .arg(window.as_millis() as i64)
            .invoke_async(&mut conn)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                // fail open, but loudly — a persistent outage otherwise silently
                // disables limits with no signal.
                tracing::warn!(error = %e, key, "redis governance unavailable; limit skipped");
                0
            }
        }
    }
}

#[async_trait]
impl Governance for RedisGovernance {
    async fn rate_allow(&self, key: &str, qps: f64) -> bool {
        if qps <= 0.0 {
            return false;
        }
        // qps >= 1: N permits per 1s. qps < 1: 1 permit per 1/qps seconds,
        // matching the in-memory backend.
        let (limit, window) = if qps < 1.0 {
            (1, Duration::from_secs_f64(1.0 / qps))
        } else {
            (qps.ceil() as i64, Duration::from_secs(1))
        };
        self.incr_window(&format!("gw:rate:{key}"), 1, window).await <= limit
    }
    async fn quota_check(&self, ak: &str, limit: i64) -> bool {
        self.quota_used(ak).await < limit
    }
    async fn quota_used(&self, ak: &str) -> i64 {
        let mut conn = self.conn.clone();
        match redis::cmd("GET")
            .arg(format!("gw:quota:{ak}"))
            .query_async::<Option<i64>>(&mut conn)
            .await
        {
            Ok(v) => v.unwrap_or(0),
            Err(e) => {
                tracing::warn!(error = %e, ak, "redis quota read failed; treating as 0");
                0
            }
        }
    }
    async fn quota_reserve(&self, key: &str, amount: i64, limit: i64) -> bool {
        let mut conn = self.conn.clone();
        // admit while spent-before < limit; the reservation itself may cross
        // the limit (same one-request overshoot the settle corrects)
        let script = redis::Script::new(
            "local v = redis.call('INCRBY', KEYS[1], ARGV[1])
             if v - tonumber(ARGV[1]) >= tonumber(ARGV[2]) then
               redis.call('DECRBY', KEYS[1], ARGV[1])
               return 0
             end
             return 1",
        );
        match script
            .key(format!("gw:quota:{key}"))
            .arg(amount)
            .arg(limit)
            .invoke_async::<i64>(&mut conn)
            .await
        {
            Ok(v) => v == 1,
            Err(e) => {
                tracing::warn!(error = %e, key, "redis quota reserve failed; admitting");
                true
            }
        }
    }
    async fn quota_settle(&self, key: &str, delta: i64) {
        if delta == 0 {
            return;
        }
        let mut conn = self.conn.clone();
        let _ = redis::cmd("INCRBY")
            .arg(format!("gw:quota:{key}"))
            .arg(delta)
            .query_async::<i64>(&mut conn)
            .await;
    }
    async fn quota_consume(&self, ak: &str, tokens: i64) {
        let mut conn = self.conn.clone();
        let _ = redis::cmd("INCRBY")
            .arg(format!("gw:quota:{ak}"))
            .arg(tokens)
            .query_async::<i64>(&mut conn)
            .await;
    }
    async fn quota_reset_all(&self) {
        // SCAN (non-blocking) + UNLINK (async free), unlike KEYS+DEL which
        // block the single-threaded server on a large keyspace.
        let mut conn = self.conn.clone();
        let mut cursor = 0u64;
        loop {
            let res: Result<(u64, Vec<String>), _> = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg("gw:quota:*")
                .arg("COUNT")
                .arg(512)
                .query_async(&mut conn)
                .await;
            let Ok((next, keys)) = res else { return };
            if !keys.is_empty() {
                let _ = redis::cmd("UNLINK")
                    .arg(keys)
                    .query_async::<i64>(&mut conn)
                    .await;
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
    }
    async fn window_allow(&self, key: &str, limit: i64, window: Duration) -> bool {
        self.incr_window(&format!("gw:qpm:{key}"), 1, window).await <= limit
    }
    async fn token_window_check(&self, key: &str, limit: i64, window: Duration) -> bool {
        let mut conn = self.conn.clone();
        let used = redis::cmd("GET")
            .arg(format!("gw:tpm:{key}"))
            .query_async::<Option<i64>>(&mut conn)
            .await
            .ok()
            .flatten()
            .unwrap_or(0);
        let _ = window;
        used < limit
    }
    async fn token_window_reserve(
        &self,
        key: &str,
        amount: i64,
        limit: i64,
        window: Duration,
    ) -> bool {
        let mut conn = self.conn.clone();
        let script = redis::Script::new(
            "local v = redis.call('INCRBY', KEYS[1], ARGV[1])
             if v == tonumber(ARGV[1]) then redis.call('PEXPIRE', KEYS[1], ARGV[3]) end
             if v - tonumber(ARGV[1]) >= tonumber(ARGV[2]) then
               redis.call('DECRBY', KEYS[1], ARGV[1])
               return 0
             end
             return 1",
        );
        match script
            .key(format!("gw:tpm:{key}"))
            .arg(amount)
            .arg(limit)
            .arg(window.as_millis() as i64)
            .invoke_async::<i64>(&mut conn)
            .await
        {
            Ok(v) => v == 1,
            Err(e) => {
                tracing::warn!(error = %e, key, "redis tpm reserve failed; admitting");
                true
            }
        }
    }
    async fn token_window_settle(&self, key: &str, delta: i64, window: Duration) {
        if delta == 0 {
            return;
        }
        self.incr_window(&format!("gw:tpm:{key}"), delta, window)
            .await;
    }
    async fn token_window_add(&self, key: &str, tokens: i64, window: Duration) {
        self.incr_window(&format!("gw:tpm:{key}"), tokens, window)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Set GW_TEST_REDIS_URL (e.g. redis://127.0.0.1:16379) to run this.
    #[tokio::test]
    async fn redis_governance_enforces_limits() {
        let Ok(url) = std::env::var("GW_TEST_REDIS_URL") else {
            return;
        };
        let g = RedisGovernance::connect(&url).await.expect("redis connect");
        let ak = format!("t{}", std::process::id());
        g.quota_reset_all().await;
        assert!(g.quota_check(&ak, 10).await);
        g.quota_consume(&ak, 10).await;
        assert_eq!(g.quota_used(&ak).await, 10);
        assert!(!g.quota_check(&ak, 10).await);

        let rkey = format!("r{}", std::process::id());
        assert!(g.quota_reserve(&rkey, 300, 100).await);
        assert!(!g.quota_reserve(&rkey, 300, 100).await);
        g.quota_settle(&rkey, 15 - 300).await;
        assert_eq!(g.quota_used(&rkey).await, 15);
        assert!(
            g.token_window_reserve(&rkey, 300, 100, Duration::from_secs(60))
                .await
        );
        assert!(
            !g.token_window_reserve(&rkey, 300, 100, Duration::from_secs(60))
                .await
        );
        g.token_window_settle(&rkey, -300, Duration::from_secs(60))
            .await;
        assert!(
            g.token_window_reserve(&rkey, 300, 100, Duration::from_secs(60))
                .await
        );

        let mkey = format!("m{}", std::process::id());
        assert!(g.window_allow(&mkey, 1, Duration::from_secs(60)).await);
        assert!(!g.window_allow(&mkey, 1, Duration::from_secs(60)).await);

        assert!(g.token_window_check(&ak, 10, Duration::from_secs(60)).await);
        g.token_window_add(&ak, 10, Duration::from_secs(60)).await;
        assert!(!g.token_window_check(&ak, 10, Duration::from_secs(60)).await);
        g.quota_reset_all().await;
    }

    #[tokio::test]
    async fn reserve_then_settle_semantics() {
        let g = MemoryGovernance::default();
        assert!(g.quota_reserve("k", 300, 100).await, "admit while under");
        assert!(!g.quota_reserve("k", 300, 100).await, "in-flight counts");
        g.quota_settle("k", 15 - 300).await;
        assert_eq!(g.quota_used("k").await, 15);
        assert!(
            g.quota_reserve("k", 300, 100).await,
            "back under after settle"
        );
        g.quota_settle("k", -300).await;
        assert_eq!(g.quota_used("k").await, 15, "refund restores");

        let w = Duration::from_secs(60);
        assert!(g.token_window_reserve("t", 300, 100, w).await);
        assert!(!g.token_window_reserve("t", 300, 100, w).await);
        g.token_window_settle("t", -300, w).await;
        assert!(g.token_window_reserve("t", 300, 100, w).await);
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
