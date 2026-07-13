//! Rate/quota governance behind a trait so a single-node deployment uses
//! in-process counters and a multi-replica deployment shares state in Redis.
//!
//! [`MemoryGovernance`] is the default; [`RedisGovernance`] keeps the same
//! semantics using atomic server-side operations (INCR + EXPIRE windows,
//! token-bucket via a Lua script).

use std::time::Duration;

use async_trait::async_trait;

use crate::{QuotaStore, RateLimiter, TokenWindow, WindowCounter};

/// Day-keyed quota buckets linger at most this long before self-expiring.
const QUOTA_TTL_MS: i64 = 2 * 24 * 60 * 60 * 1000;

/// The Redis daily-quota key for `key` on the UTC day of `at_epoch_secs` (unix
/// epoch is UTC-midnight aligned, so `/ 86400` increments exactly at midnight).
/// Rollover is implicit and identical across replicas — no reset job, no shared-
/// keyspace wipe. Callers pass the admission time so a reserve and its settle
/// hit the same day even across a midnight boundary.
fn quota_key_at(key: &str, at_epoch_secs: i64) -> String {
    format!("gw:quota:{}:{key}", at_epoch_secs / 86_400)
}

/// The day bucket for "now" — used by the single-shot reads/writes that aren't
/// part of a reserve/settle pair.
fn quota_key(key: &str) -> String {
    quota_key_at(key, crate::epoch_secs())
}

/// The governance operations the request pipeline calls.
#[async_trait]
pub trait Governance: Send + Sync + std::fmt::Debug {
    /// Rate limit: take one permit at `qps` for `key`.
    async fn rate_allow(&self, key: &str, qps: f64) -> bool;

    /// Daily quota: is `ak` under `limit`?
    async fn quota_check(&self, ak: &str, limit: i64) -> bool;
    /// Admission with reservation: admit while spent-before < `limit`, and
    /// atomically add `amount` so concurrent in-flight requests count against
    /// the budget. False = rejected (nothing reserved). `at_epoch_secs` pins the
    /// day bucket so a settle lands on the same day this reserve did, even if the
    /// request straddles UTC midnight.
    async fn quota_reserve(&self, key: &str, amount: i64, limit: i64, at_epoch_secs: i64) -> bool;
    /// Apply the settle delta (actual - reserved; negative refunds) to the day
    /// bucket the paired reserve used (`at_epoch_secs`).
    async fn quota_settle(&self, key: &str, delta: i64, at_epoch_secs: i64);
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
    async fn quota_reserve(&self, key: &str, amount: i64, limit: i64, _at: i64) -> bool {
        self.quota.reserve(key, amount, limit)
    }
    async fn quota_settle(&self, key: &str, delta: i64, _at: i64) {
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
        Ok(Self {
            conn: crate::redis_connect(url).await?,
        })
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
            .arg(quota_key(ak))
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
    async fn quota_reserve(&self, key: &str, amount: i64, limit: i64, at: i64) -> bool {
        let mut conn = self.conn.clone();
        // admit while spent-before < limit; the reservation itself may cross
        // the limit (same one-request overshoot the settle corrects). The
        // date-stamped key self-expires, so arm a TTL on first use.
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
            .key(quota_key_at(key, at))
            .arg(amount)
            .arg(limit)
            .arg(QUOTA_TTL_MS)
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
    async fn quota_settle(&self, key: &str, delta: i64, at: i64) {
        if delta == 0 {
            return;
        }
        // floor at 0 atomically, on the SAME day bucket the reserve used (`at`),
        // so a request that straddles midnight doesn't apply its negative delta
        // to the next day's counter.
        settle_floored(
            &self.conn,
            &quota_key_at(key, at),
            delta,
            Some(Duration::from_millis(QUOTA_TTL_MS as u64)),
        )
        .await;
    }
    async fn quota_consume(&self, ak: &str, tokens: i64) {
        self.incr_window(
            &quota_key(ak),
            tokens,
            Duration::from_millis(QUOTA_TTL_MS as u64),
        )
        .await;
    }
    async fn quota_reset_all(&self) {
        // No-op: quota keys are date-stamped by UTC day (see `quota_key`), so the
        // daily counter rolls over automatically at midnight for every replica.
        // The old per-instance sweep wiped the whole shared keyspace, so staggered
        // instances reset each other's counters multiple times a day.
    }
    async fn window_allow(&self, key: &str, limit: i64, window: Duration) -> bool {
        self.incr_window(&format!("gw:qpm:{key}"), 1, window).await <= limit
    }
    async fn token_window_check(&self, key: &str, limit: i64, _window: Duration) -> bool {
        let mut conn = self.conn.clone();
        let used = redis::cmd("GET")
            .arg(format!("gw:tpm:{key}"))
            .query_async::<Option<i64>>(&mut conn)
            .await
            .ok()
            .flatten()
            .unwrap_or(0);
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
        settle_floored(&self.conn, &format!("gw:tpm:{key}"), delta, Some(window)).await;
    }
    async fn token_window_add(&self, key: &str, tokens: i64, window: Duration) {
        self.incr_window(&format!("gw:tpm:{key}"), tokens, window)
            .await;
    }
}

/// Apply a settle delta and floor the counter at 0 in one atomic step, so a
/// key reset or window rollover between reserve and settle can't plant a
/// negative value that over-admits. Preserves an existing TTL, or arms one
/// when `window` is given and the key was absent.
async fn settle_floored(
    conn: &redis::aio::ConnectionManager,
    key: &str,
    delta: i64,
    window: Option<Duration>,
) {
    let mut conn = conn.clone();
    let script = redis::Script::new(
        "local v = redis.call('INCRBY', KEYS[1], ARGV[1])
         if v < 0 then redis.call('SET', KEYS[1], 0); v = 0 end
         if ARGV[2] ~= '0' and redis.call('PTTL', KEYS[1]) < 0 then
           redis.call('PEXPIRE', KEYS[1], ARGV[2])
         end
         return v",
    );
    let px = window.map(|w| w.as_millis() as i64).unwrap_or(0);
    if let Err(e) = script
        .key(key)
        .arg(delta)
        .arg(px)
        .invoke_async::<i64>(&mut conn)
        .await
    {
        tracing::warn!(error = %e, key, "redis settle failed");
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
        let now = crate::epoch_secs();
        assert!(g.quota_reserve(&rkey, 300, 100, now).await);
        assert!(!g.quota_reserve(&rkey, 300, 100, now).await);
        g.quota_settle(&rkey, 15 - 300, now).await;
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
        let now = crate::epoch_secs();
        assert!(
            g.quota_reserve("k", 300, 100, now).await,
            "admit while under"
        );
        assert!(
            !g.quota_reserve("k", 300, 100, now).await,
            "in-flight counts"
        );
        g.quota_settle("k", 15 - 300, now).await;
        assert_eq!(g.quota_used("k").await, 15);
        assert!(
            g.quota_reserve("k", 300, 100, now).await,
            "back under after settle"
        );
        g.quota_settle("k", -300, now).await;
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

    #[test]
    fn quota_key_pins_the_admission_day() {
        // the last second of a UTC day and the first of the next map to distinct
        // buckets — so a settle replaying the reserve's timestamp lands on the
        // reserve's day even after "now" has rolled past midnight
        assert_ne!(quota_key_at("k", 86_400 - 1), quota_key_at("k", 86_400));
        // any two timestamps within one UTC day share a bucket
        assert_eq!(quota_key_at("k", 60), quota_key_at("k", 86_400 - 1));
    }
}
