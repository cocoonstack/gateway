//! In-process gateway state, kept entirely in memory.
//!
//! No external storage: everything lives in process memory — an AK table, a
//! GCRA rate limiter, a daily token quota counter, a priority/round-robin
//! account pool, and a billing ledger. Layer L2.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ap_config::GatewayConfig;
use ap_consts::Protocol;
use ap_models::Account;
use dashmap::DashMap;

pub mod store;

pub use store::*;

const CACHE_MAX_ENTRIES: u64 = 10_000;

/// Resolved identity for an authenticated AK.
#[derive(Debug, Clone)]
pub struct AkInfo {
    pub ak: String,
    pub product: String,
    pub qps: f64,
    pub daily_token_quota: i64,
    /// tokens-per-minute window cap; None = unlimited.
    pub tokens_per_minute: Option<i64>,
}

/// AK auth: local key table.
#[derive(Debug, Default)]
pub struct AkAuth {
    keys: DashMap<String, AkInfo>,
}

impl AkAuth {
    pub fn authenticate(&self, ak: &str) -> Option<AkInfo> {
        self.keys.get(ak).map(|e| e.value().clone())
    }
}

/// Rate limiter (GCRA via governor), one limiter per AK.
#[derive(Default)]
pub struct RateLimiter {
    buckets: DashMap<String, Arc<governor::DefaultDirectRateLimiter>>,
}

impl RateLimiter {
    /// Take one permit. qps >= 1: replenish/burst = round(qps); 0 < qps < 1:
    /// one permit per 1/qps seconds (burst 1); qps <= 0: always denied.
    pub fn allow(&self, key: &str, qps: f64) -> bool {
        if qps <= 0.0 {
            return false;
        }
        let limiter = self
            .buckets
            .entry(key.to_owned())
            .or_insert_with(|| {
                let quota = if qps < 1.0 {
                    governor::Quota::with_period(Duration::from_secs_f64(1.0 / qps))
                        .unwrap_or_else(|| governor::Quota::per_second(NonZeroU32::MIN))
                } else {
                    let per_sec = qps.round().clamp(1.0, u32::MAX as f64) as u32;
                    governor::Quota::per_second(NonZeroU32::new(per_sec).unwrap_or(NonZeroU32::MIN))
                };
                Arc::new(governor::RateLimiter::direct(quota))
            })
            .clone();
        limiter.check().is_ok()
    }
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter")
            .field("keys", &self.buckets.len())
            .finish()
    }
}

/// Daily token quota accounting per AK; the daily reset is driven by
/// ap-task's periodic job calling `reset_all`.
#[derive(Debug, Default)]
pub struct QuotaStore {
    used: DashMap<String, i64>,
}

impl QuotaStore {
    pub fn used(&self, ak: &str) -> i64 {
        self.used.get(ak).map(|v| *v.value()).unwrap_or(0)
    }

    /// Pre-check: is there budget left before serving the request?
    pub fn check(&self, ak: &str, limit: i64) -> bool {
        self.used(ak) < limit
    }

    /// Post-consume the actual token usage.
    pub fn consume(&self, ak: &str, tokens: i64) {
        *self.used.entry(ak.to_owned()).or_insert(0) += tokens;
    }

    /// Daily reset. Driven by the ap-task background job as an in-process
    /// periodic task.
    pub fn reset_all(&self) {
        self.used.clear();
    }
}

/// Account pool: pick the highest-priority slot serving a model type, round-robin
/// within the tie.
#[derive(Debug, Default)]
pub struct AccountPool {
    accounts: Vec<Account>,
    rr: AtomicUsize,
}

impl AccountPool {
    pub fn select(&self, p: Protocol, provider: Option<&str>) -> Option<Account> {
        self.select_excluding(p, provider, &[])
    }

    /// Same as `select_excluding`, plus a health filter: accounts in cooldown are
    /// excluded.
    pub fn select_healthy(
        &self,
        p: Protocol,
        provider: Option<&str>,
        excluded: &[String],
        health: &AccountHealth,
    ) -> Option<Account> {
        let unhealthy: Vec<String> = self
            .accounts
            .iter()
            .filter(|a| a.protocols.contains(&p) && !health.available(&a.name))
            .map(|a| a.name.clone())
            .collect();
        let mut all_excluded = excluded.to_vec();
        all_excluded.extend(unhealthy);
        self.select_excluding(p, provider, &all_excluded)
    }

    /// PTU decision + failover exclusion: PTU (provisioned throughput)
    /// accounts are preferred over paygo; accounts in `excluded` (failed accounts
    /// from a failover) are skipped. Within a tier, pick by priority then round-robin.
    /// A model bound to a `provider` only uses that provider's accounts;
    /// unbound models draw from every account serving the protocol.
    pub fn select_excluding(
        &self,
        p: Protocol,
        provider: Option<&str>,
        excluded: &[String],
    ) -> Option<Account> {
        let candidates: Vec<&Account> = self
            .accounts
            .iter()
            .filter(|a| {
                a.protocols.contains(&p)
                    && !excluded.contains(&a.name)
                    && provider.is_none_or(|want| a.provider == want)
            })
            .collect();
        // PTU first, spill to paygo only when no PTU slot remains
        let tier: Vec<&Account> = {
            let ptu: Vec<&Account> = candidates.iter().copied().filter(|a| a.is_ptu()).collect();
            if ptu.is_empty() { candidates } else { ptu }
        };
        let best = tier.iter().map(|a| a.priority).min()?;
        let top: Vec<&Account> = tier
            .iter()
            .copied()
            .filter(|a| a.priority == best)
            .collect();
        let idx = self.rr.fetch_add(1, Ordering::Relaxed) % top.len();
        Some(top[idx].clone())
    }

    pub fn len(&self) -> usize {
        self.accounts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }
}

/// Fixed-window request counter, for model-level QPM.
#[derive(Debug, Default)]
pub struct WindowCounter {
    entries: DashMap<String, (Instant, i64)>,
}

impl WindowCounter {
    /// Take one permit in the current window; window resets after `window` elapses.
    pub fn allow(&self, key: &str, limit: i64, window: std::time::Duration) -> bool {
        let mut e = self
            .entries
            .entry(key.to_owned())
            .or_insert_with(|| (Instant::now(), 0));
        if e.0.elapsed() >= window {
            *e = (Instant::now(), 0);
        }
        if e.1 < limit {
            e.1 += 1;
            true
        } else {
            false
        }
    }
}

/// Fixed-window token accounting, for AK-level TPM.
#[derive(Debug, Default)]
pub struct TokenWindow {
    entries: DashMap<String, (Instant, i64)>,
}

impl TokenWindow {
    fn rotate(&self, key: &str, window: std::time::Duration) -> i64 {
        let mut e = self
            .entries
            .entry(key.to_owned())
            .or_insert_with(|| (Instant::now(), 0));
        if e.0.elapsed() >= window {
            *e = (Instant::now(), 0);
        }
        e.1
    }

    /// Pre-check: tokens already spent in this window are under the limit.
    pub fn check(&self, key: &str, limit: i64, window: std::time::Duration) -> bool {
        self.rotate(key, window) < limit
    }

    /// Post-add actual token usage.
    pub fn add(&self, key: &str, tokens: i64, window: std::time::Duration) {
        self.rotate(key, window);
        if let Some(mut e) = self.entries.get_mut(key) {
            e.1 += tokens;
        }
    }
}

/// Account health: consecutive-failure cooldown with auto-recovery.
#[derive(Debug, Default)]
pub struct AccountHealth {
    entries: DashMap<String, HealthEntry>,
}

#[derive(Debug, Default)]
struct HealthEntry {
    consecutive_failures: usize,
    cooldown_until: Option<Instant>,
}

impl AccountHealth {
    /// Record a failure; trips the account into cooldown at `threshold`.
    /// Returns true if this call tripped the cooldown.
    pub fn record_failure(
        &self,
        name: &str,
        threshold: usize,
        cooldown: std::time::Duration,
    ) -> bool {
        let mut e = self.entries.entry(name.to_owned()).or_default();
        e.consecutive_failures += 1;
        if e.consecutive_failures >= threshold && e.cooldown_until.is_none() {
            e.cooldown_until = Some(Instant::now() + cooldown);
            return true;
        }
        false
    }

    pub fn record_success(&self, name: &str) {
        if let Some(mut e) = self.entries.get_mut(name) {
            e.consecutive_failures = 0;
            e.cooldown_until = None;
        }
    }

    /// Available = not in an active cooldown (auto-recovers on expiry).
    pub fn available(&self, name: &str) -> bool {
        match self.entries.get(name) {
            Some(e) => match e.cooldown_until {
                Some(until) => Instant::now() >= until,
                None => true,
            },
            None => true,
        }
    }

    /// Health label for the accounts view: "ok" | "cooling".
    pub fn status(&self, name: &str) -> &'static str {
        if self.available(name) {
            "ok"
        } else {
            "cooling"
        }
    }
}

/// Request-level response cache with per-entry TTL and bounded capacity
/// (moka).
#[derive(Debug)]
pub struct ResponseCache {
    entries: moka::sync::Cache<String, (ap_models::GatewayResponse, Duration)>,
}

impl Default for ResponseCache {
    fn default() -> Self {
        Self {
            entries: moka::sync::Cache::builder()
                .max_capacity(CACHE_MAX_ENTRIES)
                .expire_after(PerEntryTtl)
                .build(),
        }
    }
}

impl ResponseCache {
    pub fn get(&self, key: &str) -> Option<ap_models::GatewayResponse> {
        self.entries.get(key).map(|(resp, _)| resp)
    }

    pub fn put(&self, key: String, resp: ap_models::GatewayResponse, ttl: Duration) {
        self.entries.insert(key, (resp, ttl));
    }
}

struct PerEntryTtl;

impl moka::Expiry<String, (ap_models::GatewayResponse, Duration)> for PerEntryTtl {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &(ap_models::GatewayResponse, Duration),
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(value.1)
    }

    // Re-putting a key resets its TTL (the pre-moka behavior); the default
    // would keep the original deadline.
    fn expire_after_update(
        &self,
        _key: &String,
        value: &(ap_models::GatewayResponse, Duration),
        _updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        Some(value.1)
    }
}

/// All gateway state, built once from config at startup.
#[derive(Debug)]
pub struct GatewayState {
    pub auth: AkAuth,
    pub limiter: RateLimiter,
    pub quota: QuotaStore,
    pub pool: AccountPool,
    /// Durable records (ledger/files/batches): memory by default, sqlite when
    /// `storage.sqlite_path` is configured (swapped in by the server at boot).
    pub store: Arc<dyn Store>,
    /// Model-level QPM window.
    pub qpm: WindowCounter,
    /// Product-level QPM window.
    pub product_qpm: WindowCounter,
    /// AK-level TPM window.
    pub tpm: TokenWindow,
    /// Account health (cooldown/recovery).
    pub health: AccountHealth,
    /// Request-level response cache.
    pub cache: ResponseCache,
}

impl Default for GatewayState {
    fn default() -> Self {
        Self {
            auth: AkAuth::default(),
            limiter: RateLimiter::default(),
            quota: QuotaStore::default(),
            pool: AccountPool::default(),
            store: Arc::new(MemoryStore::default()),
            qpm: WindowCounter::default(),
            product_qpm: WindowCounter::default(),
            tpm: TokenWindow::default(),
            health: AccountHealth::default(),
            cache: ResponseCache::default(),
        }
    }
}

impl GatewayState {
    pub fn from_config(cfg: &GatewayConfig) -> Self {
        let auth = AkAuth::default();
        for k in &cfg.access_keys {
            auth.keys.insert(
                k.ak.clone(),
                AkInfo {
                    ak: k.ak.clone(),
                    product: k.product.clone(),
                    qps: k.qps,
                    daily_token_quota: k.daily_token_quota,
                    tokens_per_minute: k.tokens_per_minute,
                },
            );
        }
        let accounts = cfg
            .accounts
            .iter()
            .map(|a| Account {
                name: a.name.clone(),
                provider: a.provider.clone(),
                priority: a.priority,
                tier: a.tier.clone(),
                endpoint: a.endpoint.clone(),
                api_key_env: a.api_key_env.clone(),
                secret_key_env: a.secret_key_env.clone(),
                // validated by GatewayConfig::validate, so unwrap-free filter is safe
                protocols: a
                    .protocols
                    .iter()
                    .filter_map(|w| Protocol::from_wire(w))
                    .collect(),
            })
            .collect();
        Self {
            auth,
            pool: AccountPool {
                accounts,
                rr: AtomicUsize::new(0),
            },
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> GatewayState {
        GatewayState::from_config(&GatewayConfig::embedded_default().unwrap())
    }

    #[test]
    fn fractional_and_zero_qps() {
        let rl = RateLimiter::default();
        // qps < 1: burst 1, second immediate call denied
        assert!(rl.allow("frac", 0.5));
        assert!(!rl.allow("frac", 0.5));
        // qps <= 0: always denied
        assert!(!rl.allow("zero", 0.0));
        assert!(!rl.allow("neg", -1.0));
    }

    #[test]
    fn auth_lookup() {
        let s = state();
        assert_eq!(s.auth.authenticate("ak-demo-123").unwrap().product, "demo");
        assert!(s.auth.authenticate("nope").is_none());
    }

    #[test]
    fn rate_limit_qps1_blocks_second_immediate_call() {
        let s = state();
        assert!(s.limiter.allow("k", 1.0));
        assert!(!s.limiter.allow("k", 1.0));
    }

    #[test]
    fn quota_check_and_consume() {
        let s = state();
        assert!(s.quota.check("a", 10));
        s.quota.consume("a", 10);
        assert!(!s.quota.check("a", 10));
    }

    #[test]
    fn pool_prefers_priority_then_round_robins() {
        let s = state();
        // openai-chat has priority-1 mock-openai-1 and priority-2 mock-openai-2
        let a = s.pool.select(Protocol::OpenaiChat, Some("openai")).unwrap();
        let b = s.pool.select(Protocol::OpenaiChat, Some("openai")).unwrap();
        assert_eq!(a.name, "mock-openai-1");
        assert_eq!(b.name, "mock-openai-1"); // only one slot at best priority
        assert_eq!(
            s.pool
                .select(Protocol::AnthropicMessages, None)
                .unwrap()
                .name,
            "mock-anthropic-1"
        );
        // no account in the embedded config serves this provider binding
        assert!(
            s.pool
                .select(Protocol::Video, Some("nonexistent"))
                .is_none()
        );
    }

    #[test]
    fn pool_prefers_ptu_then_excludes_failed() {
        let s = state();
        // hunyuan: the ptu account (named "down") is preferred over paygo
        let first = s
            .pool
            .select(Protocol::OpenaiChat, Some("tencent"))
            .unwrap();
        assert_eq!(first.name, "mock-hunyuan-ptu-down");
        assert!(first.is_ptu());
        // after failover exclusion it falls back to paygo
        let next = s
            .pool
            .select_excluding(
                Protocol::OpenaiChat,
                Some("tencent"),
                &["mock-hunyuan-ptu-down".into()],
            )
            .unwrap();
        assert_eq!(next.name, "mock-hunyuan-paygo");
        assert!(!next.is_ptu());
    }
}
