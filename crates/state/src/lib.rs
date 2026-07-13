//! Gateway state: AK table, account pool, account health, response cache, and
//! two pluggable seams — the durable [`Store`] and rate/quota [`Governance`].
//! Both default to in-process; the store can be SQLite and governance can be
//! Redis for multi-replica deployments. Layer L2.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use gw_config::GatewayConfig;
use gw_consts::Protocol;
use gw_models::Account;

pub mod governance;
pub mod store;

pub use governance::{Governance, MemoryGovernance, RedisGovernance};
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
/// gw-task's periodic job calling `reset_all`.
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

    /// Daily reset. Driven by the gw-task background job as an in-process
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

    /// Post-add actual token usage. Rotation and increment happen under one
    /// entry guard so a concurrent rollover cannot land the tokens in the
    /// wrong window.
    pub fn add(&self, key: &str, tokens: i64, window: std::time::Duration) {
        let mut e = self
            .entries
            .entry(key.to_owned())
            .or_insert_with(|| (Instant::now(), 0));
        if e.0.elapsed() >= window {
            *e = (Instant::now(), 0);
        }
        e.1 += tokens;
    }
}

#[derive(Debug, Default)]
struct HealthEntry {
    consecutive_failures: usize,
    cooldown_until: Option<Instant>,
}

/// Account health: consecutive-failure cooldown with auto-recovery.
#[derive(Debug, Default)]
pub struct AccountHealth {
    entries: DashMap<String, HealthEntry>,
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
        // An expired cooldown re-arms the breaker: a still-failing account must
        // re-enter cooldown after its recovery probe fails, not latch open forever.
        let armed = e.cooldown_until.is_none_or(|until| Instant::now() >= until);
        if e.consecutive_failures >= threshold && armed {
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
    entries: moka::sync::Cache<String, (gw_models::GatewayResponse, Duration)>,
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
    pub fn get(&self, key: &str) -> Option<gw_models::GatewayResponse> {
        self.entries.get(key).map(|(resp, _)| resp)
    }

    pub fn put(&self, key: String, resp: gw_models::GatewayResponse, ttl: Duration) {
        self.entries.insert(key, (resp, ttl));
    }
}

struct PerEntryTtl;

impl moka::Expiry<String, (gw_models::GatewayResponse, Duration)> for PerEntryTtl {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &(gw_models::GatewayResponse, Duration),
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(value.1)
    }

    // Re-putting a key resets its TTL (the pre-moka behavior); the default
    // would keep the original deadline.
    fn expire_after_update(
        &self,
        _key: &String,
        value: &(gw_models::GatewayResponse, Duration),
        _updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        Some(value.1)
    }
}

/// All gateway state. `auth` and `pool` are config-derived and rebuilt on a
/// live reload; the other four are runtime seams preserved across reloads.
#[derive(Debug)]
pub struct GatewayState {
    pub auth: AkAuth,
    pub pool: AccountPool,
    pub governance: Arc<dyn Governance>,
    /// Durable records (ledger/files/batches): memory by default, sqlite when
    /// `storage.sqlite_path` is configured (swapped in by the server at boot).
    pub store: Arc<dyn Store>,
    /// Account health (cooldown/recovery).
    pub health: Arc<AccountHealth>,
    /// Request-level response cache.
    pub cache: Arc<ResponseCache>,
}

impl Default for GatewayState {
    fn default() -> Self {
        Self {
            auth: AkAuth::default(),
            pool: AccountPool::default(),
            governance: Arc::new(MemoryGovernance::default()),
            store: Arc::new(MemoryStore::default()),
            health: Arc::new(AccountHealth::default()),
            cache: Arc::new(ResponseCache::default()),
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

    /// Rebuild the config-derived state (AK table, account pool) from `cfg`
    /// while preserving the runtime seams — governance, store, account health,
    /// and the response cache — from `prev`. In-flight requests keep running on
    /// their own snapshot; new requests see this one.
    pub fn reload_from(cfg: &GatewayConfig, prev: &GatewayState) -> Self {
        let fresh = Self::from_config(cfg);
        Self {
            auth: fresh.auth,
            pool: fresh.pool,
            governance: prev.governance.clone(),
            store: prev.store.clone(),
            health: prev.health.clone(),
            cache: prev.cache.clone(),
        }
    }
}

/// A consistent (config, derived-state) pair. A request loads one snapshot and
/// runs to completion on it, so a mid-flight reload never splits its view.
#[derive(Debug)]
pub struct Snapshot {
    pub cfg: Arc<GatewayConfig>,
    pub state: Arc<GatewayState>,
}

/// Lock-free live configuration: `load()` on the hot path is a pointer read;
/// `reload()` atomically swaps in a fresh snapshot (config-derived state
/// rebuilt, seams preserved).
#[derive(Clone)]
pub struct SharedConfig {
    inner: Arc<arc_swap::ArcSwap<Snapshot>>,
}

impl SharedConfig {
    pub fn new(cfg: Arc<GatewayConfig>, state: Arc<GatewayState>) -> Self {
        Self {
            inner: Arc::new(arc_swap::ArcSwap::from_pointee(Snapshot { cfg, state })),
        }
    }

    /// The current snapshot — a cheap atomic load; hold it for the whole request.
    pub fn load(&self) -> Arc<Snapshot> {
        self.inner.load_full()
    }

    /// Swap in a new config, rebuilding derived state and preserving seams.
    pub fn reload(&self, cfg: GatewayConfig) {
        let prev = self.inner.load();
        let state = GatewayState::reload_from(&cfg, &prev.state);
        self.inner.store(Arc::new(Snapshot {
            cfg: Arc::new(cfg),
            state: Arc::new(state),
        }));
    }
}

impl std::fmt::Debug for SharedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SharedConfig")
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

    #[tokio::test]
    async fn reload_swaps_keys_but_preserves_seams() {
        let cfg = Arc::new(GatewayConfig::embedded_default().unwrap());
        let boot = Arc::new(GatewayState::from_config(&cfg));
        // write a durable record through the seam so we can prove it survives
        let file_id = boot
            .store
            .file_put("batch", "keep me".into())
            .await
            .unwrap()
            .id;
        let store_ptr = Arc::as_ptr(&boot.store);
        let health_ptr = Arc::as_ptr(&boot.health);
        let shared = SharedConfig::new(cfg, boot);

        // the boot config has ak-demo-123 but not ak-new
        let snap = shared.load();
        assert!(snap.state.auth.authenticate("ak-demo-123").is_some());
        assert!(snap.state.auth.authenticate("ak-new").is_none());

        // reload with a config carrying a different key set
        let new_cfg = GatewayConfig::from_yaml(
            "listen: {host: h, port: 1}\naccess_keys: [{ak: ak-new, product: p, qps: 5, daily_token_quota: 100}]",
        )
        .unwrap();
        shared.reload(new_cfg);

        let snap = shared.load();
        // config-derived state rebuilt: new key present, old gone
        assert!(snap.state.auth.authenticate("ak-new").is_some());
        assert!(snap.state.auth.authenticate("ak-demo-123").is_none());
        // seams preserved: same store/health instances, durable record intact
        assert_eq!(Arc::as_ptr(&snap.state.store), store_ptr);
        assert_eq!(Arc::as_ptr(&snap.state.health), health_ptr);
        assert!(
            snap.state.store.file_get(&file_id).await.unwrap().is_some(),
            "durable file survived the reload"
        );
    }

    #[test]
    fn auth_lookup() {
        let s = state();
        assert_eq!(s.auth.authenticate("ak-demo-123").unwrap().product, "demo");
        assert!(s.auth.authenticate("nope").is_none());
    }

    #[tokio::test]
    async fn governance_rate_and_quota_via_state() {
        let s = state();
        assert!(s.governance.rate_allow("k", 1.0).await);
        assert!(!s.governance.rate_allow("k", 1.0).await);
        assert!(s.governance.quota_check("a", 10).await);
        s.governance.quota_consume("a", 10).await;
        assert!(!s.governance.quota_check("a", 10).await);
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
    fn cooldown_rearms_after_expiry() {
        let h = AccountHealth::default();
        let cd = Duration::from_millis(10);
        assert!(!h.record_failure("acc", 2, cd));
        assert!(h.record_failure("acc", 2, cd), "threshold trips cooldown");
        assert!(!h.available("acc"));
        std::thread::sleep(Duration::from_millis(15));
        assert!(h.available("acc"), "cooldown auto-recovers");
        // recovery probe fails again: the breaker must re-trip, not latch open
        assert!(h.record_failure("acc", 2, cd), "expired cooldown re-arms");
        assert!(!h.available("acc"));
        h.record_success("acc");
        assert!(h.available("acc"));
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
