//! Gateway state: AK table, account pool, account health, response cache,
//! behind pluggable seams ([`KeyStore`], [`Store`], [`Governance`],
//! [`HealthStore`], the config store). Everything defaults to in-process;
//! Postgres/Redis back them for multi-replica deployments. Layer L2.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use gw_config::GatewayConfig;
use gw_consts::Protocol;
use gw_models::Account;

pub mod configstore;
pub mod governance;
pub mod health;
pub mod keystore;
pub mod store;

pub use configstore::{CONFIG_CHANNEL, PostgresConfigStore};
pub use governance::{Governance, MemoryGovernance, RedisGovernance};
pub use health::{HealthStore, RedisHealth};
pub use keystore::{KeyStore, PostgresKeyStore};
pub use store::*;

const CACHE_MAX_ENTRIES: u64 = 10_000;

/// Resolved identity for an authenticated AK.
#[derive(Debug, Clone)]
pub struct AkInfo {
    pub ak: String,
    pub product: String,
    /// Tenant this key belongs to (`gw_config::DEFAULT_TENANT` when undeclared).
    pub tenant: String,
    pub qps: f64,
    pub daily_token_quota: i64,
    /// tokens-per-minute window cap; None = unlimited.
    pub tokens_per_minute: Option<i64>,
    /// Unix seconds after which the key stops authenticating; None = never.
    pub expires_at_epoch_secs: Option<i64>,
    /// A banned key stays in the table but fails auth with a distinct 403.
    pub banned: bool,
    /// Per-model daily token caps overriding the tenant defaults; empty = none.
    /// Arc'd: `AkInfo` is cloned per request and the map is write-rare.
    pub model_quotas: Arc<std::collections::HashMap<String, i64>>,
}

impl AkInfo {
    /// Lifecycle state at `now` (unix seconds). Ban wins over expiry.
    pub fn status_at(&self, now_epoch_secs: i64) -> KeyStatus {
        if self.banned {
            return KeyStatus::Banned;
        }
        match self.expires_at_epoch_secs {
            Some(t) if now_epoch_secs >= t => KeyStatus::Expired,
            _ => KeyStatus::Active,
        }
    }
}

impl From<&gw_config::AkConf> for AkInfo {
    fn from(k: &gw_config::AkConf) -> Self {
        Self {
            ak: k.ak.clone(),
            product: k.product.clone(),
            tenant: k.tenant.clone(),
            qps: k.qps,
            daily_token_quota: k.daily_token_quota,
            tokens_per_minute: k.tokens_per_minute,
            expires_at_epoch_secs: k.expires_at_epoch_secs,
            banned: k.banned,
            model_quotas: Arc::new(k.model_quotas.clone()),
        }
    }
}

/// Key lifecycle state: expired and banned keys authenticate to distinct 403s.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyStatus {
    Active,
    Banned,
    Expired,
}

/// Wrap a sqlx error as an internal gateway error with context.
pub(crate) fn sqlx_err(what: &str, e: sqlx::Error) -> gw_models::GatewayError {
    gw_models::GatewayError::internal(what).with_source(e)
}

/// Current unix seconds (0 if the clock reads before the epoch).
pub fn epoch_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Where a key came from: the config file (re-applied on reload) or the admin
/// API at runtime (kept across a config reload).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeySource {
    Config,
    Admin,
}

/// AK auth: the live key table. Config keys seed it at boot and are re-applied
/// on reload; admin keys are created/updated/revoked at runtime and survive a
/// reload. A preserved seam, so those runtime edits outlive a config swap.
#[derive(Debug, Default)]
pub struct AkAuth {
    keys: DashMap<String, (AkInfo, KeySource)>,
}

impl AkAuth {
    pub fn authenticate(&self, ak: &str) -> Option<AkInfo> {
        self.keys.get(ak).map(|e| e.value().0.clone())
    }

    /// Insert or replace a key. Config ownership is sticky: an admin write to a
    /// config-declared key updates its values but keeps it `Config`, so removing
    /// it from the config file and reloading still revokes it. Config can claim
    /// an admin key (when the file starts declaring it). Atomic via the entry
    /// lock so the source can't flip under a concurrent write.
    pub fn put(&self, info: AkInfo, source: KeySource) {
        use dashmap::mapref::entry::Entry;
        match self.keys.entry(info.ak.clone()) {
            Entry::Occupied(mut e) => {
                let sticky_config = e.get().1 == KeySource::Config && source == KeySource::Admin;
                let source = if sticky_config {
                    KeySource::Config
                } else {
                    source
                };
                e.insert((info, source));
            }
            Entry::Vacant(e) => {
                e.insert((info, source));
            }
        }
    }

    /// Update quota/lifecycle fields of an existing key in place; returns the
    /// new view. `None` if the key doesn't exist.
    #[allow(clippy::too_many_arguments)]
    pub fn patch(
        &self,
        ak: &str,
        qps: Option<f64>,
        daily_token_quota: Option<i64>,
        tokens_per_minute: Option<Option<i64>>,
        expires_at_epoch_secs: Option<Option<i64>>,
        banned: Option<bool>,
    ) -> Option<AkInfo> {
        let mut e = self.keys.get_mut(ak)?;
        if let Some(v) = qps {
            e.0.qps = v;
        }
        if let Some(v) = daily_token_quota {
            e.0.daily_token_quota = v;
        }
        if let Some(v) = tokens_per_minute {
            e.0.tokens_per_minute = v;
        }
        if let Some(v) = expires_at_epoch_secs {
            e.0.expires_at_epoch_secs = v;
        }
        if let Some(v) = banned {
            e.0.banned = v;
        }
        Some(e.0.clone())
    }

    /// Every key in the table, sorted by ak for stable listings.
    pub fn list(&self) -> Vec<AkInfo> {
        let mut keys: Vec<AkInfo> = self.keys.iter().map(|e| e.value().0.clone()).collect();
        keys.sort_by(|a, b| a.ak.cmp(&b.ak));
        keys
    }

    /// Remove a key regardless of source; returns whether it existed.
    pub fn revoke(&self, ak: &str) -> bool {
        self.keys.remove(ak).is_some()
    }

    /// Re-apply the config file's key set, leaving admin-created keys untouched.
    /// Surviving config keys are upserted in place (never briefly absent, so a
    /// concurrent `authenticate` can't spuriously 401 during a reload); only
    /// config keys dropped from the new config are removed.
    pub fn reload_config_keys(&self, keys: &[gw_config::AkConf]) {
        let wanted: std::collections::HashSet<&str> = keys.iter().map(|k| k.ak.as_str()).collect();
        self.keys
            .retain(|ak, (_, src)| *src == KeySource::Admin || wanted.contains(ak.as_str()));
        for k in keys {
            self.put(AkInfo::from(k), KeySource::Config);
        }
    }
}

#[async_trait::async_trait]
impl KeyStore for AkAuth {
    async fn authenticate(&self, ak: &str) -> Option<AkInfo> {
        AkAuth::authenticate(self, ak)
    }
    async fn put(&self, info: AkInfo, source: KeySource) -> gw_models::GResult<()> {
        AkAuth::put(self, info, source);
        Ok(())
    }
    async fn patch(
        &self,
        ak: &str,
        qps: Option<f64>,
        daily_token_quota: Option<i64>,
        tokens_per_minute: Option<Option<i64>>,
        expires_at_epoch_secs: Option<Option<i64>>,
        banned: Option<bool>,
    ) -> gw_models::GResult<Option<AkInfo>> {
        Ok(AkAuth::patch(
            self,
            ak,
            qps,
            daily_token_quota,
            tokens_per_minute,
            expires_at_epoch_secs,
            banned,
        ))
    }
    async fn revoke(&self, ak: &str) -> gw_models::GResult<bool> {
        Ok(AkAuth::revoke(self, ak))
    }
    async fn list(&self) -> gw_models::GResult<Vec<AkInfo>> {
        Ok(AkAuth::list(self))
    }
    async fn reload_config_keys(&self, keys: &[gw_config::AkConf]) -> gw_models::GResult<()> {
        AkAuth::reload_config_keys(self, keys);
        Ok(())
    }
}

/// Rate limiter (GCRA via governor), one limiter per AK.
#[derive(Default)]
pub struct RateLimiter {
    buckets: DashMap<String, (f64, Arc<governor::DefaultDirectRateLimiter>)>,
}

impl RateLimiter {
    /// Take one permit. qps >= 1: replenish/burst = round(qps); 0 < qps < 1:
    /// one permit per 1/qps seconds (burst 1); qps <= 0: always denied.
    /// A bucket whose stored qps differs is rebuilt, so an admin PATCH or a
    /// config reload takes effect immediately (a rebuilt bucket starts full).
    pub fn allow(&self, key: &str, qps: f64) -> bool {
        if qps <= 0.0 {
            return false;
        }
        let mut e = self
            .buckets
            .entry(key.to_owned())
            .or_insert_with(|| (qps, new_bucket(qps)));
        if e.0 != qps {
            *e = (qps, new_bucket(qps));
        }
        let limiter = e.1.clone();
        drop(e);
        limiter.check().is_ok()
    }
}

fn new_bucket(qps: f64) -> Arc<governor::DefaultDirectRateLimiter> {
    let quota = if qps < 1.0 {
        governor::Quota::with_period(Duration::from_secs_f64(1.0 / qps))
            .unwrap_or_else(|| governor::Quota::per_second(NonZeroU32::MIN))
    } else {
        let per_sec = qps.round().clamp(1.0, u32::MAX as f64) as u32;
        governor::Quota::per_second(NonZeroU32::new(per_sec).unwrap_or(NonZeroU32::MIN))
    };
    Arc::new(governor::RateLimiter::direct(quota))
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
    /// Build the pool from config accounts (protocols validated at config load,
    /// so the unwrap-free filter drops nothing).
    pub fn from_config(cfg: &GatewayConfig) -> Self {
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
                protocols: a
                    .protocols
                    .iter()
                    .filter_map(|w| Protocol::from_wire(w))
                    .collect(),
            })
            .collect();
        Self {
            accounts,
            rr: AtomicUsize::new(0),
        }
    }

    pub fn select(&self, p: Protocol, provider: Option<&str>) -> Option<Account> {
        self.select_excluding(p, provider, &[])
    }

    /// Same as `select_excluding`, plus a health filter: accounts in cooldown are
    /// excluded.
    pub async fn select_healthy(
        &self,
        p: Protocol,
        provider: Option<&str>,
        excluded: &[String],
        health: &dyn HealthStore,
    ) -> Option<Account> {
        let candidates: Vec<&Account> = self
            .accounts
            .iter()
            .filter(|a| a.protocols.contains(&p))
            .collect();
        let checks =
            futures::future::join_all(candidates.iter().map(|a| health.available(&a.name))).await;
        let unhealthy: Vec<String> = candidates
            .iter()
            .zip(checks)
            .filter(|(_, ok)| !ok)
            .map(|(a, _)| a.name.clone())
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
        // an expired cooldown re-arms: a still-failing account re-trips, not latches open
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

    // re-putting resets the TTL; moka's default would keep the original deadline
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
    /// Live key table — a preserved seam (admin key edits survive a reload).
    /// In-memory by default; Postgres for a fleet-shared key set.
    pub auth: Arc<dyn KeyStore>,
    pub pool: AccountPool,
    pub governance: Arc<dyn Governance>,
    /// Durable records (ledger/files/batches): memory by default, sqlite when
    /// `storage.sqlite_path` is configured (swapped in by the server at boot).
    pub store: Arc<dyn Store>,
    /// Account health (cooldown/recovery): in-process by default, Redis for
    /// fleet-wide cooldown.
    pub health: Arc<dyn HealthStore>,
    /// Request-level response cache.
    pub cache: Arc<ResponseCache>,
}

impl Default for GatewayState {
    fn default() -> Self {
        Self {
            auth: Arc::new(AkAuth::default()),
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
            auth.put(AkInfo::from(k), KeySource::Config);
        }
        Self {
            auth: Arc::new(auth),
            pool: AccountPool::from_config(cfg),
            ..Default::default()
        }
    }

    /// Rebuild the config-derived account pool from `cfg` while preserving the
    /// runtime seams — key table, governance, store, account health, and the
    /// response cache — from `prev`. The key table's config-sourced entries are
    /// re-applied from the new config; admin-created keys are kept. In-flight
    /// requests keep running on their own snapshot; new requests see this one.
    /// Fails (leaving `prev` live) when a networked key table can't be updated.
    pub async fn reload_from(cfg: &GatewayConfig, prev: &GatewayState) -> gw_models::GResult<Self> {
        prev.auth.reload_config_keys(&cfg.access_keys).await?;
        Ok(Self {
            auth: prev.auth.clone(),
            pool: AccountPool::from_config(cfg),
            governance: prev.governance.clone(),
            store: prev.store.clone(),
            health: prev.health.clone(),
            cache: prev.cache.clone(),
        })
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
    /// Serializes reloads: two divergent concurrent reloads (SIGHUP racing the
    /// config feed) would otherwise interleave key-table writes and could pair
    /// one reload's cfg pointer with the other's key set.
    reload_lock: Arc<tokio::sync::Mutex<()>>,
}

impl SharedConfig {
    pub fn new(cfg: Arc<GatewayConfig>, state: Arc<GatewayState>) -> Self {
        Self {
            inner: Arc::new(arc_swap::ArcSwap::from_pointee(Snapshot { cfg, state })),
            reload_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// The current snapshot — a cheap atomic load; hold it for the whole request.
    pub fn load(&self) -> Arc<Snapshot> {
        self.inner.load_full()
    }

    /// Swap in a new config, rebuilding derived state and preserving seams.
    /// On error (networked key table unreachable) the old snapshot stays live.
    pub async fn reload(&self, cfg: GatewayConfig) -> gw_models::GResult<()> {
        let _serialized = self.reload_lock.lock().await;
        let prev = self.inner.load();
        let state = GatewayState::reload_from(&cfg, &prev.state).await?;
        self.inner.store(Arc::new(Snapshot {
            cfg: Arc::new(cfg),
            state: Arc::new(state),
        }));
        Ok(())
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
    fn rate_limit_qps_change_takes_effect_immediately() {
        let rl = RateLimiter::default();
        assert!(rl.allow("k", 1.0));
        assert!(!rl.allow("k", 1.0), "second call at qps=1 denied");
        assert!(rl.allow("k", 100.0), "raised qps rebuilds the bucket");
        assert!(rl.allow("k", 100.0));
        assert!(rl.allow("k", 0.5), "lowered qps rebuilds too (burst 1)");
        assert!(!rl.allow("k", 0.5), "and throttles at the new rate");
    }

    #[test]
    fn fractional_and_zero_qps() {
        let rl = RateLimiter::default();
        assert!(rl.allow("frac", 0.5));
        assert!(!rl.allow("frac", 0.5));
        assert!(!rl.allow("zero", 0.0));
        assert!(!rl.allow("neg", -1.0));
    }

    #[tokio::test]
    async fn reload_swaps_keys_but_preserves_seams() {
        let cfg = Arc::new(GatewayConfig::embedded_default().unwrap());
        let boot = Arc::new(GatewayState::from_config(&cfg));
        let file_id = boot
            .store
            .file_put("batch", "keep me".into())
            .await
            .unwrap()
            .id;
        let store_ptr = Arc::as_ptr(&boot.store);
        let health_ptr = Arc::as_ptr(&boot.health);
        let shared = SharedConfig::new(cfg, boot);

        let snap = shared.load();
        assert!(snap.state.auth.authenticate("ak-demo-123").await.is_some());
        assert!(snap.state.auth.authenticate("ak-new").await.is_none());

        let new_cfg = GatewayConfig::from_yaml(
            "listen: {host: h, port: 1}\naccess_keys: [{ak: ak-new, product: p, qps: 5, daily_token_quota: 100}]",
        )
        .unwrap();
        shared.reload(new_cfg).await.unwrap();

        let snap = shared.load();
        assert!(snap.state.auth.authenticate("ak-new").await.is_some());
        assert!(snap.state.auth.authenticate("ak-demo-123").await.is_none());
        assert_eq!(Arc::as_ptr(&snap.state.store), store_ptr);
        assert_eq!(Arc::as_ptr(&snap.state.health), health_ptr);
        assert!(
            snap.state.store.file_get(&file_id).await.unwrap().is_some(),
            "durable file survived the reload"
        );
    }

    #[test]
    fn admin_keys_survive_reload_config_keys_do_not() {
        let auth = AkAuth::default();
        auth.put(
            AkInfo {
                ak: "ak-config".into(),
                product: "p".into(),
                tenant: "default".into(),
                qps: 1.0,
                daily_token_quota: 10,
                tokens_per_minute: None,
                expires_at_epoch_secs: None,
                banned: false,
                model_quotas: Default::default(),
            },
            KeySource::Config,
        );
        auth.put(
            AkInfo {
                ak: "ak-admin".into(),
                product: "p".into(),
                tenant: "default".into(),
                qps: 1.0,
                daily_token_quota: 10,
                tokens_per_minute: None,
                expires_at_epoch_secs: None,
                banned: false,
                model_quotas: Default::default(),
            },
            KeySource::Admin,
        );
        let new = GatewayConfig::from_yaml(
            "listen: {host: h, port: 1}\naccess_keys: [{ak: ak-config2, product: p, qps: 2, daily_token_quota: 20}]",
        )
        .unwrap();
        auth.reload_config_keys(&new.access_keys);
        assert!(
            auth.authenticate("ak-config").is_none(),
            "old config key dropped"
        );
        assert!(
            auth.authenticate("ak-config2").is_some(),
            "new config key applied"
        );
        assert!(
            auth.authenticate("ak-admin").is_some(),
            "admin key preserved"
        );
        let patched = auth
            .patch("ak-admin", Some(9.0), None, Some(Some(5)), None, Some(true))
            .unwrap();
        assert_eq!(patched.qps, 9.0);
        assert_eq!(patched.tokens_per_minute, Some(5));
        assert!(patched.banned);
        assert!(auth.revoke("ak-admin"));
        assert!(auth.authenticate("ak-admin").is_none());
        assert!(!auth.revoke("ak-admin"));
    }

    #[test]
    fn reload_never_briefly_drops_a_surviving_config_key() {
        let auth = AkAuth::default();
        auth.put(
            AkInfo {
                ak: "ak-keep".into(),
                product: "p".into(),
                tenant: "default".into(),
                qps: 1.0,
                daily_token_quota: 10,
                tokens_per_minute: None,
                expires_at_epoch_secs: None,
                banned: false,
                model_quotas: Default::default(),
            },
            KeySource::Config,
        );
        let cfg = GatewayConfig::from_yaml(
            "listen: {host: h, port: 1}\naccess_keys: [{ak: ak-keep, product: p, qps: 2, daily_token_quota: 20}, {ak: ak-add, product: p, qps: 1, daily_token_quota: 5}]",
        )
        .unwrap();
        auth.reload_config_keys(&cfg.access_keys);
        assert!(auth.authenticate("ak-keep").is_some());
        assert!(auth.authenticate("ak-add").is_some());
        assert_eq!(auth.authenticate("ak-keep").unwrap().daily_token_quota, 20);
    }

    #[test]
    fn admin_overwrite_of_a_config_key_stays_revocable_by_config() {
        let auth = AkAuth::default();
        let info = |ak: &str| AkInfo {
            ak: ak.into(),
            product: "p".into(),
            tenant: "default".into(),
            qps: 1.0,
            daily_token_quota: 10,
            tokens_per_minute: None,
            expires_at_epoch_secs: None,
            banned: false,
            model_quotas: Default::default(),
        };
        auth.put(info("ak-x"), KeySource::Config);
        auth.put(info("ak-x"), KeySource::Admin);
        let cfg = GatewayConfig::from_yaml(
            "listen: {host: h, port: 1}\naccess_keys: [{ak: ak-other, product: p, qps: 1, daily_token_quota: 5}]",
        )
        .unwrap();
        auth.reload_config_keys(&cfg.access_keys);
        assert!(
            auth.authenticate("ak-x").is_none(),
            "config revocation must not be defeated by a prior admin overwrite"
        );
        auth.put(info("ak-adm"), KeySource::Admin);
        let cfg2 = GatewayConfig::from_yaml(
            "listen: {host: h, port: 1}\naccess_keys: [{ak: ak-adm, product: p, qps: 1, daily_token_quota: 5}]",
        )
        .unwrap();
        auth.reload_config_keys(&cfg2.access_keys);
        let cfg3 = GatewayConfig::from_yaml("listen: {host: h, port: 1}\naccess_keys: []").unwrap();
        auth.reload_config_keys(&cfg3.access_keys);
        assert!(
            auth.authenticate("ak-adm").is_none(),
            "config-claimed key is revocable by config"
        );
    }

    #[test]
    fn key_status_lifecycle() {
        let mut info = AkInfo {
            ak: "k".into(),
            product: "p".into(),
            tenant: "default".into(),
            qps: 1.0,
            daily_token_quota: 10,
            tokens_per_minute: None,
            expires_at_epoch_secs: None,
            banned: false,
            model_quotas: Default::default(),
        };
        assert_eq!(info.status_at(i64::MAX), KeyStatus::Active);
        info.expires_at_epoch_secs = Some(100);
        assert_eq!(info.status_at(99), KeyStatus::Active);
        assert_eq!(info.status_at(100), KeyStatus::Expired);
        info.banned = true;
        assert_eq!(info.status_at(0), KeyStatus::Banned, "ban wins over expiry");
    }

    #[tokio::test]
    async fn auth_lookup() {
        let s = state();
        assert_eq!(
            s.auth.authenticate("ak-demo-123").await.unwrap().product,
            "demo"
        );
        assert!(s.auth.authenticate("nope").await.is_none());
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
        assert!(h.record_failure("acc", 2, cd), "expired cooldown re-arms");
        assert!(!h.available("acc"));
        h.record_success("acc");
        assert!(h.available("acc"));
    }

    #[test]
    fn pool_prefers_ptu_then_excludes_failed() {
        let s = state();
        let first = s
            .pool
            .select(Protocol::OpenaiChat, Some("tencent"))
            .unwrap();
        assert_eq!(first.name, "mock-hunyuan-ptu-down");
        assert!(first.is_ptu());
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
