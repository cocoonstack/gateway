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

pub mod admission;
pub mod alerts;
pub mod avail;
pub mod configstore;
pub mod content;
pub mod governance;
pub mod health;
pub mod keystore;
pub mod store;

pub use alerts::{AlertBus, AlertEvent};
pub use avail::{AvailState, AvailStore, classify};
pub use configstore::{CONFIG_CHANNEL, PostgresConfigStore};
pub use content::{ContentRecord, sealing_available};
pub use governance::{Governance, MemoryGovernance, RedisGovernance};
pub use health::{HealthStore, RedisHealth};
pub use keystore::{KeyStore, PostgresKeyStore};
pub use store::*;

const CACHE_MAX_ENTRIES: u64 = 10_000;
/// Postgres `IF NOT EXISTS` DDL can still race in `pg_class` when replicas
/// bootstrap together; every schema DDL runs under this lock.
const PG_SCHEMA_LOCK_SQL: &str = "SELECT pg_advisory_xact_lock(hashtext('cocoon_gateway_schema'))";
/// A failure streak idle this long restarts on the next failure — mirrors the
/// Redis health backend's failure-key TTL so both backends trip identically.
const FAILS_DECAY: Duration = Duration::from_secs(3_600);

/// Resolved identity for an authenticated AK.
#[derive(Debug, Clone)]
pub struct AkInfo {
    pub ak: String,
    pub product: String,
    /// Tenant this key belongs to (`gw_config::DEFAULT_TENANT` when undeclared).
    pub tenant: String,
    /// End user this key is issued to (one key = one user); `None` = shared key.
    pub owner: Option<String>,
    pub qps: f64,
    pub daily_token_quota: i64,
    /// tokens-per-minute window cap; None = unlimited.
    pub tokens_per_minute: Option<i64>,
    /// Unix seconds after which the key stops authenticating; None = never.
    pub expires_at_epoch_secs: Option<i64>,
    /// A banned key stays in the table but fails auth with a distinct 403.
    pub banned: bool,
    /// Abuse auto-suspension deadline; the key self-recovers when it passes.
    /// Runtime state — a config reload must not clear it.
    pub suspended_until_epoch_secs: Option<i64>,
    /// Per-model daily token caps overriding the tenant defaults; empty = none.
    /// Arc'd: `AkInfo` is cloned per request and the map is write-rare.
    pub model_quotas: Arc<std::collections::HashMap<String, i64>>,
}

impl AkInfo {
    /// Lifecycle state at `now` (unix seconds). Ban wins over suspension,
    /// suspension over expiry; an elapsed suspension self-recovers.
    pub fn status_at(&self, now_epoch_secs: i64) -> KeyStatus {
        if self.banned {
            return KeyStatus::Banned;
        }
        if self
            .suspended_until_epoch_secs
            .is_some_and(|t| now_epoch_secs < t)
        {
            return KeyStatus::Suspended;
        }
        match self.expires_at_epoch_secs {
            Some(t) if now_epoch_secs >= t => KeyStatus::Expired,
            _ => KeyStatus::Active,
        }
    }

    /// The attributed end user: this key's non-empty `owner` (authoritative),
    /// else the caller-supplied `fallback` (request metadata / the realtime
    /// `x-gw-user` hint). The one resolution every surface shares.
    pub fn attributed_user<'a>(&'a self, fallback: &'a str) -> &'a str {
        self.owner_override().unwrap_or(fallback)
    }

    /// The non-empty `owner` identity when this key overrides attribution.
    pub fn owner_override(&self) -> Option<&str> {
        self.owner.as_deref().filter(|s| !s.is_empty())
    }

    /// Apply a partial quota/lifecycle patch.
    pub fn apply_patch(&mut self, patch: &KeyPatch) {
        if let Some(v) = patch.qps {
            self.qps = v;
        }
        if let Some(v) = patch.daily_token_quota {
            self.daily_token_quota = v;
        }
        if let Some(v) = patch.tokens_per_minute {
            self.tokens_per_minute = v;
        }
        if let Some(v) = patch.expires_at_epoch_secs {
            self.expires_at_epoch_secs = v;
        }
        if let Some(v) = patch.banned {
            self.banned = v;
        }
        if let Some(v) = patch.suspended_until_epoch_secs {
            self.suspended_until_epoch_secs = v;
        }
    }
}

impl From<&gw_config::AkConf> for AkInfo {
    fn from(k: &gw_config::AkConf) -> Self {
        Self {
            ak: k.ak.clone(),
            product: k.product.clone(),
            tenant: k.tenant.clone(),
            owner: k.owner.clone(),
            qps: k.qps,
            daily_token_quota: k.daily_token_quota,
            tokens_per_minute: k.tokens_per_minute,
            expires_at_epoch_secs: k.expires_at_epoch_secs,
            banned: k.banned,
            suspended_until_epoch_secs: None,
            model_quotas: Arc::new(k.model_quotas.clone()),
        }
    }
}

/// A partial quota/lifecycle update: `None` = leave the field; on the
/// double-`Option` fields `Some(None)` = clear, `Some(Some(v))` = set.
#[derive(Debug, Clone, Default)]
pub struct KeyPatch {
    pub qps: Option<f64>,
    pub daily_token_quota: Option<i64>,
    pub tokens_per_minute: Option<Option<i64>>,
    pub expires_at_epoch_secs: Option<Option<i64>>,
    pub banned: Option<bool>,
    pub suspended_until_epoch_secs: Option<Option<i64>>,
}

/// Key lifecycle state: expired and banned keys authenticate to distinct 403s.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyStatus {
    Active,
    Banned,
    Expired,
    Suspended,
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
    /// config-declared key keeps it `Config` (so a reload can still revoke it),
    /// while a config write claims an admin key. Atomic via the entry lock.
    pub fn put(&self, mut info: AkInfo, source: KeySource) {
        use dashmap::mapref::entry::Entry;
        match self.keys.entry(info.ak.clone()) {
            Entry::Occupied(mut e) => {
                let sticky_config = e.get().1 == KeySource::Config && source == KeySource::Admin;
                let source = if sticky_config {
                    KeySource::Config
                } else {
                    source
                };
                // an upsert never clears a runtime suspension — only a patch
                // does (mirrors the Postgres upsert, which omits the column)
                if info.suspended_until_epoch_secs.is_none() {
                    info.suspended_until_epoch_secs = e.get().0.suspended_until_epoch_secs;
                }
                e.insert((info, source));
            }
            Entry::Vacant(e) => {
                e.insert((info, source));
            }
        }
    }

    /// Update quota/lifecycle fields of an existing key in place; returns the
    /// new view. `None` if the key doesn't exist.
    pub fn patch(&self, ak: &str, patch: &KeyPatch) -> Option<AkInfo> {
        let mut e = self.keys.get_mut(ak)?;
        e.0.apply_patch(patch);
        Some(e.0.clone())
    }

    /// A page of keys, sorted by ak (stable), optionally confined to `tenant`,
    /// `offset..offset+limit` — the filter applies before paging.
    pub fn list(&self, tenant: Option<&str>, offset: usize, limit: usize) -> Vec<AkInfo> {
        let mut keys: Vec<AkInfo> = self
            .keys
            .iter()
            .map(|e| e.value().0.clone())
            .filter(|k| tenant.is_none_or(|t| t == k.tenant))
            .collect();
        keys.sort_by(|a, b| a.ak.cmp(&b.ak));
        keys.into_iter().skip(offset).take(limit).collect()
    }

    /// Remove a key regardless of source; returns whether it existed.
    pub fn revoke(&self, ak: &str) -> bool {
        self.keys.remove(ak).is_some()
    }

    /// Re-apply the config file's key set, leaving admin-created keys untouched.
    /// Surviving keys are upserted in place (never briefly absent) so a
    /// concurrent `authenticate` can't spuriously 401 mid-reload.
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
    async fn patch(&self, ak: &str, patch: &KeyPatch) -> gw_models::GResult<Option<AkInfo>> {
        Ok(AkAuth::patch(self, ak, patch))
    }
    async fn revoke(&self, ak: &str) -> gw_models::GResult<bool> {
        Ok(AkAuth::revoke(self, ak))
    }
    async fn list(
        &self,
        tenant: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> gw_models::GResult<Vec<AkInfo>> {
        Ok(AkAuth::list(self, tenant, offset, limit))
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
    /// Take one permit. qps >= 1: replenish/burst = round(qps); 0 < qps < 1: one
    /// permit per 1/qps seconds (burst 1); qps <= 0 or non-finite: always denied
    /// (a NaN never equals the stored qps, so it would rebuild a full bucket
    /// every call — an unlimited bypass). A bucket whose stored qps differs is
    /// rebuilt (starts full) so limit changes apply at once.
    pub fn allow(&self, key: &str, qps: f64) -> bool {
        if !qps.is_finite() || qps <= 0.0 {
            return false;
        }
        let mut e = slot_mut(&self.buckets, key, || (qps, new_bucket(qps)));
        if e.0 != qps {
            *e = (qps, new_bucket(qps));
        }
        let limiter = e.1.clone();
        drop(e);
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

/// Daily token quota accounting per AK; reset daily by the gw-task job.
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

    /// Post-consume actual usage; saturating against a hostile i64::MAX count.
    pub fn consume(&self, ak: &str, tokens: i64) {
        let mut e = slot_mut(&self.used, ak, || 0);
        *e = e.saturating_add(tokens);
    }

    /// Admission with reservation, atomic under the entry guard.
    pub fn reserve(&self, key: &str, amount: i64, limit: i64) -> bool {
        reserve_on(&mut slot_mut(&self.used, key, || 0), amount, limit)
    }

    /// Apply the settle delta (actual - reserved); never below zero.
    pub fn settle(&self, key: &str, delta: i64) {
        settle_on(&mut slot_mut(&self.used, key, || 0), delta);
    }

    /// Daily reset.
    pub fn reset_all(&self) {
        self.used.clear();
    }
}

/// Account pool: pick the highest-priority slot serving a model type, round-robin
/// within the tie. Slots are `Arc`'d — selection is per-request, so handing out
/// a refcount bump beats copying the account's strings every time.
#[derive(Debug, Default)]
pub struct AccountPool {
    accounts: Vec<Arc<Account>>,
    rr: AtomicUsize,
}

impl AccountPool {
    /// Build the pool from config accounts (protocols validated at config load).
    pub fn from_config(cfg: &GatewayConfig) -> Self {
        let accounts = cfg
            .accounts
            .iter()
            .map(|a| {
                Arc::new(Account {
                    name: a.name.clone(),
                    provider: a.provider.clone(),
                    priority: a.priority,
                    tier: a.tier.clone(),
                    endpoint: a.endpoint.clone(),
                    api_key_env: a.api_key_env.clone(),
                    secret_key_env: a.secret_key_env.clone(),
                    cost_input_price_per_1k_micros: a.cost_input_price_per_1k_micros,
                    cost_output_price_per_1k_micros: a.cost_output_price_per_1k_micros,
                    protocols: a
                        .protocols
                        .iter()
                        .filter_map(|w| Protocol::from_wire(w))
                        .collect(),
                })
            })
            .collect();
        Self {
            accounts,
            rr: AtomicUsize::new(0),
        }
    }

    pub fn select(&self, p: Protocol, provider: Option<&str>) -> Option<Arc<Account>> {
        self.select_excluding(p, provider, &[])
    }

    /// [`Self::select_excluding`] plus a health filter: cooldown accounts are
    /// excluded. Only accounts that could actually be selected (protocol AND
    /// provider match) are health-checked — the others would waste lookups.
    pub async fn select_healthy(
        &self,
        p: Protocol,
        provider: Option<&str>,
        excluded: &[String],
        health: &dyn HealthStore,
    ) -> Option<Arc<Account>> {
        let candidates: Vec<&Arc<Account>> = self
            .accounts
            .iter()
            .filter(|a| a.protocols.contains(&p) && provider.is_none_or(|want| a.provider == want))
            .collect();
        let checks =
            futures::future::join_all(candidates.iter().map(|a| health.available(&a.name))).await;
        let unhealthy: Vec<&str> = candidates
            .iter()
            .zip(checks)
            .filter(|(_, ok)| !ok)
            .map(|(a, _)| a.name.as_str())
            .collect();
        self.select_with(p, provider, |name| {
            excluded.iter().any(|e| e == name) || unhealthy.contains(&name)
        })
    }

    /// PTU accounts are preferred over paygo; `excluded` (failed-over) accounts
    /// are skipped; within a tier, pick by priority then round-robin. A model
    /// bound to a `provider` only uses that provider's accounts.
    pub fn select_excluding(
        &self,
        p: Protocol,
        provider: Option<&str>,
        excluded: &[String],
    ) -> Option<Arc<Account>> {
        self.select_with(p, provider, |name| excluded.iter().any(|e| e == name))
    }

    pub fn len(&self) -> usize {
        self.accounts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }

    fn select_with(
        &self,
        p: Protocol,
        provider: Option<&str>,
        is_excluded: impl Fn(&str) -> bool,
    ) -> Option<Arc<Account>> {
        let candidates: Vec<&Arc<Account>> = self
            .accounts
            .iter()
            .filter(|a| {
                a.protocols.contains(&p)
                    && !is_excluded(&a.name)
                    && provider.is_none_or(|want| a.provider == want)
            })
            .collect();
        let tier: Vec<&Arc<Account>> = {
            let ptu: Vec<&Arc<Account>> =
                candidates.iter().copied().filter(|a| a.is_ptu()).collect();
            if ptu.is_empty() { candidates } else { ptu }
        };
        let best = tier.iter().map(|a| a.priority).min()?;
        let top: Vec<&Arc<Account>> = tier
            .iter()
            .copied()
            .filter(|a| a.priority == best)
            .collect();
        let idx = self.rr.fetch_add(1, Ordering::Relaxed) % top.len();
        Some(Arc::clone(top[idx]))
    }
}

/// Fixed-window token accounting, for AK-level TPM and (at amount 1) QPM.
#[derive(Debug, Default)]
pub struct TokenWindow {
    entries: DashMap<String, (Instant, i64)>,
}

impl TokenWindow {
    /// Windowed admission with reservation, atomic under the entry guard.
    pub fn reserve(&self, key: &str, amount: i64, limit: i64, window: std::time::Duration) -> bool {
        reserve_on(&mut self.slot(key, window).1, amount, limit)
    }

    /// Apply the settle delta to the current window; never below zero.
    pub fn settle(&self, key: &str, delta: i64, window: std::time::Duration) {
        settle_on(&mut self.slot(key, window).1, delta);
    }

    /// Post-add actual token usage (saturating on a hostile i64::MAX count).
    pub fn add(&self, key: &str, tokens: i64, window: std::time::Duration) {
        let mut e = self.slot(key, window);
        e.1 = e.1.saturating_add(tokens);
    }

    /// The current window's entry, rotated if elapsed — under one entry guard so
    /// a concurrent rollover can't land tokens in the wrong window.
    fn slot(
        &self,
        key: &str,
        window: std::time::Duration,
    ) -> dashmap::mapref::one::RefMut<'_, String, (Instant, i64)> {
        let mut e = slot_mut(&self.entries, key, || (Instant::now(), 0));
        if e.0.elapsed() >= window {
            *e = (Instant::now(), 0);
        }
        e
    }
}

#[derive(Debug, Default)]
struct HealthEntry {
    consecutive_failures: usize,
    last_failure: Option<Instant>,
    cooldown_until: Option<Instant>,
}

/// Account health: consecutive-failure cooldown with auto-recovery. A streak
/// idle past [`FAILS_DECAY`] restarts at 1 — the same self-expiry the Redis
/// backend's failure-key TTL applies, so both backends trip identically.
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
        let mut e = slot_mut(&self.entries, name, Default::default);
        if e.last_failure.is_some_and(|at| at.elapsed() >= FAILS_DECAY) {
            e.consecutive_failures = 0;
        }
        e.last_failure = Some(Instant::now());
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
        self.entries
            .get(name)
            .is_none_or(|e| e.cooldown_until.is_none_or(|until| Instant::now() >= until))
    }
}

/// Request-level response cache: per-entry TTL, keyed by the request hash;
/// in-process by default, Redis when `storage.shared_cache` is set.
#[async_trait::async_trait]
pub trait ResponseCache: Send + Sync + std::fmt::Debug {
    async fn get(&self, key: &str) -> Option<gw_models::GatewayResponse>;
    async fn put(&self, key: String, resp: gw_models::GatewayResponse, ttl: Duration);
}

/// In-process response cache (moka, bounded, per-entry TTL). The default.
#[derive(Debug)]
pub struct MemoryResponseCache {
    entries: moka::sync::Cache<String, (gw_models::GatewayResponse, Duration)>,
}

impl Default for MemoryResponseCache {
    fn default() -> Self {
        Self {
            entries: moka::sync::Cache::builder()
                .max_capacity(CACHE_MAX_ENTRIES)
                .expire_after(PerEntryTtl)
                .build(),
        }
    }
}

#[async_trait::async_trait]
impl ResponseCache for MemoryResponseCache {
    async fn get(&self, key: &str) -> Option<gw_models::GatewayResponse> {
        self.entries.get(key).map(|(resp, _)| resp)
    }

    async fn put(&self, key: String, resp: gw_models::GatewayResponse, ttl: Duration) {
        self.entries.insert(key, (resp, ttl));
    }
}

/// Fleet-shared response cache in Redis (`gw:cache:*`, JSON values, PX TTL).
/// Errors degrade to a miss — the request just recomputes.
pub struct RedisResponseCache {
    conn: redis::aio::ConnectionManager,
}

impl std::fmt::Debug for RedisResponseCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RedisResponseCache")
    }
}

impl RedisResponseCache {
    pub async fn connect(url: &str) -> Result<Self, String> {
        Ok(Self {
            conn: redis_connect(url).await?,
        })
    }
}

#[async_trait::async_trait]
impl ResponseCache for RedisResponseCache {
    async fn get(&self, key: &str) -> Option<gw_models::GatewayResponse> {
        let mut conn = self.conn.clone();
        let raw: Option<String> = redis::cmd("GET")
            .arg(format!("gw:cache:{key}"))
            .query_async(&mut conn)
            .await
            .ok()
            .flatten();
        raw.and_then(|s| match serde_json::from_str(&s) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!(error = %e, "cached response failed to parse; treating as miss");
                None
            }
        })
    }

    async fn put(&self, key: String, resp: gw_models::GatewayResponse, ttl: Duration) {
        let Ok(raw) = serde_json::to_string(&resp) else {
            return;
        };
        let mut conn = self.conn.clone();
        if let Err(e) = redis::cmd("SET")
            .arg(format!("gw:cache:{key}"))
            .arg(raw)
            .arg("PX")
            .arg(ttl.as_millis() as u64)
            .query_async::<()>(&mut conn)
            .await
        {
            tracing::warn!(error = %e, "redis cache put failed");
        }
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
    /// Live key table (admin edits survive a reload); Postgres for fleet-shared.
    pub auth: Arc<dyn KeyStore>,
    pub pool: AccountPool,
    pub governance: Arc<dyn Governance>,
    /// Durable records (ledger/files/batches); sqlite/postgres when configured.
    pub store: Arc<dyn Store>,
    /// Account health (cooldown/recovery); Redis for fleet-wide cooldown.
    pub health: Arc<dyn HealthStore>,
    /// Request-level response cache: in-process by default, Redis when shared.
    pub cache: Arc<dyn ResponseCache>,
    /// Per-model availability counters; Redis for fleet-wide aggregation.
    pub avail: Arc<dyn avail::AvailStore>,
    /// Advisory alert bus; the server's dispatch task drains it.
    pub alerts: Arc<alerts::AlertBus>,
}

impl Default for GatewayState {
    fn default() -> Self {
        Self {
            auth: Arc::new(AkAuth::default()),
            pool: AccountPool::default(),
            governance: Arc::new(MemoryGovernance::default()),
            store: Arc::new(MemoryStore::default()),
            health: Arc::new(AccountHealth::default()),
            cache: Arc::new(MemoryResponseCache::default()),
            avail: Arc::new(avail::MemoryAvail::default()),
            alerts: Arc::new(alerts::AlertBus::default()),
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

    /// Build state with the backends the config selects: Postgres (key table +
    /// durable store) aborts on failure — it is the source of truth; Redis
    /// (governance/health/cache) fails soft and stays in-process.
    pub async fn build(cfg: &GatewayConfig) -> gw_models::GResult<Self> {
        let mut state = GatewayState {
            pool: AccountPool::from_config(cfg),
            ..Default::default()
        };
        if cfg.storage.postgres_url.is_empty() {
            for k in &cfg.access_keys {
                state.auth.put(AkInfo::from(k), KeySource::Config).await?;
            }
        } else {
            let ks = PostgresKeyStore::connect(&cfg.storage.postgres_url).await?;
            ks.reload_config_keys(&cfg.access_keys).await?;
            state.auth = Arc::new(ks);
            tracing::info!("key store = postgres (config keys seeded)");
            state.store = Arc::new(
                PostgresStore::connect_with_cap(
                    &cfg.storage.postgres_url,
                    cfg.storage.ledger_max_rows,
                )
                .await?,
            );
            tracing::info!("store = postgres");
        }
        if cfg.storage.postgres_url.is_empty() && !cfg.storage.sqlite_path.is_empty() {
            state.store = Arc::new(
                SqliteStore::open_with_cap(&cfg.storage.sqlite_path, cfg.storage.ledger_max_rows)
                    .await?,
            );
            tracing::info!(path = %cfg.storage.sqlite_path, "store = sqlite");
        }
        if !cfg.storage.redis_url.is_empty() {
            if cfg.storage.shared_cache {
                match RedisResponseCache::connect(&cfg.storage.redis_url).await {
                    Ok(c) => {
                        state.cache = Arc::new(c);
                        tracing::info!("response cache = redis (fleet-shared)");
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "redis cache connect failed; staying in-process")
                    }
                }
            }
            match RedisGovernance::connect(&cfg.storage.redis_url).await {
                Ok(g) => {
                    state.governance = Arc::new(g);
                    tracing::info!(url = %cfg.storage.redis_url, "governance = redis");
                }
                Err(e) => tracing::error!(error = %e, "redis connect failed; staying in-process"),
            }
            match RedisHealth::connect(&cfg.storage.redis_url).await {
                Ok(h) => {
                    state.health = Arc::new(h);
                    tracing::info!("account health = redis (fleet-wide cooldown)");
                }
                Err(e) => {
                    tracing::error!(error = %e, "redis health connect failed; staying in-process")
                }
            }
            match avail::RedisAvail::connect(&cfg.storage.redis_url).await {
                Ok(a) => {
                    state.avail = Arc::new(a);
                    tracing::info!("model availability = redis (fleet-wide counts)");
                }
                Err(e) => {
                    tracing::error!(error = %e, "redis avail connect failed; staying in-process")
                }
            }
        }
        Ok(state)
    }

    /// Rebuild the config-derived pool and re-apply config keys while preserving
    /// the runtime seams from `prev`; in-flight requests keep their own snapshot.
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
            avail: prev.avail.clone(),
            alerts: prev.alerts.clone(),
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
/// `reload()` atomically swaps in a fresh snapshot.
#[derive(Clone)]
pub struct SharedConfig {
    inner: Arc<arc_swap::ArcSwap<Snapshot>>,
    /// Serializes reloads: two concurrent reloads could otherwise pair one
    /// reload's cfg pointer with the other's key set.
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
        // cfg.generation (a stable hash set at parse) keys the preserved cache so
        // it can't serve a pre-reload entry across a model remap
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

/// The entry for `key`, inserting `init()` on first use — the key String is
/// allocated only on the miss path.
fn slot_mut<'a, V>(
    map: &'a DashMap<String, V>,
    key: &str,
    init: impl FnOnce() -> V,
) -> dashmap::mapref::one::RefMut<'a, String, V> {
    if let Some(e) = map.get_mut(key) {
        return e;
    }
    map.entry(key.to_owned()).or_insert_with(init)
}

/// Admit while spent-before < `limit`, adding `amount` so in-flight work
/// counts — the one in-process reserve semantics (Redis mirrors it in
/// `reserve_capped`).
fn reserve_on(counter: &mut i64, amount: i64, limit: i64) -> bool {
    if *counter >= limit {
        return false;
    }
    *counter = counter.saturating_add(amount);
    true
}

/// Apply a settle delta, flooring at zero (Redis mirrors it in `settle_floored`).
fn settle_on(counter: &mut i64, delta: i64) {
    *counter = counter.saturating_add(delta).max(0);
}

/// Wrap a sqlx error as an internal gateway error with context.
pub(crate) fn sqlx_err(what: &str, e: sqlx::Error) -> gw_models::GatewayError {
    gw_models::GatewayError::internal(what).with_source(e)
}

pub(crate) async fn setup_schema(
    pool: &sqlx::PgPool,
    what: &str,
    ddls: &[&'static str],
) -> gw_models::GResult<()> {
    for ddl in ddls.iter().copied() {
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| sqlx_err(&format!("begin {what} schema"), e))?;
        sqlx::query(PG_SCHEMA_LOCK_SQL)
            .execute(&mut *tx)
            .await
            .map_err(|e| sqlx_err(&format!("lock {what} schema"), e))?;
        sqlx::query(ddl)
            .execute(&mut *tx)
            .await
            .map_err(|e| sqlx_err(&format!("create {what} schema"), e))?;
        tx.commit()
            .await
            .map_err(|e| sqlx_err(&format!("commit {what} schema"), e))?;
    }
    Ok(())
}

/// Open a Redis connection manager (the governance/health/cache backends).
pub(crate) async fn redis_connect(url: &str) -> Result<redis::aio::ConnectionManager, String> {
    let client = redis::Client::open(url).map_err(|e| format!("redis open: {e}"))?;
    redis::aio::ConnectionManager::new(client)
        .await
        .map_err(|e| format!("redis connect: {e}"))
}

/// Current unix seconds (0 if the clock reads before the epoch).
pub fn epoch_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Unix milliseconds; erasure markers use this so an erase-then-resubmit in
/// the same second isn't misjudged as pre-erasure content.
pub fn epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> GatewayState {
        GatewayState::from_config(&GatewayConfig::embedded_default().unwrap())
    }

    fn ak_info(ak: &str) -> AkInfo {
        AkInfo {
            ak: ak.into(),
            product: "p".into(),
            tenant: "default".into(),
            owner: None,
            qps: 1.0,
            daily_token_quota: 10,
            tokens_per_minute: None,
            expires_at_epoch_secs: None,
            banned: false,
            suspended_until_epoch_secs: None,
            model_quotas: Default::default(),
        }
    }

    #[test]
    fn suspension_orders_below_ban_and_self_recovers() {
        let mut ak = ak_info("k");
        ak.suspended_until_epoch_secs = Some(100);
        assert_eq!(ak.status_at(50), KeyStatus::Suspended);
        assert_eq!(ak.status_at(100), KeyStatus::Active, "deadline passed");
        ak.banned = true;
        assert_eq!(ak.status_at(50), KeyStatus::Banned, "ban wins");
        ak.banned = false;
        ak.expires_at_epoch_secs = Some(40);
        assert_eq!(
            ak.status_at(50),
            KeyStatus::Suspended,
            "suspension reported over expiry"
        );
    }

    #[test]
    fn config_reapply_keeps_runtime_suspension() {
        let auth = AkAuth::default();
        auth.put(ak_info("k"), KeySource::Config);
        auth.patch(
            "k",
            &KeyPatch {
                suspended_until_epoch_secs: Some(Some(i64::MAX)),
                ..Default::default()
            },
        );
        auth.put(ak_info("k"), KeySource::Config);
        assert_eq!(
            auth.authenticate("k").unwrap().suspended_until_epoch_secs,
            Some(i64::MAX),
            "re-applied config key keeps the suspension"
        );
    }

    #[test]
    fn attributed_user_prefers_nonempty_owner_else_fallback() {
        let mut ak = ak_info("k");
        assert_eq!(ak.attributed_user("hint"), "hint", "no owner → fallback");
        ak.owner = Some(String::new());
        assert_eq!(
            ak.attributed_user("hint"),
            "hint",
            "empty owner is not an identity → fallback"
        );
        ak.owner = Some("alice".into());
        assert_eq!(
            ak.attributed_user("hint"),
            "alice",
            "owner wins over fallback"
        );
        assert_eq!(ak.attributed_user(""), "alice");
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

    #[test]
    fn non_finite_qps_is_denied() {
        let rl = RateLimiter::default();
        for _ in 0..3 {
            assert!(
                !rl.allow("nan", f64::NAN),
                "NaN must fail closed, not rebuild a full bucket per call"
            );
        }
        assert!(!rl.allow("inf", f64::INFINITY));
    }

    #[tokio::test]
    async fn reload_swaps_keys_but_preserves_seams() {
        let cfg = Arc::new(GatewayConfig::embedded_default().unwrap());
        let boot = Arc::new(GatewayState::from_config(&cfg));
        let file_id = boot
            .store
            .file_put("default", "batch", "keep me".into())
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
        auth.put(ak_info("ak-config"), KeySource::Config);
        auth.put(ak_info("ak-admin"), KeySource::Admin);
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
            .patch(
                "ak-admin",
                &KeyPatch {
                    qps: Some(9.0),
                    tokens_per_minute: Some(Some(5)),
                    banned: Some(true),
                    ..Default::default()
                },
            )
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
        auth.put(ak_info("ak-keep"), KeySource::Config);
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
        auth.put(ak_info("ak-x"), KeySource::Config);
        auth.put(ak_info("ak-x"), KeySource::Admin);
        let cfg = GatewayConfig::from_yaml(
            "listen: {host: h, port: 1}\naccess_keys: [{ak: ak-other, product: p, qps: 1, daily_token_quota: 5}]",
        )
        .unwrap();
        auth.reload_config_keys(&cfg.access_keys);
        assert!(
            auth.authenticate("ak-x").is_none(),
            "config revocation must not be defeated by a prior admin overwrite"
        );
        auth.put(ak_info("ak-adm"), KeySource::Admin);
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

    #[tokio::test]
    async fn redis_response_cache_roundtrip() {
        let Ok(url) = std::env::var("GW_TEST_REDIS_URL") else {
            return;
        };
        let c = RedisResponseCache::connect(&url).await.expect("connect");
        let key = format!("t{}", std::process::id());
        let resp = gw_models::GatewayResponse {
            message: "cached hello".into(),
            prompt_tokens: 3,
            ..Default::default()
        };
        let json = serde_json::to_string(&resp).unwrap();
        serde_json::from_str::<gw_models::GatewayResponse>(&json).expect("serde roundtrip");
        c.put(key.clone(), resp, Duration::from_millis(500)).await;
        let got = c.get(&key).await.expect("hit");
        assert_eq!(got.message, "cached hello");
        assert_eq!(got.prompt_tokens, 3);
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(c.get(&key).await.is_none(), "TTL expires");
    }

    #[tokio::test]
    async fn build_selects_sqlite_store_and_seeds_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.db");
        let yaml = format!(
            "listen: {{host: h, port: 1}}\nstorage: {{sqlite_path: '{}'}}\naccess_keys: [{{ak: k1, product: p, qps: 1, daily_token_quota: 10}}]",
            path.to_str().unwrap()
        );
        let cfg = GatewayConfig::from_yaml(&yaml).unwrap();
        let st = GatewayState::build(&cfg).await.unwrap();
        assert!(st.auth.authenticate("k1").await.is_some());
        let f = st
            .store
            .file_put("default", "batch", "x".into())
            .await
            .unwrap();
        assert!(st.store.file_get(&f.id).await.unwrap().is_some());
        assert!(!st.store.distributed_batches());
    }

    #[test]
    fn key_status_lifecycle() {
        let mut info = ak_info("k");
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
        assert_eq!(b.name, "mock-openai-1");
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
    fn failure_streak_decays_when_idle() {
        let h = AccountHealth::default();
        let cd = Duration::from_millis(10);
        assert!(!h.record_failure("a", 2, cd));
        h.entries.get_mut("a").unwrap().last_failure = Instant::now().checked_sub(FAILS_DECAY);
        assert!(
            !h.record_failure("a", 2, cd),
            "an hour-idle streak restarts at 1 instead of instantly re-tripping"
        );
        assert!(h.record_failure("a", 2, cd), "then trips at threshold");
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
