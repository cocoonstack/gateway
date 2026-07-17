//! Per-model availability from minute-bucketed success/error counts. The
//! claim path only bumps an in-process buffer; a background task flushes it
//! to the store (Redis when the fleet shares one), so recording never adds a
//! network hop to a request.

use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

/// Buckets older than this fall off the memory ring / expire in Redis. History
/// beyond the classification window belongs to metrics, not this store.
const RETAIN_MINUTES: i64 = 60;

/// Availability verdict over the recent window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AvailState {
    Available,
    Unstable,
    Unavailable,
    /// Too little traffic in the window to judge.
    NoData,
}

/// Classify a window's counts against the configured error-rate thresholds.
pub fn classify(
    ok: u64,
    err: u64,
    min_samples: u64,
    unstable: f64,
    unavailable: f64,
) -> AvailState {
    let total = ok + err;
    if total < min_samples.max(1) {
        return AvailState::NoData;
    }
    let rate = err as f64 / total as f64;
    if rate >= unavailable {
        AvailState::Unavailable
    } else if rate >= unstable {
        AvailState::Unstable
    } else {
        AvailState::Available
    }
}

/// Claim-path recorder: counts accumulate in-process and [`Self::flush`]
/// folds them into the store under the flush-time minute (sub-period skew
/// across a minute boundary is accepted).
pub struct AvailTracker {
    buffer: DashMap<String, (u64, u64)>,
    store: Arc<dyn AvailStore>,
}

impl AvailTracker {
    pub fn new(store: Arc<dyn AvailStore>) -> Self {
        Self {
            buffer: DashMap::new(),
            store,
        }
    }

    /// Bump the model's (ok, err) pair; allocates only on a model's first
    /// record since the last flush.
    pub fn record(&self, model: &str, ok: bool) {
        if let Some(mut e) = self.buffer.get_mut(model) {
            bump(e.value_mut(), ok);
            return;
        }
        bump(
            self.buffer
                .entry(model.to_owned())
                .or_insert((0, 0))
                .value_mut(),
            ok,
        );
    }

    /// Drain the buffer into the store. Increments landing mid-drain are
    /// either taken now or survive for the next flush.
    pub async fn flush(&self) {
        let minute = crate::epoch_secs() / 60;
        let models: Vec<String> = self.buffer.iter().map(|e| e.key().clone()).collect();
        for m in models {
            if let Some((m, (ok, err))) = self.buffer.remove(&m) {
                self.store.add(&m, minute, ok, err).await;
            }
        }
    }

    /// Summed (ok, err) over `[since_minute, until_minute]`.
    pub async fn window(&self, model: &str, since_minute: i64, until_minute: i64) -> (u64, u64) {
        self.store.window(model, since_minute, until_minute).await
    }
}

impl std::fmt::Debug for AvailTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AvailTracker")
    }
}

fn bump(counts: &mut (u64, u64), ok: bool) {
    if ok {
        counts.0 += 1;
    } else {
        counts.1 += 1;
    }
}

/// Minute-bucket store behind the flush task.
#[async_trait]
pub trait AvailStore: Send + Sync + std::fmt::Debug {
    /// Fold one flush increment into `minute`'s bucket.
    async fn add(&self, model: &str, minute: i64, ok: u64, err: u64);
    /// Summed (ok, err) over buckets in `[since_minute, until_minute]`.
    async fn window(&self, model: &str, since_minute: i64, until_minute: i64) -> (u64, u64);
}

/// In-process ring for single-node deployments.
#[derive(Debug, Default)]
pub struct MemoryAvail {
    rings: DashMap<String, VecDeque<(i64, u64, u64)>>,
}

#[async_trait]
impl AvailStore for MemoryAvail {
    async fn add(&self, model: &str, minute: i64, ok: u64, err: u64) {
        let mut ring = self.rings.entry(model.to_owned()).or_default();
        match ring.back_mut() {
            Some(b) if b.0 == minute => {
                b.1 += ok;
                b.2 += err;
            }
            _ => ring.push_back((minute, ok, err)),
        }
        while ring.front().is_some_and(|b| b.0 <= minute - RETAIN_MINUTES) {
            ring.pop_front();
        }
    }

    async fn window(&self, model: &str, since_minute: i64, until_minute: i64) -> (u64, u64) {
        self.rings
            .get(model)
            .map(|ring| {
                ring.iter()
                    .filter(|b| b.0 >= since_minute && b.0 <= until_minute)
                    .fold((0, 0), |(o, e), b| (o + b.1, e + b.2))
            })
            .unwrap_or((0, 0))
    }
}

/// Fleet-shared buckets: every instance's flush increments the same keys.
/// A Redis outage drops the increment — availability is advisory, matching
/// the governance fail-open posture.
pub struct RedisAvail {
    conn: redis::aio::ConnectionManager,
}

impl RedisAvail {
    pub async fn connect(url: &str) -> Result<Self, String> {
        Ok(Self {
            conn: crate::redis_connect(url).await?,
        })
    }
}

impl std::fmt::Debug for RedisAvail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RedisAvail")
    }
}

#[async_trait]
impl AvailStore for RedisAvail {
    async fn add(&self, model: &str, minute: i64, ok: u64, err: u64) {
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        for (key, n) in [
            (count_key(model, minute, "ok"), ok),
            (count_key(model, minute, "err"), err),
        ] {
            if n > 0 {
                pipe.cmd("INCRBY").arg(&key).arg(n).ignore();
                pipe.cmd("EXPIRE")
                    .arg(&key)
                    .arg(RETAIN_MINUTES * 60)
                    .ignore();
            }
        }
        if let Err(e) = pipe.query_async::<()>(&mut conn).await {
            tracing::warn!(error = %e, model, "redis avail unavailable; counts dropped");
        }
    }

    async fn window(&self, model: &str, since_minute: i64, until_minute: i64) -> (u64, u64) {
        let mut keys = Vec::new();
        for minute in since_minute..=until_minute {
            keys.push(count_key(model, minute, "ok"));
            keys.push(count_key(model, minute, "err"));
        }
        let mut conn = self.conn.clone();
        let vals: Vec<Option<u64>> =
            match redis::cmd("MGET").arg(&keys).query_async(&mut conn).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, model, "redis avail unavailable; empty window");
                    return (0, 0);
                }
            };
        vals.chunks(2).fold((0, 0), |(o, e), pair| {
            (
                o + pair.first().copied().flatten().unwrap_or(0),
                e + pair.get(1).copied().flatten().unwrap_or(0),
            )
        })
    }
}

fn count_key(model: &str, minute: i64, kind: &str) -> String {
    format!("gw:avail:{model}:{minute}:{kind}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_thresholds() {
        assert_eq!(classify(0, 0, 20, 0.1, 0.5), AvailState::NoData);
        assert_eq!(classify(19, 0, 20, 0.1, 0.5), AvailState::NoData);
        assert_eq!(classify(20, 0, 20, 0.1, 0.5), AvailState::Available);
        assert_eq!(classify(18, 2, 20, 0.1, 0.5), AvailState::Unstable);
        assert_eq!(classify(10, 10, 20, 0.1, 0.5), AvailState::Unavailable);
        assert_eq!(classify(0, 20, 20, 0.1, 0.5), AvailState::Unavailable);
        // min_samples 0 still needs at least one sample
        assert_eq!(classify(0, 0, 0, 0.1, 0.5), AvailState::NoData);
    }

    #[tokio::test]
    async fn tracker_buffers_flushes_and_windows() {
        let tracker = AvailTracker::new(Arc::new(MemoryAvail::default()));
        for _ in 0..3 {
            tracker.record("m", true);
        }
        tracker.record("m", false);
        let minute = crate::epoch_secs() / 60;
        assert_eq!(tracker.window("m", minute - 5, minute).await, (0, 0));
        tracker.flush().await;
        assert_eq!(tracker.window("m", minute - 5, minute).await, (3, 1));
        // second flush with an empty buffer adds nothing
        tracker.flush().await;
        assert_eq!(tracker.window("m", minute - 5, minute).await, (3, 1));
    }

    #[tokio::test]
    async fn memory_ring_prunes_and_bounds_window() {
        let store = MemoryAvail::default();
        store.add("m", 100, 5, 1).await;
        store.add("m", 101, 2, 0).await;
        assert_eq!(store.window("m", 100, 101).await, (7, 1));
        assert_eq!(store.window("m", 101, 101).await, (2, 0));
        store.add("m", 100 + RETAIN_MINUTES, 1, 0).await;
        assert_eq!(
            store.window("m", 100, 100).await,
            (0, 0),
            "old bucket pruned"
        );
        assert_eq!(store.window("m", 0, i64::MAX).await, (3, 0));
    }
}
