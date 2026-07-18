//! Per-model availability from minute-bucketed success/error counts. The
//! claim path never touches the network: the in-process store writes its
//! ring directly, and the Redis store buffers samples until the background
//! flush drains them.

use std::collections::VecDeque;

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

impl AvailState {
    pub fn as_str(self) -> &'static str {
        match self {
            AvailState::Available => "available",
            AvailState::Unstable => "unstable",
            AvailState::Unavailable => "unavailable",
            AvailState::NoData => "no_data",
        }
    }
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

/// Minute-bucket availability store. `record` runs on the claim path and must
/// stay network-free; the flush task calls `flush` so a buffering backend can
/// drain (sub-period skew across a minute boundary is accepted).
#[async_trait]
pub trait AvailStore: Send + Sync + std::fmt::Debug {
    fn record(&self, model: &str, ok: bool);
    /// Drain buffered samples; no-op for stores that write directly.
    async fn flush(&self) {}
    /// Summed (ok, err) over buckets in `[since_minute, until_minute]`.
    async fn window(&self, model: &str, since_minute: i64, until_minute: i64) -> (u64, u64);
}

/// In-process ring for single-node deployments; records write it directly.
#[derive(Debug, Default)]
pub struct MemoryAvail {
    rings: DashMap<String, VecDeque<(i64, u64, u64)>>,
}

impl MemoryAvail {
    fn add(&self, model: &str, minute: i64, ok: u64, err: u64) {
        let mut ring = crate::slot_mut(&self.rings, model, VecDeque::new);
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
}

#[async_trait]
impl AvailStore for MemoryAvail {
    fn record(&self, model: &str, ok: bool) {
        let minute = crate::epoch_secs() / 60;
        self.add(model, minute, u64::from(ok), u64::from(!ok));
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

/// Fleet-shared buckets: samples buffer in-process and every instance's flush
/// increments the same keys. A Redis outage drops the increment —
/// availability is advisory, matching the governance fail-open posture.
pub struct RedisAvail {
    conn: redis::aio::ConnectionManager,
    buffer: DashMap<String, (u64, u64)>,
}

impl RedisAvail {
    pub async fn connect(url: &str) -> Result<Self, String> {
        Ok(Self {
            conn: crate::redis_connect(url).await?,
            buffer: DashMap::new(),
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
    fn record(&self, model: &str, ok: bool) {
        let mut e = crate::slot_mut(&self.buffer, model, || (0, 0));
        if ok {
            e.0 += 1;
        } else {
            e.1 += 1;
        }
    }

    /// Increments landing mid-drain are either taken now or survive for the
    /// next flush.
    async fn flush(&self) {
        let minute = crate::epoch_secs() / 60;
        let models: Vec<String> = self.buffer.iter().map(|e| e.key().clone()).collect();
        for m in models {
            let Some((m, (ok, err))) = self.buffer.remove(&m) else {
                continue;
            };
            let mut conn = self.conn.clone();
            let mut pipe = redis::pipe();
            for (key, n) in [
                (count_key(&m, minute, "ok"), ok),
                (count_key(&m, minute, "err"), err),
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
                tracing::warn!(error = %e, model = %m, "redis avail unavailable; counts dropped");
            }
        }
    }

    async fn window(&self, model: &str, since_minute: i64, until_minute: i64) -> (u64, u64) {
        let keys: Vec<String> = (since_minute..=until_minute)
            .flat_map(|m| [count_key(model, m, "ok"), count_key(model, m, "err")])
            .collect();
        let mut conn = self.conn.clone();
        let vals: Vec<Option<u64>> =
            match redis::cmd("MGET").arg(&keys).query_async(&mut conn).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, model, "redis avail unavailable; empty window");
                    return (0, 0);
                }
            };
        vals.chunks_exact(2).fold((0, 0), |(o, e), pair| {
            (o + pair[0].unwrap_or(0), e + pair[1].unwrap_or(0))
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
        assert_eq!(classify(0, 0, 0, 0.1, 0.5), AvailState::NoData);
    }

    #[tokio::test]
    async fn memory_records_and_windows() {
        let store = MemoryAvail::default();
        for _ in 0..3 {
            store.record("m", true);
        }
        store.record("m", false);
        store.flush().await;
        let minute = crate::epoch_secs() / 60;
        assert_eq!(store.window("m", minute - 5, minute).await, (3, 1));
    }

    #[tokio::test]
    async fn memory_ring_prunes_and_bounds_window() {
        let store = MemoryAvail::default();
        store.add("m", 100, 5, 1);
        store.add("m", 101, 2, 0);
        assert_eq!(store.window("m", 100, 101).await, (7, 1));
        assert_eq!(store.window("m", 101, 101).await, (2, 0));
        store.add("m", 100 + RETAIN_MINUTES, 1, 0);
        assert_eq!(
            store.window("m", 100, 100).await,
            (0, 0),
            "old bucket pruned"
        );
        assert_eq!(store.window("m", 0, i64::MAX).await, (3, 0));
    }

    #[tokio::test]
    async fn redis_avail_buffers_flushes_and_windows() {
        let Ok(url) = std::env::var("GW_TEST_REDIS_URL") else {
            return;
        };
        let store = RedisAvail::connect(&url).await.expect("redis connect");
        let model = format!("m-{}", std::process::id());
        store.record(&model, true);
        store.record(&model, true);
        store.record(&model, false);
        let minute = crate::epoch_secs() / 60;
        assert_eq!(
            store.window(&model, minute - 1, minute + 1).await,
            (0, 0),
            "buffered until flush"
        );
        store.flush().await;
        assert_eq!(store.window(&model, minute - 1, minute + 1).await, (2, 1));
    }
}
