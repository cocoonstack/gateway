//! Per-account call latency: an in-process EWMA the pool ranks same-priority
//! accounts by under `stability.latency_routing`. Each instance learns its own
//! view; an unknown or stale account ranks first so it gets sampled.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

// an account unseen this long is re-probed ahead of every measured one
const STALE_AFTER: Duration = Duration::from_secs(60);
// weight of the newest sample
const ALPHA: f64 = 0.2;

/// Exponentially weighted call latency per account, in milliseconds; a clone shares the samples.
#[derive(Debug, Default, Clone)]
pub struct Latency {
    samples: Arc<DashMap<String, (f64, Instant)>>,
}

impl Latency {
    pub fn record(&self, name: &str, elapsed: Duration) {
        let ms = elapsed.as_secs_f64() * 1e3;
        let mut e = crate::slot_mut(&self.samples, name, || (ms, Instant::now()));
        e.0 += ALPHA * (ms - e.0);
        e.1 = Instant::now();
    }

    /// The ranking key: the EWMA, or 0 for an account never or not recently seen.
    pub fn rank(&self, name: &str) -> f64 {
        self.samples
            .get(name)
            .filter(|e| e.1.elapsed() < STALE_AFTER)
            .map_or(0.0, |e| e.0)
    }

    #[cfg(test)]
    pub(crate) fn backdate(&self, name: &str, by: Duration) {
        if let Some(mut e) = self.samples.get_mut(name) {
            e.1 -= by;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ewma_tracks_samples_and_stale_or_unknown_accounts_rank_first() {
        let l = Latency::default();
        assert_eq!(l.rank("a"), 0.0);
        l.record("a", Duration::from_millis(100));
        assert_eq!(l.rank("a"), 100.0);
        l.record("a", Duration::from_millis(200));
        assert_eq!(l.rank("a"), 120.0);
        l.backdate("a", STALE_AFTER);
        assert_eq!(l.rank("a"), 0.0, "a stale sample counts as unknown");
        l.record("a", Duration::from_millis(50));
        assert_eq!(l.rank("a"), 106.0, "the EWMA survives the stale window");
    }
}
