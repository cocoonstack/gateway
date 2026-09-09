//! Concurrent long-lived streams per access key — realtime sessions and MCP
//! listen streams — bounded by `max_live_streams_per_key`; a guard releases
//! the slot when the stream ends.

use std::sync::Arc;

use dashmap::DashMap;

#[derive(Debug, Default)]
pub struct LiveStreams {
    open: DashMap<String, usize>,
}

impl LiveStreams {
    /// Take a slot for `ak`; `None` at `cap` (0 = unlimited).
    pub fn open(self: &Arc<Self>, ak: &str, cap: usize) -> Option<StreamGuard> {
        let mut n = crate::slot_mut(&self.open, ak, || 0);
        if cap > 0 && *n >= cap {
            return None;
        }
        *n += 1;
        drop(n);
        Some(StreamGuard {
            streams: Arc::clone(self),
            ak: ak.to_owned(),
        })
    }
}

/// One held stream slot; dropping it frees the slot.
#[derive(Debug)]
pub struct StreamGuard {
    streams: Arc<LiveStreams>,
    ak: String,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        // the entry lock is released before the removal, which would deadlock on it
        let left = self.streams.open.get_mut(&self.ak).map(|mut n| {
            *n = n.saturating_sub(1);
            *n
        });
        if left == Some(0) {
            self.streams.open.remove_if(&self.ak, |_, n| *n == 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_are_capped_and_released_on_drop() {
        let streams = Arc::new(LiveStreams::default());
        let a = streams.open("k", 2).expect("first slot");
        let b = streams.open("k", 2).expect("second slot");
        assert!(streams.open("k", 2).is_none(), "cap reached");
        assert!(streams.open("other", 2).is_some(), "caps are per key");
        assert_eq!(streams.open.get("k").as_deref(), Some(&2));
        drop(a);
        assert_eq!(streams.open.get("k").as_deref(), Some(&1));
        assert!(streams.open("k", 2).is_some());
        drop(b);
        assert!(streams.open("k", 0).is_some(), "0 = unlimited");
    }
}
