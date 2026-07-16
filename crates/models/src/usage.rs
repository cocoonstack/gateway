//! Unified usage view.
//!
//! `CommonUsage` is the normalized cross-vendor usage view. It is populated
//! from each engine's raw usage payload by a post-processing step; when
//! extraction misses or fails it stays `None` and callers fall back to the
//! top-level token fields on `GatewayResponse`.

/// Normalized token accounting across vendors.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CommonUsage {
    /// input tokens excluding cache.
    pub platform_input: i64,
    pub read_cache: i64,
    pub write_cache: i64,
    /// completion tokens, excluding reasoning.
    pub completion: i64,
    pub reason: i64,
}

impl CommonUsage {
    /// All input-side tokens: fresh input plus cache reads and writes.
    pub fn prompt_total(&self) -> i64 {
        self.platform_input
            .saturating_add(self.read_cache)
            .saturating_add(self.write_cache)
    }

    /// All output-side tokens: completion plus reasoning.
    pub fn completion_total(&self) -> i64 {
        self.completion.saturating_add(self.reason)
    }
}
