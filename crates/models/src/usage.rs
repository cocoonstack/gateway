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
    /// audio tokens inside `platform_input` / `completion` (realtime, audio
    /// chat); priced apart by the model's token_rate.
    pub audio_input: i64,
    pub audio_output: i64,
    /// 1-hour cache writes inside `write_cache`.
    pub write_cache_1h: i64,
}

impl CommonUsage {
    /// Openai-shaped usage parts under the never-trust-upstream clamps:
    /// cached ⊆ prompt, reasoning ⊆ completion, nothing negative. The ONE
    /// place those rules live — extraction and the direct-setting engines
    /// both call it.
    pub fn from_openai_parts(prompt: i64, completion: i64, cached: i64, reasoning: i64) -> Self {
        let prompt = prompt.max(0);
        let completion = completion.max(0);
        let read_cache = cached.clamp(0, prompt);
        let reason = reasoning.clamp(0, completion);
        Self {
            platform_input: prompt - read_cache,
            read_cache,
            write_cache: 0,
            completion: completion - reason,
            reason,
            audio_input: 0,
            audio_output: 0,
            write_cache_1h: 0,
        }
    }

    /// Audio subsets of the input/completion counts, clamped to their parents.
    pub fn with_audio(mut self, audio_input: i64, audio_output: i64) -> Self {
        self.audio_input = audio_input.clamp(0, self.platform_input);
        self.audio_output = audio_output.clamp(0, self.completion);
        self
    }

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
