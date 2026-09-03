//! Platform-total / platform-input token computation.
//!
//! Billing is precision-sensitive, so this applies a weighted-sum formula
//! exactly: each token component (prompt / audio prompt / read-cache /
//! write-cache incl. the 1h tier / completion / audio completion / reasoning)
//! is scaled by a configurable weight and summed, with the result rounded
//! half-away-from-zero (matching Rust `f64::round`). The default rate (config
//! miss) is 1:1 across the board, i.e. a plain sum. `prompt_includes_cache`
//! deducts cache from prompt before weighting.

/// Per-channel/model billing weights (default: 1:1).
#[derive(Debug, Clone, PartialEq)]
pub struct TokenRate {
    /// upstream prompt_tokens already includes read+write cache → deduct first.
    pub prompt_includes_cache: bool,
    pub prompt_weight: f64,
    /// audio input tokens (a subset of prompt) at their own price
    pub audio_prompt_weight: f64,
    pub read_cache_weight: f64,
    pub write_cache_weight: f64,
    /// 1-hour cache writes (a subset of write_cache) at their premium
    pub write_cache_1h_weight: f64,
    pub completion_weight: f64,
    /// audio output tokens (a subset of completion) at their own price
    pub audio_completion_weight: f64,
    pub reasoning_weight: f64,
}

impl Default for TokenRate {
    /// Default pay-go rate: all weights 1.0, prompt does not include cache.
    fn default() -> Self {
        Self {
            prompt_includes_cache: false,
            prompt_weight: 1.0,
            audio_prompt_weight: 1.0,
            read_cache_weight: 1.0,
            write_cache_weight: 1.0,
            write_cache_1h_weight: 1.0,
            completion_weight: 1.0,
            audio_completion_weight: 1.0,
            reasoning_weight: 1.0,
        }
    }
}

/// Token components of one call. `audio_prompt` ⊆ `prompt`,
/// `write_cache_1h` ⊆ `write_cache`, `audio_completion` ⊆ `completion`.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokenInput {
    pub prompt: i64,
    pub audio_prompt: i64,
    pub read_cache: i64,
    pub write_cache: i64,
    pub write_cache_1h: i64,
    pub completion: i64,
    pub audio_completion: i64,
    pub reasoning: i64,
}

/// Cost in micro-dollars for one call at per-1k-token prices. Saturating, so a
/// malformed/hostile token count can't overflow the multiply into a wrong bill.
pub fn cost_micros(prompt: i64, completion: i64, price_per_1k: (i64, i64)) -> i64 {
    (prompt.saturating_mul(price_per_1k.0) / 1000)
        .saturating_add(completion.saturating_mul(price_per_1k.1) / 1000)
}

/// Weighted input-side tokens: prompt (text and audio) plus cache reads and
/// writes (5m and 1h tiers).
pub fn weighted_prompt(input: &TokenInput, rate: &TokenRate) -> i64 {
    let prompt = normalize_prompt(input, rate);
    let audio = input.audio_prompt.clamp(0, prompt);
    let write_1h = input.write_cache_1h.clamp(0, input.write_cache.max(0));
    let sum = (prompt - audio) as f64 * rate.prompt_weight
        + audio as f64 * rate.audio_prompt_weight
        + input.read_cache as f64 * rate.read_cache_weight
        + (input.write_cache - write_1h) as f64 * rate.write_cache_weight
        + write_1h as f64 * rate.write_cache_1h_weight;
    round_tokens(sum)
}

/// Weighted output-side tokens: completion (text and audio) plus reasoning.
pub fn weighted_completion(input: &TokenInput, rate: &TokenRate) -> i64 {
    let audio = input.audio_completion.clamp(0, input.completion.max(0));
    let sum = (input.completion - audio) as f64 * rate.completion_weight
        + audio as f64 * rate.audio_completion_weight
        + input.reasoning as f64 * rate.reasoning_weight;
    round_tokens(sum)
}

/// A long-context tier: past `threshold` prompt tokens the whole call bills
/// at the tier's multipliers (Anthropic's >200k pricing).
pub fn long_context_scale(
    prompt_total: i64,
    threshold: i64,
    weights: (f64, f64),
    billable: (i64, i64),
) -> (i64, i64) {
    if prompt_total <= threshold {
        return billable;
    }
    (
        round_tokens(billable.0 as f64 * weights.0),
        round_tokens(billable.1 as f64 * weights.1),
    )
}

/// Weighted (prompt, completion) for the paths carrying no cache/reasoning
/// components (estimates, realtime turns).
pub fn weighted_pair(prompt: i64, completion: i64, rate: &TokenRate) -> (i64, i64) {
    let input = TokenInput {
        prompt,
        completion,
        ..Default::default()
    };
    (
        weighted_prompt(&input, rate),
        weighted_completion(&input, rate),
    )
}

/// Cache-normalized prompt (clamped at 0).
fn normalize_prompt(input: &TokenInput, rate: &TokenRate) -> i64 {
    let mut prompt = input.prompt;
    if rate.prompt_includes_cache {
        prompt -= input.read_cache + input.write_cache;
    }
    prompt.max(0)
}

fn round_tokens(sum: f64) -> i64 {
    if sum < 0.0 { 0 } else { sum.round() as i64 }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> TokenInput {
        TokenInput {
            prompt: 10,
            read_cache: 2,
            write_cache: 1,
            completion: 5,
            reasoning: 2,
            ..Default::default()
        }
    }

    #[test]
    fn audio_and_1h_cache_subsets_take_their_own_weights() {
        let rate = TokenRate {
            audio_prompt_weight: 4.0,
            audio_completion_weight: 8.0,
            write_cache_weight: 1.25,
            write_cache_1h_weight: 2.0,
            ..Default::default()
        };
        let input = TokenInput {
            prompt: 10,
            audio_prompt: 4,
            write_cache: 6,
            write_cache_1h: 2,
            completion: 5,
            audio_completion: 3,
            ..Default::default()
        };
        assert_eq!(weighted_prompt(&input, &rate), 31);
        assert_eq!(weighted_completion(&input, &rate), 26);
        let hostile = TokenInput {
            prompt: 2,
            audio_prompt: 9,
            completion: 1,
            audio_completion: 9,
            ..Default::default()
        };
        assert_eq!(weighted_prompt(&hostile, &rate), 8);
        assert_eq!(weighted_completion(&hostile, &rate), 8);
        assert_eq!(
            long_context_scale(250_000, 200_000, (2.0, 1.5), (100, 10)),
            (200, 15)
        );
        assert_eq!(
            long_context_scale(200_000, 200_000, (2.0, 1.5), (100, 10)),
            (100, 10)
        );
    }

    #[test]
    fn default_rate_is_plain_sum() {
        let rate = TokenRate::default();
        assert_eq!(weighted_prompt(&sample(), &rate), 13);
        assert_eq!(weighted_completion(&sample(), &rate), 7);
    }

    #[test]
    fn prompt_includes_cache_deducts_before_weighting() {
        let rate = TokenRate {
            prompt_includes_cache: true,
            ..Default::default()
        };
        assert_eq!(weighted_prompt(&sample(), &rate), 10);
    }

    #[test]
    fn weights_and_rounding() {
        let rate = TokenRate {
            prompt_weight: 1.5,
            completion_weight: 0.5,
            ..Default::default()
        };
        assert_eq!(weighted_prompt(&sample(), &rate), 18);
        assert_eq!(weighted_completion(&sample(), &rate), 5);
    }

    #[test]
    fn cache_discount_weights_bill_each_side() {
        let rate = TokenRate {
            read_cache_weight: 0.1,
            write_cache_weight: 1.25,
            ..Default::default()
        };
        let input = TokenInput {
            prompt: 100,
            read_cache: 1000,
            write_cache: 40,
            completion: 50,
            reasoning: 10,
            ..Default::default()
        };
        assert_eq!(weighted_prompt(&input, &rate), 250);
        assert_eq!(weighted_completion(&input, &rate), 60);
    }

    #[test]
    fn weighted_pair_carries_flat_counts() {
        let rate = TokenRate {
            prompt_weight: 0.5,
            ..Default::default()
        };
        assert_eq!(weighted_pair(100, 50, &rate), (50, 50));
    }

    #[test]
    fn negative_prompt_clamped_to_zero() {
        let rate = TokenRate {
            prompt_includes_cache: true,
            ..Default::default()
        };
        let input = TokenInput {
            prompt: 1,
            read_cache: 5,
            write_cache: 5,
            ..Default::default()
        };
        assert_eq!(weighted_prompt(&input, &rate), 10);
    }
}
