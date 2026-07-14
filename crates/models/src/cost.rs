//! Platform-total / platform-input token computation.
//!
//! Billing is precision-sensitive, so this applies a weighted-sum formula
//! exactly: each token component (prompt / read-cache / write-cache / completion
//! / reasoning) is scaled by a configurable weight and summed, with the result
//! rounded half-away-from-zero (matching Rust `f64::round`). The default rate
//! (config miss) is 1:1 across the board, i.e. a plain sum.
//! `prompt_includes_cache` deducts cache from prompt before weighting.

/// Per-channel/model billing weights (default: 1:1).
#[derive(Debug, Clone, PartialEq)]
pub struct TokenRate {
    /// upstream prompt_tokens already includes read+write cache → deduct first.
    pub prompt_includes_cache: bool,
    pub prompt_weight: f64,
    pub read_cache_weight: f64,
    pub write_cache_weight: f64,
    pub completion_weight: f64,
    pub reasoning_weight: f64,
}

impl Default for TokenRate {
    /// Default pay-go rate: all weights 1.0, prompt does not include cache.
    fn default() -> Self {
        Self {
            prompt_includes_cache: false,
            prompt_weight: 1.0,
            read_cache_weight: 1.0,
            write_cache_weight: 1.0,
            completion_weight: 1.0,
            reasoning_weight: 1.0,
        }
    }
}

/// Token components of one call.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokenInput {
    pub prompt: i64,
    pub read_cache: i64,
    pub write_cache: i64,
    pub completion: i64,
    pub reasoning: i64,
}

/// Cache-normalized prompt (clamped at 0).
fn normalize_prompt(input: &TokenInput, rate: &TokenRate) -> i64 {
    let mut prompt = input.prompt;
    if rate.prompt_includes_cache {
        prompt -= input.read_cache + input.write_cache;
    }
    prompt.max(0)
}

/// Cost in micro-dollars for one call at per-1k-token prices. Saturating, so a
/// malformed/hostile token count can't overflow the multiply into a wrong bill.
pub fn cost_micros(prompt: i64, completion: i64, price_per_1k: (i64, i64)) -> i64 {
    (prompt.saturating_mul(price_per_1k.0) / 1000)
        .saturating_add(completion.saturating_mul(price_per_1k.1) / 1000)
}

/// Weighted platform-total token count.
pub fn platform_total(input: &TokenInput, rate: &TokenRate) -> i64 {
    let prompt = normalize_prompt(input, rate) as f64;
    let total = prompt * rate.prompt_weight
        + input.read_cache as f64 * rate.read_cache_weight
        + input.write_cache as f64 * rate.write_cache_weight
        + input.completion as f64 * rate.completion_weight
        + input.reasoning as f64 * rate.reasoning_weight;
    if total < 0.0 { 0 } else { total.round() as i64 }
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
        }
    }

    #[test]
    fn default_rate_is_plain_sum() {
        assert_eq!(platform_total(&sample(), &TokenRate::default()), 20);
    }

    #[test]
    fn prompt_includes_cache_deducts_before_weighting() {
        let rate = TokenRate {
            prompt_includes_cache: true,
            ..Default::default()
        };
        assert_eq!(platform_total(&sample(), &rate), 17);
    }

    #[test]
    fn weights_and_rounding() {
        let rate = TokenRate {
            prompt_weight: 1.5,
            completion_weight: 0.5,
            ..Default::default()
        };
        assert_eq!(platform_total(&sample(), &rate), 23);
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
        assert_eq!(platform_total(&input, &rate), 10);
    }
}
