//! Usage normalization.
//!
//! Engines stash the vendor's raw `usage` subtree on `GatewayResponse
//! .raw_usage`; this pure function maps it into the normalized
//! [`CommonUsage`] view. The DAG post-process node calls it.

use gw_models::CommonUsage;
use serde_json::Value;

/// The vendor's own charge for the call in micro-dollars, when the usage subtree
/// carries one: OpenRouter's `cost` in USD on both wires, buffered and streamed,
/// or xAI's `cost_in_usd_ticks` at 1e-10 USD a tick.
pub fn extract_vendor_cost_micros(v: &Value) -> Option<i64> {
    let micros = match v.get("cost").and_then(Value::as_f64) {
        Some(usd) => usd * 1_000_000.0,
        None => v.get("cost_in_usd_ticks")?.as_f64()? / 1e4,
    };
    (micros.is_finite() && micros >= 0.0).then_some(micros.round() as i64)
}

/// A normalized usage view of the vendor's usage subtree (Anthropic or OpenAI
/// field map); `None` for a total-only vendor, callers keep the top-level counts.
pub fn extract_common_usage(v: &Value, messages_protocol: bool) -> Option<CommonUsage> {
    fn get(v: &Value, path: &[&str]) -> i64 {
        let mut cur = v;
        for p in path {
            match cur.get(p) {
                Some(n) => cur = n,
                None => return 0,
            }
        }
        cur.as_i64().unwrap_or(0)
    }

    let keys: &[&str] = if messages_protocol {
        &["input_tokens", "output_tokens"]
    } else {
        &["prompt_tokens", "completion_tokens"]
    };
    if keys.iter().all(|k| v.get(k).is_none()) {
        return None;
    }
    Some(if messages_protocol {
        // floored and saturating: a hostile usage must not refund quota or overflow
        let input = get(v, &["input_tokens"]).max(0);
        let output = get(v, &["output_tokens"]).max(0);
        let read_cache = get(v, &["cache_read_input_tokens"]).max(0);
        let write_cache = get(v, &["cache_creation_input_tokens"]).max(0);
        // thinking tokens are already inside output_tokens
        let reason = get(v, &["output_tokens_details", "thinking_tokens"]).clamp(0, output);
        CommonUsage {
            platform_input: input,
            read_cache,
            write_cache,
            completion: output - reason,
            reason,
            audio_input: 0,
            audio_output: 0,
            write_cache_1h: get(v, &["cache_creation", "ephemeral_1h_input_tokens"])
                .clamp(0, write_cache),
        }
    } else {
        let prompt = get(v, &["prompt_tokens"]).max(0);
        let completion = get(v, &["completion_tokens"]).max(0);
        let reason = get(v, &["completion_tokens_details", "reasoning_tokens"]).max(0);
        // xAI's chat wire adds reasoning into total_tokens instead of completion_tokens
        let outside = get(v, &["total_tokens"])
            .saturating_sub(prompt)
            .saturating_sub(completion)
            .clamp(0, reason);
        CommonUsage::from_openai_parts(
            prompt,
            completion.saturating_add(outside),
            get(v, &["prompt_tokens_details", "cached_tokens"]),
            reason,
        )
        .with_cache_write(get(v, &["prompt_tokens_details", "cache_write_tokens"]))
        .with_audio(
            get(v, &["prompt_tokens_details", "audio_tokens"]),
            get(v, &["completion_tokens_details", "audio_tokens"]),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vendor_cost_from_either_wire() {
        let chat = serde_json::json!({"prompt_tokens":8,"completion_tokens":4,"cost":3.6e-06});
        assert_eq!(extract_vendor_cost_micros(&chat), Some(4));
        let messages = serde_json::json!({"input_tokens":8,"output_tokens":4,"cost":0.000084});
        assert_eq!(extract_vendor_cost_micros(&messages), Some(84));
        let big = serde_json::json!({"cost":1.5});
        assert_eq!(extract_vendor_cost_micros(&big), Some(1_500_000));
        let xai = serde_json::json!({"prompt_tokens":650,"cost_in_usd_ticks":26_860_000i64});
        assert_eq!(extract_vendor_cost_micros(&xai), Some(2686));
        let image = serde_json::json!({"cost_in_usd_ticks":400_000_000i64});
        assert_eq!(extract_vendor_cost_micros(&image), Some(40_000));
    }

    #[test]
    fn vendor_cost_absent_or_unusable() {
        for raw in [
            serde_json::json!({"prompt_tokens":8,"completion_tokens":4}),
            serde_json::json!({"cost":null}),
            serde_json::json!({"cost":"0.001"}),
            serde_json::json!({"cost":-1.0}),
            serde_json::json!({"cost":f64::NAN}),
            serde_json::json!({"cost":f64::INFINITY}),
            serde_json::json!({"cost_in_usd_ticks":null}),
            serde_json::json!({"cost_in_usd_ticks":-1.0}),
        ] {
            assert_eq!(extract_vendor_cost_micros(&raw), None, "{raw}");
        }
    }

    #[test]
    fn xai_chat_reasoning_outside_completion_still_bills_as_output() {
        let raw = serde_json::json!({"prompt_tokens":650,"completion_tokens":3,"total_tokens":1041,
            "prompt_tokens_details":{"cached_tokens":640},
            "completion_tokens_details":{"reasoning_tokens":388}});
        let u = extract_common_usage(&raw, false).unwrap();
        assert_eq!((u.platform_input, u.read_cache), (10, 640));
        assert_eq!((u.completion, u.reason), (3, 388));
        assert_eq!(
            u.completion_total(),
            391,
            "reasoning bills at the output rate"
        );

        let normalized = serde_json::json!({"prompt_tokens":220,"completion_tokens":302,"total_tokens":522,
            "completion_tokens_details":{"reasoning_tokens":299}});
        let u = extract_common_usage(&normalized, false).unwrap();
        assert_eq!(
            (u.completion, u.reason, u.completion_total()),
            (3, 299, 302),
            "a vendor counting reasoning inside completion is untouched"
        );
    }

    #[test]
    fn an_inflated_total_cannot_bill_past_the_reasoning_count() {
        let raw = serde_json::json!({"prompt_tokens":10,"completion_tokens":5,"total_tokens":9_000,
            "completion_tokens_details":{"reasoning_tokens":2}});
        let u = extract_common_usage(&raw, false).unwrap();
        assert_eq!((u.completion, u.reason, u.completion_total()), (5, 2, 7));

        let negative = serde_json::json!({"prompt_tokens":10,"completion_tokens":5,"total_tokens":9_000,
            "completion_tokens_details":{"reasoning_tokens":-3}});
        let u = extract_common_usage(&negative, false).unwrap();
        assert_eq!((u.completion, u.reason), (5, 0));
    }

    #[test]
    fn openai_map() {
        let raw = serde_json::json!({"prompt_tokens":10,"completion_tokens":5,"total_tokens":15,
            "prompt_tokens_details":{"cached_tokens":4},
            "completion_tokens_details":{"reasoning_tokens":2}});
        let u = extract_common_usage(&raw, false).unwrap();
        assert_eq!(u.platform_input, 6);
        assert_eq!(u.read_cache, 4);
        assert_eq!(u.completion, 3);
        assert_eq!(u.reason, 2);
    }

    #[test]
    fn openai_cache_writes_leave_the_fresh_input_count() {
        let raw = serde_json::json!({"prompt_tokens":5567,"completion_tokens":4,"total_tokens":5571,
            "prompt_tokens_details":{"cached_tokens":0,"cache_write_tokens":5564},
            "completion_tokens_details":{"reasoning_tokens":0}});
        let u = extract_common_usage(&raw, false).unwrap();
        assert_eq!(
            (u.platform_input, u.write_cache, u.read_cache),
            (3, 5564, 0)
        );
        assert_eq!(u.prompt_total(), 5567);

        let raw = serde_json::json!({"prompt_tokens":10,"completion_tokens":2,
            "prompt_tokens_details":{"cached_tokens":4,"cache_write_tokens":99}});
        let u = extract_common_usage(&raw, false).unwrap();
        assert_eq!(
            (u.platform_input, u.read_cache, u.write_cache),
            (0, 4, 6),
            "a write past the fresh remainder is capped, not added"
        );
    }

    #[test]
    fn malformed_usage_never_bills_negative_or_inflated() {
        let raw = serde_json::json!({"prompt_tokens":3,"completion_tokens":2,"total_tokens":5,
            "prompt_tokens_details":{"cached_tokens":9},
            "completion_tokens_details":{"reasoning_tokens":9}});
        let u = extract_common_usage(&raw, false).unwrap();
        assert_eq!(u.platform_input, 0, "clamped, not negative");
        assert_eq!(u.completion, 0, "clamped, not negative");
        assert_eq!(u.read_cache, 3, "capped at prompt_tokens");
        assert_eq!(u.reason, 2, "capped at completion_tokens");
        assert_eq!(
            u.platform_input + u.read_cache + u.write_cache + u.completion + u.reason,
            5,
            "parts sum to the vendor total — no overbilling"
        );
    }

    #[test]
    fn anthropic_map() {
        let raw =
            serde_json::json!({"input_tokens":8,"output_tokens":6,"cache_read_input_tokens":2});
        let u = extract_common_usage(&raw, true).unwrap();
        assert_eq!(u.platform_input, 8);
        assert_eq!(u.completion, 6);
        assert_eq!(u.read_cache, 2);
    }

    #[test]
    fn audio_and_1h_cache_subsets_are_read_and_clamped() {
        let raw = serde_json::json!({"prompt_tokens":57,"completion_tokens":233,
            "prompt_tokens_details":{"text_tokens":16,"audio_tokens":41,"cached_tokens":0},
            "completion_tokens_details":{"text_tokens":53,"audio_tokens":180}});
        let u = extract_common_usage(&raw, false).unwrap();
        assert_eq!((u.audio_input, u.audio_output), (41, 180));
        let raw = serde_json::json!({"input_tokens":12,"output_tokens":5,
            "cache_creation_input_tokens":3402,
            "cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":9999}});
        let u = extract_common_usage(&raw, true).unwrap();
        assert_eq!(u.write_cache_1h, 3402, "capped at the cache-write total");
    }

    #[test]
    fn anthropic_negative_usage_is_floored() {
        let raw =
            serde_json::json!({"input_tokens":-5,"output_tokens":-3,"cache_read_input_tokens":-1});
        let u = extract_common_usage(&raw, true).unwrap();
        assert_eq!(u.platform_input, 0, "negative floored, no quota refund");
        assert_eq!(u.completion, 0);
        assert_eq!(u.read_cache, 0);
    }

    #[test]
    fn partless_usage_is_none_not_zeros() {
        assert!(extract_common_usage(&Value::Null, false).is_none());
        let total_only = serde_json::json!({"total_tokens": 9});
        assert!(
            extract_common_usage(&total_only, false).is_none(),
            "a total-only vendor must fall back to top-level counts"
        );
        assert!(extract_common_usage(&total_only, true).is_none());
        let zeroed = serde_json::json!({"prompt_tokens": 0, "completion_tokens": 0});
        assert_eq!(
            extract_common_usage(&zeroed, false),
            Some(CommonUsage::default()),
            "explicitly-zero parts stay a real view"
        );
    }
}
