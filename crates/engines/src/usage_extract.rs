//! Usage normalization.
//!
//! Engines stash the vendor's raw `usage` subtree on `GatewayResponse
//! .raw_usage`; this pure function maps it into the normalized
//! [`CommonUsage`] view. The DAG post-process node calls it.

use gw_models::CommonUsage;
use serde_json::Value;

/// Extract a normalized usage view from the vendor's usage subtree.
/// `messages_protocol` selects the Anthropic field map; otherwise OpenAI's.
/// `None` when none of the mapped part fields are present (a total-only
/// vendor) — callers fall back to the top-level counts.
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
        // Anthropic: input/output (+ cache fields). Never trust upstream — floor
        // each part at 0 and sum saturating, so a malformed/hostile usage can't
        // go negative (which would refund quota) or overflow the total.
        let input = get(v, &["input_tokens"]).max(0);
        let output = get(v, &["output_tokens"]).max(0);
        let read_cache = get(v, &["cache_read_input_tokens"]).max(0);
        let write_cache = get(v, &["cache_creation_input_tokens"]).max(0);
        CommonUsage {
            platform_input: input,
            read_cache,
            write_cache,
            completion: output,
            reason: 0,
        }
    } else {
        CommonUsage::from_openai_parts(
            get(v, &["prompt_tokens"]),
            get(v, &["completion_tokens"]),
            get(v, &["prompt_tokens_details", "cached_tokens"]),
            get(v, &["completion_tokens_details", "reasoning_tokens"]),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
