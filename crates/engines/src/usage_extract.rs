//! Usage normalization.
//!
//! Engines stash the vendor's raw `usage` subtree bytes on `GatewayResponse
//! .raw_usage_json`; this pure function maps them into the normalized
//! [`CommonUsage`] view. The DAG post-process node calls it.

use gw_models::CommonUsage;
use serde_json::Value;

/// Extract a normalized usage view from raw vendor usage JSON.
/// `messages_protocol` selects the Anthropic field map; otherwise OpenAI's.
/// Returns `None` when the bytes are empty/unparseable — callers fall back to
/// the top-level token fields.
pub fn extract_common_usage(raw: &[u8], messages_protocol: bool) -> Option<CommonUsage> {
    if raw.is_empty() {
        return None;
    }
    let v: Value = serde_json::from_slice(raw).ok()?;
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

    Some(if messages_protocol {
        // Anthropic: input/output (+ cache fields). Never trust upstream — floor
        // each part at 0 and sum saturating, so a malformed/hostile usage can't
        // go negative (which would refund quota) or overflow the total.
        let input = get(&v, &["input_tokens"]).max(0);
        let output = get(&v, &["output_tokens"]).max(0);
        let read_cache = get(&v, &["cache_read_input_tokens"]).max(0);
        let write_cache = get(&v, &["cache_creation_input_tokens"]).max(0);
        CommonUsage {
            platform_input: input,
            read_cache,
            write_cache,
            completion: output,
            reason: 0,
        }
    } else {
        // OpenAI: prompt/completion/total (+ details)
        let prompt = get(&v, &["prompt_tokens"]).max(0);
        let completion = get(&v, &["completion_tokens"]).max(0);
        // cached ⊆ prompt and reasoning ⊆ completion by the vendor contract,
        // but never trust upstream: cap the parts so malformed usage can't go
        // negative or make the parts sum past the vendor's own totals
        // (billing recomputes the total from these parts).
        let read_cache = get(&v, &["prompt_tokens_details", "cached_tokens"]).clamp(0, prompt);
        let reason =
            get(&v, &["completion_tokens_details", "reasoning_tokens"]).clamp(0, completion);
        CommonUsage {
            platform_input: prompt - read_cache,
            read_cache,
            write_cache: 0,
            completion: completion - reason,
            reason,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_map() {
        let raw = br#"{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15,
            "prompt_tokens_details":{"cached_tokens":4},
            "completion_tokens_details":{"reasoning_tokens":2}}"#;
        let u = extract_common_usage(raw, false).unwrap();
        assert_eq!(u.platform_input, 6);
        assert_eq!(u.read_cache, 4);
        assert_eq!(u.completion, 3);
        assert_eq!(u.reason, 2);
    }

    #[test]
    fn malformed_usage_never_bills_negative_or_inflated() {
        let raw = br#"{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5,
            "prompt_tokens_details":{"cached_tokens":9},
            "completion_tokens_details":{"reasoning_tokens":9}}"#;
        let u = extract_common_usage(raw, false).unwrap();
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
        let raw = br#"{"input_tokens":8,"output_tokens":6,"cache_read_input_tokens":2}"#;
        let u = extract_common_usage(raw, true).unwrap();
        assert_eq!(u.platform_input, 8);
        assert_eq!(u.completion, 6);
        assert_eq!(u.read_cache, 2);
    }

    #[test]
    fn anthropic_negative_usage_is_floored() {
        let raw = br#"{"input_tokens":-5,"output_tokens":-3,"cache_read_input_tokens":-1}"#;
        let u = extract_common_usage(raw, true).unwrap();
        assert_eq!(u.platform_input, 0, "negative floored, no quota refund");
        assert_eq!(u.completion, 0);
        assert_eq!(u.read_cache, 0);
    }

    #[test]
    fn empty_or_garbage_is_none() {
        assert!(extract_common_usage(b"", false).is_none());
        assert!(extract_common_usage(b"not-json", false).is_none());
    }
}
