//! Reasoning ("thinking") interchange for the OpenAI-compatible chat surface:
//! the `reasoning_details` unit format that OpenAI-compatible clients already
//! replay (OpenRouter's), its conversion to and from Anthropic thinking
//! blocks, and the effort ↔ budget maps the cross-family engines apply.

use serde_json::{Map, Value};

/// `format` marker on Anthropic-signed units, so a replay knows which vendor
/// can verify them.
pub const FORMAT_ANTHROPIC: &str = "anthropic-claude-v1";

/// How an Anthropic model generation takes a thinking request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThinkingDialect {
    /// `thinking: {type: enabled, budget_tokens}` — Claude 3.x, 4, 4.1, 4.5.
    Budget,
    /// `thinking: {type: adaptive}` + `output_config.effort` — Claude 4.6,
    /// which predates the `display` knob.
    Adaptive,
    /// Adaptive with `display: summarized`, so the reasoning prose comes back:
    /// 4.7+, the 5 family, Fable, Mythos, and any unrecognized Claude model.
    AdaptiveSummarized,
}

/// Reasoning effort → thinking budget in tokens; `None` for `none` and for
/// unknown vocabulary. Fixed per level, not a share of `max_tokens`: Anthropic
/// renders the budget into the prompt, so a moving budget thrashes the cache.
/// `max` stays under the 64K output cap with the answer budget on top.
pub fn effort_budget(effort: &str) -> Option<i64> {
    Some(match effort {
        "minimal" | "low" => 1024,
        "medium" => 4096,
        "high" => 16384,
        "xhigh" => 24576,
        "max" => 32768,
        _ => return None,
    })
}

/// Thinking budget → reasoning effort (the OpenAI and Anthropic `effort`
/// vocabularies coincide).
pub fn budget_effort(budget: i64) -> &'static str {
    match budget {
        ..=4999 => "low",
        5000..=9999 => "medium",
        _ => "high",
    }
}

/// The thinking dialect of a model on the Anthropic wire, by name. Vendors
/// speaking that wire (MiniMax, GLM, Kimi) cloned the budget dialect.
pub fn anthropic_thinking_dialect(model: &str) -> ThinkingDialect {
    // Bedrock ids carry a vendor (and region) prefix: `us.anthropic.claude-…`
    let model = model.find("claude").map_or(model, |i| &model[i..]);
    if !model.starts_with("claude") || model.starts_with("claude-3") {
        return ThinkingDialect::Budget;
    }
    let Some(rest) = model.find("-4-").map(|i| &model[i + 3..]) else {
        return ThinkingDialect::AdaptiveSummarized;
    };
    // "-4-<minor>-…" versus "-4-<yyyymmdd>" (Claude 4.0)
    match rest.as_bytes() {
        [minor, b'-', ..] | [minor] if minor.is_ascii_digit() => match minor {
            b'0'..=b'5' => ThinkingDialect::Budget,
            b'6' => ThinkingDialect::Adaptive,
            _ => ThinkingDialect::AdaptiveSummarized,
        },
        _ => ThinkingDialect::Budget,
    }
}

/// Whether a content block is Anthropic thinking (`thinking` /
/// `redacted_thinking`).
pub fn is_thinking_block(block: &Value) -> bool {
    matches!(
        block["type"].as_str(),
        Some("thinking" | "redacted_thinking")
    )
}

/// An Anthropic `thinking` / `redacted_thinking` block as a `reasoning_details`
/// unit: `reasoning.text` carrying the signature, `reasoning.encrypted`
/// carrying the opaque data. Other blocks yield `None`.
pub fn thinking_block_to_detail(mut block: Value, index: usize) -> Option<Value> {
    let mut detail = Map::new();
    match block["type"].as_str() {
        Some("thinking") => {
            detail.insert("type".into(), "reasoning.text".into());
            detail.insert("text".into(), take_string_or_empty(&mut block, "thinking"));
            detail.insert("signature".into(), block["signature"].take());
        }
        Some("redacted_thinking") => {
            detail.insert("type".into(), "reasoning.encrypted".into());
            detail.insert("data".into(), take_string_or_empty(&mut block, "data"));
        }
        _ => return None,
    }
    detail.insert("format".into(), FORMAT_ANTHROPIC.into());
    detail.insert("index".into(), index.into());
    Some(Value::Object(detail))
}

/// `display: omitted` may leave the prose out; a missing string is `""`, not
/// `null`, on either wire.
fn take_string_or_empty(block: &mut Value, key: &str) -> Value {
    match block.get_mut(key).map(Value::take) {
        Some(value @ Value::String(_)) => value,
        _ => Value::String(String::new()),
    }
}

/// Inverse of [`thinking_block_to_detail`] for a replayed unit: only signed
/// text and Anthropic-format encrypted data become blocks — the vendor rejects
/// an unsigned thinking block, so unsigned prose is dropped rather than
/// forwarded to fail. Anthropic-shaped units pass through as they are.
pub fn detail_to_thinking_block(mut detail: Value) -> Option<Value> {
    let mut block = Map::new();
    match detail["type"].as_str() {
        Some("reasoning.text") => {
            let signature = detail["signature"].take();
            if signature.as_str().is_none_or(str::is_empty) {
                return None;
            }
            block.insert("type".into(), "thinking".into());
            block.insert("thinking".into(), detail["text"].take());
            block.insert("signature".into(), signature);
        }
        Some("reasoning.encrypted") if detail["format"] == FORMAT_ANTHROPIC => {
            block.insert("type".into(), "redacted_thinking".into());
            block.insert("data".into(), detail["data"].take());
        }
        Some("thinking" | "redacted_thinking") => return Some(detail),
        _ => return None,
    }
    Some(Value::Object(block))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn effort_and_budget_maps_are_monotonic() {
        assert!(effort_budget("low") < effort_budget("medium"));
        assert!(effort_budget("medium") < effort_budget("high"));
        assert!(effort_budget("high") < effort_budget("xhigh"));
        assert!(effort_budget("xhigh") < effort_budget("max"));
        assert_eq!(effort_budget("none"), None);
        assert_eq!(effort_budget("bogus"), None);
        assert_eq!(budget_effort(1024), "low");
        assert_eq!(budget_effort(8192), "medium");
        assert_eq!(budget_effort(32768), "high");
    }

    #[test]
    fn anthropic_model_generations_pick_their_thinking_dialect() {
        use ThinkingDialect::*;
        for (model, want) in [
            ("claude-3-7-sonnet-20250219", Budget),
            ("claude-opus-4-20250514", Budget),
            ("claude-opus-4-1-20250805", Budget),
            ("claude-sonnet-4-5-20250929", Budget),
            ("claude-haiku-4-5", Budget),
            ("claude-opus-4-6", Adaptive),
            ("claude-sonnet-4-6-20260301", Adaptive),
            ("claude-opus-4-8", AdaptiveSummarized),
            ("claude-sonnet-5", AdaptiveSummarized),
            ("claude-fable-5", AdaptiveSummarized),
            ("claude-mythos-5", AdaptiveSummarized),
            ("claude-opus-5-1", AdaptiveSummarized),
            ("MiniMax-M3", Budget),
            ("anthropic.claude-3-5-sonnet-20241022-v2:0", Budget),
            ("us.anthropic.claude-sonnet-4-5-20250929-v1:0", Budget),
            ("anthropic.claude-opus-4-6-v1:0", Adaptive),
            ("global.anthropic.claude-sonnet-5-v1:0", AdaptiveSummarized),
        ] {
            assert_eq!(anthropic_thinking_dialect(model), want, "{model}");
        }
    }

    #[test]
    fn thinking_blocks_round_trip_through_details() {
        let thinking = json!({"type":"thinking","thinking":"private","signature":"sig"});
        let redacted = json!({"type":"redacted_thinking","data":"opaque"});
        let text = json!({"type":"text","text":"answer"});
        let details: Vec<Value> = [thinking.clone(), redacted.clone(), text]
            .into_iter()
            .enumerate()
            .filter_map(|(i, b)| thinking_block_to_detail(b, i))
            .collect();
        assert_eq!(
            details,
            [
                json!({"type":"reasoning.text","text":"private","signature":"sig","format":FORMAT_ANTHROPIC,"index":0}),
                json!({"type":"reasoning.encrypted","data":"opaque","format":FORMAT_ANTHROPIC,"index":1}),
            ]
        );
        let blocks: Vec<Value> = details
            .into_iter()
            .filter_map(detail_to_thinking_block)
            .collect();
        assert_eq!(blocks, [thinking, redacted]);
    }

    #[test]
    fn unsigned_and_foreign_details_do_not_become_blocks() {
        assert_eq!(
            detail_to_thinking_block(json!({"type":"reasoning.text","text":"unsigned"})),
            None
        );
        assert_eq!(
            detail_to_thinking_block(
                json!({"type":"reasoning.encrypted","data":"x","format":"openai-responses-v1"})
            ),
            None
        );
        assert_eq!(
            detail_to_thinking_block(json!({"type":"reasoning.summary","summary":"s"})),
            None
        );
        let native = json!({"type":"thinking","thinking":"t","signature":"s"});
        assert_eq!(detail_to_thinking_block(native.clone()), Some(native));
    }
}
