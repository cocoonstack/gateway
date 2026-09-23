//! Reasoning ("thinking") interchange for the OpenAI-compatible chat surface:
//! the `reasoning_details` unit format that OpenAI-compatible clients already
//! replay (OpenRouter's), its conversion to and from Anthropic thinking
//! blocks, and the effort ↔ budget maps the cross-family engines apply.

use std::borrow::Cow;

use serde_json::{Map, Value};

/// `format` marker on Anthropic-signed units, so a replay knows which vendor
/// can verify them.
pub const FORMAT_ANTHROPIC: &str = "anthropic-claude-v1";

/// Effort tiers, weakest first.
const EFFORT_TIERS: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];

/// Sampling knobs OpenAI answers 400 for while a request reasons.
const SAMPLING_KNOBS: [&str; 4] = [
    "temperature",
    "top_p",
    "presence_penalty",
    "frequency_penalty",
];

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

/// Reasoning effort → thinking budget, fixed per level (a `max_tokens` share
/// would thrash the prompt cache); `None` for `none` and unknown vocabulary.
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
/// vocabularies coincide); buckets split midway between [`effort_budget`]'s
/// levels so a canonical budget round-trips to its own effort.
pub fn budget_effort(budget: i64) -> &'static str {
    match budget {
        ..=2559 => "low",
        2560..=10239 => "medium",
        10240..=20479 => "high",
        20480..=28671 => "xhigh",
        _ => "max",
    }
}

/// The upstream a reasoning body is built for; the tiers a model takes differ per wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffortWire {
    Chat,
    Responses,
    Bedrock,
}

/// The GPT generation of an id OpenAI serves itself (`gpt-5.6-luna` → `(5, 6)`); a
/// vendor-prefixed id is `None`, since OpenRouter and Bedrock normalize the request themselves.
fn openai_generation(model: &str) -> Option<(u32, u32)> {
    let version = model
        .strip_prefix("ft:")
        .unwrap_or(model)
        .strip_prefix("gpt-")?
        .split('-')
        .next()?;
    let (major, minor) = version.split_once('.').unwrap_or((version, "0"));
    let (major, minor) = (major.parse().ok()?, minor.parse().ok()?);
    (major >= 5).then_some((major, minor))
}

/// Whether the request reasons: any effort but `none`, or no effort on GPT-5.0 or GPT-6 onward.
fn reasoning_engaged(generation: (u32, u32), effort: Option<&str>) -> bool {
    match effort {
        Some(effort) => effort != "none",
        None => generation.0 >= 6 || generation == (5, 0),
    }
}

/// An effort the model takes on this wire: past its ceiling clamps down, below its floor
/// clamps up, since either is a vendor 400.
pub fn openai_effort<'a>(model: &str, effort: Cow<'a, str>, wire: EffortWire) -> Cow<'a, str> {
    let generation = openai_generation(model);
    let always_reasons = model.contains("gpt-6-astra");
    let takes_minimal =
        !always_reasons && matches!(generation, None | Some((5, 0))) && wire != EffortWire::Bedrock;
    let takes_none = !always_reasons && generation != Some((5, 0));
    match effort.as_ref() {
        "none" if !takes_none => {
            return Cow::Borrowed(if takes_minimal {
                "minimal"
            } else {
                EFFORT_TIERS[0]
            });
        }
        "minimal" if !takes_minimal => return Cow::Borrowed(EFFORT_TIERS[0]),
        _ => {}
    }
    let top = match (wire, generation) {
        (EffortWire::Chat, Some(generation)) if generation <= (5, 1) => "high",
        (EffortWire::Chat, Some(_)) => "xhigh",
        (EffortWire::Responses, Some(generation)) if generation < (5, 6) => "xhigh",
        _ => "max",
    };
    let tier = |name: &str| EFFORT_TIERS.iter().position(|t| *t == name);
    match (tier(&effort), tier(top)) {
        (Some(want), Some(ceiling)) if want > ceiling => Cow::Borrowed(top),
        _ => effort,
    }
}

/// Clamp the body's effort to a tier the model takes and drop the sampling knobs once it
/// reasons; `logprobs`/`top_logprobs`/`stop` stay so the vendor's own 400 reaches the client.
pub fn normalize_openai_body(model: &str, body: &mut Map<String, Value>, wire: EffortWire) {
    let slot = if body.contains_key("reasoning_effort") {
        body.get_mut("reasoning_effort")
    } else {
        body.get_mut("reasoning")
            .and_then(|reasoning| reasoning.get_mut("effort"))
    };
    let effort = if let Some(slot) = slot
        && slot.is_string()
        && let Value::String(effort) = slot.take()
    {
        *slot = openai_effort(model, Cow::Owned(effort), wire).into();
        slot.as_str()
    } else {
        None
    };
    if openai_generation(model).is_some_and(|generation| reasoning_engaged(generation, effort)) {
        for knob in SAMPLING_KNOBS {
            body.remove(knob);
        }
    }
}

/// The thinking dialect of a model on the Anthropic wire, by name. Vendors
/// speaking that wire (MiniMax, GLM, Kimi) cloned the budget dialect.
pub fn anthropic_thinking_dialect(model: &str) -> ThinkingDialect {
    // ids on Bedrock carry a vendor (and region) prefix: `us.anthropic.claude-…`
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

/// A `thinking` / `redacted_thinking` block as a `reasoning_details` unit
/// (`reasoning.text` + signature, or `reasoning.encrypted`); other blocks yield `None`.
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

/// Inverse of [`thinking_block_to_detail`]: only signed text and Anthropic
/// encrypted data become blocks (the vendor rejects unsigned thinking).
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

/// `display: omitted` may leave the prose out; a missing string is `""`, not
/// `null`, on either wire.
fn take_string_or_empty(block: &mut Value, key: &str) -> Value {
    match block.get_mut(key).map(Value::take) {
        Some(value @ Value::String(_)) => value,
        _ => Value::String(String::new()),
    }
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
        for effort in ["low", "medium", "high", "xhigh", "max"] {
            assert_eq!(budget_effort(effort_budget(effort).unwrap()), effort);
        }
        assert_eq!(budget_effort(8192), "medium");
        assert_eq!(budget_effort(65536), "max");
    }

    #[test]
    fn openai_generations_take_their_own_tiers_per_wire() {
        use EffortWire::*;
        for (model, wire, effort, want) in [
            ("gpt-5", Chat, "max", "high"),
            ("gpt-5-mini", Chat, "xhigh", "high"),
            ("gpt-5", Chat, "none", "minimal"),
            ("gpt-5.1", Chat, "max", "high"),
            ("gpt-5.1", Chat, "none", "none"),
            ("gpt-5.4", Chat, "max", "xhigh"),
            ("gpt-5.4", Chat, "minimal", "low"),
            ("gpt-5.4", Responses, "max", "xhigh"),
            ("gpt-5.5", Responses, "max", "xhigh"),
            ("gpt-5.6-luna", Chat, "max", "xhigh"),
            ("gpt-5.6-luna", Chat, "none", "none"),
            ("gpt-5.6-luna", Responses, "max", "max"),
            ("gpt-6-astra", Chat, "max", "xhigh"),
            ("gpt-6-astra", Chat, "none", "low"),
            ("gpt-6-astra", Responses, "max", "max"),
            ("gpt-6-sol", Chat, "none", "none"),
            ("gpt-6-sol", Chat, "max", "xhigh"),
            ("gpt-6-luna", Chat, "minimal", "low"),
            ("gpt-6-luna", Responses, "none", "none"),
            ("ft:gpt-5.4-mini:org::abc", Chat, "max", "xhigh"),
            ("openai/gpt-5.6-luna", Chat, "max", "max"),
            ("openai/gpt-5.6-luna", Chat, "minimal", "minimal"),
            ("openai/gpt-6-astra", Chat, "none", "low"),
            ("us.openai.gpt-5.6-luna", Bedrock, "max", "max"),
            ("us.openai.gpt-5.6-luna", Bedrock, "none", "none"),
            ("us.openai.gpt-5.6-luna", Bedrock, "minimal", "low"),
            ("us.openai.gpt-6-astra", Bedrock, "none", "low"),
            ("us.openai.gpt-6-sol", Bedrock, "none", "none"),
            ("openai/gpt-6-luna", Chat, "none", "none"),
            ("deepseek-v4", Chat, "max", "max"),
            ("claude-sonnet-5", Chat, "max", "max"),
        ] {
            assert_eq!(
                openai_effort(model, Cow::Borrowed(effort), wire),
                want,
                "{model} on {wire:?} asked {effort}"
            );
        }
    }

    #[test]
    fn sampling_knobs_go_only_once_the_request_reasons() {
        let body = |effort: Option<&str>| -> Map<String, Value> {
            let mut b: Map<String, Value> = serde_json::from_value(json!({
                "temperature": 0.5, "top_p": 0.9, "presence_penalty": 0.5,
                "frequency_penalty": 0.5, "logprobs": true, "stop": ["END"]
            }))
            .unwrap();
            if let Some(effort) = effort {
                b.insert("reasoning_effort".into(), effort.into());
            }
            b
        };
        let kept = |model: &str, effort: Option<&str>| {
            let mut b = body(effort);
            normalize_openai_body(model, &mut b, EffortWire::Chat);
            b.contains_key("temperature") && b.contains_key("presence_penalty")
        };

        for (model, effort) in [
            ("gpt-5.1", Some("high")),
            ("gpt-5.4", Some("low")),
            ("gpt-5.6-luna", Some("xhigh")),
            ("gpt-5", None),
            ("gpt-5", Some("minimal")),
            ("gpt-6-astra", None),
            ("gpt-6-astra", Some("none")),
            ("gpt-6-sol", None),
        ] {
            assert!(!kept(model, effort), "{model} reasons at {effort:?}");
        }

        for (model, effort) in [
            ("gpt-5.1", None),
            ("gpt-5.4", Some("none")),
            ("gpt-5.5", Some("none")),
            ("gpt-5.6-terra", None),
            ("gpt-6-luna", Some("none")),
            ("openai/gpt-6-astra", Some("high")),
            ("us.openai.gpt-5.6-luna", Some("high")),
            ("deepseek-v4", Some("high")),
        ] {
            assert!(kept(model, effort), "{model} does not reason at {effort:?}");
        }

        let mut b = body(Some("high"));
        normalize_openai_body("gpt-6-astra", &mut b, EffortWire::Chat);
        assert_eq!(
            b.keys().collect::<Vec<_>>(),
            ["logprobs", "reasoning_effort", "stop"],
            "only what the vendor must refuse itself stays"
        );
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
            ("claude-opus-5-5", AdaptiveSummarized),
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
