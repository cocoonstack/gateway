//! Realtime dialect knowledge: per-vendor turn-start and turn-boundary frame
//! detection for the WebSocket bridge. The views layer only does socket
//! plumbing; what a vendor's frames mean lives here with the other engines.

use serde_json::Value;

/// Whether a client frame is the OpenAI-dialect generation trigger. The bridge
/// parses text and binary frames alike, so a binary-encoded event can't slip
/// past the gate.
pub fn is_response_create(frame: &Value) -> bool {
    frame["type"] == "response.create"
}

/// The keys that carry human text in realtime frames (both directions); audio
/// and other base64 payloads never ride under these names, so a rewrite here
/// cannot corrupt them.
const TEXT_KEYS: [&str; 4] = ["text", "transcript", "instructions", "delta"];

/// Visit the text-bearing fields of a realtime frame with a visitor that may
/// rewrite them; returns summed hits. The content-security seam for the
/// WebSocket surface — which fields are text is dialect knowledge owned here.
pub fn visit_frame_text(v: &mut Value, f: &mut impl FnMut(&mut String) -> usize) -> usize {
    match v {
        Value::Array(a) => a.iter_mut().map(|x| visit_frame_text(x, f)).sum(),
        Value::Object(o) => o
            .iter_mut()
            .map(|(k, x)| match x {
                Value::String(s) if TEXT_KEYS.contains(&k.as_str()) => f(s),
                _ => visit_frame_text(x, f),
            })
            .sum(),
        _ => 0,
    }
}

/// A non-OpenAI realtime dialect (Gemini Live family): no turn-start signal to
/// gate before generation; metered off the vendor's own turn-complete frame.
pub fn is_gemini_realtime(provider: &str) -> bool {
    matches!(provider, "google" | "gemini" | "vertex")
}

/// Whether `frame` is a server-initiated (VAD) turn start the gateway must gate.
pub fn realtime_turn_started(provider: &str, frame: &Value) -> bool {
    !is_gemini_realtime(provider) && frame["type"] == "response.created"
}

/// Per-dialect turn boundary → the turn's (input, output) tokens: `Some((0, 0))`
/// for a cancelled/empty turn (so its reservation settles instead of orphaning),
/// `None` for a non-boundary frame. Keyed by provider so every dialect is metered.
pub fn realtime_usage(provider: &str, frame: &Value) -> Option<(i64, i64)> {
    let usage = if is_gemini_realtime(provider) {
        // cumulative usage rides many frames — settle only on turnComplete or it double-counts
        if frame["serverContent"]["turnComplete"] != Value::Bool(true) {
            return None;
        }
        let u = &frame["usageMetadata"];
        let it = u["promptTokenCount"].as_i64().unwrap_or(0);
        let ot = u["responseTokenCount"]
            .as_i64()
            .or_else(|| u["candidatesTokenCount"].as_i64())
            .unwrap_or(0);
        (it, ot)
    } else {
        // a turn ends on response.done, any status, possibly with zero usage
        if frame["type"] != "response.done" {
            return None;
        }
        let u = &frame["response"]["usage"];
        (
            u["input_tokens"].as_i64().unwrap_or(0),
            u["output_tokens"].as_i64().unwrap_or(0),
        )
    };
    // floor at 0 so a negative upstream count can't refund quota or bill negative
    Some((usage.0.max(0), usage.1.max(0)))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn visit_frame_text_hits_text_fields_not_audio() {
        let mut frame = json!({
            "type": "conversation.item.create",
            "item": {"content": [
                {"type": "input_text", "text": "hello"},
                {"type": "input_audio", "audio": "AAAA1381234567890AAA"}
            ]},
            "instructions": "be brief"
        });
        let mut seen = Vec::new();
        visit_frame_text(&mut frame, &mut |s| {
            seen.push(s.clone());
            *s = "X".into();
            1
        });
        seen.sort();
        assert_eq!(seen, vec!["be brief".to_owned(), "hello".to_owned()]);
        assert_eq!(frame["item"]["content"][0]["text"], "X");
        assert_eq!(
            frame["item"]["content"][1]["audio"], "AAAA1381234567890AAA",
            "audio payloads are never rewritten"
        );
    }

    #[test]
    fn realtime_usage_per_dialect() {
        let done = json!({"type":"response.done","response":{"usage":{"input_tokens":12,"output_tokens":34}}});
        assert_eq!(realtime_usage("openai", &done), Some((12, 34)));
        assert_eq!(realtime_usage("azure", &done), Some((12, 34)));
        assert_eq!(
            realtime_usage("openai", &json!({"type":"response.delta","delta":"hi"})),
            None
        );
        let g = json!({"serverContent":{"turnComplete":true},"usageMetadata":{"promptTokenCount":5,"responseTokenCount":9}});
        assert_eq!(realtime_usage("gemini", &g), Some((5, 9)));
        let g2 = json!({"serverContent":{"turnComplete":true},"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":7}});
        assert_eq!(realtime_usage("google", &g2), Some((5, 7)));
        assert_eq!(
            realtime_usage(
                "gemini",
                &json!({"serverContent":{"generationComplete":true},"usageMetadata":{"promptTokenCount":5,"responseTokenCount":9}})
            ),
            None,
            "generationComplete alone is an interim frame — not billed"
        );
        assert_eq!(
            realtime_usage(
                "gemini",
                &json!({"usageMetadata":{"promptTokenCount":5,"responseTokenCount":9}})
            ),
            None,
            "interim cumulative usage is not billed"
        );
        assert_eq!(realtime_usage("gemini", &json!({"serverContent":{}})), None);
        assert_eq!(
            realtime_usage(
                "openai",
                &json!({"type":"response.done","response":{"usage":{"input_tokens":0,"output_tokens":0}}})
            ),
            Some((0, 0)),
            "a zero-usage response.done is still a turn boundary — its reservation must settle"
        );
    }
}
