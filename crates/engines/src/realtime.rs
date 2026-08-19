//! Realtime dialect knowledge: per-vendor turn-start and turn-boundary frame
//! detection for the WebSocket bridge. The views layer only does socket
//! plumbing; what a vendor's frames mean lives here with the other engines.

use serde_json::Value;

/// Whether a client frame is the OpenAI-dialect generation trigger.
pub fn is_response_create(frame: &Value) -> bool {
    frame["type"] == "response.create"
}

/// The client frame that starts a generation — the admission point. OpenAI
/// signals it as `response.create`, Gemini Live as a completed client turn.
pub fn is_client_turn(provider: &str, frame: &Value) -> bool {
    if is_gemini_realtime(provider) {
        return frame["clientContent"]["turnComplete"] == Value::Bool(true)
            || frame["client_content"]["turn_complete"] == Value::Bool(true);
    }
    is_response_create(frame)
}

/// String values that never carry human text (base64 media, protocol ids);
/// everything else is scanned, fail closed, and config objects still recurse.
fn skip_scalar(k: &str, text_delta: bool) -> bool {
    matches!(
        k,
        "audio"
            | "data"
            | "file_data"
            | "file_url"
            | "image_url"
            | "type"
            | "object"
            | "status"
            | "role"
            | "voice"
            | "model"
            | "format"
    ) || (k == "delta" && !text_delta)
        || k == "id"
        || k.ends_with("_id")
}

/// Whether a frame's top-level `delta` carries text (audio deltas reuse the key
/// for base64 a rewrite would corrupt).
fn delta_is_text(frame_type: &str) -> bool {
    frame_type.contains("text")
        || frame_type.contains("transcript")
        || frame_type.contains("arguments")
}

/// Visit a realtime frame's text-bearing string leaves with a visitor that may
/// rewrite them; returns summed hits (see [`skip_scalar`] for what is not text).
pub fn visit_frame_text(v: &mut Value, f: &mut impl FnMut(&mut String) -> usize) -> usize {
    let text_delta = v["type"].as_str().map(delta_is_text).unwrap_or(false);
    walk(v, text_delta, f)
}

fn walk(v: &mut Value, text_delta: bool, f: &mut impl FnMut(&mut String) -> usize) -> usize {
    match v {
        // bare strings inside arrays (schema enum values, prompt variables)
        Value::String(s) => f(s),
        Value::Array(a) => a.iter_mut().map(|x| walk(x, text_delta, f)).sum(),
        Value::Object(o) => o
            .iter_mut()
            .map(|(k, x)| match x {
                Value::String(_) if skip_scalar(k, text_delta) => 0,
                // identifier lists, never prose
                _ if k == "modalities" || k == "output_modalities" => 0,
                _ => walk(x, text_delta, f),
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

/// Text and opaque payload units carried by one OpenAI-dialect output-delta
/// frame. Opaque units count base64 quanta.
pub fn realtime_output_delta(frame: &Value) -> (Option<&str>, usize) {
    // Gemini Live: delivered output rides serverContent.modelTurn parts
    if let Some(parts) = frame["serverContent"]["modelTurn"]["parts"].as_array() {
        let opaque = parts
            .iter()
            .map(|p| {
                p["inlineData"]["data"]
                    .as_str()
                    .map_or(0, |d| d.len().div_ceil(4))
                    + p["text"].as_str().map_or(0, |t| t.len().div_ceil(4))
            })
            .sum();
        return (None, opaque);
    }
    let frame_type = frame["type"].as_str().unwrap_or_default();
    let Some(delta) = frame["delta"].as_str() else {
        return (None, 0);
    };
    if delta_is_text(frame_type) {
        (Some(delta), 0)
    } else if frame_type.contains("audio") {
        (None, delta.len().div_ceil(4))
    } else {
        (None, 0)
    }
}

/// A per-dialect turn boundary's (input, output) tokens — `Some((0, 0))` for a
/// cancelled/empty turn so its reservation settles, `None` off a boundary.
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

/// The audio share of a turn's (input, output) tokens, for models that price
/// audio apart from text; zero when the vendor reports no modality split.
pub fn realtime_audio_tokens(provider: &str, frame: &Value) -> (i64, i64) {
    if is_gemini_realtime(provider) {
        let modality = |details: &Value| {
            details
                .as_array()
                .map(|ds| {
                    ds.iter()
                        .filter(|d| d["modality"] == "AUDIO")
                        .map(|d| d["tokenCount"].as_i64().unwrap_or(0).max(0))
                        .sum()
                })
                .unwrap_or(0)
        };
        let u = &frame["usageMetadata"];
        return (
            modality(&u["promptTokensDetails"]),
            modality(&u["responseTokensDetails"]),
        );
    }
    let u = &frame["response"]["usage"];
    (
        u["input_token_details"]["audio_tokens"]
            .as_i64()
            .unwrap_or(0)
            .max(0),
        u["output_token_details"]["audio_tokens"]
            .as_i64()
            .unwrap_or(0)
            .max(0),
    )
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
    fn output_delta_distinguishes_text_from_audio() {
        let text = json!({"type":"response.output_text.delta","delta":"hello"});
        assert_eq!(realtime_output_delta(&text), (Some("hello"), 0));
        let audio = json!({"type":"response.output_audio.delta","delta":"YWJjZA=="});
        assert_eq!(realtime_output_delta(&audio), (None, 2));
    }

    #[test]
    fn visit_frame_text_covers_tool_output_and_arguments() {
        let mut frame = json!({
            "type": "conversation.item.create",
            "item": {"type": "function_call_output", "call_id": "call_138123456789",
                     "output": "call me at 13812345678"}
        });
        let hits = visit_frame_text(&mut frame, &mut |s| {
            *s = "X".into();
            1
        });
        assert_eq!(hits, 1, "function output is scanned, ids are not");
        assert_eq!(frame["item"]["output"], "X");
        assert_eq!(frame["item"]["call_id"], "call_138123456789");

        let mut tools = json!({
            "type": "session.update",
            "session": {"tools": [{"name": "t", "description": "desc", "parameters": {
                "type": "object",
                "properties": {"city": {"type": "string", "title": "City name",
                                         "enum": ["ForbiddenWord town"]}}
            }}]}
        });
        let mut seen = Vec::new();
        visit_frame_text(&mut tools, &mut |s| {
            seen.push(s.clone());
            1
        });
        seen.sort();
        assert_eq!(
            seen,
            vec![
                "City name".to_owned(),
                "ForbiddenWord town".to_owned(),
                "desc".to_owned(),
                "t".to_owned(),
            ],
            "every text leaf of a tool schema is scanned; `type` identifiers are not"
        );

        let mut modal = json!({
            "type": "session.update",
            "session": {"output_modalities": ["audio"],
                         "instructions": "x"}
        });
        assert_eq!(
            visit_frame_text(&mut modal, &mut |_| 1),
            1,
            "modality identifier lists are never scanned"
        );
        let mut img = json!({
            "type": "conversation.item.create",
            "item": {"content": [{"type": "input_image",
                                   "image_url": "data:image/png;base64,AAAA13812345678A=="}]}
        });
        assert_eq!(
            visit_frame_text(&mut img, &mut |_| 1),
            0,
            "image data URIs are never rewritten"
        );

        let mut file = json!({
            "type": "session.update",
            "session": {"prompt": {"variables": {"doc": {"type": "input_file",
                "filename": "a.pdf", "file_data": "data:application/pdf;base64,AAAA13812345678A"}}}}
        });
        assert_eq!(
            visit_frame_text(&mut file, &mut |_| 1),
            1,
            "only the filename is scanned; file payloads are never rewritten"
        );

        let mut err = json!({"type": "error", "error": {"message": "boom jane@corp.com"}});
        assert_eq!(
            visit_frame_text(&mut err, &mut |_| 1),
            1,
            "error messages are scanned"
        );
    }

    #[test]
    fn transcription_prompt_variables_and_tool_names_are_scanned() {
        let mut session = json!({
            "type": "session.update",
            "session": {
                "audio": {"input": {"format": "pcm16",
                                     "transcription": {"prompt": "expect ForbiddenWord"}}},
                "instructions": "be brief",
                "voice": "alloy",
                "model": "gpt-realtime"
            }
        });
        assert_eq!(
            visit_frame_text(&mut session, &mut |_| 1),
            2,
            "instructions AND the transcription prompt inside audio config are scanned"
        );
        let mut prompt = json!({
            "type": "session.update",
            "session": {"prompt": {"variables": {"customer": "jane@corp.com"}}}
        });
        assert_eq!(
            visit_frame_text(&mut prompt, &mut |_| 1),
            1,
            "prompt variables are scanned"
        );
        let mut tools = json!({
            "type": "session.update",
            "session": {"tools": [{"name": "ForbiddenWord_tool"}],
                         "tool_choice": {"name": "ForbiddenWord_tool"}}
        });
        assert_eq!(
            visit_frame_text(&mut tools, &mut |_| 1),
            2,
            "tool identifiers are scanned like REST"
        );
    }

    #[test]
    fn delta_visited_only_on_text_frames() {
        let mut text = json!({"type": "response.output_text.delta", "delta": "hi"});
        assert_eq!(visit_frame_text(&mut text, &mut |_| 1), 1);
        let mut transcript = json!({"type": "response.audio_transcript.delta", "delta": "hi"});
        assert_eq!(visit_frame_text(&mut transcript, &mut |_| 1), 1);
        let mut audio =
            json!({"type": "response.output_audio.delta", "delta": "AAAA13812345678A=="});
        assert_eq!(
            visit_frame_text(&mut audio, &mut |_| 1),
            0,
            "base64 audio deltas are never visited"
        );
    }

    #[test]
    fn realtime_usage_per_dialect() {
        let done = json!({"type":"response.done","response":{"usage":{"input_tokens":12,"output_tokens":34,
            "input_token_details":{"text_tokens":2,"audio_tokens":10},
            "output_token_details":{"text_tokens":4,"audio_tokens":30}}}});
        let gem_out = json!({"serverContent": {"modelTurn": {"parts": [
            {"text": "pong"}, {"inlineData": {"mimeType": "audio/pcm", "data": "AAAAAAAAAAAA"}}
        ]}}});
        assert_eq!(realtime_output_delta(&gem_out), (None, 4));
        assert!(is_client_turn(
            "openai",
            &json!({"type": "response.create"})
        ));
        assert!(!is_client_turn(
            "openai",
            &json!({"clientContent": {"turnComplete": true}})
        ));
        assert!(is_client_turn(
            "gemini",
            &json!({"clientContent": {"turnComplete": true}})
        ));
        assert!(is_client_turn(
            "google",
            &json!({"client_content": {"turn_complete": true}})
        ));
        assert!(!is_client_turn(
            "gemini",
            &json!({"clientContent": {"turnComplete": false}})
        ));
        assert!(!is_client_turn("gemini", &json!({"setup": {"model": "m"}})));
        assert_eq!(realtime_usage("openai", &done), Some((12, 34)));
        assert_eq!(realtime_usage("azure", &done), Some((12, 34)));
        assert_eq!(realtime_audio_tokens("openai", &done), (10, 30));
        assert_eq!(
            realtime_audio_tokens(
                "gemini",
                &json!({"usageMetadata":{"promptTokensDetails":[{"modality":"AUDIO","tokenCount":7},{"modality":"TEXT","tokenCount":1}],
                                         "responseTokensDetails":[{"modality":"AUDIO","tokenCount":9}]}})
            ),
            (7, 9)
        );
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
