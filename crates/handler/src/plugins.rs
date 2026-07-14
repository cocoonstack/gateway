//! Request/response plugins: rule-based, no cloud security service; rules
//! come from config.security.
//!
//! The pre-stage runs before the DAG: a blocklist hit -> Block (the request skips
//! the engine and billing); DLP redacts inbound messages, the Responses native
//! body, and the family typed params. The post-stage redacts the outbound
//! message. Outbound DLP needs the whole message, so when it is enabled the
//! streaming surfaces buffer the response and replay the redacted text (see
//! `spawn_stream_pipeline`) instead of forwarding raw deltas.

use gw_config::SecurityConf;
use gw_models::{Block, GatewayRequest, GatewayResponse};

/// Blocklist check. Returns Block on a hit (block=true implies hit=true). Scans
/// the same inbound text the DLP pass covers — chat messages, the Responses
/// native body, and the family typed params — so no surface is a bypass.
pub fn security_check(sec: &SecurityConf, request: &GatewayRequest) -> Option<Block> {
    if sec.blocklist.is_empty() {
        return None;
    }
    // sec.blocklist is already lower-cased at config load (see SecurityConf)
    let hit = |text: &str| {
        let lower = text.to_lowercase();
        sec.blocklist
            .iter()
            .any(|w| !w.is_empty() && lower.contains(w))
    };
    let mut blocked = false;
    for msg in &request.message {
        blocked |= hit(&msg.content);
        if let Some(parts) = &msg.parts {
            visit_text(parts, &mut |t| blocked |= hit(t));
        }
    }
    if let Some(param) = request.model_param_v2.as_ref() {
        visit_text(&param.raw, &mut |t| blocked |= hit(t));
        if let Some(typed) = param.typed.as_ref() {
            visit_typed_text(typed, &mut |t| blocked |= hit(t));
        }
    }
    if blocked {
        let e = gw_consts::error_code::exceptions::empty_resp_err();
        return Some(Block::blocked(e.msg, e.code as i32));
    }
    None
}

/// Visit every string leaf in a JSON value.
fn visit_text(v: &serde_json::Value, f: &mut impl FnMut(&str)) {
    match v {
        serde_json::Value::String(s) => f(s),
        serde_json::Value::Array(a) => a.iter().for_each(|x| visit_text(x, f)),
        serde_json::Value::Object(o) => o.values().for_each(|x| visit_text(x, f)),
        _ => {}
    }
}

/// Visit the free-text fields of the family typed params. Chat `tools` and
/// `tool_choice` are client-supplied JSON forwarded to the vendor, so their
/// strings (function names/descriptions) must be visited too.
fn visit_typed_text(typed: &gw_models::TypedParams, f: &mut impl FnMut(&str)) {
    use gw_models::TypedParams as T;
    match typed {
        T::Chat(p) => {
            if let Some(s) = &p.system {
                f(s);
            }
            if let Some(t) = &p.tools {
                visit_text(t, f);
            }
            if let Some(t) = &p.tool_choice {
                visit_text(t, f);
            }
        }
        T::Embeddings(p) => p.input.iter().for_each(|s| f(s)),
        T::AudioTts(p) => f(&p.input),
        T::Image(p) => f(&p.prompt),
        T::Video(p) => f(&p.prompt),
        T::Search(p) => f(&p.query),
        T::AudioStt(_) => {}
    }
}

/// DLP inbound redaction: emails and 11-digit phone numbers.
pub fn dlp_redact_request(sec: &SecurityConf, request: &mut GatewayRequest) -> usize {
    if !sec.dlp_redact {
        return 0;
    }
    let mut hits = 0;
    for msg in &mut request.message {
        let (redacted, n) = redact(&msg.content);
        if n > 0 {
            msg.content = redacted;
            hits += n;
        }
        // Multimodal: the engines forward `parts` (not `content`) when present,
        // so PII must be scrubbed inside the parts' text blocks too — otherwise a
        // multimodal request leaks the original PII to the vendor unredacted.
        if let Some(parts) = &mut msg.parts {
            hits += redact_parts_text(parts);
        }
    }
    // Non-chat surfaces carry user text outside `message`: the Responses native
    // body (`raw.input`, instructions, …) and the family typed params
    // (embeddings/tts/image/video/search). Scrub those too or they reach the
    // vendor unredacted.
    if let Some(param) = request.model_param_v2.as_mut() {
        hits += redact_value(&mut param.raw);
        if let Some(typed) = param.typed.as_mut() {
            hits += redact_typed(typed);
        }
    }
    hits
}

/// Redact every string leaf in a JSON value in place (only PII patterns change;
/// non-text values pass through). Used for the Responses native passthrough body.
fn redact_value(v: &mut serde_json::Value) -> usize {
    match v {
        serde_json::Value::String(s) => {
            let (redacted, n) = redact(s);
            if n > 0 {
                *s = redacted;
            }
            n
        }
        serde_json::Value::Array(a) => a.iter_mut().map(redact_value).sum(),
        serde_json::Value::Object(o) => o.values_mut().map(redact_value).sum(),
        _ => 0,
    }
}

/// Redact the free-text fields of the family typed params.
fn redact_typed(typed: &mut gw_models::TypedParams) -> usize {
    use gw_models::TypedParams as T;
    let mut redact_str = |s: &mut String| {
        let (redacted, n) = redact(s);
        if n > 0 {
            *s = redacted;
        }
        n
    };
    match typed {
        T::Chat(p) => {
            let mut n = p.system.as_mut().map(&mut redact_str).unwrap_or(0);
            // `tools`/`tool_choice` are client JSON forwarded to the vendor
            if let Some(t) = p.tools.as_mut() {
                n += redact_value(t);
            }
            if let Some(t) = p.tool_choice.as_mut() {
                n += redact_value(t);
            }
            n
        }
        T::Embeddings(p) => p.input.iter_mut().map(&mut redact_str).sum(),
        T::AudioTts(p) => redact_str(&mut p.input),
        T::Image(p) => redact_str(&mut p.prompt),
        T::Video(p) => redact_str(&mut p.prompt),
        T::Search(p) => redact_str(&mut p.query),
        T::AudioStt(_) => 0,
    }
}

/// Redact PII inside a multimodal `parts` array's text blocks, in place.
/// Returns the hit count. Non-text parts (images etc.) are left untouched.
fn redact_parts_text(parts: &mut serde_json::Value) -> usize {
    let Some(arr) = parts.as_array_mut() else {
        return 0;
    };
    let mut hits = 0;
    for p in arr {
        if p["type"] == "text"
            && let Some(t) = p["text"].as_str()
        {
            let (redacted, n) = redact(t);
            if n > 0 {
                p["text"] = serde_json::Value::String(redacted);
                hits += n;
            }
        }
    }
    hits
}

/// DLP outbound redaction: the flat `message`, plus the structured payloads the
/// non-chat surfaces actually return — `response_v2` (the Responses/embeddings/
/// image native body) and `tool_calls` — so vendor-introduced PII can't leak
/// through a field the surface serializes verbatim.
pub fn dlp_redact_response(sec: &SecurityConf, response: &mut GatewayResponse) -> usize {
    if !sec.dlp_redact {
        return 0;
    }
    let (redacted, n) = redact(&response.message);
    if n > 0 {
        response.message = redacted;
    }
    let mut hits = n;
    if let Some(v) = &mut response.response_v2 {
        hits += redact_value(v);
    }
    if let Some(v) = &mut response.tool_calls {
        hits += redact_value(v);
    }
    hits
}

/// Hand-rolled scanner (no regex dep): masks `local@domain.tld` email shapes and
/// 11-digit CN-mobile runs (1[3-9]xxxxxxxxx). Two-pass: mark spans, then rebuild.
/// Returns (redacted, hit count).
fn redact(text: &str) -> (String, usize) {
    let chars: Vec<char> = text.chars().collect();
    let n = chars.len();
    let is_word = |c: char| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-';

    // span = (start, end_exclusive, replacement)
    let mut spans: Vec<(usize, usize, &str)> = Vec::new();

    // emails: expand around each '@'
    for (i, &c) in chars.iter().enumerate() {
        if c != '@' {
            continue;
        }
        let mut start = i;
        while start > 0 && is_word(chars[start - 1]) {
            start -= 1;
        }
        let mut end = i + 1;
        while end < n && is_word(chars[end]) {
            end += 1;
        }
        let has_local = start < i;
        let domain_has_dot = chars[i + 1..end].contains(&'.');
        if has_local && domain_has_dot {
            spans.push((start, end, "[REDACTED_EMAIL]"));
        }
    }

    // phones: 1[3-9] + 9 digits, not embedded in a longer digit run or an email span
    let in_span =
        |i: usize, spans: &[(usize, usize, &str)]| spans.iter().any(|&(s, e, _)| i >= s && i < e);
    let mut i = 0;
    while i + 10 < n {
        if chars[i] == '1'
            && matches!(chars[i + 1], '3'..='9')
            && chars[i..i + 11].iter().all(|c| c.is_ascii_digit())
            && (i == 0 || !chars[i - 1].is_ascii_digit())
            && (i + 11 >= n || !chars[i + 11].is_ascii_digit())
            && !in_span(i, &spans)
        {
            spans.push((i, i + 11, "[REDACTED_PHONE]"));
            i += 11;
        } else {
            i += 1;
        }
    }

    if spans.is_empty() {
        return (text.to_owned(), 0);
    }
    spans.sort_by_key(|&(s, _, _)| s);
    let hits = spans.len();
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0;
    for (s, e, rep) in spans {
        if s > cursor {
            out.extend(&chars[cursor..s]);
        }
        out.push_str(rep);
        cursor = e;
    }
    if cursor < n {
        out.extend(&chars[cursor..]);
    }
    (out, hits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gw_models::ChatMsg;

    fn sec() -> SecurityConf {
        SecurityConf {
            blocklist: vec!["forbiddenword".into()],
            dlp_redact: true,
        }
    }

    #[test]
    fn blocklist_hits() {
        let req = GatewayRequest {
            message: vec![ChatMsg::text("user", "say ForbiddenWord now")],
            ..Default::default()
        };
        let block = security_check(&sec(), &req).unwrap();
        assert!(block.block && block.hit);
        assert_eq!(block.err_code, 4003);
        assert!(security_check(&sec(), &GatewayRequest::default()).is_none());
    }

    #[test]
    fn redacts_email_and_phone() {
        let (r, n) = redact("mail me at john.doe@example.com or call 13812345678 ok");
        assert_eq!(n, 2);
        assert!(r.contains("[REDACTED_EMAIL]"), "{r}");
        assert!(r.contains("[REDACTED_PHONE]"), "{r}");
        assert!(!r.contains("example.com") && !r.contains("13812345678"));
    }

    #[test]
    fn leaves_clean_text_alone() {
        let (r, n) = redact("nothing sensitive here 123");
        assert_eq!(n, 0);
        assert_eq!(r, "nothing sensitive here 123");
    }

    #[test]
    fn dlp_redacts_multimodal_parts_not_just_content() {
        let mut msg = ChatMsg::text("user", "see image");
        msg.parts = Some(serde_json::json!([
            {"type":"text","text":"my email is jane@corp.com"},
            {"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}
        ]));
        let mut req = GatewayRequest {
            message: vec![msg],
            ..Default::default()
        };
        let n = dlp_redact_request(&sec(), &mut req);
        assert!(n >= 1, "must redact PII inside parts");
        let parts = req.message[0].parts.as_ref().unwrap();
        let text_part = &parts[0]["text"];
        assert!(
            text_part.as_str().unwrap().contains("[REDACTED_EMAIL]"),
            "parts text should be redacted: {text_part}"
        );
        assert!(
            !parts.to_string().contains("jane@corp.com"),
            "original email must not survive anywhere in parts"
        );
        assert_eq!(parts[1]["type"], "image_url");
    }
}
