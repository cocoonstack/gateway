//! Request/response plugins, rule-based from config.security. The pre-stage
//! runs before the DAG: a blocklist hit → Block (skipping engine and billing);
//! DLP redacts inbound messages, the Responses native body, and the family
//! typed params. The post-stage redacts the outbound message; streaming
//! surfaces buffer and replay the redacted text when outbound DLP is on.

use gw_config::SecurityConf;
use gw_models::{Block, GatewayRequest, GatewayResponse};

/// Blocklist check. Returns Block on a hit (block=true implies hit=true).
/// Scans the same inbound text DLP covers, so no surface is a bypass; takes
/// `&mut` only to share the one traversal with the redaction pass.
pub fn security_check(sec: &SecurityConf, request: &mut GatewayRequest) -> Option<Block> {
    if sec.blocklist.is_empty() {
        return None;
    }
    // the shared traversal counts hits; a String is never rewritten here
    let mut scan = |s: &mut String| usize::from(blocklist_hit(sec, s));
    let mut blocked = 0;
    for msg in &mut request.message {
        blocked += usize::from(blocklist_hit(sec, &msg.content));
        if let Some(parts) = &mut msg.parts {
            blocked += walk_json_strings(parts, &mut scan);
        }
    }
    if let Some(param) = request.model_param_v2.as_mut() {
        blocked += walk_json_strings(&mut param.raw, &mut scan);
        if let Some(typed) = param.typed.as_mut() {
            blocked += for_each_typed_text(typed, &mut scan);
        }
    }
    if blocked > 0 {
        let e = gw_consts::error_code::exceptions::empty_resp_err();
        return Some(Block::blocked(e.msg, e.code as i32));
    }
    None
}

/// Case-insensitive blocklist test; terms are pre-lowercased at config load.
/// ASCII text matches without allocating; non-ASCII falls back to a lowercase copy.
fn blocklist_hit(sec: &SecurityConf, text: &str) -> bool {
    if text.is_ascii() {
        return sec
            .blocklist
            .iter()
            .any(|w| contains_ignore_ascii_case(text, w));
    }
    let lower = text.to_lowercase();
    sec.blocklist.iter().any(|w| lower.contains(w))
}

fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    let (h, n) = (haystack.as_bytes(), needle.as_bytes());
    if n.is_empty() || n.len() > h.len() {
        return false;
    }
    h.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n))
}

/// Walk every string leaf of a JSON value with a visitor that may rewrite it;
/// returns summed hits. One walker serves both the blocklist scan and DLP.
fn walk_json_strings(v: &mut serde_json::Value, f: &mut impl FnMut(&mut String) -> usize) -> usize {
    match v {
        serde_json::Value::String(s) => f(s),
        serde_json::Value::Array(a) => a.iter_mut().map(|x| walk_json_strings(x, f)).sum(),
        serde_json::Value::Object(o) => o.values_mut().map(|x| walk_json_strings(x, f)).sum(),
        _ => 0,
    }
}

/// The free-text fields of the family typed params — the ONE field list both
/// the blocklist scan and DLP redaction traverse, so a field added here is
/// covered by both. Chat `tools`/`tool_choice` are client JSON forwarded to
/// the vendor, so their string leaves count too.
fn for_each_typed_text(
    typed: &mut gw_models::TypedParams,
    f: &mut impl FnMut(&mut String) -> usize,
) -> usize {
    use gw_models::TypedParams as T;
    match typed {
        T::Chat(p) => {
            let mut n = p.system.as_mut().map(&mut *f).unwrap_or(0);
            if let Some(t) = p.tools.as_mut() {
                n += walk_json_strings(t, f);
            }
            if let Some(t) = p.tool_choice.as_mut() {
                n += walk_json_strings(t, f);
            }
            n
        }
        T::Embeddings(p) => p.input.iter_mut().map(&mut *f).sum(),
        T::AudioTts(p) => f(&mut p.input),
        T::Image(p) => f(&mut p.prompt),
        T::Video(p) => f(&mut p.prompt),
        T::Search(p) => f(&mut p.query),
        T::AudioStt(_) => 0,
    }
}

/// Blocklist scan over a realtime frame's text-bearing fields — the same terms
/// every REST surface blocks, so the WebSocket surface is not a bypass.
pub fn realtime_frame_blocked(sec: &SecurityConf, frame: &mut serde_json::Value) -> Option<Block> {
    if sec.blocklist.is_empty() {
        return None;
    }
    let hits =
        gw_engines::realtime::visit_frame_text(frame, &mut |s| usize::from(blocklist_hit(sec, s)));
    (hits > 0).then(|| {
        let e = gw_consts::error_code::exceptions::empty_resp_err();
        Block::blocked(e.msg, e.code as i32)
    })
}

/// DLP-redact a realtime frame's text-bearing fields in place; the hit count.
/// Per-frame best effort: a PII span straddling two deltas is not caught — a
/// realtime relay cannot buffer the way the REST stream surfaces do.
pub fn dlp_redact_realtime_frame(sec: &SecurityConf, frame: &mut serde_json::Value) -> usize {
    if !sec.dlp_redact {
        return 0;
    }
    gw_engines::realtime::visit_frame_text(frame, &mut redact_str)
}

/// DLP inbound redaction: emails and 11-digit phone numbers.
pub fn dlp_redact_request(sec: &SecurityConf, request: &mut GatewayRequest) -> usize {
    if !sec.dlp_redact {
        return 0;
    }
    let mut hits = 0;
    for msg in &mut request.message {
        hits += redact_str(&mut msg.content);
        // engines forward `parts` (not `content`) when present, so PII must be
        // scrubbed inside the parts' text blocks too
        if let Some(parts) = &mut msg.parts {
            hits += redact_parts_text(parts);
        }
    }
    // non-chat surfaces carry user text outside `message` (Responses raw body,
    // family typed params) — scrub those too or they reach the vendor unredacted
    if let Some(param) = request.model_param_v2.as_mut() {
        hits += walk_json_strings(&mut param.raw, &mut redact_str);
        if let Some(typed) = param.typed.as_mut() {
            hits += for_each_typed_text(typed, &mut redact_str);
        }
    }
    hits
}

/// Redact one string in place; the hit count.
fn redact_str(s: &mut String) -> usize {
    match redact(s) {
        Some((redacted, n)) => {
            *s = redacted;
            n
        }
        None => 0,
    }
}

/// Redact PII inside a multimodal `parts` array's text blocks, in place.
/// Deliberately narrower than the blocklist scan: non-text parts (image URLs,
/// base64 data) are never rewritten, so a redaction can't corrupt them.
fn redact_parts_text(parts: &mut serde_json::Value) -> usize {
    let Some(arr) = parts.as_array_mut() else {
        return 0;
    };
    let mut hits = 0;
    for p in arr {
        if p["type"] == "text"
            && let Some(t) = p["text"].as_str()
            && let Some((redacted, n)) = redact(t)
        {
            p["text"] = serde_json::Value::String(redacted);
            hits += n;
        }
    }
    hits
}

/// DLP outbound redaction: the flat `message` plus the structured payloads the
/// non-chat surfaces return (`response_v2`, `tool_calls`), so vendor-introduced
/// PII can't leak through a field the surface serializes verbatim.
pub fn dlp_redact_response(sec: &SecurityConf, response: &mut GatewayResponse) -> usize {
    if !sec.dlp_redact {
        return 0;
    }
    let mut hits = redact_str(&mut response.message);
    if let Some(v) = &mut response.response_v2 {
        hits += walk_json_strings(v, &mut redact_str);
    }
    if let Some(v) = &mut response.tool_calls {
        hits += walk_json_strings(v, &mut redact_str);
    }
    hits
}

/// Cheap byte scan gating the full scanner: an email needs an '@', a phone
/// needs an 11-digit run — clean text pays no allocation at all.
fn has_pii_candidate(b: &[u8]) -> bool {
    let mut digits = 0;
    for &c in b {
        if c == b'@' {
            return true;
        }
        if c.is_ascii_digit() {
            digits += 1;
            if digits >= 11 {
                return true;
            }
        } else {
            digits = 0;
        }
    }
    false
}

/// Hand-rolled scanner (no regex dep): masks `local@domain.tld` email shapes and
/// 11-digit CN-mobile runs (1[3-9]xxxxxxxxx). Two-pass: mark spans, then rebuild.
/// `None` when nothing matched — the common case, kept allocation-free.
fn redact(text: &str) -> Option<(String, usize)> {
    if !has_pii_candidate(text.as_bytes()) {
        return None;
    }
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
        return None;
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
    Some((out, hits))
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
        let mut req = GatewayRequest {
            message: vec![ChatMsg::text("user", "say ForbiddenWord now")],
            ..Default::default()
        };
        let block = security_check(&sec(), &mut req).unwrap();
        assert!(block.block && block.hit);
        assert_eq!(block.err_code, 4003);
        assert!(security_check(&sec(), &mut GatewayRequest::default()).is_none());
    }

    #[test]
    fn blocklist_matches_non_ascii_text() {
        let s = SecurityConf {
            blocklist: vec!["forbiddenword".into(), "禁词".into()],
            dlp_redact: false,
        };
        let mut req = GatewayRequest {
            message: vec![ChatMsg::text("user", "前文 FORBIDDENWORD 后文")],
            ..Default::default()
        };
        assert!(security_check(&s, &mut req).is_some());
        let mut req = GatewayRequest {
            message: vec![ChatMsg::text("user", "包含 禁词 的内容")],
            ..Default::default()
        };
        assert!(security_check(&s, &mut req).is_some());
    }

    #[test]
    fn redacts_email_and_phone() {
        let (r, n) = redact("mail me at john.doe@example.com or call 13812345678 ok").unwrap();
        assert_eq!(n, 2);
        assert!(r.contains("[REDACTED_EMAIL]"), "{r}");
        assert!(r.contains("[REDACTED_PHONE]"), "{r}");
        assert!(!r.contains("example.com") && !r.contains("13812345678"));
    }

    #[test]
    fn leaves_clean_text_alone() {
        assert!(redact("nothing sensitive here 123").is_none());
        assert!(redact("digits 1381234567 stop at ten").is_none());
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
