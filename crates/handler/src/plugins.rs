//! Request/response plugins, rule-based from config.security. The pre-stage
//! runs before the DAG: a blocklist hit → Block (skipping engine and billing);
//! DLP redacts inbound messages, the Responses native body, and the family
//! typed params. The post-stage redacts the outbound message; streaming
//! surfaces buffer and replay the redacted text when outbound DLP is on.

use gw_config::{Action, SecurityConf};
use gw_models::{Block, GatewayRequest, GatewayResponse};

const BLOCKED_MSG: &str = "this content cannot be answered, please try a different request";

/// One content rule that fired on a request, for the security-event stream.
pub struct RuleHit {
    pub rule: String,
    pub action: Action,
    pub count: i64,
}

/// The result of scanning one request: whether to deny it, plus every rule that
/// fired (for the audit stream). A `block`-action hit fills `block`; `flag`/
/// `shadow` hits only populate `hits`.
#[derive(Default)]
pub struct ScanOutcome {
    pub block: Option<Block>,
    pub hits: Vec<RuleHit>,
}

/// Scan inbound text against the blocklist and the regex recognizers. One
/// traversal of the same fields DLP covers, so no surface is a bypass. `&mut`
/// only to share the traversal; nothing is rewritten here.
pub fn security_check(sec: &SecurityConf, request: &mut GatewayRequest) -> ScanOutcome {
    if sec.blocklist.is_empty() && sec.regexes.is_empty() {
        return ScanOutcome::default();
    }
    let mut counts = ScanCounts::new(sec);
    let mut visit = |s: &mut String| counts.visit(s);
    for msg in &mut request.message {
        visit(&mut msg.content);
        if let Some(parts) = &mut msg.parts {
            walk_part_text(parts, &mut visit);
        }
    }
    if let Some(param) = request.model_param_v2.as_mut() {
        walk_json_strings(&mut param.raw, &mut visit);
        if let Some(typed) = param.typed.as_mut() {
            for_each_typed_text(typed, &mut visit);
        }
    }
    counts.outcome()
}

/// Per-rule hit counters for one scan, shared by the REST request scan and the
/// realtime frame scan so the two apply identical action semantics.
struct ScanCounts<'a> {
    sec: &'a SecurityConf,
    blocklist: i64,
    regex: Vec<i64>,
}

impl<'a> ScanCounts<'a> {
    fn new(sec: &'a SecurityConf) -> Self {
        Self {
            sec,
            blocklist: 0,
            regex: vec![0; sec.regexes.len()],
        }
    }

    fn visit(&mut self, s: &str) -> usize {
        self.blocklist += i64::from(blocklist_hit(self.sec, s));
        for (i, r) in self.sec.regexes.iter().enumerate() {
            self.regex[i] += r.re.find_iter(s).count() as i64;
        }
        0
    }

    /// Fold the counts into a [`ScanOutcome`]: a rule with count > 0 is a hit
    /// at its action; any block-action hit denies.
    fn outcome(self) -> ScanOutcome {
        let mut hits = Vec::new();
        if self.blocklist > 0 {
            hits.push(RuleHit {
                rule: "blocklist".to_owned(),
                action: self.sec.blocklist_action,
                count: self.blocklist,
            });
        }
        for (r, count) in self.sec.regexes.iter().zip(self.regex) {
            if count > 0 {
                hits.push(RuleHit {
                    rule: r.name.clone(),
                    action: r.action,
                    count,
                });
            }
        }
        let block = hits
            .iter()
            .any(|h| h.action == Action::Block)
            .then(|| Block::blocked(BLOCKED_MSG, gw_consts::ErrCode::EMPTY_RESP.value() as i32));
        ScanOutcome { block, hits }
    }
}

/// All inbound text a request carries — message content, multimodal text
/// parts, the Responses raw body, and the family typed params — collected via
/// the same traversals the blocklist scan and DLP run, so the field lists
/// cannot drift apart. The one text view moderation and content retention
/// operate on. `&mut` only to share those traversals; nothing is rewritten.
pub fn inbound_text(request: &mut GatewayRequest) -> String {
    let mut out = String::new();
    let mut collect = |s: &mut String| {
        push_text(&mut out, s);
        0
    };
    for m in &mut request.message {
        collect(&mut m.content);
        if let Some(parts) = &mut m.parts {
            walk_part_text(parts, &mut collect);
        }
    }
    if let Some(param) = request.model_param_v2.as_mut() {
        walk_json_strings(&mut param.raw, &mut collect);
        if let Some(typed) = param.typed.as_mut() {
            for_each_typed_text(typed, &mut collect);
        }
    }
    out
}

fn push_text(out: &mut String, s: &str) {
    if !s.is_empty() {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(s);
    }
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

/// Keys under a multimodal part whose value is binary, a URL, or a structural
/// identifier — never prose: skipped whole so a rewrite can't corrupt them and
/// base64 noise can't false-match a blocklist term. Every OTHER string leaf (a
/// text block's `text`, a `tool_result` block's `content`, a `tool_use` block's
/// `input`) is visited, so no content type is a scan bypass.
fn skip_part_key(k: &str) -> bool {
    matches!(
        k,
        "image_url"
            | "input_audio"
            | "source"
            | "data"
            | "url"
            | "file_data"
            | "file_url"
            | "file"
            | "type"
            | "media_type"
    ) || k == "id"
        || k.ends_with("_id")
}

/// Walk a multimodal `parts` value's text leaves with a visitor that may rewrite
/// them (skipping [`skip_part_key`] subtrees); returns summed hits.
fn walk_part_text(v: &mut serde_json::Value, f: &mut impl FnMut(&mut String) -> usize) -> usize {
    match v {
        serde_json::Value::String(s) => f(s),
        serde_json::Value::Array(a) => a.iter_mut().map(|x| walk_part_text(x, f)).sum(),
        serde_json::Value::Object(o) => o
            .iter_mut()
            .filter(|(k, _)| !skip_part_key(k))
            .map(|(_, x)| walk_part_text(x, f))
            .sum(),
        _ => 0,
    }
}

/// Scan a realtime frame's text-bearing fields against the blocklist AND the
/// regex recognizers, honoring their actions — the same policy every REST
/// surface runs, so the WebSocket surface is not a bypass. A block-action hit
/// denies the frame. `collect_text` also gathers the frame's text (for
/// moderation) in the same traversal, so a moderated tenant pays one walk per
/// frame, not two.
pub fn realtime_frame_scan(
    sec: &SecurityConf,
    frame: &mut serde_json::Value,
    collect_text: bool,
) -> (ScanOutcome, String) {
    let scan_rules = !sec.blocklist.is_empty() || !sec.regexes.is_empty();
    let mut counts = ScanCounts::new(sec);
    let mut text = String::new();
    if scan_rules || collect_text {
        gw_engines::realtime::visit_frame_text(frame, &mut |s| {
            if scan_rules {
                counts.visit(s);
            }
            if collect_text {
                push_text(&mut text, s);
            }
            0
        });
    }
    (counts.outcome(), text)
}

/// DLP-redact a realtime frame's text-bearing fields in place; the hit count.
/// Honors both `dlp_redact` (PII) and `detect_secrets` (credentials), the same
/// as the REST path — a realtime frame must not be a secret-redaction bypass.
/// Per-frame best effort: a PII span straddling two deltas is not caught — a
/// realtime relay cannot buffer the way the REST stream surfaces do.
pub fn dlp_redact_realtime_frame(sec: &SecurityConf, frame: &mut serde_json::Value) -> usize {
    if !sec.dlp_redact && !sec.detect_secrets {
        return 0;
    }
    let (pii, secrets) = (sec.dlp_redact, sec.detect_secrets);
    gw_engines::realtime::visit_frame_text(frame, &mut |s| redact_in_place(s, pii, secrets))
}

/// DLP inbound redaction: emails, 11-digit phone numbers, and — when
/// `detect_secrets` is on — API keys / credentials.
pub fn dlp_redact_request(sec: &SecurityConf, request: &mut GatewayRequest) -> usize {
    if !sec.dlp_redact && !sec.detect_secrets {
        return 0;
    }
    let secrets = sec.detect_secrets;
    let pii = sec.dlp_redact;
    let mut redact_field = |s: &mut String| redact_in_place(s, pii, secrets);
    let mut hits = 0;
    for msg in &mut request.message {
        hits += redact_field(&mut msg.content);
        // engines forward `parts` (not `content`) when present, so PII must be
        // scrubbed inside the parts' text blocks too
        if let Some(parts) = &mut msg.parts {
            hits += walk_part_text(parts, &mut redact_field);
        }
    }
    // non-chat surfaces carry user text outside `message` (Responses raw body,
    // family typed params) — scrub those too or they reach the vendor unredacted
    if let Some(param) = request.model_param_v2.as_mut() {
        hits += walk_json_strings(&mut param.raw, &mut redact_field);
        if let Some(typed) = param.typed.as_mut() {
            hits += for_each_typed_text(typed, &mut redact_field);
        }
    }
    hits
}

/// A privacy-safe copy of `text` for content retention: PII and secrets are
/// ALWAYS stripped, independent of the tenant's forwarding DLP flags. Retention
/// owns its own redaction so a `redacted` row — or a keyless `full` downgrade —
/// can never persist raw secrets/PII even when inline DLP is disabled.
pub fn redact_retained(text: &str) -> String {
    let mut s = text.to_owned();
    redact_in_place(&mut s, true, true);
    s
}

/// Redact one string in place (email/phone via `pii`, secrets via `secrets`);
/// the hit count.
fn redact_in_place(s: &mut String, pii: bool, secrets: bool) -> usize {
    let mut hits = 0;
    if pii && let Some((redacted, n)) = redact(s) {
        *s = redacted;
        hits += n;
    }
    if secrets && let Some((redacted, n)) = redact_secrets(s) {
        *s = redacted;
        hits += n;
    }
    hits
}

/// Mask credential shapes (API keys, tokens, private-key headers) with
/// `[REDACTED_SECRET]`. High-precision patterns to avoid mauling normal text.
fn redact_secrets(text: &str) -> Option<(String, usize)> {
    static SECRETS: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        #[allow(clippy::expect_used)] // a compile-time literal, covered by tests
        regex::Regex::new(
            r"sk-[A-Za-z0-9_-]{20,}|AKIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9]{36,}|xox[baprs]-[A-Za-z0-9-]{10,}|-----BEGIN [A-Z ]*PRIVATE KEY-----",
        )
        .expect("static secret patterns compile")
    });
    let mut count = 0;
    let out = SECRETS.replace_all(text, |_: &regex::Captures| {
        count += 1;
        "[REDACTED_SECRET]"
    });
    (count > 0).then(|| (out.into_owned(), count))
}

/// DLP outbound redaction: the flat `message` plus the structured payloads the
/// non-chat surfaces return (`response_v2`, `tool_calls`), so vendor-introduced
/// PII or echoed credentials can't leak through a field the surface serializes
/// verbatim. Honors both `dlp_redact` and `detect_secrets`, like the inbound
/// and realtime paths.
pub fn dlp_redact_response(sec: &SecurityConf, response: &mut GatewayResponse) -> usize {
    if !sec.redacts_output() {
        return 0;
    }
    let (pii, secrets) = (sec.dlp_redact, sec.detect_secrets);
    let mut redact_field = |s: &mut String| redact_in_place(s, pii, secrets);
    let mut hits = redact_field(&mut response.message);
    if let Some(v) = &mut response.response_v2 {
        hits += walk_json_strings(v, &mut redact_field);
    }
    if let Some(v) = &mut response.tool_calls {
        hits += walk_json_strings(v, &mut redact_field);
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
            ..Default::default()
        }
    }

    #[test]
    fn blocklist_hits() {
        let mut req = GatewayRequest {
            message: vec![ChatMsg::text("user", "say ForbiddenWord now")],
            ..Default::default()
        };
        let block = security_check(&sec(), &mut req).block.unwrap();
        assert!(block.block && block.hit);
        assert_eq!(block.err_code, 4003);
        assert!(
            security_check(&sec(), &mut GatewayRequest::default())
                .block
                .is_none()
        );
    }

    #[test]
    fn blocklist_matches_non_ascii_text() {
        let s = SecurityConf {
            blocklist: vec!["forbiddenword".into(), "禁词".into()],
            dlp_redact: false,
            ..Default::default()
        };
        let mut req = GatewayRequest {
            message: vec![ChatMsg::text("user", "前文 FORBIDDENWORD 后文")],
            ..Default::default()
        };
        assert!(security_check(&s, &mut req).block.is_some());
        let mut req = GatewayRequest {
            message: vec![ChatMsg::text("user", "包含 禁词 的内容")],
            ..Default::default()
        };
        assert!(security_check(&s, &mut req).block.is_some());
    }

    #[test]
    fn inbound_text_covers_non_chat_raw_and_typed() {
        use gw_models::{ModelParamV2, TypedParams};
        let mut param = ModelParamV2::with_name(gw_consts::Protocol::Responses, "m");
        param.raw = serde_json::json!({"input": "secret responses text", "model": "m"});
        let mut req = GatewayRequest {
            model_param_v2: Some(param),
            ..Default::default()
        };
        assert!(
            inbound_text(&mut req).contains("secret responses text"),
            "Responses input rides in raw with an empty message"
        );

        let mut param = ModelParamV2::with_name(gw_consts::Protocol::Embeddings, "m");
        param.typed = Some(TypedParams::Embeddings(gw_models::EmbeddingParams {
            input: vec!["typed embed text".into()],
            dimensions: None,
        }));
        let mut req = GatewayRequest {
            model_param_v2: Some(param),
            ..Default::default()
        };
        assert!(
            inbound_text(&mut req).contains("typed embed text"),
            "embeddings input rides in typed with an empty message"
        );
    }

    fn ssn_block() -> SecurityConf {
        SecurityConf {
            regexes: vec![gw_config::CompiledRule {
                name: "ssn".into(),
                action: Action::Block,
                re: regex::Regex::new(r"\d{3}-\d{2}-\d{4}").unwrap(),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn realtime_frame_scan_honors_regex_and_actions() {
        let s = ssn_block();
        let mut frame = serde_json::json!({"type":"input_text","text":"my ssn is 123-45-6789"});
        let (out, text) = realtime_frame_scan(&s, &mut frame, true);
        assert!(out.block.is_some(), "regex Block denies on realtime too");
        assert!(
            text.contains("123-45-6789"),
            "collect_text gathers the frame text in the same walk"
        );

        let s2 = SecurityConf {
            blocklist: vec!["watch".into()],
            blocklist_action: Action::Flag,
            ..Default::default()
        };
        let mut frame = serde_json::json!({"type":"input_text","text":"please watch this"});
        let (out, text) = realtime_frame_scan(&s2, &mut frame, false);
        assert!(out.block.is_none(), "flag does not block realtime");
        assert_eq!(out.hits.len(), 1);
        assert!(text.is_empty(), "no text collected unless asked");
    }

    #[test]
    fn realtime_frame_redacts_secrets_when_detect_secrets_on() {
        let s = SecurityConf {
            detect_secrets: true,
            dlp_redact: false,
            ..Default::default()
        };
        let mut frame = serde_json::json!({
            "type":"input_text","text":"here is sk-abcdefghijklmnopqrstuvwxyz012345"
        });
        let n = dlp_redact_realtime_frame(&s, &mut frame);
        assert_eq!(n, 1, "secret masked even with dlp_redact off");
        let text = frame["text"].as_str().unwrap();
        assert!(
            text.contains("[REDACTED_SECRET]") && !text.contains("sk-abc"),
            "{text}"
        );

        let none = SecurityConf::default();
        let mut frame =
            serde_json::json!({"type":"input_text","text":"sk-abcdefghijklmnopqrstuvwxyz012345"});
        assert_eq!(
            dlp_redact_realtime_frame(&none, &mut frame),
            0,
            "both flags off leaves the frame untouched"
        );
    }

    #[test]
    fn response_redaction_honors_detect_secrets_alone() {
        let s = SecurityConf {
            detect_secrets: true,
            dlp_redact: false,
            ..Default::default()
        };
        let mut resp = GatewayResponse {
            message: "your key is sk-abcdefghijklmnopqrstuvwxyz012345".into(),
            ..Default::default()
        };
        assert_eq!(dlp_redact_response(&s, &mut resp), 1);
        assert!(
            resp.message.contains("[REDACTED_SECRET]") && !resp.message.contains("sk-abc"),
            "{}",
            resp.message
        );
    }

    #[test]
    fn redact_retained_strips_pii_and_secrets_unconditionally() {
        let out =
            redact_retained("mail john.doe@example.com key sk-abcdefghijklmnopqrstuvwxyz012345");
        assert!(out.contains("[REDACTED_EMAIL]"), "{out}");
        assert!(out.contains("[REDACTED_SECRET]"), "{out}");
        assert!(
            !out.contains("example.com") && !out.contains("sk-abc"),
            "{out}"
        );
        assert_eq!(redact_retained("clean text"), "clean text");
    }

    #[test]
    fn flag_action_records_a_hit_without_blocking() {
        let s = SecurityConf {
            blocklist: vec!["watchword".into()],
            blocklist_action: Action::Flag,
            ..Default::default()
        };
        let mut req = GatewayRequest {
            message: vec![ChatMsg::text("user", "contains watchword here")],
            ..Default::default()
        };
        let out = security_check(&s, &mut req);
        assert!(out.block.is_none(), "flag does not deny");
        assert_eq!(out.hits.len(), 1);
        assert_eq!(out.hits[0].action, Action::Flag);
    }

    #[test]
    fn regex_rule_blocks_and_redact_secrets_masks() {
        let s = ssn_block();
        let mut req = GatewayRequest {
            message: vec![ChatMsg::text("user", "my ssn is 123-45-6789")],
            ..Default::default()
        };
        assert!(
            security_check(&s, &mut req).block.is_some(),
            "regex Block denies"
        );

        let (masked, n) = redact_secrets("key sk-abcdefghijklmnopqrstuvwxyz012345 end").unwrap();
        assert_eq!(n, 1);
        assert!(masked.contains("[REDACTED_SECRET]") && !masked.contains("sk-abc"));
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

    #[test]
    fn blocklist_scans_tool_result_text_but_not_base64() {
        let mut msg = ChatMsg::text("user", String::new());
        msg.parts = Some(serde_json::json!([
            {"type":"tool_result","tool_use_id":"toolu_1","content":"leak forbiddenword here"},
        ]));
        let mut req = GatewayRequest {
            message: vec![msg],
            ..Default::default()
        };
        assert!(
            security_check(&sec(), &mut req).block.is_some(),
            "a blocklisted term inside a tool_result block must be caught"
        );

        // a base64 image whose noise happens to contain the term must NOT block
        let mut clean = ChatMsg::text("user", "look");
        let noise = format!("AAAA{}BBBB", "forbiddenword");
        clean.parts = Some(serde_json::json!([
            {"type":"text","text":"look at this"},
            {"type":"image","source":{"type":"base64","media_type":"image/png","data": noise}},
            {"type":"image_url","image_url":{"url": format!("data:image/png;base64,{noise}")}},
        ]));
        let mut req = GatewayRequest {
            message: vec![clean],
            ..Default::default()
        };
        assert!(
            security_check(&sec(), &mut req).block.is_none(),
            "base64 image data must not be scanned for blocklist terms"
        );
    }

    #[test]
    fn dlp_redacts_tool_result_content() {
        let mut msg = ChatMsg::text("user", String::new());
        msg.parts = Some(serde_json::json!([
            {"type":"tool_result","tool_use_id":"toolu_2",
             "content":[{"type":"text","text":"reach me at bob@corp.com"}]},
        ]));
        let mut req = GatewayRequest {
            message: vec![msg],
            ..Default::default()
        };
        assert!(dlp_redact_request(&sec(), &mut req) >= 1);
        let parts = req.message[0].parts.as_ref().unwrap();
        assert!(
            !parts.to_string().contains("bob@corp.com"),
            "PII inside a tool_result block must be redacted: {parts}"
        );
    }
}
