//! Request/response plugins, rule-based from config.security. The pre-stage
//! runs before the DAG: a blocklist hit → Block (skipping engine and billing);
//! DLP redacts inbound messages, the Responses native body, and the family
//! typed params. The post-stage redacts the outbound message; streaming
//! surfaces buffer and replay the redacted text when outbound DLP is on.

use gw_config::{Action, SecurityConf};
use gw_models::{Block, ChatMsg, GatewayRequest, GatewayResponse, ModelParamV2};

const BLOCKED_MSG: &str = "this content cannot be answered, please try a different request";
const MAX_NATIVE_EVENT_SCAN_BYTES: usize = 4 * 1024 * 1024;
const MAX_NATIVE_EVENT_SCAN_FIELDS: usize = 4096;

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
/// traversal of every string leaf, signed-thinking opaque fields included, so
/// no field choice is a bypass. `&mut` only to share the traversal; nothing
/// is rewritten here.
pub fn security_check(sec: &SecurityConf, request: &mut GatewayRequest) -> ScanOutcome {
    if sec.blocklist.is_empty() && sec.regexes.is_empty() {
        return ScanOutcome::default();
    }
    let mut counts = ScanCounts::new(sec);
    for_each_request_text(request, SignedThinking::Visit, &mut |s, _| counts.visit(s));
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

/// The one text view moderation reviews and content retention records —
/// signed-thinking prose included (policy must read what the vendor said),
/// opaque signatures and redacted-thinking data excluded (unscannable).
/// `&mut` only to share the traversal; nothing is rewritten.
pub fn inbound_text(request: &mut GatewayRequest) -> String {
    let mut out = String::new();
    for_each_request_text(request, SignedThinking::Prose, &mut |s, _| {
        push_text(&mut out, s);
        0
    });
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

/// Apply moderator mask spans (byte ranges into [`inbound_text`]) back onto
/// the request's text slots. Signed thinking cannot be rewritten without
/// invalidating the continuation, so a probing pass first counts spans landing
/// in protected prose; on `Err` nothing has been mutated and the caller
/// denies. Returns the number of replaced ranges.
pub fn apply_moderation_mask(
    request: &mut GatewayRequest,
    spans: &[std::ops::Range<usize>],
) -> Result<usize, usize> {
    let mut dry = SpanMasker::new(spans);
    let mut protected = 0;
    for_each_request_text(request, SignedThinking::Prose, &mut |text, prose| {
        let overlaps = dry.probe(text);
        if prose {
            protected += overlaps;
        }
        0
    });
    if protected > 0 {
        return Err(protected);
    }
    let mut masker = SpanMasker::new(spans);
    for_each_request_text(request, SignedThinking::Prose, &mut |text, prose| {
        // protected prose still advances the offset cursor; the probing pass
        // proved no span lands in it
        if prose {
            masker.probe(text)
        } else {
            masker.apply(text)
        }
    });
    Ok(masker.hits)
}

/// Moderator mask spans applied to one realtime frame (spans address the
/// frame's collected text).
pub fn apply_mask_spans_frame(
    frame: &mut serde_json::Value,
    spans: &[std::ops::Range<usize>],
) -> usize {
    let mut masker = SpanMasker::new(spans);
    gw_engines::realtime::visit_frame_text(frame, &mut |s| masker.apply(s))
}

/// Replays the `push_text` walk (empty slots skipped, '\n' joiners) to map
/// global byte spans back onto individual slots. Spans are merged up front and
/// snapped to char boundaries so a sloppy external service can't split UTF-8.
struct SpanMasker {
    spans: Vec<std::ops::Range<usize>>,
    base: usize,
    seen_any: bool,
    hits: usize,
}

impl SpanMasker {
    fn new(spans: &[std::ops::Range<usize>]) -> Self {
        let mut spans: Vec<_> = spans.iter().filter(|r| r.start < r.end).cloned().collect();
        spans.sort_by_key(|r| r.start);
        spans.dedup_by(|next, prev| {
            if next.start <= prev.end {
                prev.end = prev.end.max(next.end);
                true
            } else {
                false
            }
        });
        Self {
            spans,
            base: 0,
            seen_any: false,
            hits: 0,
        }
    }

    /// Advance the offset cursor over one slot, mirroring the `push_text`
    /// walk; `None` for the empty slots that walk skips.
    fn advance(&mut self, len: usize) -> Option<(usize, usize)> {
        if len == 0 {
            return None;
        }
        if self.seen_any {
            self.base += 1;
        }
        self.seen_any = true;
        let start = self.base;
        self.base += len;
        Some((start, self.base))
    }

    /// Count spans overlapping this slot without rewriting it.
    fn probe(&mut self, s: &str) -> usize {
        let Some((start, end)) = self.advance(s.len()) else {
            return 0;
        };
        self.spans
            .iter()
            .filter(|span| span.start < end && span.end > start)
            .count()
    }

    fn apply(&mut self, s: &mut String) -> usize {
        let Some((start, end)) = self.advance(s.len()) else {
            return 0;
        };
        let mut replaced = 0;
        for span in self.spans.iter().rev() {
            if span.start >= end || span.end <= start {
                continue;
            }
            let lo = snap_floor(s, span.start.saturating_sub(start));
            let hi = snap_ceil(s, span.end.saturating_sub(start).min(s.len()));
            s.replace_range(lo..hi, "[MASKED]");
            replaced += 1;
        }
        self.hits += replaced;
        replaced
    }
}

fn snap_floor(s: &str, mut i: usize) -> usize {
    i = i.min(s.len());
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

fn snap_ceil(s: &str, mut i: usize) -> usize {
    i = i.min(s.len());
    while i < s.len() && !s.is_char_boundary(i) {
        i += 1;
    }
    i
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

/// How a traversal treats the signed thinking blocks of a native assistant
/// turn. Everywhere else such blocks are ordinary client data and every
/// policy degrades to `Visit`.
#[derive(Clone, Copy, PartialEq)]
enum SignedThinking {
    /// Every string leaf, opaque proof included — the security scans; a
    /// field choice must never dodge the scanner.
    Visit,
    /// Skip the whole block — a rewrite would invalidate the signature.
    Skip,
    /// Prose without the opaque signature/data — the moderation/retention
    /// view and the mask walk, whose offsets must match that view.
    Prose,
}

/// Every text-bearing field of a whole request — each message plus the tail
/// params — the ONE per-request traversal the blocklist scan, inbound-text
/// view, mask application, and DLP redaction all share, so they can't drift
/// apart field by field. The visitor's second argument marks signed-thinking
/// prose (only ever true under [`SignedThinking::Prose`]).
fn for_each_request_text(
    request: &mut GatewayRequest,
    signed: SignedThinking,
    f: &mut impl FnMut(&mut String, bool) -> usize,
) -> usize {
    let mut n = 0;
    let native = request.preserve_anthropic_wire;
    for msg in &mut request.message {
        let policy = if native && msg.role == gw_consts::role::AI {
            signed
        } else {
            SignedThinking::Visit
        };
        n += for_each_message_text(msg, policy, f);
    }
    if let Some(param) = request.model_param_v2.as_mut() {
        n += for_each_param_text(param, &mut |s| f(s, false));
    }
    n
}

/// The text-bearing fields of one chat turn — flat content, multimodal parts,
/// and assistant tool_calls, all forwarded to the vendor by the engines — the
/// ONE per-message field list every scan and rewrite traverses, so a
/// `ChatMsg` field added here is covered by all of them.
fn for_each_message_text(
    msg: &mut ChatMsg,
    signed: SignedThinking,
    f: &mut impl FnMut(&mut String, bool) -> usize,
) -> usize {
    let mut n = f(&mut msg.content, false);
    if let Some(parts) = &mut msg.parts {
        n += walk_part_text(parts, signed, f);
    }
    if let Some(tc) = &mut msg.tool_calls {
        n += walk_json_strings(tc, &mut |s| f(s, false));
    }
    n
}

/// The text-bearing fields of the request tail — the raw native body plus the
/// family typed params — the ONE tail field list all three scans traverse.
/// Responses `raw` is the whole wire body: content blocks (media-aware walk,
/// base64 kept out of the scan) mixed with arbitrary bags (`tools`, `metadata`)
/// the same walk scans in full. Chat/Messages put only a flat vendor `extra`
/// passthrough there, so every string leaf is visited.
fn for_each_param_text(
    param: &mut ModelParamV2,
    f: &mut impl FnMut(&mut String) -> usize,
) -> usize {
    let mut n = if matches!(param.protocol, gw_consts::Protocol::Responses) {
        walk_part_text(&mut param.raw, SignedThinking::Visit, &mut |s, _| f(s))
    } else {
        walk_json_strings(&mut param.raw, f)
    };
    if let Some(typed) = param.typed.as_mut() {
        n += for_each_typed_text(typed, f);
    }
    n
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
        T::Moderation(p) => p.input.iter_mut().map(&mut *f).sum(),
        T::Rerank(p) => f(&mut p.query) + p.documents.iter_mut().map(&mut *f).sum::<usize>(),
        T::AudioStt(_) => 0,
    }
}

/// Whether a `type` value names a media content block — one whose payload keys
/// carry base64/URL data rather than prose (`{"type":"image", ...}`,
/// `input_image`, `input_file`, …). Only inside such a block does
/// [`walk_part_text`] skip the media payload keys.
fn is_media_block(ty: &str) -> bool {
    matches!(
        ty,
        "image"
            | "image_url"
            | "input_image"
            | "audio"
            | "input_audio"
            | "input_file"
            | "file"
            | "document"
    )
}

/// Claude signs the complete thinking block, not just the `signature` field.
/// Rewriting its prose or opaque data invalidates a tool-use continuation.
fn is_signed_thinking_block(block: &serde_json::Map<String, serde_json::Value>) -> bool {
    match block.get("type").and_then(serde_json::Value::as_str) {
        Some("thinking") => block
            .get("signature")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|signature| !signature.is_empty()),
        Some("redacted_thinking") => block
            .get("data")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|data| !data.is_empty()),
        _ => false,
    }
}

/// A media block's payload keys: base64/URL leaves a rewrite would corrupt or
/// noise could false-match, the nested media containers (`image_url`,
/// `input_audio`, Chat's `file`), and file-handle references (`file_id`).
/// Skipped only inside a [`is_media_block`] object; the same name in a tool
/// argument, a JSON Schema, or a metadata bag is prose and stays scanned.
fn is_media_payload_key(k: &str) -> bool {
    matches!(
        k,
        "image_url"
            | "input_audio"
            | "source"
            | "data"
            | "url"
            | "file"
            | "file_id"
            | "file_data"
            | "file_url"
    )
}

/// Walk a content array with a visitor that may rewrite prose. Only direct
/// blocks of the array honor the signed-thinking policy; a lookalike nested
/// in a tool argument or metadata remains ordinary client data.
fn walk_part_text(
    v: &mut serde_json::Value,
    signed: SignedThinking,
    f: &mut impl FnMut(&mut String, bool) -> usize,
) -> usize {
    if let serde_json::Value::Array(parts) = v {
        return parts
            .iter_mut()
            .map(|part| {
                if signed != SignedThinking::Visit
                    && part.as_object().is_some_and(is_signed_thinking_block)
                {
                    if signed == SignedThinking::Skip {
                        0
                    } else {
                        walk_signed_prose(part, f)
                    }
                } else {
                    walk_part_value(part, &mut |s| f(s, false))
                }
            })
            .sum();
    }
    walk_part_value(v, &mut |s| f(s, false))
}

/// A signed block's prose fields — everything but the opaque
/// signature/redacted data, which no scanner can read and no mask may touch.
fn walk_signed_prose(
    v: &mut serde_json::Value,
    f: &mut impl FnMut(&mut String, bool) -> usize,
) -> usize {
    let opaque_key = match v.get("type").and_then(serde_json::Value::as_str) {
        Some("redacted_thinking") => "data",
        _ => "signature",
    };
    let Some(object) = v.as_object_mut() else {
        return 0;
    };
    object
        .iter_mut()
        .map(|(key, value)| {
            if key == opaque_key {
                0
            } else {
                walk_part_value(value, &mut |s| f(s, true))
            }
        })
        .sum()
}

fn walk_part_value(v: &mut serde_json::Value, f: &mut impl FnMut(&mut String) -> usize) -> usize {
    match v {
        serde_json::Value::String(s) => f(s),
        serde_json::Value::Array(a) => a.iter_mut().map(|x| walk_part_value(x, f)).sum(),
        serde_json::Value::Object(o) => {
            let media = o
                .get("type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(is_media_block);
            o.iter_mut()
                .map(|(key, value)| {
                    if media && is_media_payload_key(key) {
                        0
                    } else {
                        walk_part_value(value, f)
                    }
                })
                .sum()
        }
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

/// Sensitive text inside a signed thinking block cannot be rewritten without
/// invalidating it. Detect those cases — opaque signature/data included, so a
/// verbatim secret cannot ride an unscannable field to the vendor — and the
/// handler rejects the request instead of silently bypassing DLP. `&mut` only
/// to share the walk; nothing is rewritten.
pub fn protected_thinking_dlp_hits(sec: &SecurityConf, request: &mut GatewayRequest) -> usize {
    if (!sec.dlp_redact && !sec.detect_secrets) || !request.preserve_anthropic_wire {
        return 0;
    }
    let (pii, secrets) = (sec.dlp_redact, sec.detect_secrets);
    let mut hits = 0;
    for message in &mut request.message {
        if message.role != gw_consts::role::AI {
            continue;
        }
        let Some(parts) = message
            .parts
            .as_mut()
            .and_then(serde_json::Value::as_array_mut)
        else {
            continue;
        };
        for block in &mut *parts {
            if block.as_object().is_some_and(is_signed_thinking_block) {
                hits += walk_part_value(block, &mut |text| redaction_hits(text, pii, secrets));
            }
        }
    }
    hits
}

/// DLP inbound redaction: emails, 11-digit phone numbers, and — when
/// `detect_secrets` is on — API keys / credentials.
pub fn dlp_redact_request(sec: &SecurityConf, request: &mut GatewayRequest) -> usize {
    if !sec.dlp_redact && !sec.detect_secrets {
        return 0;
    }
    let (pii, secrets) = (sec.dlp_redact, sec.detect_secrets);
    for_each_request_text(request, SignedThinking::Skip, &mut |s, _| {
        redact_in_place(s, pii, secrets)
    })
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

/// [`redact_in_place`]'s hit count without the rewrite — the probe used where
/// redaction is impossible (signed thinking) or unwanted (buffered events).
fn redaction_hits(text: &str, pii: bool, secrets: bool) -> usize {
    let mut hits = 0;
    if pii && let Some((_, n)) = redact(text) {
        hits += n;
    }
    if secrets && let Some((_, n)) = redact_secrets(text) {
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

/// DLP probe of native Anthropic SSE events before buffered replay. Signature
/// deltas and encrypted redacted-thinking data are opaque; text, tool JSON,
/// citations, and forward-compatible delta payloads remain covered. `&mut`
/// only to share the walk family; nothing is rewritten.
pub fn anthropic_event_dlp_hits(
    sec: &SecurityConf,
    chunks: &mut [gw_models::StreamChunk],
) -> usize {
    if !sec.redacts_output() {
        return 0;
    }
    let (pii, secrets) = (sec.dlp_redact, sec.detect_secrets);
    let mut fragments = EventFragments::default();
    let mut scan = |text: &mut String| redaction_hits(text, pii, secrets);
    let mut hits: usize = chunks
        .iter_mut()
        .filter_map(|chunk| chunk.anthropic_event.as_mut())
        .map(|event| walk_anthropic_event(event, &mut fragments, &mut scan))
        .sum();
    if fragments.overflowed {
        return hits.max(1);
    }
    hits += fragments
        .values
        .values()
        .map(|text| redaction_hits(text, pii, secrets))
        .sum::<usize>();
    hits
}

#[derive(Default)]
struct EventFragments {
    values: std::collections::HashMap<(u64, String), String>,
    bytes: usize,
    overflowed: bool,
}

impl EventFragments {
    fn append(&mut self, index: u64, path: &str, text: &str) {
        if self.overflowed {
            return;
        }
        let key = (index, path.to_owned());
        let new_key = !self.values.contains_key(&key);
        let added = text
            .len()
            .saturating_add(if new_key { path.len() } else { 0 });
        if self.bytes.saturating_add(added) > MAX_NATIVE_EVENT_SCAN_BYTES
            || (new_key && self.values.len() >= MAX_NATIVE_EVENT_SCAN_FIELDS)
        {
            self.overflowed = true;
            self.values.clear();
            return;
        }
        self.bytes = self.bytes.saturating_add(added);
        self.values.entry(key).or_default().push_str(text);
    }
}

fn walk_anthropic_event(
    event: &mut serde_json::Value,
    fragments: &mut EventFragments,
    f: &mut impl FnMut(&mut String) -> usize,
) -> usize {
    if event["type"] == "message_start" {
        let mut hits = walk_object_excluding(event, &["message"], f);
        if let Some(message) = event.get_mut("message") {
            hits += walk_object_excluding(message, &["content"], f);
            if let Some(content) = message.get_mut("content") {
                hits += walk_part_text(content, SignedThinking::Prose, &mut |s, _| f(s));
            }
        }
        return hits;
    }
    if event["type"] == "content_block_start" {
        let mut hits = walk_object_excluding(event, &["content_block"], f);
        if let Some(block) = event.get_mut("content_block") {
            hits += if block.as_object().is_some_and(is_signed_thinking_block) {
                walk_signed_prose(block, &mut |s, _| f(s))
            } else {
                walk_part_value(block, f)
            };
        }
        return hits;
    }
    if event["type"] == "content_block_delta" && event.get("delta").is_some() {
        let opaque_key: Option<&'static str> = match event["delta"]["type"].as_str() {
            Some("signature_delta") => Some("signature"),
            Some("redacted_thinking_delta") => Some("data"),
            _ => None,
        };
        let index = event.get("index").and_then(serde_json::Value::as_u64);
        let mut hits = walk_object_excluding(event, &["delta"], f);
        if let Some(delta) = event.get_mut("delta") {
            match index {
                Some(index) => collect_delta_payload(delta, index, opaque_key, fragments),
                None => {
                    hits += match opaque_key {
                        Some(key) => walk_object_excluding(delta, &["type", key], f),
                        None => walk_object_excluding(delta, &["type"], f),
                    };
                }
            }
        }
        return hits;
    }
    walk_part_value(event, f)
}

fn walk_object_excluding(
    value: &mut serde_json::Value,
    excluded: &[&str],
    f: &mut impl FnMut(&mut String) -> usize,
) -> usize {
    let Some(object) = value.as_object_mut() else {
        return walk_part_value(value, f);
    };
    object
        .iter_mut()
        .filter(|(key, _)| !excluded.contains(&key.as_str()))
        .map(|(_, value)| walk_part_value(value, f))
        .sum()
}

fn collect_delta_payload(
    delta: &serde_json::Value,
    index: u64,
    opaque_key: Option<&str>,
    fragments: &mut EventFragments,
) {
    let Some(object) = delta.as_object() else {
        collect_delta_fragments(delta, index, "delta", fragments);
        return;
    };
    for (key, value) in object {
        if key != "type" && Some(key.as_str()) != opaque_key {
            collect_delta_fragments(value, index, &format!("delta/{key}"), fragments);
        }
        if fragments.overflowed {
            break;
        }
    }
}

fn collect_delta_fragments(
    value: &serde_json::Value,
    index: u64,
    path: &str,
    fragments: &mut EventFragments,
) {
    if fragments.overflowed {
        return;
    }
    match value {
        serde_json::Value::String(text) => {
            fragments.append(index, path, text);
        }
        serde_json::Value::Array(values) => {
            for (position, value) in values.iter().enumerate() {
                collect_delta_fragments(value, index, &format!("{path}/{position}"), fragments);
                if fragments.overflowed {
                    break;
                }
            }
        }
        serde_json::Value::Object(object) => {
            for (key, value) in object {
                collect_delta_fragments(value, index, &format!("{path}/{key}"), fragments);
                if fragments.overflowed {
                    break;
                }
            }
        }
        _ => {}
    }
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
    let native_hits = response
        .anthropic_content
        .as_mut()
        .map(|content| walk_part_text(content, SignedThinking::Skip, &mut |s, _| redact_field(s)))
        .unwrap_or(0);
    // Native text normally duplicates normalized fields. Count a native-only
    // hit when normalized output was clean, without double-counting text.
    if hits == 0 && native_hits > 0 {
        hits = native_hits;
    }
    hits
}

/// Strip signed thinking the tenant's policy cannot serve — a block-action
/// rule hit (the same [`ScanCounts`] verdict a replay would meet inbound) or
/// a DLP hit nothing can redact without breaking the signature. The client
/// still gets the redacted normalized turn instead of a hard failure after
/// the upstream tokens are spent. Returns the hit count; the caller drops
/// the buffered raw events.
pub fn strip_unservable_thinking(sec: &SecurityConf, response: &mut GatewayResponse) -> usize {
    let dlp = sec.redacts_output();
    if !dlp && sec.blocklist.is_empty() && sec.regexes.is_empty() {
        return 0;
    }
    let Some(blocks) = response
        .anthropic_content
        .as_mut()
        .and_then(serde_json::Value::as_array_mut)
    else {
        return 0;
    };
    let (pii, secrets) = (sec.dlp_redact, sec.detect_secrets);
    let mut counts = ScanCounts::new(sec);
    let mut hits = 0;
    for block in &mut *blocks {
        if !block.as_object().is_some_and(is_signed_thinking_block) {
            continue;
        }
        walk_signed_prose(block, &mut |text, _| {
            counts.visit(text);
            if dlp {
                hits += redaction_hits(text, pii, secrets);
            }
            0
        });
    }
    let rules = counts.outcome();
    if rules.block.is_some() {
        hits += rules
            .hits
            .iter()
            .filter(|h| h.action == Action::Block)
            .map(|h| h.count as usize)
            .sum::<usize>();
    }
    if hits > 0 {
        blocks.retain(|block| !block.as_object().is_some_and(is_signed_thinking_block));
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

    #[test]
    fn mask_spans_map_across_slots_like_inbound_text() {
        let mut req = GatewayRequest {
            message: vec![
                ChatMsg::text("user", "first part"),
                ChatMsg::text("user", "the secret word"),
            ],
            ..Default::default()
        };
        let joined = inbound_text(&mut req.clone());
        let i = joined.find("secret").unwrap();
        let span = i..i + "secret".len();
        let hits = apply_moderation_mask(&mut req, std::slice::from_ref(&span)).unwrap();
        assert_eq!(hits, 1);
        assert_eq!(req.message[0].content, "first part", "untouched slot");
        assert_eq!(req.message[1].content, "the [MASKED] word");
    }

    #[test]
    fn mask_spans_snap_to_char_boundaries_and_merge() {
        let mut req = GatewayRequest {
            message: vec![ChatMsg::text("user", "码字abc")],
            ..Default::default()
        };
        let hits = apply_moderation_mask(&mut req, &[2..4, 4..7]).unwrap();
        assert_eq!(hits, 1, "overlapping ranges merge to one replacement");
        assert!(req.message[0].content.contains("[MASKED]"));
    }

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
        assert!(block.block);
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
            anthropic_content: Some(serde_json::json!([
                {"type":"thinking","thinking":"signed content","signature":"opaque-signature"},
                {"type":"text","text":"your key is sk-abcdefghijklmnopqrstuvwxyz012345"}
            ])),
            ..Default::default()
        };
        assert_eq!(dlp_redact_response(&s, &mut resp), 1);
        assert!(
            resp.message.contains("[REDACTED_SECRET]") && !resp.message.contains("sk-abc"),
            "{}",
            resp.message
        );
        let native = resp.anthropic_content.as_ref().unwrap();
        assert_eq!(native[0]["signature"], "opaque-signature");
        assert!(
            native[1]["text"]
                .as_str()
                .unwrap()
                .contains("[REDACTED_SECRET]")
        );

        let native = serde_json::json!([
            {"type":"thinking","thinking":"clean","signature":"opaque-signature"},
            {"type":"text","text":"clean answer"}
        ]);
        let mut clean = GatewayResponse {
            message: "clean answer".to_owned(),
            anthropic_content: Some(native.clone()),
            ..Default::default()
        };
        assert_eq!(dlp_redact_response(&s, &mut clean), 0);
        assert_eq!(clean.anthropic_content, Some(native));
    }

    #[test]
    fn unservable_thinking_is_stripped_and_the_turn_still_serves() {
        let s = SecurityConf {
            detect_secrets: true,
            dlp_redact: false,
            ..Default::default()
        };
        let native = serde_json::json!([
            {
                "type":"thinking",
                "thinking":"credential sk-abcdefghijklmnopqrstuvwxyz012345",
                "signature":"opaque-signature-must-not-be-rewritten"
            },
            {
                "type":"redacted_thinking",
                "data":"sk-opaque-data-not-scanned-outbound",
                "extra":"sk-abcdefghijklmnopqrstuvwxyz012345"
            },
            {"type":"text","text":"clean answer"}
        ]);
        let mut response = GatewayResponse {
            message: "clean answer".to_owned(),
            anthropic_content: Some(native),
            ..Default::default()
        };

        assert_eq!(strip_unservable_thinking(&s, &mut response), 2);
        assert_eq!(
            response.anthropic_content,
            Some(serde_json::json!([{"type":"text","text":"clean answer"}])),
            "protected blocks are dropped, the visible turn survives"
        );
        assert_eq!(dlp_redact_response(&s, &mut response), 0);
        assert_eq!(response.message, "clean answer");
    }

    #[test]
    fn blocklisted_thinking_prose_is_stripped_before_serving() {
        let mut response = GatewayResponse {
            message: "clean answer".to_owned(),
            anthropic_content: Some(serde_json::json!([
                {"type":"thinking","thinking":"mentions ForbiddenWord","signature":"sig"},
                {"type":"text","text":"clean answer"}
            ])),
            ..Default::default()
        };
        assert_eq!(strip_unservable_thinking(&sec(), &mut response), 1);
        assert_eq!(
            response.anthropic_content,
            Some(serde_json::json!([{"type":"text","text":"clean answer"}]))
        );

        let mut clean = GatewayResponse {
            anthropic_content: Some(serde_json::json!([
                {"type":"thinking","thinking":"harmless","signature":"sig"}
            ])),
            ..Default::default()
        };
        assert_eq!(strip_unservable_thinking(&sec(), &mut clean), 0);
        assert!(clean.anthropic_content.unwrap().as_array().unwrap().len() == 1);
    }

    #[test]
    fn native_event_dlp_joins_split_deltas_and_skips_signatures() {
        let security = SecurityConf {
            detect_secrets: true,
            dlp_redact: false,
            ..Default::default()
        };
        let event = |value| gw_models::StreamChunk {
            anthropic_event: Some(value),
            ..Default::default()
        };
        let mut chunks = vec![
            event(serde_json::json!({
                "type":"content_block_delta","index":1,
                "delta":{"type":"input_json_delta","partial_json":"{not-json:sk-abcdefghij"}
            })),
            event(serde_json::json!({
                "type":"content_block_delta","index":1,
                "delta":{"type":"input_json_delta","partial_json":"klmnopqrstuvwxyz012345"}
            })),
        ];
        assert!(anthropic_event_dlp_hits(&security, &mut chunks) >= 1);

        let mut opaque = vec![event(serde_json::json!({
            "type":"content_block_delta","index":0,
            "delta":{"type":"signature_delta","signature":"sk-abcdefghijklmnopqrstuvwxyz012345"}
        }))];
        assert_eq!(anthropic_event_dlp_hits(&security, &mut opaque), 0);

        let mut opaque_with_extra = vec![event(serde_json::json!({
            "type":"content_block_delta","index":0,
            "delta":{
                "type":"signature_delta",
                "signature":"opaque-signature",
                "extra":"sk-abcdefghijklmnopqrstuvwxyz012345"
            }
        }))];
        assert_eq!(
            anthropic_event_dlp_hits(&security, &mut opaque_with_extra),
            1,
            "only the signature field itself is opaque"
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
    fn signed_thinking_is_read_only_scanned_and_never_rewritten() {
        let signed = serde_json::json!([
            {
                "type":"thinking",
                "thinking":"forbiddenword jane@corp.com",
                "signature":"sk-abcdefghijklmnopqrstuvwxyz012345"
            },
            {"type":"redacted_thinking","data":"forbiddenword jane@corp.com","extra":"ops@corp.com"},
            {"type":"text","text":"ordinary jane@corp.com"}
        ]);
        let mut message = ChatMsg::text("assistant", String::new());
        message.parts = Some(signed.clone());
        let mut request = GatewayRequest {
            message: vec![message],
            preserve_anthropic_wire: true,
            ..Default::default()
        };

        assert!(
            security_check(&sec(), &mut request).block.is_some(),
            "plaintext thinking remains subject to blocklist policy"
        );
        assert_eq!(
            protected_thinking_dlp_hits(&sec(), &mut request),
            3,
            "prose + extra + opaque data emails all counted"
        );
        let hits = dlp_redact_request(&sec(), &mut request);
        let parts = request.message[0].parts.as_ref().unwrap();
        assert_eq!(&parts[0], &signed[0]);
        assert_eq!(&parts[1], &signed[1]);
        assert!(hits >= 1, "ordinary text remains subject to DLP");
        assert!(
            parts[2]["text"]
                .as_str()
                .unwrap()
                .contains("[REDACTED_EMAIL]")
        );

        let mut unsigned = ChatMsg::text("assistant", String::new());
        unsigned.parts = Some(serde_json::json!([
            {"type":"thinking","thinking":"forbiddenword","signature":""}
        ]));
        let mut unsigned_request = GatewayRequest {
            message: vec![unsigned],
            ..Default::default()
        };
        assert!(
            security_check(&sec(), &mut unsigned_request)
                .block
                .is_some(),
            "an unsigned lookalike must not become a DLP bypass"
        );
    }

    #[test]
    fn opaque_fields_are_no_smuggling_channel_inbound() {
        let mut message = ChatMsg::text("assistant", String::new());
        message.parts = Some(serde_json::json!([
            {"type":"redacted_thinking","data":"contains forbiddenword"}
        ]));
        let mut request = GatewayRequest {
            message: vec![message],
            preserve_anthropic_wire: true,
            ..Default::default()
        };
        assert!(
            security_check(&sec(), &mut request).block.is_some(),
            "a blocklisted term inside opaque data must still deny"
        );

        let s = SecurityConf {
            detect_secrets: true,
            dlp_redact: false,
            ..Default::default()
        };
        let mut message = ChatMsg::text("assistant", String::new());
        message.parts = Some(serde_json::json!([
            {"type":"thinking","thinking":"clean","signature":"AKIAABCDEFGHIJKLMNOP"}
        ]));
        let mut request = GatewayRequest {
            message: vec![message],
            preserve_anthropic_wire: true,
            ..Default::default()
        };
        assert_eq!(
            protected_thinking_dlp_hits(&s, &mut request),
            1,
            "a verbatim credential in the signature field is caught"
        );
    }

    #[test]
    fn nested_thinking_lookalike_is_not_treated_as_protected() {
        let mut message = ChatMsg::text("assistant", String::new());
        message.parts = Some(serde_json::json!([{
            "type":"tool_use",
            "id":"tool-1",
            "name":"probe",
            "input":{
                "nested":{
                    "type":"thinking",
                    "thinking":"forbiddenword jane@corp.com",
                    "signature":"fake"
                }
            }
        }]));
        let mut request = GatewayRequest {
            message: vec![message],
            preserve_anthropic_wire: true,
            ..Default::default()
        };

        assert!(security_check(&sec(), &mut request).block.is_some());
        assert_eq!(protected_thinking_dlp_hits(&sec(), &mut request), 0);
        assert!(dlp_redact_request(&sec(), &mut request) >= 1);
        let parts = request.message[0].parts.as_ref().unwrap();
        assert!(!parts.to_string().contains("jane@corp.com"));
    }

    #[test]
    fn moderation_reads_signed_thinking_but_refuses_to_mask_it() {
        let mut message = ChatMsg::text("assistant", String::new());
        message.parts = Some(serde_json::json!([{
            "type":"thinking",
            "thinking":"sensitive thought",
            "signature":"opaque"
        }]));
        let mut request = GatewayRequest {
            message: vec![message],
            preserve_anthropic_wire: true,
            ..Default::default()
        };
        let reviewed = inbound_text(&mut request);
        let start = reviewed.find("sensitive").unwrap();
        let span = start..start + "sensitive".len();
        let original = request.message[0].parts.clone();

        assert_eq!(
            apply_moderation_mask(&mut request, std::slice::from_ref(&span)),
            Err(1)
        );
        assert_eq!(request.message[0].parts, original);
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
    fn responses_raw_skips_media_but_scans_text() {
        let noise = format!("data:image/png;base64,AAAA{}BBBB", "forbiddenword");
        let mut param = gw_models::ModelParamV2::with_name(gw_consts::Protocol::Responses, "m");
        param.raw = serde_json::json!({"model":"m","input":[{"role":"user","content":[
            {"type":"input_image","image_url": noise},
            {"type":"input_text","text":"hello"}]}]});
        let mut req = GatewayRequest {
            model_param_v2: Some(param),
            ..Default::default()
        };
        assert!(
            security_check(&sec(), &mut req).block.is_none(),
            "base64 image_url in the raw body must not match the blocklist"
        );

        let mut param = gw_models::ModelParamV2::with_name(gw_consts::Protocol::Responses, "m");
        param.raw = serde_json::json!({"input":[{"role":"user","content":[
            {"type":"input_text","text":"say forbiddenword"}]}]});
        let mut req = GatewayRequest {
            model_param_v2: Some(param),
            ..Default::default()
        };
        assert!(
            security_check(&sec(), &mut req).block.is_some(),
            "raw input_text is still scanned"
        );
    }

    #[test]
    fn dlp_leaves_raw_file_data_intact() {
        let s = SecurityConf {
            detect_secrets: true,
            dlp_redact: false,
            ..Default::default()
        };
        let blob = "sk-abcdefghijklmnopqrstuvwxyz012345";
        let mut param = gw_models::ModelParamV2::with_name(gw_consts::Protocol::Responses, "m");
        param.raw = serde_json::json!({"input":[{"role":"user","content":[
            {"type":"input_file","filename":"a.pdf","file_data": blob}]}]});
        let mut req = GatewayRequest {
            model_param_v2: Some(param),
            ..Default::default()
        };
        dlp_redact_request(&s, &mut req);
        assert!(
            req.model_param_v2.unwrap().raw.to_string().contains(blob),
            "secret-shaped base64 noise inside file_data must not be rewritten"
        );
    }

    #[test]
    fn media_file_handles_and_nested_containers_are_skipped() {
        let mut img = gw_models::ModelParamV2::with_name(gw_consts::Protocol::Responses, "m");
        img.raw = serde_json::json!({"input":[{"role":"user","content":[
            {"type":"input_image","file_id":"file-forbiddenword"}]}]});
        let mut file = gw_models::ModelParamV2::with_name(gw_consts::Protocol::Responses, "m");
        file.raw = serde_json::json!({"input":[{"role":"user","content":[
            {"type":"input_file","file_id":"file-forbiddenword"}]}]});
        for param in [img, file] {
            let mut req = GatewayRequest {
                model_param_v2: Some(param),
                ..Default::default()
            };
            assert!(
                security_check(&sec(), &mut req).block.is_none(),
                "a file_id handle is a reference, not content"
            );
        }

        let mut msg = ChatMsg::text("user", String::new());
        msg.parts = Some(serde_json::json!([
            {"type":"file","file":{"filename":"a.pdf","file_data":"JVBERforbiddenword"}}]));
        let mut req = GatewayRequest {
            message: vec![msg],
            ..Default::default()
        };
        assert!(
            security_check(&sec(), &mut req).block.is_none(),
            "base64 inside a Chat `file` container must not block"
        );

        let mut msg = ChatMsg::text("user", String::new());
        msg.parts = Some(serde_json::json!([
            {"type":"tool_use","id":"t","name":"q","input":{"file_id":"forbiddenword"}}]));
        let mut req = GatewayRequest {
            message: vec![msg],
            ..Default::default()
        };
        assert!(
            security_check(&sec(), &mut req).block.is_some(),
            "file_id in a tool argument is not a media handle"
        );
    }

    #[test]
    fn responses_raw_scans_tool_schema_and_metadata_not_just_media() {
        for raw in [
            serde_json::json!({"model":"m","tools":[{"type":"function","function":{
                "name":"q","parameters":{"type":"object","properties":{
                    "source":{"type":"string","description":"say forbiddenword"}}}}}]}),
            serde_json::json!({"model":"m","metadata":{"user_id":"forbiddenword"}}),
        ] {
            let mut param = gw_models::ModelParamV2::with_name(gw_consts::Protocol::Responses, "m");
            param.raw = raw;
            let mut req = GatewayRequest {
                model_param_v2: Some(param),
                ..Default::default()
            };
            assert!(
                security_check(&sec(), &mut req).block.is_some(),
                "prose under a media-like key outside a media block must be scanned"
            );
        }
    }

    #[test]
    fn flat_extra_bag_is_fully_scanned_not_content_block_skipped() {
        for proto in [
            gw_consts::Protocol::OpenaiChat,
            gw_consts::Protocol::AnthropicMessages,
        ] {
            let mut param = gw_models::ModelParamV2::with_name(proto, "m");
            param.raw = serde_json::json!({
                "type":"say forbiddenword",
                "metadata":{"user_id":"forbiddenword"}
            });
            let mut req = GatewayRequest {
                model_param_v2: Some(param),
                ..Default::default()
            };
            assert!(
                security_check(&sec(), &mut req).block.is_some(),
                "flat extra prose under media-like keys must be scanned ({proto:?})"
            );
        }
    }

    #[test]
    fn blocklist_and_dlp_cover_tool_call_arguments() {
        let mut msg = ChatMsg::text("assistant", String::new());
        msg.tool_calls = Some(serde_json::json!([{
            "id":"call_1","type":"function",
            "function":{"name":"send_mail","arguments":"{\"body\":\"say forbiddenword\"}"}
        }]));
        let mut req = GatewayRequest {
            message: vec![msg],
            ..Default::default()
        };
        assert!(
            security_check(&sec(), &mut req).block.is_some(),
            "a blocklisted term inside tool_calls arguments must be caught"
        );
        assert!(
            inbound_text(&mut req).contains("forbiddenword"),
            "moderation view must include tool_calls arguments"
        );

        let mut msg = ChatMsg::text("assistant", String::new());
        msg.tool_calls = Some(serde_json::json!([{
            "id":"call_2","type":"function",
            "function":{"name":"send_mail","arguments":"{\"to\":\"jane@corp.com\"}"}
        }]));
        let mut req = GatewayRequest {
            message: vec![msg],
            ..Default::default()
        };
        assert!(dlp_redact_request(&sec(), &mut req) >= 1);
        let tc = req.message[0].tool_calls.as_ref().unwrap();
        assert!(
            !tc.to_string().contains("jane@corp.com"),
            "PII inside tool_calls arguments must be redacted: {tc}"
        );
    }

    #[test]
    fn tool_use_input_generic_keys_are_scanned() {
        let mut msg = ChatMsg::text("user", String::new());
        msg.parts = Some(serde_json::json!([
            {"type":"tool_use","id":"toolu_1","name":"fetch",
             "input":{"url":"https://x.test/forbiddenword","data":"reach ops@example.com"}}
        ]));
        let mut req = GatewayRequest {
            message: vec![msg.clone()],
            ..Default::default()
        };
        assert!(
            security_check(&sec(), &mut req).block.is_some(),
            "blocklist must reach tool_use input values named url/data"
        );
        let mut req = GatewayRequest {
            message: vec![msg],
            ..Default::default()
        };
        assert!(dlp_redact_request(&sec(), &mut req) >= 1);
        let parts = req.message[0].parts.as_ref().unwrap();
        assert!(
            !parts.to_string().contains("ops@example.com"),
            "PII inside tool_use input must be redacted: {parts}"
        );
    }

    #[test]
    fn tool_input_source_is_scanned() {
        for input in [
            serde_json::json!({"source":"cite forbiddenword"}),
            serde_json::json!({"source":{"doc":"d","text":"cite forbiddenword"}}),
        ] {
            let mut msg = ChatMsg::text("user", String::new());
            msg.parts = Some(serde_json::json!([
                {"type":"tool_use","id":"toolu_2","name":"quote","input":input}
            ]));
            let mut req = GatewayRequest {
                message: vec![msg],
                ..Default::default()
            };
            assert!(
                security_check(&sec(), &mut req).block.is_some(),
                "a tool argument named source must be scanned whatever its shape"
            );
        }
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
