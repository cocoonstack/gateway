//! Short-lived, fail-open consistency checks for Anthropic thinking signatures.
//!
//! Anthropic does not expose the verification key to a gateway. This module
//! therefore does not claim to verify Claude's signature cryptographically. It
//! remembers what this gateway delivered for a tool-use turn and rejects only
//! when the same authenticated key, model, and tool id return with a different
//! protected thinking sequence. Unknown or expired anchors remain fail-open.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use gw_models::{ChatMsg, GatewayRequest};
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;
type Digest = [u8; 32];

pub const THINKING_SIGNATURE_TTL: Duration = Duration::from_secs(10 * 60);
const MAX_ENTRIES: usize = 250_000;
/// Compatible upstreams reuse tool ids ("call_1") across conversations; an
/// anchor keeps the last few fingerprints so a collision cannot invalidate another one.
const MAX_ANCHOR_FINGERPRINTS: usize = 4;
const MAX_STREAM_BLOCKS: usize = 1024;
const MAX_STREAM_CAPTURE: usize = 4 * 1024 * 1024;
const MAX_OPAQUE_FIELD: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewVerdict {
    NoEvidence,
    Match,
    Miss,
    Mismatch,
}

impl ReviewVerdict {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoEvidence => "no_evidence",
            Self::Match => "match",
            Self::Miss => "miss",
            Self::Mismatch => "mismatch",
        }
    }
}

/// The (user-scope, model) pair one audited request runs under.
#[derive(Debug, Clone)]
pub struct AuditContext {
    scope: Digest,
    model: String,
}

/// Remembers what signed thinking each scope was actually served, so a
/// tool-loop continuation can be audited against it.
#[derive(Clone)]
pub struct ThinkingSignatureAudit {
    inner: Arc<AuditInner>,
}

impl fmt::Debug for ThinkingSignatureAudit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ThinkingSignatureAudit")
            .field("ttl", &self.inner.ttl)
            .finish_non_exhaustive()
    }
}

impl Default for ThinkingSignatureAudit {
    fn default() -> Self {
        Self::new()
    }
}

impl ThinkingSignatureAudit {
    /// Audit with the default ten-minute consistency window.
    pub fn new() -> Self {
        Self::with_ttl(THINKING_SIGNATURE_TTL)
    }

    fn with_ttl(ttl: Duration) -> Self {
        Self::with_limits(ttl, MAX_ENTRIES)
    }

    fn with_limits(ttl: Duration, max_entries: usize) -> Self {
        let now = Instant::now();
        Self {
            inner: Arc::new(AuditInner {
                key: random_key(),
                ttl,
                max_entries,
                cache: Mutex::new(AuditCache {
                    entries: HashMap::new(),
                    next_sweep: now + ttl,
                }),
            }),
        }
    }

    /// Review the latest Anthropic tool-result continuation.
    ///
    /// The scope must come from authenticated gateway state. Invalid or
    /// incomplete request shapes intentionally produce a fail-open verdict.
    pub fn review_request(
        &self,
        request: &GatewayRequest,
        scope: &str,
    ) -> (Option<AuditContext>, ReviewVerdict) {
        let result = self.review_request_inner(request, scope);
        metrics::counter!(
            "gateway_thinking_signature_review_total",
            "result" => result.1.as_str(),
        )
        .increment(1);
        result
    }

    fn review_request_inner(
        &self,
        request: &GatewayRequest,
        scope: &str,
    ) -> (Option<AuditContext>, ReviewVerdict) {
        let Some(scope) = nonempty_bounded(scope, 4096) else {
            return (None, ReviewVerdict::NoEvidence);
        };
        let Some(model) = request
            .model_param_v2
            .as_ref()
            .map(|param| param.model_name.trim())
            .and_then(|model| nonempty_bounded(model, 256))
        else {
            return (None, ReviewVerdict::NoEvidence);
        };
        let context = AuditContext {
            scope: self.digest(b"gateway-thinking-scope-v1", &[scope.as_bytes()]),
            model: model.to_owned(),
        };
        let messages = &request.message;

        // Anthropic validates only the latest logical assistant turn (older thinking may be
        // omitted); consecutive same-role messages form one turn
        let mut trailing_user_start = messages.len();
        while trailing_user_start > 0
            && messages[trailing_user_start - 1].role == gw_consts::role::USER
        {
            trailing_user_start -= 1;
        }
        if trailing_user_start == messages.len() {
            return (Some(context), ReviewVerdict::NoEvidence);
        }
        let mut result_ids = HashSet::new();
        for message in &messages[trailing_user_start..] {
            result_ids.extend(tool_result_ids(message));
        }
        if result_ids.is_empty() {
            return (Some(context), ReviewVerdict::NoEvidence);
        }

        let mut sequence = ProtectedSequence::default();
        if let Some(assistant_end) = trailing_user_start
            .checked_sub(1)
            .filter(|index| messages[*index].role == gw_consts::role::AI)
        {
            let mut assistant_start = assistant_end;
            while assistant_start > 0 && messages[assistant_start - 1].role == gw_consts::role::AI {
                assistant_start -= 1;
            }
            for message in &messages[assistant_start..=assistant_end] {
                if let Some(Value::Array(content)) = message.parts.as_ref() {
                    sequence.append_content(content);
                }
            }
        }

        // thinking disabled: the client must strip the replayed blocks and the upstream
        // arbitrates; enabled: a known anchor missing its blocks is still a Mismatch
        if sequence.blocks.is_empty() && !request.reasoning_engaged() {
            return (Some(context), ReviewVerdict::Miss);
        }

        let fingerprint = self.sequence_fingerprint(&context, &sequence.blocks);
        let mut saw_match = false;
        for tool_id in result_ids {
            let anchor_present = sequence.tool_ids.contains(&tool_id);
            match self.review_anchor(&context, tool_id, anchor_present, fingerprint) {
                ReviewVerdict::Match => saw_match = true,
                ReviewVerdict::Mismatch => {
                    return (Some(context), ReviewVerdict::Mismatch);
                }
                ReviewVerdict::Miss | ReviewVerdict::NoEvidence => {}
            }
        }
        let verdict = if saw_match {
            ReviewVerdict::Match
        } else {
            ReviewVerdict::Miss
        };
        (Some(context), verdict)
    }

    /// Remember protected blocks from a complete successful JSON response.
    pub fn remember_content(&self, context: &AuditContext, content: &[Value]) {
        self.remember_sequence(context, &ProtectedSequence::from_content(content));
    }

    /// Capture native Anthropic events. Registration happens only after a
    /// complete `message_stop` and all protected blocks have stopped.
    pub fn stream_capture(&self, context: Option<AuditContext>) -> Option<ThinkingStreamCapture> {
        context.map(|context| ThinkingStreamCapture::new(self.clone(), context))
    }

    fn review_anchor(
        &self,
        context: &AuditContext,
        tool_id: &str,
        anchor_present: bool,
        actual: SequenceFingerprint,
    ) -> ReviewVerdict {
        let anchor = self.anchor_digest(context, tool_id);
        let now = Instant::now();
        let Ok(mut cache) = self.inner.cache.lock() else {
            metrics::counter!(
                "gateway_thinking_signature_cache_events_total",
                "event" => "lock_error",
            )
            .increment(1);
            return ReviewVerdict::Miss;
        };
        let Some(entry) = cache.entries.get(&anchor) else {
            return ReviewVerdict::Miss;
        };
        if entry.expires_at <= now {
            cache.entries.remove(&anchor);
            return ReviewVerdict::Miss;
        }
        let matched = anchor_present
            && entry.fingerprints.iter().any(|expected| {
                expected.opaque == actual.opaque
                    && expected
                        .strict
                        .is_none_or(|strict| actual.strict == Some(strict))
            });
        if matched {
            ReviewVerdict::Match
        } else {
            ReviewVerdict::Mismatch
        }
    }

    fn remember_sequence(&self, context: &AuditContext, sequence: &ProtectedSequence) {
        if sequence.invalid || sequence.tool_ids.is_empty() || !sequence.has_opaque_proof {
            return;
        }
        let fingerprint = self.sequence_fingerprint(context, &sequence.blocks);
        let now = Instant::now();
        let expires_at = now + self.inner.ttl;
        let anchors = sequence
            .tool_ids
            .iter()
            .map(|tool_id| self.anchor_digest(context, tool_id))
            .collect::<HashSet<_>>();
        if anchors.len() > self.inner.max_entries {
            metrics::counter!(
                "gateway_thinking_signature_cache_events_total",
                "event" => "capacity_drop",
            )
            .increment(1);
            return;
        }
        let Ok(mut cache) = self.inner.cache.lock() else {
            metrics::counter!(
                "gateway_thinking_signature_cache_events_total",
                "event" => "lock_error",
            )
            .increment(1);
            return;
        };
        let additional = |cache: &AuditCache| {
            anchors
                .iter()
                .filter(|anchor| !cache.entries.contains_key(*anchor))
                .count()
        };
        if cache.entries.len().saturating_add(additional(&cache)) > self.inner.max_entries
            && now >= cache.next_sweep
        {
            cache.entries.retain(|_, entry| entry.expires_at > now);
            cache.next_sweep = now + self.inner.ttl;
        }
        if cache.entries.len().saturating_add(additional(&cache)) > self.inner.max_entries {
            metrics::counter!(
                "gateway_thinking_signature_cache_events_total",
                "event" => "capacity_drop",
            )
            .increment(1);
            return;
        }
        for anchor in anchors {
            match cache.entries.get_mut(&anchor) {
                Some(entry) if entry.expires_at > now => {
                    if !entry.fingerprints.contains(&fingerprint) {
                        if entry.fingerprints.len() >= MAX_ANCHOR_FINGERPRINTS {
                            entry.fingerprints.remove(0);
                        }
                        entry.fingerprints.push(fingerprint);
                    }
                    entry.expires_at = expires_at;
                }
                _ => {
                    cache.entries.insert(
                        anchor,
                        CacheEntry {
                            fingerprints: vec![fingerprint],
                            expires_at,
                        },
                    );
                }
            }
        }
        metrics::counter!(
            "gateway_thinking_signature_cache_events_total",
            "event" => "remember",
        )
        .increment(sequence.tool_ids.len() as u64);
    }

    fn anchor_digest(&self, context: &AuditContext, tool_id: &str) -> Digest {
        self.digest(
            b"gateway-thinking-anchor-v1",
            &[&context.scope, context.model.as_bytes(), tool_id.as_bytes()],
        )
    }

    fn sequence_fingerprint(
        &self,
        context: &AuditContext,
        blocks: &[ProtectedBlock],
    ) -> SequenceFingerprint {
        let has_visible_summary = blocks.iter().any(|block| {
            matches!(block, ProtectedBlock::Thinking { thinking, .. } if !thinking.is_empty())
        });
        SequenceFingerprint {
            opaque: self.sequence_digest(context, blocks, false),
            // with display:"omitted" Anthropic ignores client text in an empty thinking field
            strict: has_visible_summary.then(|| self.sequence_digest(context, blocks, true)),
        }
    }

    fn sequence_digest(
        &self,
        context: &AuditContext,
        blocks: &[ProtectedBlock],
        include_visible_thinking: bool,
    ) -> Digest {
        let mut mac = new_mac(&self.inner.key);
        update_field(
            &mut mac,
            if include_visible_thinking {
                b"gateway-thinking-sequence-strict-v1"
            } else {
                b"gateway-thinking-sequence-opaque-v1"
            },
        );
        update_field(&mut mac, &context.scope);
        update_field(&mut mac, context.model.as_bytes());
        for block in blocks {
            match block {
                ProtectedBlock::Thinking {
                    thinking,
                    signature,
                } => {
                    update_field(&mut mac, b"thinking");
                    if include_visible_thinking {
                        update_field(&mut mac, thinking.as_bytes());
                    }
                    update_field(&mut mac, signature.as_bytes());
                }
                ProtectedBlock::RedactedThinking { data } => {
                    update_field(&mut mac, b"redacted_thinking");
                    update_field(&mut mac, data.as_bytes());
                }
            }
        }
        finalize_digest(mac)
    }

    fn digest(&self, domain: &[u8], fields: &[&[u8]]) -> Digest {
        let mut mac = new_mac(&self.inner.key);
        update_field(&mut mac, domain);
        for field in fields {
            update_field(&mut mac, field);
        }
        finalize_digest(mac)
    }
}

struct AuditInner {
    key: Digest,
    ttl: Duration,
    max_entries: usize,
    cache: Mutex<AuditCache>,
}

struct AuditCache {
    entries: HashMap<Digest, CacheEntry>,
    next_sweep: Instant,
}

struct CacheEntry {
    fingerprints: Vec<SequenceFingerprint>,
    expires_at: Instant,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct SequenceFingerprint {
    opaque: Digest,
    strict: Option<Digest>,
}

fn new_mac(key: &Digest) -> HmacSha256 {
    #[allow(clippy::expect_used)]
    HmacSha256::new_from_slice(key).expect("HMAC-SHA256 accepts any key length")
}

fn random_key() -> Digest {
    use chacha20poly1305::XChaCha20Poly1305;
    use chacha20poly1305::aead::{KeyInit, OsRng};

    let generated = XChaCha20Poly1305::generate_key(&mut OsRng);
    let mut key = [0; 32];
    key.copy_from_slice(&generated);
    key
}

fn nonempty_bounded(value: &str, max: usize) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty() && value.len() <= max).then_some(value)
}

fn update_field(mac: &mut HmacSha256, value: &[u8]) {
    mac.update(&(value.len() as u64).to_be_bytes());
    mac.update(value);
}

fn finalize_digest(mac: HmacSha256) -> Digest {
    let bytes = mac.finalize().into_bytes();
    let mut digest = [0; 32];
    digest.copy_from_slice(&bytes);
    digest
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProtectedBlock<'a> {
    Thinking {
        thinking: &'a str,
        signature: &'a str,
    },
    RedactedThinking {
        data: &'a str,
    },
}

#[derive(Debug, Default)]
struct ProtectedSequence<'a> {
    blocks: Vec<ProtectedBlock<'a>>,
    tool_ids: Vec<&'a str>,
    has_opaque_proof: bool,
    invalid: bool,
}

impl<'a> ProtectedSequence<'a> {
    fn from_content(content: &'a [Value]) -> Self {
        let mut sequence = Self::default();
        sequence.append_content(content);
        sequence
    }

    fn append_content(&mut self, content: &'a [Value]) {
        for block in content {
            match block.get("type").and_then(Value::as_str) {
                Some("thinking") => {
                    self.invalid |= invalid_bounded_string(block.get("thinking"))
                        || invalid_bounded_string(block.get("signature"));
                    let thinking = bounded_string(block.get("thinking"));
                    let signature = bounded_string(block.get("signature"));
                    self.has_opaque_proof |= !signature.is_empty();
                    self.blocks.push(ProtectedBlock::Thinking {
                        thinking,
                        signature,
                    });
                }
                Some("redacted_thinking") => {
                    self.invalid |= invalid_bounded_string(block.get("data"));
                    let data = bounded_string(block.get("data"));
                    self.has_opaque_proof |= !data.is_empty();
                    self.blocks.push(ProtectedBlock::RedactedThinking { data });
                }
                Some("tool_use") => {
                    self.invalid |= invalid_bounded_string(block.get("id"));
                    let id = bounded_string(block.get("id"));
                    if !id.is_empty() {
                        self.tool_ids.push(id);
                    }
                }
                _ => {}
            }
        }
    }
}

fn bounded_string(value: Option<&Value>) -> &str {
    value
        .and_then(Value::as_str)
        .filter(|value| value.len() <= MAX_OPAQUE_FIELD)
        .unwrap_or_default()
}

fn invalid_bounded_string(value: Option<&Value>) -> bool {
    match value {
        Some(Value::String(value)) => value.len() > MAX_OPAQUE_FIELD,
        Some(_) => true,
        None => false,
    }
}

fn tool_result_ids(message: &ChatMsg) -> impl Iterator<Item = &str> {
    message
        .parts
        .as_ref()
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_result"))
        .filter_map(|block| block.get("tool_use_id").and_then(Value::as_str))
}

#[derive(Debug)]
enum CapturedBlock {
    Thinking {
        thinking: String,
        signature: String,
        complete: bool,
    },
    RedactedThinking {
        data: String,
        complete: bool,
    },
    ToolUse {
        id: String,
        complete: bool,
    },
    Ignored,
}

impl CapturedBlock {
    fn mark_complete(&mut self) {
        match self {
            Self::Thinking { complete, .. }
            | Self::RedactedThinking { complete, .. }
            | Self::ToolUse { complete, .. } => *complete = true,
            Self::Ignored => {}
        }
    }

    fn complete(&self) -> bool {
        match self {
            Self::Thinking { complete, .. }
            | Self::RedactedThinking { complete, .. }
            | Self::ToolUse { complete, .. } => *complete,
            Self::Ignored => true,
        }
    }

    fn captured_len(&self) -> usize {
        match self {
            Self::Thinking {
                thinking,
                signature,
                ..
            } => thinking.len().saturating_add(signature.len()),
            Self::RedactedThinking { data, .. } => data.len(),
            Self::ToolUse { id, .. } => id.len(),
            Self::Ignored => 0,
        }
    }
}

/// Accumulates a live stream's signed thinking blocks so the finished turn
/// registers just like a buffered one.
pub struct ThinkingStreamCapture {
    audit: ThinkingSignatureAudit,
    context: AuditContext,
    disabled: bool,
    registered: bool,
    blocks: BTreeMap<u64, CapturedBlock>,
    captured_bytes: usize,
}

impl ThinkingStreamCapture {
    fn new(audit: ThinkingSignatureAudit, context: AuditContext) -> Self {
        Self {
            audit,
            context,
            disabled: false,
            registered: false,
            blocks: BTreeMap::new(),
            captured_bytes: 0,
        }
    }

    /// Fold one outbound SSE event into the capture.
    pub fn observe(&mut self, event: &Value) {
        if self.disabled || self.registered {
            return;
        }
        match event.get("type").and_then(Value::as_str) {
            Some("content_block_start") => self.start_block(event),
            Some("content_block_delta") => self.append_delta(event),
            Some("content_block_stop") => {
                if let Some(index) = event.get("index").and_then(Value::as_u64)
                    && let Some(block) = self.blocks.get_mut(&index)
                {
                    block.mark_complete();
                }
            }
            Some("message_stop") => self.register(),
            Some("error") => self.disable(),
            _ => {}
        }
    }

    fn start_block(&mut self, event: &Value) {
        let Some(index) = event.get("index").and_then(Value::as_u64) else {
            return;
        };
        let Some(block) = event.get("content_block") else {
            return;
        };
        let invalid = match block.get("type").and_then(Value::as_str) {
            Some("thinking") => {
                invalid_bounded_string(block.get("thinking"))
                    || invalid_bounded_string(block.get("signature"))
            }
            Some("redacted_thinking") => invalid_bounded_string(block.get("data")),
            Some("tool_use") => invalid_bounded_string(block.get("id")),
            _ => false,
        };
        if invalid {
            self.disable();
            return;
        }
        let captured = match block.get("type").and_then(Value::as_str) {
            Some("thinking") => CapturedBlock::Thinking {
                thinking: bounded_string(block.get("thinking")).to_owned(),
                signature: bounded_string(block.get("signature")).to_owned(),
                complete: false,
            },
            Some("redacted_thinking") => CapturedBlock::RedactedThinking {
                data: bounded_string(block.get("data")).to_owned(),
                complete: false,
            },
            Some("tool_use") => CapturedBlock::ToolUse {
                id: bounded_string(block.get("id")).to_owned(),
                complete: false,
            },
            _ => CapturedBlock::Ignored,
        };
        if !self.blocks.contains_key(&index) && self.blocks.len() >= MAX_STREAM_BLOCKS {
            self.disable();
            return;
        }
        let prior_len = self
            .blocks
            .get(&index)
            .map(CapturedBlock::captured_len)
            .unwrap_or_default();
        let next_bytes = self
            .captured_bytes
            .saturating_sub(prior_len)
            .saturating_add(captured.captured_len());
        if next_bytes > MAX_STREAM_CAPTURE {
            self.disable();
            return;
        }
        self.captured_bytes = next_bytes;
        self.blocks.insert(index, captured);
    }

    fn append_delta(&mut self, event: &Value) {
        let Some(index) = event.get("index").and_then(Value::as_u64) else {
            return;
        };
        let Some(delta) = event.get("delta") else {
            return;
        };
        let prior_len = self
            .blocks
            .get(&index)
            .map(CapturedBlock::captured_len)
            .unwrap_or_default();
        let updated = match (
            self.blocks.get_mut(&index),
            delta.get("type").and_then(Value::as_str),
        ) {
            (Some(CapturedBlock::Thinking { thinking, .. }), Some("thinking_delta")) => {
                append_bounded(thinking, delta.get("thinking"))
            }
            (Some(CapturedBlock::Thinking { signature, .. }), Some("signature_delta")) => {
                replace_bounded(signature, delta.get("signature"))
            }
            _ => Some(()),
        };
        let next_len = self
            .blocks
            .get(&index)
            .map(CapturedBlock::captured_len)
            .unwrap_or_default();
        let next_bytes = self
            .captured_bytes
            .saturating_sub(prior_len)
            .saturating_add(next_len);
        match updated {
            Some(()) if next_bytes <= MAX_STREAM_CAPTURE => {
                self.captured_bytes = next_bytes;
            }
            _ => self.disable(),
        }
    }

    fn register(&mut self) {
        if self.disabled || self.blocks.values().any(|block| !block.complete()) {
            return;
        }
        let mut sequence = ProtectedSequence::default();
        for block in self.blocks.values() {
            match block {
                CapturedBlock::Thinking {
                    thinking,
                    signature,
                    ..
                } => {
                    sequence.has_opaque_proof |= !signature.is_empty();
                    sequence.blocks.push(ProtectedBlock::Thinking {
                        thinking,
                        signature,
                    });
                }
                CapturedBlock::RedactedThinking { data, .. } => {
                    sequence.has_opaque_proof |= !data.is_empty();
                    sequence
                        .blocks
                        .push(ProtectedBlock::RedactedThinking { data });
                }
                CapturedBlock::ToolUse { id, .. } if !id.is_empty() => {
                    sequence.tool_ids.push(id);
                }
                CapturedBlock::ToolUse { .. } | CapturedBlock::Ignored => {}
            }
        }
        self.audit.remember_sequence(&self.context, &sequence);
        drop(sequence);
        self.blocks.clear();
        self.captured_bytes = 0;
        self.registered = true;
    }

    fn disable(&mut self) {
        self.disabled = true;
        self.blocks.clear();
        self.captured_bytes = 0;
    }
}

fn append_bounded(target: &mut String, value: Option<&Value>) -> Option<()> {
    let Some(value) = value.and_then(Value::as_str) else {
        return Some(());
    };
    if target.len().saturating_add(value.len()) <= MAX_OPAQUE_FIELD {
        target.push_str(value);
        Some(())
    } else {
        None
    }
}

fn replace_bounded(target: &mut String, value: Option<&Value>) -> Option<()> {
    let Some(value) = value.and_then(Value::as_str) else {
        return Some(());
    };
    if value.len() <= MAX_OPAQUE_FIELD {
        target.clear();
        target.push_str(value);
        Some(())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gw_consts::Protocol;
    use gw_models::ModelParamV2;
    use serde_json::json;

    fn message(role: &str, content: Value) -> ChatMsg {
        let mut message = ChatMsg::text(role, String::new());
        message.parts = Some(content);
        message
    }

    fn request_with_messages(model: &str, messages: Vec<ChatMsg>) -> GatewayRequest {
        let mut param = ModelParamV2::with_name(Protocol::AnthropicMessages, model);
        param.raw = json!({"thinking":{"type":"enabled","budget_tokens":1024}});
        GatewayRequest {
            message: messages,
            model_param_v2: Some(param),
            ..Default::default()
        }
    }

    fn seed_context(audit: &ThinkingSignatureAudit, scope: &str, model: &str) -> AuditContext {
        let request = request_with_messages(
            model,
            vec![message(
                "user",
                json!([{"type":"text","text":"use a tool"}]),
            )],
        );
        let (context, verdict) = audit.review_request(&request, scope);
        assert_eq!(verdict, ReviewVerdict::NoEvidence);
        context.unwrap_or_else(|| panic!("trusted request should create an audit context"))
    }

    fn remember(
        audit: &ThinkingSignatureAudit,
        scope: &str,
        model: &str,
        thinking: &str,
        signature: &str,
        tool_id: &str,
    ) {
        let context = seed_context(audit, scope, model);
        audit.remember_content(
            &context,
            &json!([
                {"type":"thinking","thinking":thinking,"signature":signature},
                {"type":"tool_use","id":tool_id,"name":"probe","input":{}}
            ])
            .as_array()
            .cloned()
            .unwrap_or_default(),
        );
    }

    fn continuation(
        model: &str,
        thinking: &str,
        signature: &str,
        assistant_tool_id: &str,
        result_tool_id: &str,
    ) -> GatewayRequest {
        request_with_messages(
            model,
            vec![
                message(
                    "assistant",
                    json!([
                        {"type":"thinking","thinking":thinking,"signature":signature},
                        {"type":"tool_use","id":assistant_tool_id,"name":"probe","input":{}}
                    ]),
                ),
                message(
                    "user",
                    json!([{"type":"tool_result","tool_use_id":result_tool_id,"content":"ok"}]),
                ),
            ],
        )
    }

    #[test]
    fn exact_matches_tampered_mismatches_and_unknown_anchor_misses() {
        let audit = ThinkingSignatureAudit::new();
        remember(&audit, "key-a", "claude", "", "sig-original", "tool-known");

        let (_, exact) = audit.review_request(
            &continuation("claude", "", "sig-original", "tool-known", "tool-known"),
            "key-a",
        );
        let (_, tampered) = audit.review_request(
            &continuation("claude", "", "sig-tampered", "tool-known", "tool-known"),
            "key-a",
        );
        let (_, unknown) = audit.review_request(
            &continuation("claude", "", "sig-any", "tool-new", "tool-new"),
            "key-a",
        );

        assert_eq!(exact, ReviewVerdict::Match);
        assert_eq!(tampered, ReviewVerdict::Mismatch);
        assert_eq!(unknown, ReviewVerdict::Miss);
    }

    #[test]
    fn visible_summary_is_strict_but_omitted_display_text_is_ignored() {
        let audit = ThinkingSignatureAudit::new();
        remember(
            &audit,
            "key-a",
            "claude",
            "visible summary",
            "sig-summary",
            "tool-summary",
        );
        let (_, modified) = audit.review_request(
            &continuation(
                "claude",
                "changed summary",
                "sig-summary",
                "tool-summary",
                "tool-summary",
            ),
            "key-a",
        );
        assert_eq!(modified, ReviewVerdict::Mismatch);

        remember(&audit, "key-a", "claude", "", "sig-omitted", "tool-omitted");
        let (_, ignored) = audit.review_request(
            &continuation(
                "claude",
                "client text is ignored",
                "sig-omitted",
                "tool-omitted",
                "tool-omitted",
            ),
            "key-a",
        );
        assert_eq!(ignored, ReviewVerdict::Match);
    }

    #[test]
    fn redacted_data_and_protected_block_order_are_checked() {
        let audit = ThinkingSignatureAudit::new();
        let context = seed_context(&audit, "key-a", "claude");
        let original = json!([
            {"type":"thinking","thinking":"","signature":"sig-a"},
            {"type":"redacted_thinking","data":"opaque-data"},
            {"type":"thinking","thinking":"","signature":"sig-b"},
            {"type":"tool_use","id":"tool-known","name":"probe","input":{}}
        ]);
        audit.remember_content(
            &context,
            original.as_array().map(Vec::as_slice).unwrap_or(&[]),
        );

        let make = |protected: Value| {
            request_with_messages(
                "claude",
                vec![
                    message("assistant", protected),
                    message(
                        "user",
                        json!([{"type":"tool_result","tool_use_id":"tool-known","content":"ok"}]),
                    ),
                ],
            )
        };
        let (_, exact) = audit.review_request(&make(original.clone()), "key-a");
        let (_, changed_data) = audit.review_request(
            &make(json!([
                {"type":"thinking","thinking":"","signature":"sig-a"},
                {"type":"redacted_thinking","data":"changed"},
                {"type":"thinking","thinking":"","signature":"sig-b"},
                {"type":"tool_use","id":"tool-known","name":"probe","input":{}}
            ])),
            "key-a",
        );
        let (_, reordered) = audit.review_request(
            &make(json!([
                {"type":"redacted_thinking","data":"opaque-data"},
                {"type":"thinking","thinking":"","signature":"sig-a"},
                {"type":"thinking","thinking":"","signature":"sig-b"},
                {"type":"tool_use","id":"tool-known","name":"probe","input":{}}
            ])),
            "key-a",
        );

        assert_eq!(exact, ReviewVerdict::Match);
        assert_eq!(changed_data, ReviewVerdict::Mismatch);
        assert_eq!(reordered, ReviewVerdict::Mismatch);
    }

    #[test]
    fn known_result_rejects_deleted_or_reidentified_assistant_anchor() {
        let audit = ThinkingSignatureAudit::new();
        remember(&audit, "key-a", "claude", "", "sig-original", "tool-known");

        let deleted = request_with_messages(
            "claude",
            vec![message(
                "user",
                json!([{"type":"tool_result","tool_use_id":"tool-known","content":"ok"}]),
            )],
        );
        let (_, deleted_verdict) = audit.review_request(&deleted, "key-a");
        let (_, reidentified) = audit.review_request(
            &continuation("claude", "", "sig-original", "tool-changed", "tool-known"),
            "key-a",
        );
        let (_, changed_both) = audit.review_request(
            &continuation("claude", "", "sig-original", "tool-changed", "tool-changed"),
            "key-a",
        );

        assert_eq!(deleted_verdict, ReviewVerdict::Mismatch);
        assert_eq!(reidentified, ReviewVerdict::Mismatch);
        assert_eq!(changed_both, ReviewVerdict::Miss);
    }

    #[test]
    fn thinking_disabled_continuation_may_strip_protected_blocks() {
        let audit = ThinkingSignatureAudit::new();
        remember(&audit, "key-a", "claude", "", "sig-original", "tool-known");

        let mut stripped = request_with_messages(
            "claude",
            vec![
                message(
                    "assistant",
                    json!([{"type":"tool_use","id":"tool-known","name":"probe","input":{}}]),
                ),
                message(
                    "user",
                    json!([{"type":"tool_result","tool_use_id":"tool-known","content":"ok"}]),
                ),
            ],
        );
        if let Some(param) = stripped.model_param_v2.as_mut() {
            param.raw = json!({"thinking":{"type":"disabled"}});
        }

        let (_, verdict) = audit.review_request(&stripped, "key-a");
        assert_eq!(verdict, ReviewVerdict::Miss);
    }

    #[test]
    fn colliding_tool_ids_keep_both_conversations_reviewable() {
        let audit = ThinkingSignatureAudit::new();
        remember(&audit, "key-a", "claude", "", "sig-first", "call_1");
        remember(&audit, "key-a", "claude", "", "sig-second", "call_1");

        let (_, first) = audit.review_request(
            &continuation("claude", "", "sig-first", "call_1", "call_1"),
            "key-a",
        );
        let (_, second) = audit.review_request(
            &continuation("claude", "", "sig-second", "call_1", "call_1"),
            "key-a",
        );
        let (_, tampered) = audit.review_request(
            &continuation("claude", "", "sig-forged", "call_1", "call_1"),
            "key-a",
        );

        assert_eq!(first, ReviewVerdict::Match);
        assert_eq!(second, ReviewVerdict::Match);
        assert_eq!(tampered, ReviewVerdict::Mismatch);
    }

    #[test]
    fn only_latest_logical_assistant_tool_turn_is_reviewed() {
        let audit = ThinkingSignatureAudit::new();
        remember(&audit, "key-a", "claude", "", "sig-old", "tool-old");
        let request = request_with_messages(
            "claude",
            vec![
                message(
                    "assistant",
                    json!([
                        {"type":"thinking","thinking":"","signature":"sig-old-tampered"},
                        {"type":"tool_use","id":"tool-old","name":"probe","input":{}}
                    ]),
                ),
                message(
                    "user",
                    json!([{"type":"tool_result","tool_use_id":"tool-old","content":"ok"}]),
                ),
                message(
                    "assistant",
                    json!([{"type":"thinking","thinking":"","signature":"sig-new"}]),
                ),
                message(
                    "assistant",
                    json!([{"type":"tool_use","id":"tool-new","name":"probe","input":{}}]),
                ),
                message(
                    "user",
                    json!([{"type":"tool_result","tool_use_id":"tool-new","content":"ok"}]),
                ),
            ],
        );

        let (_, verdict) = audit.review_request(&request, "key-a");
        assert_eq!(verdict, ReviewVerdict::Miss);
    }

    #[test]
    fn ttl_scope_and_model_misses_fail_open() {
        let audit = ThinkingSignatureAudit::with_ttl(Duration::from_millis(10));
        remember(&audit, "key-a", "claude-a", "", "sig", "tool");
        let request = continuation("claude-a", "", "tampered", "tool", "tool");

        let (_, other_scope) = audit.review_request(&request, "key-b");
        let (_, other_model) = audit.review_request(
            &continuation("claude-b", "", "tampered", "tool", "tool"),
            "key-a",
        );
        std::thread::sleep(Duration::from_millis(25));
        let (_, expired) = audit.review_request(&request, "key-a");

        assert_eq!(other_scope, ReviewVerdict::Miss);
        assert_eq!(other_model, ReviewVerdict::Miss);
        assert_eq!(expired, ReviewVerdict::Miss);
        assert_eq!(THINKING_SIGNATURE_TTL, Duration::from_secs(600));
    }

    #[test]
    fn stream_registers_only_after_complete_message_stop() {
        let audit = ThinkingSignatureAudit::new();
        let context = seed_context(&audit, "key-a", "claude");
        let mut capture = audit
            .stream_capture(Some(context))
            .unwrap_or_else(|| panic!("context should create stream capture"));
        for event in [
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":"stale-start"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"intermediate"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig-stream"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"tool-stream","name":"probe","input":{}}}),
            json!({"type":"content_block_stop","index":1}),
        ] {
            capture.observe(&event);
        }
        let exact_request = continuation("claude", "", "sig-stream", "tool-stream", "tool-stream");
        let (_, before_stop) = audit.review_request(&exact_request, "key-a");
        capture.observe(&json!({"type":"message_stop"}));
        let (_, after_stop) = audit.review_request(&exact_request, "key-a");
        let (_, tampered) = audit.review_request(
            &continuation("claude", "", "sig-tampered", "tool-stream", "tool-stream"),
            "key-a",
        );
        let (_, concatenated) = audit.review_request(
            &continuation(
                "claude",
                "",
                "stale-startintermediatesig-stream",
                "tool-stream",
                "tool-stream",
            ),
            "key-a",
        );

        assert_eq!(before_stop, ReviewVerdict::Miss);
        assert_eq!(after_stop, ReviewVerdict::Match);
        assert_eq!(tampered, ReviewVerdict::Mismatch);
        assert_eq!(concatenated, ReviewVerdict::Mismatch);
    }

    #[test]
    fn oversized_protected_fields_are_not_partially_cached() {
        let audit = ThinkingSignatureAudit::new();
        let context = seed_context(&audit, "key-a", "claude");
        let oversized = "x".repeat(MAX_OPAQUE_FIELD + 1);
        let response = json!([
            {"type":"thinking","thinking":oversized,"signature":"sig"},
            {"type":"tool_use","id":"tool","name":"probe","input":{}}
        ]);
        audit.remember_content(
            &context,
            response.as_array().map(Vec::as_slice).unwrap_or(&[]),
        );

        let (_, verdict) = audit.review_request(
            &continuation("claude", "", "tampered", "tool", "tool"),
            "key-a",
        );
        assert_eq!(verdict, ReviewVerdict::Miss);
    }

    #[test]
    fn capacity_drop_keeps_existing_anchors_and_stays_bounded() {
        let audit = ThinkingSignatureAudit::with_limits(Duration::from_secs(600), 1);
        remember(&audit, "key-a", "claude", "", "sig-one", "tool-one");
        remember(&audit, "key-a", "claude", "", "sig-two", "tool-two");

        let (_, existing) = audit.review_request(
            &continuation("claude", "", "sig-one", "tool-one", "tool-one"),
            "key-a",
        );
        let (_, dropped) = audit.review_request(
            &continuation("claude", "", "sig-two", "tool-two", "tool-two"),
            "key-a",
        );
        assert_eq!(existing, ReviewVerdict::Match);
        assert_eq!(dropped, ReviewVerdict::Miss);
        assert_eq!(audit.inner.cache.lock().unwrap().entries.len(), 1);
    }
}
