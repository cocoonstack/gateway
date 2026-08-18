//! `GatewayResponse` — the unified engine response. Runtime-only fields
//! (raw usage bytes) are excluded from (de)serialization.

use serde_json::Value;

use crate::usage::CommonUsage;

/// Unified response produced by every engine's `run()`.
/// Clone-able so the request-level cache can replay it.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct GatewayResponse {
    pub message: String,
    /// model-requested tool calls (openai tool_calls array / anthropic tool_use blocks).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<serde_json::Value>,
    /// reasoning prose on a non-native surface (Anthropic thinking text,
    /// OpenAI-compatible `reasoning_content`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reasoning: String,
    /// reasoning units in emission order (`reasoning_details` shape): the
    /// signed/encrypted proof a later turn replays.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_details: Option<Vec<Value>>,
    /// model name reported by the vendor.
    pub model: String,

    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub read_cached_prompt_tokens: i64,
    pub is_messages_protocol: bool,
    pub reasoning_tokens: i64,
    pub total_tokens: i64,

    /// v2 typed response payload (dynamic for now).
    pub response_v2: Option<Value>,
    /// Native Anthropic content blocks. Anthropic views use this to preserve
    /// signed thinking and redacted-thinking blocks in their original order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anthropic_content: Option<Value>,
    pub finish_reason: String,

    /// PTU spilled over to pay-go account.
    pub ptu_spillover: bool,
    /// Non-token units the vendor metered (TTS input characters, transcription
    /// seconds, rerank search units); priced by the model's `unit_price_micros`.
    #[serde(default)]
    pub billed_units: i64,
    /// sora2 async step marker.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub step: String,

    /// normalized usage view, filled by the CommonUsage post-processor.
    pub common_usage: Option<CommonUsage>,

    /// the stream was committed to the client and then broke off; `message`
    /// holds what was delivered (billing estimates from it when usage is absent).
    #[serde(skip)]
    pub aborted: bool,
    /// raw usage sub-tree from the vendor body/last SSE frame.
    #[serde(skip)]
    pub raw_usage: Option<Value>,
}

impl GatewayResponse {
    /// Whether the native response contains opaque Anthropic thinking proof.
    /// Such responses must not enter the general response cache, which can
    /// outlive the ten-minute consistency cache or be shared through Redis.
    pub fn has_protected_anthropic_content(&self) -> bool {
        self.anthropic_content
            .as_ref()
            .and_then(Value::as_array)
            .is_some_and(|blocks| {
                blocks
                    .iter()
                    .any(crate::request::is_protected_anthropic_block)
            })
    }
}

/// A mid-stream failure as rendered to the client: the contract
/// classification, a human-readable message (no internal numeric codes), and
/// the upstream's original status when an upstream HTTP response was received.
#[derive(Debug, Clone)]
pub struct StreamError {
    pub class: gw_consts::ErrClass,
    pub message: String,
    pub original_status: Option<u16>,
}

impl StreamError {
    /// Build from an internal error. `None` for client-closed (499): the
    /// peer is gone and no frame is rendered.
    pub fn from_error(e: crate::GatewayError) -> Option<Self> {
        let class = gw_consts::ErrClass::classify(e.code, e.http_status)?;
        let original_status = e.original_status();
        Some(Self {
            class,
            message: e.message,
            original_status,
        })
    }

    /// Terminal frame for an already-committed stream: a generic upstream
    /// failure becomes ModelStreamError; transient classes stay sharper.
    pub fn from_committed_error(e: crate::GatewayError) -> Option<Self> {
        let mut error = Self::from_error(e)?;
        if error.class == gw_consts::ErrClass::ModelError {
            error.class = gw_consts::ErrClass::ModelStreamError;
        }
        Some(error)
    }
}

/// One streamed response fragment, forwarded to the client as it arrives.
#[derive(Debug, Default, Clone)]
pub struct StreamChunk {
    pub delta: String,
    /// reasoning prose delta.
    pub reasoning: String,
    /// completed reasoning units (`reasoning_details` shape) — a signed
    /// thinking block once its signature arrived, or a vendor's own units.
    pub reasoning_details: Option<Vec<Value>>,
    /// tool-call delta fragment (vendor wire shape), forwarded as it arrives.
    pub tool_calls: Option<Value>,
    pub finish_reason: Option<String>,
    /// final (prompt, completion, total) token counts; sent once at stream end.
    pub usage_totals: Option<(i64, i64, i64)>,
    /// cache/reasoning detail riding with `usage_totals` when the vendor sent it.
    pub common_usage: Option<CommonUsage>,
    /// The vendor's own SSE event (Anthropic Messages, OpenAI Responses). The
    /// matching native view forwards it verbatim — signed thinking, reasoning
    /// items and all; other protocol views ignore it.
    pub native_event: Option<Value>,
    /// set when the pipeline failed mid-stream; views emit it as an error frame.
    pub error: Option<Box<StreamError>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_zero_value() {
        let r = GatewayResponse::default();
        assert_eq!(r.message, "");
        assert_eq!(r.prompt_tokens, 0);
        assert!(r.common_usage.is_none());
    }

    #[test]
    fn empty_step_is_omitted() {
        let r = GatewayResponse::default();
        let j = serde_json::to_value(&r).unwrap();
        assert!(j.get("step").is_none());
    }

    #[test]
    fn detects_protected_anthropic_content() {
        let protected = GatewayResponse {
            anthropic_content: Some(serde_json::json!([
                {"type":"thinking","thinking":"","signature":"opaque"},
                {"type":"text","text":"answer"}
            ])),
            ..Default::default()
        };
        assert!(protected.has_protected_anthropic_content());

        let plain = GatewayResponse {
            anthropic_content: Some(serde_json::json!([{"type":"text","text":"answer"}])),
            ..Default::default()
        };
        assert!(!plain.has_protected_anthropic_content());
    }

    #[test]
    fn committed_generic_vendor_error_is_stream_specific() {
        let e = crate::GatewayError::new(
            gw_consts::ErrCode::FED_RESP_STATUS_NOT_ZERO,
            502,
            "upstream error",
        );
        let e = StreamError::from_committed_error(e).unwrap();
        assert_eq!(e.class, gw_consts::ErrClass::ModelStreamError);
        assert_eq!(e.original_status, None);
    }

    #[test]
    fn committed_known_throttle_stays_actionable() {
        let e = crate::GatewayError::new(
            gw_consts::ErrCode::FED_RESP_STATUS_NOT_ZERO,
            429,
            "rate limited",
        )
        .with_original_status(429);
        let e = StreamError::from_committed_error(e).unwrap();
        assert_eq!(e.class, gw_consts::ErrClass::Throttling);
        assert_eq!(e.original_status, Some(429));
    }
}
