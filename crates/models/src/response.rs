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
    pub finish_reason: String,

    /// PTU spilled over to pay-go account.
    pub ptu_spillover: bool,
    /// sora2 async step marker.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub step: String,

    /// normalized usage view, filled by the CommonUsage post-processor.
    pub common_usage: Option<CommonUsage>,

    /// the stream was committed to the client and then broke off; `message`
    /// holds what was delivered (billing estimates from it when usage is absent).
    #[serde(skip)]
    pub aborted: bool,
    /// raw usage sub-tree bytes from the vendor body/last SSE frame.
    #[serde(skip)]
    pub raw_usage_json: Vec<u8>,
}

/// One streamed response fragment, forwarded to the client as it arrives.
#[derive(Debug, Default, Clone)]
pub struct StreamChunk {
    pub delta: String,
    /// tool-call delta fragment (vendor wire shape), forwarded as it arrives.
    pub tool_calls: Option<Value>,
    pub finish_reason: Option<String>,
    /// final (prompt, completion, total) token counts; sent once at stream end.
    pub usage_totals: Option<(i64, i64, i64)>,
    /// set when the pipeline failed mid-stream; views emit it as an error frame.
    pub error: Option<String>,
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
}
