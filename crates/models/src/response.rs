//! `GatewayResponse` — the unified engine response.
//!
//! 64-bit integer fields use `i64`. `ModelResponseInterface` (ResponseV2) is
//! held as `serde_json::Value`. The `EngineRecorder` and `RawUsageJSON`
//! fields are runtime-only and excluded from (de)serialization.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

use crate::recorder::Recorder;
use crate::usage::CommonUsage;

/// Unified response produced by every engine's `run()`.
/// Clone-able so the request-level cache can replay it.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct GatewayResponse {
    /// generated chat content.
    pub message: String,
    /// model-requested tool calls (shape varies by protocol: openai tool_calls array /
    /// anthropic tool_use blocks).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<serde_json::Value>,
    /// embedding vector.
    pub embeddings: Vec<f32>,
    /// model name reported by the vendor.
    pub model: String,

    // --- token accounting ---
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub read_cached_prompt_tokens: i64,
    pub write_cached_prompt_tokens: i64,
    pub explicit_cache_hit: bool,
    pub is_messages_protocol: bool,
    pub reasoning_tokens: i64,
    pub total_tokens: i64,

    /// vendor timestamp.
    pub upstream_latency_ms: i64,
    /// upstream HTTP status.
    pub http_code: i64,
    /// vendor error type.
    pub err_type: String,
    /// vendor error code.
    pub err_code: String,

    /// v2 typed response payload (dynamic for now).
    pub response_v2: Option<Value>,
    /// proxy instance used.
    pub proxy_url: String,
    /// actually-requested model when auto-routing.
    pub request_model: String,
    /// tokens to deduct post-request.
    pub post_consume_tokens: i64,
    /// finish reason.
    pub finish_reason: String,

    /// arbitrary key-value info to record in session.
    pub inner_downstream_info: HashMap<String, Value>,

    /// gemini traffic classification.
    pub gemini_traffic_type: String,
    /// PTU spilled over to pay-go account.
    pub ptu_spillover: bool,
    /// minimax voice clone/design extra cost.
    pub minimax_voice_generate_tokens: i64,
    /// sora2 async step marker.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub step: String,

    pub product_switched: bool,
    pub switch_product: String,

    /// normalized usage view, filled by the CommonUsage post-processor.
    pub common_usage: Option<CommonUsage>,

    // --- runtime-only, not part of the serialized model ---
    /// the stream was committed to the client and then broke off; `message`
    /// holds what was delivered (billing estimates from it when usage is absent).
    #[serde(skip)]
    pub aborted: bool,
    /// original engine recorder, kept for downstream fallback.
    #[serde(skip)]
    pub engine_recorder: Option<Arc<dyn Recorder>>,
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
        assert!(r.engine_recorder.is_none());
    }

    #[test]
    fn empty_step_is_omitted() {
        let r = GatewayResponse::default();
        let j = serde_json::to_value(&r).unwrap();
        assert!(j.get("step").is_none());
    }
}
