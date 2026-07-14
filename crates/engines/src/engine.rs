//! The core engine abstraction: an engine call returns `EngineOutcome`
//! (response, http_code, block) with the error folded into the `Result`.

use gw_consts::ErrCode;
use gw_models::{Block, GResult, GatewayError, GatewayResponse};
use serde_json::Value;

pub use gw_models::StreamChunk;

/// One upstream usage count, floored at 0 — a vendor must never drive a negative
/// into billing/quota. The overflow ceiling is applied later at the metering
/// sinks (see `gw_state::clamp_tokens`).
pub fn tok(v: &Value) -> i64 {
    v.as_i64().unwrap_or(0).max(0)
}

/// Detect a vendor error envelope and turn it into a `GatewayError`. Covers
/// OpenAI-style `{"error":{…}}` and MiniMax-style `{"type":"error",…}`. The
/// HTTP status is the real upstream status if already an error, else the
/// vendor's `http_code`/`code` if it looks like one, else 502.
pub fn vendor_error(http_status: u16, v: &Value) -> Option<GatewayError> {
    let err = v.get("error").filter(|e| e.is_object())?;
    let message = err["message"]
        .as_str()
        .unwrap_or("upstream error")
        .to_owned();
    let status = if http_status >= 400 {
        http_status
    } else {
        err["http_code"]
            .as_str()
            .and_then(|s| s.parse::<u16>().ok())
            .or_else(|| err["code"].as_u64().map(|c| c as u16))
            .or_else(|| err["code"].as_str().and_then(|s| s.parse::<u16>().ok()))
            .filter(|c| *c >= 400)
            .unwrap_or(502)
    };
    Some(GatewayError::new(
        ErrCode::FED_RESP_STATUS_NOT_ZERO,
        status,
        message,
    ))
}

/// Fill `total_tokens` from prompt + completion when the vendor omitted it.
pub fn fill_total_if_zero(resp: &mut GatewayResponse) {
    if resp.total_tokens == 0 {
        resp.total_tokens = resp.prompt_tokens.saturating_add(resp.completion_tokens);
    }
}

/// What a single upstream call produced.
#[derive(Debug, Default)]
pub struct EngineOutcome {
    pub response: GatewayResponse,
    pub http_code: u16,
    pub block: Block,
    /// decoded stream chunks when the request was streaming and no live
    /// channel was attached (chunks were already forwarded otherwise).
    pub chunks: Vec<StreamChunk>,
    /// chunks were forwarded through the request's `stream_tx` as they arrived.
    pub streamed_live: bool,
}

impl EngineOutcome {
    /// A successful (200, unblocked) outcome carrying `response`.
    pub fn ok(response: GatewayResponse) -> Self {
        Self::with_status(response, 200)
    }

    /// A non-streaming, unblocked outcome carrying `response` at `http_code`.
    pub fn with_status(response: GatewayResponse, http_code: u16) -> Self {
        Self {
            response,
            http_code,
            block: Block::allow(),
            chunks: Vec::new(),
            streamed_live: false,
        }
    }
}

/// One engine per upstream model method. An engine's job is strictly
/// request → upstream → parse; cross-cutting work (usage normalization,
/// error mapping, quota, billing, retries) belongs to DAG nodes.
#[async_trait::async_trait]
pub trait ModelEngine: Send + Sync {
    /// Perform the upstream call.
    async fn run(&self) -> GResult<EngineOutcome>;
}
