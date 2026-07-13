//! The core engine abstraction.
//!
//! An engine call conceptually returns a `(response, http_code, block, error)`
//! tuple. Rust folds the `(response, http_code, block)` triple into
//! `EngineOutcome` and the trailing `error` into the `Result`.

use gw_consts::ErrCode;
use gw_models::{Block, GResult, GatewayError, GatewayResponse, Recorder};
use serde_json::Value;

pub use gw_models::StreamChunk;

/// Detect a vendor error envelope and turn it into a `GatewayError`.
/// Covers OpenAI-style `{"error":{message,type,code}}` and MiniMax-style
/// `{"type":"error","error":{http_code,message}}`, normalized from each engine's
/// error branch. The HTTP status is the real upstream status if it's already an error, else the
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

/// What a single upstream call produced.
#[derive(Debug, Default)]
pub struct EngineOutcome {
    pub response: GatewayResponse,
    /// upstream HTTP status.
    pub http_code: u16,
    /// content-safety verdict.
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
        Self {
            response,
            http_code: 200,
            block: Block::allow(),
            chunks: Vec::new(),
            streamed_live: false,
        }
    }
}

/// One engine per upstream model method.
///
/// An engine's job is strictly request → upstream → parse:
/// build the vendor request, send it, parse the raw response into `GatewayResponse`.
/// Cross-cutting work (usage normalization, error-code mapping, quota, billing,
/// retries) belongs to DAG nodes, not here.
#[async_trait::async_trait]
pub trait ModelEngine: Send + Sync {
    /// Perform the upstream call.
    async fn run(&self) -> GResult<EngineOutcome>;

    /// The per-request latency recorder.
    fn recorder(&self) -> &dyn Recorder;
}
