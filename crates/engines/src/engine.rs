//! The core engine abstraction: an engine call returns `EngineOutcome`
//! (response, http_code, block) with the error folded into the `Result`.

use gw_consts::ErrCode;
use gw_models::{Block, GResult, GatewayError, GatewayResponse};
use serde_json::Value;

pub use gw_models::StreamChunk;

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

    /// A streaming outcome: chunks, liveness, and the abort flag from the pump.
    pub fn from_pump(
        mut response: GatewayResponse,
        http_code: u16,
        pump: crate::pump::PumpResult,
    ) -> Self {
        response.aborted = pump.aborted;
        Self {
            response,
            http_code,
            block: Block::allow(),
            chunks: pump.chunks,
            streamed_live: pump.streamed_live,
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

/// One upstream usage count, floored at 0 — a vendor must never drive a negative
/// into billing/quota. The overflow ceiling is applied later at the metering
/// sinks (see `gw_state::clamp_tokens`).
pub fn tok(v: &Value) -> i64 {
    v.as_i64().unwrap_or(0).max(0)
}

/// Detect a vendor error and turn it into a `GatewayError`. Covers the
/// enveloped shapes (OpenAI `{"error":{…}}`, MiniMax `{"type":"error",…}`) at
/// any status, plus — because some vendors (Bedrock, DashScope native) answer
/// 4xx/5xx with a FLAT body — any JSON reply on an error status, so an
/// upstream failure can never parse as an empty success. The HTTP status is
/// the real upstream status if already an error, else the envelope's
/// `http_code`/`code` if it looks like one, else 502.
pub fn vendor_error(http_status: u16, v: &Value) -> Option<GatewayError> {
    let err = v.get("error").filter(|e| e.is_object());
    let message = match (err, http_status >= 400) {
        (Some(e), _) => e["message"].as_str().unwrap_or("upstream error"),
        (None, true) => v["message"]
            .as_str()
            .or_else(|| v["msg"].as_str())
            .unwrap_or("upstream error"),
        (None, false) => return None,
    }
    .to_owned();
    let status = if http_status >= 400 {
        http_status
    } else {
        err.and_then(|e| {
            e["http_code"]
                .as_str()
                .and_then(|s| s.parse::<u16>().ok())
                .or_else(|| e["code"].as_u64().map(|c| c as u16))
                .or_else(|| e["code"].as_str().and_then(|s| s.parse::<u16>().ok()))
        })
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn enveloped_error_maps_status_and_message() {
        let v = json!({"error": {"message": "overloaded", "code": "529"}});
        let e = vendor_error(200, &v).unwrap();
        assert_eq!((e.http_status, e.message.as_str()), (529, "overloaded"));
        let e = vendor_error(503, &v).unwrap();
        assert_eq!(e.http_status, 503);
    }

    #[test]
    fn flat_error_body_on_an_error_status_is_still_an_error() {
        let bedrock = json!({"message": "The security token is invalid."});
        let e = vendor_error(403, &bedrock).unwrap();
        assert_eq!(e.http_status, 403);
        assert_eq!(e.message, "The security token is invalid.");
        let dashscope =
            json!({"code": "Throttling", "message": "rate exceeded", "request_id": "r"});
        let e = vendor_error(429, &dashscope).unwrap();
        assert_eq!((e.http_status, e.message.as_str()), (429, "rate exceeded"));
        assert_eq!(
            vendor_error(500, &json!({"weird": true})).unwrap().message,
            "upstream error"
        );
    }

    #[test]
    fn success_shapes_are_not_errors() {
        assert!(vendor_error(200, &json!({"choices": []})).is_none());
        assert!(vendor_error(200, &json!({"error": "string not object"})).is_none());
    }
}
