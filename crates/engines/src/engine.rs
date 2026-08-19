//! The core engine abstraction: an engine call returns `EngineOutcome`
//! (response, http_code, block) with the error folded into the `Result`.

use gw_consts::ErrCode;
use gw_models::{Block, GResult, GatewayError, GatewayResponse, StreamError};
use serde_json::{Map, Value};

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
    /// The external error sent after a live stream had already committed.
    pub terminal_error: Option<StreamError>,
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
            terminal_error: None,
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
            terminal_error: pump.terminal_error,
        }
    }
}

/// One engine per upstream model method: strictly request → upstream → parse;
/// cross-cutting work belongs to DAG nodes.
#[async_trait::async_trait]
pub trait ModelEngine: Send + Sync {
    /// Perform the upstream call. `&mut self`: the engine owns its request
    /// (cloned once at dispatch, used exactly once), so body building may move
    /// the single-use payloads out instead of cloning them again.
    async fn run(&mut self) -> GResult<EngineOutcome>;
}

/// One upstream usage count floored at 0 (a vendor must never drive a negative
/// into billing); the ceiling is `gw_state::clamp_tokens` at the sinks.
pub fn tok(v: &Value) -> i64 {
    v.as_i64().unwrap_or(0).max(0)
}

/// Move the string at `ptr` (a static, unescaped JSON Pointer) out of `v`; walks
/// the segments itself since `pointer_mut` allocates per segment.
pub fn take_string(v: &mut Value, ptr: &str) -> Option<String> {
    let mut cur = v;
    for segment in ptr.split('/').skip(1) {
        cur = match cur {
            Value::Object(map) => map.get_mut(segment)?,
            Value::Array(items) => items.get_mut(segment.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    match cur.take() {
        Value::String(s) => Some(s),
        _ => None,
    }
}

/// A vendor error as a `GatewayError`: enveloped shapes at any status plus any
/// JSON body on an error status (Bedrock/DashScope answer 4xx flat), so a
/// failure never parses as an empty success; status = upstream's, else the
/// envelope's `http_code`/`code`, else 502.
pub fn vendor_error(http_status: u16, v: &Value) -> Option<GatewayError> {
    let err = v.get("error").filter(|e| e.is_object());
    let message = match (err, http_status >= 400) {
        (Some(e), _) => e["message"].as_str().unwrap_or("upstream error"),
        // the AWS auth layer capitalizes `Message`
        (None, true) => v["message"]
            .as_str()
            .or_else(|| v["msg"].as_str())
            .or_else(|| v["Message"].as_str())
            .unwrap_or("upstream error"),
        (None, false) => return None,
    }
    .to_owned();
    let original_status = if http_status >= 400 {
        Some(http_status)
    } else {
        err.and_then(|e| {
            e["http_code"]
                .as_str()
                .and_then(|s| s.parse::<u16>().ok())
                .or_else(|| e["code"].as_u64().and_then(|c| u16::try_from(c).ok()))
                .or_else(|| e["code"].as_str().and_then(|s| s.parse::<u16>().ok()))
        })
        .filter(|c| *c >= 400)
    };
    let error = GatewayError::new(
        ErrCode::FED_RESP_STATUS_NOT_ZERO,
        original_status.unwrap_or(502),
        message,
    );
    Some(match original_status {
        Some(status) => error.with_original_status(status),
        None => error,
    })
}

pub(crate) fn reject_minimax_error(v: &Value) -> GResult<()> {
    let code = v["base_resp"]["status_code"].as_i64().unwrap_or(0);
    if code == 0 {
        return Ok(());
    }
    Err(GatewayError::new(
        ErrCode::FED_RESP_STATUS_NOT_ZERO,
        502,
        format!("minimax base_resp {code}: {}", v["base_resp"]["status_msg"]),
    ))
}

/// Drop the empty object some vendors emit ahead of accumulated arguments
/// (`{}{"command":…}`) and complete an empty no-argument string to `{}`; an
/// unparseable string passes through untouched.
pub fn normalize_tool_arguments(calls: &mut Value) {
    let Some(calls) = calls.as_array_mut() else {
        return;
    };
    for call in calls {
        let Some(Value::String(args)) = call
            .get_mut("function")
            .and_then(|function| function.get_mut("arguments"))
        else {
            continue;
        };
        let trimmed = args.trim_start();
        if trimmed.is_empty() {
            args.clear();
            args.push_str("{}");
            continue;
        }
        let Some(rest) = trimmed.strip_prefix("{}") else {
            continue;
        };
        let rest = rest.trim_start();
        if serde_json::from_str::<Map<String, Value>>(rest).is_ok() {
            let stripped = args.len() - rest.len();
            args.drain(..stripped);
        }
    }
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
        assert_eq!(e.original_status(), Some(529));
        let e = vendor_error(503, &v).unwrap();
        assert_eq!(e.http_status, 503);
        assert_eq!(e.original_status(), Some(503));
        let e = vendor_error(200, &json!({"error": {"message": "overloaded"}})).unwrap();
        assert_eq!(e.http_status, 502);
        assert_eq!(e.original_status(), None);
    }

    #[test]
    fn flat_error_body_on_an_error_status_is_still_an_error() {
        let bedrock = json!({"message": "The security token is invalid."});
        let e = vendor_error(403, &bedrock).unwrap();
        assert_eq!(e.http_status, 403);
        assert_eq!(e.message, "The security token is invalid.");
        let aws_auth = json!({"Message": "Invalid API Key format"});
        let e = vendor_error(403, &aws_auth).unwrap();
        assert_eq!(e.message, "Invalid API Key format");
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

    #[test]
    fn tool_arguments_drop_a_leading_empty_object() {
        let mut calls = json!([{
            "id": "tooluse_1", "type": "function",
            "function": {"name": "shell", "arguments": "{}{\"command\": \"ls\"}"}
        }]);
        normalize_tool_arguments(&mut calls);
        assert_eq!(
            calls[0]["function"]["arguments"],
            json!("{\"command\": \"ls\"}")
        );
    }

    #[test]
    fn empty_tool_arguments_complete_to_an_object() {
        for original in ["", "  "] {
            let mut calls = json!([{"function": {"name": "now", "arguments": original}}]);
            normalize_tool_arguments(&mut calls);
            assert_eq!(calls[0]["function"]["arguments"], json!("{}"));
        }
    }

    #[test]
    fn well_formed_and_unrecoverable_tool_arguments_are_untouched() {
        for original in [
            "{\"command\":\"ls\"}",
            "{}",
            "not json at all",
            "{\"a\":",
            "{}{\"a\":",
            "{}\"ls\"",
            "{}[1,2]",
            "{}123",
        ] {
            let mut calls = json!([{"function": {"name": "shell", "arguments": original}}]);
            normalize_tool_arguments(&mut calls);
            assert_eq!(
                calls[0]["function"]["arguments"],
                json!(original),
                "arguments {original:?} must be passed through"
            );
        }
    }

    #[test]
    fn tool_arguments_survive_shapes_without_a_string_payload() {
        let mut calls = json!([{"function": {"name": "shell"}}, {"no_function": true}]);
        normalize_tool_arguments(&mut calls);
        assert_eq!(calls[0]["function"]["name"], json!("shell"));
        let mut not_an_array = json!({"function": {"arguments": "{}{}"}});
        normalize_tool_arguments(&mut not_an_array);
        assert_eq!(not_an_array["function"]["arguments"], json!("{}{}"));
    }
}
