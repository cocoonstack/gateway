//! Shared SSE pump: the one buffered/live decode loop every streaming engine
//! drives its vendor frames through.

use gw_models::{GResult, GatewayError, StreamChunk};
use serde_json::Value;

use crate::sse::SseDecoder;
use crate::transport::UpstreamBody;

/// What a pump run produced.
#[derive(Debug, Default)]
pub struct PumpResult {
    /// decoded chunks, when no live channel consumed them.
    pub chunks: Vec<StreamChunk>,
    /// chunks were forwarded through the live channel as they arrived.
    pub streamed_live: bool,
    /// The response was committed to the client (bytes delivered) and then the
    /// client vanished or the upstream broke. The engine finalizes what was
    /// delivered so billing can account for it; failover must not run — a
    /// retry would splice a second generation into the same stream.
    pub aborted: bool,
}

/// Drive a vendor SSE reply through `apply` (which owns the engine's
/// accumulation state): a buffered body decodes in one pass; a live stream is
/// decoded as bytes arrive and forwarded through `tx` when one is attached.
///
/// A JSON body is the vendor refusing to stream — callers dispatch that
/// themselves before pumping.
/// A stream request answered with JSON is an error body: surface the vendor's
/// error envelope. A JSON body with no envelope falls through to
/// [`pump_sse`]'s generic "expected sse" error.
pub(crate) fn reject_json_error(what: &str, status: u16, body: &UpstreamBody) -> GResult<()> {
    if let UpstreamBody::Json(b) = body {
        let v: Value = serde_json::from_slice(b)
            .map_err(|e| GatewayError::internal(format!("parse {what} reply")).with_source(e))?;
        if let Some(err) = crate::engine::vendor_error(status, &v) {
            return Err(err);
        }
    }
    Ok(())
}

/// A mid-stream transport/decode fault: after bytes reached the client it is a
/// committed abort — keep what was delivered, no failover (`None`); before
/// that it is a plain upstream failure (`Some(err)`).
fn stream_fault(vendor: &'static str, e: &str, sent_any: bool) -> Option<GatewayError> {
    if sent_any {
        tracing::warn!(vendor, error = %e, "upstream stream failed mid-response");
        None
    } else {
        Some(GatewayError::new(
            gw_consts::ErrCode::FED_RESP_RPC_FAILED,
            502,
            format!("upstream stream failed: {e}"),
        ))
    }
}

pub async fn pump_sse<F>(
    vendor: &'static str,
    body: UpstreamBody,
    tx: Option<tokio::sync::mpsc::Sender<StreamChunk>>,
    mut apply: F,
) -> GResult<PumpResult>
where
    F: FnMut(&Value) -> GResult<Vec<StreamChunk>>,
{
    use futures::StreamExt;
    let mut out = PumpResult::default();
    match body {
        UpstreamBody::Json(_) => {
            return Err(GatewayError::internal(format!(
                "expected sse from {vendor}"
            )));
        }
        UpstreamBody::Sse(b) => {
            let (events, _done) = SseDecoder::decode_all(&b)
                .map_err(|e| GatewayError::internal(format!("decode {vendor} sse body: {e}")))?;
            for ev in events {
                let v: Value = serde_json::from_slice(ev.as_bytes()).map_err(|e| {
                    GatewayError::internal(format!("parse {vendor} sse frame")).with_source(e)
                })?;
                out.chunks.extend(apply(&v)?);
            }
        }
        UpstreamBody::SseStream(mut s) => {
            let mut dec = SseDecoder::default();
            let mut sent_any = false;
            while let Some(item) = s.next().await {
                let bytes = match item.map_err(|e| stream_fault(vendor, &e, sent_any)) {
                    Ok(b) => b,
                    Err(Some(err)) => return Err(err),
                    Err(None) => {
                        out.aborted = true;
                        break;
                    }
                };
                let events = match dec
                    .feed(&bytes)
                    .map_err(|e| stream_fault(vendor, &e, sent_any))
                {
                    Ok(events) => events,
                    Err(Some(err)) => return Err(err),
                    Err(None) => {
                        out.aborted = true;
                        break;
                    }
                };
                for data in events {
                    let v: Value = serde_json::from_str(&data).map_err(|e| {
                        GatewayError::internal(format!("parse {vendor} sse frame")).with_source(e)
                    })?;
                    // A vendor error frame (or any apply failure) after bytes
                    // reached the client is a committed abort, NOT a failover
                    // signal — replaying would splice a second generation onto
                    // the same stream.
                    let chunks = match apply(&v) {
                        Ok(c) => c,
                        Err(e) if sent_any => {
                            tracing::warn!(vendor, error = %e, "vendor error frame after commit");
                            out.aborted = true;
                            out.streamed_live = tx.is_some();
                            return Ok(out);
                        }
                        Err(e) => return Err(e),
                    };
                    for chunk in chunks {
                        match &tx {
                            Some(sender) => {
                                if sender.send(chunk).await.is_err() {
                                    if sent_any {
                                        // client left mid-response: finalize the
                                        // delivered part for billing
                                        out.aborted = true;
                                    } else {
                                        return Err(GatewayError::client_closed(
                                            "client stream closed",
                                        ));
                                    }
                                    out.streamed_live = true;
                                    return Ok(out);
                                }
                                sent_any = true;
                            }
                            None => out.chunks.push(chunk),
                        }
                    }
                }
                if out.aborted {
                    break;
                }
            }
            out.streamed_live = sent_any;
        }
    }
    Ok(out)
}
