//! Shared SSE pump: the one buffered/live decode loop every streaming engine
//! drives its vendor frames through.

use gw_models::{GResult, GatewayError, StreamChunk, StreamError};
use serde_json::Value;

use crate::sse::SseDecoder;
use crate::transport::{StreamFault, UpstreamBody};

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

/// Deliver the terminal error frame for a committed abort (best effort — the
/// client may already be gone) and mark the pump result aborted. Committed
/// implies a live channel: `sent_any` only ever turns true with `tx` attached.
async fn abort_frame(
    tx: &Option<tokio::sync::mpsc::Sender<StreamChunk>>,
    out: &mut PumpResult,
    error: StreamError,
) {
    debug_assert!(tx.is_some(), "committed abort implies a live channel");
    if let Some(sender) = tx {
        let _ = sender
            .send(StreamChunk {
                error: Some(error),
                ..Default::default()
            })
            .await;
    }
    out.aborted = true;
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
                // A fault after bytes reached the client is a committed abort,
                // not a failover signal: emit the terminal error frame and keep
                // what was delivered. Before commit it is a plain upstream
                // failure, eligible for failover.
                let bytes = match item {
                    Ok(b) => b,
                    Err(fault) if sent_any => {
                        tracing::warn!(vendor, error = %fault.message, "upstream stream failed mid-response");
                        abort_frame(&tx, &mut out, fault.stream_error()).await;
                        break;
                    }
                    Err(fault) => return Err(fault.into_error()),
                };
                let events = match dec.feed(&bytes) {
                    Ok(events) => events,
                    Err(e) if sent_any => {
                        tracing::warn!(vendor, error = %e, "upstream stream failed mid-response");
                        let fault = StreamFault {
                            timeout: false,
                            message: e,
                        };
                        abort_frame(&tx, &mut out, fault.stream_error()).await;
                        break;
                    }
                    Err(e) => {
                        return Err(StreamFault {
                            timeout: false,
                            message: e,
                        }
                        .into_error());
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
                            if let Some(error) = StreamError::from_error(e) {
                                abort_frame(&tx, &mut out, error).await;
                            } else {
                                out.aborted = true;
                            }
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
