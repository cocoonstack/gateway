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
            let (events, _done) = SseDecoder::decode_all(&b);
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
                let bytes = match item {
                    Ok(b) => b,
                    Err(e) if sent_any => {
                        // committed: keep what was delivered, no failover
                        tracing::warn!(vendor, error = %e, "upstream stream failed mid-response");
                        out.aborted = true;
                        break;
                    }
                    Err(e) => {
                        return Err(GatewayError::new(
                            gw_consts::ErrCode::FED_RESP_RPC_FAILED,
                            502,
                            format!("upstream stream failed: {e}"),
                        ));
                    }
                };
                for data in dec.feed(&bytes) {
                    let v: Value = serde_json::from_str(&data).map_err(|e| {
                        GatewayError::internal(format!("parse {vendor} sse frame")).with_source(e)
                    })?;
                    for chunk in apply(&v)? {
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
