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
    /// The external terminal error already sent after a committed stream.
    /// Kept separately from `aborted`: a client disconnect has no provider
    /// error to report.
    pub terminal_error: Option<StreamError>,
}

/// A stream request answered with JSON is an error body: surface the vendor's
/// envelope; one without an envelope falls through to [`pump_sse`]'s "expected sse".
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

/// Deliver the terminal error frame for a committed abort (best effort) and mark
/// the pump result aborted; committed implies `tx` is attached.
async fn abort_frame(
    tx: &Option<tokio::sync::mpsc::Sender<StreamChunk>>,
    out: &mut PumpResult,
    error: StreamError,
) {
    debug_assert!(tx.is_some(), "committed abort implies a live channel");
    out.terminal_error = Some(error.clone());
    if let Some(sender) = tx {
        let _ = sender
            .send(StreamChunk {
                error: Some(Box::new(error)),
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
    F: FnMut(Value) -> GResult<Vec<StreamChunk>>,
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
                out.chunks.extend(apply(v)?);
            }
        }
        UpstreamBody::SseStream(mut s) => {
            let mut dec = SseDecoder::default();
            let mut sent_any = false;
            while let Some(item) = s.next().await {
                // after bytes reached the client a fault is a committed abort (terminal frame,
                // keep what was delivered), before commit a failover-eligible upstream failure
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
                    Err(e) => {
                        let fault = StreamFault {
                            timeout: false,
                            message: e,
                        };
                        if sent_any {
                            tracing::warn!(vendor, error = %fault.message, "upstream stream failed mid-response");
                            abort_frame(&tx, &mut out, fault.stream_error()).await;
                            break;
                        }
                        return Err(fault.into_error());
                    }
                };
                for data in events {
                    // a bad frame after commit is an abort, not a failover: a replay would splice
                    // a second generation onto the same stream
                    let applied = serde_json::from_str(&data)
                        .map_err(|e| {
                            GatewayError::internal(format!("parse {vendor} sse frame"))
                                .with_source(e)
                        })
                        .and_then(&mut apply);
                    let chunks = match applied {
                        Ok(c) => c,
                        Err(e) if sent_any => {
                            tracing::warn!(vendor, error = %e, "vendor frame error after commit");
                            if let Some(error) = StreamError::from_committed_error(e) {
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
                                        // client left mid-response: bill the delivered part
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

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn malformed_frame_is_error_before_commit_and_abort_after_commit() {
        let malformed = || {
            UpstreamBody::SseStream(
                futures::stream::iter([Ok(bytes::Bytes::from_static(b"data: not-json\n\n"))])
                    .boxed(),
            )
        };
        let (tx, _rx) = tokio::sync::mpsc::channel(4);
        assert!(
            pump_sse("test", malformed(), Some(tx), |_| Ok(Vec::new()))
                .await
                .is_err(),
            "a pre-commit parse error remains eligible for failover"
        );

        let frames = [
            Ok(bytes::Bytes::from_static(
                b"data: {\"delta\":\"first\"}\n\n",
            )),
            Ok(bytes::Bytes::from_static(b"data: not-json\n\n")),
        ];
        let body = UpstreamBody::SseStream(futures::stream::iter(frames).boxed());
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let result = pump_sse("test", body, Some(tx), |v| {
            Ok(vec![StreamChunk {
                delta: v["delta"].as_str().unwrap_or_default().to_owned(),
                ..Default::default()
            }])
        })
        .await
        .unwrap();

        assert!(result.aborted);
        assert!(result.streamed_live);
        assert_eq!(
            result.terminal_error.as_ref().map(|error| error.class),
            Some(gw_consts::ErrClass::InternalServer)
        );
        assert_eq!(rx.recv().await.unwrap().delta, "first");
        assert_eq!(
            rx.recv()
                .await
                .unwrap()
                .error
                .as_ref()
                .map(|error| error.class),
            Some(gw_consts::ErrClass::InternalServer)
        );
    }
}
