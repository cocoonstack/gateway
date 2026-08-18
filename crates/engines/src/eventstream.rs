//! AWS EventStream (`application/vnd.amazon.eventstream`): the binary framing
//! Bedrock's InvokeModelWithResponseStream answers in. Each message is a
//! prelude (total length, headers length, CRC), typed headers, a payload and
//! a trailing CRC. Bedrock's `chunk` events carry the model's own JSON frame
//! base64-encoded under `bytes`; the adapter re-emits those frames as SSE
//! `data:` lines so the shared pump and every family's frame handler stay
//! unchanged.

use base64::Engine as _;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;

use crate::transport::StreamFault;

/// The largest message a well-behaved model stream carries is far below this;
/// anything bigger is a framing error, not a payload to buffer.
const MAX_MESSAGE: usize = 16 * 1024 * 1024;

const CRC: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);

/// One decoded message: Bedrock's `chunk` events, or a terminal exception.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    /// `:message-type: event` — the payload as sent.
    Event {
        event_type: String,
        payload: Vec<u8>,
    },
    /// `:message-type: exception` / `error` — the fault text.
    Exception(String),
}

/// Incremental decoder over a byte stream of concatenated messages.
#[derive(Debug, Default)]
pub struct Decoder {
    buf: Vec<u8>,
}

impl Decoder {
    /// Feed the next bytes; every message completed by them is returned.
    pub fn feed(&mut self, bytes: &[u8]) -> Result<Vec<Frame>, String> {
        self.buf.extend_from_slice(bytes);
        let mut frames = Vec::new();
        let mut consumed = 0;
        while self.buf.len() - consumed >= 12 {
            let msg = &self.buf[consumed..];
            let total = be_u32(&msg[0..4]) as usize;
            let headers_len = be_u32(&msg[4..8]) as usize;
            if !(16..=MAX_MESSAGE).contains(&total) || headers_len > total - 16 {
                return Err(format!(
                    "eventstream: bad prelude (total {total}, headers {headers_len})"
                ));
            }
            if msg.len() < total {
                break;
            }
            if crc32(&msg[0..8]) != be_u32(&msg[8..12]) {
                return Err("eventstream: prelude crc mismatch".to_owned());
            }
            if crc32(&msg[..total - 4]) != be_u32(&msg[total - 4..total]) {
                return Err("eventstream: message crc mismatch".to_owned());
            }
            frames.push(parse_message(
                &msg[12..12 + headers_len],
                &msg[12 + headers_len..total - 4],
            )?);
            consumed += total;
        }
        self.buf.drain(..consumed);
        Ok(frames)
    }
}

/// Encode one `:message-type: event` message (the mock and tests speak the
/// wire back at the decoder).
pub fn encode_event(event_type: &str, payload: &[u8]) -> Vec<u8> {
    encode(
        &[
            (":message-type", "event"),
            (":event-type", event_type),
            (":content-type", "application/json"),
        ],
        payload,
    )
}

fn encode(string_headers: &[(&str, &str)], payload: &[u8]) -> Vec<u8> {
    let mut headers = Vec::new();
    for (name, value) in string_headers {
        headers.push(name.len() as u8);
        headers.extend_from_slice(name.as_bytes());
        headers.push(7);
        headers.extend_from_slice(&(value.len() as u16).to_be_bytes());
        headers.extend_from_slice(value.as_bytes());
    }
    let total = 12 + headers.len() + payload.len() + 4;
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&(total as u32).to_be_bytes());
    out.extend_from_slice(&(headers.len() as u32).to_be_bytes());
    out.extend_from_slice(&crc32(&out).to_be_bytes());
    out.extend_from_slice(&headers);
    out.extend_from_slice(payload);
    let crc = crc32(&out);
    out.extend_from_slice(&crc.to_be_bytes());
    out
}

/// Bedrock's `chunk` payload for a model frame: `{"bytes": base64(frame)}`.
pub fn chunk_payload(frame: &[u8]) -> Vec<u8> {
    let b64 = base64::engine::general_purpose::STANDARD.encode(frame);
    format!("{{\"bytes\":\"{b64}\"}}").into_bytes()
}

/// A Bedrock response stream as SSE bytes: every `chunk` becomes one
/// `data: <model frame>` event; an exception ends the stream as a fault.
pub fn to_sse(
    stream: BoxStream<'static, Result<Bytes, StreamFault>>,
) -> BoxStream<'static, Result<Bytes, StreamFault>> {
    let mut decoder = Decoder::default();
    Box::pin(stream.map(move |item| {
        let bytes = item?;
        let frames = decoder.feed(&bytes).map_err(fault)?;
        let mut out = Vec::new();
        for frame in frames {
            match frame {
                Frame::Event {
                    event_type,
                    payload,
                } => {
                    out.extend_from_slice(b"data: ");
                    out.extend_from_slice(&chunk_bytes(&payload, &event_type)?);
                    out.extend_from_slice(b"\n\n");
                }
                Frame::Exception(message) => return Err(fault(message)),
            }
        }
        Ok(Bytes::from(out))
    }))
}

/// The model frame inside an InvokeModel `chunk`; a Converse event (no `bytes`)
/// gets its `:event-type` injected as `type`.
fn chunk_bytes(payload: &[u8], event_type: &str) -> Result<Vec<u8>, StreamFault> {
    let mut v: serde_json::Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(_) => return Ok(payload.to_vec()),
    };
    match v.get("bytes").and_then(|b| b.as_str()) {
        Some(b64) => base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| fault(format!("eventstream: chunk bytes not base64: {e}"))),
        None => {
            if let Some(obj) = v.as_object_mut() {
                obj.insert("type".into(), event_type.into());
            }
            serde_json::to_vec(&v).map_err(|e| fault(format!("eventstream: {e}")))
        }
    }
}

fn fault(message: String) -> StreamFault {
    StreamFault {
        timeout: false,
        message,
    }
}

fn parse_message(mut headers: &[u8], payload: &[u8]) -> Result<Frame, String> {
    let (mut message_type, mut event_type, mut exception_type, mut error_message) =
        (None, None, None, None);
    while !headers.is_empty() {
        let name_len = headers[0] as usize;
        let name = headers
            .get(1..1 + name_len)
            .and_then(|n| std::str::from_utf8(n).ok())
            .ok_or("eventstream: bad header name")?;
        headers = &headers[1 + name_len..];
        let (kind, rest) = headers
            .split_first()
            .ok_or("eventstream: truncated header")?;
        let (value, rest) = match kind {
            0 | 1 => (None, rest),
            2 => (None, rest.get(1..).ok_or("eventstream: truncated header")?),
            3 => (None, rest.get(2..).ok_or("eventstream: truncated header")?),
            4 => (None, rest.get(4..).ok_or("eventstream: truncated header")?),
            5 | 8 => (None, rest.get(8..).ok_or("eventstream: truncated header")?),
            9 => (None, rest.get(16..).ok_or("eventstream: truncated header")?),
            6 | 7 => {
                let len = rest
                    .get(0..2)
                    .map(|l| u16::from_be_bytes([l[0], l[1]]) as usize)
                    .ok_or("eventstream: truncated header")?;
                let bytes = rest
                    .get(2..2 + len)
                    .ok_or("eventstream: truncated header")?;
                (
                    (*kind == 7).then(|| std::str::from_utf8(bytes).unwrap_or_default()),
                    &rest[2 + len..],
                )
            }
            other => return Err(format!("eventstream: unknown header type {other}")),
        };
        headers = rest;
        match name {
            ":message-type" => message_type = value,
            ":event-type" => event_type = value,
            ":exception-type" | ":error-code" => exception_type = value,
            ":error-message" => error_message = value,
            _ => {}
        }
    }
    match message_type {
        Some("event") | None => Ok(Frame::Event {
            event_type: event_type.unwrap_or_default().to_owned(),
            payload: payload.to_vec(),
        }),
        Some(_) => {
            let detail = error_message
                .map(str::to_owned)
                .or_else(|| {
                    serde_json::from_slice::<serde_json::Value>(payload)
                        .ok()
                        .and_then(|v| v["message"].as_str().map(str::to_owned))
                })
                .unwrap_or_else(|| String::from_utf8_lossy(payload).into_owned());
            Ok(Frame::Exception(format!(
                "{}: {detail}",
                exception_type.unwrap_or("exception")
            )))
        }
    }
}

fn be_u32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

fn crc32(bytes: &[u8]) -> u32 {
    CRC.checksum(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_matches_the_ieee_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn round_trips_chunks_split_across_arbitrary_byte_boundaries() {
        let a = encode_event("chunk", &chunk_payload(br#"{"type":"message_start"}"#));
        let b = encode_event("chunk", &chunk_payload(br#"{"type":"message_stop"}"#));
        let wire: Vec<u8> = a.iter().chain(&b).copied().collect();
        let mut dec = Decoder::default();
        let mut frames = Vec::new();
        for piece in wire.chunks(7) {
            frames.extend(dec.feed(piece).unwrap());
        }
        assert_eq!(frames.len(), 2);
        assert!(matches!(&frames[0], Frame::Event { event_type, .. } if event_type == "chunk"));
        let Frame::Event { payload, .. } = &frames[1] else {
            panic!("event")
        };
        assert_eq!(
            chunk_bytes(payload, "chunk").unwrap(),
            br#"{"type":"message_stop"}"#
        );
        assert_eq!(
            chunk_bytes(br#"{"contentBlockIndex":0}"#, "contentBlockStop").unwrap(),
            br#"{"contentBlockIndex":0,"type":"contentBlockStop"}"#
        );
    }

    #[test]
    fn exceptions_and_corrupt_frames_are_faults() {
        let mut msg = encode_event("chunk", b"{}");
        let n = msg.len();
        msg[n - 1] ^= 0xFF;
        assert!(Decoder::default().feed(&msg).unwrap_err().contains("crc"));

        let out = encode(
            &[
                (":message-type", "exception"),
                (":exception-type", "modelStreamErrorException"),
            ],
            br#"{"message":"model stopped"}"#,
        );
        assert_eq!(
            Decoder::default().feed(&out).unwrap(),
            vec![Frame::Exception(
                "modelStreamErrorException: model stopped".to_owned()
            )]
        );
    }

    #[tokio::test]
    async fn adapter_emits_one_sse_data_line_per_chunk() {
        let wire = encode_event("chunk", &chunk_payload(br#"{"a":1}"#));
        let stream = futures::stream::iter(vec![Ok(Bytes::from(wire))]);
        let out: Vec<_> = to_sse(Box::pin(stream)).collect().await;
        assert_eq!(out.len(), 1);
        assert_eq!(&out[0].as_ref().unwrap()[..], b"data: {\"a\":1}\n\n");
    }
}
