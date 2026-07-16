//! SSE frame decoder — pure logic.
//!
//! Feed raw bytes, get back the `data:` payloads of complete events. Handles
//! partial frames across feeds and the OpenAI `[DONE]` sentinel. Transport-free
//! so it tests offline.

/// Cap on one buffered event: a peer that never sends a blank-line terminator
/// must fail the stream, not grow the buffer without bound.
const MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;

/// Incremental server-sent-events decoder (data-only, which is what LLM vendors use).
#[derive(Debug, Default)]
pub struct SseDecoder {
    buf: Vec<u8>,
    done: bool,
}

impl SseDecoder {
    /// Push bytes; returns the `data:` payloads of every event completed so far.
    /// `[DONE]` flips `is_done` and is not returned as a payload.
    pub fn feed(&mut self, bytes: &[u8]) -> Result<Vec<String>, String> {
        // Buffer raw bytes and only decode complete events: a network chunk can
        // end mid-way through a multi-byte UTF-8 character, and decoding each
        // chunk separately would corrupt it permanently.
        self.buf.extend_from_slice(bytes);
        let mut out = Vec::new();
        while let Some(end) = event_boundary(&self.buf) {
            let event = String::from_utf8_lossy(&self.buf[..end]);
            for line in event.lines() {
                if let Some(data) = line.strip_prefix("data:") {
                    let data = data.strip_prefix(' ').unwrap_or(data);
                    if data == "[DONE]" {
                        self.done = true;
                    } else if !data.is_empty() {
                        out.push(data.to_owned());
                    }
                }
            }
            self.buf.drain(..end);
        }
        if self.buf.len() > MAX_EVENT_BYTES {
            return Err(format!(
                "sse event exceeds {MAX_EVENT_BYTES} bytes without a terminator"
            ));
        }
        Ok(out)
    }

    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Decode a fully buffered SSE body in one go.
    pub fn decode_all(bytes: &[u8]) -> Result<(Vec<String>, bool), String> {
        let mut d = SseDecoder::default();
        let events = d.feed(bytes)?;
        Ok((events, d.is_done()))
    }
}

/// Index just past the first blank-line event separator. Vendors frame with
/// either LF (`\n\n`, OpenAI) or CRLF (`\r\n\r\n`, Google) — a decoder that
/// only splits on `\n\n` never completes a CRLF-framed event.
fn event_boundary(buf: &[u8]) -> Option<usize> {
    let mut i = 0;
    while i + 1 < buf.len() {
        if buf[i] == b'\n' {
            if buf[i + 1] == b'\n' {
                return Some(i + 2);
            }
            if buf.len() > i + 2 && buf[i + 1] == b'\r' && buf[i + 2] == b'\n' {
                return Some(i + 3);
            }
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_events_and_done() {
        let body = b"data: {\"a\":1}\n\ndata: {\"b\":2}\n\ndata: [DONE]\n\n";
        let (events, done) = SseDecoder::decode_all(body).unwrap();
        assert_eq!(events, vec![r#"{"a":1}"#, r#"{"b":2}"#]);
        assert!(done);
    }

    #[test]
    fn handles_split_frames_across_feeds() {
        let mut d = SseDecoder::default();
        assert!(d.feed(b"data: {\"a\"").unwrap().is_empty());
        let got = d.feed(b":1}\n\ndata: [DO").unwrap();
        assert_eq!(got, vec![r#"{"a":1}"#]);
        assert!(!d.is_done());
        assert!(d.feed(b"NE]\n\n").unwrap().is_empty());
        assert!(d.is_done());
    }

    #[test]
    fn multibyte_utf8_split_across_feeds_survives() {
        let mut d = SseDecoder::default();
        let payload = "data: {\"t\":\"你好😀\"}\n\n".as_bytes();
        let (a, b) = payload.split_at(13);
        assert!(std::str::from_utf8(a).is_err(), "split must land mid-char");
        assert!(d.feed(a).unwrap().is_empty());
        assert_eq!(d.feed(b).unwrap(), vec![r#"{"t":"你好😀"}"#]);
    }

    #[test]
    fn crlf_framed_events_split() {
        let mut d = SseDecoder::default();
        let got = d
            .feed(b"data: {\"a\":1}\r\n\r\ndata: {\"b\":2}\r\n\r\n")
            .unwrap();
        assert_eq!(got, vec![r#"{"a":1}"#, r#"{"b":2}"#]);
        let mut d = SseDecoder::default();
        assert!(d.feed(b"data: x\r\n\r").unwrap().is_empty());
        assert_eq!(d.feed(b"\n").unwrap(), vec!["x"]);
    }

    #[test]
    fn crlf_tolerated() {
        let (events, done) =
            SseDecoder::decode_all(b"data: x\r\n\n\ndata: [DONE]\r\n\n\n").unwrap();
        assert_eq!(events, vec!["x"]);
        assert!(done);
    }

    #[test]
    fn unterminated_event_past_the_cap_errors() {
        let mut d = SseDecoder::default();
        let chunk = vec![b'x'; MAX_EVENT_BYTES / 2 + 1];
        assert!(d.feed(&chunk).unwrap().is_empty());
        assert!(d.feed(&chunk).is_err(), "unbounded buffering must fail");
    }
}
