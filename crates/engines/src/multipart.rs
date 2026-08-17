//! `multipart/form-data` bodies for the upload endpoints — OpenAI's audio
//! transcription/translation and image edits take files, not JSON.

use sha2::{Digest, Sha256};

/// A form under construction: `text` and `file` parts, then `finish` for
/// the content-type header value and the body bytes.
pub struct Form {
    boundary: String,
    body: Vec<u8>,
}

impl Form {
    /// The boundary is the digest of the largest payload, so it cannot occur
    /// inside it (or, in any practical sense, inside the smaller parts).
    pub fn new(payload: &[u8]) -> Self {
        let digest = Sha256::digest(payload);
        Self {
            boundary: format!("gw-{}", hex::encode(&digest[..16])),
            body: Vec::with_capacity(payload.len() + 512),
        }
    }

    pub fn text(&mut self, name: &str, value: &str) {
        self.open(name, None, None);
        self.body.extend_from_slice(value.as_bytes());
        self.body.extend_from_slice(b"\r\n");
    }

    pub fn file(&mut self, name: &str, filename: &str, content_type: &str, bytes: &[u8]) {
        self.open(name, Some(filename), Some(content_type));
        self.body.extend_from_slice(bytes);
        self.body.extend_from_slice(b"\r\n");
    }

    pub fn finish(mut self) -> (String, Vec<u8>) {
        self.body.extend_from_slice(b"--");
        self.body.extend_from_slice(self.boundary.as_bytes());
        self.body.extend_from_slice(b"--\r\n");
        (
            format!("multipart/form-data; boundary={}", self.boundary),
            self.body,
        )
    }

    fn open(&mut self, name: &str, filename: Option<&str>, content_type: Option<&str>) {
        self.body.extend_from_slice(b"--");
        self.body.extend_from_slice(self.boundary.as_bytes());
        self.body
            .extend_from_slice(b"\r\nContent-Disposition: form-data; name=\"");
        self.body.extend_from_slice(name.as_bytes());
        self.body.push(b'"');
        if let Some(filename) = filename {
            self.body.extend_from_slice(b"; filename=\"");
            self.body.extend_from_slice(filename.as_bytes());
            self.body.push(b'"');
        }
        if let Some(content_type) = content_type {
            self.body.extend_from_slice(b"\r\nContent-Type: ");
            self.body.extend_from_slice(content_type.as_bytes());
        }
        self.body.extend_from_slice(b"\r\n\r\n");
    }
}

/// The audio container of an uploaded payload, as the file extension and
/// content type the vendor sniffs; `mp3` when unrecognized.
pub fn audio_kind(bytes: &[u8]) -> (&'static str, &'static str) {
    match bytes {
        [b'R', b'I', b'F', b'F', ..] => ("wav", "audio/wav"),
        [b'f', b'L', b'a', b'C', ..] => ("flac", "audio/flac"),
        [b'O', b'g', b'g', b'S', ..] => ("ogg", "audio/ogg"),
        [0x1A, 0x45, 0xDF, 0xA3, ..] => ("webm", "audio/webm"),
        [_, _, _, _, b'f', b't', b'y', b'p', ..] => ("m4a", "audio/mp4"),
        _ => ("mp3", "audio/mpeg"),
    }
}

/// The image container of an uploaded payload; `png` when unrecognized.
pub fn image_kind(bytes: &[u8]) -> (&'static str, &'static str) {
    match bytes {
        [0xFF, 0xD8, 0xFF, ..] => ("jpg", "image/jpeg"),
        [
            b'R',
            b'I',
            b'F',
            b'F',
            _,
            _,
            _,
            _,
            b'W',
            b'E',
            b'B',
            b'P',
            ..,
        ] => ("webp", "image/webp"),
        _ => ("png", "image/png"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn form_frames_parts_and_closes() {
        let mut form = Form::new(b"payload");
        form.text("model", "whisper-1");
        form.file("file", "audio.wav", "audio/wav", b"RIFFdata");
        let (content_type, body) = form.finish();
        let boundary = content_type
            .strip_prefix("multipart/form-data; boundary=")
            .unwrap();
        let body = String::from_utf8(body).unwrap();
        assert_eq!(
            body,
            format!(
                "--{b}\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nwhisper-1\r\n\
                 --{b}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"audio.wav\"\r\nContent-Type: audio/wav\r\n\r\nRIFFdata\r\n\
                 --{b}--\r\n",
                b = boundary
            )
        );
        assert!(!"payload".contains(boundary));
    }

    #[test]
    fn sniffs_containers() {
        assert_eq!(audio_kind(b"RIFF....WAVE"), ("wav", "audio/wav"));
        assert_eq!(audio_kind(b"ID3\x04"), ("mp3", "audio/mpeg"));
        assert_eq!(
            audio_kind(b"\x00\x00\x00\x18ftypM4A "),
            ("m4a", "audio/mp4")
        );
        assert_eq!(image_kind(b"\x89PNG"), ("png", "image/png"));
        assert_eq!(image_kind(b"\xFF\xD8\xFF\xE0"), ("jpg", "image/jpeg"));
    }
}
