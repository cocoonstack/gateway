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

/// The play length of an uploaded WAV or MP3 payload, for duration billing
/// when the vendor reports none. Other containers answer `None`.
pub fn audio_seconds(bytes: &[u8]) -> Option<f64> {
    match bytes {
        [b'R', b'I', b'F', b'F', ..] => wav_seconds(bytes),
        [b'f', b'L', b'a', b'C', ..]
        | [b'O', b'g', b'g', b'S', ..]
        | [0x1A, 0x45, 0xDF, 0xA3, ..]
        | [_, _, _, _, b'f', b't', b'y', b'p', ..] => None,
        _ => mp3_seconds(bytes),
    }
}

fn wav_seconds(bytes: &[u8]) -> Option<f64> {
    let le32 = |at: usize| {
        bytes
            .get(at..at + 4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
    };
    let mut at = 12;
    let mut byte_rate = 0;
    while let Some(size) = le32(at + 4) {
        let id = bytes.get(at..at + 4)?;
        match id {
            b"fmt " => byte_rate = le32(at + 16)?,
            b"data" if byte_rate > 0 => {
                let data = size.min(bytes.len().saturating_sub(at + 8));
                return Some(data as f64 / byte_rate as f64);
            }
            _ => {}
        }
        at += 8 + size + (size & 1);
    }
    None
}

/// Layer III frames only (the `mp3` container); frame headers are walked, not
/// decoded, so a VBR file sums exactly.
fn mp3_seconds(bytes: &[u8]) -> Option<f64> {
    const V1_KBPS: [u32; 16] = [
        0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 0,
    ];
    const V2_KBPS: [u32; 16] = [
        0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, 0,
    ];
    let mut at = 0;
    if let [b'I', b'D', b'3', _, _, flags, s0, s1, s2, s3, ..] = bytes {
        let size = [s0, s1, s2, s3]
            .iter()
            .fold(0usize, |acc, &b| (acc << 7) | (*b & 0x7F) as usize);
        at = 10 + size + if flags & 0x10 != 0 { 10 } else { 0 };
    }
    let mut seconds = 0.0;
    let mut frames = 0u32;
    while let Some(h) = bytes.get(at..at + 4) {
        if h[0] != 0xFF || h[1] & 0xE0 != 0xE0 || h[1] & 0x06 != 0x02 {
            break;
        }
        let (kbps, samples, sample_rates): (&[u32; 16], f64, [u32; 3]) = match (h[1] >> 3) & 0x03 {
            0b11 => (&V1_KBPS, 1152.0, [44_100, 48_000, 32_000]),
            0b10 => (&V2_KBPS, 576.0, [22_050, 24_000, 16_000]),
            0b00 => (&V2_KBPS, 576.0, [11_025, 12_000, 8_000]),
            _ => break,
        };
        let bitrate = kbps[(h[2] >> 4) as usize] * 1000;
        let sample_rate = match sample_rates.get(((h[2] >> 2) & 0x03) as usize) {
            Some(&sr) => sr,
            None => break,
        };
        if bitrate == 0 {
            break;
        }
        let padding = ((h[2] >> 1) & 1) as usize;
        let frame_len = (samples as usize / 8) * bitrate as usize / sample_rate as usize + padding;
        seconds += samples / sample_rate as f64;
        frames += 1;
        at += frame_len;
    }
    (frames > 0).then_some(seconds)
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

    #[test]
    fn wav_and_mp3_play_lengths_are_read_from_the_container() {
        let mut wav = Vec::new();
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&(36u32 + 32_000).to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16u32.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&8_000u32.to_le_bytes());
        wav.extend_from_slice(&16_000u32.to_le_bytes());
        wav.extend_from_slice(&2u16.to_le_bytes());
        wav.extend_from_slice(&16u16.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&32_000u32.to_le_bytes());
        wav.resize(wav.len() + 32_000, 0);
        assert_eq!(audio_seconds(&wav), Some(2.0));

        let mut mp3 = vec![b'I', b'D', b'3', 3, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0];
        for _ in 0..100 {
            mp3.extend_from_slice(&[0xFF, 0xFB, 0x90, 0x00]);
            mp3.resize(mp3.len() + 417 - 4, 0);
        }
        let secs = audio_seconds(&mp3).unwrap();
        assert!((secs - 100.0 * 1152.0 / 44_100.0).abs() < 1e-9, "{secs}");
        assert_eq!(audio_seconds(b"OggS...."), None);
    }
}
