//! Content-at-rest sealing for the retention store: authenticated encryption
//! (XChaCha20-Poly1305) under a process-wide deployment key from
//! `GW_CONTENT_KEY` (64 hex chars = 32 bytes). No key configured → sealing is
//! unavailable, and `full` retention refuses to store raw content.

use std::sync::LazyLock;

use base64::Engine as _;
use chacha20poly1305::aead::{Aead, KeyInit, OsRng};
use chacha20poly1305::{AeadCore, XChaCha20Poly1305};

/// The process-wide content key, loaded once from `GW_CONTENT_KEY`.
static CIPHER: LazyLock<Option<XChaCha20Poly1305>> = LazyLock::new(|| {
    let raw = std::env::var("GW_CONTENT_KEY").ok()?;
    match hex::decode(raw.trim()) {
        Ok(bytes) if bytes.len() == 32 => Some(XChaCha20Poly1305::new(bytes.as_slice().into())),
        _ => {
            tracing::error!("GW_CONTENT_KEY is not 64 hex chars (32 bytes); content sealing off");
            None
        }
    }
});

/// One stored prompt or response.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ContentRecord {
    pub created_at_epoch_secs: i64,
    pub request_id: String,
    pub ak: String,
    pub user_id: String,
    pub tenant: String,
    /// "prompt" | "response".
    pub kind: String,
    /// Sealed (base64 nonce||ciphertext) or plaintext, per [`ContentRecord::sealed`].
    pub content: String,
    /// Whether `content` is sealed ciphertext.
    pub sealed: bool,
    /// Unix seconds after which the purge job deletes this row; 0 = keep.
    pub expires_at_epoch_secs: i64,
}

/// Whether a deployment key is configured (so `full` retention may store raw).
pub fn sealing_available() -> bool {
    CIPHER.is_some()
}

/// Seal `plaintext` into base64(nonce ‖ ciphertext); `None` when no key is set.
pub fn seal(plaintext: &str) -> Option<String> {
    seal_with(CIPHER.as_ref()?, plaintext)
}

/// Reverse [`seal`]; `None` when no key is set or the input is malformed/forged.
pub fn open(sealed: &str) -> Option<String> {
    open_with(CIPHER.as_ref()?, sealed)
}

fn seal_with(cipher: &XChaCha20Poly1305, plaintext: &str) -> Option<String> {
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let ct = cipher.encrypt(&nonce, plaintext.as_bytes()).ok()?;
    let mut out = nonce.to_vec();
    out.extend_from_slice(&ct);
    Some(base64::engine::general_purpose::STANDARD.encode(out))
}

fn open_with(cipher: &XChaCha20Poly1305, sealed: &str) -> Option<String> {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(sealed)
        .ok()?;
    if raw.len() < 24 {
        return None;
    }
    let (nonce, ct) = raw.split_at(24);
    let pt = cipher.decrypt(nonce.into(), ct).ok()?;
    String::from_utf8(pt).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seal_open_roundtrips_and_rejects_tamper() {
        let cipher = XChaCha20Poly1305::new((&[7u8; 32]).into());
        let sealed = seal_with(&cipher, "credit card 4111 1111 1111 1111").unwrap();
        assert!(!sealed.contains("4111"), "ciphertext hides the plaintext");
        assert_eq!(
            open_with(&cipher, &sealed).unwrap(),
            "credit card 4111 1111 1111 1111"
        );
        let other = XChaCha20Poly1305::new((&[8u8; 32]).into());
        assert!(open_with(&other, &sealed).is_none(), "wrong key can't open");
        let mut tampered = sealed.clone();
        tampered.push('x');
        assert!(
            open_with(&cipher, &tampered).is_none(),
            "tamper is rejected"
        );
    }
}
