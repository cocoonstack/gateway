//! AWS Signature Version 4 — pure computation, no I/O.
//!
//! Implements the canonical-request → string-to-sign → signing-key → signature
//! chain per the AWS docs, verified against AWS's published test vectors. Used
//! by the aws-family engines to build the Authorization header; against the
//! MockTransport the header is inert, but the computation is the real one the
//! live transport sends.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub struct SigV4Params<'a> {
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub region: &'a str,
    pub service: &'a str,
    /// x-amz-date, e.g. "20150830T123600Z"
    pub amz_date: &'a str,
    pub method: &'a str,
    pub canonical_uri: &'a str,
    pub canonical_query: &'a str,
    /// (lowercase-name, trimmed-value), MUST be sorted by name.
    pub headers: &'a [(&'a str, &'a str)],
    pub payload: &'a [u8],
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    // HMAC-SHA256 accepts any key length (RFC 2104), so this cannot fire
    #[allow(clippy::expect_used)]
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// The derived signing key: kSecret → kDate → kRegion → kService → kSigning.
pub fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    hmac(&k_service, b"aws4_request")
}

/// Compute the SigV4 signature (hex) and the full Authorization header value.
pub fn sign(p: &SigV4Params) -> (String, String) {
    let signed_headers: Vec<&str> = p.headers.iter().map(|(n, _)| *n).collect();
    let signed_headers = signed_headers.join(";");
    let mut canonical_headers = String::new();
    for (n, v) in p.headers {
        canonical_headers.push_str(n);
        canonical_headers.push(':');
        canonical_headers.push_str(v);
        canonical_headers.push('\n');
    }
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        p.method,
        p.canonical_uri,
        p.canonical_query,
        canonical_headers,
        signed_headers,
        sha256_hex(p.payload)
    );

    let date = &p.amz_date[..8];
    let scope = format!("{date}/{}/{}/aws4_request", p.region, p.service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        p.amz_date,
        sha256_hex(canonical_request.as_bytes())
    );

    let key = signing_key(p.secret_key, date, p.region, p.service);
    let signature = hex::encode(hmac(&key, string_to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        p.access_key
    );
    (signature, authorization)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

    #[test]
    fn aws_doc_signing_key_vector() {
        let key = signing_key(SECRET, "20150830", "us-east-1", "iam");
        assert_eq!(
            hex::encode(key),
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
        );
    }

    #[test]
    fn aws_doc_get_listusers_signature_vector() {
        let (signature, authorization) = sign(&SigV4Params {
            access_key: "AKIDEXAMPLE",
            secret_key: SECRET,
            region: "us-east-1",
            service: "iam",
            amz_date: "20150830T123600Z",
            method: "GET",
            canonical_uri: "/",
            canonical_query: "Action=ListUsers&Version=2010-05-08",
            headers: &[
                (
                    "content-type",
                    "application/x-www-form-urlencoded; charset=utf-8",
                ),
                ("host", "iam.amazonaws.com"),
                ("x-amz-date", "20150830T123600Z"),
            ],
            payload: b"",
        });
        assert_eq!(
            signature,
            "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        );
        assert!(authorization.starts_with(
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request"
        ));
    }
}
