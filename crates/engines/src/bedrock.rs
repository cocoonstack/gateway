//! AWS Bedrock plumbing shared by the engines that speak it: SigV4 request
//! signing (region from the endpoint host, the configured model id in the
//! invoke path) and the billed-token headers every InvokeModel reply carries.

use gw_models::GResult;
use serde_json::Value;

use crate::base::Base;
use crate::sigv4::{SigV4Params, sign};
use crate::transport::HeaderMap;

/// Deterministic SigV4 date for the mock round; live calls stamp now.
const MOCK_AMZ_DATE: &str = "20250101T000000Z";

/// SigV4 headers for a bedrock-style call. `creds` = real `(access_key, secret_key)`
/// at go-live (from the account's env-var pair), else the inert mock credentials.
/// The region is the endpoint host's (`bedrock-runtime.<region>.amazonaws.com`).
pub(crate) fn aws_headers(
    host: &str,
    uri: &str,
    payload: &[u8],
    creds: Option<(&str, &str)>,
) -> Vec<(String, String)> {
    let stamped;
    let amz_date = match creds {
        Some(_) => {
            stamped = amz_date_now();
            stamped.as_str()
        }
        None => MOCK_AMZ_DATE,
    };
    let (access_key, secret_key) = creds.unwrap_or(("AKIDMOCK", "mock-secret"));
    let region = host
        .strip_prefix("bedrock-runtime.")
        .and_then(|rest| rest.split('.').next())
        .filter(|region| !region.is_empty())
        .unwrap_or("us-east-1");
    let canonical = canonical_uri(uri);
    let (_, authorization) = sign(&SigV4Params {
        access_key,
        secret_key,
        region,
        service: "bedrock",
        amz_date,
        method: "POST",
        canonical_uri: &canonical,
        canonical_query: "",
        headers: &[("host", host), ("x-amz-date", amz_date)],
        payload,
    });
    vec![
        ("host".into(), host.into()),
        ("x-amz-date".into(), amz_date.into()),
        ("authorization".into(), authorization),
        // Bedrock InvokeModel requires accept; content-type is added by post_json.
        ("accept".into(), "application/json".into()),
    ]
}

fn amz_date_now() -> String {
    amz_date(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0),
    )
}

/// A UTC epoch second as SigV4's `YYYYMMDDTHHMMSSZ` (civil-from-days, no
/// calendar dependency).
fn amz_date(secs: i64) -> String {
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (h, m, s) = (rem / 3600, rem % 3600 / 60, rem % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(mo <= 2);
    format!("{y:04}{mo:02}{d:02}T{h:02}{m:02}{s:02}Z")
}

pub(crate) fn invoke_uri(model: &str) -> String {
    format!("/model/{model}/invoke")
}

/// SigV4's canonical URI: the wire path with every byte outside the
/// unreserved set percent-encoded once (`:` in `…-v1:0` → `%3A`), `/` kept.
fn canonical_uri(path: &str) -> String {
    let mut out = String::with_capacity(path.len() + 8);
    for byte in path.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// One Bedrock invoke: host + scheme from the account endpoint at go-live
/// (else the mock sentinel); SigV4 signs this same host so URL and signature
/// agree. Raw extras merge before signing so the signature covers the exact
/// bytes sent — and the body serializes once, not per layer.
pub(crate) async fn bedrock_invoke(
    base: &mut Base,
    uri: &str,
    mut body: Value,
) -> GResult<(u16, Value, HeaderMap)> {
    if let Some(obj) = body.as_object_mut() {
        let raw = base.take_raw();
        crate::base::merge_raw_extras_owned(obj, raw);
    }
    let root = base.base_url("mock://bedrock-runtime.us-east-1.amazonaws.com");
    let host = root.split_once("://").map(|(_, h)| h).unwrap_or(root);
    let payload = crate::base::body_bytes(&body)?;
    let creds = base.aws_credentials();
    let headers = aws_headers(
        host,
        uri,
        &payload,
        creds
            .as_ref()
            .map(|(a, s): &(String, String)| (a.as_str(), s.as_str())),
    );
    base.post_json_bytes(&format!("{root}{uri}"), headers, payload)
        .await
}

/// Bedrock stamps every InvokeModel reply with the billed token counts; the
/// bodies carry them only for some families (Llama yes, Command R only when
/// streaming), so the headers win when present.
pub(crate) fn bedrock_header_usage(headers: &HeaderMap, body: (i64, i64)) -> (i64, i64) {
    let count = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i64>().ok())
    };
    match (
        count("x-amzn-bedrock-input-token-count"),
        count("x-amzn-bedrock-output-token-count"),
    ) {
        (Some(input), Some(output)) => (input, output),
        _ => body,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amz_date_is_utc_sigv4_shaped_and_region_comes_from_the_host() {
        assert_eq!(amz_date(1_709_251_199), "20240229T235959Z");
        assert_eq!(amz_date(0), "19700101T000000Z");
        assert_eq!(amz_date(1_786_988_810), "20260817T174650Z");
        assert!(amz_date_now().starts_with("20"));
        assert_eq!(
            canonical_uri("/model/meta.llama3-1-8b-instruct-v1:0/invoke"),
            "/model/meta.llama3-1-8b-instruct-v1%3A0/invoke"
        );

        let live = aws_headers(
            "bedrock-runtime.eu-west-1.amazonaws.com",
            "/model/x/invoke",
            b"{}",
            Some(("AKIDEXAMPLE", "secret")),
        );
        let auth = &live.iter().find(|(k, _)| k == "authorization").unwrap().1;
        assert!(auth.contains("/eu-west-1/bedrock/aws4_request"), "{auth}");
        let date = &live.iter().find(|(k, _)| k == "x-amz-date").unwrap().1;
        assert_ne!(date, MOCK_AMZ_DATE);
    }
}
