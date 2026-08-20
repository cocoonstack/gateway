//! AWS Bedrock plumbing shared by the engines that speak it: SigV4 request
//! signing (region from the endpoint host, the configured model id in the
//! invoke path) or a Bedrock API key as bearer, and the billed-token headers
//! every InvokeModel reply carries.

use gw_models::{GResult, GatewayResponse, StreamChunk};
use serde_json::Value;

use crate::base::Base;
use crate::sigv4::{SigV4Params, sign};
use crate::transport::{HeaderMap, Headers, UpstreamBody, UpstreamResponse};

/// Deterministic SigV4 date for the mock round; live calls stamp now.
const MOCK_AMZ_DATE: &str = "20250101T000000Z";

/// SigV4 headers for a bedrock-style call: real `creds` at go-live, else the
/// inert mock pair; the region is parsed from the endpoint host.
pub(crate) fn aws_headers(
    host: &str,
    uri: &str,
    payload: &[u8],
    creds: Option<(&str, &str)>,
) -> Headers {
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
        ("host", host.into()),
        ("x-amz-date", amz_date.into()),
        ("authorization", authorization),
        // InvokeModel requires accept; content-type is unsigned and added by the caller
        ("accept", "application/json".into()),
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

pub(crate) fn invoke_uri(model: &str, stream: bool) -> String {
    if stream {
        format!("/model/{model}/invoke-with-response-stream")
    } else {
        format!("/model/{model}/invoke")
    }
}

pub(crate) fn converse_uri(model: &str, stream: bool) -> String {
    if stream {
        format!("/model/{model}/converse-stream")
    } else {
        format!("/model/{model}/converse")
    }
}

/// SigV4's canonical URI: the wire path with every byte outside the
/// unreserved set percent-encoded once (`:` in `…-v1:0` → `%3A`), `/` kept.
fn canonical_uri(path: &str) -> String {
    const ESCAPED: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~')
        .remove(b'/');
    percent_encoding::utf8_percent_encode(path, ESCAPED).to_string()
}

/// One InvokeModel[WithResponseStream] call: SigV4 signs the exact bytes sent
/// (extras merged first, body serialized once); a stream comes back re-framed as SSE.
pub(crate) async fn bedrock_send(
    base: &mut Base,
    model: &str,
    body: Value,
) -> GResult<UpstreamResponse> {
    let uri = invoke_uri(model, base.request.stream);
    bedrock_send_uri(base, &uri, body).await
}

/// [`bedrock_send`] at an explicit path (InvokeModel or Converse).
pub(crate) async fn bedrock_send_uri(
    base: &mut Base,
    uri: &str,
    mut body: Value,
) -> GResult<UpstreamResponse> {
    if let Some(obj) = body.as_object_mut() {
        let raw = base.take_raw();
        crate::base::merge_raw_extras_owned(obj, raw);
    }
    let stream = base.request.stream;
    let root = base.base_url("mock://bedrock-runtime.us-east-1.amazonaws.com");
    let host = root.split_once("://").map(|(_, h)| h).unwrap_or(root);
    let payload = crate::base::body_bytes(&body)?;
    let mut headers = match base.bedrock_bearer() {
        Some(token) => vec![
            ("host", host.to_owned()),
            ("authorization", format!("Bearer {token}")),
            ("accept", "application/json".into()),
        ],
        None => {
            let creds = base.aws_credentials();
            aws_headers(
                host,
                uri,
                &payload,
                creds
                    .as_ref()
                    .map(|(a, s): &(String, String)| (a.as_str(), s.as_str())),
            )
        }
    };
    headers.push(("content-type", "application/json".into()));
    let mut reply = base
        .send_bytes(&format!("{root}{uri}"), headers, payload, stream)
        .await?;
    if let UpstreamBody::SseStream(s) = reply.body {
        reply.body = UpstreamBody::SseStream(crate::eventstream::to_sse(s));
    }
    Ok(reply)
}

/// The streamed form: the family's frame handler runs inside the shared pump;
/// an error status (always a JSON body) surfaces the vendor envelope first.
pub(crate) async fn bedrock_stream<F>(
    base: &mut Base,
    body: Value,
    apply: F,
) -> GResult<(u16, crate::pump::PumpResult)>
where
    F: FnMut(Value) -> GResult<Vec<StreamChunk>>,
{
    let model = base.model_name()?.to_owned();
    let reply = bedrock_send(base, &model, body).await?;
    let status = reply.status;
    crate::pump::reject_json_error("bedrock", status, &reply.body)?;
    let r =
        crate::pump::pump_sse("bedrock", reply.body, base.request.stream_tx.clone(), apply).await?;
    Ok((status, r))
}

/// The last streamed frame carries `amazon-bedrock-invocationMetrics` with the
/// billed counts, authoritative over whatever the family's frames said.
pub(crate) fn invocation_metrics(frame: &Value, resp: &mut GatewayResponse) {
    let metrics = &frame["amazon-bedrock-invocationMetrics"];
    if let Some(n) = metrics["inputTokenCount"].as_i64() {
        resp.prompt_tokens = n;
    }
    if let Some(n) = metrics["outputTokenCount"].as_i64() {
        resp.completion_tokens = n;
    }
}

/// The buffered form for the non-streaming engines: parsed JSON plus the reply
/// headers the billed counts ride in.
pub(crate) async fn bedrock_invoke(
    base: &mut Base,
    model: &str,
    body: Value,
) -> GResult<(u16, Value, HeaderMap)> {
    let mut reply = bedrock_send(base, model, body).await?.buffered().await?;
    let headers = std::mem::take(&mut reply.headers);
    let (status, v) = crate::base::parse_json_reply(reply)?;
    Ok((status, v, headers))
}

/// Billed input tokens, `fallback` when the header is absent — an embed reply
/// carries the input header alone, so the pairwise form never matches it.
pub(crate) fn bedrock_input_tokens(headers: &HeaderMap, fallback: i64) -> i64 {
    token_header(headers, "x-amzn-bedrock-input-token-count").unwrap_or(fallback)
}

/// Bedrock stamps every InvokeModel reply with the billed counts while only some
/// family bodies carry them, so the headers win when present.
pub(crate) fn bedrock_header_usage(headers: &HeaderMap, body: (i64, i64)) -> (i64, i64) {
    match (
        token_header(headers, "x-amzn-bedrock-input-token-count"),
        token_header(headers, "x-amzn-bedrock-output-token-count"),
    ) {
        (Some(input), Some(output)) => (input, output),
        _ => body,
    }
}

fn token_header(headers: &HeaderMap, name: &str) -> Option<i64> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
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
        let auth = &live.iter().find(|(k, _)| *k == "authorization").unwrap().1;
        assert!(auth.contains("/eu-west-1/bedrock/aws4_request"), "{auth}");
        let date = &live.iter().find(|(k, _)| *k == "x-amz-date").unwrap().1;
        assert_ne!(date, MOCK_AMZ_DATE);
    }
}
