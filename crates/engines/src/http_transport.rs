//! Real HTTP transport (reqwest + rustls) and the default scheme-routing dispatch.
//!
//! Engines address accounts without a configured endpoint via `mock://` sentinel
//! URLs. [`DispatchTransport`] — the server default — keeps those in-process
//! ([`MockTransport`]) and sends real URLs over HTTP, so going live is purely an
//! account-config change. SSE responses come back as a live byte stream;
//! engines decode incrementally or drain via `UpstreamResponse::buffered`.

use std::collections::HashMap;
use std::time::Duration;

use gw_models::{GResult, GatewayError};

use crate::transport::{
    DEFAULT_CONNECT_RETRIES, MockTransport, StreamFault, Transport, UpstreamBody, UpstreamRequest,
    UpstreamResponse, upstream_fault_code,
};

const RETRY_BACKOFF: Duration = Duration::from_millis(100);
// A hung connect (black-holed SYN) must surface as a connect error — which the
// retry predicate covers — instead of burning the whole request timeout.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Live per-account request timeout and connect-phase retry budget.
#[derive(Debug, Clone, Copy)]
pub struct UpstreamPolicy {
    pub timeout: Duration,
    pub connect_retries: u32,
}

impl Default for UpstreamPolicy {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            connect_retries: DEFAULT_CONNECT_RETRIES,
        }
    }
}

/// The vendor's `Retry-After` seconds, capped; unparseable waits at least a
/// second, absent falls back to the connect path's linear backoff.
fn status_backoff(headers: &reqwest::header::HeaderMap, attempt: u32) -> Duration {
    const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);
    const MIN_HEADER_WAIT: Duration = Duration::from_secs(1);
    match headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
    {
        Some(v) => v
            .trim()
            .parse::<u64>()
            .map(|secs| Duration::from_secs(secs).min(MAX_RETRY_AFTER))
            .unwrap_or_else(|_| (RETRY_BACKOFF * attempt).max(MIN_HEADER_WAIT)),
        None => RETRY_BACKOFF * attempt,
    }
}

/// The default plus per-account upstream policies, swapped as one unit on reload.
#[derive(Debug, Default)]
struct Policies {
    default: UpstreamPolicy,
    per_account: HashMap<String, UpstreamPolicy>,
}

/// Real HTTP transport (reqwest + rustls). Per-account policy lives behind an
/// `ArcSwap` so a config reload can update timeouts/retries without a restart.
#[derive(Debug)]
pub struct HttpTransport {
    client: reqwest::Client,
    policies: arc_swap::ArcSwap<Policies>,
}

impl HttpTransport {
    pub fn new(timeout: Duration) -> GResult<Self> {
        Self::with_policies(
            UpstreamPolicy {
                timeout,
                ..UpstreamPolicy::default()
            },
            HashMap::new(),
        )
    }

    pub fn with_policies(
        default: UpstreamPolicy,
        per_account: HashMap<String, UpstreamPolicy>,
    ) -> GResult<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|e| GatewayError::internal("build http client").with_source(e))?;
        Ok(Self {
            client,
            policies: arc_swap::ArcSwap::from_pointee(Policies {
                default,
                per_account,
            }),
        })
    }

    pub fn policy_for(&self, account: &str) -> UpstreamPolicy {
        let p = self.policies.load();
        p.per_account.get(account).copied().unwrap_or(p.default)
    }
}

#[async_trait::async_trait]
impl Transport for HttpTransport {
    fn reload_policies(
        &self,
        default: UpstreamPolicy,
        per_account: HashMap<String, UpstreamPolicy>,
    ) {
        self.policies.store(std::sync::Arc::new(Policies {
            default,
            per_account,
        }));
    }

    async fn send(&self, req: UpstreamRequest) -> GResult<UpstreamResponse> {
        let method = reqwest::Method::from_bytes(req.method.as_bytes())
            .map_err(|e| GatewayError::bad_request(format!("bad method: {e}")))?;
        let policy = self.policy_for(&req.account);
        let timeout = policy.timeout;
        let retry_budget = req
            .replay_account
            .as_ref()
            .map_or(policy.connect_retries, |account| {
                account.connect_retries.unwrap_or(DEFAULT_CONNECT_RETRIES)
            });
        let body = bytes::Bytes::from(req.body);
        let mut attempt = 0u32;
        let resp = loop {
            let mut builder = self.client.request(method.clone(), &req.url);
            // reqwest's request timeout is a TOTAL deadline including the body,
            // which would kill a streaming generation longer than the policy —
            // streams get a header-phase deadline here and an idle gap cap below
            if !req.stream {
                builder = builder.timeout(timeout);
            }
            for (k, v) in &req.headers {
                builder = builder.header(k, v);
            }
            let sent = builder.body(body.clone()).send();
            let result = if req.stream {
                match tokio::time::timeout(timeout, sent).await {
                    Ok(r) => r,
                    Err(_) => {
                        return Err(GatewayError::new(
                            gw_consts::ErrCode::FED_RESP_TIMEOUT,
                            502,
                            format!(
                                "upstream request failed: no response headers within {:?}",
                                timeout
                            ),
                        ));
                    }
                }
            } else {
                sent.await
            };
            match result {
                Ok(resp)
                    if req.replay_account.as_ref().is_some_and(|account| {
                        account.retry_status.contains(&resp.status().as_u16())
                    }) && attempt < retry_budget =>
                {
                    attempt += 1;
                    metrics::counter!(
                        "gateway_upstream_status_retries_total",
                        "account" => req.account.clone(),
                        "status" => resp.status().as_u16().to_string(),
                    )
                    .increment(1);
                    let wait = status_backoff(resp.headers(), attempt);
                    drop(resp);
                    tokio::time::sleep(wait).await;
                }
                Ok(resp) => break resp,
                Err(e) if e.is_connect() && attempt < retry_budget => {
                    attempt += 1;
                    metrics::counter!(
                        "gateway_upstream_connect_retries_total",
                        "account" => req.account.clone(),
                    )
                    .increment(1);
                    tokio::time::sleep(RETRY_BACKOFF * attempt).await;
                }
                Err(e) => {
                    return Err(GatewayError::new(
                        upstream_fault_code(e.is_timeout()),
                        502,
                        format!("upstream request failed: {e}"),
                    ));
                }
            }
        };
        let status = resp.status().as_u16();
        let is_sse = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|ct| ct.starts_with("text/event-stream"))
            .unwrap_or(false);
        if is_sse {
            use futures::TryStreamExt;
            let stream = Box::pin(resp.bytes_stream().map_err(|e| StreamFault {
                timeout: e.is_timeout(),
                message: e.to_string(),
            }));
            return Ok(UpstreamResponse {
                status,
                body: idle_capped(stream, timeout),
            });
        }
        let read = resp.bytes();
        let bytes = if req.stream {
            tokio::time::timeout(timeout, read).await.map_err(|_| {
                GatewayError::new(
                    gw_consts::ErrCode::FED_RESP_TIMEOUT,
                    502,
                    format!("upstream body not read within {:?}", timeout),
                )
            })?
        } else {
            read.await
        };
        let bytes = bytes.map_err(|e| {
            // both stay 502 (failover-eligible); the code split preserves the
            // external 408-vs-424 classification
            let what = if e.is_timeout() {
                "read upstream body timed out"
            } else {
                "read upstream body failed"
            };
            GatewayError::new(upstream_fault_code(e.is_timeout()), 502, what).with_source(e)
        })?;
        Ok(UpstreamResponse {
            status,
            body: UpstreamBody::Json(bytes),
        })
    }
}

/// Wrap a live SSE byte stream so a vendor that stops sending for `gap` yields
/// one terminal error item — no gap between chunks may exceed the policy
/// timeout, but an actively flowing stream lives as long as the generation.
fn idle_capped(
    stream: futures::stream::BoxStream<'static, Result<bytes::Bytes, StreamFault>>,
    gap: Duration,
) -> UpstreamBody {
    use futures::StreamExt;
    UpstreamBody::SseStream(Box::pin(futures::stream::unfold(
        Some(stream),
        move |state| async move {
            let mut s = state?;
            match tokio::time::timeout(gap, s.next()).await {
                Ok(Some(item)) => Some((item, Some(s))),
                Ok(None) => None,
                Err(_) => Some((
                    Err(StreamFault {
                        timeout: true,
                        message: format!("stream idle for {gap:?}"),
                    }),
                    None,
                )),
            }
        },
    )))
}

/// Default transport: `mock://` sentinel URLs (accounts with no configured
/// endpoint) stay in-process, everything else goes over real HTTP.
#[derive(Debug)]
pub struct DispatchTransport {
    mock: MockTransport,
    http: HttpTransport,
}

impl DispatchTransport {
    pub fn new(timeout: Duration) -> GResult<Self> {
        Ok(Self {
            mock: MockTransport,
            http: HttpTransport::new(timeout)?,
        })
    }

    pub fn with_policies(
        default_policy: UpstreamPolicy,
        per_account: HashMap<String, UpstreamPolicy>,
    ) -> GResult<Self> {
        Ok(Self {
            mock: MockTransport,
            http: HttpTransport::with_policies(default_policy, per_account)?,
        })
    }
}

#[async_trait::async_trait]
impl Transport for DispatchTransport {
    fn reload_policies(
        &self,
        default: UpstreamPolicy,
        per_account: HashMap<String, UpstreamPolicy>,
    ) {
        self.http.reload_policies(default, per_account);
    }

    async fn send(&self, req: UpstreamRequest) -> GResult<UpstreamResponse> {
        if req.url.starts_with("mock://") {
            self.mock.send(req).await
        } else {
            self.http.send(req).await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    async fn flaky(
        status: u16,
        fail_times: u32,
        retry_after: Option<&'static str>,
    ) -> (String, Arc<AtomicU32>) {
        let hits = Arc::new(AtomicU32::new(0));
        let seen = hits.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                use tokio::io::AsyncReadExt;
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await;
                let n = seen.fetch_add(1, Ordering::SeqCst);
                let (code, extra) = if n < fail_times {
                    (
                        status,
                        retry_after
                            .map(|v| format!("Retry-After: {v}\r\n"))
                            .unwrap_or_default(),
                    )
                } else {
                    (200, String::new())
                };
                let body = "{\"ok\":true}";
                let resp = format!(
                    "HTTP/1.1 {code} X\r\nContent-Type: application/json\r\n{extra}\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                use tokio::io::AsyncWriteExt;
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
        });
        (format!("http://{addr}/v1/chat/completions"), hits)
    }

    fn request(url: String) -> UpstreamRequest {
        UpstreamRequest {
            protocol: gw_consts::Protocol::OpenaiChat,
            method: "POST".into(),
            url,
            headers: Vec::new(),
            body: b"{}".to_vec(),
            stream: false,
            account: "acct".into(),
            replay_account: None,
        }
    }

    async fn send_with(retry_status: Vec<u16>, url: String) -> u16 {
        let policy = UpstreamPolicy {
            timeout: Duration::from_secs(5),
            connect_retries: 2,
        };
        let t = HttpTransport::with_policies(policy, HashMap::new()).unwrap();
        let mut req = request(url);
        if !retry_status.is_empty() {
            req.replay_account = Some(Arc::new(gw_models::Account {
                connect_retries: Some(2),
                retry_status,
                ..Default::default()
            }));
        }
        t.send(req).await.unwrap().status
    }

    #[tokio::test]
    async fn a_status_not_declared_retryable_is_returned_as_is() {
        let (url, hits) = flaky(502, 1, None).await;
        let status = send_with(Vec::new(), url).await;
        assert_eq!(status, 502, "default must never replay a reached vendor");
        assert_eq!(hits.load(Ordering::SeqCst), 1, "exactly one upstream call");
    }

    #[tokio::test]
    async fn a_declared_status_is_replayed_until_it_succeeds() {
        let (url, hits) = flaky(502, 1, None).await;
        let status = send_with(vec![502], url).await;
        assert_eq!(status, 200, "the retry must surface the successful attempt");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            2,
            "one failure plus one replay"
        );
    }

    #[tokio::test]
    async fn replays_are_bounded_by_the_retry_budget() {
        let (url, hits) = flaky(429, 99, None).await;
        let status = send_with(vec![429], url).await;
        assert_eq!(status, 429, "a vendor that never recovers still answers");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            3,
            "initial call plus connect_retries replays"
        );
    }

    #[tokio::test]
    async fn a_streaming_request_replays_declared_statuses_too() {
        let (url, hits) = flaky(502, 1, None).await;
        let policy = UpstreamPolicy {
            timeout: Duration::from_secs(5),
            connect_retries: 2,
        };
        let t = HttpTransport::with_policies(policy, HashMap::new()).unwrap();
        let mut req = request(url);
        req.stream = true;
        req.replay_account = Some(Arc::new(gw_models::Account {
            connect_retries: Some(2),
            retry_status: vec![502],
            ..Default::default()
        }));
        let resp = t.send(req).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn retry_after_is_honoured_and_capped() {
        let mut h = reqwest::header::HeaderMap::new();
        assert_eq!(
            status_backoff(&h, 2),
            RETRY_BACKOFF * 2,
            "no header falls back to backoff"
        );
        h.insert(reqwest::header::RETRY_AFTER, "3".parse().unwrap());
        assert_eq!(status_backoff(&h, 1), Duration::from_secs(3));
        h.insert(reqwest::header::RETRY_AFTER, "9000".parse().unwrap());
        assert_eq!(
            status_backoff(&h, 1),
            Duration::from_secs(30),
            "a hostile header is capped"
        );
        h.insert(
            reqwest::header::RETRY_AFTER,
            "Wed, 21 Oct 2026 07:28:00 GMT".parse().unwrap(),
        );
        assert_eq!(
            status_backoff(&h, 3),
            Duration::from_secs(1),
            "an unparsed header still waits at least a second"
        );
    }
}
