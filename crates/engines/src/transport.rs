//! Transport isolation — the egress seam. Engines never hold an HTTP client:
//! they build an [`UpstreamRequest`] and hand it to a [`Transport`].
//! [`MockTransport`] (`mock_transport`) fabricates deterministic vendor
//! responses (the test default); `HttpTransport` and the scheme-routing
//! `DispatchTransport` (the server default) live in `http_transport`.

use std::sync::Arc;

use gw_consts::Protocol;
use gw_models::{GResult, GatewayError, StreamError};
pub use reqwest::header::HeaderMap;

pub use crate::mock_transport::MockTransport;

/// Fixed "created" timestamp for deterministic mock payloads.
pub const MOCK_CREATED: i64 = 1_720_000_000;
/// 1x1 PNG-ish placeholder bytes, base64. Deterministic image/audio payload.
pub const MOCK_B64: &str = "TU9DS0JZVEVT"; // "MOCKBYTES"
pub(crate) const DEFAULT_CONNECT_RETRIES: u32 = 1;

/// Wire headers an engine attaches; names are always literals.
pub type Headers = Vec<(&'static str, String)>;

/// A vendor-bound request an engine built, ready to hand to a [`Transport`].
#[derive(Debug, Clone)]
pub struct UpstreamRequest {
    pub protocol: Protocol,
    pub method: &'static str,
    pub url: String,
    pub headers: Headers,
    pub body: Vec<u8>,
    pub stream: bool,
    /// upstream account slot handling this call (used by failover/downtime simulation).
    pub account: String,
    /// Selected account snapshot when it permits status replay.
    pub replay_account: Option<Arc<gw_models::Account>>,
}

/// A live-stream item failure: a transport fault or one of our deadlines.
/// `timeout` selects the ModelTimeout classification downstream.
#[derive(Debug)]
pub struct StreamFault {
    pub timeout: bool,
    pub message: String,
}

impl StreamFault {
    /// The pre-commit form: an ordinary upstream failure eligible for
    /// failover (internal 502 keeps it above the 5xx threshold).
    pub fn into_error(self) -> GatewayError {
        GatewayError::new(
            upstream_fault_code(self.timeout),
            502,
            format!("upstream stream failed: {}", self.message),
        )
    }

    /// The post-commit form: the terminal in-stream error frame.
    pub fn stream_error(&self) -> StreamError {
        StreamError {
            class: if self.timeout {
                gw_consts::ErrClass::ModelTimeout
            } else {
                gw_consts::ErrClass::ModelStreamError
            },
            message: format!("upstream stream failed: {}", self.message),
            original_status: None,
        }
    }
}

/// The upstream-fault code: deadlines classify as ModelTimeout downstream,
/// everything else as the generic RPC failure.
pub(crate) fn upstream_fault_code(timeout: bool) -> gw_consts::ErrCode {
    if timeout {
        gw_consts::ErrCode::FED_RESP_TIMEOUT
    } else {
        gw_consts::ErrCode::FED_RESP_RPC_FAILED
    }
}

/// Body of an upstream response: buffered JSON, buffered SSE bytes, or live
/// SSE bytes yielded as the vendor sends them.
pub enum UpstreamBody {
    Json(bytes::Bytes),
    Sse(Vec<u8>),
    SseStream(futures::stream::BoxStream<'static, Result<bytes::Bytes, StreamFault>>),
}

impl std::fmt::Debug for UpstreamBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpstreamBody::Json(b) => f.debug_tuple("Json").field(&b.len()).finish(),
            UpstreamBody::Sse(b) => f.debug_tuple("Sse").field(&b.len()).finish(),
            UpstreamBody::SseStream(_) => f.write_str("SseStream(..)"),
        }
    }
}

#[derive(Debug)]
pub struct UpstreamResponse {
    pub status: u16,
    pub body: UpstreamBody,
    /// Response headers, moved out of the HTTP reply (empty on the mock).
    pub headers: HeaderMap,
}

impl UpstreamResponse {
    /// Drain a live SSE stream into buffered bytes; Json/Sse pass through.
    /// Engines that don't forward incrementally call this once up front.
    /// Capped so a hostile upstream can't OOM the buffered path.
    pub async fn buffered(mut self) -> GResult<Self> {
        const MAX_BUFFERED_SSE: usize = 64 * 1024 * 1024;
        if let UpstreamBody::SseStream(mut s) = self.body {
            use futures::StreamExt;
            let mut buf = Vec::new();
            while let Some(item) = s.next().await {
                let bytes = item.map_err(StreamFault::into_error)?;
                buf.extend_from_slice(&bytes);
                if buf.len() > MAX_BUFFERED_SSE {
                    return Err(GatewayError::new(
                        gw_consts::ErrCode::FED_RESP_RPC_FAILED,
                        502,
                        format!("upstream sse body exceeds {MAX_BUFFERED_SSE} bytes"),
                    ));
                }
            }
            self.body = UpstreamBody::Sse(buf);
        }
        Ok(self)
    }
}

#[async_trait::async_trait]
pub trait Transport: Send + Sync + std::fmt::Debug {
    async fn send(&self, req: UpstreamRequest) -> GResult<UpstreamResponse>;

    /// Apply reloaded per-account upstream policy (timeouts/retries) live.
    /// Default no-op: only the HTTP-backed transports carry policy.
    fn reload_policies(
        &self,
        _default: crate::http_transport::UpstreamPolicy,
        _per_account: std::collections::HashMap<String, crate::http_transport::UpstreamPolicy>,
    ) {
    }
}

pub type SharedTransport = Arc<dyn Transport>;
