//! Shared engine scaffolding: request + transport, the account go-live seam
//! (endpoint/key resolution), and the JSON round-trip helpers the family and
//! bespoke engines build on.

use gw_models::{GResult, GatewayError};
use serde_json::Value;

use crate::transport::{SharedTransport, UpstreamBody, UpstreamRequest, UpstreamResponse};

pub(crate) struct Base {
    pub request: gw_models::GatewayRequest,
    pub transport: SharedTransport,
}

impl Base {
    pub fn new(request: gw_models::GatewayRequest, transport: SharedTransport) -> Self {
        Self { request, transport }
    }

    pub fn account(&self) -> String {
        self.request.account_name().to_owned()
    }

    /// The go-live seam: the account's configured endpoint when set, else the
    /// `mock_sentinel` (offline — MockTransport routes by the path in it).
    pub fn base_url(&self, mock_sentinel: &str) -> String {
        self.request
            .account
            .as_ref()
            .map(|a| a.base_url(mock_sentinel).to_owned())
            .unwrap_or_else(|| mock_sentinel.to_owned())
    }

    /// The account's API key (read from its env var at call time when live),
    /// else the inert "mock" sentinel.
    pub fn api_key(&self) -> String {
        self.request
            .account
            .as_ref()
            .and_then(|a| a.api_key())
            .unwrap_or_else(|| "mock".to_owned())
    }

    /// AWS `(access_key, secret_key)` from the account's env-var pair, if both set.
    pub fn aws_credentials(&self) -> Option<(String, String)> {
        self.request
            .account
            .as_ref()
            .and_then(|a| a.aws_credentials())
    }

    pub fn param(&self) -> GResult<&gw_models::ModelParamV2> {
        self.request
            .model_param_v2
            .as_ref()
            .ok_or_else(|| GatewayError::bad_request("missing model param"))
    }

    pub fn model_name(&self) -> GResult<&str> {
        Ok(self.param()?.model_name.as_str())
    }

    pub fn chat_params(&self) -> Option<&gw_models::ChatParams> {
        match self.param().ok()?.typed.as_ref()? {
            gw_models::TypedParams::Chat(p) => Some(p),
            _ => None,
        }
    }

    /// The last message's content — the free-text fallback the non-chat
    /// families use when typed params are absent.
    pub fn last_message_text(&self) -> &str {
        self.request
            .message
            .last()
            .map(|m| m.content.as_str())
            .unwrap_or_default()
    }

    /// System text for wires that carry it out-of-band: `typed.system` first,
    /// then the request's system turns, newline-joined. Engines that consume
    /// this must skip system-role messages when building their message lists.
    pub fn system_text(&self) -> String {
        let mut out = String::new();
        if let Some(s) = self.chat_params().and_then(|p| p.system.as_deref()) {
            out.push_str(s);
        }
        for m in &self.request.message {
            if m.role == gw_consts::role::SYSTEM && !m.content.is_empty() {
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(&m.content);
            }
        }
        out
    }

    /// Bearer auth headers (the OpenAI-shaped families); real key when the
    /// account is live, inert "mock" otherwise.
    pub fn bearer_headers(&self) -> Vec<(String, String)> {
        vec![
            ("content-type".into(), "application/json".into()),
            ("authorization".into(), format!("Bearer {}", self.api_key())),
        ]
    }

    /// Build and send an upstream POST, buffering a live SSE stream. Engines
    /// that stream dispatch on the body type themselves.
    pub async fn send_upstream(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body: Value,
        stream: bool,
    ) -> GResult<UpstreamResponse> {
        self.send_upstream_raw(url, headers, body, stream)
            .await?
            .buffered()
            .await
    }

    /// Like [`Self::send_upstream`] but leaves a live SSE stream undrained so
    /// the caller can pump it incrementally.
    pub async fn send_upstream_raw(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body: Value,
        stream: bool,
    ) -> GResult<UpstreamResponse> {
        let bytes = body_bytes(&body)?;
        self.send_bytes(url, headers, bytes, stream).await
    }

    /// Build and send an upstream POST from pre-serialized bytes — the SigV4
    /// engines sign the exact payload they send.
    pub async fn send_bytes(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
        stream: bool,
    ) -> GResult<UpstreamResponse> {
        let param = self.param()?;
        let up = UpstreamRequest {
            protocol: param.protocol,
            method: "POST".to_owned(),
            url: url.to_owned(),
            headers,
            body,
            stream,
            account: self.account(),
        };
        self.transport.send(up).await
    }

    /// POST body to `url` with Bearer auth, expect JSON back (non-streaming).
    pub async fn round_trip(&self, url: &str, body: Value) -> GResult<(u16, Value)> {
        self.round_trip_with(url, self.bearer_headers(), body).await
    }

    /// POST body to `url` with explicit headers, expect JSON back (non-streaming).
    pub async fn round_trip_with(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body: Value,
    ) -> GResult<(u16, Value)> {
        let reply = self.send_upstream(url, headers, body, false).await?;
        parse_json_reply(reply)
    }

    /// Bespoke-engine POST (no buffering): merges `param.raw` passthrough into
    /// the body (typed fields stay authoritative) and ensures a JSON
    /// content-type, so every field the caller set reaches the vendor.
    pub async fn post_raw(
        &self,
        url: &str,
        mut headers: Vec<(String, String)>,
        mut body: Value,
        stream: bool,
    ) -> GResult<UpstreamResponse> {
        if let Some(obj) = body.as_object_mut() {
            merge_raw_extras(obj, &self.param()?.raw);
        }
        ensure_json_content_type(&mut headers);
        self.send_upstream_raw(url, headers, body, stream).await
    }

    /// [`Self::post_raw`] + buffer + parse, expect JSON back (non-streaming).
    pub async fn post_json(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body: Value,
    ) -> GResult<(u16, Value)> {
        let reply = self
            .post_raw(url, headers, body, false)
            .await?
            .buffered()
            .await?;
        parse_json_reply(reply)
    }

    /// [`Self::send_bytes`] + buffer + parse for a pre-serialized body (no raw
    /// merge — the caller already folded extras in before signing/sending).
    /// The ensured content-type is an unsigned header on the AWS engines.
    pub async fn post_json_bytes(
        &self,
        url: &str,
        mut headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> GResult<(u16, Value)> {
        ensure_json_content_type(&mut headers);
        let reply = self
            .send_bytes(url, headers, body, false)
            .await?
            .buffered()
            .await?;
        parse_json_reply(reply)
    }
}

/// Serialize a request body straight to bytes (no intermediate String pass).
pub(crate) fn body_bytes(body: &Value) -> GResult<Vec<u8>> {
    serde_json::to_vec(body)
        .map_err(|e| GatewayError::internal("serialize request body").with_source(e))
}

fn ensure_json_content_type(headers: &mut Vec<(String, String)>) {
    if !headers
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("content-type"))
    {
        headers.insert(0, ("content-type".into(), "application/json".into()));
    }
}

/// Merge `raw` passthrough fields into a wire body; typed fields stay
/// authoritative (`or_insert`).
pub(crate) fn merge_raw_extras(body: &mut serde_json::Map<String, Value>, raw: &Value) {
    if let Value::Object(extra) = raw {
        for (k, v) in extra {
            body.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }
}

/// Decode a buffered JSON reply, surfacing vendor error envelopes instead of
/// parsing them as broken success (bespoke engines add their own vendor-
/// specific checks, e.g. minimax base_resp, on top of this).
fn parse_json_reply(reply: UpstreamResponse) -> GResult<(u16, Value)> {
    let bytes = match &reply.body {
        UpstreamBody::Json(b) => b,
        UpstreamBody::Sse(_) | UpstreamBody::SseStream(_) => {
            return Err(GatewayError::internal(
                "unexpected sse body for json engine",
            ));
        }
    };
    let v: Value = serde_json::from_slice(bytes)
        .map_err(|e| GatewayError::internal("parse upstream response").with_source(e))?;
    if let Some(err) = crate::engine::vendor_error(reply.status, &v) {
        return Err(err);
    }
    Ok((reply.status, v))
}

/// Declare an engine struct that is pure `Base` scaffolding.
macro_rules! base_engine {
    ($name:ident) => {
        pub struct $name {
            base: crate::base::Base,
        }

        impl $name {
            pub fn new(
                request: gw_models::GatewayRequest,
                transport: crate::transport::SharedTransport,
            ) -> Self {
                Self {
                    base: crate::base::Base::new(request, transport),
                }
            }
        }
    };
}
pub(crate) use base_engine;
