//! `GatewayRequest` — the unified engine request — and the [`domain`] types it
//! references.

pub use domain::*;

/// Everything an engine needs to serve one request.
#[derive(Debug, Default, Clone)]
pub struct GatewayRequest {
    /// Arc'd: selected from the shared pool per request and cloned along the
    /// engine seam — sharing beats copying six strings per hop.
    pub account: Option<std::sync::Arc<Account>>,
    pub message: Vec<ChatMsg>,
    pub stream: bool,
    pub model_param_v2: Option<ModelParamV2>,
    pub ak: String,
    pub is_online: bool,
    /// End-user attribution from request metadata (OpenAI `user` / Anthropic
    /// `metadata.user_id` / `x-gw-user`); only trusted when the key is shared —
    /// a key's own `owner` overrides it at billing.
    pub user_id: Option<String>,
    /// Correlation id assigned at ingress; joins access log, ledger, and audit
    /// events for one request. Empty until the handler stamps it.
    pub request_id: String,
    /// When set, a streaming-capable engine forwards chunks here as they arrive
    /// instead of buffering; the bounded channel is the backpressure seam.
    pub stream_tx: Option<tokio::sync::mpsc::Sender<crate::StreamChunk>>,
}

impl GatewayRequest {
    /// The model type to dispatch on, if a v2 param is present.
    pub fn protocol(&self) -> Option<gw_consts::Protocol> {
        self.model_param_v2.as_ref().map(|p| p.protocol)
    }

    /// The serving account's name; empty when none is selected.
    pub fn account_name(&self) -> &str {
        self.account.as_ref().map(|a| a.name.as_str()).unwrap_or("")
    }
}

/// One queued batch item: a message list plus the client-supplied end-user
/// attribution. `user` is persisted with the item so a distributed drainer on
/// another instance still attributes and budgets it (owner still overrides at
/// billing time); empty when the submitter gave no `user`/`x-gw-user`.
#[derive(Debug, Clone)]
pub struct BatchItem {
    pub messages: Vec<ChatMsg>,
    pub user: String,
}

/// Domain types referenced by `GatewayRequest`. Per-vendor long-tail fields
/// ride in `raw`/`extra` passthroughs rather than being individually modeled.
pub mod domain {
    use serde_json::Value;

    use crate::params::TypedParams;

    /// Typed, dispatch-aware payload for a model call: `protocol` is the
    /// dispatch key, `typed` the family params, vendor extras ride in `raw`.
    #[derive(Debug, Clone)]
    pub struct ModelParamV2 {
        pub protocol: gw_consts::Protocol,
        /// public model name from the caller; empty if a wire type was sent directly.
        pub model_name: String,
        /// original caller model when a quota fallback swapped `model_name`;
        /// the response echoes it and the ledger records both.
        pub fallback_from: Option<String>,
        pub typed: Option<TypedParams>,
        /// untyped vendor extras, passed through verbatim.
        pub raw: Value,
    }

    impl ModelParamV2 {
        pub fn new(protocol: gw_consts::Protocol) -> Self {
            Self {
                protocol,
                ..Default::default()
            }
        }

        pub fn with_name(protocol: gw_consts::Protocol, name: impl Into<String>) -> Self {
            Self {
                protocol,
                model_name: name.into(),
                ..Default::default()
            }
        }
    }

    // `Protocol` has no Default, so the struct's Default is manual.
    impl Default for ModelParamV2 {
        fn default() -> Self {
            Self {
                protocol: gw_consts::Protocol::OpenaiChat,
                model_name: String::new(),
                fallback_from: None,
                typed: None,
                raw: Value::Null,
            }
        }
    }

    /// One chat turn: `content` is the flattened text; multimodal parts and
    /// tool-call fields ride alongside for lossless vendor rebuild.
    #[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
    pub struct ChatMsg {
        pub role: String,
        pub content: String,
        /// original multimodal parts array; takes priority over `content` when present.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub parts: Option<Value>,
        /// tool_calls carried by an assistant message (OpenAI wire shape).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub tool_calls: Option<Value>,
        /// the call id a role:"tool" result message refers back to.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub tool_call_id: Option<String>,
    }

    impl ChatMsg {
        pub fn text(role: impl Into<String>, content: impl Into<String>) -> Self {
            Self {
                role: role.into(),
                content: content.into(),
                ..Default::default()
            }
        }
    }

    /// An upstream account/credential slot.
    #[derive(Debug, Default, Clone)]
    pub struct Account {
        pub name: String,
        pub provider: String,
        pub priority: i32,
        /// consts::account_tier::{PTU, PAYGO}; empty = paygo.
        pub tier: String,
        /// upstream base URL; empty = the engine's `mock://…` sentinel. A real
        /// URL routes over HttpTransport — going live is a pure config change.
        pub endpoint: String,
        /// env var holding this account's API key (empty = mock creds), read at
        /// request time; for AWS it holds the access key id (see `secret_key_env`).
        pub api_key_env: String,
        /// AWS SigV4 only: env var holding the secret access key.
        pub secret_key_env: String,
        /// vendor cost per 1k tokens (micros); zero = untracked.
        pub cost_input_price_per_1k_micros: i64,
        pub cost_output_price_per_1k_micros: i64,
        pub protocols: Vec<gw_consts::Protocol>,
    }

    impl Account {
        pub fn is_ptu(&self) -> bool {
            self.tier == gw_consts::account_tier::PTU
        }

        /// Base URL for building the upstream request: the account's endpoint if
        /// set, else the given `mock://…` sentinel.
        pub fn base_url<'a>(&'a self, mock_sentinel: &'a str) -> &'a str {
            if self.endpoint.is_empty() {
                mock_sentinel
            } else {
                self.endpoint.trim_end_matches('/')
            }
        }

        /// The account's API key, read from its env var at call time; `None` =
        /// use the mock placeholder. Never stored on the struct, never logged.
        pub fn api_key(&self) -> Option<String> {
            if self.api_key_env.is_empty() {
                return None;
            }
            std::env::var(&self.api_key_env).ok()
        }

        /// AWS SigV4 credentials read from the two env vars at call time; `None`
        /// unless both resolve. Never stored or logged.
        pub fn aws_credentials(&self) -> Option<(String, String)> {
            if self.api_key_env.is_empty() || self.secret_key_env.is_empty() {
                return None;
            }
            let access = std::env::var(&self.api_key_env).ok()?;
            let secret = std::env::var(&self.secret_key_env).ok()?;
            Some((access, secret))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gw_consts::Protocol;

    #[test]
    fn dispatch_protocol() {
        let empty = GatewayRequest::default();
        assert!(empty.protocol().is_none());
        let req = GatewayRequest {
            model_param_v2: Some(ModelParamV2::new(Protocol::AnthropicMessages)),
            ..Default::default()
        };
        assert_eq!(req.protocol(), Some(Protocol::AnthropicMessages));
    }

    #[test]
    fn account_endpoint_and_key_seam() {
        let a = Account::default();
        assert_eq!(a.base_url("mock://x"), "mock://x");
        assert!(a.api_key().is_none());

        let a = Account {
            endpoint: "https://api.vendor.com/".into(),
            ..Default::default()
        };
        assert_eq!(a.base_url("mock://x"), "https://api.vendor.com");

        let var = "GW_TEST_ACCOUNT_KEY_SEAM";
        // SAFETY: the var name is unique to this test and nothing reads it concurrently.
        unsafe { std::env::set_var(var, "sk-secret-123") };
        let a = Account {
            api_key_env: var.into(),
            ..Default::default()
        };
        assert_eq!(a.api_key().as_deref(), Some("sk-secret-123"));
        // SAFETY: same unique var; no concurrent reader.
        unsafe { std::env::remove_var(var) };
        assert!(a.api_key().is_none());
    }
}
