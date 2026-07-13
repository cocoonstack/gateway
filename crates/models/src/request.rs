//! `GatewayRequest` — the unified engine request.
//!
//! Several fields reference large internal domain types (`Account`,
//! `UserConf`, `ProductConf`, `ModelParamInterface`, …), represented by the
//! types in [`domain`].

use std::collections::HashMap;

pub use domain::*;

/// Everything an engine needs to serve one request.
#[derive(Debug, Default, Clone)]
pub struct GatewayRequest {
    /// upstream account serving the request.
    pub account: Option<Account>,
    /// history + prompt (v1).
    pub message: Vec<ChatMsg>,
    /// pressure/load test flag (v1).
    pub pressure: bool,
    /// streaming output (v1).
    pub stream: bool,
    pub user_config: UserConf,
    pub product_config: ProductConf,
    pub origin_product_config: Option<ProductConf>,
    /// v2 params — the model-type-bearing payload.
    pub model_param_v2: Option<ModelParamV2>,
    pub ak: String,
    /// proxy region (sg/us).
    pub proxy: String,
    pub storage_info_collect: StorageInfoCollect,
    pub is_online: bool,
    pub extra_params: ReqExtraParam,
    /// metrics tags to attach.
    pub metrics_map: HashMap<String, String>,
    pub realtime_params: RealtimeParam,
    /// When set, a streaming-capable engine forwards chunks here as they
    /// arrive from the vendor instead of buffering; the bounded channel is
    /// the backpressure seam.
    pub stream_tx: Option<tokio::sync::mpsc::Sender<crate::StreamChunk>>,
}

impl GatewayRequest {
    /// The model type to dispatch on, if a v2 param is present.
    pub fn protocol(&self) -> Option<gw_consts::Protocol> {
        self.model_param_v2.as_ref().map(|p| p.protocol)
    }
}

/// Domain types referenced by `GatewayRequest`. Per-vendor long-tail fields
/// ride in `raw`/`extra` passthroughs rather than being individually modeled.
pub mod domain {
    use serde_json::Value;

    use crate::params::TypedParams;

    /// Typed, dispatch-aware payload for a model call.
    /// `protocol` is the dispatch key; `model_name` is the public model name the
    /// caller sent (e.g. "gpt-4o") which config maps to a Protocol; `typed` holds
    /// the family-typed params; vendor extras ride in `raw`.
    /// (No derived `Default` — `Protocol` has none; a manual impl lives in the parent module.)
    #[derive(Debug, Clone)]
    pub struct ModelParamV2 {
        pub protocol: gw_consts::Protocol,
        /// public model name from the caller, pre-mapping. Empty if caller sent a wire type directly.
        pub model_name: String,
        /// original caller model when a quota fallback swapped `model_name`;
        /// the response echoes it and the ledger records both.
        pub fallback_from: Option<String>,
        /// family-typed params (chat/embeddings/image/audio/video/search).
        pub typed: Option<TypedParams>,
        /// untyped vendor extras, passed through verbatim.
        pub raw: Value,
    }

    impl ModelParamV2 {
        pub fn new(protocol: gw_consts::Protocol) -> Self {
            Self {
                protocol,
                model_name: String::new(),
                fallback_from: None,
                typed: None,
                raw: Value::Null,
            }
        }

        pub fn with_name(protocol: gw_consts::Protocol, name: impl Into<String>) -> Self {
            Self {
                protocol,
                model_name: name.into(),
                fallback_from: None,
                typed: None,
                raw: Value::Null,
            }
        }

        pub fn with_typed(mut self, typed: TypedParams) -> Self {
            self.typed = Some(typed);
            self
        }
    }

    /// One chat turn.
    /// `content` is the flattened text; multimodal parts and tool-call fields ride
    /// alongside so engines can rebuild the vendor wire form losslessly.
    #[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
    pub struct ChatMsg {
        pub role: String,
        pub content: String,
        /// original multimodal parts array ([{type:"text"|"image_url",...}]); takes priority over content when present.
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
    /// A named slot with provider tag, priority, tier (ptu/paygo), the model types
    /// it serves, and — for live endpoints — endpoint + credential env vars.
    #[derive(Debug, Default, Clone)]
    pub struct Account {
        pub name: String,
        pub provider: String,
        pub priority: i32,
        /// consts::account_tier::{PTU, PAYGO}; empty = paygo.
        pub tier: String,
        /// upstream base URL. Empty → engines use their `mock://…` sentinel (default,
        /// routed by MockTransport). A real URL → engines route there over
        /// HttpTransport — going live is a pure config change.
        pub endpoint: String,
        /// name of the env var holding this account's API key (empty → mock creds).
        /// The real secret is read from the environment at request time — it never
        /// lives in config files.
        /// For AWS accounts this holds the *access key* env var; the paired secret
        /// key env var is `secret_key_env`.
        pub api_key_env: String,
        /// AWS SigV4 accounts only: env var holding the secret access key (paired
        /// with `api_key_env` = the access key id). Empty for non-AWS vendors.
        pub secret_key_env: String,
        /// What this account's vendor charges us per 1k tokens (micros);
        /// zero = untracked. Feeds the ledger's vendor-cost column.
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

        /// The account's API key, read from its configured env var at call time.
        /// `None` → callers use the `"mock"` placeholder credential. The secret is
        /// never stored on the struct, never logged, never handled by the agent.
        pub fn api_key(&self) -> Option<String> {
            if self.api_key_env.is_empty() {
                return None;
            }
            std::env::var(&self.api_key_env).ok()
        }

        /// AWS SigV4 credentials: `(access_key_id, secret_access_key)` read from the
        /// account's two env vars at call time. `None` unless BOTH resolve — callers
        /// then fall back to the inert mock credentials. Never stored or logged.
        pub fn aws_credentials(&self) -> Option<(String, String)> {
            if self.api_key_env.is_empty() || self.secret_key_env.is_empty() {
                return None;
            }
            let access = std::env::var(&self.api_key_env).ok()?;
            let secret = std::env::var(&self.secret_key_env).ok()?;
            Some((access, secret))
        }
    }

    /// User configuration. Core field subset; unmodeled fields pass through
    /// via `extra`.
    #[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
    pub struct UserConf {
        #[serde(default)]
        pub user_name: String,
        #[serde(default)]
        pub is_online: bool,
        #[serde(default)]
        pub allowed_target_regions: Vec<String>,
        #[serde(default)]
        pub allowed_use_llm_plugin: bool,
        #[serde(default)]
        pub scene: String,
        #[serde(default)]
        pub scene_type: i32,
        #[serde(default)]
        pub resource_level: String,
        #[serde(default)]
        pub ak: String,
        /// service/personal
        #[serde(default, rename = "type")]
        pub key_type: String,
        #[serde(default)]
        pub allow_region_downgrade: bool,
        /// unmodeled fields pass through.
        #[serde(default)]
        pub extra: Value,
    }

    /// Product configuration. Core field subset; long-tail fields ride in
    /// `extra`.
    #[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
    pub struct ProductConf {
        #[serde(default)]
        pub model_name: String,
        #[serde(default)]
        pub product: String,
        #[serde(default)]
        pub model_vendor: String,
        #[serde(default)]
        pub channel_name: String,
        /// request-rate limit per time unit, 0 = unlimited.
        #[serde(default)]
        pub request_rate: i64,
        /// token-rate limit per time unit, 0 = unlimited.
        #[serde(default)]
        pub token_rate: i64,
        #[serde(default)]
        pub unit: String,
        #[serde(default)]
        pub resource_statistics: bool,
        /// nested config passthrough (PlatformConfig/ConfigMapping, etc.).
        #[serde(default)]
        pub extra: Value,
    }

    /// Side-channel params for vendor-specific extras (e.g. a vendor-specific
    /// account); core field subset, rest via `extra`.
    #[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
    pub struct ReqExtraParam {
        #[serde(default)]
        pub elevenlab_method: String,
        #[serde(default)]
        pub url2base64: bool,
        #[serde(default)]
        pub content_length: i64,
        #[serde(default)]
        pub extra: Value,
    }

    macro_rules! stub {
        ($(#[$m:meta])* $name:ident) => {
            $(#[$m])*
            #[derive(Debug, Default, Clone)]
            pub struct $name {
                /// long-tail field passthrough; upstream realtime bridging is future work.
                pub raw: Value,
            }
        };
    }

    stub!(/// Storage collection; local build has no external storage to write.
        StorageInfoCollect);
    stub!(/// Upstream realtime bridging is future work.
        RealtimeParam);
}

// gw_consts::Protocol has no Default; give ModelParamV2's field a sensible one
// via a wrapper Default on the whole struct instead.
impl Default for ModelParamV2 {
    fn default() -> Self {
        Self {
            protocol: gw_consts::Protocol::OpenaiChat,
            model_name: String::new(),
            fallback_from: None,
            typed: None,
            raw: serde_json::Value::Null,
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
        assert!(a.api_key().is_none()); // unset env → back to mock path
    }
}
