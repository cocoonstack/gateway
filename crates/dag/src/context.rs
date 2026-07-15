//! Per-request DAG context.
//!
//! One mutable value threaded through every node of the four layers. Nodes read
//! what upstream nodes produced and write what downstream nodes need.

use std::sync::Arc;

use gw_config::GatewayConfig;
use gw_engines::{EngineOutcome, SharedTransport};
use gw_models::GatewayRequest;
use gw_state::{AkInfo, GatewayState};

pub struct DagContext {
    pub cfg: Arc<GatewayConfig>,
    pub state: Arc<GatewayState>,
    pub transport: SharedTransport,

    pub request: GatewayRequest,
    pub ak: AkInfo,

    /// engine result, set by the model_access layer.
    pub outcome: Option<EngineOutcome>,
    /// decision trail as (stage, detail); joined only when read, so the hot
    /// path allocates the detail once instead of a second joined string.
    pub decisions: Vec<(&'static str, String)>,
    /// Request-level cache hit (downstream nodes short-circuit on this and skip
    /// account/engine/billing).
    pub cache_hit: bool,
    /// This request's cache key (computed by cache_lookup, reused by cache_store).
    pub cache_key: Option<String>,
    /// Governance key for the (AK, model) daily counter — set by model_quota
    /// only when a cap is configured, consumed at billing time (unconfigured
    /// pairs never touch a counter).
    pub model_quota_key: Option<String>,
    /// Tokens reserved against the AK daily quota at admission; settled to
    /// actual usage at billing, refunded whole if the pipeline fails.
    pub quota_reserved: Option<i64>,
    /// Admission timestamp (unix secs), so the daily-quota settle/refund hits the
    /// same UTC-day bucket the reserve did even if the request crosses midnight.
    pub quota_at: i64,
    /// Tokens reserved in the AK TPM window at admission (same lifecycle).
    pub tpm_reserved: Option<i64>,
}

impl DagContext {
    pub fn new(
        cfg: Arc<GatewayConfig>,
        state: Arc<GatewayState>,
        transport: SharedTransport,
        request: GatewayRequest,
        ak: AkInfo,
    ) -> Self {
        Self {
            cfg,
            state,
            transport,
            request,
            ak,
            outcome: None,
            decisions: Vec::new(),
            cache_hit: false,
            cache_key: None,
            model_quota_key: None,
            quota_reserved: None,
            quota_at: 0,
            tpm_reserved: None,
        }
    }

    pub fn decide(&mut self, node: &'static str, what: impl Into<String>) {
        self.decisions.push((node, what.into()));
    }

    /// The effective end user: the key's `owner` (authoritative) else request
    /// metadata; `""` when neither is present. Resolution lives on [`AkInfo`] so
    /// REST and realtime can't diverge on an empty owner.
    pub fn effective_user_id(&self) -> &str {
        self.ak
            .attributed_user(self.request.user_id.as_deref().unwrap_or_default())
    }

    /// The decision trail as `"stage: detail"` lines.
    pub fn decision_lines(&self) -> impl Iterator<Item = String> + '_ {
        self.decisions.iter().map(|(n, w)| format!("{n}: {w}"))
    }
}
