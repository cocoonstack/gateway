//! Per-request DAG context.
//!
//! One mutable value threaded through every node of the four layers. Nodes read
//! what upstream nodes produced and write what downstream nodes need.

use std::sync::Arc;

use gw_config::GatewayConfig;
use gw_engines::{EngineOutcome, SharedTransport};
use gw_models::{GResult, GatewayError, GatewayRequest, ModelParamV2};
use gw_state::admission::TpmReserve;
use gw_state::{AkInfo, GatewayState, UserBudget};

pub struct DagContext {
    pub cfg: Arc<GatewayConfig>,
    pub state: Arc<GatewayState>,
    pub transport: SharedTransport,

    pub request: GatewayRequest,
    pub ak: Arc<AkInfo>,

    /// engine result, set by the model_access layer.
    pub outcome: Option<EngineOutcome>,
    /// Decision trail as (stage, detail), joined only when read.
    pub decisions: Vec<(&'static str, String)>,
    /// Request-level cache hit: later nodes skip account, engine and billing.
    pub cache_hit: bool,
    /// A fallback model retries this attempt's upstream fault, so its failure is not the outcome.
    pub fallback_ahead: bool,
    pub(crate) request_limits_admitted: bool,
    /// This request's cache key (computed by cache_lookup, reused by cache_store).
    pub cache_key: Option<String>,
    /// Governance key for the (AK, model) daily counter; set only when a cap is configured.
    pub model_quota_key: Option<String>,
    /// Tokens recorded against the AK daily quota; settled at billing, refunded on failure.
    pub quota_reserved: Option<i64>,
    /// Admission unix secs, so the daily settle or refund lands in the reserve's UTC-day bucket.
    pub quota_at: i64,
    /// Tokens reserved in the AK TPM window at admission (same lifecycle).
    pub tpm_reserved: Option<TpmReserve>,
    /// The user's budget override resolved at admission, charged at settlement.
    pub user_budget: Option<UserBudget>,
    /// Outbound DLP buffered this stream; billing waits for the view's delivery result.
    pub billing_deferred: bool,
}

impl DagContext {
    pub fn new(
        cfg: Arc<GatewayConfig>,
        state: Arc<GatewayState>,
        transport: SharedTransport,
        request: GatewayRequest,
        ak: Arc<AkInfo>,
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
            fallback_ahead: false,
            request_limits_admitted: false,
            cache_key: None,
            model_quota_key: None,
            quota_reserved: None,
            quota_at: 0,
            tpm_reserved: None,
            user_budget: None,
            billing_deferred: false,
        }
    }

    pub fn decide(&mut self, node: &'static str, what: impl Into<String>) {
        self.decisions.push((node, what.into()));
    }

    /// The resolved model param; a node reaching here before resolve_model is a broken plan.
    pub fn model_param(&self) -> GResult<&ModelParamV2> {
        self.request
            .model_param_v2
            .as_ref()
            .ok_or_else(|| GatewayError::internal("model param missing after resolve_model"))
    }

    /// The effective end user: the key's `owner`, else request metadata, else `""`.
    pub fn effective_user_id(&self) -> &str {
        self.ak
            .attributed_user(self.request.user_id.as_deref().unwrap_or_default())
    }

    /// The decision trail as one `"stage: detail; …"` line.
    pub fn decisions_line(&self) -> String {
        let mut out = String::new();
        for (n, w) in &self.decisions {
            if !out.is_empty() {
                out.push_str("; ");
            }
            out.push_str(n);
            out.push_str(": ");
            out.push_str(w);
        }
        out
    }
}
