//! Gateway configuration: parsing and validation of the YAML document. The
//! document may come from a local file or the Postgres config store (gw-state);
//! this crate only defines its shape. Layer L1 — depends only on gw-consts.

use gw_consts::Protocol;
use serde::Deserialize;

/// The implicit tenant for keys that don't declare one: unrestricted unless a
/// `tenants` entry named `default` gives it limits.
pub const DEFAULT_TENANT: &str = "default";

/// The repo's default config, embedded so tests and `cargo run` work with zero setup.
pub const DEFAULT_YAML: &str = include_str!("../../../conf/gateway.yaml");

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("read config file {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("parse config: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error("account `{account}` references unknown protocol `{wire}`")]
    UnknownProtocol { account: String, wire: String },
    #[error("model `{model}` references unknown protocol `{wire}`")]
    UnknownModelMapping { model: String, wire: String },
    #[error(
        "provider `{provider}` has unknown kind `{kind}` (known: openai, anthropic, gemini, deepseek, openrouter)"
    )]
    UnknownProviderKind { provider: String, kind: String },
    #[error("model `{model}` references unknown provider `{provider}`")]
    UnknownProvider { model: String, provider: String },
    #[error("model `{model}` needs either protocol or provider")]
    ModelNeedsDispatch { model: String },
    #[error("duplicate {kind} name `{name}`")]
    DuplicateName { kind: &'static str, name: String },
    #[error("{kind} with an empty name")]
    EmptyName { kind: &'static str },
    #[error("access key `{ak}` references undeclared tenant `{tenant}`")]
    UnknownTenant { ak: String, tenant: String },
    #[error("tenant `{tenant}` entitles unknown model `{model}`")]
    UnknownEntitledModel { tenant: String, model: String },
    #[error("`{owner}` sets a daily quota for unknown model `{model}`")]
    UnknownQuotaModel { owner: String, model: String },
    #[error("tenant `{tenant}` fallback model `{model}` is unknown or not entitled")]
    BadFallbackModel { tenant: String, model: String },
    #[error("`{owner}` sets a negative price")]
    NegativePrice { owner: String },
    #[error("storage.shared_cache needs storage.redis_url")]
    SharedCacheNeedsRedis,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Listen {
    pub host: String,
    pub port: u16,
}

/// One AK row of the local key table.
#[derive(Debug, Clone, Deserialize)]
pub struct AkConf {
    pub ak: String,
    pub product: String,
    /// Tenant this key belongs to; empty = the implicit `default` tenant.
    #[serde(default)]
    pub tenant: String,
    /// End user this key is issued to (one key = one user); `None` = shared key.
    #[serde(default)]
    pub owner: Option<String>,
    pub qps: f64,
    pub daily_token_quota: i64,
    /// tokens-per-minute window limit; None = unlimited.
    #[serde(default)]
    pub tokens_per_minute: Option<i64>,
    /// Unix seconds after which the key stops authenticating; None = never.
    #[serde(default)]
    pub expires_at_epoch_secs: Option<i64>,
    /// A banned key stays in the table but fails auth with a distinct 403.
    #[serde(default)]
    pub banned: bool,
    /// Per-model daily token caps for this key, overriding the tenant defaults.
    #[serde(default)]
    pub model_quotas: std::collections::HashMap<String, i64>,
}

/// Public model name → dispatch type + demo pricing + per-model governance.
#[derive(Debug, Clone, Deserialize)]
pub struct ModelConf {
    pub name: String,
    /// Engine dispatch wire type; may be omitted when `provider` implies it.
    #[serde(default)]
    pub protocol: String,
    /// Provider shorthand: fills protocol with the provider kind's default.
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub input_price_per_1k_micros: i64,
    #[serde(default)]
    pub output_price_per_1k_micros: i64,
    /// Model-level requests-per-minute limit; None = unlimited.
    #[serde(default)]
    pub qpm: Option<i64>,
    /// Request-level cache TTL; None = this model isn't cached.
    #[serde(default)]
    pub cache_ttl_seconds: Option<u64>,
}

impl ModelConf {
    pub fn protocol(&self) -> Option<Protocol> {
        Protocol::from_wire(&self.protocol)
    }
}

/// Upstream account slot (mock credentials unless a live endpoint is configured).
#[derive(Debug, Clone, Deserialize)]
pub struct AccountConf {
    pub name: String,
    pub provider: String,
    #[serde(default = "default_priority")]
    pub priority: i32,
    /// "ptu" (provisioned throughput, preferred) or "paygo" (default).
    #[serde(default)]
    pub tier: String,
    /// Upstream base URL; empty = mock:// (MockTransport). A real URL routes to
    /// the real endpoint — going live is a pure config change.
    #[serde(default)]
    pub endpoint: String,
    /// Env var name holding this account's API key (empty = mock credentials);
    /// read at request time, so real secrets never land in the config file.
    #[serde(default)]
    pub api_key_env: String,
    /// Upstream request timeout (seconds); unset = 60.
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
    /// Connect-phase retries before giving up; unset = 1. A request that
    /// reached the vendor is never replayed.
    #[serde(default)]
    pub connect_retries: Option<u32>,
    /// AWS SigV4 accounts: env var name holding the secret access key (paired with
    /// api_key_env = access key). Leave empty for non-AWS providers.
    #[serde(default)]
    pub secret_key_env: String,
    /// What this account's vendor charges us (margin accounting); zero = untracked.
    #[serde(default)]
    pub cost_input_price_per_1k_micros: i64,
    #[serde(default)]
    pub cost_output_price_per_1k_micros: i64,
    pub protocols: Vec<String>,
}

fn default_priority() -> i32 {
    1
}

/// What a fired content rule does. `block` denies the request; `flag` lets it
/// through but records the hit; `shadow` is `flag` for a rule under evaluation —
/// same recording, and the caller can tell trial rules apart when auditing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    #[default]
    Block,
    Flag,
    Shadow,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Action::Block => "block",
            Action::Flag => "flag",
            Action::Shadow => "shadow",
        }
    }
}

/// A named regex recognizer applied to the same inbound text the blocklist
/// scans. `pattern` is compiled once at config load into [`SecurityConf::regexes`].
#[derive(Debug, Clone, Deserialize)]
pub struct RegexRule {
    pub name: String,
    pub pattern: String,
    #[serde(default)]
    pub action: Action,
}

/// Local security policy (rule-based; no cloud security service). Lives globally
/// (`security:`) and per-tenant ([`TenantConf::security`]); the tenant's wins
/// whole when present.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SecurityConf {
    /// Blocklist terms; normalized to lower-case (empties dropped) at load.
    #[serde(default)]
    pub blocklist: Vec<String>,
    /// What a blocklist hit does (default: block).
    #[serde(default)]
    pub blocklist_action: Action,
    /// Whether to DLP-redact inbound/outbound content (emails/phone numbers).
    #[serde(default)]
    pub dlp_redact: bool,
    /// Detect API keys / credentials in inbound text and redact them.
    #[serde(default)]
    pub detect_secrets: bool,
    /// Route inbound text through the wired external moderator; needs a
    /// moderator plugged into the handler (the default one allows everything).
    #[serde(default)]
    pub moderate: bool,
    /// On a moderator error, admit the request (`true`) or deny it (`false`).
    #[serde(default)]
    pub moderation_fail_open: bool,
    /// Named regex recognizers.
    #[serde(default)]
    pub regex_rules: Vec<RegexRule>,
    /// Compiled `regex_rules`, built at config load (rules that fail to compile
    /// are dropped with a warning). Never deserialized.
    #[serde(skip)]
    pub regexes: Vec<CompiledRule>,
}

/// A compiled [`RegexRule`], ready to match on the hot path.
#[derive(Debug, Clone)]
pub struct CompiledRule {
    pub name: String,
    pub action: Action,
    pub re: regex::Regex,
}

/// Account stability policy (in-memory).
#[derive(Debug, Clone, Deserialize)]
pub struct StabilityConf {
    /// Enters cooldown after this many consecutive failures.
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: usize,
    /// Cooldown duration (seconds); auto-recovers on expiry.
    #[serde(default = "default_cooldown_seconds")]
    pub cooldown_seconds: u64,
}

fn default_failure_threshold() -> usize {
    3
}
fn default_cooldown_seconds() -> u64 {
    30
}

impl Default for StabilityConf {
    fn default() -> Self {
        Self {
            failure_threshold: default_failure_threshold(),
            cooldown_seconds: default_cooldown_seconds(),
        }
    }
}

/// Durable-record backend selection.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct StorageConf {
    /// Postgres URL for the fleet-shared ledger/files/batches store; takes
    /// precedence over `sqlite_path`. Empty = unused.
    #[serde(default)]
    pub postgres_url: String,
    /// SQLite database path for the ledger/files/batches store; empty = in-memory.
    #[serde(default)]
    pub sqlite_path: String,
    /// Redis URL for shared rate/quota governance across replicas; empty = in-process.
    #[serde(default)]
    pub redis_url: String,
    /// Share the response cache in Redis too (needs `redis_url`).
    #[serde(default)]
    pub shared_cache: bool,
    /// Keep at most this many billing records (oldest pruned first); 0 = unlimited.
    #[serde(default)]
    pub ledger_max_rows: u64,
}

/// Admin surface gate: `/admin/*` is disabled unless `token_env` names an env
/// var holding a bearer token. Keep the surface off the public LB regardless.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AdminConf {
    /// Env var holding the admin bearer token; empty = admin surface disabled.
    #[serde(default)]
    pub token_env: String,
}

impl AdminConf {
    /// The configured admin token, read from its env var at call time. `None`
    /// (unset var or no `token_env`) means the admin surface is disabled.
    pub fn token(&self) -> Option<String> {
        token_from_env(&self.token_env)
    }
}

/// Product-level governance.
#[derive(Debug, Clone, Deserialize)]
pub struct ProductConfEntry {
    pub name: String,
    /// Requests-per-minute limit; None = unlimited.
    #[serde(default)]
    pub qpm: Option<i64>,
}

/// A per-1k-token price pair (micro-dollars).
#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct PriceConf {
    #[serde(default)]
    pub input_price_per_1k_micros: i64,
    #[serde(default)]
    pub output_price_per_1k_micros: i64,
}

/// Tenant-level pooled governance and model entitlement. All of a tenant's
/// keys share one rate bucket; the entitlement gates which models they may call.
#[derive(Debug, Clone, Deserialize)]
pub struct TenantConf {
    pub name: String,
    /// Pooled requests-per-second across all the tenant's keys; None = unlimited.
    #[serde(default)]
    pub qps: Option<f64>,
    /// Models this tenant may call; None = every configured model.
    #[serde(default)]
    pub models: Option<Vec<String>>,
    /// Default per-model daily token caps, metered per key.
    #[serde(default)]
    pub model_quotas: std::collections::HashMap<String, i64>,
    /// Where an over-quota request degrades to instead of hard-failing;
    /// None = pass through unmetered (the per-AK daily cap still backstops).
    #[serde(default)]
    pub fallback_model: Option<String>,
    /// Env var holding this tenant's scoped admin token; empty = none.
    #[serde(default)]
    pub admin_token_env: String,
    /// Per-model charged-price overrides (else the model's list price applies).
    #[serde(default)]
    pub model_prices: std::collections::HashMap<String, PriceConf>,
    /// Per-user daily token budget (a soft cap keyed by end user); `None` =
    /// unlimited. Enforced only when the request carries a user attribution.
    #[serde(default)]
    pub user_daily_token_quota: Option<i64>,
    /// Content-safety policy for this tenant; `None` = use the global `security:`.
    #[serde(default)]
    pub security: Option<SecurityConf>,
    /// Prompt/response retention for this tenant; `None` = retain nothing.
    #[serde(default)]
    pub retention: Option<RetentionConf>,
}

impl TenantConf {
    /// The tenant admin token, read from its env var at call time.
    pub fn admin_token(&self) -> Option<String> {
        token_from_env(&self.admin_token_env)
    }
}

/// How much request/response content a tenant retains, and for how long.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentLevel {
    /// Store nothing (the default posture).
    None,
    /// Store the post-DLP redacted text (privacy-preserving audit).
    Redacted,
    /// Store the raw text (needs `GW_CONTENT_KEY` for at-rest encryption).
    Full,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct RetentionConf {
    pub content: ContentLevel,
    /// Days after which stored content is purged; 0 = keep until manually purged.
    #[serde(default)]
    pub days: u32,
}

/// First-class provider preset: `kind` fixes the endpoint, auth style, and
/// served wire types, so going live is `kind` + `api_key_env`.
#[derive(Debug, Clone, Deserialize)]
pub struct ProviderConf {
    pub name: String,
    /// openai | anthropic | gemini | deepseek | openrouter
    pub kind: String,
    #[serde(default)]
    pub api_key_env: String,
    #[serde(default)]
    pub secret_key_env: String,
    /// Override the preset base URL (e.g. an OpenAI-compatible vendor).
    #[serde(default)]
    pub endpoint: String,
    /// Upstream request timeout (seconds); unset = 60.
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
    /// Connect-phase retries; unset = 1.
    #[serde(default)]
    pub connect_retries: Option<u32>,
}

struct ProviderPreset {
    endpoint: &'static str,
    wires: &'static [&'static str],
    default_model_wire: &'static str,
}

fn provider_preset(kind: &str) -> Option<ProviderPreset> {
    Some(match kind {
        "openai" => ProviderPreset {
            endpoint: "https://api.openai.com",
            wires: &[
                "openai-chat",
                "embeddings",
                "image",
                "tts",
                "stt",
                "responses",
                "completions",
                "realtime",
            ],
            default_model_wire: "openai-chat",
        },
        "anthropic" => ProviderPreset {
            endpoint: "https://api.anthropic.com",
            wires: &["anthropic-messages"],
            default_model_wire: "anthropic-messages",
        },
        "gemini" => ProviderPreset {
            endpoint: "https://generativelanguage.googleapis.com",
            wires: &["gemini"],
            default_model_wire: "gemini",
        },
        // OpenAI-protocol vendors: same wire shape, different base URL.
        "deepseek" => ProviderPreset {
            endpoint: "https://api.deepseek.com",
            wires: &["openai-chat"],
            default_model_wire: "openai-chat",
        },
        "openrouter" => ProviderPreset {
            endpoint: "https://openrouter.ai/api",
            wires: &["openai-chat"],
            default_model_wire: "openai-chat",
        },
        _ => return None,
    })
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayConfig {
    pub listen: Listen,
    #[serde(default)]
    pub access_keys: Vec<AkConf>,
    #[serde(default)]
    pub models: Vec<ModelConf>,
    #[serde(default)]
    pub accounts: Vec<AccountConf>,
    #[serde(default)]
    pub security: SecurityConf,
    #[serde(default)]
    pub stability: StabilityConf,
    #[serde(default)]
    pub products: Vec<ProductConfEntry>,
    #[serde(default)]
    pub tenants: Vec<TenantConf>,
    #[serde(default)]
    pub storage: StorageConf,
    /// First-class provider presets; each expands into an upstream account.
    #[serde(default)]
    pub providers: Vec<ProviderConf>,
    /// Admin surface gate (dynamic config reload / key management).
    #[serde(default)]
    pub admin: AdminConf,
    /// Trust `x-real-ip` / `x-forwarded-for` for the audit source IP. Off by
    /// default: the audit records the real TCP peer, which a client can't forge.
    /// Enable only when a trusted proxy fronts the gateway and sets those headers.
    #[serde(default)]
    pub trust_proxy_headers: bool,
    /// Stable hash of the source document; see [`Self::generation`].
    #[serde(skip)]
    generation: u64,
    /// name → index lookups, built once after parse to avoid per-request scans.
    #[serde(skip)]
    model_idx: std::collections::HashMap<String, usize>,
    #[serde(skip)]
    ak_idx: std::collections::HashMap<String, usize>,
    #[serde(skip)]
    product_idx: std::collections::HashMap<String, usize>,
    #[serde(skip)]
    tenant_idx: std::collections::HashMap<String, usize>,
}

impl GatewayConfig {
    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        let mut cfg: GatewayConfig = serde_yaml::from_str(yaml)?;
        cfg.normalize()?;
        cfg.validate()?;
        cfg.build_indices();
        // a hash of the document, not a per-process counter: every replica must
        // agree on it, and a restarted node must not hit stale cache entries
        cfg.generation = stable_hash(yaml);
        Ok(cfg)
    }

    fn build_indices(&mut self) {
        self.model_idx = index_by(&self.models, |m| &m.name);
        self.ak_idx = index_by(&self.access_keys, |a| &a.ak);
        self.product_idx = index_by(&self.products, |p| &p.name);
        self.tenant_idx = index_by(&self.tenants, |t| &t.name);
    }

    /// Expand provider presets: fill each model's default wire type and
    /// synthesize an account per provider (explicit same-name accounts win).
    fn normalize(&mut self) -> Result<(), ConfigError> {
        for k in &mut self.access_keys {
            if k.tenant.is_empty() {
                k.tenant = DEFAULT_TENANT.to_owned();
            }
        }
        for m in &mut self.models {
            if !m.protocol.is_empty() {
                continue;
            }
            let Some(pname) = m.provider.as_deref() else {
                return Err(ConfigError::ModelNeedsDispatch {
                    model: m.name.clone(),
                });
            };
            let provider = self
                .providers
                .iter()
                .find(|p| p.name == pname)
                .ok_or_else(|| ConfigError::UnknownProvider {
                    model: m.name.clone(),
                    provider: pname.to_owned(),
                })?;
            let preset = provider_preset(&provider.kind).ok_or_else(|| {
                ConfigError::UnknownProviderKind {
                    provider: provider.name.clone(),
                    kind: provider.kind.clone(),
                }
            })?;
            m.protocol = preset.default_model_wire.to_owned();
        }
        for p in &self.providers {
            let preset =
                provider_preset(&p.kind).ok_or_else(|| ConfigError::UnknownProviderKind {
                    provider: p.name.clone(),
                    kind: p.kind.clone(),
                })?;
            if self.accounts.iter().any(|a| a.name == p.name) {
                continue;
            }
            self.accounts.push(AccountConf {
                name: p.name.clone(),
                provider: p.name.clone(),
                priority: 1,
                tier: String::new(),
                cost_input_price_per_1k_micros: 0,
                cost_output_price_per_1k_micros: 0,
                timeout_seconds: p.timeout_seconds,
                connect_retries: p.connect_retries,
                endpoint: if p.endpoint.is_empty() {
                    preset.endpoint.to_owned()
                } else {
                    p.endpoint.clone()
                },
                api_key_env: p.api_key_env.clone(),
                secret_key_env: p.secret_key_env.clone(),
                protocols: preset.wires.iter().map(|w| (*w).to_owned()).collect(),
            });
        }
        // normalize the global policy and every tenant override once at load
        compile_security(&mut self.security);
        for t in &mut self.tenants {
            if let Some(sec) = t.security.as_mut() {
                compile_security(sec);
            }
        }
        Ok(())
    }

    pub fn load(path: &str) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_owned(),
            source,
        })?;
        Self::from_yaml(&text)
    }

    /// The embedded conf/gateway.yaml.
    pub fn embedded_default() -> Result<Self, ConfigError> {
        Self::from_yaml(DEFAULT_YAML)
    }

    /// Every wire string must resolve to a known Protocol up front.
    fn validate(&self) -> Result<(), ConfigError> {
        if self.storage.shared_cache && self.storage.redis_url.is_empty() {
            return Err(ConfigError::SharedCacheNeedsRedis);
        }
        // negative prices would make cost accounting non-monotonic (the usage
        // rollup's max-upsert relies on per-column monotone sums)
        let neg = |i: i64, o: i64| i < 0 || o < 0;
        for m in &self.models {
            if neg(m.input_price_per_1k_micros, m.output_price_per_1k_micros) {
                return Err(ConfigError::NegativePrice {
                    owner: format!("model {}", m.name),
                });
            }
        }
        for a in &self.accounts {
            if neg(
                a.cost_input_price_per_1k_micros,
                a.cost_output_price_per_1k_micros,
            ) {
                return Err(ConfigError::NegativePrice {
                    owner: format!("account {}", a.name),
                });
            }
        }
        for t in &self.tenants {
            for (model, p) in &t.model_prices {
                if neg(p.input_price_per_1k_micros, p.output_price_per_1k_micros) {
                    return Err(ConfigError::NegativePrice {
                        owner: format!("tenant {} price for {model}", t.name),
                    });
                }
            }
        }
        for m in &self.models {
            if m.protocol().is_none() {
                return Err(ConfigError::UnknownModelMapping {
                    model: m.name.clone(),
                    wire: m.protocol.clone(),
                });
            }
        }
        for a in &self.accounts {
            for w in &a.protocols {
                if Protocol::from_wire(w).is_none() {
                    return Err(ConfigError::UnknownProtocol {
                        account: a.name.clone(),
                        wire: w.clone(),
                    });
                }
            }
        }
        check_unique("model", self.models.iter().map(|m| m.name.as_str()))?;
        check_unique("access_key", self.access_keys.iter().map(|a| a.ak.as_str()))?;
        check_unique("product", self.products.iter().map(|p| p.name.as_str()))?;
        check_unique("provider", self.providers.iter().map(|p| p.name.as_str()))?;
        check_unique("tenant", self.tenants.iter().map(|t| t.name.as_str()))?;
        // a colon in a tenant name would alias another tenant's `ub:{tenant}:{user}` budget key
        for t in &self.tenants {
            if t.name.contains(':') {
                return Err(ConfigError::DuplicateName {
                    kind: "tenant (':' not allowed in name)",
                    name: t.name.clone(),
                });
            }
        }
        // a typo'd tenant would silently fall back to the unrestricted default — reject at load
        for k in &self.access_keys {
            if !self.is_known_tenant(&k.tenant) {
                return Err(ConfigError::UnknownTenant {
                    ak: k.ak.clone(),
                    tenant: k.tenant.clone(),
                });
            }
        }
        for t in &self.tenants {
            for m in t.models.iter().flatten() {
                if !self.model_exists(m) {
                    return Err(ConfigError::UnknownEntitledModel {
                        tenant: t.name.clone(),
                        model: m.clone(),
                    });
                }
            }
            self.check_models_known(format!("tenant {}", t.name), t.model_quotas.keys())?;
            self.check_models_known(
                format!("tenant {} (model_prices)", t.name),
                t.model_prices.keys(),
            )?;
            if let Some(fb) = &t.fallback_model
                && (!self.model_exists(fb)
                    || !t.models.as_ref().is_none_or(|allow| allow.contains(fb)))
            {
                return Err(ConfigError::BadFallbackModel {
                    tenant: t.name.clone(),
                    model: fb.clone(),
                });
            }
        }
        for k in &self.access_keys {
            self.check_models_known(format!("access key {}", k.ak), k.model_quotas.keys())?;
        }
        // health/failover key by name — a duplicate would cool down the wrong account
        check_unique("account", self.accounts.iter().map(|a| a.name.as_str()))?;
        Ok(())
    }

    /// Config generation (a stable hash of the source document); mixed into
    /// cache keys so a reload to a different config can't serve a stale entry.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn find_product(&self, name: &str) -> Option<&ProductConfEntry> {
        self.products.get(*self.product_idx.get(name)?)
    }

    pub fn find_tenant(&self, name: &str) -> Option<&TenantConf> {
        self.tenants.get(*self.tenant_idx.get(name)?)
    }

    /// Whether keys may reference `name` as their tenant. A linear scan so it
    /// also works during `validate()`, before the indices are built.
    pub fn is_known_tenant(&self, name: &str) -> bool {
        name == DEFAULT_TENANT || self.tenants.iter().any(|t| t.name == name)
    }

    fn model_exists(&self, name: &str) -> bool {
        self.models.iter().any(|c| c.name == name)
    }

    /// Every name in `names` must be a configured model, else the quota/price
    /// entry under `owner` is a typo that would silently never apply.
    fn check_models_known<'a>(
        &self,
        owner: String,
        names: impl Iterator<Item = &'a String>,
    ) -> Result<(), ConfigError> {
        for m in names {
            if !self.model_exists(m) {
                return Err(ConfigError::UnknownQuotaModel {
                    owner,
                    model: m.clone(),
                });
            }
        }
        Ok(())
    }

    /// The effective content-safety policy for `tenant`: the tenant's own
    /// override when present, else the global `security:`.
    pub fn security_for(&self, tenant: &str) -> &SecurityConf {
        self.find_tenant(tenant)
            .and_then(|t| t.security.as_ref())
            .unwrap_or(&self.security)
    }

    /// The retention policy for `tenant`, if it configured one.
    pub fn retention_for(&self, tenant: &str) -> Option<&RetentionConf> {
        self.find_tenant(tenant).and_then(|t| t.retention.as_ref())
    }

    /// Whether `tenant` may call `model`: a declared tenant without an
    /// allowlist (and the implicit default) allows every model. An undeclared
    /// non-default tenant fails closed — a runtime key outliving the reload
    /// that deleted its tenant loses entitlement, not becomes unrestricted.
    pub fn tenant_allows_model(&self, tenant: &str, model: &str) -> bool {
        match self.find_tenant(tenant) {
            Some(t) => t
                .models
                .as_ref()
                .is_none_or(|allow| allow.iter().any(|m| m == model)),
            None => tenant == DEFAULT_TENANT,
        }
    }

    pub fn find_ak(&self, ak: &str) -> Option<&AkConf> {
        self.access_keys.get(*self.ak_idx.get(ak)?)
    }

    pub fn find_model(&self, name: &str) -> Option<&ModelConf> {
        self.models.get(*self.model_idx.get(name)?)
    }

    /// Pricing for a public model name; zero if unlisted.
    pub fn prices_for(&self, name: &str) -> (i64, i64) {
        self.find_model(name)
            .map(|m| (m.input_price_per_1k_micros, m.output_price_per_1k_micros))
            .unwrap_or((0, 0))
    }

    /// Charged pricing for `tenant` on `model`: the tenant's override when one
    /// is configured, else the model's list price.
    pub fn prices_for_tenant(&self, tenant: &str, model: &str) -> (i64, i64) {
        if let Some(p) = self
            .find_tenant(tenant)
            .and_then(|t| t.model_prices.get(model))
        {
            return (p.input_price_per_1k_micros, p.output_price_per_1k_micros);
        }
        self.prices_for(model)
    }
}

/// Normalize a security policy at load: lower-case the blocklist (so scans
/// don't rebuild it per request) and compile the regex rules (dropping any that
/// fail to compile, loudly).
fn compile_security(sec: &mut SecurityConf) {
    sec.blocklist = sec
        .blocklist
        .iter()
        .filter(|w| !w.is_empty())
        .map(|w| w.to_lowercase())
        .collect();
    sec.regexes = sec
        .regex_rules
        .iter()
        .filter_map(|r| match regex::Regex::new(&r.pattern) {
            Ok(re) => Some(CompiledRule {
                name: r.name.clone(),
                action: r.action,
                re,
            }),
            Err(e) => {
                tracing::warn!(rule = %r.name, error = %e, "dropping uncompilable regex rule");
                None
            }
        })
        .collect();
}

/// A token read from the named env var at call time; `None` when the name is
/// empty or the var is unset/empty.
fn token_from_env(var: &str) -> Option<String> {
    if var.is_empty() {
        return None;
    }
    std::env::var(var).ok().filter(|t| !t.is_empty())
}

/// Deterministic hash of the config document, stable across processes.
fn stable_hash(yaml: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::hash::DefaultHasher::new();
    yaml.hash(&mut h);
    h.finish()
}

/// Build a name → slot-index map for O(1) lookups.
fn index_by<T>(items: &[T], key: impl Fn(&T) -> &str) -> std::collections::HashMap<String, usize> {
    items
        .iter()
        .enumerate()
        .map(|(i, x)| (key(x).to_owned(), i))
        .collect()
}

/// Reject duplicate and empty names: lookups are last-wins (a duplicate is
/// ambiguous) and an empty name is unreferenceable.
fn check_unique<'a>(
    kind: &'static str,
    names: impl Iterator<Item = &'a str>,
) -> Result<(), ConfigError> {
    let mut seen = std::collections::HashSet::new();
    for name in names {
        if name.is_empty() {
            return Err(ConfigError::EmptyName { kind });
        }
        if !seen.insert(name) {
            return Err(ConfigError::DuplicateName {
                kind,
                name: name.to_owned(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_is_stable_per_document_and_changes_on_edit() {
        let a = "listen: {host: h, port: 1}\nmodels: [{name: m, protocol: openai-chat}]";
        let b = "listen: {host: h, port: 1}\nmodels: [{name: m2, protocol: openai-chat}]";
        let g1 = GatewayConfig::from_yaml(a).unwrap().generation();
        let g2 = GatewayConfig::from_yaml(a).unwrap().generation();
        let g3 = GatewayConfig::from_yaml(b).unwrap().generation();
        assert_eq!(g1, g2, "same document → same generation (fleet-stable)");
        assert_ne!(g1, g3, "a changed document → a different generation");
    }

    const PROVIDER_YAML: &str = r#"
listen: {host: 127.0.0.1, port: 0}
providers:
  - {name: openai, kind: openai, api_key_env: OPENAI_API_KEY}
  - {name: anthropic, kind: anthropic, api_key_env: ANTHROPIC_API_KEY}
  - {name: gemini, kind: gemini, api_key_env: GEMINI_API_KEY, endpoint: "https://gw.example.com"}
models:
  - {name: gpt-x, provider: openai}
  - {name: claude-x, provider: anthropic}
  - {name: gemini-x, provider: gemini, protocol: gemini}
"#;

    #[test]
    fn openai_protocol_vendors_expand() {
        let yaml = r#"
listen: {host: 127.0.0.1, port: 0}
providers:
  - {name: deepseek, kind: deepseek, api_key_env: DEEPSEEK_KEY}
  - {name: openrouter, kind: openrouter, api_key_env: OPENROUTER_KEY}
models:
  - {name: deepseek-chat, provider: deepseek}
  - {name: some-model, provider: openrouter}
"#;
        let cfg = GatewayConfig::from_yaml(yaml).unwrap();
        assert_eq!(
            cfg.find_model("deepseek-chat").unwrap().protocol(),
            Some(Protocol::OpenaiChat)
        );
        let ds = cfg.accounts.iter().find(|a| a.name == "deepseek").unwrap();
        assert_eq!(ds.endpoint, "https://api.deepseek.com");
        let orr = cfg
            .accounts
            .iter()
            .find(|a| a.name == "openrouter")
            .unwrap();
        assert_eq!(orr.endpoint, "https://openrouter.ai/api");
    }

    #[test]
    fn provider_presets_expand_to_accounts_and_model_defaults() {
        let cfg = GatewayConfig::from_yaml(PROVIDER_YAML).unwrap();
        assert_eq!(
            cfg.find_model("gpt-x").unwrap().protocol(),
            Some(Protocol::OpenaiChat)
        );
        assert_eq!(
            cfg.find_model("claude-x").unwrap().protocol(),
            Some(Protocol::AnthropicMessages)
        );
        let acc = cfg.accounts.iter().find(|a| a.name == "openai").unwrap();
        assert_eq!(acc.endpoint, "https://api.openai.com");
        assert_eq!(acc.api_key_env, "OPENAI_API_KEY");
        assert!(acc.protocols.iter().any(|w| w == "embeddings"));
        let gem = cfg.accounts.iter().find(|a| a.name == "gemini").unwrap();
        assert_eq!(gem.endpoint, "https://gw.example.com");
    }

    #[test]
    fn duplicate_names_are_rejected() {
        let yaml = r#"
listen: {host: h, port: 1}
models:
  - {name: dup, protocol: openai-chat}
  - {name: dup, protocol: openai-chat}
"#;
        assert!(matches!(
            GatewayConfig::from_yaml(yaml),
            Err(ConfigError::DuplicateName { kind: "model", .. })
        ));
    }

    #[test]
    fn duplicate_account_and_provider_names_rejected() {
        let dup_account = r#"
listen: {host: h, port: 1}
accounts:
  - {name: same, provider: a, protocols: ["openai-chat"]}
  - {name: same, provider: b, protocols: ["openai-chat"]}
"#;
        assert!(matches!(
            GatewayConfig::from_yaml(dup_account),
            Err(ConfigError::DuplicateName {
                kind: "account",
                ..
            })
        ));
        let dup_provider = r#"
listen: {host: h, port: 1}
providers:
  - {name: openai, kind: openai}
  - {name: openai, kind: deepseek}
"#;
        assert!(matches!(
            GatewayConfig::from_yaml(dup_provider),
            Err(ConfigError::DuplicateName {
                kind: "provider",
                ..
            })
        ));
    }

    #[test]
    fn provider_errors() {
        let bad_kind = "providers: [{name: x, kind: nope}]
listen: {host: h, port: 1}";
        assert!(matches!(
            GatewayConfig::from_yaml(bad_kind),
            Err(ConfigError::UnknownProviderKind { .. })
        ));
        let no_dispatch = "models: [{name: m}]
listen: {host: h, port: 1}";
        assert!(matches!(
            GatewayConfig::from_yaml(no_dispatch),
            Err(ConfigError::ModelNeedsDispatch { .. })
        ));
        let bad_provider = "models: [{name: m, provider: ghost}]
listen: {host: h, port: 1}";
        assert!(matches!(
            GatewayConfig::from_yaml(bad_provider),
            Err(ConfigError::UnknownProvider { .. })
        ));
    }

    #[test]
    fn upstream_policy_fields_parse_and_inherit() {
        let yaml = r#"
listen: {host: h, port: 1}
providers:
  - {name: openai, kind: openai, timeout_seconds: 30, connect_retries: 3}
accounts:
  - {name: slow, provider: x, priority: 1, protocols: ["openai-chat"], timeout_seconds: 120}
"#;
        let cfg = GatewayConfig::from_yaml(yaml).unwrap();
        let slow = cfg.accounts.iter().find(|a| a.name == "slow").unwrap();
        assert_eq!(slow.timeout_seconds, Some(120));
        assert_eq!(slow.connect_retries, None);
        let preset = cfg.accounts.iter().find(|a| a.name == "openai").unwrap();
        assert_eq!(preset.timeout_seconds, Some(30));
        assert_eq!(preset.connect_retries, Some(3));
    }

    #[test]
    fn explicit_account_wins_over_preset() {
        let yaml = r#"
listen: {host: h, port: 1}
providers: [{name: openai, kind: openai}]
accounts:
  - {name: openai, provider: openai, priority: 5, protocols: ["openai-chat"], endpoint: "https://mine.example.com"}
"#;
        let cfg = GatewayConfig::from_yaml(yaml).unwrap();
        let matching: Vec<_> = cfg.accounts.iter().filter(|a| a.name == "openai").collect();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].endpoint, "https://mine.example.com");
    }

    #[test]
    fn embedded_default_parses_and_validates() {
        let cfg = GatewayConfig::embedded_default().unwrap();
        assert_eq!(cfg.listen.port, 8080);
        assert!(cfg.find_ak("ak-demo-123").is_some());
        let m = cfg.find_model("gpt-4o").unwrap();
        assert_eq!(m.protocol(), Some(Protocol::OpenaiChat));
        assert_eq!(cfg.prices_for("claude-sonnet"), (3000, 15000));
    }

    #[test]
    fn blocklist_normalized_lowercase_at_load() {
        let yaml = r#"
listen: {host: h, port: 1}
security: {blocklist: ["Example.COM", "", "  BadWord "]}
providers: []
models: []
accounts: []
access_keys: []
"#;
        let cfg = GatewayConfig::from_yaml(yaml).unwrap();
        assert_eq!(cfg.security.blocklist, vec!["example.com", "  badword "]);
    }

    #[test]
    fn tenant_validation_and_entitlement() {
        let yaml = r#"
listen: {host: h, port: 1}
models: [{name: m1, protocol: openai-chat}, {name: m2, protocol: openai-chat}]
tenants: [{name: t1, qps: 5, models: [m1]}]
access_keys:
  - {ak: k1, tenant: t1, product: p, qps: 1, daily_token_quota: 10}
  - {ak: k2, product: p, qps: 1, daily_token_quota: 10}
"#;
        let cfg = GatewayConfig::from_yaml(yaml).unwrap();
        assert_eq!(cfg.find_ak("k2").unwrap().tenant, DEFAULT_TENANT);
        assert!(cfg.tenant_allows_model("t1", "m1"));
        assert!(!cfg.tenant_allows_model("t1", "m2"));
        assert!(cfg.tenant_allows_model(DEFAULT_TENANT, "m2"));
        assert!(
            !cfg.tenant_allows_model("ghost", "m1"),
            "an undeclared (deleted) tenant must fail closed"
        );
        assert_eq!(cfg.find_tenant("t1").unwrap().qps, Some(5.0));

        let undeclared = r#"
listen: {host: h, port: 1}
models: [{name: m1, protocol: openai-chat}]
access_keys: [{ak: k1, tenant: ghost, product: p, qps: 1, daily_token_quota: 10}]
"#;
        assert!(matches!(
            GatewayConfig::from_yaml(undeclared),
            Err(ConfigError::UnknownTenant { .. })
        ));

        let bad_model = r#"
listen: {host: h, port: 1}
models: [{name: m1, protocol: openai-chat}]
tenants: [{name: t1, models: [nope]}]
"#;
        assert!(matches!(
            GatewayConfig::from_yaml(bad_model),
            Err(ConfigError::UnknownEntitledModel { .. })
        ));

        let dup = r#"
listen: {host: h, port: 1}
tenants: [{name: t1}, {name: t1}]
"#;
        assert!(matches!(
            GatewayConfig::from_yaml(dup),
            Err(ConfigError::DuplicateName { kind: "tenant", .. })
        ));

        let neg_price = "listen: {host: h, port: 1}\nmodels: [{name: m1, protocol: openai-chat, input_price_per_1k_micros: -1}]";
        assert!(
            matches!(
                GatewayConfig::from_yaml(neg_price),
                Err(ConfigError::NegativePrice { .. })
            ),
            "negative prices are rejected at load"
        );

        let colon = "listen: {host: h, port: 1}\ntenants: [{name: 'a:b'}]";
        assert!(
            GatewayConfig::from_yaml(colon).is_err(),
            "a colon in a tenant name is rejected (budget-key aliasing)"
        );

        let bad_quota = r#"
listen: {host: h, port: 1}
models: [{name: m1, protocol: openai-chat}]
tenants: [{name: t1, model_quotas: {ghost: 10}}]
"#;
        assert!(matches!(
            GatewayConfig::from_yaml(bad_quota),
            Err(ConfigError::UnknownQuotaModel { .. })
        ));

        let bad_fallback = r#"
listen: {host: h, port: 1}
models: [{name: m1, protocol: openai-chat}, {name: m2, protocol: openai-chat}]
tenants: [{name: t1, models: [m1], fallback_model: m2}]
"#;
        assert!(matches!(
            GatewayConfig::from_yaml(bad_fallback),
            Err(ConfigError::BadFallbackModel { .. })
        ));
    }

    #[test]
    fn shared_cache_requires_redis() {
        let bad = "listen: {host: h, port: 1}\nstorage: {shared_cache: true}";
        assert!(matches!(
            GatewayConfig::from_yaml(bad),
            Err(ConfigError::SharedCacheNeedsRedis)
        ));
        let ok =
            "listen: {host: h, port: 1}\nstorage: {shared_cache: true, redis_url: 'redis://x'}";
        assert!(GatewayConfig::from_yaml(ok).is_ok());
    }

    #[test]
    fn tenant_price_override_resolution() {
        let yaml = r#"
listen: {host: h, port: 1}
models: [{name: m1, protocol: openai-chat, input_price_per_1k_micros: 10, output_price_per_1k_micros: 20}]
tenants: [{name: t1, model_prices: {m1: {input_price_per_1k_micros: 30, output_price_per_1k_micros: 60}}}]
"#;
        let cfg = GatewayConfig::from_yaml(yaml).unwrap();
        assert_eq!(cfg.prices_for_tenant("t1", "m1"), (30, 60));
        assert_eq!(cfg.prices_for_tenant("default", "m1"), (10, 20));

        let bad = r#"
listen: {host: h, port: 1}
models: [{name: m1, protocol: openai-chat}]
tenants: [{name: t1, model_prices: {ghost: {input_price_per_1k_micros: 1}}}]
"#;
        assert!(matches!(
            GatewayConfig::from_yaml(bad),
            Err(ConfigError::UnknownQuotaModel { .. })
        ));
    }

    #[test]
    fn bad_protocol_rejected() {
        let yaml = r#"
listen: { host: 127.0.0.1, port: 1 }
models: [{ name: x, protocol: nope }]
"#;
        assert!(matches!(
            GatewayConfig::from_yaml(yaml),
            Err(ConfigError::UnknownModelMapping { .. })
        ));
    }
}
