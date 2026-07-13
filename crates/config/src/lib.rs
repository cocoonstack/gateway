//! Local file configuration.
//!
//! There is no config center: everything comes from a local YAML file. Layer L1 —
//! depends only on gw-consts.

use gw_consts::Protocol;
use serde::Deserialize;

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
}

/// Build a name → slot-index map for O(1) lookups.
fn index_by<T>(items: &[T], key: impl Fn(&T) -> &str) -> std::collections::HashMap<String, usize> {
    items
        .iter()
        .enumerate()
        .map(|(i, x)| (key(x).to_owned(), i))
        .collect()
}

/// Reject duplicate names — name lookups are last-wins, so a duplicate is an
/// ambiguous config that should fail at load, not silently pick one entry.
fn check_unique<'a>(
    kind: &'static str,
    names: impl Iterator<Item = &'a str>,
) -> Result<(), ConfigError> {
    let mut seen = std::collections::HashSet::new();
    for name in names {
        if !seen.insert(name) {
            return Err(ConfigError::DuplicateName {
                kind,
                name: name.to_owned(),
            });
        }
    }
    Ok(())
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
    /// requests per second allowed for this AK.
    pub qps: f64,
    /// daily token budget for this AK.
    pub daily_token_quota: i64,
    /// tokens-per-minute window limit; None = unlimited.
    #[serde(default)]
    pub tokens_per_minute: Option<i64>,
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
    /// Upstream base URL; empty = the engine uses the mock:// sentinel (default,
    /// routes through MockTransport). A real URL routes to the real endpoint
    /// (HttpTransport) — going live is a pure config change.
    #[serde(default)]
    pub endpoint: String,
    /// Env var name holding this account's API key (empty = use mock credentials).
    /// Real secrets never land in the config file; the process reads the env var
    /// by this name at request time.
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
    pub protocols: Vec<String>,
}

fn default_priority() -> i32 {
    1
}

/// Local security policy (rule-based; no cloud security service).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SecurityConf {
    /// Blocklist: a hit triggers Block.
    #[serde(default)]
    pub blocklist: Vec<String>,
    /// Whether to DLP-redact inbound/outbound content (emails/phone numbers).
    #[serde(default)]
    pub dlp_redact: bool,
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
    /// SQLite database path for the ledger/files/batches store; empty = in-memory.
    #[serde(default)]
    pub sqlite_path: String,
    /// Redis URL for shared rate/quota governance across replicas; empty = in-process.
    #[serde(default)]
    pub redis_url: String,
    /// Keep at most this many billing records (oldest pruned first); 0 = unlimited.
    #[serde(default)]
    pub ledger_max_rows: u64,
}

/// Admin surface gate. `/admin/*` is disabled unless `token_env` names an env
/// var holding a bearer token; requests must present it. Keep the admin surface
/// off the public load balancer regardless (bind it to a private network).
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
        if self.token_env.is_empty() {
            return None;
        }
        std::env::var(&self.token_env)
            .ok()
            .filter(|t| !t.is_empty())
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

/// First-class provider preset: `kind` fixes the endpoint, auth style, and
/// served wire types, so going live is `kind` + `api_key_env`.
#[derive(Debug, Clone, Deserialize)]
pub struct ProviderConf {
    pub name: String,
    /// openai | anthropic | gemini
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
    pub storage: StorageConf,
    /// First-class provider presets; each expands into an upstream account.
    #[serde(default)]
    pub providers: Vec<ProviderConf>,
    /// Admin surface gate (dynamic config reload / key management).
    #[serde(default)]
    pub admin: AdminConf,
    /// name → index lookups, built once after parse to avoid per-request scans.
    #[serde(skip)]
    model_idx: std::collections::HashMap<String, usize>,
    #[serde(skip)]
    ak_idx: std::collections::HashMap<String, usize>,
    #[serde(skip)]
    product_idx: std::collections::HashMap<String, usize>,
}

/// The repo's default config, embedded so tests and `cargo run` work with zero setup.
pub const DEFAULT_YAML: &str = include_str!("../../../conf/gateway.yaml");

impl GatewayConfig {
    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        let mut cfg: GatewayConfig = serde_yaml::from_str(yaml)?;
        cfg.normalize()?;
        cfg.validate()?;
        cfg.build_indices();
        Ok(cfg)
    }

    fn build_indices(&mut self) {
        self.model_idx = index_by(&self.models, |m| &m.name);
        self.ak_idx = index_by(&self.access_keys, |a| &a.ak);
        self.product_idx = index_by(&self.products, |p| &p.name);
    }

    /// Expand provider presets: fill each model's default wire type and
    /// synthesize an account per provider (explicit same-name accounts win).
    fn normalize(&mut self) -> Result<(), ConfigError> {
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
        // account health and failover exclusion key by name — duplicates would
        // cool down / exclude the wrong physical account.
        check_unique("account", self.accounts.iter().map(|a| a.name.as_str()))?;
        Ok(())
    }

    pub fn find_product(&self, name: &str) -> Option<&ProductConfEntry> {
        self.products.get(*self.product_idx.get(name)?)
    }

    pub fn find_ak(&self, ak: &str) -> Option<&AkConf> {
        self.access_keys.get(*self.ak_idx.get(ak)?)
    }

    /// Look up a public model name (e.g. "gpt-4o").
    pub fn find_model(&self, name: &str) -> Option<&ModelConf> {
        self.models.get(*self.model_idx.get(name)?)
    }

    /// Pricing for a public model name; zero if unlisted.
    pub fn prices_for(&self, name: &str) -> (i64, i64) {
        self.find_model(name)
            .map(|m| (m.input_price_per_1k_micros, m.output_price_per_1k_micros))
            .unwrap_or((0, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // endpoint override wins over the preset default
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
        // the synthesized preset account inherits the provider's policy
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
