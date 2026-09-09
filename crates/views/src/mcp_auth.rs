//! Credentials for upstream MCP servers: a static bearer from the environment,
//! or an OAuth 2.0 access token the gateway fetches with the client-credentials
//! or refresh-token grant and caches until it nears expiry.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use gw_config::{McpGrant, McpOAuthConf, McpServerConf};
use serde_json::Value;

// a token is renewed this long before its own expiry so an in-flight call never presents a stale one
const EXPIRY_MARGIN: Duration = Duration::from_secs(30);
const DEFAULT_EXPIRES_IN: u64 = 3_600;
// a token endpoint claiming more is capped: Instant arithmetic must not overflow
const MAX_EXPIRES_IN: u64 = 30 * 86_400;
const TOKEN_TIMEOUT: Duration = Duration::from_secs(10);

/// Per-server token cache; one entry per OAuth-configured server.
#[derive(Debug, Default)]
pub struct McpAuth {
    tokens: Mutex<HashMap<String, Token>>,
    /// One fetch in flight per server, so a cold cache costs one token round
    /// trip and one server's slow token endpoint never stalls another's.
    fetching: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl McpAuth {
    /// The bearer to present to `conf`'s server: its static key, else a cached
    /// or freshly fetched OAuth token; `None` when the server takes no credential.
    pub async fn bearer(
        &self,
        client: &reqwest::Client,
        conf: &McpServerConf,
    ) -> Result<Option<String>, String> {
        if let Some(key) = conf.api_key() {
            return Ok(Some(key));
        }
        let Some(oauth) = &conf.oauth else {
            return Ok(None);
        };
        if let Some(access) = self.cached(&conf.name) {
            return Ok(Some(access));
        }
        let gate = self.server_gate(&conf.name);
        let _one_at_a_time = gate.lock().await;
        if let Some(access) = self.cached(&conf.name) {
            return Ok(Some(access));
        }
        let refresh = self.lock().get(&conf.name).and_then(|t| t.refresh.clone());
        let token = fetch(client, oauth, refresh.as_deref()).await?;
        let access = token.access.clone();
        self.lock().insert(conf.name.clone(), token);
        Ok(Some(access))
    }

    fn cached(&self, server: &str) -> Option<String> {
        self.lock()
            .get(server)
            .filter(|t| t.expires_at > Instant::now())
            .map(|t| t.access.clone())
    }

    /// Expire `server`'s token: the upstream refused it, so the next call fetches anew.
    pub fn invalidate(&self, server: &str) {
        if let Some(t) = self.lock().get_mut(server) {
            t.expires_at = Instant::now();
        }
    }

    /// Forget every token (a config reload may have changed the clients).
    pub fn clear(&self) {
        self.lock().clear();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Token>> {
        self.tokens.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn server_gate(&self, server: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.fetching
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(server.to_owned())
            .or_default()
            .clone()
    }
}

#[derive(Debug)]
struct Token {
    access: String,
    expires_at: Instant,
    refresh: Option<String>,
}

/// One token-endpoint round trip; `refresh` is the rotated token from the last reply, else the configured seed.
async fn fetch(
    client: &reqwest::Client,
    oauth: &McpOAuthConf,
    refresh: Option<&str>,
) -> Result<Token, String> {
    let secret = oauth.client_secret();
    let seed;
    let mut form = vec![("client_id", oauth.client_id.as_str())];
    match oauth.grant {
        McpGrant::ClientCredentials => form.push(("grant_type", "client_credentials")),
        McpGrant::RefreshToken => {
            seed = oauth.refresh_token();
            let token = refresh.or(seed.as_deref()).ok_or_else(|| {
                format!("refresh token env `{}` is unset", oauth.refresh_token_env)
            })?;
            form.push(("grant_type", "refresh_token"));
            form.push(("refresh_token", token));
        }
    }
    if !oauth.scope.is_empty() {
        form.push(("scope", &oauth.scope));
    }
    if let Some(secret) = secret.as_deref() {
        form.push(("client_secret", secret));
    }
    let resp = client
        .post(&oauth.token_url)
        .timeout(TOKEN_TIMEOUT)
        .form(&form)
        .send()
        .await
        .map_err(|e| format!("token request: {e}"))?;
    let status = resp.status();
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("token reply: {e}"))?;
    let mut reply: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    if !status.is_success() {
        return Err(format!(
            "token endpoint {status}: {} {}",
            reply["error"].as_str().unwrap_or_default(),
            reply["error_description"].as_str().unwrap_or_default()
        ));
    }
    let Some(Value::String(access)) = reply.get_mut("access_token").map(Value::take) else {
        return Err("token reply carries no access_token".to_owned());
    };
    let lifetime = Duration::from_secs(
        reply["expires_in"]
            .as_u64()
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_EXPIRES_IN)
            .min(MAX_EXPIRES_IN),
    );
    // a short-lived token is still cached for half its life instead of refetched per call
    let usable = lifetime - EXPIRY_MARGIN.min(lifetime / 2);
    let rotated = match reply.get_mut("refresh_token").map(Value::take) {
        Some(Value::String(t)) => Some(t),
        _ => refresh.map(str::to_owned),
    };
    Ok(Token {
        access,
        expires_at: Instant::now() + usable,
        refresh: rotated,
    })
}
