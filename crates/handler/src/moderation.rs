//! The moderation seam: an optional external content-review pass in the pre-stage.
//! The default [`AllowModerator`] is a no-op, so the hot path pays nothing;
//! [`BedrockGuardrail`] reviews through AWS Bedrock Guardrails `ApplyGuardrail`.

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use gw_config::ModerationConf;
use serde_json::{Value, json};

/// The `ApplyGuardrail` assessment lists that carry an `action` per entity.
const ASSESSMENT_LISTS: [(&str, &str); 6] = [
    ("sensitiveInformationPolicy", "piiEntities"),
    ("sensitiveInformationPolicy", "regexes"),
    ("wordPolicy", "customWords"),
    ("wordPolicy", "managedWordLists"),
    ("topicPolicy", "topics"),
    ("contentPolicy", "filters"),
];

/// A moderator's decision on one request's text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Allow,
    /// Redact these byte ranges of the reviewed text, then serve; offsets address the string `review` saw.
    Mask(Vec<Range<usize>>),
    /// Serve via the tenant's fallback model; denies when there is none or the surface cannot switch.
    Degrade,
    /// Deny with a user-facing reason.
    Deny(String),
}

/// A pluggable content moderator over the request's concatenated inbound text; `Err` is a failure.
#[async_trait::async_trait]
pub trait Moderator: Send + Sync + std::fmt::Debug {
    async fn review(&self, text: &str) -> Result<Verdict, String>;
}

/// The default: allow everything.
#[derive(Debug, Default)]
pub struct AllowModerator;

#[async_trait::async_trait]
impl Moderator for AllowModerator {
    async fn review(&self, _text: &str) -> Result<Verdict, String> {
        Ok(Verdict::Allow)
    }
}

/// AWS Bedrock Guardrails: a `BLOCKED` assessment denies, `ANONYMIZED` PII
/// entities mask their matches, anything else allows.
#[derive(Debug)]
pub struct BedrockGuardrail {
    client: reqwest::Client,
    url: String,
    api_key: String,
    source: String,
    timeout: Duration,
}

impl BedrockGuardrail {
    fn new(conf: &ModerationConf, api_key: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: format!(
                "{}/guardrail/{}/version/{}/apply",
                conf.endpoint.trim_end_matches('/'),
                conf.guardrail_id,
                conf.guardrail_version
            ),
            api_key,
            source: conf.source.clone(),
            timeout: Duration::from_secs(conf.timeout_seconds),
        }
    }
}

#[async_trait::async_trait]
impl Moderator for BedrockGuardrail {
    async fn review(&self, text: &str) -> Result<Verdict, String> {
        let body = json!({"source": self.source, "content": [{"text": {"text": text}}]});
        let resp = self
            .client
            .post(&self.url)
            .timeout(self.timeout)
            .bearer_auth(&self.api_key)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .map_err(|e| format!("guardrail request: {e}"))?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| format!("guardrail reply: {e}"))?;
        let reply: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(format!(
                "guardrail status {status}: {}",
                reply["message"].as_str().unwrap_or_default()
            ));
        }
        Ok(guardrail_verdict(text, &reply))
    }
}

/// The moderator the config names, else the allow-all default.
pub fn from_config(conf: Option<&ModerationConf>) -> Arc<dyn Moderator> {
    let Some(conf) = conf else {
        return default_moderator();
    };
    let api_key = conf.api_key().unwrap_or_else(|| {
        tracing::warn!(var = %conf.api_key_env, "moderation api key env is unset; reviews will fail");
        String::new()
    });
    Arc::new(BedrockGuardrail::new(conf, api_key))
}

pub fn default_moderator() -> Arc<dyn Moderator> {
    Arc::new(AllowModerator)
}

/// Map an `ApplyGuardrail` reply onto a verdict over the reviewed `text`.
fn guardrail_verdict(text: &str, reply: &Value) -> Verdict {
    if reply["action"] != "GUARDRAIL_INTERVENED" {
        return Verdict::Allow;
    }
    let mut blocked: Vec<&str> = Vec::new();
    let mut masked = Vec::new();
    for a in reply["assessments"].as_array().into_iter().flatten() {
        for (policy, list) in ASSESSMENT_LISTS {
            for e in a[policy][list].as_array().into_iter().flatten() {
                let label = e["name"].as_str().or(e["type"].as_str()).unwrap_or("word");
                match e["action"].as_str() {
                    Some("BLOCKED") => blocked.push(label),
                    Some("ANONYMIZED") => {
                        if let Some(needle) = e["match"].as_str().filter(|m| !m.is_empty()) {
                            masked.extend(text.match_indices(needle).map(|(i, m)| i..i + m.len()));
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    if !blocked.is_empty() {
        blocked.sort_unstable();
        blocked.dedup();
        return Verdict::Deny(format!("blocked by guardrail: {}", blocked.join(", ")));
    }
    if masked.is_empty() {
        Verdict::Deny(
            reply["actionReason"]
                .as_str()
                .unwrap_or("blocked by guardrail")
                .to_owned(),
        )
    } else {
        Verdict::Mask(masked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocked_entities_deny_with_their_types() {
        let reply = json!({"action":"GUARDRAIL_INTERVENED","actionReason":"Guardrail blocked.","assessments":[{
            "sensitiveInformationPolicy":{"piiEntities":[{"action":"BLOCKED","match":"123-45-6789","type":"US_SOCIAL_SECURITY_NUMBER"}],"regexes":[]},
            "wordPolicy":{"customWords":[{"action":"BLOCKED","match":"forbiddenword"}],"managedWordLists":null},
            "topicPolicy":{"topics":[{"action":"BLOCKED","name":"crypto-investing","type":"DENY"}]}}]});
        assert_eq!(
            guardrail_verdict("my ssn is 123-45-6789 and forbiddenword", &reply),
            Verdict::Deny(
                "blocked by guardrail: US_SOCIAL_SECURITY_NUMBER, crypto-investing, word".into()
            )
        );
    }

    #[test]
    fn anonymized_entities_mask_every_match() {
        let text = "Mail bob@example.com or call 415-555-0134; again bob@example.com";
        let reply = json!({"action":"GUARDRAIL_INTERVENED","actionReason":"Guardrail masked.","assessments":[{
            "sensitiveInformationPolicy":{"piiEntities":[
                {"action":"ANONYMIZED","match":"bob@example.com","type":"EMAIL"},
                {"action":"ANONYMIZED","match":"415-555-0134","type":"PHONE"}],"regexes":[]}}]});
        assert_eq!(
            guardrail_verdict(text, &reply),
            Verdict::Mask(vec![5..20, 49..64, 29..41])
        );
        assert_eq!(&text[5..20], "bob@example.com");
        assert_eq!(&text[29..41], "415-555-0134");
        assert_eq!(&text[49..64], "bob@example.com");
    }

    #[test]
    fn no_intervention_allows_and_an_opaque_intervention_denies() {
        assert_eq!(
            guardrail_verdict("hi", &json!({"action":"NONE"})),
            Verdict::Allow
        );
        assert_eq!(
            guardrail_verdict(
                "hi",
                &json!({"action":"GUARDRAIL_INTERVENED","actionReason":"Guardrail blocked."})
            ),
            Verdict::Deny("Guardrail blocked.".into())
        );
    }

    #[tokio::test]
    async fn review_posts_the_apply_body_with_the_bearer_key() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let n = sock.read(&mut buf).await.unwrap();
            let req = String::from_utf8_lossy(&buf[..n]).into_owned();
            let reply = r#"{"action":"GUARDRAIL_INTERVENED","assessments":[{"wordPolicy":{"customWords":[{"action":"BLOCKED","match":"bad"}]}}]}"#;
            sock.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}", reply.len()).as_bytes()).await.unwrap();
            req
        });
        let conf: ModerationConf = serde_json::from_value(json!({
            "kind": "bedrock_guardrail", "endpoint": format!("http://{addr}"), "api_key_env": "",
            "guardrail_id": "g1", "guardrail_version": "1", "source": "INPUT"
        }))
        .unwrap();
        let verdict = BedrockGuardrail::new(&conf, "ABSK-test".into())
            .review("bad word")
            .await
            .unwrap();
        assert_eq!(verdict, Verdict::Deny("blocked by guardrail: word".into()));
        let req = server.await.unwrap();
        assert!(
            req.starts_with("POST /guardrail/g1/version/1/apply HTTP/1.1"),
            "{req}"
        );
        assert!(req.contains("authorization: Bearer ABSK-test"), "{req}");
        assert!(
            req.ends_with(r#"{"content":[{"text":{"text":"bad word"}}],"source":"INPUT"}"#),
            "{req}"
        );
    }
}
