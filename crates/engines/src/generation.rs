//! Post-hoc usage of an OpenRouter generation, which OpenRouter records even for a cancelled stream.

use std::time::Duration;

use gw_consts::Protocol;
use gw_models::{Account, GResult};
use serde_json::{Value, json};

use crate::base::{parse_json_reply, versioned_url};
use crate::transport::{Transport, UpstreamRequest};

const OPENROUTER_KIND: &str = "openrouter";
/// Waits before each lookup of an abandoned stream's recorded usage; OpenRouter records one in about 5 s.
const LOOKUP_DELAYS: [Duration; 4] = [
    Duration::from_secs(3),
    Duration::from_secs(5),
    Duration::from_secs(10),
    Duration::from_secs(20),
];
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn recorded_usage(
    transport: &dyn Transport,
    account: &Account,
    id: &str,
    closing: impl Future<Output = ()>,
) -> Option<Value> {
    let key = account.api_key()?;
    tokio::pin!(closing);
    for delay in LOOKUP_DELAYS {
        let lookup = async {
            tokio::time::sleep(delay).await;
            tokio::time::timeout(
                LOOKUP_TIMEOUT,
                openrouter_usage(transport, account, id, &key),
            )
            .await
        };
        tokio::select! {
            () = &mut closing => {
                tracing::warn!(id, "shutting down before the generation record; billing the estimate");
                return None;
            }
            found = lookup => match found {
                Ok(Ok(Some(usage))) => return Some(usage),
                Ok(Ok(None)) => {}
                Ok(Err(e)) => tracing::warn!(error = %e, id, "generation usage lookup failed"),
                Err(_) => tracing::warn!(id, "generation usage lookup timed out"),
            },
        }
    }
    tracing::warn!(
        id,
        "no recorded usage for an aborted stream; billing the estimate"
    );
    None
}

pub fn records_generation(account: &Account, id: &str) -> bool {
    account.wire_kind() == OPENROUTER_KIND
        && !account.endpoint.is_empty()
        && !id.is_empty()
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// The generation's native counts and cost as chat-wire usage; `None` until OpenRouter records it.
async fn openrouter_usage(
    transport: &dyn Transport,
    account: &Account,
    id: &str,
    key: &str,
) -> GResult<Option<Value>> {
    let resp = transport
        .send(UpstreamRequest {
            protocol: Protocol::OpenaiChat,
            method: "GET",
            url: versioned_url(account.base_url(""), &format!("generation?id={id}")),
            headers: vec![("authorization", format!("Bearer {key}"))],
            body: Vec::new(),
            stream: false,
            account: account.name.clone(),
            replay_account: None,
            output_cap: 0,
        })
        .await?;
    if resp.status == 404 {
        return Ok(None);
    }
    let (_, doc) = parse_json_reply(resp)?;
    Ok(generation_usage(&doc["data"]))
}

/// Map `/generation` data to chat-wire usage, reasoning inside completion as in `completion_tokens`.
fn generation_usage(data: &Value) -> Option<Value> {
    let count = |k: &str| data[k].as_i64();
    let completion = count("native_tokens_completion")?;
    Some(json!({
        "prompt_tokens": count("native_tokens_prompt")?,
        "completion_tokens": completion,
        "prompt_tokens_details": {"cached_tokens": count("native_tokens_cached").unwrap_or(0)},
        "completion_tokens_details": {"reasoning_tokens": count("native_tokens_reasoning").unwrap_or(0).clamp(0, completion)},
        "cost": data["total_cost"],
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::transport::{UpstreamBody, UpstreamResponse};

    #[derive(Debug)]
    struct Canned(u16, &'static str, Mutex<Vec<String>>);

    #[async_trait::async_trait]
    impl Transport for Canned {
        async fn send(&self, req: UpstreamRequest) -> GResult<UpstreamResponse> {
            self.2
                .lock()
                .unwrap()
                .push(format!("{} {}", req.method, req.url));
            Ok(UpstreamResponse {
                status: self.0,
                body: UpstreamBody::Json(bytes::Bytes::from_static(self.1.as_bytes())),
                headers: Default::default(),
            })
        }
    }

    fn openrouter() -> Account {
        Account {
            name: "or".into(),
            kind: OPENROUTER_KIND.into(),
            endpoint: "https://openrouter.ai/api".into(),
            api_key_env: "PATH".into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_recorded_generation_maps_to_chat_usage() {
        let t = Canned(
            200,
            r#"{"data":{"cancelled":true,"native_tokens_prompt":11,"native_tokens_completion":73,"native_tokens_cached":4,"native_tokens_reasoning":73,"total_cost":9.09e-05}}"#,
            Mutex::default(),
        );
        let usage = openrouter_usage(&t, &openrouter(), "gen-1792232109-kUA52UZsI2", "k")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            t.2.lock().unwrap().as_slice(),
            ["GET https://openrouter.ai/api/v1/generation?id=gen-1792232109-kUA52UZsI2"]
        );
        let common = crate::extract_common_usage(&usage, false).unwrap();
        assert_eq!(
            (
                common.prompt_total(),
                common.read_cache,
                common.completion_total(),
                common.reason
            ),
            (11, 4, 73, 73)
        );
        assert_eq!(crate::extract_vendor_cost_micros(&usage), Some(91));
    }

    #[tokio::test]
    async fn an_unrecorded_generation_yields_nothing() {
        let missing = Canned(404, r#"{"error":{"code":404}}"#, Mutex::default());
        assert!(
            openrouter_usage(&missing, &openrouter(), "gen-1", "k")
                .await
                .unwrap()
                .is_none()
        );
        let pending = Canned(
            200,
            r#"{"data":{"native_tokens_prompt":11}}"#,
            Mutex::default(),
        );
        assert!(
            openrouter_usage(&pending, &openrouter(), "gen-1", "k")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[derive(Debug, Default)]
    struct Silent(std::sync::atomic::AtomicUsize);

    #[async_trait::async_trait]
    impl Transport for Silent {
        async fn send(&self, _: UpstreamRequest) -> GResult<UpstreamResponse> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::future::pending().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_lookup_that_never_answers_times_out_each_try() {
        let t = Silent::default();
        let usage = recorded_usage(&t, &openrouter(), "gen-1", std::future::pending()).await;
        assert!(usage.is_none());
        assert_eq!(
            t.0.load(std::sync::atomic::Ordering::SeqCst),
            LOOKUP_DELAYS.len()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_closing_gateway_skips_the_lookup() {
        let t = Silent::default();
        let usage = recorded_usage(&t, &openrouter(), "gen-1", std::future::ready(())).await;
        assert!(usage.is_none());
        assert_eq!(t.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn only_an_openrouter_account_with_a_plain_id_records_generations() {
        assert!(records_generation(
            &openrouter(),
            "gen-1792232109-kUA52UZsI2"
        ));
        let other = Account {
            kind: "openai".into(),
            ..openrouter()
        };
        assert!(!records_generation(&other, "gen-1"));
        let raw = Account {
            kind: String::new(),
            provider: OPENROUTER_KIND.into(),
            ..openrouter()
        };
        assert!(records_generation(&raw, "gen-1"));
        assert!(!records_generation(&openrouter(), "gen/../x"));
        assert!(!records_generation(&openrouter(), ""));
    }
}
