//! Post-hoc usage of an OpenRouter generation, which OpenRouter records even for a cancelled stream.

use gw_consts::Protocol;
use gw_models::{Account, GResult, GatewayError};
use serde_json::{Value, json};

use crate::base::versioned_url;
use crate::transport::{Transport, UpstreamBody, UpstreamRequest};

const OPENROUTER_KIND: &str = "openrouter";

/// Whether `account` records generation `id` for a later [`openrouter_usage`] lookup.
pub fn records_generation(account: &Account, id: &str) -> bool {
    account.kind == OPENROUTER_KIND
        && !account.endpoint.is_empty()
        && !account.api_key_env.is_empty()
        && !id.is_empty()
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// The generation's native counts and cost as chat-wire usage; `None` until OpenRouter records it.
pub async fn openrouter_usage(
    transport: &dyn Transport,
    account: &Account,
    id: &str,
) -> GResult<Option<Value>> {
    let Some(key) = account.api_key() else {
        return Ok(None);
    };
    let resp = transport
        .send(UpstreamRequest {
            protocol: Protocol::OpenaiChat,
            method: "GET",
            url: versioned_url(
                account.endpoint.trim_end_matches('/'),
                &format!("generation?id={id}"),
            ),
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
    let UpstreamBody::Json(body) = resp.body else {
        return Err(lookup_failed("generation lookup returned no JSON body"));
    };
    if resp.status != 200 {
        return Err(lookup_failed(format!(
            "generation lookup returned {}",
            resp.status
        )));
    }
    let doc: Value = serde_json::from_slice(&body)
        .map_err(|e| lookup_failed(format!("generation lookup body: {e}")))?;
    Ok(generation_usage(&doc["data"]))
}

fn lookup_failed(message: impl Into<String>) -> GatewayError {
    GatewayError::new(gw_consts::ErrCode::FED_RESP_RPC_FAILED, 502, message)
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
    use crate::transport::UpstreamResponse;

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
        let usage = openrouter_usage(&t, &openrouter(), "gen-1792232109-kUA52UZsI2")
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
            openrouter_usage(&missing, &openrouter(), "gen-1")
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
            openrouter_usage(&pending, &openrouter(), "gen-1")
                .await
                .unwrap()
                .is_none()
        );
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
        assert!(!records_generation(&openrouter(), "gen/../x"));
        assert!(!records_generation(&openrouter(), ""));
    }
}
