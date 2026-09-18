use std::error::Error;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

mod common;

#[tokio::test]
async fn chat_to_responses_rejects_non_object_tool_calls() -> Result<(), Box<dyn Error>> {
    for calls in [
        r#"["broken"]"#,
        r#"[{"id":"c","type":"function","function":"broken"}]"#,
        r#"[{"id":"c","type":"function"}]"#,
    ] {
        let body = format!(
            r#"{{"model":"gpt-5-responses","messages":[{{"role":"assistant","tool_calls":{calls}}}]}}"#
        );
        let response = common::app()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("authorization", "Bearer ak-demo-123")
                    .header("content-type", "application/json")
                    .body(Body::from(body))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{calls}");
        let bytes = to_bytes(response.into_body(), usize::MAX).await?;
        let error: Value = serde_json::from_slice(&bytes)?;
        assert_eq!(error["error"]["code"], "validation_exception", "{error}");
    }
    Ok(())
}
