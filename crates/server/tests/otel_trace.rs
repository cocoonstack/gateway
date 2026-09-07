//! The request span reaches an OTLP exporter with the route, the pipeline
//! fields, and the caller's trace context; its own binary, since the global
//! subscriber is process-wide.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use gw_config::GatewayConfig;
use gw_state::GatewayState;
use gw_views::AppState;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use tower::ServiceExt;
use tracing_subscriber::layer::SubscriberExt as _;

fn attrs(span: &SpanData) -> HashMap<&str, String> {
    span.attributes
        .iter()
        .map(|kv| (kv.key.as_str(), kv.value.to_string()))
        .collect()
}

#[tokio::test]
async fn request_span_exports_route_pipeline_fields_and_the_caller_context() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
    tracing::subscriber::set_global_default(subscriber).expect("first subscriber in this process");
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let cfg = Arc::new(GatewayConfig::embedded_default().unwrap());
    let state = Arc::new(GatewayState::from_config(&cfg));
    let app = gw_views::app(AppState::new(
        cfg,
        state,
        Arc::new(gw_engines::MockTransport),
    ));
    let chat = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .header("authorization", "Bearer ak-demo-123")
        .header("x-gw-user", "u-trace")
        .header(
            "traceparent",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        )
        .body(Body::from(
            r#"{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(chat).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let miss = Request::builder()
        .uri("/v1/nothing")
        .header("authorization", "Bearer ak-demo-123")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.oneshot(miss).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );

    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let names: Vec<&str> = spans.iter().map(|s| s.name.as_ref()).collect();
    let chat = spans
        .iter()
        .find(|s| s.name == "/v1/chat/completions")
        .unwrap_or_else(|| panic!("chat span missing in {names:?}"));
    let a = attrs(chat);
    assert_eq!(a["http.request.method"], "POST");
    assert_eq!(a["http.route"], "/v1/chat/completions");
    assert_eq!(a["http.response.status_code"], "200");
    assert_eq!(a["gw.surface"], "chat_completions");
    assert_eq!(a["gw.model"], "gpt-4o");
    assert_eq!(a["gw.tenant"], "default");
    assert_eq!(a["gw.user_id"], "u-trace");
    assert!(
        a["gw.request_id"].starts_with("req-"),
        "{}",
        a["gw.request_id"]
    );
    assert!(a["gw.prompt_tokens"].parse::<i64>().unwrap() > 0);
    assert!(!a.contains_key("otel.status_code"), "a 200 is not an error");
    assert_eq!(
        chat.span_context.trace_id().to_string(),
        "0af7651916cd43dd8448eb211c80319c"
    );
    assert_eq!(chat.parent_span_id.to_string(), "b7ad6b7169203331");
    let miss = spans
        .iter()
        .find(|s| s.name == "GET")
        .unwrap_or_else(|| panic!("unmatched-route span missing in {names:?}"));
    assert_eq!(attrs(miss)["http.response.status_code"], "404");
}
