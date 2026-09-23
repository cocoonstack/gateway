//! Shared fixtures for the gw-server integration tests.

#![allow(dead_code)]

use std::sync::Arc;

use axum::Router;
use axum::response::Response;
use gw_config::GatewayConfig;
use gw_state::GatewayState;
use gw_views::AppState;
use serde_json::Value;

#[allow(clippy::expect_used)]
pub fn app() -> Router {
    let cfg = Arc::new(GatewayConfig::embedded_default().expect("embedded config"));
    let state = Arc::new(GatewayState::from_config(&cfg));
    gw_views::app(AppState::new(
        cfg,
        state,
        Arc::new(gw_engines::MockTransport),
    ))
}

#[allow(clippy::expect_used)]
pub async fn body_json(resp: Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&bytes).expect("json")
}
