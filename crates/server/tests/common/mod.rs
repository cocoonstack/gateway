//! Shared fixtures for the gw-server integration tests.

use std::sync::Arc;

use axum::Router;
use gw_config::GatewayConfig;
use gw_state::GatewayState;
use gw_views::AppState;

/// The composed router over the embedded default config and the mock transport.
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
