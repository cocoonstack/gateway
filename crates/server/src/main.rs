//! Service entrypoint.
//!
//! Load local config (AP_GATEWAY_CONF path, else the embedded default), build
//! in-process state, select the upstream transport (`AP_TRANSPORT`), spawn local
//! background tasks, serve the views router with graceful shutdown
//! (SIGINT/SIGTERM → drain).
//!
//! Accounts with a configured `endpoint` egress to real vendors; accounts
//! without one are served by the in-process mock. `AP_TRANSPORT=mock` forces
//! zero egress. `tracing` is local structured logging to stdout only.

use std::env;
use std::sync::Arc;

use ap_config::GatewayConfig;
use ap_state::GatewayState;
use ap_views::AppState;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cfg = match env::var("AP_GATEWAY_CONF") {
        Ok(path) => {
            tracing::info!("loading config from {path}");
            GatewayConfig::load(&path)?
        }
        Err(_) => {
            tracing::info!("using embedded default config (set AP_GATEWAY_CONF to override)");
            GatewayConfig::embedded_default()?
        }
    };

    // AP_PORT wins over the config file.
    let port = env::var("AP_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(cfg.listen.port);
    let addr = format!("{}:{port}", cfg.listen.host);

    let cfg = Arc::new(cfg);
    let mut state = GatewayState::from_config(&cfg);
    if !cfg.storage.sqlite_path.is_empty() {
        state.store = Arc::new(
            ap_state::SqliteStore::open_with_cap(
                &cfg.storage.sqlite_path,
                cfg.storage.ledger_max_rows,
            )
            .await?,
        );
        tracing::info!(path = %cfg.storage.sqlite_path, "store = sqlite");
    }
    let state = Arc::new(state);
    tracing::info!(
        access_keys = cfg.access_keys.len(),
        models = cfg.models.len(),
        accounts = state.pool.len(),
        "gateway state built"
    );

    // Local background task: AK daily quota reset
    let quota_task = ap_task::spawn_quota_reset(state.clone(), ap_task::DAILY);

    let transport = select_transport(&cfg)?;
    let app_state = AppState::new(cfg, state, transport);

    let prometheus = metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder()?;
    let router = ap_views::app(app_state).route(
        "/metrics",
        axum::routing::get(move || {
            let prometheus = prometheus.clone();
            async move { prometheus.render() }
        }),
    );

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("ap listening on http://{addr}");

    // graceful shutdown (drain on SIGINT/SIGTERM)
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    quota_task.abort();
    tracing::info!("ap drained and exiting");
    Ok(())
}

/// Choose the upstream transport from `AP_TRANSPORT`: `mock` forces zero egress,
/// `http` forces real HTTP (accounts without an endpoint fail loudly), anything
/// else routes `mock://` sentinels in-process and real URLs over HTTP.
fn select_transport(cfg: &GatewayConfig) -> anyhow::Result<ap_engines::SharedTransport> {
    use ap_engines::http_transport::UpstreamPolicy;
    let default_policy = UpstreamPolicy::default();
    let per_account: std::collections::HashMap<String, UpstreamPolicy> = cfg
        .accounts
        .iter()
        .filter(|a| a.timeout_seconds.is_some() || a.connect_retries.is_some())
        .map(|a| {
            (
                a.name.clone(),
                UpstreamPolicy {
                    timeout: a
                        .timeout_seconds
                        .map(std::time::Duration::from_secs)
                        .unwrap_or(default_policy.timeout),
                    connect_retries: a.connect_retries.unwrap_or(default_policy.connect_retries),
                },
            )
        })
        .collect();
    Ok(match env::var("AP_TRANSPORT").as_deref() {
        Ok("mock") => {
            tracing::info!("transport = mock (zero egress)");
            std::sync::Arc::new(ap_engines::MockTransport)
        }
        Ok("http") => {
            tracing::info!("transport = http (accounts without an endpoint fail)");
            std::sync::Arc::new(ap_engines::http_transport::HttpTransport::with_policies(
                default_policy,
                per_account,
            )?)
        }
        _ => {
            tracing::info!("transport = auto (mock:// in-process, real URLs over HTTP)");
            std::sync::Arc::new(
                ap_engines::http_transport::DispatchTransport::with_policies(
                    default_policy,
                    per_account,
                )?,
            )
        }
    })
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => tracing::error!("install SIGTERM handler: {e}"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("SIGINT received, draining"),
        _ = terminate => tracing::info!("SIGTERM received, draining"),
    }
}
