//! Service entrypoint.
//!
//! Load local config (GW_CONFIG path, else the embedded default), build
//! in-process state, select the upstream transport (`GW_TRANSPORT`), spawn local
//! background tasks, serve the views router with graceful shutdown
//! (SIGINT/SIGTERM → drain).
//!
//! Accounts with a configured `endpoint` egress to real vendors; accounts
//! without one are served by the in-process mock. `GW_TRANSPORT=mock` forces
//! zero egress. `tracing` is local structured logging to stdout only.

use std::env;
use std::sync::Arc;

use gw_config::GatewayConfig;
use gw_state::GatewayState;
use gw_views::AppState;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // The config source (file path or the embedded default) is captured so a
    // runtime reload — SIGHUP or POST /admin/reload — re-reads the same source.
    let config_source = env::var("GW_CONFIG").ok();
    let load_from_source = {
        let src = config_source.clone();
        move || -> Result<GatewayConfig, String> {
            match &src {
                Some(path) => GatewayConfig::load(path).map_err(|e| e.to_string()),
                None => GatewayConfig::embedded_default().map_err(|e| e.to_string()),
            }
        }
    };
    match &config_source {
        Some(path) => tracing::info!("loading config from {path}"),
        None => tracing::info!("using embedded default config (set GW_CONFIG to override)"),
    }
    let cfg = load_from_source().map_err(|e| anyhow::anyhow!(e))?;

    // GW_HOST / GW_PORT win over the config file (GW_HOST=0.0.0.0 for containers).
    let host = env::var("GW_HOST").unwrap_or_else(|_| cfg.listen.host.clone());
    let port = env::var("GW_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(cfg.listen.port);
    let addr = format!("{host}:{port}");

    let cfg = Arc::new(cfg);
    let mut state = GatewayState::from_config(&cfg);
    if !cfg.storage.redis_url.is_empty() {
        match gw_state::RedisGovernance::connect(&cfg.storage.redis_url).await {
            Ok(g) => {
                state.governance = Arc::new(g);
                tracing::info!(url = %cfg.storage.redis_url, "governance = redis");
            }
            Err(e) => {
                tracing::error!(error = %e, "redis connect failed; staying in-process");
            }
        }
    }
    if !cfg.storage.sqlite_path.is_empty() {
        state.store = Arc::new(
            gw_state::SqliteStore::open_with_cap(
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

    // Local background task: AK daily quota reset (governance is a preserved
    // seam, so this keeps resetting the live counters across reloads).
    let quota_task = gw_task::spawn_quota_reset(state.clone(), gw_task::DAILY);

    let transport = select_transport(&cfg)?;
    let shared = gw_state::SharedConfig::new(cfg, state);
    let loader: gw_views::ConfigLoader = Arc::new(load_from_source);
    let app_state = AppState::with_config(shared, transport, Some(loader));

    // SIGHUP → live reload (rebuild AK table / models / providers / accounts;
    // keep governance / store / health / cache; storage-backend changes need a restart).
    #[cfg(unix)]
    {
        let app = app_state.clone();
        tokio::spawn(async move {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
                Ok(mut sighup) => {
                    while sighup.recv().await.is_some() {
                        match app.reload() {
                            Ok(()) => tracing::info!("SIGHUP: config reloaded"),
                            Err(e) => tracing::error!(error = %e, "SIGHUP: reload failed"),
                        }
                    }
                }
                Err(e) => tracing::error!(error = %e, "install SIGHUP handler failed"),
            }
        });
    }

    let prometheus = metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder()?;
    let router = gw_views::app(app_state).route(
        "/metrics",
        axum::routing::get(move || {
            let prometheus = prometheus.clone();
            async move { prometheus.render() }
        }),
    );

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("gw listening on http://{addr}");

    // graceful shutdown (drain on SIGINT/SIGTERM)
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    quota_task.abort();
    tracing::info!("gw drained and exiting");
    Ok(())
}

/// Choose the upstream transport from `GW_TRANSPORT`: `mock` forces zero egress,
/// `http` forces real HTTP (accounts without an endpoint fail loudly), anything
/// else routes `mock://` sentinels in-process and real URLs over HTTP.
fn select_transport(cfg: &GatewayConfig) -> anyhow::Result<gw_engines::SharedTransport> {
    use gw_engines::http_transport::UpstreamPolicy;
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
    Ok(match env::var("GW_TRANSPORT").as_deref() {
        Ok("mock") => {
            tracing::info!("transport = mock (zero egress)");
            std::sync::Arc::new(gw_engines::MockTransport)
        }
        Ok("http") => {
            tracing::info!("transport = http (accounts without an endpoint fail)");
            std::sync::Arc::new(gw_engines::http_transport::HttpTransport::with_policies(
                default_policy,
                per_account,
            )?)
        }
        _ => {
            tracing::info!("transport = auto (mock:// in-process, real URLs over HTTP)");
            std::sync::Arc::new(
                gw_engines::http_transport::DispatchTransport::with_policies(
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
