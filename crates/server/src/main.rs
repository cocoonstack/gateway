//! Service entrypoint: load config (GW_CONFIG path, else the embedded default;
//! with `storage.postgres_url` the config store is the source of truth and the
//! file only seeds it), build state, select the transport (`GW_TRANSPORT`),
//! spawn background tasks and the config change feed, serve with graceful
//! shutdown. Accounts with an `endpoint` egress to real vendors; the rest are
//! served by the in-process mock; `GW_TRANSPORT=mock` forces zero egress.

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

    // reloads re-read this captured source
    let config_source = env::var("GW_CONFIG").ok();
    let read_source_text = {
        let src = config_source.clone();
        move || -> Result<String, String> {
            match &src {
                Some(path) => {
                    std::fs::read_to_string(path).map_err(|e| format!("read config {path}: {e}"))
                }
                None => Ok(gw_config::DEFAULT_YAML.to_owned()),
            }
        }
    };
    match &config_source {
        Some(path) => tracing::info!("loading config from {path}"),
        None => tracing::info!("using embedded default config (set GW_CONFIG to override)"),
    }
    let boot_yaml = read_source_text().map_err(|e| anyhow::anyhow!(e))?;
    let cfg = GatewayConfig::from_yaml(&boot_yaml)?;

    let config_store = if cfg.storage.postgres_url.is_empty() {
        None
    } else {
        Some(Arc::new(
            gw_state::PostgresConfigStore::connect(&cfg.storage.postgres_url).await?,
        ))
    };
    let cfg = match &config_store {
        Some(store) => match store.load_latest().await? {
            Some((version, yaml)) => {
                tracing::info!(version, "config = postgres store");
                GatewayConfig::from_yaml(&yaml)?
            }
            None => {
                let version = store.publish(&boot_yaml).await?;
                tracing::info!(version, "config store seeded from the local source");
                cfg
            }
        },
        None => cfg,
    };

    // GW_HOST / GW_PORT win over the config file (GW_HOST=0.0.0.0 for containers).
    let host = env::var("GW_HOST").unwrap_or_else(|_| cfg.listen.host.clone());
    let port = env::var("GW_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(cfg.listen.port);
    let addr = format!("{host}:{port}");

    let cfg = Arc::new(cfg);
    let state = GatewayState::build(&cfg).await?;
    let state = Arc::new(state);
    tracing::info!(
        access_keys = cfg.access_keys.len(),
        models = cfg.models.len(),
        accounts = state.pool.len(),
        "gateway state built"
    );

    // daily quota reset; governance is a preserved seam, so this survives reloads
    let quota_task = gw_task::spawn_quota_reset(state.clone(), gw_task::DAILY);
    let purge_task = gw_task::spawn_content_purge(state.clone(), gw_task::PURGE_PERIOD);
    let rollup_task = gw_task::spawn_usage_rollup(state.clone(), gw_task::ROLLUP_PERIOD);
    let distributed_batches = state.store.distributed_batches();

    let transport = select_transport()?;
    let postgres_url = cfg.storage.postgres_url.clone();
    let shared = gw_state::SharedConfig::new(cfg, state);
    let loader: gw_views::ConfigLoader = match &config_store {
        Some(store) => {
            let store = store.clone();
            Arc::new(move || {
                let store = store.clone();
                Box::pin(async move {
                    match store.load_latest().await.map_err(|e| e.to_string())? {
                        Some((_, yaml)) => {
                            GatewayConfig::from_yaml(&yaml).map_err(|e| e.to_string())
                        }
                        None => Err("config store is empty".to_owned()),
                    }
                }) as gw_views::ConfigFuture
            })
        }
        None => Arc::new(move || {
            let text = read_source_text();
            Box::pin(async move { GatewayConfig::from_yaml(&text?).map_err(|e| e.to_string()) })
                as gw_views::ConfigFuture
        }),
    };
    let mut app_state = AppState::with_config(shared, transport, Some(loader));
    if let Some(store) = &config_store {
        app_state = app_state.with_config_store(store.clone());
    }

    // fleet batch drain: on a distributed store any instance claims submitted batches
    if distributed_batches {
        let offline = app_state.offline.clone();
        tokio::spawn(async move {
            offline
                .drain_forever(120, std::time::Duration::from_secs(2))
                .await
        });
        tracing::info!("batch drain loop started (distributed store)");
    }

    // change feed: reload on every published config version; reconnects forever
    if config_store.is_some() {
        let app = app_state.clone();
        tokio::spawn(async move {
            loop {
                match gw_state::configstore::subscribe(&postgres_url).await {
                    Ok(mut versions) => {
                        tracing::info!("config change feed connected");
                        // a publish during a reconnect gap notified no one — catch up
                        if let Err(e) = app.reload().await {
                            tracing::error!(error = %e, "config feed: catch-up reload failed");
                        }
                        while let Some(version) = versions.recv().await {
                            match app.reload().await {
                                Ok(()) => tracing::info!(version, "config feed: reloaded"),
                                Err(e) => {
                                    tracing::error!(error = %e, "config feed: reload failed");
                                }
                            }
                        }
                        tracing::warn!("config change feed dropped; reconnecting");
                    }
                    Err(e) => tracing::warn!(error = %e, "config change feed connect failed"),
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        });
    }

    // SIGHUP → live reload (storage-backend changes still need a restart)
    #[cfg(unix)]
    {
        let app = app_state.clone();
        tokio::spawn(async move {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
                Ok(mut sighup) => {
                    while sighup.recv().await.is_some() {
                        match app.reload().await {
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

    // connect-info so the audit trail can root the source IP at the TCP peer
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    quota_task.abort();
    purge_task.abort();
    rollup_task.abort();
    tracing::info!("gw drained and exiting");
    Ok(())
}

/// Choose the upstream transport from `GW_TRANSPORT`: `mock` forces zero egress,
/// `http` forces real HTTP (accounts without an endpoint fail loudly), anything
/// else routes `mock://` sentinels in-process and real URLs over HTTP. Built
/// with default policies — the handler pushes the config-derived ones at
/// construction and on every reload.
fn select_transport() -> anyhow::Result<gw_engines::SharedTransport> {
    Ok(match env::var("GW_TRANSPORT").as_deref() {
        Ok("mock") => {
            tracing::info!("transport = mock (zero egress)");
            std::sync::Arc::new(gw_engines::MockTransport)
        }
        Ok("http") => {
            tracing::info!("transport = http (accounts without an endpoint fail)");
            std::sync::Arc::new(gw_engines::http_transport::HttpTransport::with_policies(
                Default::default(),
                Default::default(),
            )?)
        }
        _ => {
            tracing::info!("transport = auto (mock:// in-process, real URLs over HTTP)");
            std::sync::Arc::new(
                gw_engines::http_transport::DispatchTransport::with_policies(
                    Default::default(),
                    Default::default(),
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
