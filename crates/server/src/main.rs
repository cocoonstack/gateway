//! Service entrypoint.
//!
//! Load config (GW_CONFIG path, else the embedded default; with
//! `storage.postgres_url` set the Postgres config store is the source of truth
//! and the file only seeds it), build state with the configured backends,
//! select the upstream transport (`GW_TRANSPORT`), spawn local background
//! tasks and the config change feed, serve the views router with graceful
//! shutdown (SIGINT/SIGTERM → drain).
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
    // When storage.postgres_url is set, the Postgres config store becomes the
    // source of truth instead: the file only seeds an empty store, and reloads
    // read the latest published version.
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
    let mut state = GatewayState::from_config(&cfg);
    if !cfg.storage.postgres_url.is_empty() {
        use gw_state::KeyStore;
        let ks = gw_state::PostgresKeyStore::connect(&cfg.storage.postgres_url).await?;
        ks.reload_config_keys(&cfg.access_keys).await?;
        state.auth = Arc::new(ks);
        tracing::info!("key store = postgres (config keys seeded)");
    }
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
        match gw_state::RedisHealth::connect(&cfg.storage.redis_url).await {
            Ok(h) => {
                state.health = Arc::new(h);
                tracing::info!("account health = redis (fleet-wide cooldown)");
            }
            Err(e) => {
                tracing::error!(error = %e, "redis health connect failed; staying in-process");
            }
        }
    }
    if !cfg.storage.postgres_url.is_empty() {
        state.store = Arc::new(
            gw_state::PostgresStore::connect_with_cap(
                &cfg.storage.postgres_url,
                cfg.storage.ledger_max_rows,
            )
            .await?,
        );
        tracing::info!("store = postgres");
    } else if !cfg.storage.sqlite_path.is_empty() {
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

    // Config change feed: every instance reloads when a new version is
    // published (PUT /admin/config anywhere in the fleet). Reconnects forever.
    if config_store.is_some() {
        let app = app_state.clone();
        tokio::spawn(async move {
            loop {
                match gw_state::configstore::subscribe(&postgres_url).await {
                    Ok(mut versions) => {
                        tracing::info!("config change feed connected");
                        // catch up: a publish during a reconnect gap fired its
                        // NOTIFY to no one, so re-read latest on every connect
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

    // SIGHUP → live reload (rebuild AK table / models / providers / accounts;
    // keep governance / store / health / cache; storage-backend changes need a restart).
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
