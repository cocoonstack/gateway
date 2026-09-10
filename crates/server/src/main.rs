//! Service entrypoint: load config (GW_CONFIG path, else the embedded default;
//! with `storage.postgres_url` the config store is the source of truth and the
//! file only seeds it), build state, select the transport (`GW_TRANSPORT`),
//! spawn background tasks and the config change feed, serve with graceful
//! shutdown. Accounts with an `endpoint` egress to real vendors; the rest are
//! served by the in-process mock; `GW_TRANSPORT=mock` forces zero egress.

use std::borrow::Cow;
use std::env;
use std::sync::Arc;
use std::time::Duration;

use gw_config::GatewayConfig;
use gw_state::GatewayState;
use gw_views::AppState;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, Layer as _};

const BATCH_STALE_SECS: i64 = 120;
const BATCH_POLL: Duration = Duration::from_secs(2);
const CONFIG_FEED_RETRY: Duration = Duration::from_secs(5);
const USAGE: &str = "usage: gw [--version | --help]\nconfiguration comes from the environment: GW_CONFIG, GW_TRANSPORT, GW_PORT";

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if let Some(flag) = env::args().nth(1) {
        return match flag.as_str() {
            "--version" | "-V" => {
                println!("gw {}", env!("CARGO_PKG_VERSION"));
                Ok(())
            }
            "--help" | "-h" => {
                println!("{USAGE}");
                Ok(())
            }
            _ => Err(anyhow::anyhow!("unknown argument {flag}\n{USAGE}")),
        };
    }
    let tracer_provider = init_tracing()?;

    // reloads re-read this captured source
    let config_source = env::var("GW_CONFIG").ok();
    match &config_source {
        Some(path) => tracing::info!("loading config from {path}"),
        None => tracing::info!("using embedded default config (set GW_CONFIG to override)"),
    }
    let boot_yaml = read_source_text(config_source.as_deref())
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
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

    // the GW_HOST / GW_PORT env vars win over the config file (GW_HOST=0.0.0.0 for containers)
    let host = env::var("GW_HOST").unwrap_or_else(|_| cfg.listen.host.clone());
    let port = env::var("GW_PORT")
        .ok()
        .and_then(|p| match p.parse::<u16>() {
            Ok(port) => Some(port),
            Err(e) => {
                tracing::warn!(value = %p, error = %e, "GW_PORT is not a port; using the config file");
                None
            }
        })
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

    // governance is a preserved seam: the quota reset survives reloads
    let quota_task = gw_task::spawn_quota_reset(state.clone(), gw_task::DAILY);
    let purge_task = gw_task::spawn_content_purge(state.clone(), gw_task::PURGE_PERIOD);
    let rollup_task = gw_task::spawn_usage_rollup(state.clone(), gw_task::ROLLUP_PERIOD);
    let avail_task = gw_task::spawn_avail_flush(state.clone(), gw_task::AVAIL_FLUSH_PERIOD);
    let distributes_batches = state.store.distributes_batches();

    let transport = select_transport()?;
    let postgres_url = cfg.storage.postgres_url.clone();
    let shared = gw_state::SharedConfig::new(cfg, state);
    let alert_task = gw_task::spawn_alert_dispatch(shared.clone());
    let avail_alert_task = gw_task::spawn_avail_alerts(shared.clone(), gw_task::AVAIL_ALERT_PERIOD);
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
            let src = config_source.clone();
            Box::pin(async move {
                let text = read_source_text(src.as_deref()).await?;
                GatewayConfig::from_yaml(&text).map_err(|e| e.to_string())
            }) as gw_views::ConfigFuture
        }),
    };
    let mut app_state = AppState::with_config(shared.clone(), transport, Some(loader));
    if let Some(store) = &config_store {
        app_state = app_state.with_config_store(store.clone());
    }

    // fleet batch drain: on a distributed store any instance claims submitted batches
    let (batch_shutdown_tx, batch_shutdown_rx) = tokio::sync::watch::channel(false);
    let batch_task = if distributes_batches {
        let offline = app_state.offline.clone();
        tracing::info!("batch drain loop started (distributed store)");
        Some(tokio::spawn(async move {
            offline
                .drain_until(BATCH_STALE_SECS, BATCH_POLL, batch_shutdown_rx)
                .await
        }))
    } else {
        None
    };

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
                tokio::time::sleep(CONFIG_FEED_RETRY).await;
            }
        });
    }

    // a SIGHUP triggers a live reload (storage-backend changes still need a restart)
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
            let body = prometheus.render();
            async move { body }
        }),
    );

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("gw listening on http://{addr}");

    // connect-info so the audit trail can root the source IP at the TCP peer
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal().await;
        batch_shutdown_tx.send_replace(true);
    })
    .await?;

    if let Some(task) = batch_task
        && let Err(e) = task.await
    {
        tracing::error!(error = %e, "batch drain task failed during shutdown");
    }
    gw_state::admission::flush_billing(&shared.load().state).await;
    quota_task.abort();
    purge_task.abort();
    rollup_task.abort();
    avail_task.abort();
    alert_task.abort();
    avail_alert_task.abort();
    tracing::info!("gw drained and exiting");
    if let Some(provider) = tracer_provider
        && let Err(e) = provider.shutdown()
    {
        tracing::error!(error = %e, "trace exporter shutdown");
    }
    Ok(())
}

/// Stdout logs under RUST_LOG, plus OTLP span export once OTEL_EXPORTER_OTLP_* names a collector.
fn init_tracing() -> anyhow::Result<Option<SdkTracerProvider>> {
    let log_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"))
        .add_directive(format!("{}=off", gw_views::TRACE_TARGET).parse()?);
    let provider = otlp_provider()?;
    let traces = provider.as_ref().map(|p| {
        tracing_opentelemetry::layer()
            .with_tracer(p.tracer("gw"))
            .with_filter(tracing_subscriber::filter::filter_fn(|m| {
                m.target() == gw_views::TRACE_TARGET
            }))
    });
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(log_filter))
        .with(traces)
        .init();
    Ok(provider)
}

fn otlp_provider() -> anyhow::Result<Option<SdkTracerProvider>> {
    if env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_none()
        && env::var_os("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_none()
    {
        return Ok(None);
    }
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()?;
    let mut resource = opentelemetry_sdk::Resource::builder();
    if env::var_os("OTEL_SERVICE_NAME").is_none() {
        resource = resource.with_service_name("gw");
    }
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource.build())
        .build();
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
    Ok(Some(provider))
}

async fn read_source_text(src: Option<&str>) -> Result<Cow<'static, str>, String> {
    match src {
        Some(path) => tokio::fs::read_to_string(path)
            .await
            .map(Cow::Owned)
            .map_err(|e| format!("read config {path}: {e}")),
        None => Ok(Cow::Borrowed(gw_config::DEFAULT_YAML)),
    }
}

// the GW_TRANSPORT env var: mock = zero egress, http = real HTTP, unset = mock:// in-process and real URLs over HTTP
fn select_transport() -> anyhow::Result<gw_engines::SharedTransport> {
    Ok(match env::var("GW_TRANSPORT").as_deref() {
        Ok("mock") => {
            tracing::info!("transport = mock (zero egress)");
            Arc::new(gw_engines::MockTransport)
        }
        Ok("http") => {
            tracing::info!("transport = http (accounts without an endpoint fail)");
            Arc::new(gw_engines::http_transport::HttpTransport::with_policies(
                Default::default(),
                Default::default(),
            )?)
        }
        _ => {
            tracing::info!("transport = auto (mock:// in-process, real URLs over HTTP)");
            Arc::new(
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
            Err(e) => tracing::error!(error = %e, "install SIGTERM handler failed"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("SIGINT received, draining"),
        _ = terminate => tracing::info!("SIGTERM received, draining"),
    }
}
