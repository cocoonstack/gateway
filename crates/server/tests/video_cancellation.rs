use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Router;
use axum::middleware::{Next, from_fn};
use gw_engines::transport::{
    HeaderMap, Transport, UpstreamBody, UpstreamRequest, UpstreamResponse,
};
use gw_state::{Store, UserBudgetStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

#[derive(Debug, Default)]
struct SlowVideo {
    started: Notify,
    release: Notify,
}

#[async_trait::async_trait]
impl Transport for SlowVideo {
    async fn send(&self, _: UpstreamRequest) -> gw_models::GResult<UpstreamResponse> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(UpstreamResponse {
            status: 200,
            body: UpstreamBody::Json(bytes::Bytes::from_static(
                br#"{"request_id":"audit-video-id"}"#,
            )),
            headers: HeaderMap::new(),
        })
    }
}

#[derive(Debug, Default)]
struct SlowBudget {
    armed: AtomicBool,
    started: Notify,
    release: Notify,
    inner: gw_state::MemoryUserBudgets,
}

#[async_trait::async_trait]
impl gw_state::UserBudgetStore for SlowBudget {
    async fn get(
        &self,
        tenant: &str,
        user: &str,
    ) -> gw_models::GResult<Option<gw_state::UserBudget>> {
        if self.armed.swap(false, Ordering::Relaxed) {
            self.started.notify_one();
            self.release.notified().await;
        }
        self.inner.get(tenant, user).await
    }

    async fn put(
        &self,
        tenant: &str,
        user: &str,
        budget: gw_state::UserBudget,
    ) -> gw_models::GResult<()> {
        self.inner.put(tenant, user, budget).await
    }

    async fn delete(&self, tenant: &str, user: &str) -> gw_models::GResult<bool> {
        self.inner.delete(tenant, user).await
    }

    async fn list(
        &self,
        tenant: &str,
        after: &str,
        limit: usize,
    ) -> gw_models::GResult<Vec<(String, gw_state::UserBudget)>> {
        self.inner.list(tenant, after, limit).await
    }
}

struct RequestDrop(Arc<Notify>);

impl Drop for RequestDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

async fn serve(app: Router) -> io::Result<(SocketAddr, Arc<Notify>, JoinHandle<io::Result<()>>)> {
    let dropped = Arc::new(Notify::new());
    let app = app.layer(from_fn({
        let dropped = dropped.clone();
        move |request, next: Next| {
            let guard = RequestDrop(dropped.clone());
            async move {
                let _guard = guard;
                next.run(request).await
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok((addr, dropped, server))
}

#[tokio::test]
async fn disconnected_video_submit_persists_handle() {
    let temp = tempfile::tempdir().unwrap();
    let mut cfg = gw_config::GatewayConfig::embedded_default().unwrap();
    cfg.storage.sqlite_path = temp.path().join("store.db").to_string_lossy().into_owned();
    let cfg = Arc::new(cfg);
    let state = Arc::new(gw_state::GatewayState::build(&cfg).await.unwrap());
    let transport = Arc::new(SlowVideo::default());
    let app = gw_views::app(gw_views::AppState::new(
        cfg,
        state.clone(),
        transport.clone(),
    ));
    let (addr, dropped, server) = serve(app).await.unwrap();
    let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = r#"{"model":"grok-imagine-video","prompt":"a cat","duration":2}"#;
    let request = format!(
        "POST /v1/videos/generations HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer ak-demo-123\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    client.write_all(request.as_bytes()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), transport.started.notified())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), dropped.notified())
        .await
        .unwrap();
    transport.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if state
                .store
                .video_job_get("audit-video-id")
                .await
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let job = state
        .store
        .video_job_get("audit-video-id")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.tenant, "default");
    assert_eq!(job.served_model, "grok-imagine-video");
    assert_eq!(state.store.ledger_snapshot(10).await.unwrap().0, 1);
    server.abort();
}

#[tokio::test]
async fn disconnected_video_poll_consumes_budget_once() {
    let temp = tempfile::tempdir().unwrap();
    let mut cfg = gw_config::GatewayConfig::embedded_default().unwrap();
    let sqlite_path = temp.path().join("store.db").to_string_lossy().into_owned();
    cfg.storage.sqlite_path = sqlite_path.clone();
    let cfg = Arc::new(cfg);
    let mut state = gw_state::GatewayState::build(&cfg).await.unwrap();
    let budgets = Arc::new(SlowBudget::default());
    budgets
        .put(
            "default",
            "audit-user",
            gw_state::UserBudget {
                daily_cost_quota_micros: gw_state::UserCap::Limit(100_000),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    state.user_budgets = budgets.clone();
    let state = Arc::new(state);
    state
        .store
        .video_job_put(gw_state::VideoJob {
            id: "vid-a-cat-0".into(),
            tenant: "default".into(),
            ak: gw_config::access_key_id("ak-demo-123"),
            product: "demo".into(),
            user_id: "audit-user".into(),
            model: "grok-imagine-video".into(),
            served_model: "grok-imagine-video".into(),
            account: "mock-xai-1".into(),
            unit_price_micros: 100_000,
            created_at_epoch_secs: gw_state::epoch_secs(),
        })
        .await
        .unwrap();
    let app = gw_views::app(gw_views::AppState::new(
        cfg,
        state.clone(),
        Arc::new(gw_engines::MockTransport),
    ));
    let (addr, dropped, server) = serve(app).await.unwrap();
    budgets.armed.store(true, Ordering::Relaxed);
    let request = format!(
        "GET /v1/videos/vid-a-cat-0 HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer ak-demo-123\r\nConnection: close\r\n\r\n"
    );
    let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
    client.write_all(request.as_bytes()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), budgets.started.notified())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), dropped.notified())
        .await
        .unwrap();
    budgets.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if state
                .governance
                .quota_used("cb:user:default:audit-user")
                .await
                == 200_000
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut retry = tokio::net::TcpStream::connect(addr).await.unwrap();
    retry.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), retry.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200"));
    let (_, rows) = state.store.ledger_snapshot(10).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cost_micros, 200_000);
    assert_eq!(
        state
            .governance
            .quota_used("cb:user:default:audit-user")
            .await,
        200_000
    );
    assert!(!state.store.video_job_settle("vid-a-cat-0").await.unwrap());
    let reopened = gw_state::SqliteStore::open(&sqlite_path).await.unwrap();
    assert!(!reopened.video_job_settle("vid-a-cat-0").await.unwrap());
    server.abort();
}
