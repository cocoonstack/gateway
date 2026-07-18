//! Local background tasks: periodic AK daily quota reset, retained-content
//! purge, the usage rollup, availability flush/alerting, and the alert
//! webhook dispatcher. Batch job execution lives in gw-handler::offline
//! (spawned on submit) and needs no separate poller.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use gw_state::{GatewayState, SharedConfig};

/// The production period: once a day.
pub const DAILY: Duration = Duration::from_secs(24 * 60 * 60);
/// Retained content is swept for expiry this often.
pub const PURGE_PERIOD: Duration = Duration::from_secs(60 * 60);
/// Ledger minutes are rolled into the durable usage buckets this often.
pub const ROLLUP_PERIOD: Duration = Duration::from_secs(60);
/// Buffered per-model availability counts are flushed to the store this often.
pub const AVAIL_FLUSH_PERIOD: Duration = Duration::from_secs(2);
/// Model availability is re-classified for alerting this often.
pub const AVAIL_ALERT_PERIOD: Duration = Duration::from_secs(60);

/// Spawn the daily quota reset loop. Returns the join handle (abort to stop).
/// `period` is configurable so tests don't wait 24h.
pub fn spawn_quota_reset(
    state: Arc<GatewayState>,
    period: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await; // first tick fires immediately; skip it
        loop {
            tick.tick().await;
            state.governance.quota_reset_all().await;
            tracing::info!(target: "task", "quota_reset: all AK daily counters cleared");
        }
    })
}

/// Spawn the retained-content purge loop: deletes content rows whose retention
/// window has elapsed. Returns the join handle (abort to stop).
pub fn spawn_content_purge(
    state: Arc<GatewayState>,
    period: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await;
        loop {
            tick.tick().await;
            match state.store.content_purge(gw_state::epoch_secs()).await {
                Ok(n) if n > 0 => {
                    tracing::info!(target: "task", purged = n, "content purge")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "content purge failed"),
            }
        }
    })
}

/// Spawn the usage-rollup loop: folds completed ledger minutes into the
/// durable per-user buckets. Returns the join handle (abort to stop).
pub fn spawn_usage_rollup(
    state: Arc<GatewayState>,
    period: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await;
        loop {
            tick.tick().await;
            if let Err(e) = state
                .store
                .usage_rollup_advance(gw_state::epoch_secs())
                .await
            {
                tracing::warn!(error = %e, "usage rollup failed");
            }
        }
    })
}

/// Spawn the availability flush loop: drains the in-process per-model counters
/// into the minute-bucket store, keeping the claim path free of network hops.
pub fn spawn_avail_flush(
    state: Arc<GatewayState>,
    period: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await;
        loop {
            tick.tick().await;
            state.avail.flush().await;
        }
    })
}

/// Drain the alert bus and POST each event to the configured webhook, muting
/// repeats of the same (kind, subject) within the dedup window. Exits when the
/// bus's receiver was already taken or every sender is gone.
pub fn spawn_alert_dispatch(shared: SharedConfig) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let Some(mut rx) = shared.load().state.alerts.take_receiver() else {
            return;
        };
        let client = reqwest::Client::new();
        let mut sent: HashMap<String, Instant> = HashMap::new();
        while let Some(ev) = rx.recv().await {
            let conf = shared.load().cfg.alerts.clone();
            let Some(url) = conf.webhook_url() else {
                continue;
            };
            let dedup = Duration::from_secs(conf.dedup_seconds);
            if !should_send(&mut sent, format!("{}:{}", ev.kind, ev.subject), dedup) {
                continue;
            }
            let body = serde_json::json!({
                "kind": ev.kind,
                "subject": ev.subject,
                "detail": ev.detail,
                "at_epoch_secs": ev.at_epoch_secs,
            });
            let post = client
                .post(&url)
                .header("content-type", "application/json")
                .body(body.to_string())
                .timeout(Duration::from_secs(10))
                .send()
                .await;
            match post {
                Ok(resp) if resp.status().is_success() => {}
                Ok(resp) => {
                    tracing::warn!(status = %resp.status(), kind = ev.kind, "alert webhook rejected")
                }
                Err(e) => tracing::warn!(error = %e, kind = ev.kind, "alert webhook unreachable"),
            }
        }
    })
}

/// Spawn the availability alert sweep: re-classify every model each period
/// and emit an alert on a state transition.
pub fn spawn_avail_alerts(shared: SharedConfig, period: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await;
        let mut last = HashMap::new();
        loop {
            tick.tick().await;
            avail_alert_sweep(&shared, &mut last).await;
        }
    })
}

/// One sweep round, factored out so tests drive it directly.
async fn avail_alert_sweep(
    shared: &SharedConfig,
    last: &mut HashMap<String, gw_state::AvailState>,
) {
    let snap = shared.load();
    let st = &snap.cfg.stability;
    let until = gw_state::epoch_secs() / 60;
    let since = until - (st.availability_window_minutes - 1);
    let counts = futures::future::join_all(snap.cfg.models.iter().map(|m| async {
        (
            &m.name,
            snap.state.avail.window(&m.name, since, until).await,
        )
    }))
    .await;
    for (name, (ok, err)) in counts {
        let cur = gw_state::classify(
            ok,
            err,
            st.availability_min_samples,
            st.unstable_error_rate,
            st.unavailable_error_rate,
        );
        if let Some(prev) = last.insert(name.clone(), cur)
            && prev != cur
        {
            snap.state.alerts.emit(
                "model_availability",
                name.clone(),
                format!("{} -> {}", prev.as_str(), cur.as_str()),
            );
        }
    }
}

/// Whether `key` is outside its dedup window (records the send when so).
/// Bounded: past 1024 entries the elapsed ones are swept, so a long-lived
/// dispatcher with many distinct subjects can't grow without limit.
fn should_send(sent: &mut HashMap<String, Instant>, key: String, window: Duration) -> bool {
    let now = Instant::now();
    if sent.len() > 1024 {
        sent.retain(|_, at| now.duration_since(*at) < window);
    }
    match sent.get(&key) {
        Some(at) if now.duration_since(*at) < window => false,
        _ => {
            sent.insert(key, now);
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_window_mutes_repeats() {
        let mut sent = HashMap::new();
        let w = Duration::from_secs(60);
        assert!(should_send(&mut sent, "a:x".into(), w));
        assert!(!should_send(&mut sent, "a:x".into(), w), "muted repeat");
        assert!(should_send(&mut sent, "a:y".into(), w), "distinct subject");
        assert!(
            should_send(&mut sent, "a:x".into(), Duration::ZERO),
            "elapsed window resends"
        );
    }

    #[tokio::test]
    async fn avail_sweep_alerts_on_transition_only() {
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: m, protocol: openai-chat}, {name: rt, protocol: realtime}]\nstability: {availability_min_samples: 2}";
        let cfg = std::sync::Arc::new(gw_config::GatewayConfig::from_yaml(yaml).unwrap());
        let state = std::sync::Arc::new(GatewayState::from_config(&cfg));
        let shared = SharedConfig::new(cfg, state.clone());
        let mut rx = state.alerts.take_receiver().expect("receiver");
        let mut last = HashMap::new();
        avail_alert_sweep(&shared, &mut last).await;
        avail_alert_sweep(&shared, &mut last).await;
        assert!(rx.try_recv().is_err(), "steady no_data emits nothing");
        for _ in 0..4 {
            state.avail.record("m", false);
        }
        state.avail.flush().await;
        avail_alert_sweep(&shared, &mut last).await;
        let ev = rx.try_recv().expect("transition alert");
        assert_eq!((ev.kind, ev.subject.as_str()), ("model_availability", "m"));
        assert_eq!(ev.detail, "no_data -> unavailable");
        avail_alert_sweep(&shared, &mut last).await;
        assert!(rx.try_recv().is_err(), "no repeat while the state holds");
    }

    #[tokio::test]
    async fn alert_dispatch_posts_to_the_webhook() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = sock.read(&mut buf).await.unwrap();
            sock.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
            String::from_utf8_lossy(&buf[..n]).into_owned()
        });
        // SAFETY: unique var name for this test; no concurrent reader of it.
        unsafe {
            std::env::set_var("GW_TEST_ALERT_URL", format!("http://{addr}/hook"));
        }
        let yaml = "listen: {host: h, port: 1}\nalerts: {webhook_url_env: GW_TEST_ALERT_URL}";
        let cfg = std::sync::Arc::new(gw_config::GatewayConfig::from_yaml(yaml).unwrap());
        let state = std::sync::Arc::new(GatewayState::from_config(&cfg));
        let shared = SharedConfig::new(cfg, state.clone());
        let task = spawn_alert_dispatch(shared);
        state
            .alerts
            .emit("abuse_suspend", "k9".into(), "3 rejects".into());
        let request = served.await.unwrap();
        task.abort();
        assert!(request.starts_with("POST /hook"));
        assert!(request.contains(r#""kind":"abuse_suspend""#), "{request}");
        assert!(request.contains(r#""subject":"k9""#));
    }

    #[tokio::test]
    async fn quota_reset_clears_counters() {
        let state = Arc::new(GatewayState::default());
        state.governance.quota_consume("ak-x", 42).await;
        assert_eq!(state.governance.quota_used("ak-x").await, 42);
        let handle = spawn_quota_reset(state.clone(), Duration::from_millis(20));
        for _ in 0..50 {
            if state.governance.quota_used("ak-x").await == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(state.governance.quota_used("ak-x").await, 0);
        handle.abort();
    }
}
