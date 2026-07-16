//! Local background tasks: periodic AK daily quota reset and retained-content
//! purge. Batch job execution lives in gw-handler::offline (spawned on submit)
//! and needs no separate poller.

use std::sync::Arc;
use std::time::Duration;

use gw_state::GatewayState;

/// The production period: once a day.
pub const DAILY: Duration = Duration::from_secs(24 * 60 * 60);
/// Retained content is swept for expiry this often.
pub const PURGE_PERIOD: Duration = Duration::from_secs(60 * 60);
/// Ledger minutes are rolled into the durable usage buckets this often.
pub const ROLLUP_PERIOD: Duration = Duration::from_secs(60);

/// Spawn the daily quota reset loop. Returns the join handle (abort to stop).
/// `period` is configurable so tests don't wait 24h.
pub fn spawn_quota_reset(
    state: Arc<GatewayState>,
    period: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
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

#[cfg(test)]
mod tests {
    use super::*;

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
