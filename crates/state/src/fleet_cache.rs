//! A cache of Postgres rows kept fresh across a fleet: every write NOTIFYs the
//! changed entry and each instance drops it; expiry only backs up a lost NOTIFY.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use gw_models::GResult;
use sqlx::postgres::PgListener;

/// The payload that drops every entry (no entry key is a bare `*`).
pub(crate) const FLUSH_ALL: &str = "*";

const IDLE_EXPIRY: Duration = Duration::from_secs(300);
const MAX_AGE: Duration = Duration::from_secs(3_600);
const RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// One process's view of a fleet-shared table, invalidated by its channel.
pub(crate) struct FleetCache<V> {
    channel: &'static str,
    shared: Arc<Shared<V>>,
    listener: tokio::task::JoinHandle<()>,
}

impl<V: Clone + Send + Sync + 'static> FleetCache<V> {
    /// Subscribes before returning, so no write after the first read goes unheard.
    pub(crate) async fn listen(url: &str, channel: &'static str, capacity: u64) -> GResult<Self> {
        let listener = subscribe(url, channel)
            .await
            .map_err(|e| crate::sqlx_err("listen on cache channel", e))?;
        let shared = Arc::new(Shared {
            entries: moka::future::Cache::builder()
                .max_capacity(capacity)
                .time_to_idle(IDLE_EXPIRY)
                .time_to_live(MAX_AGE)
                .build(),
            epoch: AtomicU64::new(0),
        });
        let listener = tokio::spawn(follow(
            listener,
            url.to_owned(),
            channel,
            Arc::clone(&shared),
        ));
        Ok(Self {
            channel,
            shared,
            listener,
        })
    }

    pub(crate) async fn get_with<E: Send + Sync + 'static>(
        &self,
        key: &str,
        fetch: impl Future<Output = Result<V, E>>,
    ) -> Result<V, Arc<E>> {
        let fetched_at = AtomicU64::new(u64::MAX);
        let loaded = self
            .shared
            .entries
            .try_get_with_by_ref(key, async {
                fetched_at.store(self.shared.epoch.load(Ordering::Acquire), Ordering::Release);
                fetch.await
            })
            .await;
        // checked after publication: an invalidation before the insert had no entry to evict
        let start = fetched_at.load(Ordering::Acquire);
        if start != u64::MAX && self.shared.epoch.load(Ordering::Acquire) != start {
            self.shared.entries.invalidate(key).await;
        }
        loaded
    }

    /// Commits `tx` with a NOTIFY for `key` (peers hear it once committed) and drops `key` here.
    pub(crate) async fn commit(
        &self,
        mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
        key: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(self.channel)
            .bind(key)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.shared.invalidate(key).await;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn flush(&self) {
        self.shared.invalidate(FLUSH_ALL).await;
    }
}

impl<V> fmt::Debug for FleetCache<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FleetCache")
    }
}

impl<V> Drop for FleetCache<V> {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

struct Shared<V> {
    entries: moka::future::Cache<String, V>,
    epoch: AtomicU64,
}

impl<V: Clone + Send + Sync + 'static> Shared<V> {
    async fn invalidate(&self, key: &str) {
        self.epoch.fetch_add(1, Ordering::Release);
        if key == FLUSH_ALL {
            self.entries.invalidate_all();
        } else {
            self.entries.invalidate(key).await;
        }
    }
}

/// A dedicated connection: holding one of the store's pool would starve a pool of one.
async fn subscribe(url: &str, channel: &str) -> Result<PgListener, sqlx::Error> {
    let mut listener = PgListener::connect(url).await?;
    listener.listen(channel).await?;
    Ok(listener)
}

async fn follow<V: Clone + Send + Sync + 'static>(
    mut listener: PgListener,
    url: String,
    channel: &'static str,
    shared: Arc<Shared<V>>,
) {
    loop {
        match listener.try_recv().await {
            Ok(Some(n)) => shared.invalidate(n.payload()).await,
            // reconnected after a loss: a write in the gap went unheard
            Ok(None) => shared.invalidate(FLUSH_ALL).await,
            // flushed on loss and again once listening: an entry cached in between may be stale
            Err(e) => {
                tracing::warn!(error = %e, "cache listener lost; flushing and relistening");
                shared.invalidate(FLUSH_ALL).await;
                listener = loop {
                    tokio::time::sleep(RETRY_BACKOFF).await;
                    match subscribe(&url, channel).await {
                        Ok(l) => break l,
                        Err(e) => tracing::warn!(error = %e, "cache relisten failed"),
                    }
                };
                shared.invalidate(FLUSH_ALL).await;
            }
        }
    }
}
