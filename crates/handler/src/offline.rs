//! Offline batch orchestration: a local backend runs the batch on the receiving
//! instance, a distributed store persists items for any instance's drain loop.

use std::sync::Arc;

use gw_models::{BatchItem, GatewayRequest, ModelParamV2};
use gw_state::{AkInfo, BatchItemResult, BatchJob, BatchStatus};

use crate::OnlineHandler;

/// Batch orchestration built on top of the online handler.
#[derive(Clone)]
pub struct OfflineHandler {
    pub online: OnlineHandler,
}

impl OfflineHandler {
    pub fn new(online: OnlineHandler) -> Self {
        Self { online }
    }

    /// Submit a batch: local stores run it now on a task, distributed stores leave it for the drain loop.
    pub async fn submit(
        &self,
        ak: Arc<AkInfo>,
        model: String,
        items: Vec<BatchItem>,
    ) -> gw_models::GResult<BatchJob> {
        let store = self.online.state().store.clone();
        if store.distributed_batches() {
            // persist the EFFECTIVE user: execution, billing and erasure key on one identity
            let items: Vec<BatchItem> = items
                .into_iter()
                .map(|mut i| {
                    if let Some(owner) = ak.owner_override() {
                        i.user = owner.to_owned();
                    }
                    i
                })
                .collect();
            // atomic: the job becomes claimable only once all items are saved
            store
                .batch_enqueue(&ak.ak, &ak.tenant, &model, &items)
                .await
        } else {
            let job = store
                .batch_create(&ak.ak, &ak.tenant, &model, items.len())
                .await?;
            let this = self.clone();
            let id = job.id.clone();
            // an erasure landing after this instant must stop the captured items
            let captured_at = gw_state::epoch_millis();
            // claim 0: non-distributed store — no fence, the heartbeat is a no-op
            tokio::spawn(
                async move { this.execute(&id, &ak, &model, items, 0, captured_at).await },
            );
            Ok(job)
        }
    }

    /// `claim` fences this executor; it stops once a heartbeat reports the batch reclaimed.
    async fn execute(
        &self,
        id: &str,
        ak: &Arc<AkInfo>,
        model: &str,
        items: Vec<BatchItem>,
        claim: i64,
        captured_at: i64,
    ) {
        let store = self.online.state().store.clone();
        // unfenced, this write could resurrect a batch a stale worker no longer owns
        if claim == 0
            && let Err(e) = store.batch_set_status(id, BatchStatus::Running).await
        {
            tracing::error!(error = %e, batch = %id, "batch status write failed");
        }
        // skip items a prior executor recorded; a read failure fails the job (re-running re-bills)
        let prior = match store.batch_get(id).await {
            Ok(Some(job)) => job.results,
            Ok(None) => return, // the batch row vanished; nothing to run
            Err(e) => {
                tracing::error!(error = %e, batch = %id, "batch resume read failed; failing to avoid re-billing");
                // fenced: a reclaimed stale worker must not clobber the new owner's status
                let _ = store
                    .batch_set_status_owned(id, BatchStatus::Failed, claim)
                    .await;
                return;
            }
        };
        let done_indices: std::collections::HashSet<usize> =
            prior.iter().map(|r| r.index).collect();
        use std::sync::atomic::Ordering::Relaxed;
        // heartbeat: keeps a slow item from being judged stale, flips `lost` when the fence moves
        let lost = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hb = {
            let store = store.clone();
            let id = id.to_owned();
            let lost = lost.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
                tick.tick().await;
                loop {
                    tick.tick().await;
                    if let Ok(false) = store.batch_touch(&id, claim).await {
                        lost.store(true, Relaxed);
                        break;
                    }
                }
            })
        };
        for (index, mut item) in items.into_iter().enumerate() {
            if lost.load(Relaxed) {
                break; // reclaimed by another instance; stop running new items
            }
            if done_indices.contains(&index) {
                continue; // already executed and billed before the reclaim
            }
            // fence per item, fail CLOSED: at most the in-flight item double-runs (claim 0: none)
            if claim != 0 && !matches!(store.batch_touch(id, claim).await, Ok(true)) {
                lost.store(true, Relaxed);
                break;
            }
            // re-read before dispatch (fail CLOSED): an erasure while queued blanks the stored item
            if store.distributed_batches() {
                match store.batch_item_snapshot(id, index).await {
                    Ok(Some(fresh)) => item = fresh,
                    Ok(None) | Err(_) => {
                        let result = failed_item(
                            index,
                            "item unavailable at dispatch".into(),
                            ak.attributed_user(&item.user).to_owned(),
                        );
                        if let Err(e) = store.batch_push_result(id, result).await {
                            tracing::error!(error = %e, batch = %id, "batch result write failed");
                        }
                        continue;
                    }
                }
            }
            let user = ak.attributed_user(&item.user).to_owned();
            // local backends keep no item rows: the erasure marker stops the rest (fail closed)
            let erased_mid_batch = !store.distributed_batches()
                && store
                    .user_erased_since(&ak.tenant, &user, captured_at)
                    .await
                    .unwrap_or(true);
            if erased_mid_batch || item.messages.is_empty() {
                let result = failed_item(index, "item content erased".into(), user);
                if let Err(e) = store.batch_push_result(id, result).await {
                    tracing::error!(error = %e, batch = %id, "batch result write failed");
                }
                continue;
            }
            let request = GatewayRequest {
                is_online: false,
                message: item.messages,
                user_id: (!item.user.is_empty()).then_some(item.user),
                model_param_v2: Some(ModelParamV2::with_name(
                    gw_consts::Protocol::OpenaiChat,
                    model.to_owned(),
                )),
                ..Default::default()
            };
            // each item on its own task so a pipeline panic fails the item, not the batch
            let online = self.online.clone();
            let item_ak = ak.clone();
            let ran = tokio::spawn(async move { online.run(request, item_ak).await }).await;
            let result = match ran {
                Ok(Ok(ctx)) => match ctx.outcome {
                    Some(out) => BatchItemResult {
                        index,
                        ok: true,
                        message: out.response.message,
                        total_tokens: out.response.total_tokens,
                        user,
                    },
                    None => failed_item(index, "pipeline produced no outcome".into(), user),
                },
                Ok(Err(e)) => failed_item(index, e.to_string(), user),
                Err(join_err) => failed_item(index, format!("item task failed: {join_err}"), user),
            };
            // if we lost the claim mid-run, don't persist — the new owner is authoritative
            if lost.load(Relaxed) {
                break;
            }
            if let Err(e) = store.batch_push_result(id, result).await {
                tracing::error!(error = %e, batch = %id, "batch result write failed");
            }
        }
        hb.abort();
        if lost.load(Relaxed) {
            return; // the reclaiming instance owns the terminal status now
        }
        // fenced terminal status, derived atomically from the persisted results
        if let Err(e) = store.batch_finalize(id, claim).await {
            tracing::error!(error = %e, batch = %id, "batch finalize failed");
        }
    }

    /// Fleet drain loop for distributed stores: stop claiming on shutdown, finish the claimed batch.
    pub async fn drain_until(
        &self,
        stale_secs: i64,
        poll: std::time::Duration,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        let store = self.online.state().store.clone();
        loop {
            if *shutdown.borrow() {
                return;
            }
            let claimed = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if stopping(changed, &shutdown) {
                        return;
                    }
                    continue;
                }
                claimed = store.batch_claim_pending(stale_secs) => claimed,
            };
            match claimed {
                Ok(Some((job, claim))) => {
                    // a key revoked/banned/expired since submit stops its queued work
                    let ak = match self.online.state().auth.authenticate(&job.ak).await {
                        Some(ak)
                            if ak.status_at(gw_state::epoch_secs())
                                == gw_state::KeyStatus::Active =>
                        {
                            ak
                        }
                        _ => {
                            let ak_id = gw_state::access_key_fingerprint(&job.ak);
                            tracing::warn!(batch = %job.id, ak_id, "claimed batch's key is gone or inactive; failing it");
                            let _ = store
                                .batch_set_status_owned(&job.id, BatchStatus::Failed, claim)
                                .await;
                            continue;
                        }
                    };
                    // a load failure must fail the job, not silently complete with zero results
                    let items = match store.batch_load_items(&job.id).await {
                        Ok(items) => items,
                        Err(e) => {
                            tracing::error!(error = %e, batch = %job.id, "batch item load failed; failing the job");
                            let _ = store
                                .batch_set_status_owned(&job.id, BatchStatus::Failed, claim)
                                .await;
                            continue;
                        }
                    };
                    self.execute(
                        &job.id,
                        &ak,
                        &job.model,
                        items,
                        claim,
                        gw_state::epoch_millis(),
                    )
                    .await;
                }
                Ok(None) => {
                    if pause_or_stop(&mut shutdown, poll).await {
                        return;
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "batch claim failed; backing off");
                    if pause_or_stop(&mut shutdown, poll).await {
                        return;
                    }
                }
            }
        }
    }
}

fn stopping(
    changed: Result<(), tokio::sync::watch::error::RecvError>,
    shutdown: &tokio::sync::watch::Receiver<bool>,
) -> bool {
    changed.is_err() || *shutdown.borrow()
}

async fn pause_or_stop(
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
    poll: std::time::Duration,
) -> bool {
    tokio::select! {
        biased;
        changed = shutdown.changed() => stopping(changed, shutdown),
        _ = tokio::time::sleep(poll) => false,
    }
}

fn failed_item(index: usize, message: String, user: String) -> BatchItemResult {
    BatchItemResult {
        index,
        ok: false,
        message,
        total_tokens: 0,
        user,
    }
}
