//! Offline batch orchestration: local backends execute a batch on the receiving
//! instance's background task; a distributed store (Postgres) only persists the
//! items and a fleet drain loop on any instance claims and runs them.

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

    /// Submit a batch. Local stores execute it now on a background task;
    /// distributed stores leave it pending for the fleet drain loop. Items run
    /// through the SAME online DAG (billed per item; the request cache is bypassed).
    pub async fn submit(
        &self,
        ak: AkInfo,
        model: String,
        items: Vec<BatchItem>,
    ) -> gw_models::GResult<BatchJob> {
        let store = self.online.state().store.clone();
        if store.distributed_batches() {
            // persist the EFFECTIVE user (owner overrides the hint): execution,
            // billing, and erasure must all key on the same identity
            let items: Vec<BatchItem> = items
                .into_iter()
                .map(|mut i| {
                    i.user = ak.attributed_user(&i.user).to_owned();
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
            let (id, model) = (job.id.clone(), model);
            // items are captured HERE: an erasure landing after this instant
            // must stop them, so the marker comparison point is submission,
            // not the spawned executor's first poll
            let captured_at = gw_state::epoch_millis();
            // claim 0: non-distributed store — no fence, the heartbeat is a no-op
            tokio::spawn(
                async move { this.execute(&id, &ak, &model, items, 0, captured_at).await },
            );
            Ok(job)
        }
    }

    /// Run every item of a batch through the online DAG, writing results and
    /// the terminal status, heartbeating between items. `claim` is the fence
    /// token (0 for the in-process path); once a heartbeat reports the batch
    /// reclaimed, this executor stops rather than double-running items.
    async fn execute(
        &self,
        id: &str,
        ak: &AkInfo,
        model: &str,
        items: Vec<BatchItem>,
        claim: i64,
        captured_at: i64,
    ) {
        let store = self.online.state().store.clone();
        // the distributed claim already set status=running with the fence bump;
        // only the in-process path needs this write — unfenced on the distributed
        // path it could resurrect a batch a stale worker no longer owns
        if claim == 0
            && let Err(e) = store.batch_set_status(id, BatchStatus::Running).await
        {
            tracing::error!(error = %e, batch = %id, "batch status write failed");
        }
        // skip items a prior executor already recorded (a reclaim re-runs at most
        // the in-flight one); a read failure fails the job — re-running re-bills
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
        // background heartbeat: refreshes claimed_at so a slow item isn't judged
        // stale, and flips `lost` when the fence stops matching; the per-item
        // synchronous touch below is what bounds a double-run
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
            // synchronous fence before each item: if reclaimed while stalled, stop
            // before billing the next item — at most the in-flight one double-runs
            // (the resume/dedup path tolerates that). Fail CLOSED: a touch that
            // can't confirm ownership stops us. claim 0 = in-process, unfenced.
            if claim != 0 && !matches!(store.batch_touch(id, claim).await, Ok(true)) {
                lost.store(true, Relaxed);
                break;
            }
            // re-read the stored copy just before dispatch: an erasure that
            // landed while this batch sat queued blanks the persisted item.
            // Fail CLOSED — a read error or vanished row can't prove the item
            // wasn't erased, so the stale pre-load copy never dispatches
            if store.distributed_batches() {
                match store.batch_item_snapshot(id, index).await {
                    Ok(Some(fresh)) => item = fresh,
                    Ok(None) | Err(_) => {
                        let result = BatchItemResult {
                            index,
                            ok: false,
                            message: "item unavailable at dispatch".into(),
                            total_tokens: 0,
                            user: ak.attributed_user(&item.user).to_owned(),
                        };
                        if let Err(e) = store.batch_push_result(id, result).await {
                            tracing::error!(error = %e, batch = %id, "batch result write failed");
                        }
                        continue;
                    }
                }
            }
            let user = ak.attributed_user(&item.user).to_owned();
            // local backends don't persist items, so a mid-batch erasure can't
            // blank them — the erasure marker stops the user's remaining items
            // (fail closed on a marker read error). An erased item must not
            // run: fail it instead of sending an erased prompt upstream
            let erased_mid_batch = !store.distributed_batches()
                && store
                    .user_erased_since(&ak.tenant, &user, captured_at)
                    .await
                    .unwrap_or(true);
            if erased_mid_batch || item.messages.is_empty() {
                let result = BatchItemResult {
                    index,
                    ok: false,
                    message: "item content erased".into(),
                    total_tokens: 0,
                    user,
                };
                if let Err(e) = store.batch_push_result(id, result).await {
                    tracing::error!(error = %e, batch = %id, "batch result write failed");
                }
                continue;
            }
            let request = GatewayRequest {
                is_online: false,
                ak: ak.ak.clone(),
                message: item.messages,
                user_id: (!item.user.is_empty()).then_some(item.user),
                model_param_v2: Some(ModelParamV2::with_name(
                    gw_consts::Protocol::OpenaiChat,
                    model.to_owned(),
                )),
                ..Default::default()
            };
            // each item on its own task so a pipeline panic fails that item
            // instead of wedging the batch in Running forever
            let online = self.online.clone();
            let item_ak = ak.clone();
            let ran = tokio::spawn(async move { online.run(request, item_ak).await }).await;
            let fail = |message: String| BatchItemResult {
                index,
                ok: false,
                message,
                total_tokens: 0,
                user: user.clone(),
            };
            let result = match ran {
                Ok(Ok(ctx)) => match ctx.outcome {
                    Some(out) => BatchItemResult {
                        index,
                        ok: true,
                        message: out.response.message,
                        total_tokens: out.response.total_tokens,
                        user: user.clone(),
                    },
                    None => fail("pipeline produced no outcome".into()),
                },
                Ok(Err(e)) => fail(e.to_string()),
                Err(join_err) => fail(format!("item task failed: {join_err}")),
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

    /// Fleet drain loop (distributed stores only): claim pending batches and
    /// execute them, requeuing on the way any batch whose executor went stale.
    /// Runs forever; poll interval applies only when the queue is empty.
    pub async fn drain_forever(&self, stale_secs: i64, poll: std::time::Duration) {
        let store = self.online.state().store.clone();
        loop {
            match store.batch_claim_pending(stale_secs).await {
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
                            tracing::warn!(batch = %job.id, ak = %job.ak, "claimed batch's key is gone or inactive; failing it");
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
                Ok(None) => tokio::time::sleep(poll).await,
                Err(e) => {
                    tracing::warn!(error = %e, "batch claim failed; backing off");
                    tokio::time::sleep(poll).await;
                }
            }
        }
    }
}
