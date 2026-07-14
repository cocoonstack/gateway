//! Offline batch orchestration.
//!
//! Local backends execute a submitted batch on the receiving instance's
//! background task. With a distributed store (Postgres) submission only
//! persists the items; a fleet drain loop on any instance claims and runs
//! them, so execution survives the submitter restarting.

use gw_models::{ChatMsg, GatewayRequest, ModelParamV2};
use gw_state::{AkInfo, BatchItemResult, BatchJob, BatchStatus};

use crate::OnlineHandler;

/// One batch item: a self-contained message list.
#[derive(Debug, Clone)]
pub struct BatchItem {
    pub messages: Vec<ChatMsg>,
}

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
    /// distributed stores persist the items and leave it pending for the
    /// fleet drain loop. Items run through the SAME online DAG (billing/quota/
    /// limits apply per item; the request cache is bypassed).
    pub async fn submit(
        &self,
        ak: AkInfo,
        model: String,
        items: Vec<BatchItem>,
    ) -> gw_models::GResult<BatchJob> {
        let store = self.online.state().store.clone();
        let msgs: Vec<Vec<ChatMsg>> = items.into_iter().map(|i| i.messages).collect();
        if store.distributed_batches() {
            // atomic: the job becomes claimable only once all items are saved
            store.batch_enqueue(&ak.ak, &ak.tenant, &model, &msgs).await
        } else {
            let job = store
                .batch_create(&ak.ak, &ak.tenant, &model, msgs.len())
                .await?;
            let this = self.clone();
            let (id, model) = (job.id.clone(), model.clone());
            // claim 0: in-process runs on a non-distributed store, so there is
            // no fence token and the heartbeat is a no-op
            tokio::spawn(async move { this.execute(&id, &ak, &model, msgs, 0).await });
            Ok(job)
        }
    }

    /// Run every item of a claimed/submitted batch through the online DAG,
    /// writing results and the terminal status. Heartbeats between items so a
    /// distributed claim isn't judged stale mid-run. `claim` is the fence token
    /// from [`gw_state::Store::batch_claim_pending`] (0 for the in-process path);
    /// if a heartbeat reports the batch was reclaimed, this executor stops rather
    /// than double-running items or clobbering the new owner's status.
    async fn execute(
        &self,
        id: &str,
        ak: &AkInfo,
        model: &str,
        items: Vec<Vec<ChatMsg>>,
        claim: i64,
    ) {
        let store = self.online.state().store.clone();
        // the distributed claim already set status=running atomically with the
        // fence bump; only the in-process path (claim 0, created 'pending') needs
        // this write. Writing it unfenced on the distributed path could resurrect
        // a batch a stale worker no longer owns back to Running.
        if claim == 0
            && let Err(e) = store.batch_set_status(id, BatchStatus::Running).await
        {
            tracing::error!(error = %e, batch = %id, "batch status write failed");
        }
        // resume past items already recorded by a prior (crashed) executor, so a
        // reclaim re-runs at most the one item that was in flight. A read failure
        // means we can't know what's already done — re-running everything would
        // re-bill, so fail the job instead.
        let prior = match store.batch_get(id).await {
            Ok(Some(job)) => job.results,
            Ok(None) => return, // the batch row vanished; nothing to run
            Err(e) => {
                tracing::error!(error = %e, batch = %id, "batch resume read failed; failing to avoid re-billing");
                // fenced: a reclaimed stale worker that trips this must not clobber
                // the new owner's status
                let _ = store
                    .batch_set_status_owned(id, BatchStatus::Failed, claim)
                    .await;
                return;
            }
        };
        let done_indices: std::collections::HashSet<usize> =
            prior.iter().map(|r| r.index).collect();
        use std::sync::atomic::Ordering::Relaxed;
        // A background heartbeat refreshes claimed_at so a single slow item isn't
        // judged stale mid-run; it also flips `lost` if the fence token stops
        // matching (another instance reclaimed us). Long-item liveness is its job
        // — the per-item synchronous touch below is what bounds a double-run.
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
        for (index, messages) in items.into_iter().enumerate() {
            if lost.load(Relaxed) {
                break; // reclaimed by another instance; stop running new items
            }
            if done_indices.contains(&index) {
                continue; // already executed and billed before the reclaim
            }
            // synchronous fence before starting each item: if we were reclaimed
            // while stalled, learn it here and stop before running/billing the
            // next item — so at most the one item already in flight double-runs
            // (the resume/dedup path tolerates exactly that). Fail CLOSED: a
            // touch that can't confirm ownership (reclaimed OR the store is
            // unreachable) stops us, so a partitioned worker can't keep charging
            // items it may no longer own. claim 0 = in-process, unfenced (the
            // local store's touch is a no-op that returns Ok(true)).
            if claim != 0 && !matches!(store.batch_touch(id, claim).await, Ok(true)) {
                lost.store(true, Relaxed);
                break;
            }
            let request = GatewayRequest {
                is_online: false,
                ak: ak.ak.clone(),
                message: messages,
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
            let result = match ran {
                Ok(Ok(ctx)) => match ctx.outcome {
                    Some(out) => BatchItemResult {
                        index,
                        ok: true,
                        message: out.response.message,
                        total_tokens: out.response.total_tokens,
                    },
                    None => BatchItemResult {
                        index,
                        ok: false,
                        message: "pipeline produced no outcome".into(),
                        total_tokens: 0,
                    },
                },
                Ok(Err(e)) => BatchItemResult {
                    index,
                    ok: false,
                    message: e.to_string(),
                    total_tokens: 0,
                },
                Err(join_err) => BatchItemResult {
                    index,
                    ok: false,
                    message: format!("item task failed: {join_err}"),
                    total_tokens: 0,
                },
            };
            // don't persist a result for a batch we've already lost — the new
            // owner's run of this item is authoritative
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
        // Finalize: derive the terminal status from the PERSISTED result set and
        // set it atomically, fenced on the claim. On the distributed backend this
        // is serialized with result writes, so no late result can land after the
        // decision and contradict it.
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
                    // a load failure must fail the job, not silently complete it
                    // with zero results (unwrap_or_default would).
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
                    self.execute(&job.id, &ak, &job.model, items, claim).await;
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
