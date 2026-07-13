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
            store.batch_enqueue(&ak.ak, &model, &msgs).await
        } else {
            let job = store.batch_create(&ak.ak, &model, msgs.len()).await?;
            let this = self.clone();
            let (id, model) = (job.id.clone(), model.clone());
            tokio::spawn(async move { this.execute(&id, &ak, &model, msgs).await });
            Ok(job)
        }
    }

    /// Run every item of a claimed/submitted batch through the online DAG,
    /// writing results and the terminal status. Heartbeats between items so a
    /// distributed claim isn't judged stale mid-run.
    async fn execute(&self, id: &str, ak: &AkInfo, model: &str, items: Vec<Vec<ChatMsg>>) {
        let store = self.online.state().store.clone();
        if let Err(e) = store.batch_set_status(id, BatchStatus::Running).await {
            tracing::error!(error = %e, batch = %id, "batch status write failed");
        }
        // resume past items already recorded by a prior (crashed) executor, so
        // a reclaim re-runs and re-bills at most the one item that was in flight;
        // seed any_fail from those prior results so a pre-crash failure still
        // makes the resumed batch terminal-Failed
        let prior = store
            .batch_get(id)
            .await
            .ok()
            .flatten()
            .map(|j| j.results)
            .unwrap_or_default();
        let done_indices: std::collections::HashSet<usize> =
            prior.iter().map(|r| r.index).collect();
        let mut any_fail = prior.iter().any(|r| !r.ok);
        // heartbeat claimed_at while items run so a slow item isn't judged
        // stale and reclaimed by another instance mid-execution
        let hb = {
            let store = store.clone();
            let id = id.to_owned();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
                tick.tick().await;
                loop {
                    tick.tick().await;
                    let _ = store.batch_touch(&id).await;
                }
            })
        };
        for (index, messages) in items.into_iter().enumerate() {
            if done_indices.contains(&index) {
                continue; // already executed and billed before the reclaim
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
            any_fail |= !result.ok;
            if let Err(e) = store.batch_push_result(id, result).await {
                tracing::error!(error = %e, batch = %id, "batch result write failed");
            }
        }
        hb.abort();
        let done = if any_fail {
            BatchStatus::Failed
        } else {
            BatchStatus::Completed
        };
        if let Err(e) = store.batch_set_status(id, done).await {
            tracing::error!(error = %e, batch = %id, "batch status write failed");
        }
    }

    /// Fleet drain loop (distributed stores only): claim pending batches and
    /// execute them, requeuing on the way any batch whose executor went stale.
    /// Runs forever; poll interval applies only when the queue is empty.
    pub async fn drain_forever(&self, stale_secs: i64, poll: std::time::Duration) {
        let store = self.online.state().store.clone();
        loop {
            match store.batch_claim_pending(stale_secs).await {
                Ok(Some(job)) => {
                    let ak = match self.online.state().auth.authenticate(&job.ak).await {
                        Some(ak) => ak,
                        None => {
                            tracing::warn!(batch = %job.id, ak = %job.ak, "claimed batch's key is gone; failing it");
                            let _ = store.batch_set_status(&job.id, BatchStatus::Failed).await;
                            continue;
                        }
                    };
                    let items = store.batch_load_items(&job.id).await.unwrap_or_default();
                    self.execute(&job.id, &ak, &job.model, items).await;
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
