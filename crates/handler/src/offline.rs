//! Offline batch orchestration.
//!
//! Submission runs in the background immediately; status/results are
//! queryable in-process; no external queue is involved.

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

    /// Submit a batch: registers the job and processes it on a background task.
    /// Items run through the SAME online DAG (billing/quota/limits apply per
    /// item; the request cache is bypassed so that stays true).
    pub async fn submit(
        &self,
        ak: AkInfo,
        model: String,
        items: Vec<BatchItem>,
    ) -> gw_models::GResult<BatchJob> {
        let job = self
            .online
            .state()
            .store
            .batch_create(&ak.ak, &model, items.len())
            .await?;
        let id = job.id.clone();
        let this = self.clone();
        tokio::spawn(async move {
            let state = this.online.state();
            let store = &state.store;
            if let Err(e) = store.batch_set_status(&id, BatchStatus::Running).await {
                tracing::error!(error = %e, batch = %id, "batch status write failed");
            }
            let mut any_fail = false;
            for (index, item) in items.into_iter().enumerate() {
                let request = GatewayRequest {
                    is_online: false, // offline path
                    ak: ak.ak.clone(),
                    message: item.messages,
                    model_param_v2: Some(ModelParamV2::with_name(
                        gw_consts::Protocol::OpenaiChat, // rewritten by the resolve_model node
                        model.clone(),
                    )),
                    ..Default::default()
                };
                // Each item runs on its own task so a panic inside the pipeline
                // fails that item instead of unwinding past the terminal
                // status write and wedging the batch in Running forever.
                let online = this.online.clone();
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
                if let Err(e) = store.batch_push_result(&id, result).await {
                    tracing::error!(error = %e, batch = %id, "batch result write failed");
                }
            }
            let done = if any_fail {
                BatchStatus::Failed
            } else {
                BatchStatus::Completed
            };
            if let Err(e) = store.batch_set_status(&id, done).await {
                tracing::error!(error = %e, batch = %id, "batch status write failed");
            }
        });
        Ok(job)
    }
}
