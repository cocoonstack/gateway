//! HTTP view layer.
//!
//! Layer L5. Routes:
//!   GET  /health                    liveness
//!   GET  /v1/models                 configured public models
//!   POST /v1/chat/completions       OpenAI-compatible (stream + non-stream)
//!   POST /v1/completions            OpenAI legacy text completions (non-stream)
//!   POST /v1/messages               Anthropic-compatible (stream + non-stream)
//!   POST /v1/responses              OpenAI Responses API (stream + non-stream)
//!   POST /v1/embeddings             OpenAI-compatible embeddings
//!   POST /v1/images/generations     OpenAI-compatible image generation
//!   POST /v1/images/edits           OpenAI-compatible image edit (source + mask)
//!   POST /v1/audio/speech           TTS (returns audio bytes)
//!   POST /v1/audio/transcriptions   STT (JSON carries b64 audio)
//!   POST /v1/batches                offline batch submit (inline items or input_file_id)
//!   GET  /v1/batches/{id}           batch status/results
//!   POST /v1/files                  file upload (batch input JSONL)
//!   GET  /v1/files/{id}             file metadata
//!   GET  /v1/files/{id}/content     file content download
//!   GET  /internal/ledger           local billing ledger (observability surface trimmed for the zero-egress local build)
//!   GET  /internal/accounts         account pool view
//!
//! Views parse/validate, authenticate the AK, build a `GatewayRequest`, call the
//! handler, shape the wire response, and emit one structured access-log line per
//! request (fields: ak/model/account/tokens/cost/latency).

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use gw_config::GatewayConfig;
use gw_dag::DagContext;
use gw_engines::{SharedTransport, is_implemented};
use gw_handler::{BatchItem, OfflineHandler, OnlineHandler};
use gw_models::{
    ChatMsg, ChatParams, EmbeddingParams, GResult, GatewayError, GatewayRequest, ImageParams,
    ModelParamV2, SttParams, TtsParams, TypedParams,
};
use gw_protocol::anthropic::{AnthUsage, MessagesRequest, MessagesResponse};
use gw_protocol::openai::{
    ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse, Usage,
};
use gw_state::{AkInfo, GatewayState};
use serde_json::{Value, json};

const LEDGER_PAGE_DEFAULT: usize = 100;
const STREAM_CHANNEL_CAP: usize = 64;

static REQ_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct AppState {
    pub handler: OnlineHandler,
    pub offline: OfflineHandler,
}

impl AppState {
    pub fn new(
        cfg: Arc<GatewayConfig>,
        state: Arc<GatewayState>,
        transport: SharedTransport,
    ) -> Self {
        let handler = OnlineHandler::new(cfg, state, transport);
        let offline = OfflineHandler::new(handler.clone());
        Self { handler, offline }
    }
}

/// Build the application router.
pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/models", get(list_models))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/completions", post(completions))
        .route("/v1/messages", post(messages))
        .route("/v1/responses", post(responses))
        .route("/v1/embeddings", post(embeddings))
        .route("/v1/images/generations", post(images_generations))
        .route("/v1/images/edits", post(images_edits))
        .route("/v1/audio/speech", post(audio_speech))
        .route("/v1/audio/transcriptions", post(audio_transcriptions))
        .route("/v1/batches", post(batches_submit))
        .route("/v1/batches/{id}", get(batches_get))
        .route("/v1/files", post(files_upload))
        .route("/v1/files/{id}", get(files_get))
        .route("/v1/files/{id}/content", get(files_content))
        .route("/v1/realtime", get(realtime_ws))
        .route("/internal/ledger", get(ledger))
        .route("/internal/accounts", get(accounts))
        .layer(axum::middleware::from_fn(track_requests))
        .with_state(state)
}

/// Counts every response (all statuses, every surface) with bounded labels:
/// the matched route template and the status code.
async fn track_requests(
    matched: Option<axum::extract::MatchedPath>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let route = matched.map(|m| m.as_str().to_owned()).unwrap_or_default();
    let started = Instant::now();
    let resp = next.run(req).await;
    metrics::counter!(
        "gateway_requests_total",
        "route" => route.clone(),
        "status" => resp.status().as_u16().to_string(),
    )
    .increment(1);
    metrics::histogram!("gateway_request_duration_seconds", "route" => route)
        .record(started.elapsed().as_secs_f64());
    resp
}

/// GET /v1/realtime?model=... (WebSocket upgrade).
/// An account with a real endpoint bridges the session to the vendor's
/// realtime WebSocket; an endpoint-less account serves the local mock session
/// (session.created / input_text → response.delta×n → response.done).
async fn realtime_ws(
    State(s): State<AppState>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
    ws: axum::extract::ws::WebSocketUpgrade,
) -> Response {
    // Auth: header or ?ak= (fallback when a ws client can't easily set headers)
    let ak = match authenticate(&s, &headers) {
        Ok(ak) => ak,
        Err((st, msg)) => match q
            .get("ak")
            .and_then(|k| s.handler.state.auth.authenticate(k))
        {
            Some(ak) => ak,
            None => return error_response(st, msg),
        },
    };
    let Some(model) = q.get("model").cloned() else {
        return error_response(400, "model query param is required");
    };
    // Resolve: public name or wire name, must be the Realtime family
    let mt = s
        .handler
        .cfg
        .find_model(&model)
        .and_then(|m| m.protocol())
        .or_else(|| gw_consts::Protocol::from_wire(&model));
    let Some(mt) = mt else {
        return error_response(404, format!("unknown model: {model}"));
    };
    if mt != gw_consts::Protocol::Realtime {
        return error_response(400, format!("`{model}` is not a realtime model"));
    }
    let Some(account) = s.handler.state.pool.select_healthy(
        mt,
        s.handler
            .cfg
            .find_model(&model)
            .and_then(|m| m.provider.as_deref()),
        &[],
        &s.handler.state.health,
    ) else {
        return error_response(503, format!("no healthy upstream account serves `{model}`"));
    };
    // an account with a real endpoint bridges to the vendor; else the local mock
    if account.endpoint.is_empty() {
        ws.on_upgrade(move |socket| realtime_session(socket, s, ak, model, mt, account.name))
    } else {
        ws.on_upgrade(move |socket| realtime_bridge(socket, s, ak, model, mt, account))
    }
}

/// Same governance gates as the REST surfaces — the realtime surface bills,
/// so it must also be rate/quota limited. `None` = admitted.
async fn realtime_gate(s: &AppState, ak: &AkInfo) -> Option<String> {
    let gov = &s.handler.state.governance;
    if !gov.rate_allow(&ak.ak, ak.qps).await {
        return Some(format!(
            "rate limit exceeded for ak {} (qps {})",
            ak.ak, ak.qps
        ));
    }
    if !gov.quota_check(&ak.ak, ak.daily_token_quota).await {
        return Some(format!("daily token quota exhausted for ak {}", ak.ak));
    }
    if let Some(tpm) = ak.tokens_per_minute
        && !gov
            .token_window_check(&ak.ak, tpm, std::time::Duration::from_secs(60))
            .await
    {
        return Some(format!(
            "token-per-minute limit exceeded for ak {} (tpm {tpm})",
            ak.ak
        ));
    }
    None
}

/// Bill one realtime generation (quota + TPM window + ledger + metrics).
async fn bill_realtime_turn(
    s: &AppState,
    ak: &AkInfo,
    model: &str,
    mt: gw_consts::Protocol,
    account: &str,
    it: i64,
    ot: i64,
) {
    let (p_in, p_out) = s.handler.cfg.prices_for(model);
    let gov = &s.handler.state.governance;
    gov.quota_consume(&ak.ak, it + ot).await;
    gov.token_window_add(&ak.ak, it + ot, std::time::Duration::from_secs(60))
        .await;
    let record = gw_state::BillingRecord {
        ak: ak.ak.clone(),
        product: ak.product.clone(),
        model: model.to_owned(),
        protocol: mt.as_str().to_owned(),
        account: account.to_owned(),
        prompt_tokens: it,
        completion_tokens: ot,
        total_tokens: it + ot,
        cost_micros: it * p_in / 1000 + ot * p_out / 1000,
        ptu_spillover: false,
    };
    metrics::counter!("gateway_tokens_total", "kind" => "prompt").increment(it.max(0) as u64);
    metrics::counter!("gateway_tokens_total", "kind" => "completion").increment(ot.max(0) as u64);
    if let Err(e) = s.handler.state.store.ledger_add(record).await {
        metrics::counter!("gateway_ledger_write_failures_total").increment(1);
        tracing::error!(error = %e, "realtime billing write failed");
    }
}

/// One mock realtime session (upstream is mocked).
async fn realtime_session(
    mut socket: axum::extract::ws::WebSocket,
    s: AppState,
    ak: AkInfo,
    model: String,
    mt: gw_consts::Protocol,
    account: String,
) {
    use axum::extract::ws::Message;
    let send = |v: Value| Message::Text(v.to_string().into());

    let _ = socket
        .send(send(json!({"type":"session.created",
            "session":{"model": model, "account": account}})))
        .await;

    while let Some(Ok(msg)) = socket.recv().await {
        let text = match msg {
            Message::Text(t) => t.to_string(),
            Message::Close(_) => break,
            _ => continue,
        };
        let Ok(ev) = serde_json::from_str::<Value>(&text) else {
            let _ = socket
                .send(send(json!({"type":"error","message":"invalid json event"})))
                .await;
            continue;
        };
        match ev["type"].as_str().unwrap_or_default() {
            "input_text" => {
                if let Some(denied) = realtime_gate(&s, &ak).await {
                    let _ = socket
                        .send(send(json!({"type":"error","message": denied})))
                        .await;
                    continue;
                }
                let input = ev["text"].as_str().unwrap_or_default().to_owned();
                let reply = format!("[mock-realtime:{model}] you said: {input}");
                let (it, ot) = (
                    (input.len() as i64 / 4).max(1) + 3,
                    (reply.len() as i64 / 4).max(1),
                );
                let mid = reply.len() / 2;
                let (a, b) = reply.split_at(mid);
                let _ = socket
                    .send(send(json!({"type":"response.delta","delta": a})))
                    .await;
                let _ = socket
                    .send(send(json!({"type":"response.delta","delta": b})))
                    .await;
                let _ = socket
                    .send(send(json!({"type":"response.done",
                        "usage":{"input_tokens": it, "output_tokens": ot}})))
                    .await;
                bill_realtime_turn(&s, &ak, &model, mt, &account, it, ot).await;
            }
            "session.close" => {
                let _ = socket.send(send(json!({"type":"session.closed"}))).await;
                break;
            }
            other => {
                let _ = socket
                    .send(send(json!({"type":"error",
                        "message": format!("unsupported event type `{other}`")})))
                    .await;
            }
        }
    }
}

/// Bridge one realtime session to a real upstream over WebSocket: a transparent
/// relay (the client speaks the vendor's realtime dialect through us) plus the
/// gateway's own concerns — auth, per-generation governance gates on
/// `response.create`, and billing from the vendor's `response.done` usage.
async fn realtime_bridge(
    mut client: axum::extract::ws::WebSocket,
    s: AppState,
    ak: AkInfo,
    model: String,
    mt: gw_consts::Protocol,
    account: gw_models::Account,
) {
    use axum::extract::ws::Message as CMsg;
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message as UMsg;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let send_err = |m: String| json!({"type":"error","error":{"type":"gateway_error","message":m}});

    // wss URL from the account's https endpoint
    let base = account.endpoint.trim_end_matches('/');
    let ws_base = base
        .replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1);
    let url = format!("{ws_base}/v1/realtime?model={model}");
    let mut req = match url.into_client_request() {
        Ok(r) => r,
        Err(e) => {
            let _ = client
                .send(CMsg::Text(
                    send_err(format!("bad upstream url: {e}"))
                        .to_string()
                        .into(),
                ))
                .await;
            return;
        }
    };
    let key = account.api_key().unwrap_or_else(|| "mock".to_owned());
    if let Ok(v) = format!("Bearer {key}").parse() {
        req.headers_mut().insert("authorization", v);
    }
    let upstream = match tokio_tungstenite::connect_async(req).await {
        Ok((u, _)) => u,
        Err(e) => {
            let _ = client
                .send(CMsg::Text(
                    send_err(format!("upstream realtime connect failed: {e}"))
                        .to_string()
                        .into(),
                ))
                .await;
            return;
        }
    };
    let (mut up_tx, mut up_rx) = upstream.split();
    let (mut cl_tx, mut cl_rx) = client.split();

    loop {
        tokio::select! {
            m = cl_rx.next() => match m {
                Some(Ok(CMsg::Text(t))) => {
                    // gate each generation trigger, not every control frame
                    let is_generate = serde_json::from_str::<Value>(&t)
                        .map(|v| v["type"] == "response.create")
                        .unwrap_or(false);
                    if is_generate && let Some(denied) = realtime_gate(&s, &ak).await {
                        if cl_tx
                            .send(CMsg::Text(send_err(denied).to_string().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        continue;
                    }
                    if up_tx.send(UMsg::text(t.to_string())).await.is_err() {
                        break;
                    }
                }
                Some(Ok(CMsg::Binary(b))) => {
                    if up_tx.send(UMsg::binary(b)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(CMsg::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {} // ping/pong handled by the ws stacks
            },
            m = up_rx.next() => match m {
                Some(Ok(UMsg::Text(t))) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&t)
                        && v["type"] == "response.done"
                    {
                        let usage = &v["response"]["usage"];
                        let it = usage["input_tokens"].as_i64().unwrap_or(0);
                        let ot = usage["output_tokens"].as_i64().unwrap_or(0);
                        if it + ot > 0 {
                            bill_realtime_turn(&s, &ak, &model, mt, &account.name, it, ot).await;
                        }
                    }
                    if cl_tx.send(CMsg::Text(t.to_string().into())).await.is_err() {
                        break;
                    }
                }
                Some(Ok(UMsg::Binary(b))) => {
                    if cl_tx.send(CMsg::Binary(b)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(UMsg::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {} // ping/pong handled by the ws stacks
            },
        }
    }
}

/// One structured access-log line per served request
/// (ak/model/account/prompt_tokens/.../latency_ms); local
/// stdout only (zero-egress default build).
fn log_access(surface: &str, ctx: &DagContext, started: Instant) {
    let (model, mt) = ctx
        .request
        .model_param_v2
        .as_ref()
        .map(|p| (p.model_name.clone(), p.protocol.as_str()))
        .unwrap_or_default();
    let account = ctx
        .request
        .account
        .as_ref()
        .map(|a| a.name.as_str())
        .unwrap_or("");
    let (pt, ct, tt) = ctx
        .outcome
        .as_ref()
        .map(|o| {
            (
                o.response.prompt_tokens,
                o.response.completion_tokens,
                o.response.total_tokens,
            )
        })
        .unwrap_or_default();
    let latency = started.elapsed();
    metrics::counter!("gateway_tokens_total", "kind" => "prompt").increment(pt.max(0) as u64);
    metrics::counter!("gateway_tokens_total", "kind" => "completion").increment(ct.max(0) as u64);
    tracing::info!(
        target: "access",
        surface,
        ak = %ctx.ak.ak,
        product = %ctx.ak.product,
        model = %model,
        protocol = mt,
        account,
        prompt_tokens = pt,
        completion_tokens = ct,
        total_tokens = tt,
        latency_ms = latency.as_millis() as u64,
        "request served"
    );
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "service": "gw" }))
}

/// Configured public models (the gateway's catalog view).
async fn list_models(State(s): State<AppState>) -> Json<Value> {
    let data: Vec<Value> = s
        .handler
        .cfg
        .models
        .iter()
        .map(|m| {
            let implemented = m.protocol().map(is_implemented).unwrap_or(false);
            json!({
                "id": m.name,
                "object": "model",
                "protocol": m.protocol,
                "implemented": implemented,
            })
        })
        .collect();
    Json(json!({ "object": "list", "data": data }))
}

/// Local billing ledger snapshot.
async fn ledger(
    State(s): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let limit = q
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(LEDGER_PAGE_DEFAULT);
    match s.handler.state.store.ledger_snapshot(limit).await {
        Ok((count, records)) => Json(json!({ "count": count, "records": records })).into_response(),
        Err(e) => gateway_error(e),
    }
}

/// Account pool view (name/provider/tier/priority/served model family).
async fn accounts(State(s): State<AppState>) -> Json<Value> {
    let data: Vec<Value> = s
        .handler
        .cfg
        .accounts
        .iter()
        .map(|a| {
            json!({
                "name": a.name,
                "provider": a.provider,
                "priority": a.priority,
                "tier": if a.tier.is_empty() { "paygo" } else { a.tier.as_str() },
                "health": s.handler.state.health.status(&a.name),
                "protocols": a.protocols,
            })
        })
        .collect();
    Json(json!({ "count": data.len(), "accounts": data }))
}

/// AK auth: `Authorization: Bearer <ak>` or `x-api-key: <ak>`. The error is
/// `(status, message)` so each surface can shape it to its own wire dialect.
fn authenticate(s: &AppState, headers: &HeaderMap) -> Result<AkInfo, (u16, &'static str)> {
    let ak = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").and_then(|v| v.to_str().ok()));
    let Some(ak) = ak else {
        return Err((
            401,
            "missing api key (Authorization: Bearer <ak> or x-api-key)",
        ));
    };
    s.handler
        .state
        .auth
        .authenticate(ak)
        .ok_or((401, "invalid api key"))
}

fn error_response(status: u16, message: impl Into<String>) -> Response {
    let code = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (
        code,
        Json(json!({ "error": { "message": message.into(), "type": "gateway_error" } })),
    )
        .into_response()
}

fn gateway_error(e: GatewayError) -> Response {
    let code = StatusCode::from_u16(e.http_status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    // OpenAI's error schema types `code` as string-or-null, never a number.
    (
        code,
        Json(json!({ "error": { "message": e.message, "code": e.code.value().to_string(), "type": "gateway_error" } })),
    )
        .into_response()
}

/// Anthropic's error type string for an HTTP status.
fn anthropic_error_type(status: u16) -> &'static str {
    match status {
        400 => "invalid_request_error",
        401 => "authentication_error",
        403 => "permission_error",
        404 => "not_found_error",
        413 => "request_too_large",
        429 => "rate_limit_error",
        529 => "overloaded_error",
        _ => "api_error",
    }
}

/// Anthropic-shaped error body: `{"type":"error","error":{"type","message"}}` —
/// the discriminator the Anthropic SDKs key their exception dispatch on.
fn anthropic_error(status: u16, message: impl Into<String>) -> Response {
    let code = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (
        code,
        Json(json!({
            "type": "error",
            "error": { "type": anthropic_error_type(status), "message": message.into() },
        })),
    )
        .into_response()
}

fn anthropic_gateway_error(e: GatewayError) -> Response {
    anthropic_error(e.http_status, e.message)
}

/// Run the pipeline on its own task so a client disconnect cannot cancel it
/// mid-billing: once a request is admitted, quota/ledger accounting runs to
/// completion even if the response can no longer be delivered.
async fn run_pipeline(s: &AppState, request: GatewayRequest, ak: AkInfo) -> GResult<DagContext> {
    let handler = s.handler.clone();
    match tokio::spawn(async move { handler.run(request, ak).await }).await {
        Ok(res) => res,
        Err(e) => Err(GatewayError::internal(format!("pipeline task failed: {e}"))),
    }
}

fn next_id(prefix: &str) -> String {
    format!("{prefix}-local-{}", REQ_SEQ.fetch_add(1, Ordering::Relaxed))
}

/// finish_reason cross-protocol mapping (terminal-state conversion):
/// anthropic → openai side.
fn finish_openai(fr: &str) -> String {
    match fr {
        "" | "end_turn" | "stop_sequence" | "COMPLETE" | "complete" => "stop".to_owned(),
        "max_tokens" => "length".to_owned(),
        "tool_use" => "tool_calls".to_owned(),
        other => other.to_owned(),
    }
}

/// openai → anthropic side.
fn finish_anthropic(fr: &str) -> String {
    match fr {
        "" | "stop" => "end_turn".to_owned(),
        "length" => "max_tokens".to_owned(),
        "tool_calls" => "tool_use".to_owned(),
        other => other.to_owned(),
    }
}

/// POST /v1/chat/completions (OpenAI-compatible surface)
async fn chat_completions(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ChatCompletionRequest>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers) {
        Ok(ak) => ak,
        Err((st, msg)) => return error_response(st, msg),
    };
    if body.messages.is_empty() {
        return error_response(400, "messages must not be empty");
    }

    // Full field passthrough: multimodal parts + tool message fields + sampling/tools params + unrecognized fields
    let messages: Vec<ChatMsg> = body
        .messages
        .iter()
        .map(|m| ChatMsg {
            role: m.role.clone(),
            content: m.content_text(),
            parts: m.content.as_ref().and_then(|c| match c {
                gw_protocol::openai::MessageContent::Parts(p) => Some(Value::Array(p.clone())),
                _ => None,
            }),
            tool_calls: m
                .tool_calls
                .as_ref()
                .and_then(|tc| serde_json::to_value(tc).ok()),
            tool_call_id: m.tool_call_id.clone(),
        })
        .collect();
    let typed = TypedParams::Chat(ChatParams {
        temperature: body.temperature,
        top_p: body.top_p,
        max_tokens: body.max_tokens,
        stop: body.stop.clone(),
        presence_penalty: body.presence_penalty,
        frequency_penalty: body.frequency_penalty,
        tools: body.tools.as_ref().map(|t| Value::Array(t.clone())),
        tool_choice: body.tool_choice.clone(),
        response_format: body.response_format.clone(),
        logprobs: body.logprobs,
        top_logprobs: body.top_logprobs,
        system: None,
    });
    let mut param = ModelParamV2::with_name(
        // placeholder type; the resolve_model DAG node maps model_name properly
        gw_consts::Protocol::OpenaiChat,
        body.model.clone(),
    );
    param.typed = Some(typed);
    param.raw = Value::Object(body.extra.clone());

    let request = GatewayRequest {
        is_online: true,
        stream: body.stream,
        ak: ak.ak.clone(),
        message: messages,
        model_param_v2: Some(param),
        ..Default::default()
    };

    if body.stream {
        let model = body.model.clone();
        return chat_stream_response(s, request, ak, model, started).into_response();
    }

    let ctx = match run_pipeline(&s, request, ak).await {
        Ok(ctx) => ctx,
        Err(e) => return gateway_error(e),
    };
    log_access("chat_completions", &ctx, started);
    let Some(outcome) = ctx.outcome else {
        return error_response(500, "pipeline produced no outcome");
    };

    let id = next_id("chatcmpl");
    let created = chrono_now();
    let usage = Usage {
        prompt_tokens: outcome.response.prompt_tokens,
        completion_tokens: outcome.response.completion_tokens,
        total_tokens: outcome.response.total_tokens,
    };

    // tool_calls response: content=null + finish_reason=tool_calls (OpenAI semantics)
    if let Some(tc) = &outcome.response.tool_calls {
        let calls: Vec<gw_protocol::openai::ToolCall> =
            serde_json::from_value(tc.clone()).unwrap_or_default();
        let resp = ChatCompletionResponse::tool_calls(
            id,
            created,
            outcome.response.model.clone(),
            calls,
            usage,
        );
        return (StatusCode::OK, Json(resp)).into_response();
    }

    let resp = ChatCompletionResponse::text(
        id,
        created,
        outcome.response.model.clone(),
        outcome.response.message.clone(),
        finish_openai(&outcome.response.finish_reason),
        usage,
    );
    (StatusCode::OK, Json(resp)).into_response()
}

/// Run the pipeline on its own task, forwarding stream chunks through a
/// bounded channel — the backpressure seam. Engines without live streaming
/// yield their buffered chunks after the run; a final chunk carries the usage
/// totals; billing stays in the pipeline tail either way.
fn spawn_stream_pipeline(
    s: &AppState,
    mut request: GatewayRequest,
    ak: AkInfo,
    surface: &'static str,
    started: Instant,
) -> tokio::sync::mpsc::Receiver<gw_engines::StreamChunk> {
    let (tx, rx) = tokio::sync::mpsc::channel::<gw_engines::StreamChunk>(STREAM_CHANNEL_CAP);
    request.stream_tx = Some(tx.clone());
    let handler = s.handler.clone();
    tokio::spawn(async move {
        match handler.run(request, ak).await {
            Ok(ctx) => {
                log_access(surface, &ctx, started);
                if let Some(outcome) = &ctx.outcome {
                    let mut tail = if outcome.streamed_live {
                        Vec::new()
                    } else {
                        synth_chunks(outcome)
                    };
                    tail.push(gw_engines::StreamChunk {
                        usage_totals: Some((
                            outcome.response.prompt_tokens,
                            outcome.response.completion_tokens,
                            outcome.response.total_tokens,
                        )),
                        ..Default::default()
                    });
                    for c in tail {
                        if tx.send(c).await.is_err() {
                            break; // client went away; billing already happened
                        }
                    }
                }
            }
            Err(e) => {
                let _ = tx
                    .send(gw_engines::StreamChunk {
                        error: Some(e.to_string()),
                        ..Default::default()
                    })
                    .await;
            }
        }
    });
    rx
}

/// Streaming chat: pipeline chunks re-emitted as OpenAI SSE.
fn chat_stream_response(
    s: AppState,
    request: GatewayRequest,
    ak: AkInfo,
    model: String,
    started: Instant,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>> + use<>> {
    let rx = spawn_stream_pipeline(&s, request, ak, "chat_completions", started);

    struct St {
        rx: tokio::sync::mpsc::Receiver<gw_engines::StreamChunk>,
        queue: std::collections::VecDeque<Event>,
        id: String,
        created: i64,
        model: String,
        pending_finish: Option<String>,
        ended: bool,
    }
    let st = St {
        rx,
        queue: std::collections::VecDeque::new(),
        id: next_id("chatcmpl"),
        created: chrono_now(),
        model,
        pending_finish: None,
        ended: false,
    };
    let stream = futures::stream::unfold(st, |mut st| async move {
        loop {
            if let Some(ev) = st.queue.pop_front() {
                return Some((Ok::<_, Infallible>(ev), st));
            }
            if st.ended {
                return None;
            }
            match st.rx.recv().await {
                Some(c) if c.error.is_some() => {
                    let msg = c.error.unwrap_or_default();
                    st.queue.push_back(Event::default().data(
                        json!({"error": {"message": msg, "type": "gateway_error"}}).to_string(),
                    ));
                    st.queue.push_back(Event::default().data("[DONE]"));
                    st.ended = true;
                }
                Some(c) => {
                    if !c.delta.is_empty() {
                        let chunk = ChatCompletionChunk::content(
                            &st.id,
                            st.created,
                            &st.model,
                            c.delta.clone(),
                        );
                        if let Ok(payload) = serde_json::to_string(&chunk) {
                            st.queue.push_back(Event::default().data(payload));
                        }
                    }
                    if let Some(tc) = &c.tool_calls {
                        let calls = tc.as_array().cloned().unwrap_or_default();
                        let chunk =
                            ChatCompletionChunk::tool_calls(&st.id, st.created, &st.model, calls);
                        if let Ok(payload) = serde_json::to_string(&chunk) {
                            st.queue.push_back(Event::default().data(payload));
                        }
                    }
                    if let Some(fr) = c.finish_reason {
                        // held back until usage arrives so the final frame carries both
                        st.pending_finish = Some(fr);
                    }
                    if let Some((pt, ct, tt)) = c.usage_totals {
                        let usage = Usage {
                            prompt_tokens: pt,
                            completion_tokens: ct,
                            total_tokens: tt,
                        };
                        let mut fin =
                            ChatCompletionChunk::finish(&st.id, st.created, &st.model, Some(usage));
                        fin.choices[0].finish_reason = Some(
                            st.pending_finish
                                .take()
                                .unwrap_or_else(|| "stop".to_owned()),
                        );
                        if let Ok(payload) = serde_json::to_string(&fin) {
                            st.queue.push_back(Event::default().data(payload));
                        }
                        st.queue.push_back(Event::default().data("[DONE]"));
                        st.ended = true;
                    }
                }
                None => {
                    // producer gone without a usage chunk — close the stream cleanly
                    st.queue.push_back(Event::default().data("[DONE]"));
                    st.ended = true;
                }
            }
        }
    });
    Sse::new(stream)
}

/// Chunks for engines that returned a buffered response: the full message as
/// one delta plus tool calls and a finish marker.
fn synth_chunks(outcome: &gw_engines::EngineOutcome) -> Vec<gw_engines::StreamChunk> {
    let mut chunks = if outcome.chunks.is_empty() && !outcome.response.message.is_empty() {
        vec![gw_engines::StreamChunk {
            delta: outcome.response.message.clone(),
            ..Default::default()
        }]
    } else {
        outcome.chunks.clone()
    };
    if let Some(tc) = &outcome.response.tool_calls
        && !chunks.iter().any(|c| c.tool_calls.is_some())
    {
        chunks.push(gw_engines::StreamChunk {
            tool_calls: Some(tc.clone()),
            ..Default::default()
        });
    }
    if !chunks.iter().any(|c| c.finish_reason.is_some()) {
        chunks.push(gw_engines::StreamChunk {
            finish_reason: Some(if outcome.response.finish_reason.is_empty() {
                "stop".to_owned()
            } else {
                outcome.response.finish_reason.clone()
            }),
            ..Default::default()
        });
    }
    chunks
}

/// POST /v1/messages (Anthropic-compatible surface, stream + non-stream)
async fn messages(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<MessagesRequest>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers) {
        Ok(ak) => ak,
        Err((st, msg)) => return anthropic_error(st, msg),
    };
    if body.messages.is_empty() {
        return anthropic_error(400, "messages must not be empty");
    }

    let typed = TypedParams::Chat(ChatParams {
        temperature: body.temperature,
        top_p: body.top_p,
        max_tokens: body.max_tokens,
        stop: body
            .stop_sequences
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok()),
        tools: body.tools.as_ref().map(|t| Value::Array(t.clone())),
        tool_choice: body.tool_choice.clone(),
        system: body.system_text(),
        ..Default::default()
    });
    let mut param =
        ModelParamV2::with_name(gw_consts::Protocol::AnthropicMessages, body.model.clone());
    param.typed = Some(typed);
    param.raw = Value::Object(body.extra.clone());

    let request = GatewayRequest {
        is_online: true,
        stream: body.stream,
        ak: ak.ak.clone(),
        message: body
            .messages
            .iter()
            .map(|m| {
                let mut msg = ChatMsg::text(m.role.clone(), m.text());
                // preserve multimodal content blocks (image/text) for the engine
                if m.content.is_array() {
                    msg.parts = Some(m.content.clone());
                }
                msg
            })
            .collect(),
        model_param_v2: Some(param),
        ..Default::default()
    };

    // Streaming: the standard anthropic event sequence, emitted incrementally
    // as pipeline chunks arrive.
    if body.stream {
        let model = body.model.clone();
        return messages_stream_response(s, request, ak, model, started).into_response();
    }

    let ctx = match run_pipeline(&s, request, ak).await {
        Ok(ctx) => ctx,
        Err(e) => return anthropic_gateway_error(e),
    };
    log_access("messages", &ctx, started);
    let Some(outcome) = ctx.outcome else {
        return anthropic_error(500, "pipeline produced no outcome");
    };

    // Non-streaming: text + tool_use blocks
    let tool_use = anthropic_tool_blocks(&outcome.response.tool_calls);
    let mut content: Vec<gw_protocol::anthropic::ContentBlock> = Vec::new();
    if !outcome.response.message.is_empty() {
        content.push(gw_protocol::anthropic::ContentBlock::Text {
            text: outcome.response.message.clone(),
        });
    }
    for b in &tool_use {
        content.push(gw_protocol::anthropic::ContentBlock::ToolUse {
            id: b["id"].as_str().unwrap_or_default().to_owned(),
            name: b["name"].as_str().unwrap_or_default().to_owned(),
            input: b["input"].clone(),
        });
    }
    let resp = MessagesResponse::new(
        next_id("msg"),
        outcome.response.model.clone(),
        content,
        finish_anthropic(&outcome.response.finish_reason),
        AnthUsage {
            input_tokens: outcome.response.prompt_tokens,
            output_tokens: outcome.response.completion_tokens,
        },
    );
    (StatusCode::OK, Json(resp)).into_response()
}

/// The anthropic-shaped tool_use blocks inside an engine's tool_calls value.
fn anthropic_tool_blocks(tool_calls: &Option<Value>) -> Vec<Value> {
    match tool_calls {
        Some(Value::Array(blocks)) => blocks
            .iter()
            .filter(|b| b["type"] == "tool_use")
            .cloned()
            .collect(),
        _ => Vec::new(),
    }
}

/// Streaming /v1/messages: pipeline chunks re-emitted incrementally as the
/// anthropic event sequence. message_start goes out before upstream usage is
/// known, so its input_tokens is 0; the final message_delta carries the real
/// counts (SDKs accumulate usage from message_delta).
fn messages_stream_response(
    s: AppState,
    request: GatewayRequest,
    ak: AkInfo,
    model: String,
    started: Instant,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>> + use<>> {
    let rx = spawn_stream_pipeline(&s, request, ak, "messages", started);

    struct St {
        rx: tokio::sync::mpsc::Receiver<gw_engines::StreamChunk>,
        queue: std::collections::VecDeque<Event>,
        id: String,
        model: String,
        started_msg: bool,
        /// index of the open text content block, if any.
        text_idx: Option<usize>,
        next_idx: usize,
        pending_finish: Option<String>,
        ended: bool,
    }

    impl St {
        fn ev(name: &str, payload: Value) -> Event {
            Event::default().event(name).data(payload.to_string())
        }

        fn ensure_message_start(&mut self) {
            if self.started_msg {
                return;
            }
            self.started_msg = true;
            self.queue.push_back(Self::ev(
                "message_start",
                json!({"type":"message_start","message":{
                    "id": self.id, "type":"message","role":"assistant","model": self.model,
                    "content":[], "stop_reason": null,
                    "usage":{"input_tokens":0,"output_tokens":0}}}),
            ));
        }

        fn open_text(&mut self) -> usize {
            if let Some(idx) = self.text_idx {
                return idx;
            }
            let idx = self.next_idx;
            self.next_idx += 1;
            self.text_idx = Some(idx);
            self.queue.push_back(Self::ev(
                "content_block_start",
                json!({"type":"content_block_start","index":idx,
                       "content_block":{"type":"text","text":""}}),
            ));
            idx
        }

        fn close_text(&mut self) {
            if let Some(idx) = self.text_idx.take() {
                self.queue.push_back(Self::ev(
                    "content_block_stop",
                    json!({"type":"content_block_stop","index":idx}),
                ));
            }
        }

        /// The wire pattern clients expect for a tool_use block: empty `input`
        /// in the start frame, the arguments as one input_json_delta, stop.
        fn emit_tool_block(&mut self, block: &Value) {
            self.close_text();
            let idx = self.next_idx;
            self.next_idx += 1;
            self.queue.push_back(Self::ev(
                "content_block_start",
                json!({"type":"content_block_start","index":idx,
                       "content_block":{"type":"tool_use","id":block["id"],"name":block["name"],"input":{}}}),
            ));
            self.queue.push_back(Self::ev(
                "content_block_delta",
                json!({"type":"content_block_delta","index":idx,
                       "delta":{"type":"input_json_delta","partial_json":block["input"].to_string()}}),
            ));
            self.queue.push_back(Self::ev(
                "content_block_stop",
                json!({"type":"content_block_stop","index":idx}),
            ));
        }

        fn finish(&mut self, input_tokens: i64, output_tokens: i64) {
            self.ensure_message_start();
            self.close_text();
            let stop = self
                .pending_finish
                .take()
                .unwrap_or_else(|| "end_turn".to_owned());
            self.queue.push_back(Self::ev(
                "message_delta",
                json!({"type":"message_delta","delta":{"stop_reason":stop},
                       "usage":{"input_tokens":input_tokens,"output_tokens":output_tokens}}),
            ));
            self.queue
                .push_back(Self::ev("message_stop", json!({"type":"message_stop"})));
            self.ended = true;
        }
    }

    let st = St {
        rx,
        queue: std::collections::VecDeque::new(),
        id: next_id("msg"),
        model,
        started_msg: false,
        text_idx: None,
        next_idx: 0,
        pending_finish: None,
        ended: false,
    };
    let stream = futures::stream::unfold(st, |mut st| async move {
        loop {
            if let Some(ev) = st.queue.pop_front() {
                return Some((Ok::<_, Infallible>(ev), st));
            }
            if st.ended {
                return None;
            }
            match st.rx.recv().await {
                Some(c) if c.error.is_some() => {
                    let msg = c.error.unwrap_or_default();
                    st.queue.push_back(St::ev(
                        "error",
                        json!({"type":"error","error":{"type":"api_error","message":msg}}),
                    ));
                    st.ended = true;
                }
                Some(c) => {
                    if !c.delta.is_empty() {
                        st.ensure_message_start();
                        let idx = st.open_text();
                        st.queue.push_back(St::ev(
                            "content_block_delta",
                            json!({"type":"content_block_delta","index":idx,
                                   "delta":{"type":"text_delta","text":c.delta}}),
                        ));
                    }
                    if let Some(tc) = &c.tool_calls {
                        st.ensure_message_start();
                        for block in anthropic_tool_blocks(&Some(tc.clone())) {
                            st.emit_tool_block(&block);
                        }
                    }
                    if let Some(fr) = c.finish_reason {
                        st.pending_finish = Some(finish_anthropic(&fr));
                    }
                    if let Some((pt, ct, _)) = c.usage_totals {
                        st.finish(pt, ct);
                    }
                }
                None => {
                    // producer gone without a usage chunk — close out cleanly
                    st.finish(0, 0);
                }
            }
        }
    });
    Sse::new(stream)
}

/// Shared: run a non-chat family request through the pipeline.
async fn run_family(
    s: &AppState,
    ak: AkInfo,
    model: String,
    typed: TypedParams,
    messages: Vec<ChatMsg>,
) -> Result<DagContext, Response> {
    let mut param = ModelParamV2::with_name(gw_consts::Protocol::OpenaiChat, model);
    param.typed = Some(typed);
    let request = GatewayRequest {
        is_online: true,
        ak: ak.ak.clone(),
        message: messages,
        model_param_v2: Some(param),
        ..Default::default()
    };
    match run_pipeline(s, request, ak).await {
        Ok(ctx) => Ok(ctx),
        Err(e) => Err(gateway_error(e)),
    }
}

/// POST /v1/embeddings (OpenAI-compatible embeddings surface)
/// POST /v1/completions (legacy text-completion surface; prompt shape, not chat).
/// The prompt is handed as a single user message to CompletionsEngine (sends
/// `{prompt}`); response is shaped as `text_completion`. Non-streaming.
async fn completions(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers) {
        Ok(ak) => ak,
        Err((st, msg)) => return error_response(st, msg),
    };
    let model = body["model"].as_str().unwrap_or_default().to_owned();
    // prompt: string or [string] (OpenAI accepts both).
    let prompt: String = match &body["prompt"] {
        Value::String(s) => s.clone(),
        Value::Array(a) => a
            .iter()
            .filter_map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    };
    if model.is_empty() || prompt.is_empty() {
        return error_response(400, "model and prompt are required");
    }
    let typed = TypedParams::Chat(ChatParams {
        max_tokens: body["max_tokens"].as_i64(),
        temperature: body["temperature"].as_f64(),
        ..Default::default()
    });
    let mut param = ModelParamV2::with_name(gw_consts::Protocol::Completions, model);
    param.typed = Some(typed);
    let request = GatewayRequest {
        is_online: true,
        ak: ak.ak.clone(),
        message: vec![ChatMsg::text("user", prompt)],
        model_param_v2: Some(param),
        ..Default::default()
    };
    let ctx = match run_pipeline(&s, request, ak).await {
        Ok(ctx) => ctx,
        Err(e) => return gateway_error(e),
    };
    log_access("completions", &ctx, started);
    let Some(outcome) = ctx.outcome else {
        return error_response(500, "pipeline produced no outcome");
    };
    let r = &outcome.response;
    let finish = if r.finish_reason.is_empty() {
        "stop"
    } else {
        r.finish_reason.as_str()
    };
    let resp = json!({
        "id": next_id("cmpl"),
        "object": "text_completion",
        "created": chrono_now(),
        "model": r.model,
        "choices": [{"text": r.message, "index": 0, "finish_reason": finish}],
        "usage": {
            "prompt_tokens": r.prompt_tokens,
            "completion_tokens": r.completion_tokens,
            "total_tokens": r.total_tokens,
        },
    });
    (StatusCode::OK, Json(resp)).into_response()
}

/// POST /v1/responses (OpenAI Responses API surface, native body passthrough).
/// The whole request body rides as `raw` through ResponsesEngine; returns the
/// engine's native Responses response passthrough.
async fn responses(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers) {
        Ok(ak) => ak,
        Err((st, msg)) => return error_response(st, msg),
    };
    let model = body["model"].as_str().unwrap_or_default().to_owned();
    if model.is_empty() {
        return error_response(400, "model is required");
    }
    if body["input"].is_null() {
        return error_response(400, "input is required");
    }
    let stream = body["stream"].as_bool().unwrap_or(false);
    // native passthrough: the whole Responses-shaped body rides in `raw`;
    // resolve_model maps `model` → Protocol::Responses → ResponsesEngine.
    let mut param = ModelParamV2::with_name(gw_consts::Protocol::Responses, model);
    param.raw = body;
    let request = GatewayRequest {
        is_online: true,
        stream,
        ak: ak.ak.clone(),
        model_param_v2: Some(param),
        ..Default::default()
    };
    let ctx = match run_pipeline(&s, request, ak).await {
        Ok(ctx) => ctx,
        Err(e) => return gateway_error(e),
    };
    log_access("responses", &ctx, started);
    let Some(outcome) = ctx.outcome else {
        return error_response(500, "pipeline produced no outcome");
    };

    if stream {
        return responses_sse(&outcome).into_response();
    }
    match outcome.response.response_v2 {
        Some(v) => (StatusCode::OK, Json(v)).into_response(),
        None => error_response(500, "responses engine returned no payload"),
    }
}

/// Re-emit a Responses outcome as the client-facing Responses SSE sequence:
/// `response.output_text.delta` per text chunk, then `response.completed` with
/// usage (input_tokens/output_tokens — the Responses dialect). Synthesizes a
/// single delta when the engine returned a buffered (non-streaming) reply.
fn responses_sse(
    outcome: &gw_engines::EngineOutcome,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>> + use<>> {
    let r = &outcome.response;
    let mut events: Vec<Event> = Vec::new();
    events.push(Event::default().data(
        json!({"type": "response.created", "response": {"model": r.model, "status": "in_progress"}})
            .to_string(),
    ));
    // deltas: use engine chunks, or synthesize one from the full message.
    let deltas: Vec<String> = if outcome.chunks.is_empty() && !r.message.is_empty() {
        vec![r.message.clone()]
    } else {
        outcome
            .chunks
            .iter()
            .filter(|c| !c.delta.is_empty())
            .map(|c| c.delta.clone())
            .collect()
    };
    for d in deltas {
        events.push(
            Event::default()
                .data(json!({"type": "response.output_text.delta", "delta": d}).to_string()),
        );
    }
    let status = if r.finish_reason.is_empty() {
        "completed"
    } else {
        r.finish_reason.as_str()
    };
    events.push(
        Event::default().data(
            json!({"type": "response.completed", "response": {
                "model": r.model,
                "status": status,
                "usage": {
                    "input_tokens": r.prompt_tokens,
                    "output_tokens": r.completion_tokens,
                    "total_tokens": r.total_tokens,
                },
            }})
            .to_string(),
        ),
    );
    events.push(Event::default().data("[DONE]"));
    Sse::new(futures::stream::iter(
        events.into_iter().map(Ok::<_, Infallible>),
    ))
}

async fn embeddings(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers) {
        Ok(ak) => ak,
        Err((st, msg)) => return error_response(st, msg),
    };
    let model = body["model"].as_str().unwrap_or_default().to_owned();
    let input: Vec<String> = match &body["input"] {
        Value::String(x) => vec![x.clone()],
        Value::Array(a) => a
            .iter()
            .filter_map(|v| v.as_str().map(str::to_owned))
            .collect(),
        _ => vec![],
    };
    if model.is_empty() || input.is_empty() {
        return error_response(400, "model and input are required");
    }
    let typed = TypedParams::Embeddings(EmbeddingParams {
        input,
        dimensions: body["dimensions"].as_i64(),
    });
    let ctx = match run_family(&s, ak, model, typed, vec![]).await {
        Ok(ctx) => ctx,
        Err(resp) => return resp,
    };
    log_access("embeddings", &ctx, started);
    let Some(outcome) = ctx.outcome else {
        return error_response(500, "pipeline produced no outcome");
    };
    // Engine passes through the full openai-shaped response body
    match outcome.response.response_v2 {
        Some(v) => (StatusCode::OK, Json(v)).into_response(),
        None => error_response(500, "embeddings engine returned no payload"),
    }
}

/// POST /v1/images/generations (OpenAI-compatible image generation surface)
async fn images_generations(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers) {
        Ok(ak) => ak,
        Err((st, msg)) => return error_response(st, msg),
    };
    let model = body["model"].as_str().unwrap_or_default().to_owned();
    let prompt = body["prompt"].as_str().unwrap_or_default().to_owned();
    if model.is_empty() || prompt.is_empty() {
        return error_response(400, "model and prompt are required");
    }
    let typed = TypedParams::Image(ImageParams {
        prompt,
        n: body["n"].as_i64().unwrap_or(1),
        size: body["size"].as_str().map(str::to_owned),
        ..Default::default()
    });
    let ctx = match run_family(&s, ak, model, typed, vec![]).await {
        Ok(ctx) => ctx,
        Err(resp) => return resp,
    };
    log_access("images", &ctx, started);
    match ctx.outcome.and_then(|o| o.response.response_v2) {
        Some(v) => (StatusCode::OK, Json(v)).into_response(),
        None => error_response(500, "image engine returned no payload"),
    }
}

/// POST /v1/images/edits (source image + optional mask + prompt).
/// Same engine as generations; presence of `image` routes to the edit endpoint.
/// Client sends the image as base64 JSON, matching the audio surface.
async fn images_edits(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers) {
        Ok(ak) => ak,
        Err((st, msg)) => return error_response(st, msg),
    };
    let model = body["model"].as_str().unwrap_or_default().to_owned();
    let prompt = body["prompt"].as_str().unwrap_or_default().to_owned();
    let image = body["image"].as_str().unwrap_or_default().to_owned();
    if model.is_empty() || prompt.is_empty() || image.is_empty() {
        return error_response(400, "model, prompt and image are required");
    }
    let typed = TypedParams::Image(ImageParams {
        prompt,
        n: body["n"].as_i64().unwrap_or(1),
        size: body["size"].as_str().map(str::to_owned),
        image: Some(image),
        mask: body["mask"].as_str().map(str::to_owned),
    });
    let ctx = match run_family(&s, ak, model, typed, vec![]).await {
        Ok(ctx) => ctx,
        Err(resp) => return resp,
    };
    log_access("images_edits", &ctx, started);
    match ctx.outcome.and_then(|o| o.response.response_v2) {
        Some(v) => (StatusCode::OK, Json(v)).into_response(),
        None => error_response(500, "image engine returned no payload"),
    }
}

/// POST /v1/audio/speech (TTS, returns audio bytes; OpenAI-compatible surface)
async fn audio_speech(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers) {
        Ok(ak) => ak,
        Err((st, msg)) => return error_response(st, msg),
    };
    let model = body["model"].as_str().unwrap_or_default().to_owned();
    let input = body["input"].as_str().unwrap_or_default().to_owned();
    if model.is_empty() || input.is_empty() {
        return error_response(400, "model and input are required");
    }
    let format = body["response_format"].as_str().unwrap_or("mp3").to_owned();
    let typed = TypedParams::AudioTts(TtsParams {
        input,
        voice: body["voice"].as_str().map(str::to_owned),
        response_format: Some(format.clone()),
    });
    let ctx = match run_family(&s, ak, model, typed, vec![]).await {
        Ok(ctx) => ctx,
        Err(resp) => return resp,
    };
    log_access("audio_speech", &ctx, started);
    let Some(b64) = ctx
        .outcome
        .and_then(|o| o.response.response_v2)
        .and_then(|v| v["audio_b64"].as_str().map(str::to_owned))
    else {
        return error_response(500, "tts engine returned no audio");
    };
    let content_type = match format.as_str() {
        "wav" => "audio/wav",
        "pcm" => "audio/pcm",
        "opus" => "audio/opus",
        "aac" => "audio/aac",
        "flac" => "audio/flac",
        _ => "audio/mpeg",
    };
    match base64::engine::general_purpose::STANDARD.decode(&b64) {
        Ok(bytes) => (StatusCode::OK, [("content-type", content_type)], bytes).into_response(),
        Err(e) => error_response(500, format!("bad audio payload: {e}")),
    }
}

/// POST /v1/audio/transcriptions (STT; JSON carries b64 audio instead of a
/// multipart upload; field semantics match. Multipart support is future work)
async fn audio_transcriptions(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers) {
        Ok(ak) => ak,
        Err((st, msg)) => return error_response(st, msg),
    };
    let model = body["model"].as_str().unwrap_or_default().to_owned();
    let audio = body["audio_b64"].as_str().unwrap_or_default().to_owned();
    if model.is_empty() || audio.is_empty() {
        return error_response(400, "model and audio_b64 are required");
    }
    let typed = TypedParams::AudioStt(SttParams {
        audio_b64: audio,
        language: body["language"].as_str().map(str::to_owned),
    });
    let ctx = match run_family(&s, ak, model, typed, vec![]).await {
        Ok(ctx) => ctx,
        Err(resp) => return resp,
    };
    log_access("audio_transcriptions", &ctx, started);
    match ctx.outcome {
        Some(o) => (StatusCode::OK, Json(json!({ "text": o.response.message }))).into_response(),
        None => error_response(500, "stt engine returned no outcome"),
    }
}

/// POST /v1/batches (in-memory version: submit runs in background immediately)
/// Parse the `messages` array of a batch request object into engine messages.
fn parse_batch_messages(v: &Value) -> Vec<ChatMsg> {
    v["messages"]
        .as_array()
        .map(|ms| {
            ms.iter()
                .map(|m| {
                    ChatMsg::text(
                        m["role"].as_str().unwrap_or("user"),
                        m["content"].as_str().unwrap_or_default(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

async fn batches_submit(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let ak = match authenticate(&s, &headers) {
        Ok(ak) => ak,
        Err((st, msg)) => return error_response(st, msg),
    };
    let mut model = body["model"].as_str().unwrap_or_default().to_owned();
    let mut batch_items = Vec::new();

    // Two input modes: inline `items`, or an uploaded JSONL `input_file_id`
    // (the OpenAI batch pattern — each line is {custom_id,method,url,body}).
    if let Some(file_id) = body["input_file_id"].as_str() {
        let file = match s.handler.state.store.file_get(file_id).await {
            Ok(Some(f)) => f,
            Ok(None) => return error_response(404, format!("input file {file_id} not found")),
            Err(e) => return gateway_error(e),
        };
        for line in file.content.lines().filter(|l| !l.trim().is_empty()) {
            let Ok(req) = serde_json::from_str::<Value>(line) else {
                return error_response(400, "input file line is not valid json");
            };
            let reqbody = &req["body"];
            if model.is_empty()
                && let Some(m) = reqbody["model"].as_str()
            {
                model = m.to_owned();
            }
            let msgs = parse_batch_messages(reqbody);
            if msgs.is_empty() {
                return error_response(400, "input file line missing a messages array");
            }
            batch_items.push(BatchItem { messages: msgs });
        }
    } else if let Some(items) = body["items"].as_array() {
        for it in items {
            let msgs = parse_batch_messages(it);
            if msgs.is_empty() {
                return error_response(400, "each item needs a non-empty messages array");
            }
            batch_items.push(BatchItem { messages: msgs });
        }
    } else {
        return error_response(400, "either items or input_file_id is required");
    }

    if model.is_empty() || batch_items.is_empty() {
        return error_response(400, "model and non-empty items are required");
    }
    let job = match s.offline.submit(ak, model, batch_items).await {
        Ok(job) => job,
        Err(e) => return gateway_error(e),
    };
    (
        StatusCode::ACCEPTED,
        Json(json!({ "id": job.id, "status": job.status, "total": job.total })),
    )
        .into_response()
}

/// POST /v1/files (file upload; batch input JSONL, etc). Uses a JSON `file`
/// field (string content) instead of multipart, matching the audio/images surfaces.
async fn files_upload(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    // gate on a valid AK (the file store isn't AK-scoped in this local subset).
    if let Err((st, msg)) = authenticate(&s, &headers) {
        return error_response(st, msg);
    }
    let purpose = body["purpose"].as_str().unwrap_or("batch").to_owned();
    let Some(content) = body["file"].as_str() else {
        return error_response(400, "file content (string) is required");
    };
    if content.is_empty() {
        return error_response(400, "file content must not be empty");
    }
    let f = match s
        .handler
        .state
        .store
        .file_put(&purpose, content.to_owned())
        .await
    {
        Ok(f) => f,
        Err(e) => return gateway_error(e),
    };
    (
        StatusCode::OK,
        Json(json!({
            "id": f.id, "object": "file", "bytes": f.bytes,
            "purpose": f.purpose, "created_at": chrono_now(),
        })),
    )
        .into_response()
}

/// GET /v1/files/{id} (file metadata).
async fn files_get(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    // ids are sequential — without auth any client could enumerate all files.
    if let Err((st, msg)) = authenticate(&s, &headers) {
        return error_response(st, msg);
    }
    match s.handler.state.store.file_get(&id).await {
        Ok(Some(f)) => (
            StatusCode::OK,
            Json(json!({"id": f.id, "object": "file", "bytes": f.bytes, "purpose": f.purpose})),
        )
            .into_response(),
        Ok(None) => error_response(404, format!("file {id} not found")),
        Err(e) => gateway_error(e),
    }
}

/// GET /v1/files/{id}/content (download raw content: batch output, etc).
async fn files_content(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err((st, msg)) = authenticate(&s, &headers) {
        return error_response(st, msg);
    }
    match s.handler.state.store.file_get(&id).await {
        Ok(Some(f)) => (StatusCode::OK, f.content).into_response(),
        Ok(None) => error_response(404, format!("file {id} not found")),
        Err(e) => gateway_error(e),
    }
}

/// GET /v1/batches/{id}
async fn batches_get(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err((st, msg)) = authenticate(&s, &headers) {
        return error_response(st, msg);
    }
    match s.handler.state.store.batch_get(&id).await {
        Ok(Some(job)) => (StatusCode::OK, Json(json!(job))).into_response(),
        Ok(None) => error_response(404, format!("batch {id} not found")),
        Err(e) => gateway_error(e),
    }
}

fn chrono_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_app() -> Router {
        let cfg = Arc::new(GatewayConfig::embedded_default().unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        app(AppState::new(
            cfg,
            state,
            Arc::new(gw_engines::MockTransport),
        ))
    }

    async fn body_json(resp: Response) -> Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn chat_req(auth: Option<&str>, body: &str) -> Request<Body> {
        let mut b = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json");
        if let Some(a) = auth {
            b = b.header("authorization", format!("Bearer {a}"));
        }
        b.body(Body::from(body.to_owned())).unwrap()
    }

    #[tokio::test]
    async fn requires_auth() {
        let resp = test_app()
            .oneshot(chat_req(
                None,
                r#"{"model":"gpt-4o","messages":[{"role":"user","content":"x"}]}"#,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn chat_non_stream_ok() {
        let resp = test_app()
            .oneshot(chat_req(
                Some("ak-demo-123"),
                r#"{"model":"gpt-4o","messages":[{"role":"user","content":"hello"}]}"#,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let j = body_json(resp).await;
        assert_eq!(j["object"], "chat.completion");
        assert!(
            j["choices"][0]["message"]["content"]
                .as_str()
                .unwrap()
                .contains("hello")
        );
        assert!(j["usage"]["total_tokens"].as_i64().unwrap() > 0);
    }

    #[tokio::test]
    async fn unknown_model_404() {
        let resp = test_app()
            .oneshot(chat_req(
                Some("ak-demo-123"),
                r#"{"model":"nope","messages":[{"role":"user","content":"x"}]}"#,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn finish_reason_mapping_both_directions() {
        // anthropic stop_reason → openai finish_reason
        assert_eq!(finish_openai("end_turn"), "stop");
        assert_eq!(finish_openai("stop_sequence"), "stop");
        assert_eq!(finish_openai(""), "stop"); // absent → stop
        assert_eq!(finish_openai("max_tokens"), "length");
        assert_eq!(finish_openai("tool_use"), "tool_calls");
        // unknown values pass through unchanged (e.g. content-policy signals)
        assert_eq!(finish_openai("refusal"), "refusal");

        // openai finish_reason → anthropic stop_reason
        assert_eq!(finish_anthropic("stop"), "end_turn");
        assert_eq!(finish_anthropic(""), "end_turn");
        assert_eq!(finish_anthropic("length"), "max_tokens");
        assert_eq!(finish_anthropic("tool_calls"), "tool_use");
        assert_eq!(finish_anthropic("content_filter"), "content_filter");

        // round-trip on the canonical trio is stable
        for (o, a) in [
            ("stop", "end_turn"),
            ("length", "max_tokens"),
            ("tool_calls", "tool_use"),
        ] {
            assert_eq!(finish_anthropic(o), a, "openai→anthropic {o}");
            assert_eq!(finish_openai(a), o, "anthropic→openai {a}");
        }
    }
}
