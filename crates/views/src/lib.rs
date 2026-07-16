//! HTTP view layer (L5): parse/validate, authenticate the AK, build a
//! `GatewayRequest`, call the handler, shape the wire response, and emit one
//! structured access-log line per request.

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::fmt::Write as _;
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
use gw_engines::SharedTransport;
use gw_engines::realtime::{is_response_create, realtime_turn_started, realtime_usage};
use gw_handler::{BatchItem, OfflineHandler, OnlineHandler};
use gw_models::{
    ChatMsg, ChatParams, EmbeddingParams, GResult, GatewayError, GatewayRequest, ImageParams,
    ModelParamV2, SttParams, TtsParams, TypedParams,
};
use gw_protocol::anthropic::{AnthUsage, MessagesRequest, MessagesResponse};
use gw_protocol::openai::{
    ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse, Usage,
};
use gw_state::admission;
use gw_state::{AkInfo, GatewayState};
use serde_json::{Value, json};

const LEDGER_PAGE_DEFAULT: usize = 100;
const KEY_PAGE_DEFAULT: usize = 200;
const STREAM_CHANNEL_CAP: usize = 64;
/// Per-turn token reserve against the AK daily quota; settled to actuals at billing.
const REALTIME_TURN_RESERVE: i64 = 1_000;

static REQ_SEQ: AtomicU64 = AtomicU64::new(1);

/// A boxed future resolving to a freshly loaded config.
pub type ConfigFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<GatewayConfig, String>> + Send>>;
/// Reloads config from its source (file or the Postgres config store).
pub type ConfigLoader = Arc<dyn Fn() -> ConfigFuture + Send + Sync>;

#[derive(Clone)]
pub struct AppState {
    pub handler: OnlineHandler,
    pub offline: OfflineHandler,
    /// Reloads config from its source; `None` = reload not wired (tests).
    pub loader: Option<ConfigLoader>,
    /// Fleet config store; enables `PUT /admin/config`. `None` = file-based.
    pub config_store: Option<Arc<gw_state::PostgresConfigStore>>,
}

impl AppState {
    pub fn new(
        cfg: Arc<GatewayConfig>,
        state: Arc<GatewayState>,
        transport: SharedTransport,
    ) -> Self {
        Self::with_config(gw_state::SharedConfig::new(cfg, state), transport, None)
    }

    pub fn with_config(
        config: gw_state::SharedConfig,
        transport: SharedTransport,
        loader: Option<ConfigLoader>,
    ) -> Self {
        let handler = OnlineHandler::new(config, transport);
        let offline = OfflineHandler::new(handler.clone());
        Self {
            handler,
            offline,
            loader,
            config_store: None,
        }
    }

    /// Attach the fleet config store (enables `PUT /admin/config`).
    pub fn with_config_store(mut self, store: Arc<gw_state::PostgresConfigStore>) -> Self {
        self.config_store = Some(store);
        self
    }

    /// Reload config from source and swap it in atomically (transport policy
    /// rides along in the handler); storage-backend (redis/sqlite URL) changes
    /// need a restart and are ignored here.
    pub async fn reload(&self) -> Result<(), String> {
        let loader = self.loader.as_ref().ok_or("reload not configured")?;
        let cfg = loader().await?;
        self.handler.reload(cfg).await.map_err(|e| e.to_string())
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
        .route("/v1/files/{id}", get(files_get).delete(files_delete))
        .route("/v1/files/{id}/content", get(files_content))
        .route("/v1/realtime", get(realtime_ws))
        .route("/internal/ledger", get(ledger))
        .route("/internal/accounts", get(accounts))
        .route("/admin/reload", post(admin_reload))
        .route("/admin/config", axum::routing::put(admin_config_put))
        .route("/admin/keys", post(admin_key_create).get(admin_key_list))
        .route("/admin/usage", get(admin_usage))
        .route("/admin/usage/users", get(admin_usage_users))
        .route("/admin/audit/events", get(admin_security_events))
        .route("/admin/audit/ops", get(admin_audit_ops))
        .route("/admin/audit/content/{request_id}", get(admin_content_get))
        .route(
            "/admin/audit/content",
            axum::routing::delete(admin_content_erase),
        )
        .route(
            "/admin/keys/{ak}",
            axum::routing::patch(admin_key_patch).delete(admin_key_delete),
        )
        .layer(axum::middleware::from_fn(track_requests))
        .with_state(state)
}

/// Counts every response with bounded labels: route template and status code.
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

/// The AK carried as `gw-api-key.<ak>` in `Sec-WebSocket-Protocol` — the one
/// header a browser WebSocket can set; a query param would leak into LB logs.
fn ws_subprotocol_ak(headers: &HeaderMap) -> Option<String> {
    headers
        .get("sec-websocket-protocol")?
        .to_str()
        .ok()?
        .split(',')
        .map(str::trim)
        .find_map(|p| p.strip_prefix("gw-api-key."))
        .map(str::to_owned)
}

/// GET /v1/realtime?model=... (WebSocket upgrade): bridge to the vendor's
/// realtime WebSocket, or the local mock session for an endpoint-less account.
async fn realtime_ws(
    State(s): State<AppState>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
    ws: axum::extract::ws::WebSocketUpgrade,
) -> Response {
    // one consistent snapshot for the whole accept decision (cfg + state)
    let snap = s.handler.config.load();
    let ak = match authenticate(&s, &headers).await {
        Ok(ak) => ak,
        Err((st, msg)) => {
            let sub = match ws_subprotocol_ak(&headers) {
                Some(k) => snap.state.auth.authenticate(&k).await,
                None => None,
            };
            match sub {
                Some(ak) => match check_key_status(&ak) {
                    Ok(()) => ak,
                    Err((st, msg)) => return error_response(st, msg),
                },
                None => return error_response(st, msg),
            }
        }
    };
    let Some(model) = q.get("model").cloned() else {
        return error_response(400, "model query param is required");
    };
    let model_conf = snap.cfg.find_model(&model);
    let mt = model_conf
        .and_then(|m| m.protocol())
        .or_else(|| gw_consts::Protocol::from_wire(&model));
    let Some(mt) = mt else {
        return error_response(404, format!("unknown model: {model}"));
    };
    if mt != gw_consts::Protocol::Realtime {
        return error_response(400, format!("`{model}` is not a realtime model"));
    }
    // same tenant entitlement gate as REST — realtime must not be a bypass
    if !snap.cfg.tenant_allows_model(&ak.tenant, &model) {
        return error_response(
            403,
            format!("model `{model}` is not entitled for tenant `{}`", ak.tenant),
        );
    }
    let account = snap
        .state
        .pool
        .select_healthy(
            mt,
            model_conf.and_then(|m| m.provider.as_deref()),
            &[],
            snap.state.health.as_ref(),
        )
        .await;
    let Some(account) = account else {
        return error_response(503, format!("no healthy upstream account serves `{model}`"));
    };
    // select "realtime" so subprotocol-offering clients get a valid handshake
    let ws = ws.protocols(["realtime"]);
    // client attribution hint captured at connect (no per-turn body user field)
    let hint = user_header(&headers).unwrap_or_default();
    if account.endpoint.is_empty() {
        ws.on_upgrade(move |socket| {
            realtime_session(socket, s, ak, model, mt, account.name.clone(), hint)
        })
    } else if gw_engines::realtime::is_gemini_realtime(&account.provider) {
        // no pre-generation gate signal in this dialect — refuse rather than bill after the fact
        error_response(
            501,
            format!(
                "realtime is not supported for provider `{}`",
                account.provider
            ),
        )
    } else {
        ws.on_upgrade(move |socket| realtime_bridge(socket, s, ak, model, mt, account, hint))
    }
}

/// A turn admitted by [`realtime_gate`]: the freshly re-authenticated key,
/// the reserves taken, the admission day (the paired settle/refund lands on
/// the same bucket), and the admission snapshot (settlement must not drift
/// from the admission config when a reload lands mid-turn).
struct RealtimeAdmit {
    ak: AkInfo,
    /// Effective attribution user for this turn: the key's owner if set, else
    /// the client's connect-time `x-gw-user` hint; empty for an ownerless key
    /// with no hint. Captured at admission so billing and budget agree.
    user: String,
    reserved: i64,
    /// Tokens reserved in the AK TPM window; `None` when the key has no TPM cap.
    tpm_reserved: Option<i64>,
    at: i64,
    /// Per-turn correlation id for the ledger row.
    request_id: String,
    snap: Arc<gw_state::Snapshot>,
}

impl RealtimeAdmit {
    /// Refund this turn's unsettled reserves — for a turn dropped before its boundary frame.
    async fn refund(&self) {
        self.snap
            .state
            .governance
            .refund_reserves(&self.ak.ak, self.reserved, self.tpm_reserved, self.at)
            .await;
    }
}

/// The REST admission chain applied per realtime generation via the shared
/// [`admission`] checks, with the key re-fetched each turn so mid-session
/// bans/de-entitlements take effect. Two deliberate divergences from the DAG:
/// over-quota denies instead of degrading (a session can't swap models
/// mid-stream), and the reserve is a fixed turn estimate. Reserves are taken
/// last so a denial never leaves one behind; a failed TPM reserve rolls back
/// the daily reserve just taken.
async fn realtime_gate(
    s: &AppState,
    ak: &AkInfo,
    model: &str,
    hint: &str,
) -> Result<RealtimeAdmit, String> {
    let snap = s.handler.config.load();
    let (cfg, state) = (&snap.cfg, &snap.state);
    let ak = match state.auth.authenticate(&ak.ak).await {
        Some(fresh) if fresh.status_at(gw_state::epoch_secs()) == gw_state::KeyStatus::Active => {
            fresh
        }
        _ => return Err(format!("access key {} is no longer valid", ak.ak)),
    };
    if !cfg.tenant_allows_model(&ak.tenant, model) {
        return Err(format!(
            "model `{model}` is not entitled for tenant `{}`",
            ak.tenant
        ));
    }
    let gov = state.governance.as_ref();
    admission::check_tenant_rate(gov, cfg, &ak.tenant).await?;
    admission::check_ak_rate(gov, &ak).await?;
    admission::check_product_qpm(gov, cfg, &ak.product).await?;
    admission::check_model_qpm(gov, cfg, model).await?;
    admission::check_user_budget(gov, cfg, &ak.tenant, ak.attributed_user(hint)).await?;
    if let Some(limit) = admission::model_quota_limit(cfg, &ak, model)
        && !gov
            .quota_check(&admission::model_quota_key(&ak.ak, model), limit)
            .await
    {
        return Err(format!("model quota exhausted for `{model}`"));
    }
    let at = gw_state::epoch_secs();
    admission::reserve_daily(gov, &ak, REALTIME_TURN_RESERVE, at).await?;
    let tpm_reserved = match admission::reserve_tpm(gov, &ak, REALTIME_TURN_RESERVE).await {
        Ok(reserved) => reserved,
        Err(denied) => {
            gov.quota_settle(&ak.ak, -REALTIME_TURN_RESERVE, at).await;
            return Err(denied);
        }
    };
    let user = ak.attributed_user(hint).to_owned();
    Ok(RealtimeAdmit {
        ak,
        user,
        reserved: REALTIME_TURN_RESERVE,
        tpm_reserved,
        at,
        request_id: gw_handler::new_request_id(),
        snap,
    })
}

/// Settle one realtime turn via the shared [`admission::settle_and_bill`]
/// orchestration, on the turn's admission snapshot; a zero-usage terminal
/// frame (cancelled/empty turn) refunds the reserves and writes nothing.
async fn bill_realtime_turn(
    admit: &RealtimeAdmit,
    model: &str,
    mt: gw_consts::Protocol,
    account: &str,
    it: i64,
    ot: i64,
) {
    let ak = &admit.ak;
    // clamp parts and total so a hostile count can't overflow shared counters
    let (it, ot) = (gw_state::clamp_tokens(it), gw_state::clamp_tokens(ot));
    let total = gw_state::clamp_tokens(it.saturating_add(ot));
    if total == 0 {
        admit.refund().await;
        return;
    }
    let (cfg, state) = (&admit.snap.cfg, &admit.snap.state);
    let model_quota_key = admission::model_quota_limit(cfg, ak, model)
        .map(|_| admission::model_quota_key(&ak.ak, model));
    admission::settle_and_bill(
        state.governance.as_ref(),
        state.store.as_ref(),
        cfg,
        admission::SettleInput {
            billing: gw_state::BillingInput {
                ak: &ak.ak,
                product: &ak.product,
                tenant: &ak.tenant,
                user_id: admit.user.as_str(),
                request_id: &admit.request_id,
                requested_model: model,
                served_model: model,
                protocol: mt.as_str(),
                account,
                prompt: it,
                completion: ot,
                total,
                ptu_spillover: false,
                estimated: false,
            },
            reserved: admit.reserved,
            tpm_reserved: admit.tpm_reserved,
            reserved_at: admit.at,
            model_quota_key,
        },
    )
    .await;
    admission::consume_user_budget(
        state.governance.as_ref(),
        cfg,
        &ak.tenant,
        admit.user.as_str(),
        total,
    )
    .await;
    metrics::counter!("gateway_tokens_total", "kind" => "prompt").increment(it as u64);
    metrics::counter!("gateway_tokens_total", "kind" => "completion").increment(ot as u64);
}

/// One mock realtime session.
async fn realtime_session(
    mut socket: axum::extract::ws::WebSocket,
    s: AppState,
    ak: AkInfo,
    model: String,
    mt: gw_consts::Protocol,
    account: String,
    hint: String,
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
        let Ok(mut ev) = serde_json::from_str::<Value>(&text) else {
            let _ = socket
                .send(send(json!({"type":"error","message":"invalid json event"})))
                .await;
            continue;
        };
        if let Err(reason) = rt_inbound_policy(&s, &ak, &hint, &mut ev).await {
            let _ = socket
                .send(send(json!({"type":"error","message": reason})))
                .await;
            continue;
        }
        match ev["type"].as_str().unwrap_or_default() {
            "input_text" => {
                let admit = match realtime_gate(&s, &ak, &model, &hint).await {
                    Ok(a) => a,
                    Err(denied) => {
                        let _ = socket
                            .send(send(json!({"type":"error","message": denied})))
                            .await;
                        continue;
                    }
                };
                let input = ev["text"].as_str().unwrap_or_default().to_owned();
                let reply = format!("[mock-realtime:{model}] you said: {input}");
                let (it, ot) = (
                    (input.len() as i64 / 4).max(1) + 3,
                    (reply.len() as i64 / 4).max(1),
                );
                let mid = (0..=reply.len() / 2)
                    .rev()
                    .find(|&i| reply.is_char_boundary(i))
                    .unwrap_or(0);
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
                bill_realtime_turn(&admit, &model, mt, &account, it, ot).await;
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

/// Cross the axum↔tungstenite text-frame boundary without copying: both wrap
/// `bytes::Bytes`, so the payload stays refcounted and is only re-validated as
/// UTF-8. The lossy fallback is unreachable in practice (input was validated).
fn client_text_to_upstream(
    t: axum::extract::ws::Utf8Bytes,
) -> tokio_tungstenite::tungstenite::Message {
    let b = bytes::Bytes::from(t);
    match tokio_tungstenite::tungstenite::Utf8Bytes::try_from(b.clone()) {
        Ok(u) => tokio_tungstenite::tungstenite::Message::Text(u),
        Err(_) => {
            tokio_tungstenite::tungstenite::Message::text(String::from_utf8_lossy(&b).into_owned())
        }
    }
}

/// The reverse direction of [`client_text_to_upstream`].
fn upstream_text_to_client(
    t: tokio_tungstenite::tungstenite::Utf8Bytes,
) -> axum::extract::ws::Message {
    let b = bytes::Bytes::from(t);
    match axum::extract::ws::Utf8Bytes::try_from(b.clone()) {
        Ok(u) => axum::extract::ws::Message::Text(u),
        Err(_) => axum::extract::ws::Message::Text(String::from_utf8_lossy(&b).into_owned().into()),
    }
}

/// Bridge one realtime session to a real upstream over WebSocket: transparent
/// relay plus auth, per-generation gates, and per-turn billing. Only the OpenAI
/// dialect reaches here — [`realtime_ws`] refuses providers it can't gate; the
/// Gemini metering in [`realtime_usage`] is groundwork for a future adapter.
/// Per-dialect frame semantics live in [`gw_engines::realtime`].
async fn realtime_bridge(
    mut client: axum::extract::ws::WebSocket,
    s: AppState,
    ak: AkInfo,
    model: String,
    mt: gw_consts::Protocol,
    account: Arc<gw_models::Account>,
    hint: String,
) {
    use axum::extract::ws::Message as CMsg;
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message as UMsg;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let send_err = |m: String| json!({"type":"error","error":{"type":"gateway_error","message":m}});

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

    let mut generations = 0u64;
    // boundary frames recognized; zero while generations flowed = unmetered dialect
    let mut recognized = 0u64;
    // the one admitted turn awaiting settle (the OpenAI dialect allows a single
    // active response); refunded on exit so its reserve never leaks
    let mut pending: Option<RealtimeAdmit> = None;
    // denied server-VAD turn: swallow its upstream frames until its terminal frame
    let mut suppress = false;
    // outbound DLP redactions summed within a turn, recorded once at its boundary
    let mut out_redacted = 0i64;
    loop {
        tokio::select! {
            m = cl_rx.next() => {
                // text and binary frames are parsed alike so neither encoding
                // bypasses the gate or the content-security pass; a non-JSON
                // frame (raw audio) carries no scannable text and relays as-is
                let (frame, mut forward) = match m {
                    Some(Ok(CMsg::Text(t))) => (serde_json::from_str::<Value>(&t).ok(), client_text_to_upstream(t)),
                    Some(Ok(CMsg::Binary(b))) => (serde_json::from_slice::<Value>(&b).ok(), UMsg::binary(b)),
                    Some(Ok(CMsg::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => continue, // ping/pong handled by the ws stacks
                };
                if let Some(mut frame) = frame {
                    match rt_inbound_policy(&s, &ak, &hint, &mut frame).await {
                        Err(reason) => {
                            if cl_tx
                                .send(CMsg::Text(send_err(reason).to_string().into()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                            continue;
                        }
                        Ok(redacted) => {
                            if redacted > 0 {
                                forward = UMsg::text(frame.to_string());
                            }
                        }
                    }
                    // gate each generation trigger, not every control frame.
                    // With a turn already admitted the trigger relays ungated:
                    // upstream rejects the duplicate, and a raced accept is
                    // caught by the response.created gate below
                    if is_response_create(&frame) && pending.is_none() {
                        match realtime_gate(&s, &ak, &model, &hint).await {
                            Ok(admit) => {
                                pending = Some(admit);
                                generations += 1;
                            }
                            Err(denied) => {
                                if cl_tx
                                    .send(CMsg::Text(send_err(denied).to_string().into()))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                                continue;
                            }
                        }
                    }
                }
                if up_tx.send(forward).await.is_err() {
                    break;
                }
            },
            m = up_rx.next() => {
                // text and binary frames are parsed alike so a vendor encoding
                // its JSON events as binary can't bypass settlement or DLP;
                // non-JSON binary (audio) relays unchanged, suppress-gated
                let (frame, was_text, raw_text, raw_bytes) = match m {
                    Some(Ok(UMsg::Text(t))) => {
                        (serde_json::from_str::<Value>(&t).ok(), true, Some(t), None)
                    }
                    Some(Ok(UMsg::Binary(b))) => {
                        (serde_json::from_slice::<Value>(&b).ok(), false, None, Some(b))
                    }
                    Some(Ok(UMsg::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => continue, // ping/pong handled by the ws stacks
                };
                let mut relay = true;
                let mut redacted: Option<String> = None;
                let mut turn_ended = false;
                match frame {
                    Some(mut v) => {
                        if suppress {
                            relay = false;
                            if realtime_usage(&account.provider, &v).is_some() {
                                suppress = false;
                            }
                        }
                        // server-VAD: OpenAI auto-starts a turn with no client
                        // response.create — gate it here like a manual one
                        else if realtime_turn_started(&account.provider, &v) && pending.is_none() {
                            match realtime_gate(&s, &ak, &model, &hint).await {
                                Ok(admit) => pending = Some(admit),
                                Err(denied) => {
                                    let _ = up_tx
                                        .send(UMsg::text(json!({"type":"response.cancel"}).to_string()))
                                        .await;
                                    let _ = cl_tx
                                        .send(CMsg::Text(send_err(denied).to_string().into()))
                                        .await;
                                    suppress = true;
                                    relay = false;
                                }
                            }
                        } else if let Some((it, ot)) = realtime_usage(&account.provider, &v) {
                            // turn boundary — settle the admitted turn;
                            // a boundary with no gated turn bills unreserved
                            match pending.take() {
                                Some(a) => {
                                    bill_realtime_turn(&a, &model, mt, &account.name, it, ot).await
                                }
                                None if it.saturating_add(ot) > 0 => {
                                    // re-authenticate so billing uses the key's current
                                    // identity, not the stale handshake snapshot
                                    let snap = s.handler.config.load();
                                    let billed = snap
                                        .state
                                        .auth
                                        .authenticate(&ak.ak)
                                        .await
                                        .unwrap_or_else(|| ak.clone());
                                    let user = billed.attributed_user(&hint).to_owned();
                                    let unreserved = RealtimeAdmit {
                                        ak: billed,
                                        user,
                                        reserved: 0,
                                        tpm_reserved: None,
                                        at: gw_state::epoch_secs(),
                                        request_id: gw_handler::new_request_id(),
                                        snap,
                                    };
                                    bill_realtime_turn(
                                        &unreserved,
                                        &model,
                                        mt,
                                        &account.name,
                                        it,
                                        ot,
                                    )
                                    .await
                                }
                                None => {}
                            }
                            recognized += 1;
                            turn_ended = true;
                        }
                        // outbound DLP, per frame (a span straddling deltas is
                        // beyond a relay that cannot buffer)
                        let cfg = s.handler.cfg();
                        let n = if relay {
                            gw_handler::plugins::dlp_redact_realtime_frame(
                                cfg.security_for(&ak.tenant),
                                &mut v,
                            )
                        } else {
                            0
                        };
                        if n > 0 {
                            redacted = Some(v.to_string());
                        }
                        // per-token events would be too hot: sum the turn, record once at its boundary
                        out_redacted += n as i64;
                        if turn_ended {
                            flush_rt_out_dlp(&s, &ak, &hint, out_redacted).await;
                            out_redacted = 0;
                        }
                    }
                    // a denied turn's non-JSON output (e.g. audio deltas) is dropped too
                    None => relay = !suppress,
                }
                if relay {
                    let out = match (redacted, was_text, raw_text, raw_bytes) {
                        (Some(json), true, _, _) => CMsg::Text(json.into()),
                        (Some(json), false, _, _) => CMsg::Binary(json.into_bytes().into()),
                        (None, _, Some(t), _) => upstream_text_to_client(t),
                        (None, _, _, Some(b)) => CMsg::Binary(b),
                        (None, _, None, None) => continue,
                    };
                    if cl_tx.send(out).await.is_err() {
                        break;
                    }
                }
            },
        }
    }
    if let Some(a) = pending {
        a.refund().await;
    }
    // a turn aborted before its boundary (upstream drop) still applied its
    // redactions per frame — flush the pending count so the audit isn't lost
    flush_rt_out_dlp(&s, &ak, &hint, out_redacted).await;
    if generations > 0 && recognized == 0 {
        tracing::warn!(
            account = %account.name,
            model = %model,
            generations,
            "realtime bridge relayed generations but saw no usage frame — vendor dialect not recognized?"
        );
    }
}

/// One structured access-log line per served request; local stdout only.
fn log_access(surface: &str, ctx: &DagContext, started: Instant) {
    let (model, mt) = ctx
        .request
        .model_param_v2
        .as_ref()
        .map(|p| (p.model_name.as_str(), p.protocol.as_str()))
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
    let user_id = ctx.effective_user_id();
    metrics::counter!("gateway_tokens_total", "kind" => "prompt").increment(pt.max(0) as u64);
    metrics::counter!("gateway_tokens_total", "kind" => "completion").increment(ct.max(0) as u64);
    tracing::info!(
        target: "access",
        surface,
        request_id = %ctx.request.request_id,
        ak = %ctx.ak.ak,
        product = %ctx.ak.product,
        user_id,
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

/// Configured public models, filtered to the caller's tenant entitlement.
async fn list_models(State(s): State<AppState>, Authed(ak): Authed) -> Response {
    let cfg = s.handler.cfg();
    let data: Vec<Value> = cfg
        .models
        .iter()
        .filter(|m| cfg.tenant_allows_model(&ak.tenant, &m.name))
        .map(|m| {
            json!({
                "id": m.name,
                "object": "model",
                "protocol": m.protocol,
                "implemented": m.protocol().is_some(),
            })
        })
        .collect();
    let mut resp = json!({ "object": "list" });
    resp["data"] = Value::Array(data);
    Json(resp).into_response()
}

/// Local billing ledger snapshot.
async fn ledger(
    State(s): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    let limit = q_num(&q, "limit", LEDGER_PAGE_DEFAULT);
    match s.handler.state().store.ledger_snapshot(limit).await {
        Ok((count, records)) => Json(json!({ "count": count, "records": records })).into_response(),
        Err(e) => gateway_error(e),
    }
}

/// Account pool view (name/provider/tier/priority/served model family).
async fn accounts(State(s): State<AppState>) -> Json<Value> {
    let cfg = s.handler.cfg();
    let health = &s.handler.state().health;
    let mut data: Vec<Value> = Vec::with_capacity(cfg.accounts.len());
    for a in &cfg.accounts {
        data.push(json!({
            "name": a.name,
            "provider": a.provider,
            "priority": a.priority,
            "tier": if a.tier.is_empty() { gw_consts::account_tier::PAYGO } else { a.tier.as_str() },
            "health": health.status(&a.name).await,
            "protocols": a.protocols,
        }));
    }
    let mut resp = json!({ "count": data.len() });
    resp["accounts"] = Value::Array(data);
    Json(resp)
}

/// The `x-gw-user` attribution hint; surfaces fall back to the body's own user
/// field. See [`gw_models::GatewayRequest::user_id`] for the trust model.
fn user_header(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-gw-user")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .filter(|s| !s.is_empty())
}

/// The one request-metadata attribution precedence the REST surfaces apply:
/// `x-gw-user` header, else the dialect's own user field (OpenAI `user`,
/// Anthropic `metadata.user_id`). Batch items invert it — per-item `user`
/// first — so shared-key batches keep per-item attribution.
fn user_hint(headers: &HeaderMap, field: &Value) -> Option<String> {
    user_header(headers).or_else(|| field.as_str().map(str::to_owned))
}

/// AK auth: `Authorization: Bearer <ak>` or `x-api-key: <ak>`. The error is
/// `(status, message)` so each surface can shape it to its own wire dialect.
async fn authenticate(s: &AppState, headers: &HeaderMap) -> Result<AkInfo, (u16, &'static str)> {
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
    let info = s
        .handler
        .state()
        .auth
        .authenticate(ak)
        .await
        .ok_or((401, "invalid api key"))?;
    check_key_status(&info)?;
    Ok(info)
}

/// Lifecycle gate shared by every auth path: banned and expired keys stay in
/// the table but fail with distinct 403s (unlike a revoked key's 401).
fn check_key_status(info: &AkInfo) -> Result<(), (u16, &'static str)> {
    match info.status_at(gw_state::epoch_secs()) {
        gw_state::KeyStatus::Active => Ok(()),
        gw_state::KeyStatus::Banned => Err((403, "access key is banned")),
        gw_state::KeyStatus::Expired => Err((403, "access key has expired")),
    }
}

/// [`authenticate`] as an extractor, for the surfaces sharing the
/// OpenAI-shaped error; `messages` (Anthropic error shape) and `realtime_ws`
/// (subprotocol fallback) run their own. Runs before the body extractor, so
/// an unauthenticated payload is never parsed.
struct Authed(AkInfo);

impl axum::extract::FromRequestParts<AppState> for Authed {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        s: &AppState,
    ) -> Result<Self, Self::Rejection> {
        match authenticate(s, &parts.headers).await {
            Ok(ak) => Ok(Authed(ak)),
            Err((st, msg)) => Err(error_response(st, msg)),
        }
    }
}

fn error_response(status: u16, message: impl Into<String>) -> Response {
    let code = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (
        code,
        Json(json!({ "error": { "message": message.into(), "type": "gateway_error" } })),
    )
        .into_response()
}

/// Who an admin bearer token speaks for: the global operator or one tenant.
enum AdminScope {
    Global,
    Tenant(String),
}

impl AdminScope {
    /// Whether this scope may act on a key belonging to `tenant`.
    fn covers(&self, tenant: &str) -> bool {
        match self {
            AdminScope::Global => true,
            AdminScope::Tenant(t) => t == tenant,
        }
    }

    /// (actor, scope) for the audit trail: who the token spoke for.
    fn audit_identity(&self) -> (&str, &'static str) {
        match self {
            AdminScope::Global => ("global", "global"),
            AdminScope::Tenant(t) => (t.as_str(), "tenant"),
        }
    }

    /// The tenant a scoped read is confined to: a tenant admin sees only its
    /// own; the global admin may narrow with `?tenant=`.
    fn tenant_filter<'a>(&'a self, q: &'a HashMap<String, String>) -> Option<&'a str> {
        match self {
            AdminScope::Tenant(t) => Some(t.as_str()),
            AdminScope::Global => q.get("tenant").map(String::as_str),
        }
    }
}

impl axum::extract::FromRequestParts<AppState> for AdminScope {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        s: &AppState,
    ) -> Result<Self, Self::Rejection> {
        admin_auth(s, &parts.headers)
    }
}

/// A numeric query param, or `default` when absent/unparseable.
fn q_num<T: std::str::FromStr>(q: &HashMap<String, String>, key: &str, default: T) -> T {
    q.get(key).and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Write one realtime security event (`user` already resolved). The shared sink
/// for realtime blocklist/regex hits, moderation denials, and inbound DLP hits.
async fn write_rt_event(
    s: &AppState,
    ak: &AkInfo,
    user: &str,
    rule: &str,
    action: &str,
    hits: i64,
) {
    gw_state::SecurityEvent {
        created_at_epoch_secs: gw_state::epoch_secs(),
        request_id: String::new(),
        ak: ak.ak.clone(),
        user_id: user.to_owned(),
        tenant: ak.tenant.clone(),
        surface: "realtime".to_owned(),
        rule: rule.to_owned(),
        action: action.to_owned(),
        hits,
    }
    .record(s.handler.state().store.as_ref())
    .await;
}

/// Record a turn's summed outbound DLP redactions as one event; no-op at zero.
async fn flush_rt_out_dlp(s: &AppState, ak: &AkInfo, hint: &str, count: i64) {
    if count > 0 {
        write_rt_event(s, ak, ak.attributed_user(hint), "dlp", "redact", count).await;
    }
}

/// The full inbound content policy for one realtime frame — the same chain
/// every REST surface runs (scan + hit events, moderation, DLP + event),
/// shared by both WebSocket paths. `Err(reason)` denies the frame; `Ok(n)` is
/// the DLP redaction count (n > 0 means the frame was rewritten).
async fn rt_inbound_policy(
    s: &AppState,
    ak: &AkInfo,
    hint: &str,
    frame: &mut Value,
) -> Result<usize, String> {
    let cfg = s.handler.cfg();
    let sec = cfg.security_for(&ak.tenant);
    let (scan, text) = gw_handler::plugins::realtime_frame_scan(sec, frame, sec.moderate);
    emit_rt_hits(s, ak, &scan.hits, hint).await;
    if let Some(block) = scan.block {
        return Err(block.message);
    }
    if let Some(reason) = realtime_moderate(s, sec, ak, hint, &text).await {
        return Err(reason);
    }
    let redacted = gw_handler::plugins::dlp_redact_realtime_frame(sec, frame);
    if redacted > 0 {
        write_rt_event(
            s,
            ak,
            ak.attributed_user(hint),
            "dlp",
            "redact",
            redacted as i64,
        )
        .await;
    }
    Ok(redacted)
}

/// Record a realtime frame's content-safety hits to the security-event stream
/// (parity with the REST surfaces).
async fn emit_rt_hits(
    s: &AppState,
    ak: &AkInfo,
    hits: &[gw_handler::plugins::RuleHit],
    hint: &str,
) {
    let user = ak.attributed_user(hint);
    for hit in hits {
        write_rt_event(s, ak, user, &hit.rule, hit.action.as_str(), hit.count).await;
    }
}

/// Moderate a realtime frame's inbound `text` (collected by the frame scan)
/// via the wired moderator — parity with the REST surface, so a moderated
/// tenant can't be bypassed over the WebSocket. `Some(reason)` denies the
/// frame; records a moderation event.
async fn realtime_moderate(
    s: &AppState,
    sec: &gw_config::SecurityConf,
    ak: &AkInfo,
    hint: &str,
    text: &str,
) -> Option<String> {
    if !sec.moderate || text.is_empty() {
        return None;
    }
    let reason = s.handler.moderate_text(sec, text).await?;
    write_rt_event(s, ak, ak.attributed_user(hint), "moderation", "block", 1).await;
    Some(reason)
}

/// The caller IP for the admin audit trail, resolved at request entry — before
/// any config mutation the handler performs — so the op that flips
/// `trust_proxy_headers` is audited under the policy in effect when it
/// arrived, not the one it just installed. Empty when the router is driven
/// without connect info (the test harness).
struct AuditSourceIp(String);

impl axum::extract::FromRequestParts<AppState> for AuditSourceIp {
    type Rejection = Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        s: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let peer = parts
            .extensions
            .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
            .map(|ci| ci.0);
        Ok(AuditSourceIp(source_ip(
            peer,
            &parts.headers,
            s.handler.cfg().trust_proxy_headers,
        )))
    }
}

/// The caller IP for the audit trail. Roots at the real TCP `peer`, which a
/// client cannot forge. Only when `trust_proxy` is set (a trusted proxy fronts
/// the gateway) does it read `x-real-ip`, then the RIGHTMOST `x-forwarded-for`
/// hop (the one that proxy appended) — never the leftmost, which a client forges.
fn source_ip(peer: Option<std::net::SocketAddr>, headers: &HeaderMap, trust_proxy: bool) -> String {
    if trust_proxy {
        if let Some(ip) = headers.get("x-real-ip").and_then(|v| v.to_str().ok()) {
            return ip.trim().to_owned();
        }
        if let Some(ip) = headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.rsplit(',').next())
        {
            return ip.trim().to_owned();
        }
    }
    peer.map(|p| p.ip().to_string()).unwrap_or_default()
}

/// Record one admin-plane mutation to the audit trail (who/what/when/where).
/// Best-effort: a store failure is logged, never fails the operation.
async fn audit_admin(
    s: &AppState,
    scope: &AdminScope,
    source: String,
    action: &str,
    target: &str,
    summary: String,
) {
    let (actor, scope_kind) = scope.audit_identity();
    let entry = gw_state::AdminAudit {
        created_at_epoch_secs: gw_state::epoch_secs(),
        actor: actor.to_owned(),
        scope: scope_kind.to_owned(),
        action: action.to_owned(),
        target: target.to_owned(),
        summary,
        source_ip: source,
    };
    if let Err(e) = s.handler.state().store.admin_audit_add(&entry).await {
        tracing::warn!(error = %e, action, "admin audit write failed");
    }
}

/// Admin gate: the global token is checked first (a colliding tenant token
/// grants global, never the reverse), then each tenant's token. 404 while no
/// admin token is configured, so probing can't tell the surface from a
/// nonexistent route.
#[allow(clippy::result_large_err)] // admin plane, not hot; boxing would noise every call site
fn admin_auth(s: &AppState, headers: &HeaderMap) -> Result<AdminScope, Response> {
    let cfg = s.handler.cfg();
    let global = cfg.admin.token();
    if global.is_none() && !cfg.tenants.iter().any(|t| t.admin_token().is_some()) {
        return Err(error_response(404, "not found"));
    }
    let presented = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let Some(presented) = presented else {
        return Err(error_response(401, "invalid admin token"));
    };
    if global.is_some_and(|g| ct_eq(&g, presented)) {
        return Ok(AdminScope::Global);
    }
    if let Some(t) = cfg
        .tenants
        .iter()
        .find(|t| t.admin_token().is_some_and(|tok| ct_eq(&tok, presented)))
    {
        return Ok(AdminScope::Tenant(t.name.clone()));
    }
    Err(error_response(401, "invalid admin token"))
}

/// Global-token gate for fleet-wide operations (reload, config publish).
#[allow(clippy::result_large_err)] // admin plane, not hot; boxing would noise every call site
fn require_global_admin(s: &AppState, headers: &HeaderMap) -> Result<(), Response> {
    match admin_auth(s, headers)? {
        AdminScope::Global => Ok(()),
        AdminScope::Tenant(_) => Err(error_response(403, "requires the global admin token")),
    }
}

/// Key lookup under an admin scope: another tenant's key answers 404 (not
/// 403), so a tenant admin can't probe which keys exist outside its scope.
async fn scoped_key(
    s: &AppState,
    scope: &AdminScope,
    ak: &str,
) -> Result<Option<AkInfo>, Response> {
    match s.handler.state().auth.authenticate(ak).await {
        Some(existing) if !scope.covers(&existing.tenant) => {
            Err(error_response(404, format!("key {ak} not found")))
        }
        found => Ok(found),
    }
}

/// The admin surfaces' public view of a key — one shape for PATCH and GET.
fn ak_public_json(k: &AkInfo) -> Value {
    json!({
        "ak": k.ak, "product": k.product, "tenant": k.tenant, "owner": k.owner,
        "qps": k.qps, "daily_token_quota": k.daily_token_quota,
        "tokens_per_minute": k.tokens_per_minute,
        "expires_at_epoch_secs": k.expires_at_epoch_secs,
        "banned": k.banned,
    })
}

/// A tenant-owned store lookup: another tenant's resource answers 404 (not
/// 403), so sequential ids can't be probed for cross-tenant existence.
#[allow(clippy::result_large_err)] // mirrors the surrounding admin/lookup helpers
fn tenant_owned<T>(
    found: GResult<Option<T>>,
    owner: impl Fn(&T) -> &str,
    tenant: &str,
    kind: &str,
    id: &str,
) -> Result<T, Response> {
    match found {
        Ok(Some(x)) if owner(&x) == tenant => Ok(x),
        Ok(_) => Err(error_response(404, format!("{kind} {id} not found"))),
        Err(e) => Err(gateway_error(e)),
    }
}

/// Constant-time string equality for bearer-token checks.
fn ct_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

/// POST /admin/reload — re-read config from source and swap it in atomically;
/// governance, store, health, and cache are preserved.
async fn admin_reload(
    State(s): State<AppState>,
    headers: HeaderMap,
    AuditSourceIp(source): AuditSourceIp,
) -> Response {
    if let Err(r) = require_global_admin(&s, &headers) {
        return r;
    }
    match s.reload().await {
        Ok(()) => {
            let cfg = s.handler.cfg();
            tracing::info!(
                access_keys = cfg.access_keys.len(),
                models = cfg.models.len(),
                accounts = cfg.accounts.len(),
                "config reloaded"
            );
            audit_admin(&s, &AdminScope::Global, source, "reload", "", String::new()).await;
            (
                StatusCode::OK,
                Json(json!({
                    "status": "reloaded",
                    "access_keys": cfg.access_keys.len(),
                    "models": cfg.models.len(),
                    "accounts": cfg.accounts.len(),
                })),
            )
                .into_response()
        }
        Err(e) => error_response(500, format!("reload failed: {e}")),
    }
}

/// POST /admin/keys — create (or replace) a runtime access key. Admin keys
/// survive a config reload; the config file remains the boot baseline.
async fn admin_key_create(
    State(s): State<AppState>,
    scope: AdminScope,
    AuditSourceIp(source): AuditSourceIp,
    Json(body): Json<Value>,
) -> Response {
    let (Some(ak), Some(product)) = (body["ak"].as_str(), body["product"].as_str()) else {
        return error_response(400, "ak and product are required");
    };
    let default_tenant = match &scope {
        AdminScope::Global => gw_config::DEFAULT_TENANT,
        AdminScope::Tenant(t) => t.as_str(),
    };
    let tenant = body["tenant"]
        .as_str()
        .filter(|t| !t.is_empty())
        .unwrap_or(default_tenant);
    if !scope.covers(tenant) {
        return error_response(403, "tenant admin may only create keys in its own tenant");
    }
    // a typo'd tenant would silently create an unrestricted key
    if !s.handler.cfg().is_known_tenant(tenant) {
        return error_response(400, format!("unknown tenant `{tenant}`"));
    }
    if let Err(r) = scoped_key(&s, &scope, ak).await {
        return r;
    }
    let info = AkInfo {
        ak: ak.to_owned(),
        product: product.to_owned(),
        tenant: tenant.to_owned(),
        owner: body["owner"].as_str().map(str::to_owned),
        qps: body["qps"].as_f64().unwrap_or(0.0),
        daily_token_quota: body["daily_token_quota"].as_i64().unwrap_or(0),
        tokens_per_minute: body["tokens_per_minute"].as_i64(),
        expires_at_epoch_secs: body["expires_at_epoch_secs"].as_i64(),
        banned: body["banned"].as_bool().unwrap_or(false),
        model_quotas: Arc::new(
            body["model_quotas"]
                .as_object()
                .map(|o| {
                    o.iter()
                        .filter_map(|(m, v)| Some((m.clone(), v.as_i64()?)))
                        .collect()
                })
                .unwrap_or_default(),
        ),
    };
    if let Err(e) = s
        .handler
        .state()
        .auth
        .put(info, gw_state::KeySource::Admin)
        .await
    {
        return gateway_error(e);
    }
    audit_admin(
        &s,
        &scope,
        source,
        "key_create",
        ak,
        format!("tenant={tenant}"),
    )
    .await;
    (
        StatusCode::CREATED,
        Json(json!({ "ak": ak, "status": "created" })),
    )
        .into_response()
}

/// PATCH /admin/keys/{ak} — only the fields present in the body change.
async fn admin_key_patch(
    State(s): State<AppState>,
    scope: AdminScope,
    AuditSourceIp(source): AuditSourceIp,
    Path(ak): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    if let Err(r) = scoped_key(&s, &scope, &ak).await {
        return r;
    }
    // absent = leave, null = clear, number = set; malformed (incl. u64 overflow) leaves unchanged
    let tri = |field: &str| match body.get(field) {
        Some(Value::Null) => Some(None),
        Some(v) if v.is_i64() || v.is_u64() => v.as_i64().map(Some),
        _ => None,
    };
    let patch = gw_state::KeyPatch {
        qps: body["qps"].as_f64(),
        daily_token_quota: body["daily_token_quota"].as_i64(),
        tokens_per_minute: tri("tokens_per_minute"),
        expires_at_epoch_secs: tri("expires_at_epoch_secs"),
        banned: body["banned"].as_bool(),
    };
    let patched = s.handler.state().auth.patch(&ak, &patch).await;
    match patched {
        Err(e) => gateway_error(e),
        Ok(Some(info)) => {
            audit_admin(&s, &scope, source, "key_patch", &ak, String::new()).await;
            (StatusCode::OK, Json(ak_public_json(&info))).into_response()
        }
        Ok(None) => error_response(404, format!("key {ak} not found")),
    }
}

/// DELETE /admin/keys/{ak} — revoke a key (config- or admin-sourced).
async fn admin_key_delete(
    State(s): State<AppState>,
    scope: AdminScope,
    AuditSourceIp(source): AuditSourceIp,
    Path(ak): Path<String>,
) -> Response {
    if let Err(r) = scoped_key(&s, &scope, &ak).await {
        return r;
    }
    match s.handler.state().auth.revoke(&ak).await {
        Err(e) => gateway_error(e),
        Ok(true) => {
            audit_admin(&s, &scope, source, "key_delete", &ak, String::new()).await;
            (
                StatusCode::OK,
                Json(json!({ "ak": ak, "status": "revoked" })),
            )
                .into_response()
        }
        Ok(false) => error_response(404, format!("key {ak} not found")),
    }
}

/// PUT /admin/config — validate, publish to the fleet config store, and reload
/// this instance; peers converge via the store's change feed. Global admin only.
async fn admin_config_put(
    State(s): State<AppState>,
    headers: HeaderMap,
    AuditSourceIp(source): AuditSourceIp,
    body: String,
) -> Response {
    if let Err(r) = require_global_admin(&s, &headers) {
        return r;
    }
    let Some(store) = &s.config_store else {
        return error_response(
            400,
            "config store not configured (set storage.postgres_url)",
        );
    };
    if let Err(e) = GatewayConfig::from_yaml(&body) {
        return error_response(400, format!("invalid config: {e}"));
    }
    let version = match store.publish(&body).await {
        Ok(v) => v,
        Err(e) => return gateway_error(e),
    };
    // audit before the local reload can fail — the published version already leads the fleet
    let reload = s.reload().await;
    let detail = match &reload {
        Ok(()) => String::new(),
        Err(e) => format!("local reload failed: {e}"),
    };
    audit_admin(
        &s,
        &AdminScope::Global,
        source,
        "config_publish",
        &version.to_string(),
        detail,
    )
    .await;
    match reload {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({ "status": "published", "version": version })),
        )
            .into_response(),
        Err(e) => error_response(
            500,
            format!("published v{version} but local reload failed: {e}"),
        ),
    }
}

/// GET /admin/keys?offset=&limit= — a page of the key table, scoped: a tenant
/// admin sees only its own keys. Paginated so a fleet key table never loads whole.
async fn admin_key_list(
    State(s): State<AppState>,
    scope: AdminScope,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    let offset = q_num(&q, "offset", 0);
    let limit = q_num(&q, "limit", KEY_PAGE_DEFAULT);
    // the scope filters in the store before paging, or a tenant admin's page could come back empty
    let tenant = scope.tenant_filter(&q);
    let listed = match s.handler.state().auth.list(tenant, offset, limit).await {
        Ok(v) => v,
        Err(e) => return gateway_error(e),
    };
    let keys: Vec<Value> = listed
        .into_iter()
        .filter(|k| scope.covers(&k.tenant))
        .map(|k| ak_public_json(&k))
        .collect();
    let mut resp = json!({ "count": keys.len(), "offset": offset });
    resp["keys"] = Value::Array(keys);
    Json(resp).into_response()
}

/// GET /admin/usage — ledger rollup by (tenant, requested model). A tenant
/// admin sees only its own tenant; the global admin may filter with ?tenant=.
async fn admin_usage(
    State(s): State<AppState>,
    scope: AdminScope,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    let filter = scope.tenant_filter(&q);
    let usage = match s.handler.state().store.ledger_usage(filter).await {
        Ok(rows) => rows,
        Err(e) => return gateway_error(e),
    };
    Json(json!({ "usage": usage })).into_response()
}

/// GET /admin/usage/users?user=&since=&until= — precise per-user cost over a
/// billing period, grouped by (user, requested model). Tenant-scoped like
/// [`admin_usage`]; `since`/`until` are unix seconds (default: all time).
async fn admin_usage_users(
    State(s): State<AppState>,
    scope: AdminScope,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    let tenant = scope.tenant_filter(&q);
    let since = q_num(&q, "since", 0);
    let until = q_num(&q, "until", i64::MAX);
    let usage = match s
        .handler
        .state()
        .store
        .usage_by_user(tenant, q.get("user").map(String::as_str), since, until)
        .await
    {
        Ok(rows) => rows,
        Err(e) => return gateway_error(e),
    };
    if q.get("format").map(String::as_str) == Some("csv") {
        let mut csv = String::from(
            "user_id,model,requests,prompt_tokens,completion_tokens,total_tokens,cost_micros,vendor_cost_micros\n",
        );
        for u in &usage {
            let _ = writeln!(
                csv,
                "{},{},{},{},{},{},{},{}",
                csv_field(&u.user_id),
                csv_field(&u.model),
                u.requests,
                u.prompt_tokens,
                u.completion_tokens,
                u.total_tokens,
                u.cost_micros,
                u.vendor_cost_micros,
            );
        }
        return ([("content-type", "text/csv")], csv).into_response();
    }
    Json(json!({ "usage": usage })).into_response()
}

/// A CSV field, RFC-4180 quoted AND neutralized against spreadsheet formula
/// injection: a field opening with a formula trigger (`= + - @` / tab / CR) is
/// prefixed with `'` so Excel/Sheets treat it as text (the value is
/// attacker-controlled — it can carry a user id).
fn csv_field(s: &str) -> String {
    let needs_prefix = s
        .chars()
        .next()
        .is_some_and(|c| matches!(c, '=' | '+' | '-' | '@' | '\t' | '\r'));
    let body = if needs_prefix {
        format!("'{s}")
    } else {
        s.to_owned()
    };
    if body.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", body.replace('"', "\"\""))
    } else {
        body
    }
}

/// GET /admin/audit/events?limit= — content-safety hits (no prompt text), newest
/// first. Tenant-scoped: a tenant admin sees only its own tenant's events.
async fn admin_security_events(
    State(s): State<AppState>,
    scope: AdminScope,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    let tenant = scope.tenant_filter(&q);
    let limit = q_num(&q, "limit", LEDGER_PAGE_DEFAULT);
    match s.handler.state().store.security_events(tenant, limit).await {
        Ok(events) => Json(json!({ "events": events })).into_response(),
        Err(e) => gateway_error(e),
    }
}

/// GET /admin/audit/ops?limit= — admin-operation audit trail, newest first.
/// Global admin only (the trail spans all tenants).
async fn admin_audit_ops(
    State(s): State<AppState>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    if let Err(r) = require_global_admin(&s, &headers) {
        return r;
    }
    let limit = q_num(&q, "limit", LEDGER_PAGE_DEFAULT);
    match s.handler.state().store.admin_audit_list(limit).await {
        Ok(entries) => Json(json!({ "entries": entries })).into_response(),
        Err(e) => gateway_error(e),
    }
}

/// GET /admin/audit/content/{request_id} — the retained prompt/response rows
/// for one request, unsealed when the content key is present (a sealed row
/// without it returns `content: null`). Tenant-scoped like the other reads.
async fn admin_content_get(
    State(s): State<AppState>,
    scope: AdminScope,
    Path(request_id): Path<String>,
) -> Response {
    let rows = match s.handler.state().store.content_for(&request_id).await {
        Ok(rows) => rows,
        Err(e) => return gateway_error(e),
    };
    let entries: Vec<Value> = rows
        .into_iter()
        .filter(|r| scope.covers(&r.tenant))
        .map(|r| {
            let content = if r.sealed {
                gw_state::content::open(&r.content)
                    .map(Value::String)
                    .unwrap_or(Value::Null)
            } else {
                Value::String(r.content)
            };
            json!({
                "created_at_epoch_secs": r.created_at_epoch_secs,
                "kind": r.kind,
                "ak": r.ak,
                "user_id": r.user_id,
                "tenant": r.tenant,
                "sealed": r.sealed,
                "expires_at_epoch_secs": r.expires_at_epoch_secs,
                "content": content,
            })
        })
        .collect();
    Json(json!({ "request_id": request_id, "entries": entries })).into_response()
}

/// DELETE /admin/audit/content?user= — erase every retained trace of one end
/// user's content (the GDPR/PIPL right-to-erasure hook): retained rows, batch
/// result messages, leftover terminal batch inputs. Tenant-scoped; the
/// `content_erase` audit entry commits with the deletion, so a recorded
/// success can't separate from it. Ledger rows and security events carry no
/// content and are kept.
async fn admin_content_erase(
    State(s): State<AppState>,
    scope: AdminScope,
    AuditSourceIp(source): AuditSourceIp,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    let Some(user) = q.get("user").filter(|u| !u.is_empty()) else {
        return error_response(400, "user is required");
    };
    let tenant = scope.tenant_filter(&q);
    let (actor, scope_kind) = scope.audit_identity();
    let audit = gw_state::AdminAudit {
        created_at_epoch_secs: gw_state::epoch_secs(),
        actor: actor.to_owned(),
        scope: scope_kind.to_owned(),
        action: "content_erase".to_owned(),
        target: user.clone(),
        summary: String::new(),
        source_ip: source,
    };
    match s
        .handler
        .state()
        .store
        .content_erase_user(tenant, user, audit)
        .await
    {
        Ok(deleted) => Json(json!({ "user": user, "deleted": deleted })).into_response(),
        Err(e) => gateway_error(e),
    }
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

/// Run the pipeline on its own task so a client disconnect can't cancel it
/// mid-billing: once admitted, quota/ledger accounting runs to completion.
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

/// The wire default when an engine reported no finish reason.
fn finish_or_stop(fr: &str) -> &str {
    if fr.is_empty() { "stop" } else { fr }
}

/// finish_reason mapping, anthropic → openai.
fn finish_openai(fr: &str) -> String {
    match fr {
        "" | "end_turn" | "stop_sequence" | "COMPLETE" | "complete" => "stop".to_owned(),
        "max_tokens" => "length".to_owned(),
        "tool_use" => "tool_calls".to_owned(),
        other => other.to_owned(),
    }
}

/// finish_reason mapping, openai → anthropic.
fn finish_anthropic(fr: &str) -> String {
    match fr {
        "" | "stop" => "end_turn".to_owned(),
        "length" => "max_tokens".to_owned(),
        "tool_calls" => "tool_use".to_owned(),
        other => other.to_owned(),
    }
}

/// The OpenAI wire usage. When the normalized parts are known they rebuild the
/// totals — OpenAI counts cached reads inside `prompt_tokens` and reasoning
/// inside `completion_tokens`, while an Anthropic engine reports cache tokens
/// OUTSIDE `input_tokens` — so the details always stay subsets of the totals.
fn openai_usage(pt: i64, ct: i64, tt: i64, u: Option<gw_models::CommonUsage>) -> Usage {
    let (pt, ct, tt) = u.map_or((pt, ct, tt), |d| {
        let (p, c) = (d.prompt_total(), d.completion_total());
        (p, c, p.saturating_add(c))
    });
    Usage {
        prompt_tokens: pt,
        completion_tokens: ct,
        total_tokens: tt,
        prompt_tokens_details: u.filter(|d| d.read_cache > 0).map(|d| {
            gw_protocol::openai::PromptTokensDetails {
                cached_tokens: d.read_cache,
            }
        }),
        completion_tokens_details: u.filter(|d| d.reason > 0).map(|d| {
            gw_protocol::openai::CompletionTokensDetails {
                reasoning_tokens: d.reason,
            }
        }),
    }
}

/// The Anthropic wire usage: cache tokens ride OUTSIDE `input_tokens`. When
/// the normalized parts are known they rebuild input/output — an OpenAI
/// engine's `prompt_tokens` already contains its cached reads, and passing it
/// through next to `cache_read_input_tokens` would double-count them.
fn anthropic_usage(pt: i64, ct: i64, u: Option<gw_models::CommonUsage>) -> AnthUsage {
    match u {
        Some(u) => AnthUsage {
            input_tokens: u.platform_input,
            output_tokens: u.completion_total(),
            cache_read_input_tokens: u.read_cache,
            cache_creation_input_tokens: u.write_cache,
        },
        None => AnthUsage {
            input_tokens: pt,
            output_tokens: ct,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
        },
    }
}

/// The Responses wire usage: like OpenAI, cached reads count inside
/// `input_tokens` and reasoning inside `output_tokens`, so the normalized
/// parts rebuild the totals and the details stay subsets.
fn responses_usage(pt: i64, ct: i64, tt: i64, u: Option<gw_models::CommonUsage>) -> Value {
    let Some(u) = u else {
        return json!({"input_tokens": pt, "output_tokens": ct, "total_tokens": tt});
    };
    let (p, c) = (u.prompt_total(), u.completion_total());
    let mut usage =
        json!({"input_tokens": p, "output_tokens": c, "total_tokens": p.saturating_add(c)});
    if u.read_cache > 0 {
        usage["input_tokens_details"] = json!({"cached_tokens": u.read_cache});
    }
    if u.reason > 0 {
        usage["output_tokens_details"] = json!({"reasoning_tokens": u.reason});
    }
    usage
}

/// POST /v1/chat/completions (OpenAI-compatible surface)
async fn chat_completions(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    Json(body): Json<ChatCompletionRequest>,
) -> Response {
    let started = Instant::now();
    if body.messages.is_empty() {
        return error_response(400, "messages must not be empty");
    }

    let messages: Vec<ChatMsg> = body
        .messages
        .into_iter()
        .map(|m| {
            let content = m.content_text();
            ChatMsg {
                role: m.role,
                content,
                parts: m.content.and_then(|c| match c {
                    gw_protocol::openai::MessageContent::Parts(p) => Some(Value::Array(p)),
                    _ => None,
                }),
                tool_calls: m.tool_calls.and_then(|tc| serde_json::to_value(tc).ok()),
                tool_call_id: m.tool_call_id,
            }
        })
        .collect();
    let typed = TypedParams::Chat(ChatParams {
        temperature: body.temperature,
        top_p: body.top_p,
        max_tokens: body.max_tokens,
        stop: body.stop,
        presence_penalty: body.presence_penalty,
        frequency_penalty: body.frequency_penalty,
        tools: body.tools.map(Value::Array),
        tool_choice: body.tool_choice,
        response_format: body.response_format,
        logprobs: body.logprobs,
        top_logprobs: body.top_logprobs,
        system: None,
    });
    let stream_model = body.stream.then(|| body.model.clone());
    let mut param = ModelParamV2::with_name(
        // placeholder type; the resolve_model DAG node maps model_name properly
        gw_consts::Protocol::OpenaiChat,
        body.model,
    );
    param.typed = Some(typed);
    param.raw = Value::Object(body.extra);
    let user_id = user_hint(&headers, &param.raw["user"]);

    let request = GatewayRequest {
        is_online: true,
        stream: body.stream,
        ak: ak.ak.clone(),
        message: messages,
        model_param_v2: Some(param),
        user_id,
        ..Default::default()
    };

    if let Some(model) = stream_model {
        return chat_stream_response(s, request, ak, model, started).into_response();
    }

    let ctx = match run_pipeline(&s, request, ak).await {
        Ok(ctx) => ctx,
        Err(e) => return gateway_error(e),
    };
    log_access("chat_completions", &ctx, started);
    let Some(mut outcome) = ctx.outcome else {
        return error_response(500, "pipeline produced no outcome");
    };

    let id = next_id("chatcmpl");
    let created = gw_state::epoch_secs();
    let usage = openai_usage(
        outcome.response.prompt_tokens,
        outcome.response.completion_tokens,
        outcome.response.total_tokens,
        outcome.response.common_usage,
    );
    let model_out = outcome.response.model;

    if let Some(tc) = outcome.response.tool_calls.take() {
        let calls: Vec<gw_protocol::openai::ToolCall> =
            serde_json::from_value(tc).unwrap_or_default();
        let resp = ChatCompletionResponse::tool_calls(id, created, model_out, calls, usage);
        return (StatusCode::OK, Json(resp)).into_response();
    }

    let resp = ChatCompletionResponse::text(
        id,
        created,
        model_out,
        outcome.response.message,
        finish_openai(&outcome.response.finish_reason),
        usage,
    );
    (StatusCode::OK, Json(resp)).into_response()
}

/// Run the pipeline on its own task, forwarding stream chunks through a bounded
/// channel (the backpressure seam); a final chunk carries the usage totals.
/// Outbound DLP forces buffering — a masked span may straddle deltas — so the
/// tail is then synthesized from the already-redacted final message instead of
/// the raw decoded deltas.
fn spawn_stream_pipeline(
    s: &AppState,
    mut request: GatewayRequest,
    ak: AkInfo,
    surface: &'static str,
    started: Instant,
) -> tokio::sync::mpsc::Receiver<gw_engines::StreamChunk> {
    let (tx, rx) = tokio::sync::mpsc::channel::<gw_engines::StreamChunk>(STREAM_CHANNEL_CAP);
    let dlp = s.handler.cfg().security_for(&ak.tenant).redacts_output();
    if !dlp {
        request.stream_tx = Some(tx.clone());
    }
    let handler = s.handler.clone();
    tokio::spawn(async move {
        match handler.run(request, ak).await {
            Ok(ctx) => {
                log_access(surface, &ctx, started);
                if let Some(outcome) = ctx.outcome {
                    let usage_totals = (
                        outcome.response.prompt_tokens,
                        outcome.response.completion_tokens,
                        outcome.response.total_tokens,
                    );
                    let common_usage = outcome.response.common_usage;
                    let mut tail = if dlp {
                        redacted_stream_tail(outcome)
                    } else if outcome.streamed_live {
                        Vec::new()
                    } else {
                        synth_chunks(outcome)
                    };
                    tail.push(gw_engines::StreamChunk {
                        usage_totals: Some(usage_totals),
                        common_usage,
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

/// A per-protocol SSE encode state driven by [`sse_stream`].
trait SseEncodeState: Send + 'static {
    fn queue(&mut self) -> &mut VecDeque<Event>;
    /// Apply one pipeline chunk (`None` = producer gone); `true` = the stream
    /// is over once `queue` drains.
    fn apply(&mut self, chunk: Option<gw_engines::StreamChunk>) -> bool;
}

/// The one queue-drain / recv / dispatch loop every streaming surface shares;
/// per-protocol event shaping stays in each [`SseEncodeState`].
fn sse_stream<S: SseEncodeState>(
    rx: tokio::sync::mpsc::Receiver<gw_engines::StreamChunk>,
    st: S,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>> + use<S>> {
    let stream =
        futures::stream::unfold((rx, st, false), |(mut rx, mut st, mut ended)| async move {
            loop {
                if let Some(ev) = st.queue().pop_front() {
                    return Some((Ok::<_, Infallible>(ev), (rx, st, ended)));
                }
                if ended {
                    return None;
                }
                ended = st.apply(rx.recv().await);
            }
        });
    Sse::new(stream)
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
        queue: VecDeque<Event>,
        id: String,
        created: i64,
        model: String,
        pending_finish: Option<String>,
    }
    impl SseEncodeState for St {
        fn queue(&mut self) -> &mut VecDeque<Event> {
            &mut self.queue
        }
        fn apply(&mut self, chunk: Option<gw_engines::StreamChunk>) -> bool {
            match chunk {
                Some(gw_engines::StreamChunk {
                    error: Some(msg), ..
                }) => {
                    self.queue.push_back(Event::default().data(
                        json!({"error": {"message": msg, "type": "gateway_error"}}).to_string(),
                    ));
                    self.queue.push_back(Event::default().data("[DONE]"));
                    true
                }
                Some(mut c) => {
                    if !c.delta.is_empty() {
                        let chunk = ChatCompletionChunk::content(
                            &self.id,
                            self.created,
                            &self.model,
                            std::mem::take(&mut c.delta),
                        );
                        if let Ok(payload) = serde_json::to_string(&chunk) {
                            self.queue.push_back(Event::default().data(payload));
                        }
                    }
                    if let Some(tc) = c.tool_calls.take() {
                        let calls = match tc {
                            Value::Array(a) => a,
                            _ => Vec::new(),
                        };
                        let chunk = ChatCompletionChunk::tool_calls(
                            &self.id,
                            self.created,
                            &self.model,
                            calls,
                        );
                        if let Ok(payload) = serde_json::to_string(&chunk) {
                            self.queue.push_back(Event::default().data(payload));
                        }
                    }
                    if let Some(fr) = c.finish_reason {
                        // held back until usage arrives so the final frame carries both
                        self.pending_finish = Some(fr);
                    }
                    let Some((pt, ct, tt)) = c.usage_totals else {
                        return false;
                    };
                    let usage = openai_usage(pt, ct, tt, c.common_usage);
                    let mut fin = ChatCompletionChunk::finish(
                        &self.id,
                        self.created,
                        &self.model,
                        Some(usage),
                    );
                    fin.choices[0].finish_reason = Some(
                        self.pending_finish
                            .take()
                            .unwrap_or_else(|| "stop".to_owned()),
                    );
                    if let Ok(payload) = serde_json::to_string(&fin) {
                        self.queue.push_back(Event::default().data(payload));
                    }
                    self.queue.push_back(Event::default().data("[DONE]"));
                    true
                }
                None => {
                    self.queue.push_back(Event::default().data("[DONE]"));
                    true
                }
            }
        }
    }
    sse_stream(
        rx,
        St {
            queue: VecDeque::new(),
            id: next_id("chatcmpl"),
            created: gw_state::epoch_secs(),
            model,
            pending_finish: None,
        },
    )
}

/// Chunks for an engine that returned a buffered response.
fn synth_chunks(outcome: gw_engines::EngineOutcome) -> Vec<gw_engines::StreamChunk> {
    let mut resp = outcome.response;
    let mut chunks = if outcome.chunks.is_empty() && !resp.message.is_empty() {
        vec![gw_engines::StreamChunk {
            delta: resp.message,
            ..Default::default()
        }]
    } else {
        outcome.chunks
    };
    if let Some(tc) = resp.tool_calls.take()
        && !chunks.iter().any(|c| c.tool_calls.is_some())
    {
        chunks.push(gw_engines::StreamChunk {
            tool_calls: Some(tc),
            ..Default::default()
        });
    }
    if !chunks.iter().any(|c| c.finish_reason.is_some()) {
        chunks.push(gw_engines::StreamChunk {
            finish_reason: Some(finish_or_stop(&resp.finish_reason).to_owned()),
            ..Default::default()
        });
    }
    chunks
}

/// The stream tail under outbound DLP: unlike [`synth_chunks`] it never replays
/// the raw pre-redaction deltas, so no unmasked text ever leaves.
fn redacted_stream_tail(outcome: gw_engines::EngineOutcome) -> Vec<gw_engines::StreamChunk> {
    let mut resp = outcome.response;
    let mut chunks = Vec::new();
    if !resp.message.is_empty() {
        chunks.push(gw_engines::StreamChunk {
            delta: resp.message,
            ..Default::default()
        });
    }
    if let Some(tc) = resp.tool_calls.take() {
        chunks.push(gw_engines::StreamChunk {
            tool_calls: Some(tc),
            ..Default::default()
        });
    }
    chunks.push(gw_engines::StreamChunk {
        finish_reason: Some(finish_or_stop(&resp.finish_reason).to_owned()),
        ..Default::default()
    });
    chunks
}

/// POST /v1/messages (Anthropic-compatible surface, stream + non-stream)
async fn messages(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<MessagesRequest>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers).await {
        Ok(ak) => ak,
        Err((st, msg)) => return anthropic_error(st, msg),
    };
    if body.messages.is_empty() {
        return anthropic_error(400, "messages must not be empty");
    }

    let system = body.system_text();
    let typed = TypedParams::Chat(ChatParams {
        temperature: body.temperature,
        top_p: body.top_p,
        max_tokens: body.max_tokens,
        stop: body
            .stop_sequences
            .and_then(|s| serde_json::to_value(s).ok()),
        tools: body.tools.map(Value::Array),
        tool_choice: body.tool_choice,
        system,
        ..Default::default()
    });
    let stream_model = body.stream.then(|| body.model.clone());
    let mut param = ModelParamV2::with_name(gw_consts::Protocol::AnthropicMessages, body.model);
    param.typed = Some(typed);
    param.raw = Value::Object(body.extra);
    let user_id = user_hint(&headers, &param.raw["metadata"]["user_id"]);

    let request = GatewayRequest {
        is_online: true,
        stream: body.stream,
        ak: ak.ak.clone(),
        message: body
            .messages
            .into_iter()
            .map(|m| {
                let text = m.text();
                let mut msg = ChatMsg::text(m.role, text);
                if m.content.is_array() {
                    msg.parts = Some(m.content);
                }
                msg
            })
            .collect(),
        model_param_v2: Some(param),
        user_id,
        ..Default::default()
    };

    if let Some(model) = stream_model {
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

    let usage = anthropic_usage(
        outcome.response.prompt_tokens,
        outcome.response.completion_tokens,
        outcome.response.common_usage,
    );
    let tool_use = anthropic_tool_blocks(outcome.response.tool_calls.as_ref());
    let mut content: Vec<gw_protocol::anthropic::ContentBlock> = Vec::new();
    if !outcome.response.message.is_empty() {
        content.push(gw_protocol::anthropic::ContentBlock::Text {
            text: outcome.response.message,
        });
    }
    for mut b in tool_use {
        content.push(gw_protocol::anthropic::ContentBlock::ToolUse {
            id: b["id"].as_str().unwrap_or_default().to_owned(),
            name: b["name"].as_str().unwrap_or_default().to_owned(),
            input: b["input"].take(),
        });
    }
    let resp = MessagesResponse::new(
        next_id("msg"),
        outcome.response.model,
        content,
        finish_anthropic(&outcome.response.finish_reason),
        usage,
    );
    (StatusCode::OK, Json(resp)).into_response()
}

/// tool_use blocks for an engine's tool_calls: native blocks pass through;
/// OpenAI-shaped calls convert via [`gw_protocol::anthropic::tool_calls_to_tool_use`].
fn anthropic_tool_blocks(tool_calls: Option<&Value>) -> Vec<Value> {
    let Some(Value::Array(blocks)) = tool_calls else {
        return Vec::new();
    };
    let native: Vec<Value> = blocks
        .iter()
        .filter(|b| b["type"] == "tool_use")
        .cloned()
        .collect();
    if !native.is_empty() {
        return native;
    }
    gw_protocol::anthropic::tool_calls_to_tool_use(blocks)
}

/// Streaming /v1/messages as the anthropic event sequence. message_start goes
/// out before usage is known (input_tokens 0); the final message_delta carries
/// the real counts, which SDKs accumulate from.
fn messages_stream_response(
    s: AppState,
    request: GatewayRequest,
    ak: AkInfo,
    model: String,
    started: Instant,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>> + use<>> {
    let rx = spawn_stream_pipeline(&s, request, ak, "messages", started);

    struct St {
        queue: VecDeque<Event>,
        id: String,
        model: String,
        started_msg: bool,
        text_idx: Option<usize>,
        next_idx: usize,
        /// OpenAI-shaped tool-call fragments, accumulated until the stream ends.
        tool_frags: Option<Value>,
        pending_finish: Option<String>,
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

        fn finish(
            &mut self,
            input_tokens: i64,
            output_tokens: i64,
            detail: Option<gw_models::CommonUsage>,
        ) {
            self.ensure_message_start();
            if let Some(frags) = self.tool_frags.take() {
                for block in anthropic_tool_blocks(Some(&frags)) {
                    self.emit_tool_block(&block);
                }
            }
            self.close_text();
            let stop = self
                .pending_finish
                .take()
                .unwrap_or_else(|| "end_turn".to_owned());
            let usage = anthropic_usage(input_tokens, output_tokens, detail);
            self.queue.push_back(Self::ev(
                "message_delta",
                json!({"type":"message_delta","delta":{"stop_reason":stop},"usage": usage}),
            ));
            self.queue
                .push_back(Self::ev("message_stop", json!({"type":"message_stop"})));
        }
    }

    impl SseEncodeState for St {
        fn queue(&mut self) -> &mut VecDeque<Event> {
            &mut self.queue
        }
        fn apply(&mut self, chunk: Option<gw_engines::StreamChunk>) -> bool {
            match chunk {
                Some(gw_engines::StreamChunk {
                    error: Some(msg), ..
                }) => {
                    self.queue.push_back(St::ev(
                        "error",
                        json!({"type":"error","error":{"type":"api_error","message":msg}}),
                    ));
                    true
                }
                Some(c) => {
                    if !c.delta.is_empty() {
                        self.ensure_message_start();
                        let idx = self.open_text();
                        self.queue.push_back(St::ev(
                            "content_block_delta",
                            json!({"type":"content_block_delta","index":idx,
                                   "delta":{"type":"text_delta","text":c.delta}}),
                        ));
                    }
                    if let Some(tc) = &c.tool_calls {
                        self.ensure_message_start();
                        let native = tc
                            .as_array()
                            .map(|a| a.iter().any(|b| b["type"] == "tool_use"))
                            .unwrap_or(false);
                        if native {
                            for block in anthropic_tool_blocks(Some(tc)) {
                                self.emit_tool_block(&block);
                            }
                        } else {
                            gw_engines::merge_tool_call_fragments(&mut self.tool_frags, tc);
                        }
                    }
                    if let Some(fr) = c.finish_reason {
                        self.pending_finish = Some(finish_anthropic(&fr));
                    }
                    if let Some((pt, ct, _)) = c.usage_totals {
                        self.finish(pt, ct, c.common_usage);
                        return true;
                    }
                    false
                }
                None => {
                    self.finish(0, 0, None);
                    true
                }
            }
        }
    }

    sse_stream(
        rx,
        St {
            queue: VecDeque::new(),
            id: next_id("msg"),
            model,
            started_msg: false,
            text_idx: None,
            next_idx: 0,
            tool_frags: None,
            pending_finish: None,
        },
    )
}

/// Run a non-chat family request through the pipeline. `mt` is only a
/// placeholder protocol — the resolve_model DAG node maps the real one.
async fn run_family(
    s: &AppState,
    ak: AkInfo,
    model: String,
    mt: gw_consts::Protocol,
    typed: TypedParams,
    messages: Vec<ChatMsg>,
    user_id: Option<String>,
) -> Result<DagContext, Response> {
    let mut param = ModelParamV2::with_name(mt, model);
    param.typed = Some(typed);
    let request = GatewayRequest {
        is_online: true,
        ak: ak.ak.clone(),
        message: messages,
        model_param_v2: Some(param),
        user_id,
        ..Default::default()
    };
    match run_pipeline(s, request, ak).await {
        Ok(ctx) => Ok(ctx),
        Err(e) => Err(gateway_error(e)),
    }
}

/// The engine's native payload, or a 500 naming the engine that returned none.
/// A pre-stage content block answers 400 with the block message — these
/// surfaces have no in-band content_filter shape, and falling through would
/// misreport the block as an engine failure.
fn response_v2_or_500(outcome: Option<gw_engines::EngineOutcome>, engine: &str) -> Response {
    match outcome {
        Some(o) if o.block.block => error_response(400, o.response.message),
        Some(o) => match o.response.response_v2 {
            Some(v) => (StatusCode::OK, Json(v)).into_response(),
            None => error_response(500, format!("{engine} engine returned no payload")),
        },
        None => error_response(500, format!("{engine} engine returned no payload")),
    }
}

/// POST /v1/completions (legacy text completions; non-stream). The prompt rides
/// as a single user message to CompletionsEngine.
async fn completions(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    Json(mut body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let model = body["model"].as_str().unwrap_or_default().to_owned();
    // prompt: string or [string] (OpenAI accepts both)
    let prompt = match body.get_mut("prompt").map(Value::take) {
        Some(Value::String(s)) => s,
        Some(Value::Array(a)) => a
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
    let ctx = match run_family(
        &s,
        ak,
        model,
        gw_consts::Protocol::Completions,
        typed,
        vec![ChatMsg::text("user", prompt)],
        user_hint(&headers, &body["user"]),
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(resp) => return resp,
    };
    log_access("completions", &ctx, started);
    let Some(outcome) = ctx.outcome else {
        return error_response(500, "pipeline produced no outcome");
    };
    let r = &outcome.response;
    let finish = finish_or_stop(&r.finish_reason);
    let resp = json!({
        "id": next_id("cmpl"),
        "object": "text_completion",
        "created": gw_state::epoch_secs(),
        "model": r.model,
        "choices": [{"text": r.message, "index": 0, "finish_reason": finish}],
        "usage": openai_usage(
            r.prompt_tokens,
            r.completion_tokens,
            r.total_tokens,
            r.common_usage
        ),
    });
    (StatusCode::OK, Json(resp)).into_response()
}

/// POST /v1/responses — native passthrough: the whole body rides as `raw`
/// through ResponsesEngine and its native response is returned as-is.
async fn responses(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let model = body["model"].as_str().unwrap_or_default().to_owned();
    if model.is_empty() {
        return error_response(400, "model is required");
    }
    if body["input"].is_null() {
        return error_response(400, "input is required");
    }
    let stream = body["stream"].as_bool().unwrap_or(false);
    let user_id = user_hint(&headers, &body["user"]);
    let stream_model = stream.then(|| model.clone());
    let mut param = ModelParamV2::with_name(gw_consts::Protocol::Responses, model);
    param.raw = body;
    let request = GatewayRequest {
        is_online: true,
        stream,
        ak: ak.ak.clone(),
        model_param_v2: Some(param),
        user_id,
        ..Default::default()
    };

    if let Some(model) = stream_model {
        return responses_stream_response(s, request, ak, model, started).into_response();
    }

    let ctx = match run_pipeline(&s, request, ak).await {
        Ok(ctx) => ctx,
        Err(e) => return gateway_error(e),
    };
    log_access("responses", &ctx, started);
    response_v2_or_500(ctx.outcome, "responses")
}

/// Streaming /v1/responses as the Responses SSE dialect; live for real vendors,
/// buffered-then-redacted when outbound DLP is on.
fn responses_stream_response(
    s: AppState,
    request: GatewayRequest,
    ak: AkInfo,
    model: String,
    started: Instant,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>> + use<>> {
    let rx = spawn_stream_pipeline(&s, request, ak, "responses", started);

    struct St {
        queue: VecDeque<Event>,
        model: String,
        created: bool,
        status: String,
    }
    impl St {
        fn ensure_created(&mut self) {
            if self.created {
                return;
            }
            self.created = true;
            self.queue.push_back(Event::default().data(
                json!({"type":"response.created","response":{"model":self.model,"status":"in_progress"}})
                    .to_string(),
            ));
        }
    }
    impl SseEncodeState for St {
        fn queue(&mut self) -> &mut VecDeque<Event> {
            &mut self.queue
        }
        fn apply(&mut self, chunk: Option<gw_engines::StreamChunk>) -> bool {
            match chunk {
                Some(gw_engines::StreamChunk {
                    error: Some(msg), ..
                }) => {
                    self.ensure_created();
                    self.queue.push_back(
                        Event::default().data(
                            json!({"type":"error","error":{"type":"gateway_error","message":msg}})
                                .to_string(),
                        ),
                    );
                    self.queue.push_back(Event::default().data("[DONE]"));
                    true
                }
                Some(c) => {
                    self.ensure_created();
                    if !c.delta.is_empty() {
                        self.queue.push_back(
                            Event::default().data(
                                json!({"type":"response.output_text.delta","delta":c.delta})
                                    .to_string(),
                            ),
                        );
                    }
                    if let Some(fr) = c.finish_reason {
                        self.status = fr;
                    }
                    let Some((pt, ct, tt)) = c.usage_totals else {
                        return false;
                    };
                    self.queue.push_back(
                        Event::default().data(
                            json!({"type":"response.completed","response":{
                                "model": self.model, "status": self.status,
                                "usage": responses_usage(pt, ct, tt, c.common_usage),
                            }})
                            .to_string(),
                        ),
                    );
                    self.queue.push_back(Event::default().data("[DONE]"));
                    true
                }
                None => {
                    self.ensure_created();
                    self.queue.push_back(Event::default().data("[DONE]"));
                    true
                }
            }
        }
    }
    sse_stream(
        rx,
        St {
            queue: VecDeque::new(),
            model,
            created: false,
            status: "completed".to_owned(),
        },
    )
}

/// POST /v1/embeddings (OpenAI-compatible surface)
async fn embeddings(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    Json(mut body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let model = body["model"].as_str().unwrap_or_default().to_owned();
    let input: Vec<String> = match body.get_mut("input").map(Value::take) {
        Some(Value::String(x)) => vec![x],
        Some(Value::Array(a)) => a
            .into_iter()
            .filter_map(|v| match v {
                Value::String(s) => Some(s),
                _ => None,
            })
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
    let ctx = match run_family(
        &s,
        ak,
        model,
        gw_consts::Protocol::OpenaiChat,
        typed,
        vec![],
        user_hint(&headers, &body["user"]),
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(resp) => return resp,
    };
    log_access("embeddings", &ctx, started);
    response_v2_or_500(ctx.outcome, "embeddings")
}

/// POST /v1/images/generations (OpenAI-compatible image generation surface)
async fn images_generations(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
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
    let ctx = match run_family(
        &s,
        ak,
        model,
        gw_consts::Protocol::OpenaiChat,
        typed,
        vec![],
        user_hint(&headers, &body["user"]),
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(resp) => return resp,
    };
    log_access("images", &ctx, started);
    response_v2_or_500(ctx.outcome, "image")
}

/// POST /v1/images/edits — same engine as generations; presence of `image`
/// routes to the edit endpoint; the image arrives as base64 JSON.
async fn images_edits(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
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
    let ctx = match run_family(
        &s,
        ak,
        model,
        gw_consts::Protocol::OpenaiChat,
        typed,
        vec![],
        user_hint(&headers, &body["user"]),
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(resp) => return resp,
    };
    log_access("images_edits", &ctx, started);
    response_v2_or_500(ctx.outcome, "image")
}

/// POST /v1/audio/speech (TTS, returns audio bytes; OpenAI-compatible surface)
async fn audio_speech(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
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
    let ctx = match run_family(
        &s,
        ak,
        model,
        gw_consts::Protocol::OpenaiChat,
        typed,
        vec![],
        user_hint(&headers, &body["user"]),
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(resp) => return resp,
    };
    log_access("audio_speech", &ctx, started);
    if let Some(o) = ctx.outcome.as_ref().filter(|o| o.block.block) {
        return error_response(400, o.response.message.clone());
    }
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

/// POST /v1/audio/transcriptions (STT; JSON carries b64 audio, not multipart).
async fn audio_transcriptions(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let model = body["model"].as_str().unwrap_or_default().to_owned();
    let audio = body["audio_b64"].as_str().unwrap_or_default().to_owned();
    if model.is_empty() || audio.is_empty() {
        return error_response(400, "model and audio_b64 are required");
    }
    let typed = TypedParams::AudioStt(SttParams {
        audio_b64: audio,
        language: body["language"].as_str().map(str::to_owned),
    });
    let ctx = match run_family(
        &s,
        ak,
        model,
        gw_consts::Protocol::OpenaiChat,
        typed,
        vec![],
        user_hint(&headers, &body["user"]),
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(resp) => return resp,
    };
    log_access("audio_transcriptions", &ctx, started);
    match ctx.outcome {
        Some(o) if o.block.block => error_response(400, o.response.message),
        Some(o) => (StatusCode::OK, Json(json!({ "text": o.response.message }))).into_response(),
        None => error_response(500, "stt engine returned no outcome"),
    }
}

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

/// POST /v1/batches (inline `items` or an uploaded JSONL `input_file_id`).
async fn batches_submit(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    Json(body): Json<Value>,
) -> Response {
    let mut model = body["model"].as_str().unwrap_or_default().to_owned();
    let mut batch_items = Vec::new();
    // batch-level attribution hint; a per-item body `user` overrides it
    let hint = user_header(&headers);
    let item_user = |v: &Value| {
        v["user"]
            .as_str()
            .or(hint.as_deref())
            .unwrap_or_default()
            .to_owned()
    };

    if let Some(file_id) = body["input_file_id"].as_str() {
        let found = s.handler.state().store.file_get(file_id).await;
        let file = match tenant_owned(found, |f| &f.tenant, &ak.tenant, "input file", file_id) {
            Ok(f) => f,
            Err(resp) => return resp,
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
            batch_items.push(BatchItem {
                messages: msgs,
                user: item_user(reqbody),
            });
        }
    } else if let Some(items) = body["items"].as_array() {
        for it in items {
            let msgs = parse_batch_messages(it);
            if msgs.is_empty() {
                return error_response(400, "each item needs a non-empty messages array");
            }
            batch_items.push(BatchItem {
                messages: msgs,
                user: item_user(it),
            });
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

/// POST /v1/files — a JSON `file` string field instead of multipart, matching
/// the audio/images surfaces.
async fn files_upload(
    State(s): State<AppState>,
    Authed(ak): Authed,
    Json(body): Json<Value>,
) -> Response {
    let purpose = body["purpose"].as_str().unwrap_or("batch").to_owned();
    let Some(content) = body["file"].as_str() else {
        return error_response(400, "file content (string) is required");
    };
    if content.is_empty() {
        return error_response(400, "file content must not be empty");
    }
    let f = match s
        .handler
        .state()
        .store
        .file_put(&ak.tenant, &purpose, content.to_owned())
        .await
    {
        Ok(f) => f,
        Err(e) => return gateway_error(e),
    };
    (
        StatusCode::OK,
        Json(json!({
            "id": f.id, "object": "file", "bytes": f.bytes,
            "purpose": f.purpose, "created_at": gw_state::epoch_secs(),
        })),
    )
        .into_response()
}

/// GET /v1/files/{id}; another tenant's file answers 404, not 403.
async fn files_get(
    State(s): State<AppState>,
    Authed(ak): Authed,
    Path(id): Path<String>,
) -> Response {
    let found = s.handler.state().store.file_get(&id).await;
    match tenant_owned(found, |f| &f.tenant, &ak.tenant, "file", &id) {
        Ok(f) => (
            StatusCode::OK,
            Json(json!({"id": f.id, "object": "file", "bytes": f.bytes, "purpose": f.purpose})),
        )
            .into_response(),
        Err(resp) => resp,
    }
}

/// DELETE /v1/files/{id} — remove an uploaded file (OpenAI-compatible). Files
/// are tenant-owned assets; erasing one end user's rows inside an uploaded
/// JSONL is the tenant's call — delete the file and re-upload if needed.
async fn files_delete(
    State(s): State<AppState>,
    Authed(ak): Authed,
    Path(id): Path<String>,
) -> Response {
    // one guarded delete — a check-then-delete pair would race a concurrent
    // delete + id reuse into removing another tenant's file
    match s.handler.state().store.file_delete(&id, &ak.tenant).await {
        Ok(true) => Json(json!({"id": id, "object": "file", "deleted": true})).into_response(),
        Ok(false) => error_response(404, format!("file {id} not found")),
        Err(e) => gateway_error(e),
    }
}

/// GET /v1/files/{id}/content (download raw content: batch output, etc).
async fn files_content(
    State(s): State<AppState>,
    Authed(ak): Authed,
    Path(id): Path<String>,
) -> Response {
    let found = s.handler.state().store.file_get(&id).await;
    match tenant_owned(found, |f| &f.tenant, &ak.tenant, "file", &id) {
        Ok(f) => (StatusCode::OK, f.content).into_response(),
        Err(resp) => resp,
    }
}

/// GET /v1/batches/{id}. A batch owned by another tenant answers 404.
async fn batches_get(
    State(s): State<AppState>,
    Authed(ak): Authed,
    Path(id): Path<String>,
) -> Response {
    let found = s.handler.state().store.batch_get(&id).await;
    match tenant_owned(found, |j| &j.tenant, &ak.tenant, "batch", &id) {
        Ok(job) => (StatusCode::OK, Json(json!(job))).into_response(),
        Err(resp) => resp,
    }
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
    async fn user_attribution_and_request_id_land_in_the_ledger() {
        let cfg = Arc::new(GatewayConfig::embedded_default().unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let app_state = AppState::new(cfg, state, Arc::new(gw_engines::MockTransport));
        let store = app_state.handler.state().store.clone();
        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("authorization", "Bearer ak-demo-123")
            .header("x-gw-user", "user-42")
            .body(Body::from(
                r#"{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}"#,
            ))
            .unwrap();
        let resp = app(app_state).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let (_, records) = store.ledger_snapshot(10).await.unwrap();
        let row = records.last().expect("a ledger row");
        assert_eq!(row.user_id, "user-42", "x-gw-user attributed the cost");
        assert!(!row.request_id.is_empty(), "request_id stamped");
        assert!(row.created_at_epoch_secs > 0, "created_at stamped");
        let by_user = store
            .usage_by_user(None, Some("user-42"), 0, i64::MAX)
            .await
            .unwrap();
        assert_eq!(by_user.len(), 1);
        assert!(by_user[0].total_tokens > 0);
    }

    #[test]
    fn source_ip_roots_at_peer_and_ignores_forgeable_headers_untrusted() {
        let peer: std::net::SocketAddr = "203.0.113.7:5000".parse().unwrap();
        let mut h = HeaderMap::new();
        h.insert("x-real-ip", "10.0.0.5".parse().unwrap());
        h.insert("x-forwarded-for", "1.2.3.4, 10.0.0.9".parse().unwrap());
        assert_eq!(
            source_ip(Some(peer), &h, false),
            "203.0.113.7",
            "untrusted: forgeable headers ignored, the TCP peer wins"
        );
        assert_eq!(
            source_ip(None, &h, false),
            "",
            "no peer, no forgeable header"
        );
        assert_eq!(
            source_ip(Some(peer), &h, true),
            "10.0.0.5",
            "trusted proxy: x-real-ip wins"
        );
        h.remove("x-real-ip");
        assert_eq!(source_ip(Some(peer), &h, true), "10.0.0.9", "rightmost hop");
    }

    #[test]
    fn csv_field_neutralizes_formula_injection() {
        assert_eq!(csv_field("alice"), "alice");
        assert_eq!(
            csv_field("+cmd"),
            "'+cmd",
            "formula trigger prefixed with '"
        );
        assert_eq!(
            csv_field("=SUM(A1,A2)"),
            "\"'=SUM(A1,A2)\"",
            "prefixed AND quoted (has a comma)"
        );
        assert_eq!(csv_field("a,b"), "\"a,b\"");
    }

    #[tokio::test]
    async fn dlp_hit_is_recorded_as_a_security_event() {
        let cfg = Arc::new(GatewayConfig::embedded_default().unwrap());
        assert!(cfg.security.dlp_redact, "embedded config has DLP on");
        let state = Arc::new(GatewayState::from_config(&cfg));
        let app_state = AppState::new(cfg, state, Arc::new(gw_engines::MockTransport));
        let store = app_state.handler.state().store.clone();
        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("authorization", "Bearer ak-demo-123")
            .body(Body::from(
                r#"{"model":"gpt-4o","messages":[{"role":"user","content":"mail me at a@b.com"}]}"#,
            ))
            .unwrap();
        let resp = app(app_state).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let events = store.security_events(None, 10).await.unwrap();
        assert!(
            events.iter().any(|e| e.rule == "dlp"),
            "an inbound PII redaction was recorded, no prompt text stored"
        );
    }

    #[tokio::test]
    async fn file_delete_is_tenant_scoped() {
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: gpt-4o, protocol: openai-chat}]\ntenants: [{name: t1}, {name: t2}]\naccess_keys: [{ak: k1, tenant: t1, product: p, qps: 10, daily_token_quota: 1000}, {ak: k2, tenant: t2, product: p, qps: 10, daily_token_quota: 1000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let app_state = AppState::new(cfg, state, Arc::new(gw_engines::MockTransport));
        let store = app_state.handler.state().store.clone();
        let f = store.file_put("t1", "batch", "line".into()).await.unwrap();
        let router = app(app_state);

        let del = |token: &'static str, id: String| {
            Request::builder()
                .method("DELETE")
                .uri(format!("/v1/files/{id}"))
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };
        let resp = router
            .clone()
            .oneshot(del("k2", f.id.clone()))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "cross-tenant delete answers 404 and removes nothing"
        );
        assert!(store.file_get(&f.id).await.unwrap().is_some());

        let resp = router
            .clone()
            .oneshot(del("k1", f.id.clone()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_json(resp).await["deleted"], true);
        assert!(store.file_get(&f.id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn content_erase_is_tenant_scoped_and_audited() {
        let yaml = "listen: {host: h, port: 1}\nadmin: {token_env: GW_TEST_ERASE_ADMIN}\nmodels: [{name: gpt-4o, protocol: openai-chat}]\ntenants: [{name: t1}, {name: t2, admin_token_env: GW_TEST_ERASE_T2}]\naccess_keys: [{ak: k1, tenant: t1, product: p, qps: 10, daily_token_quota: 1000}]";
        // SAFETY: unique var names for this test; no concurrent reader of them.
        unsafe {
            std::env::set_var("GW_TEST_ERASE_ADMIN", "root-tok");
            std::env::set_var("GW_TEST_ERASE_T2", "t2-tok");
        }
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let app_state = AppState::new(cfg, state, Arc::new(gw_engines::MockTransport));
        let store = app_state.handler.state().store.clone();
        let rec = |req: &str, tenant: &str| gw_state::ContentRecord {
            created_at_epoch_secs: 100,
            request_id: req.into(),
            ak: "k1".into(),
            user_id: "u1".into(),
            tenant: tenant.into(),
            kind: "prompt".into(),
            content: "hello".into(),
            sealed: false,
            expires_at_epoch_secs: 0,
        };
        store.content_add(&rec("r1", "t1")).await.unwrap();
        store.content_add(&rec("r2", "t2")).await.unwrap();
        let router = app(app_state);

        let erase = |token: &'static str| {
            Request::builder()
                .method("DELETE")
                .uri("/admin/audit/content?user=u1")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };
        let resp = router.clone().oneshot(erase("t2-tok")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let j = body_json(resp).await;
        assert_eq!(j["deleted"], 1, "tenant admin erases only its own tenant");
        assert_eq!(
            store.content_for("r1").await.unwrap().len(),
            1,
            "the other tenant's row is untouched"
        );

        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/admin/audit/content")
                    .header("authorization", "Bearer root-tok")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "user is required");

        let resp = router.clone().oneshot(erase("root-tok")).await.unwrap();
        assert_eq!(
            body_json(resp).await["deleted"],
            1,
            "global erase gets the rest"
        );

        let ops = router
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/admin/audit/ops")
                    .header("authorization", "Bearer root-tok")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let j = body_json(ops).await;
        let erases: Vec<_> = j["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["action"] == "content_erase" && e["target"] == "u1")
            .collect();
        assert_eq!(erases.len(), 2, "both erasures audited");
    }

    #[tokio::test]
    async fn full_retention_without_key_never_stores_raw_even_with_dlp_off() {
        let yaml = "listen: {host: h, port: 1}\nadmin: {token_env: GW_TEST_CONTENT_ADMIN}\nmodels: [{name: gpt-4o, protocol: openai-chat}]\naccounts: [{name: a1, provider: openai, protocols: ['openai-chat']}]\ntenants: [{name: t1, retention: {content: full, days: 1}, security: {dlp_redact: false, detect_secrets: false}}]\naccess_keys: [{ak: k1, tenant: t1, product: p, qps: 100, daily_token_quota: 100000}]";
        // SAFETY: unique var name for this test; no concurrent reader of it.
        unsafe { std::env::set_var("GW_TEST_CONTENT_ADMIN", "s3cret") };
        assert!(
            !gw_state::sealing_available(),
            "test env has no content key"
        );
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let app_state = AppState::new(cfg, state, Arc::new(gw_engines::MockTransport));
        let store = app_state.handler.state().store.clone();
        let router = app(app_state);
        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("authorization", "Bearer k1")
            .body(Body::from(
                r#"{"model":"gpt-4o","messages":[{"role":"user","content":"here is sk-abcdefghijklmnopqrstuvwxyz012345"}]}"#,
            ))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let (_, rows) = store.ledger_snapshot(1).await.unwrap();
        let stored = store.content_for(&rows[0].request_id).await.unwrap();
        let prompt = stored
            .iter()
            .find(|c| c.kind == "prompt")
            .expect("prompt stored");
        assert!(!prompt.sealed, "no key → unsealed");
        assert!(
            prompt.content.contains("[REDACTED_SECRET]"),
            "secret masked: {}",
            prompt.content
        );
        for c in &stored {
            assert!(
                !c.content.contains("sk-abc"),
                "raw secret never persisted: {}",
                c.content
            );
        }

        let read = router
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/admin/audit/content/{}", rows[0].request_id))
                    .header("authorization", "Bearer s3cret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(read.status(), StatusCode::OK);
        let j = body_json(read).await;
        let entries = j["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2, "prompt and response rows read back");
        let prompt_entry = entries
            .iter()
            .find(|e| e["kind"] == "prompt")
            .expect("prompt entry");
        assert!(
            prompt_entry["content"]
                .as_str()
                .unwrap()
                .contains("[REDACTED_SECRET]"),
            "read-back returns the redacted text"
        );
    }

    #[derive(Debug)]
    struct DenyModerator;

    #[async_trait::async_trait]
    impl gw_handler::moderation::Moderator for DenyModerator {
        async fn review(&self, _text: &str) -> Result<gw_handler::moderation::Verdict, String> {
            Ok(gw_handler::moderation::Verdict::Deny(
                "blocked by moderator".into(),
            ))
        }
    }

    #[tokio::test]
    async fn realtime_moderates_and_records_inbound_dlp() {
        let yaml = "listen: {host: h, port: 1}\nsecurity: {moderate: true, detect_secrets: true}\nmodels: [{name: rt, protocol: realtime}]\naccess_keys: [{ak: k1, product: p, qps: 10, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let handler = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        )
        .with_moderator(Arc::new(DenyModerator));
        let offline = OfflineHandler::new(handler.clone());
        let app = AppState {
            handler,
            offline,
            loader: None,
            config_store: None,
        };
        let ak = app.handler.state().auth.authenticate("k1").await.unwrap();
        let cfg = app.handler.cfg();
        let sec = cfg.security_for(&ak.tenant);

        assert_eq!(
            realtime_moderate(&app, sec, &ak, "", "hello there")
                .await
                .as_deref(),
            Some("blocked by moderator")
        );
        let mut secret = json!({"type":"input_text","text":"sk-abcdefghijklmnopqrstuvwxyz012345"});
        let n = gw_handler::plugins::dlp_redact_realtime_frame(sec, &mut secret);
        assert!(n > 0);
        write_rt_event(&app, &ak, ak.attributed_user(""), "dlp", "redact", n as i64).await;

        let events = app
            .handler
            .state()
            .store
            .security_events(None, 10)
            .await
            .unwrap();
        assert!(
            events.iter().any(|e| e.rule == "moderation"),
            "moderation event"
        );
        assert!(
            events.iter().any(|e| e.rule == "dlp"),
            "inbound realtime DLP event"
        );
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

    #[tokio::test]
    async fn realtime_gate_reserves_settles_and_refunds() {
        let cfg = Arc::new(GatewayConfig::embedded_default().unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let s = AppState::new(cfg, state, Arc::new(gw_engines::MockTransport));
        let ak = s
            .handler
            .state()
            .auth
            .authenticate("ak-demo-123")
            .await
            .unwrap();
        let gov = || s.handler.state().governance.clone();
        let used = || async { gov().quota_used(&ak.ak).await };

        let a1 = realtime_gate(&s, &ak, "gpt-4o", "").await.expect("admit");
        assert_eq!(used().await, REALTIME_TURN_RESERVE, "reserved up front");

        bill_realtime_turn(&a1, "gpt-4o", gw_consts::Protocol::Realtime, "acc", 30, 70).await;
        assert_eq!(used().await, 100, "settled to actual (30 + 70)");

        let a2 = realtime_gate(&s, &ak, "gpt-4o", "").await.expect("admit");
        assert_eq!(used().await, 100 + REALTIME_TURN_RESERVE);
        gov().quota_settle(&a2.ak.ak, -a2.reserved, a2.at).await;
        assert_eq!(used().await, 100, "dropped turn refunded whole");

        let a3 = realtime_gate(&s, &ak, "gpt-4o", "").await.expect("admit");
        assert_eq!(used().await, 100 + REALTIME_TURN_RESERVE);
        let ledger_before = s.handler.state().store.ledger_snapshot(1).await.unwrap().0;
        bill_realtime_turn(&a3, "gpt-4o", gw_consts::Protocol::Realtime, "acc", 0, 0).await;
        assert_eq!(used().await, 100, "zero-usage turn refunds its reserve");
        let ledger_after = s.handler.state().store.ledger_snapshot(1).await.unwrap().0;
        assert_eq!(
            ledger_before, ledger_after,
            "zero-usage turn writes no ledger row"
        );
    }

    #[tokio::test]
    async fn realtime_gate_reserves_tpm_and_rolls_back_on_denial() {
        let cfg = Arc::new(GatewayConfig::embedded_default().unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let s = AppState::new(cfg, state, Arc::new(gw_engines::MockTransport));
        let ak = s
            .handler
            .state()
            .auth
            .authenticate("ak-tpm-tiny")
            .await
            .unwrap();
        let gov = s.handler.state().governance.clone();

        let a1 = realtime_gate(&s, &ak, "gpt-4o", "")
            .await
            .expect("first admits");
        assert_eq!(a1.tpm_reserved, Some(REALTIME_TURN_RESERVE));
        let daily_before = gov.quota_used(&ak.ak).await;

        assert!(
            realtime_gate(&s, &ak, "gpt-4o", "").await.is_err(),
            "second turn denied by the TPM reserve"
        );
        assert_eq!(
            gov.quota_used(&ak.ak).await,
            daily_before,
            "a TPM-denied turn rolls back its daily reserve"
        );
    }

    #[tokio::test]
    async fn realtime_settles_on_the_admission_snapshot() {
        let price = |per_1k: i64| {
            format!(
                "listen: {{host: h, port: 1}}\nmodels: [{{name: rt, protocol: realtime, input_price_per_1k_micros: {per_1k}, output_price_per_1k_micros: {per_1k}}}]\naccess_keys: [{{ak: k-rt, product: p, qps: 10, daily_token_quota: 100000}}]"
            )
        };
        let cfg = Arc::new(GatewayConfig::from_yaml(&price(1_000_000)).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let s = AppState::new(cfg, state, Arc::new(gw_engines::MockTransport));
        let ak = s.handler.state().auth.authenticate("k-rt").await.unwrap();

        let admit = realtime_gate(&s, &ak, "rt", "").await.expect("admit");
        s.handler
            .reload(GatewayConfig::from_yaml(&price(2_000_000)).unwrap())
            .await
            .unwrap();
        bill_realtime_turn(&admit, "rt", gw_consts::Protocol::Realtime, "acc", 100, 100).await;

        let (_, records) = s.handler.state().store.ledger_snapshot(1).await.unwrap();
        assert_eq!(
            records[0].cost_micros, 200_000,
            "settled at the admission price, not the reloaded one"
        );
    }

    #[tokio::test]
    async fn realtime_attributes_user_from_owner_then_header_hint() {
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: rt, protocol: realtime, input_price_per_1k_micros: 1000, output_price_per_1k_micros: 1000}]\naccess_keys: [{ak: k-shared, product: p, qps: 10, daily_token_quota: 100000}, {ak: k-owned, product: p, qps: 10, daily_token_quota: 100000, owner: bob}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let s = AppState::new(cfg, state, Arc::new(gw_engines::MockTransport));

        let shared = s
            .handler
            .state()
            .auth
            .authenticate("k-shared")
            .await
            .unwrap();
        let admit = realtime_gate(&s, &shared, "rt", "alice")
            .await
            .expect("admit");
        assert_eq!(admit.user, "alice", "ownerless key attributes to the hint");
        bill_realtime_turn(&admit, "rt", gw_consts::Protocol::Realtime, "acc", 40, 60).await;

        let owned = s
            .handler
            .state()
            .auth
            .authenticate("k-owned")
            .await
            .unwrap();
        let admit = realtime_gate(&s, &owned, "rt", "mallory")
            .await
            .expect("admit");
        assert_eq!(
            admit.user, "bob",
            "owner is authoritative over a spoofed hint"
        );
        bill_realtime_turn(&admit, "rt", gw_consts::Protocol::Realtime, "acc", 10, 20).await;

        let (_, records) = s.handler.state().store.ledger_snapshot(2).await.unwrap();
        let users: std::collections::HashSet<&str> =
            records.iter().map(|r| r.user_id.as_str()).collect();
        assert!(
            users.contains("alice"),
            "shared-key turn billed to header hint"
        );
        assert!(users.contains("bob"), "owned-key turn billed to owner");
        assert!(
            !users.contains("mallory"),
            "spoofed hint never overrides owner"
        );
    }

    #[test]
    fn finish_reason_mapping_both_directions() {
        assert_eq!(finish_openai("end_turn"), "stop");
        assert_eq!(finish_openai("stop_sequence"), "stop");
        assert_eq!(finish_openai(""), "stop");
        assert_eq!(finish_openai("max_tokens"), "length");
        assert_eq!(finish_openai("tool_use"), "tool_calls");
        assert_eq!(finish_openai("refusal"), "refusal");

        assert_eq!(finish_anthropic("stop"), "end_turn");
        assert_eq!(finish_anthropic(""), "end_turn");
        assert_eq!(finish_anthropic("length"), "max_tokens");
        assert_eq!(finish_anthropic("tool_calls"), "tool_use");
        assert_eq!(finish_anthropic("content_filter"), "content_filter");

        for (o, a) in [
            ("stop", "end_turn"),
            ("length", "max_tokens"),
            ("tool_calls", "tool_use"),
        ] {
            assert_eq!(finish_anthropic(o), a, "openai→anthropic {o}");
            assert_eq!(finish_openai(a), o, "anthropic→openai {a}");
        }
    }

    #[test]
    fn openai_usage_counts_anthropic_cache_inside_prompt() {
        let u = gw_models::CommonUsage {
            platform_input: 8,
            read_cache: 2,
            write_cache: 1,
            completion: 5,
            reason: 0,
        };
        let w = openai_usage(999, 999, 999, Some(u));
        assert_eq!(
            w.prompt_tokens, 11,
            "cache reads/writes belong inside OpenAI prompt_tokens"
        );
        assert_eq!(w.total_tokens, 16);
        assert_eq!(w.prompt_tokens_details.unwrap().cached_tokens, 2);

        let w = openai_usage(8, 5, 13, None);
        assert_eq!((w.prompt_tokens, w.total_tokens), (8, 13));
        assert!(w.prompt_tokens_details.is_none());
    }

    #[test]
    fn anthropic_usage_excludes_cache_from_input() {
        let u = gw_models::CommonUsage {
            platform_input: 6,
            read_cache: 4,
            write_cache: 0,
            completion: 3,
            reason: 2,
        };
        let w = anthropic_usage(999, 999, Some(u));
        assert_eq!(
            w.input_tokens, 6,
            "OpenAI cached reads must not double-count into input_tokens"
        );
        assert_eq!(w.output_tokens, 5);
        assert_eq!(w.cache_read_input_tokens, 4);

        let w = anthropic_usage(10, 5, None);
        assert_eq!((w.input_tokens, w.cache_read_input_tokens), (10, 0));
    }

    #[test]
    fn responses_usage_rebuilds_from_common_usage() {
        let u = gw_models::CommonUsage {
            platform_input: 8,
            read_cache: 2,
            write_cache: 1,
            completion: 5,
            reason: 2,
        };
        let w = responses_usage(999, 999, 999, Some(u));
        assert_eq!(
            (w["input_tokens"].as_i64(), w["output_tokens"].as_i64()),
            (Some(11), Some(7)),
            "totals rebuilt from the normalized parts, not the raw args"
        );
        assert_eq!(w["total_tokens"], 18);
        assert_eq!(w["input_tokens_details"]["cached_tokens"], 2);
        assert_eq!(w["output_tokens_details"]["reasoning_tokens"], 2);

        let w = responses_usage(9, 4, 13, None);
        assert_eq!(w["input_tokens"], 9);
        assert_eq!(w["total_tokens"], 13);
        assert!(w.get("input_tokens_details").is_none());
    }
}
