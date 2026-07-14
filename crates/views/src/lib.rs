//! HTTP view layer (L5): parse/validate, authenticate the AK, build a
//! `GatewayRequest`, call the handler, shape the wire response, and emit one
//! structured access-log line per request.

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
        .route("/v1/files/{id}", get(files_get))
        .route("/v1/files/{id}/content", get(files_content))
        .route("/v1/realtime", get(realtime_ws))
        .route("/internal/ledger", get(ledger))
        .route("/internal/accounts", get(accounts))
        .route("/admin/reload", post(admin_reload))
        .route("/admin/config", axum::routing::put(admin_config_put))
        .route("/admin/keys", post(admin_key_create).get(admin_key_list))
        .route("/admin/usage", get(admin_usage))
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
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
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
    if account.endpoint.is_empty() {
        ws.on_upgrade(move |socket| realtime_session(socket, s, ak, model, mt, account.name))
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
        ws.on_upgrade(move |socket| realtime_bridge(socket, s, ak, model, mt, account))
    }
}

/// A turn admitted by [`realtime_gate`]: the freshly re-authenticated key,
/// the reserves taken, the admission day (the paired settle/refund lands on
/// the same bucket), and the admission snapshot (settlement must not drift
/// from the admission config when a reload lands mid-turn).
struct RealtimeAdmit {
    ak: AkInfo,
    reserved: i64,
    /// Tokens reserved in the AK TPM window; `None` when the key has no TPM cap.
    tpm_reserved: Option<i64>,
    at: i64,
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
async fn realtime_gate(s: &AppState, ak: &AkInfo, model: &str) -> Result<RealtimeAdmit, String> {
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
    Ok(RealtimeAdmit {
        ak,
        reserved: REALTIME_TURN_RESERVE,
        tpm_reserved,
        at,
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
                requested_model: model,
                served_model: model,
                protocol: mt.as_str(),
                account,
                prompt: it,
                completion: ot,
                total,
                ptu_spillover: false,
            },
            reserved: admit.reserved,
            tpm_reserved: admit.tpm_reserved,
            reserved_at: admit.at,
            model_quota_key,
        },
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
        // same blocklist + inbound DLP every REST surface runs
        let sec = &s.handler.cfg().security;
        if let Some(block) = gw_handler::plugins::realtime_frame_blocked(sec, &mut ev) {
            let _ = socket
                .send(send(json!({"type":"error","message": block.message})))
                .await;
            continue;
        }
        gw_handler::plugins::dlp_redact_realtime_frame(sec, &mut ev);
        match ev["type"].as_str().unwrap_or_default() {
            "input_text" => {
                let admit = match realtime_gate(&s, &ak, &model).await {
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
    account: gw_models::Account,
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
    // admitted turns awaiting settle, FIFO; refunded on exit so reserves never leak
    let mut pending: std::collections::VecDeque<RealtimeAdmit> = std::collections::VecDeque::new();
    // denied server-VAD turn: swallow its upstream frames until its terminal frame
    let mut suppress = false;
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
                    // same blocklist + inbound DLP every REST surface runs
                    let sec = &s.handler.cfg().security;
                    if let Some(block) = gw_handler::plugins::realtime_frame_blocked(sec, &mut frame) {
                        if cl_tx
                            .send(CMsg::Text(send_err(block.message).to_string().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        continue;
                    }
                    if gw_handler::plugins::dlp_redact_realtime_frame(sec, &mut frame) > 0 {
                        forward = UMsg::text(frame.to_string());
                    }
                    // gate each generation trigger, not every control frame
                    if is_response_create(&frame) {
                        match realtime_gate(&s, &ak, &model).await {
                            Ok(admit) => {
                                pending.push_back(admit);
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
                        else if realtime_turn_started(&account.provider, &v) && pending.is_empty() {
                            match realtime_gate(&s, &ak, &model).await {
                                Ok(admit) => pending.push_back(admit),
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
                            // turn boundary — settle the matching admitted turn (FIFO);
                            // a boundary with no gated turn bills unreserved
                            match pending.pop_front() {
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
                                    let unreserved = RealtimeAdmit {
                                        ak: billed,
                                        reserved: 0,
                                        tpm_reserved: None,
                                        at: gw_state::epoch_secs(),
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
                        }
                        // outbound DLP, per frame (a span straddling deltas is
                        // beyond a relay that cannot buffer)
                        if relay
                            && gw_handler::plugins::dlp_redact_realtime_frame(
                                &s.handler.cfg().security,
                                &mut v,
                            ) > 0
                        {
                            redacted = Some(v.to_string());
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
    for a in pending {
        a.refund().await;
    }
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

/// Configured public models, filtered to the caller's tenant entitlement.
async fn list_models(State(s): State<AppState>, headers: HeaderMap) -> Response {
    let ak = match authenticate(&s, &headers).await {
        Ok(ak) => ak,
        Err((st, msg)) => return error_response(st, msg),
    };
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
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let limit = q
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(LEDGER_PAGE_DEFAULT);
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
            "tier": if a.tier.is_empty() { "paygo" } else { a.tier.as_str() },
            "health": health.status(&a.name).await,
            "protocols": a.protocols,
        }));
    }
    let mut resp = json!({ "count": data.len() });
    resp["accounts"] = Value::Array(data);
    Json(resp)
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
        "ak": k.ak, "product": k.product, "tenant": k.tenant,
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
async fn admin_reload(State(s): State<AppState>, headers: HeaderMap) -> Response {
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
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let scope = match admin_auth(&s, &headers) {
        Ok(scope) => scope,
        Err(r) => return r,
    };
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
        qps: body["qps"].as_f64().unwrap_or(0.0),
        daily_token_quota: body["daily_token_quota"].as_i64().unwrap_or(0),
        tokens_per_minute: body["tokens_per_minute"].as_i64(),
        expires_at_epoch_secs: body["expires_at_epoch_secs"].as_i64(),
        banned: body["banned"].as_bool().unwrap_or(false),
        model_quotas: std::sync::Arc::new(
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
    (
        StatusCode::CREATED,
        Json(json!({ "ak": ak, "status": "created" })),
    )
        .into_response()
}

/// PATCH /admin/keys/{ak} — only the fields present in the body change.
async fn admin_key_patch(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path(ak): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    let scope = match admin_auth(&s, &headers) {
        Ok(scope) => scope,
        Err(r) => return r,
    };
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
        Ok(Some(info)) => (StatusCode::OK, Json(ak_public_json(&info))).into_response(),
        Ok(None) => error_response(404, format!("key {ak} not found")),
    }
}

/// DELETE /admin/keys/{ak} — revoke a key (config- or admin-sourced).
async fn admin_key_delete(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path(ak): Path<String>,
) -> Response {
    let scope = match admin_auth(&s, &headers) {
        Ok(scope) => scope,
        Err(r) => return r,
    };
    if let Err(r) = scoped_key(&s, &scope, &ak).await {
        return r;
    }
    match s.handler.state().auth.revoke(&ak).await {
        Err(e) => gateway_error(e),
        Ok(true) => (
            StatusCode::OK,
            Json(json!({ "ak": ak, "status": "revoked" })),
        )
            .into_response(),
        Ok(false) => error_response(404, format!("key {ak} not found")),
    }
}

/// PUT /admin/config — validate, publish to the fleet config store, and reload
/// this instance; peers converge via the store's change feed. Global admin only.
async fn admin_config_put(State(s): State<AppState>, headers: HeaderMap, body: String) -> Response {
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
    match s.reload().await {
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

/// GET /admin/keys — the key table, scoped: a tenant admin sees only its own keys.
async fn admin_key_list(State(s): State<AppState>, headers: HeaderMap) -> Response {
    let scope = match admin_auth(&s, &headers) {
        Ok(scope) => scope,
        Err(r) => return r,
    };
    let listed = match s.handler.state().auth.list().await {
        Ok(v) => v,
        Err(e) => return gateway_error(e),
    };
    let keys: Vec<Value> = listed
        .into_iter()
        .filter(|k| scope.covers(&k.tenant))
        .map(|k| ak_public_json(&k))
        .collect();
    let mut resp = json!({ "count": keys.len() });
    resp["keys"] = Value::Array(keys);
    Json(resp).into_response()
}

/// GET /admin/usage — ledger rollup by (tenant, requested model). A tenant
/// admin sees only its own tenant; the global admin may filter with ?tenant=.
async fn admin_usage(
    State(s): State<AppState>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let scope = match admin_auth(&s, &headers) {
        Ok(scope) => scope,
        Err(r) => return r,
    };
    let filter = match &scope {
        AdminScope::Tenant(t) => Some(t.clone()),
        AdminScope::Global => q.get("tenant").cloned(),
    };
    let usage = match s
        .handler
        .state()
        .store
        .ledger_usage(filter.as_deref())
        .await
    {
        Ok(rows) => rows,
        Err(e) => return gateway_error(e),
    };
    Json(json!({ "usage": usage })).into_response()
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

/// POST /v1/chat/completions (OpenAI-compatible surface)
async fn chat_completions(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ChatCompletionRequest>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers).await {
        Ok(ak) => ak,
        Err((st, msg)) => return error_response(st, msg),
    };
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
    let mut param = ModelParamV2::with_name(
        // placeholder type; the resolve_model DAG node maps model_name properly
        gw_consts::Protocol::OpenaiChat,
        body.model.clone(),
    );
    param.typed = Some(typed);
    param.raw = Value::Object(body.extra);

    let request = GatewayRequest {
        is_online: true,
        stream: body.stream,
        ak: ak.ak.clone(),
        message: messages,
        model_param_v2: Some(param),
        ..Default::default()
    };

    if body.stream {
        return chat_stream_response(s, request, ak, body.model, started).into_response();
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
    let usage = Usage {
        prompt_tokens: outcome.response.prompt_tokens,
        completion_tokens: outcome.response.completion_tokens,
        total_tokens: outcome.response.total_tokens,
    };
    // the outcome is owned and served exactly once — move fields, don't clone
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
    let dlp = s.handler.cfg().security.dlp_redact;
    if !dlp {
        request.stream_tx = Some(tx.clone());
    }
    let handler = s.handler.clone();
    tokio::spawn(async move {
        match handler.run(request, ak).await {
            Ok(ctx) => {
                log_access(surface, &ctx, started);
                // the context is served exactly once — move the outcome, don't clone
                if let Some(outcome) = ctx.outcome {
                    let usage_totals = (
                        outcome.response.prompt_tokens,
                        outcome.response.completion_tokens,
                        outcome.response.total_tokens,
                    );
                    let mut tail = if dlp {
                        redacted_stream_tail(outcome)
                    } else if outcome.streamed_live {
                        Vec::new()
                    } else {
                        synth_chunks(outcome)
                    };
                    tail.push(gw_engines::StreamChunk {
                        usage_totals: Some(usage_totals),
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
    fn queue(&mut self) -> &mut std::collections::VecDeque<Event>;
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
        queue: std::collections::VecDeque<Event>,
        id: String,
        created: i64,
        model: String,
        pending_finish: Option<String>,
    }
    impl SseEncodeState for St {
        fn queue(&mut self) -> &mut std::collections::VecDeque<Event> {
            &mut self.queue
        }
        fn apply(&mut self, chunk: Option<gw_engines::StreamChunk>) -> bool {
            match chunk {
                Some(c) if c.error.is_some() => {
                    let msg = c.error.unwrap_or_default();
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
                    let usage = Usage {
                        prompt_tokens: pt,
                        completion_tokens: ct,
                        total_tokens: tt,
                    };
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
            queue: std::collections::VecDeque::new(),
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
    let mut param =
        ModelParamV2::with_name(gw_consts::Protocol::AnthropicMessages, body.model.clone());
    param.typed = Some(typed);
    param.raw = Value::Object(body.extra);

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
        ..Default::default()
    };

    if body.stream {
        return messages_stream_response(s, request, ak, body.model, started).into_response();
    }

    let ctx = match run_pipeline(&s, request, ak).await {
        Ok(ctx) => ctx,
        Err(e) => return anthropic_gateway_error(e),
    };
    log_access("messages", &ctx, started);
    let Some(outcome) = ctx.outcome else {
        return anthropic_error(500, "pipeline produced no outcome");
    };

    let tool_use = anthropic_tool_blocks(outcome.response.tool_calls.as_ref());
    let mut content: Vec<gw_protocol::anthropic::ContentBlock> = Vec::new();
    // the outcome is owned and served exactly once — move fields, don't clone
    if !outcome.response.message.is_empty() {
        content.push(gw_protocol::anthropic::ContentBlock::Text {
            text: outcome.response.message,
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
        outcome.response.model,
        content,
        finish_anthropic(&outcome.response.finish_reason),
        AnthUsage {
            input_tokens: outcome.response.prompt_tokens,
            output_tokens: outcome.response.completion_tokens,
        },
    );
    (StatusCode::OK, Json(resp)).into_response()
}

/// tool_use blocks for an engine's tool_calls: native blocks pass through;
/// OpenAI-shaped calls run through the dsl's openai→anthropic mapping.
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
    if !blocks.iter().any(|b| b.get("function").is_some()) {
        return Vec::new();
    }
    let envelope = json!({"choices": [{"message": {"tool_calls": blocks}}]});
    let converted = gw_protocol::dsl::transform(&envelope, gw_protocol::dsl::openai_to_anthropic());
    converted["content"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter(|b| b["type"] == "tool_use")
                .cloned()
                .collect()
        })
        .unwrap_or_default()
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
        queue: std::collections::VecDeque<Event>,
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

        fn finish(&mut self, input_tokens: i64, output_tokens: i64) {
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
            self.queue.push_back(Self::ev(
                "message_delta",
                json!({"type":"message_delta","delta":{"stop_reason":stop},
                       "usage":{"input_tokens":input_tokens,"output_tokens":output_tokens}}),
            ));
            self.queue
                .push_back(Self::ev("message_stop", json!({"type":"message_stop"})));
        }
    }

    impl SseEncodeState for St {
        fn queue(&mut self) -> &mut std::collections::VecDeque<Event> {
            &mut self.queue
        }
        fn apply(&mut self, chunk: Option<gw_engines::StreamChunk>) -> bool {
            match chunk {
                Some(c) if c.error.is_some() => {
                    let msg = c.error.unwrap_or_default();
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
                        self.finish(pt, ct);
                        return true;
                    }
                    false
                }
                None => {
                    self.finish(0, 0);
                    true
                }
            }
        }
    }

    sse_stream(
        rx,
        St {
            queue: std::collections::VecDeque::new(),
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
) -> Result<DagContext, Response> {
    let mut param = ModelParamV2::with_name(mt, model);
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

/// The engine's native payload, or a 500 naming the engine that returned none.
fn response_v2_or_500(outcome: Option<gw_engines::EngineOutcome>, engine: &str) -> Response {
    match outcome.and_then(|o| o.response.response_v2) {
        Some(v) => (StatusCode::OK, Json(v)).into_response(),
        None => error_response(500, format!("{engine} engine returned no payload")),
    }
}

/// POST /v1/completions (legacy text completions; non-stream). The prompt rides
/// as a single user message to CompletionsEngine.
async fn completions(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers).await {
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
    let ctx = match run_family(
        &s,
        ak,
        model,
        gw_consts::Protocol::Completions,
        typed,
        vec![ChatMsg::text("user", prompt)],
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
        "usage": {
            "prompt_tokens": r.prompt_tokens,
            "completion_tokens": r.completion_tokens,
            "total_tokens": r.total_tokens,
        },
    });
    (StatusCode::OK, Json(resp)).into_response()
}

/// POST /v1/responses — native passthrough: the whole body rides as `raw`
/// through ResponsesEngine and its native response is returned as-is.
async fn responses(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers).await {
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
    let mut param = ModelParamV2::with_name(gw_consts::Protocol::Responses, model.clone());
    param.raw = body;
    let request = GatewayRequest {
        is_online: true,
        stream,
        ak: ak.ak.clone(),
        model_param_v2: Some(param),
        ..Default::default()
    };

    if stream {
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
        queue: std::collections::VecDeque<Event>,
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
        fn queue(&mut self) -> &mut std::collections::VecDeque<Event> {
            &mut self.queue
        }
        fn apply(&mut self, chunk: Option<gw_engines::StreamChunk>) -> bool {
            match chunk {
                Some(c) if c.error.is_some() => {
                    self.ensure_created();
                    let msg = c.error.unwrap_or_default();
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
                                "usage":{"input_tokens":pt,"output_tokens":ct,"total_tokens":tt},
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
            queue: std::collections::VecDeque::new(),
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
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers).await {
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
    let ctx = match run_family(
        &s,
        ak,
        model,
        gw_consts::Protocol::OpenaiChat,
        typed,
        vec![],
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
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers).await {
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
    let ctx = match run_family(
        &s,
        ak,
        model,
        gw_consts::Protocol::OpenaiChat,
        typed,
        vec![],
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
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers).await {
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
    let ctx = match run_family(
        &s,
        ak,
        model,
        gw_consts::Protocol::OpenaiChat,
        typed,
        vec![],
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
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers).await {
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
    let ctx = match run_family(
        &s,
        ak,
        model,
        gw_consts::Protocol::OpenaiChat,
        typed,
        vec![],
    )
    .await
    {
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

/// POST /v1/audio/transcriptions (STT; JSON carries b64 audio, not multipart).
async fn audio_transcriptions(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let ak = match authenticate(&s, &headers).await {
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
    let ctx = match run_family(
        &s,
        ak,
        model,
        gw_consts::Protocol::OpenaiChat,
        typed,
        vec![],
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(resp) => return resp,
    };
    log_access("audio_transcriptions", &ctx, started);
    match ctx.outcome {
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
    Json(body): Json<Value>,
) -> Response {
    let ak = match authenticate(&s, &headers).await {
        Ok(ak) => ak,
        Err((st, msg)) => return error_response(st, msg),
    };
    let mut model = body["model"].as_str().unwrap_or_default().to_owned();
    let mut batch_items = Vec::new();

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

/// POST /v1/files — a JSON `file` string field instead of multipart, matching
/// the audio/images surfaces.
async fn files_upload(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let ak = match authenticate(&s, &headers).await {
        Ok(ak) => ak,
        Err((st, msg)) => return error_response(st, msg),
    };
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
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let ak = match authenticate(&s, &headers).await {
        Ok(ak) => ak,
        Err((st, msg)) => return error_response(st, msg),
    };
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

/// GET /v1/files/{id}/content (download raw content: batch output, etc).
async fn files_content(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let ak = match authenticate(&s, &headers).await {
        Ok(ak) => ak,
        Err((st, msg)) => return error_response(st, msg),
    };
    let found = s.handler.state().store.file_get(&id).await;
    match tenant_owned(found, |f| &f.tenant, &ak.tenant, "file", &id) {
        Ok(f) => (StatusCode::OK, f.content).into_response(),
        Err(resp) => resp,
    }
}

/// GET /v1/batches/{id}. A batch owned by another tenant answers 404.
async fn batches_get(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let ak = match authenticate(&s, &headers).await {
        Ok(ak) => ak,
        Err((st, msg)) => return error_response(st, msg),
    };
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

        let a1 = realtime_gate(&s, &ak, "gpt-4o").await.expect("admit");
        assert_eq!(used().await, REALTIME_TURN_RESERVE, "reserved up front");

        bill_realtime_turn(&a1, "gpt-4o", gw_consts::Protocol::Realtime, "acc", 30, 70).await;
        assert_eq!(used().await, 100, "settled to actual (30 + 70)");

        let a2 = realtime_gate(&s, &ak, "gpt-4o").await.expect("admit");
        assert_eq!(used().await, 100 + REALTIME_TURN_RESERVE);
        gov().quota_settle(&a2.ak.ak, -a2.reserved, a2.at).await;
        assert_eq!(used().await, 100, "dropped turn refunded whole");

        let a3 = realtime_gate(&s, &ak, "gpt-4o").await.expect("admit");
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

        let a1 = realtime_gate(&s, &ak, "gpt-4o")
            .await
            .expect("first admits");
        assert_eq!(a1.tpm_reserved, Some(REALTIME_TURN_RESERVE));
        let daily_before = gov.quota_used(&ak.ak).await;

        assert!(
            realtime_gate(&s, &ak, "gpt-4o").await.is_err(),
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

        let admit = realtime_gate(&s, &ak, "rt").await.expect("admit");
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
}
