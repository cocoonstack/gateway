//! HTTP view layer (L5): parse/validate, authenticate the AK, build a
//! `GatewayRequest`, call the handler, shape the wire response, and emit one
//! structured access-log line per request.

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use gw_config::GatewayConfig;
use gw_consts::ErrClass;
use gw_dag::DagContext;
use gw_engines::SharedTransport;
use gw_engines::realtime::{
    is_response_create, realtime_output_delta, realtime_turn_started, realtime_usage,
};
use gw_handler::{BatchItem, OfflineHandler, OnlineHandler};
use gw_models::{
    ChatMsg, ChatParams, EmbeddingParams, GResult, GatewayError, GatewayRequest, ImageParams,
    ModelParamV2, ReasoningParam, SttParams, TtsParams, TypedParams,
};
use gw_protocol::anthropic::{AnthUsage, MessagesRequest, tool_use_to_tool_calls};
use gw_protocol::openai::{
    ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse, Usage,
};
use gw_state::admission;
use gw_state::{
    AkInfo, GatewayState, ReviewVerdict, ThinkingSignatureAudit, ThinkingStreamCapture,
};
use serde_json::{Value, json};

const LEDGER_PAGE_DEFAULT: usize = 100;
const KEY_PAGE_DEFAULT: usize = 200;
const CONFIG_VERSION_PAGE_DEFAULT: usize = 20;
const CONTENT_PAGE_DEFAULT: usize = 200;
const CONTENT_PAGE_MAX: usize = 1_000;
const USAGE_SERIES_MAX_POINTS: i64 = 400;
const STREAM_CHANNEL_CAP: usize = 64;
/// Per-turn token reserve against the AK daily quota; settled to actuals at billing.
const REALTIME_TURN_RESERVE: i64 = 1_000;

static REQ_SEQ: AtomicU64 = AtomicU64::new(1);

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
        .route("/v1/audio/translations", post(audio_translations))
        .route("/v1/moderations", post(moderations))
        .route("/v1/rerank", post(rerank))
        .route("/v1/batches", post(batches_submit))
        .route("/v1/batches/{id}", get(batches_get))
        .route("/v1/files", post(files_upload))
        .route("/v1/files/{id}", get(files_get).delete(files_delete))
        .route("/v1/files/{id}/content", get(files_content))
        .route("/v1/realtime", get(realtime_ws))
        .route("/internal/ledger", get(ledger))
        .route("/internal/accounts", get(accounts))
        .route("/admin/reload", post(admin_reload))
        .route("/admin/config", get(admin_config_get).put(admin_config_put))
        .route("/admin/config/validate", post(admin_config_validate))
        .route("/admin/config/versions", get(admin_config_versions))
        .route(
            "/admin/config/versions/{id}/rollback",
            post(admin_config_rollback),
        )
        .route("/admin/keys", post(admin_key_create).get(admin_key_list))
        .route("/admin/usage", get(admin_usage))
        .route("/admin/usage/users", get(admin_usage_users))
        .route("/admin/usage/series", get(admin_usage_series))
        .route("/admin/models/status", get(admin_models_status))
        .route("/admin/audit/events", get(admin_security_events))
        .route("/admin/audit/ops", get(admin_audit_ops))
        .route("/admin/audit/content/{request_id}", get(admin_content_get))
        .route(
            "/admin/audit/content",
            get(admin_content_list).delete(admin_content_erase),
        )
        .route(
            "/admin/keys/{ak}",
            axum::routing::patch(admin_key_patch).delete(admin_key_delete),
        )
        .fallback(unknown_route)
        .method_not_allowed_fallback(wrong_method)
        .layer(axum::middleware::from_fn(track_requests))
        .with_state(state)
}

/// Route fallback: the envelope's 404 instead of axum's bare one.
async fn unknown_route() -> Response {
    error_response(404, "unknown route")
}

/// Method fallback: the framework 405 normalized to the contract's 400.
async fn wrong_method() -> Response {
    error_response(405, "method not allowed for this route")
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

/// In-band realtime error event. Never terminal: the session
/// stays open, and only a Close frame or disconnect ends it.
fn rt_error(class: ErrClass, message: impl Into<String>) -> Value {
    json!({"type":"error","error":{
        "type": class.openai_type(),
        "code": class.code(),
        "message": message.into(),
    }})
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
    Query(mut q): Query<HashMap<String, String>>,
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
    let Some(model) = q.remove("model") else {
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
    // client attribution hint captured at connect (no per-turn body user field)
    let hint = user_header(&headers).unwrap_or_default();
    // variant split pins for the whole session, sticky by the attributed user
    // (per-connection spread when anonymous) — the REST semantics, one level up
    let served = match model_conf {
        Some(conf) if !conf.variants.is_empty() => {
            let sticky = ak.attributed_user(&hint);
            let generated;
            let key = if sticky.is_empty() {
                generated = gw_handler::new_request_id();
                generated.as_str()
            } else {
                sticky
            };
            gw_config::pick_variant(&conf.variants, key)
                .map_or_else(|| model.clone(), |v| v.model.clone())
        }
        _ => model.clone(),
    };
    let served_conf = if served == model {
        model_conf
    } else {
        snap.cfg.find_model(&served)
    };
    let account = snap
        .state
        .pool
        .select_healthy(
            mt,
            served_conf.and_then(|m| m.provider.as_deref()),
            &[],
            snap.state.health.as_ref(),
        )
        .await;
    let Some(account) = account else {
        // an exhausted pool is a client-visible failure of the public name
        snap.state.avail.record(&model, false);
        return error_response(503, format!("no healthy upstream account serves `{model}`"));
    };
    let m = RtModel {
        requested: model,
        served,
        from_config: served_conf.is_some(),
    };
    // select "realtime" so subprotocol-offering clients get a valid handshake
    let ws = ws.protocols(["realtime"]);
    if account.endpoint.is_empty() {
        ws.on_upgrade(move |socket| {
            realtime_session(socket, s, ak, m, mt, account.name.clone(), hint)
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
        ws.on_upgrade(move |socket| realtime_bridge(socket, s, ak, m, mt, account, hint))
    }
}

/// A realtime session's model identity: the public name the client asked for
/// and the variant actually served (equal without a split). Entitlement and
/// the per-(AK, model) counter judge `requested`; capacity (QPM), pricing,
/// the upstream URL, and availability's success/error samples follow the REST
/// semantics — samples attribute to `requested`.
struct RtModel {
    requested: String,
    served: String,
    /// Whether `served` came from config at handshake — only then can a
    /// reload invalidate it (wire-direct sessions have no config row).
    from_config: bool,
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

struct RealtimeTurn {
    admit: RealtimeAdmit,
    delivered_output_tokens: i64,
}

impl RealtimeTurn {
    fn new(admit: RealtimeAdmit) -> Self {
        Self {
            admit,
            delivered_output_tokens: 0,
        }
    }

    fn record_text(&mut self, text: &str) {
        let tokens = gw_models::token_estimate::default_encoder().encode_len(text);
        self.record_units(tokens as i64);
    }

    fn estimated_output_tokens(&self) -> Option<i64> {
        (self.delivered_output_tokens > 0).then_some(self.delivered_output_tokens)
    }

    fn record_units(&mut self, units: i64) {
        self.delivered_output_tokens = self.delivered_output_tokens.saturating_add(units);
    }
}

/// The REST admission chain applied per realtime generation via the shared
/// [`admission`] checks, with the key re-fetched each turn so mid-session
/// bans/de-entitlements take effect. Denials carry the rendering class
/// directly ((ErrClass, message), no ErrCode): the WS surface is deliberately
/// outside the ErrCode-keyed hooks (abuse noting stays a REST-pipeline
/// concern). Two deliberate divergences from the DAG:
/// over-quota denies instead of degrading (a session can't swap models
/// mid-stream), and the reserve is a fixed turn estimate. Reserves are taken
/// last so a denial never leaves one behind; a failed TPM reserve rolls back
/// the daily reserve just taken.
async fn realtime_gate(
    s: &AppState,
    ak: &AkInfo,
    m: &RtModel,
    hint: &str,
) -> Result<RealtimeAdmit, (ErrClass, String)> {
    let snap = s.handler.config.load();
    let (cfg, state) = (&snap.cfg, &snap.state);
    let ak = match state.auth.authenticate(&ak.ak).await {
        Some(fresh) if fresh.status_at(gw_state::epoch_secs()) == gw_state::KeyStatus::Active => {
            fresh
        }
        _ => {
            return Err((
                ErrClass::AccessDenied,
                format!("access key {} is no longer valid", ak.ak),
            ));
        }
    };
    if !cfg.tenant_allows_model(&ak.tenant, &m.requested) {
        return Err((
            ErrClass::AccessDenied,
            format!(
                "model `{}` is not entitled for tenant `{}`",
                m.requested, ak.tenant
            ),
        ));
    }
    // the session pinned `served` at handshake; a reload may have removed it,
    // and pricing an unknown model silently bills zero — deny so the client
    // reconnects and picks against the live config. Wire-direct sessions
    // (`?model=realtime`, never in config) are exempt: nothing to vanish.
    if m.from_config && cfg.find_model(&m.served).is_none() {
        return Err((
            ErrClass::ResourceNotFound,
            format!("model `{}` is no longer configured; reconnect", m.served),
        ));
    }
    let gov = state.governance.as_ref();
    let throttled = |m: String| (ErrClass::Throttling, m);
    let quota_exceeded = |m: String| (ErrClass::ServiceQuotaExceeded, m);
    admission::check_tenant_rate(gov, cfg, &ak.tenant)
        .await
        .map_err(throttled)?;
    admission::check_ak_rate(gov, &ak)
        .await
        .map_err(throttled)?;
    admission::check_product_qpm(gov, cfg, &ak.product)
        .await
        .map_err(throttled)?;
    admission::check_model_qpm(gov, cfg, &m.served)
        .await
        .map_err(throttled)?;
    admission::check_user_budget(gov, cfg, &ak.tenant, ak.attributed_user(hint))
        .await
        .map_err(quota_exceeded)?;
    if let Some(limit) = admission::model_quota_limit(cfg, &ak, &m.requested)
        && !gov
            .quota_check(&admission::model_quota_key(&ak.ak, &m.requested), limit)
            .await
    {
        return Err((
            ErrClass::ServiceQuotaExceeded,
            format!("model quota exhausted for `{}`", m.requested),
        ));
    }
    let at = gw_state::epoch_secs();
    admission::reserve_daily(gov, &ak, REALTIME_TURN_RESERVE, at)
        .await
        .map_err(quota_exceeded)?;
    let tpm_reserved = match admission::reserve_tpm(gov, &ak, REALTIME_TURN_RESERVE).await {
        Ok(reserved) => reserved,
        Err(denied) => {
            gov.quota_settle(&ak.ak, -REALTIME_TURN_RESERVE, at).await;
            return Err((ErrClass::Throttling, denied));
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
    m: &RtModel,
    mt: gw_consts::Protocol,
    account: &str,
    it: i64,
    ot: i64,
    estimated: bool,
) {
    let ak = &admit.ak;
    // clamp parts and total so a hostile count can't overflow shared counters
    let (it, ot) = (gw_state::clamp_tokens(it), gw_state::clamp_tokens(ot));
    if it.saturating_add(ot) == 0 {
        admit.refund().await;
        return;
    }
    let (cfg, state) = (&admit.snap.cfg, &admit.snap.state);
    // pipeline parity: the same shared weighting the DAG estimate paths use
    let rate = gw_state::model_token_rate(cfg, &m.served);
    let (bp, bc) = gw_models::weighted_pair(it, ot, &rate);
    let total = gw_state::clamp_tokens(bp.saturating_add(bc));
    let model_quota_key = admission::model_quota_limit(cfg, ak, &m.requested)
        .map(|_| admission::model_quota_key(&ak.ak, &m.requested));
    admission::settle_and_bill(
        state,
        cfg,
        admission::SettleInput {
            billing: gw_state::BillingInput {
                ak: &ak.ak,
                product: &ak.product,
                tenant: &ak.tenant,
                user_id: admit.user.as_str(),
                request_id: &admit.request_id,
                requested_model: &m.requested,
                served_model: &m.served,
                protocol: mt.as_str(),
                account,
                prompt: it,
                completion: ot,
                billable_prompt: bp,
                billable_completion: bc,
                total,
                units: 0,
                ptu_spillover: false,
                estimated,
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
    if !estimated {
        state.avail.record(&m.requested, true);
    }
    metrics::counter!("gateway_tokens_total", "kind" => "prompt").increment(it as u64);
    metrics::counter!("gateway_tokens_total", "kind" => "completion").increment(ot as u64);
}

async fn settle_realtime_abort(
    turn: RealtimeTurn,
    m: &RtModel,
    mt: gw_consts::Protocol,
    account: &str,
) {
    match turn.estimated_output_tokens() {
        Some(output) => bill_realtime_turn(&turn.admit, m, mt, account, 0, output, true).await,
        None => turn.admit.refund().await,
    }
}

/// One mock realtime session.
async fn realtime_session(
    mut socket: axum::extract::ws::WebSocket,
    s: AppState,
    ak: AkInfo,
    rtm: RtModel,
    mt: gw_consts::Protocol,
    account: String,
    hint: String,
) {
    use axum::extract::ws::Message;
    let send = |v: Value| Message::Text(v.to_string().into());

    let _ = socket
        .send(send(json!({"type":"session.created",
            "session":{"model": rtm.requested, "account": account}})))
        .await;

    while let Some(Ok(msg)) = socket.recv().await {
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue,
        };
        let Ok(mut ev) = serde_json::from_str::<Value>(&text) else {
            let _ = socket
                .send(send(rt_error(ErrClass::Validation, "invalid json event")))
                .await;
            continue;
        };
        if let Err(reason) = rt_inbound_policy(&s, &ak, &hint, &mut ev).await {
            let _ = socket
                .send(send(rt_error(ErrClass::AccessDenied, reason)))
                .await;
            continue;
        }
        match ev["type"].as_str().unwrap_or_default() {
            "input_text" => {
                let admit = match realtime_gate(&s, &ak, &rtm, &hint).await {
                    Ok(a) => a,
                    Err((class, denied)) => {
                        let _ = socket.send(send(rt_error(class, denied))).await;
                        continue;
                    }
                };
                let mut turn = RealtimeTurn::new(admit);
                let input = ev["text"].as_str().unwrap_or_default();
                let reply = format!("[mock-realtime:{}] you said: {input}", rtm.served);
                let (it, ot) = (
                    (input.len() as i64 / 4).max(1) + 3,
                    (reply.len() as i64 / 4).max(1),
                );
                let mid = (0..=reply.len() / 2)
                    .rev()
                    .find(|&i| reply.is_char_boundary(i))
                    .unwrap_or(0);
                let (a, b) = reply.split_at(mid);
                for delta in [a, b] {
                    if socket
                        .send(send(json!({"type":"response.delta","delta": delta})))
                        .await
                        .is_err()
                    {
                        settle_realtime_abort(turn, &rtm, mt, &account).await;
                        return;
                    }
                    turn.record_text(delta);
                }
                if socket
                    .send(send(json!({"type":"response.done",
                        "usage":{"input_tokens": it, "output_tokens": ot}})))
                    .await
                    .is_err()
                {
                    bill_realtime_turn(&turn.admit, &rtm, mt, &account, it, ot, false).await;
                    return;
                }
                bill_realtime_turn(&turn.admit, &rtm, mt, &account, it, ot, false).await;
            }
            "session.close" => {
                let _ = socket.send(send(json!({"type":"session.closed"}))).await;
                break;
            }
            other => {
                let _ = socket
                    .send(send(rt_error(
                        ErrClass::Validation,
                        format!("unsupported event type `{other}`"),
                    )))
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
    rtm: RtModel,
    mt: gw_consts::Protocol,
    account: Arc<gw_models::Account>,
    hint: String,
) {
    use axum::extract::ws::Message as CMsg;
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message as UMsg;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let base = account.endpoint.trim_end_matches('/');
    let ws_base = base
        .replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1);
    let url = format!("{ws_base}/v1/realtime?model={}", rtm.served);
    let mut req = match url.into_client_request() {
        Ok(r) => r,
        Err(e) => {
            s.handler.state().avail.record(&rtm.requested, false);
            let _ = client
                .send(CMsg::Text(
                    rt_error(ErrClass::InternalServer, format!("bad upstream url: {e}"))
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
            s.handler.state().avail.record(&rtm.requested, false);
            let _ = client
                .send(CMsg::Text(
                    rt_error(
                        ErrClass::ModelError,
                        format!("upstream realtime connect failed: {e}"),
                    )
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
    let mut pending: Option<RealtimeTurn> = None;
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
                                .send(CMsg::Text(rt_error(ErrClass::AccessDenied, reason).to_string().into()))
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
                        match realtime_gate(&s, &ak, &rtm, &hint).await {
                            Ok(admit) => {
                                pending = Some(RealtimeTurn::new(admit));
                                generations += 1;
                            }
                            Err((class, denied)) => {
                                if cl_tx
                                    .send(CMsg::Text(rt_error(class, denied).to_string().into()))
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
                    Some(Err(_)) => {
                        // upstream died mid-session — a client-visible model error
                        s.handler.state().avail.record(&rtm.requested, false);
                        break;
                    }
                    Some(Ok(UMsg::Close(_))) | None => break,
                    Some(Ok(_)) => continue, // ping/pong handled by the ws stacks
                };
                let mut relay = true;
                let mut redacted: Option<String> = None;
                let mut turn_ended = false;
                let mut output_units = 0;
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
                            match realtime_gate(&s, &ak, &rtm, &hint).await {
                                Ok(admit) => pending = Some(RealtimeTurn::new(admit)),
                                Err((class, denied)) => {
                                    let _ = up_tx
                                        .send(UMsg::text(json!({"type":"response.cancel"}).to_string()))
                                        .await;
                                    let _ = cl_tx
                                        .send(CMsg::Text(rt_error(class, denied).to_string().into()))
                                        .await;
                                    suppress = true;
                                    relay = false;
                                }
                            }
                        } else if let Some((it, ot)) = realtime_usage(&account.provider, &v) {
                            // turn boundary — settle the admitted turn;
                            // a boundary with no gated turn bills unreserved
                            match pending.take() {
                                Some(turn) if it.saturating_add(ot) > 0 => {
                                    bill_realtime_turn(
                                        &turn.admit,
                                        &rtm,
                                        mt,
                                        &account.name,
                                        it,
                                        ot,
                                        false,
                                    )
                                    .await
                                }
                                Some(turn) => {
                                    settle_realtime_abort(turn, &rtm, mt, &account.name).await
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
                                        &rtm,
                                        mt,
                                        &account.name,
                                        it,
                                        ot,
                                        false,
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
                        let n = if relay {
                            gw_handler::plugins::dlp_redact_realtime_frame(
                                s.handler.cfg().security_for(&ak.tenant),
                                &mut v,
                            )
                        } else {
                            0
                        };
                        if n > 0 {
                            redacted = Some(v.to_string());
                        }
                        if relay {
                            let (text, opaque) = realtime_output_delta(&v);
                            output_units = text.map_or(0, |text| {
                                let tokens = gw_models::token_estimate::default_encoder()
                                    .encode_len(text);
                                tokens as i64
                            });
                            output_units = output_units.saturating_add(opaque as i64);
                        }
                        // per-token events would be too hot: sum the turn, record once at its boundary
                        out_redacted += n as i64;
                        if turn_ended {
                            flush_rt_out_dlp(&s, &ak, &hint, out_redacted).await;
                            out_redacted = 0;
                        }
                    }
                    // a denied turn's non-JSON output (e.g. audio deltas) is dropped too
                    None => {
                        relay = !suppress;
                        if relay {
                            let opaque = raw_bytes
                                .as_ref()
                                .map_or(0, |bytes| bytes.len().div_ceil(4));
                            output_units = opaque as i64;
                        }
                    }
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
                    if let Some(turn) = pending.as_mut() {
                        turn.record_units(output_units);
                    }
                }
            },
        }
    }
    if let Some(turn) = pending {
        settle_realtime_abort(turn, &rtm, mt, &account.name).await;
    }
    // a turn aborted before its boundary (upstream drop) still applied its
    // redactions per frame — flush the pending count so the audit isn't lost
    flush_rt_out_dlp(&s, &ak, &hint, out_redacted).await;
    if generations > 0 && recognized == 0 {
        tracing::warn!(
            account = %account.name,
            model = %rtm.requested,
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
    let decisions = ctx.decisions_line();
    let ak_id = gw_state::access_key_fingerprint(&ctx.ak.ak);
    metrics::counter!("gateway_tokens_total", "kind" => "prompt").increment(pt.max(0) as u64);
    metrics::counter!("gateway_tokens_total", "kind" => "completion").increment(ct.max(0) as u64);
    tracing::info!(
        target: "access",
        surface,
        request_id = %ctx.request.request_id,
        ak_id,
        product = %ctx.ak.product,
        user_id,
        model = %model,
        protocol = mt,
        account,
        prompt_tokens = pt,
        completion_tokens = ct,
        total_tokens = tt,
        latency_ms = latency.as_millis() as u64,
        decisions = %decisions,
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

/// Local billing ledger snapshot. Global-token only: the raw rows span every
/// tenant and carry the operator's vendor-cost margin basis.
async fn ledger(
    State(s): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    if let Err(r) = require_global_admin(&s, &headers) {
        return r;
    }
    let limit = q_num(&q, "limit", LEDGER_PAGE_DEFAULT);
    match s.handler.state().store.ledger_snapshot(limit).await {
        Ok((count, records)) => Json(json!({ "count": count, "records": records })).into_response(),
        Err(e) => gateway_error(e),
    }
}

/// Account pool view (name/provider/tier/priority/served model family).
/// Global-token only: account names and health are operator internals.
async fn accounts(State(s): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(r) = require_global_admin(&s, &headers) {
        return r;
    }
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
    Json(resp).into_response()
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
        gw_state::KeyStatus::Suspended => {
            Err((403, "access key is suspended for abuse; retry later"))
        }
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

/// The shared body behind [`ApiJson`]/[`AnthJson`]: delegate to `axum::Json`
/// and render the rejection through the surface's own envelope, so a
/// malformed body (400/413/415/422) cannot escape the contract.
async fn extract_json<T, S>(
    req: axum::extract::Request,
    state: &S,
    render: fn(u16, String) -> Response,
) -> Result<T, Response>
where
    axum::Json<T>:
        axum::extract::FromRequest<S, Rejection = axum::extract::rejection::JsonRejection>,
    S: Send + Sync,
{
    match <axum::Json<T> as axum::extract::FromRequest<S>>::from_request(req, state).await {
        Ok(axum::Json(v)) => Ok(v),
        Err(rej) => Err(render(rej.status().as_u16(), rej.body_text())),
    }
}

/// `axum::Json` with rejections in the OpenAI envelope.
struct ApiJson<T>(T);

impl<T, S> axum::extract::FromRequest<S> for ApiJson<T>
where
    axum::Json<T>:
        axum::extract::FromRequest<S, Rejection = axum::extract::rejection::JsonRejection>,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        extract_json(req, state, error_response).await.map(ApiJson)
    }
}

/// [`ApiJson`] in the Anthropic envelope, for `/v1/messages`.
struct AnthJson<T>(T);

impl<T, S> axum::extract::FromRequest<S> for AnthJson<T>
where
    axum::Json<T>:
        axum::extract::FromRequest<S, Rejection = axum::extract::rejection::JsonRejection>,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        extract_json(req, state, anthropic_error)
            .await
            .map(AnthJson)
    }
}

/// The contract's machine channel on every HTTP error response: the
/// classification header plus the CORS exposure browser clients need.
fn with_error_headers(class: ErrClass, mut resp: Response) -> Response {
    let headers = resp.headers_mut();
    headers.insert(
        "x-amzn-errortype",
        axum::http::HeaderValue::from_static(class.name()),
    );
    headers.insert(
        "access-control-expose-headers",
        axum::http::HeaderValue::from_static("x-amzn-errortype, retry-after"),
    );
    resp
}

/// OpenAI-shaped error body: coarse `type`, precise `code`.
fn openai_error_body(class: ErrClass, message: String) -> Value {
    json!({ "error": {
        "message": message,
        "type": class.openai_type(),
        "param": null,
        "code": class.code(),
    }})
}

/// A body-less status passthrough: the 499 client-closed case, where the
/// peer is gone and nothing is rendered.
fn bare_status(status: u16) -> Response {
    StatusCode::from_u16(status)
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
        .into_response()
}

/// Status + machine headers + JSON body: the one seam every envelope
/// renders through.
fn class_response(class: ErrClass, body: Value) -> Response {
    let status = StatusCode::from_u16(class.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    with_error_headers(class, (status, Json(body)).into_response())
}

/// OpenAI-envelope error at the class's external status.
fn class_error(class: ErrClass, message: impl Into<String>) -> Response {
    class_response(class, openai_error_body(class, message.into()))
}

/// OpenAI-envelope error from an ad-hoc (status, message) site; the external
/// status is the classification's, not the caller's literal.
fn error_response(status: u16, message: impl Into<String>) -> Response {
    match ErrClass::from_status(status) {
        Some(class) => class_error(class, message),
        None => bare_status(status),
    }
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

    /// Platform vendor cost (margin basis) is operator-only; a tenant admin
    /// sees its own charge. Every usage read applies this before serializing.
    fn sees_vendor_cost(&self) -> bool {
        matches!(self, AdminScope::Global)
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
    let (scan, text, walk_redacted) =
        gw_handler::plugins::realtime_frame_scan(sec, frame, sec.moderate);
    emit_rt_hits(s, ak, &scan.hits, hint).await;
    if let Some(block) = scan.block {
        return Err(block.message);
    }
    if sec.moderate && !text.is_empty() {
        match s.handler.moderate_rt(sec, &text).await {
            gw_handler::RtModeration::Allow => {}
            gw_handler::RtModeration::Mask(spans) => {
                let masked = gw_handler::plugins::apply_mask_spans_frame(frame, &spans);
                if masked > 0 {
                    write_rt_event(
                        s,
                        ak,
                        ak.attributed_user(hint),
                        "moderation",
                        "mask",
                        masked as i64,
                    )
                    .await;
                }
            }
            gw_handler::RtModeration::Deny(reason) => {
                write_rt_event(s, ak, ak.attributed_user(hint), "moderation", "block", 1).await;
                return Err(reason);
            }
        }
    }
    let redacted = if sec.moderate {
        gw_handler::plugins::dlp_redact_realtime_frame(sec, frame)
    } else {
        walk_redacted
    };
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

#[allow(clippy::result_large_err)] // admin plane, not hot; boxing would noise every call site
fn require_config_store(s: &AppState) -> Result<&Arc<gw_state::PostgresConfigStore>, Response> {
    s.config_store.as_ref().ok_or_else(|| {
        error_response(
            400,
            "config store not configured (set storage.postgres_url)",
        )
    })
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
    let status = k.status_at(gw_state::epoch_secs());
    json!({
        "ak": k.ak, "product": k.product, "tenant": k.tenant, "owner": k.owner,
        "qps": k.qps, "daily_token_quota": k.daily_token_quota,
        "tokens_per_minute": k.tokens_per_minute,
        "expires_at_epoch_secs": k.expires_at_epoch_secs,
        "banned": k.banned,
        "suspended_until_epoch_secs": k.suspended_until_epoch_secs,
        "status": status,
        "available": status == gw_state::KeyStatus::Active,
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
    ApiJson(body): ApiJson<Value>,
) -> Response {
    let (Some(ak), Some(product)) = (body["ak"].as_str(), body["product"].as_str()) else {
        return error_response(400, "ak and product are required");
    };
    // same ban as config load: a ':' would collide with the prefixed
    // governance keyspaces (`abuse:{ak}` could force-suspend another key)
    if ak.is_empty() || ak.contains(':') {
        return error_response(400, "ak must be non-empty and must not contain ':'");
    }
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
        suspended_until_epoch_secs: None,
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
    ApiJson(body): ApiJson<Value>,
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
        suspended_until_epoch_secs: tri("suspended_until_epoch_secs"),
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

/// GET /admin/config — the current fleet config document. Global admin only.
async fn admin_config_get(State(s): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(r) = require_global_admin(&s, &headers) {
        return r;
    }
    let store = match require_config_store(&s) {
        Ok(v) => v,
        Err(r) => return r,
    };
    match store.load_latest().await {
        Ok(Some((version, yaml))) => {
            Json(json!({ "version": version, "yaml": yaml })).into_response()
        }
        Ok(None) => error_response(404, "config store is empty"),
        Err(e) => gateway_error(e),
    }
}

/// POST /admin/config/validate — parse and validate without publishing.
async fn admin_config_validate(
    State(s): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> Response {
    if let Err(r) = require_global_admin(&s, &headers) {
        return r;
    }
    match GatewayConfig::from_yaml(&body) {
        Ok(cfg) => Json(json!({
            "valid": true,
            "generation": cfg.generation(),
            "access_keys": cfg.access_keys.len(),
            "models": cfg.models.len(),
            "accounts": cfg.accounts.len(),
            "tenants": cfg.tenants.len(),
        }))
        .into_response(),
        Err(e) => error_response(400, format!("invalid config: {e}")),
    }
}

/// GET /admin/config/versions — retained config heads, newest first.
async fn admin_config_versions(
    State(s): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    if let Err(r) = require_global_admin(&s, &headers) {
        return r;
    }
    let store = match require_config_store(&s) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let limit = q_num(&q, "limit", CONFIG_VERSION_PAGE_DEFAULT);
    match store.list_versions(limit).await {
        Ok(versions) => Json(json!({ "versions": versions })).into_response(),
        Err(e) => gateway_error(e),
    }
}

/// PUT /admin/config — validate, publish to the fleet config store, and reload
/// this instance; peers converge via the store's change feed. Global admin only.
async fn admin_config_put(
    State(s): State<AppState>,
    headers: HeaderMap,
    AuditSourceIp(source): AuditSourceIp,
    Query(q): Query<HashMap<String, String>>,
    body: String,
) -> Response {
    if let Err(r) = require_global_admin(&s, &headers) {
        return r;
    }
    let store = match require_config_store(&s) {
        Ok(v) => v,
        Err(r) => return r,
    };
    if let Err(e) = GatewayConfig::from_yaml(&body) {
        return error_response(400, format!("invalid config: {e}"));
    }
    let version = match q
        .get("expected_version")
        .and_then(|v| v.parse::<i64>().ok())
    {
        Some(expected) => match store.publish_if(&body, expected).await {
            Ok(Some(v)) => v,
            Ok(None) => {
                return error_response(
                    409,
                    format!("config head moved past version {expected}; reload and retry"),
                );
            }
            Err(e) => return gateway_error(e),
        },
        None => match store.publish(&body).await {
            Ok(v) => v,
            Err(e) => return gateway_error(e),
        },
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

/// POST /admin/config/versions/{id}/rollback — republish a retained document
/// as a new head and reload this instance. Global admin only.
async fn admin_config_rollback(
    State(s): State<AppState>,
    headers: HeaderMap,
    AuditSourceIp(source): AuditSourceIp,
    Path(source_id): Path<i64>,
) -> Response {
    if let Err(r) = require_global_admin(&s, &headers) {
        return r;
    }
    let store = match require_config_store(&s) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let yaml = match store.load_version(source_id).await {
        Ok(Some(y)) => y,
        Ok(None) => return error_response(404, format!("config version {source_id} not found")),
        Err(e) => return gateway_error(e),
    };
    // a retained document can predate stricter validation — republishing it
    // unvalidated would brick peers' reloads and fresh boots
    if let Err(e) = GatewayConfig::from_yaml(&yaml) {
        return error_response(
            400,
            format!("config version {source_id} no longer validates: {e}"),
        );
    }
    let version = match store.publish(&yaml).await {
        Ok(v) => v,
        Err(e) => return gateway_error(e),
    };
    let reload = s.reload().await;
    let detail = match &reload {
        Ok(()) => format!("source_version={source_id}"),
        Err(e) => format!("source_version={source_id}; local reload failed: {e}"),
    };
    audit_admin(
        &s,
        &AdminScope::Global,
        source,
        "config_rollback",
        &version.to_string(),
        detail,
    )
    .await;
    match reload {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "status": "rolled_back",
                "source_version": source_id,
                "version": version,
            })),
        )
            .into_response(),
        Err(e) => error_response(
            500,
            format!("published rollback v{version} but local reload failed: {e}"),
        ),
    }
}

/// GET /admin/keys?offset=&limit= — a page of the key table, scoped: a tenant
/// admin sees only its own keys. Paginated so a fleet key table never loads whole.
async fn admin_key_list(
    State(s): State<AppState>,
    scope: AdminScope,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    if let Some(ak) = q.get("ak") {
        // exact lookup: an uncovered key answers an empty page, not 404, so a
        // tenant admin cannot probe foreign key existence through the filter
        let keys: Vec<Value> = s
            .handler
            .state()
            .auth
            .authenticate(ak)
            .await
            .filter(|k| scope.covers(&k.tenant))
            .map(|k| ak_public_json(&k))
            .into_iter()
            .collect();
        let mut resp = json!({ "count": keys.len(), "offset": 0 });
        resp["keys"] = Value::Array(keys);
        return Json(resp).into_response();
    }
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

/// GET /admin/models/status — per-model availability over the configured
/// window, judged from minute-bucketed success/error counts (REST terminal
/// outcomes; realtime samples per billed turn and on session-fatal upstream
/// errors). A tenant admin sees only its entitled models.
async fn admin_models_status(State(s): State<AppState>, scope: AdminScope) -> Response {
    let cfg = s.handler.cfg();
    let st = &cfg.stability;
    let until = gw_state::epoch_secs() / 60;
    let since = until - (st.availability_window_minutes - 1);
    let state = s.handler.state();
    let entitled = cfg.models.iter().filter(|m| match &scope {
        AdminScope::Tenant(t) => cfg.tenant_allows_model(t, &m.name),
        AdminScope::Global => true,
    });
    let avail = &state.avail;
    let counts = futures::future::join_all(
        entitled.map(|m| async move { (&m.name, avail.window(&m.name, since, until).await) }),
    )
    .await;
    let rows: Vec<Value> = counts
        .into_iter()
        .map(|(name, (ok, err))| {
            let verdict = gw_state::classify(
                ok,
                err,
                st.availability_min_samples,
                st.unstable_error_rate,
                st.unavailable_error_rate,
            );
            json!({
                "model": name,
                "state": verdict,
                "requests": ok + err,
                "errors": err,
                "window_minutes": st.availability_window_minutes,
            })
        })
        .collect();
    Json(json!({ "models": rows })).into_response()
}

/// GET /admin/usage — ledger rollup by (tenant, requested model). A tenant
/// admin sees only its own tenant; the global admin may filter with ?tenant=.
async fn admin_usage(
    State(s): State<AppState>,
    scope: AdminScope,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let filter = scope.tenant_filter(&q);
    let mut usage = match s.handler.state().store.ledger_usage(filter).await {
        Ok(rows) => rows,
        Err(e) => return gateway_error(e),
    };
    if !scope.sees_vendor_cost() {
        for u in &mut usage {
            u.vendor_cost_micros = 0;
        }
    }
    Json(json!({ "usage": usage })).into_response()
}

/// GET /admin/usage/users?user=&since=&until= — precise per-user cost over a
/// billing period, grouped by (user, requested model). Tenant-scoped like
/// [`admin_usage`]; `since`/`until` are unix seconds (default: all time).
async fn admin_usage_users(
    State(s): State<AppState>,
    scope: AdminScope,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let tenant = scope.tenant_filter(&q);
    let since = q_num(&q, "since", 0);
    let until = q_num(&q, "until", i64::MAX);
    let mut usage = match s
        .handler
        .state()
        .store
        .usage_by_user(tenant, q.get("user").map(String::as_str), since, until)
        .await
    {
        Ok(rows) => rows,
        Err(e) => return gateway_error(e),
    };
    if !scope.sees_vendor_cost() {
        for u in &mut usage {
            u.vendor_cost_micros = 0;
        }
    }
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

/// GET /admin/usage/series?bucket=hour|day&since=&until=&user= — bounded
/// tenant/user usage totals for dashboard charts, scoped like the other usage reads.
async fn admin_usage_series(
    State(s): State<AppState>,
    scope: AdminScope,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let now = gw_state::epoch_secs();
    let bucket_name = q.get("bucket").map(String::as_str).unwrap_or("day");
    let bucket_secs = match bucket_name {
        "hour" => 3_600,
        "day" => 86_400,
        _ => return error_response(400, "bucket must be `hour` or `day`"),
    };
    let since = q_num(&q, "since", now.saturating_sub(29 * 86_400));
    let until = q_num(&q, "until", now);
    if since < 0 || until < since {
        return error_response(400, "since/until must be a valid non-negative range");
    }
    let first = since - since.rem_euclid(bucket_secs);
    let points = (until - first) / bucket_secs + 1;
    if points > USAGE_SERIES_MAX_POINTS {
        return error_response(
            400,
            format!("usage series is limited to {USAGE_SERIES_MAX_POINTS} points"),
        );
    }
    let tenant = scope.tenant_filter(&q);
    let user = q.get("user").map(String::as_str);
    let mut by_bucket: std::collections::BTreeMap<i64, gw_state::UserUsageRow> = match s
        .handler
        .state()
        .store
        .usage_series(tenant, user, since, until, bucket_secs)
        .await
    {
        Ok(rows) => rows.into_iter().collect(),
        Err(e) => return gateway_error(e),
    };
    let redact_vendor = !scope.sees_vendor_cost();
    let mut series = Vec::with_capacity(points as usize);
    for idx in 0..points {
        let start = first.saturating_add(idx * bucket_secs);
        let end = start.saturating_add(bucket_secs - 1).min(until);
        let totals = by_bucket
            .remove(&start)
            .unwrap_or_else(|| gw_state::UserUsageRow::zero(String::new(), String::new()));
        series.push(json!({
            "start": start,
            "end": end,
            "requests": totals.requests,
            "prompt_tokens": totals.prompt_tokens,
            "completion_tokens": totals.completion_tokens,
            "total_tokens": totals.total_tokens,
            "cost_micros": totals.cost_micros,
            "vendor_cost_micros": if redact_vendor { 0 } else { totals.vendor_cost_micros },
        }));
    }
    Json(json!({
        "bucket": bucket_name,
        "since": since,
        "until": until,
        "series": series,
    }))
    .into_response()
}

/// A CSV field, RFC-4180 quoted AND neutralized against spreadsheet formula
/// injection: a field opening with a formula trigger (`= + - @` / tab / CR) is
/// prefixed with `'` so Excel/Sheets treat it as text (the value is
/// attacker-controlled — it can carry a user id).
fn csv_field(s: &str) -> std::borrow::Cow<'_, str> {
    let needs_prefix = s
        .chars()
        .next()
        .is_some_and(|c| matches!(c, '=' | '+' | '-' | '@' | '\t' | '\r'));
    let body: std::borrow::Cow<'_, str> = if needs_prefix {
        format!("'{s}").into()
    } else {
        s.into()
    };
    if body.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", body.replace('"', "\"\"")).into()
    } else {
        body
    }
}

/// GET /admin/audit/events?limit= — content-safety hits (no prompt text), newest
/// first. Tenant-scoped: a tenant admin sees only its own tenant's events.
async fn admin_security_events(
    State(s): State<AppState>,
    scope: AdminScope,
    Query(q): Query<HashMap<String, String>>,
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
    Query(q): Query<HashMap<String, String>>,
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

/// A retained row's body for an admin read: unsealed when possible, `null`
/// when the seal key cannot open it — never the raw ciphertext.
fn unsealed_content(sealed: bool, content: String) -> Value {
    if sealed {
        gw_state::content::open(&content)
            .map(Value::String)
            .unwrap_or(Value::Null)
    } else {
        Value::String(content)
    }
}

/// GET /admin/audit/content/{request_id} — the retained prompt/response/terminal rows
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
            let content = unsealed_content(r.sealed, r.content);
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

/// GET /admin/audit/content?user=&limit=&include= — retained-content rows for
/// one end user, newest first; metadata only unless `include=bodies`, which
/// inlines each row's unsealed content (`null` when the seal key is absent,
/// as on the per-request read). Tenant-scoped exactly like the erase below.
async fn admin_content_list(
    State(s): State<AppState>,
    scope: AdminScope,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let Some(user) = q.get("user").filter(|u| !u.is_empty()) else {
        return error_response(400, "user is required");
    };
    let tenant = scope.tenant_filter(&q);
    let limit = q_num(&q, "limit", CONTENT_PAGE_DEFAULT).min(CONTENT_PAGE_MAX);
    let with_bodies = q.get("include").is_some_and(|v| v == "bodies");
    match s
        .handler
        .state()
        .store
        .content_list_user(tenant, user, limit, with_bodies)
        .await
    {
        Ok(rows) => {
            let rows: Vec<Value> = rows
                .into_iter()
                .map(|r| {
                    let mut row = json!({
                        "request_id": r.request_id,
                        "user_id": r.user_id,
                        "kind": r.kind,
                        "created_at_epoch_secs": r.created_at_epoch_secs,
                    });
                    if with_bodies {
                        row["content"] = unsealed_content(r.sealed, r.content);
                    }
                    row
                })
                .collect();
            Json(json!({ "rows": rows })).into_response()
        }
        Err(e) => gateway_error(e),
    }
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
    Query(q): Query<HashMap<String, String>>,
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

/// OpenAI-envelope rendering of an internal error: classified, never a
/// status passthrough — a terminal vendor 401 lands as 424 ModelError, not
/// as the client's own 401.
fn gateway_error(e: GatewayError) -> Response {
    let Some(class) = ErrClass::classify(e.code, e.http_status) else {
        return bare_status(e.http_status);
    };
    let original_status = e.original_status();
    let resource = e.resource;
    let mut body = openai_error_body(class, e.message);
    if class == ErrClass::ModelError {
        if let Some(os) = original_status {
            body["error"]["original_status_code"] = os.into();
        }
        if let Some(r) = resource {
            body["error"]["resource_name"] = r.into();
        }
    }
    class_response(class, body)
}

/// Anthropic-shaped error body — `error.type` is the discriminator the
/// Anthropic SDKs key their exception dispatch on; `code` is additive.
fn anthropic_error_body(class: ErrClass, message: String) -> Value {
    json!({
        "type": "error",
        "error": {
            "type": class.anthropic_type(),
            "code": class.code(),
            "message": message,
        },
    })
}

fn anthropic_error_with(class: ErrClass, message: impl Into<String>) -> Response {
    class_response(class, anthropic_error_body(class, message.into()))
}

/// Anthropic-shaped error from an ad-hoc (status, message) site.
fn anthropic_error(status: u16, message: impl Into<String>) -> Response {
    match ErrClass::from_status(status) {
        Some(class) => anthropic_error_with(class, message),
        None => bare_status(status),
    }
}

/// [`gateway_error`]'s classification, rendered in the Anthropic shape.
fn anthropic_gateway_error(e: GatewayError) -> Response {
    match ErrClass::classify(e.code, e.http_status) {
        Some(class) => anthropic_error_with(class, e.message),
        None => bare_status(e.http_status),
    }
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

async fn terminal_response(ctx: &DagContext, response: Response) -> Response {
    gw_handler::persist_terminal_response(ctx, response.status().as_u16()).await;
    response
}

fn next_id(prefix: &str) -> String {
    format!("{prefix}-local-{}", REQ_SEQ.fetch_add(1, Ordering::Relaxed))
}

/// The wire default when an engine reported no finish reason.
fn finish_or_stop(fr: &str) -> &str {
    if fr.is_empty() { "stop" } else { fr }
}

/// Engine tool calls in OpenAI shape: the anthropic engine hands over native `tool_use` blocks.
fn openai_tool_calls(calls: Value, index: &mut usize) -> Vec<Value> {
    match calls {
        Value::Array(a) => tool_use_to_tool_calls(a, index),
        other => vec![other],
    }
}

/// finish_reason mapping, anthropic → openai.
fn finish_openai(fr: String) -> String {
    match fr.as_str() {
        "" | "end_turn" | "stop_sequence" | "COMPLETE" | "complete" => "stop".to_owned(),
        "max_tokens" => "length".to_owned(),
        "tool_use" => "tool_calls".to_owned(),
        _ => fr,
    }
}

/// finish_reason mapping, openai → anthropic.
fn finish_anthropic(fr: String) -> String {
    match fr.as_str() {
        "" | "stop" => "end_turn".to_owned(),
        "length" => "max_tokens".to_owned(),
        "tool_calls" => "tool_use".to_owned(),
        _ => fr,
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

/// The chat surface's reasoning request: OpenAI's flat `reasoning_effort`, or
/// OpenRouter's `reasoning{effort, max_tokens, enabled}` (`enabled` alone means
/// the vendor's default depth; `enabled: false` turns it off).
fn chat_reasoning(effort: Option<String>, reasoning: Option<Value>) -> Option<Box<ReasoningParam>> {
    let mut param = ReasoningParam {
        effort,
        ..Default::default()
    };
    if let Some(Value::Object(mut reasoning)) = reasoning {
        if let Some(Value::String(effort)) = reasoning.remove("effort") {
            param.effort = Some(effort);
        }
        param.budget_tokens = reasoning.get("max_tokens").and_then(Value::as_i64);
        if param.effort.is_none() && param.budget_tokens.is_none() {
            param.effort = match reasoning.get("enabled").and_then(Value::as_bool) {
                Some(true) => Some("medium".to_owned()),
                Some(false) => Some("none".to_owned()),
                None => None,
            };
        }
    }
    (param.effort.is_some() || param.budget_tokens.is_some()).then(|| Box::new(param))
}

/// POST /v1/chat/completions (OpenAI-compatible surface)
async fn chat_completions(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    ApiJson(body): ApiJson<ChatCompletionRequest>,
) -> Response {
    let started = Instant::now();
    if body.messages.is_empty() {
        return error_response(400, "messages must not be empty");
    }

    let messages: Vec<ChatMsg> = body
        .messages
        .into_iter()
        .map(|m| {
            let (content, parts) = m
                .content
                .map(|c| c.into_text_and_parts())
                .unwrap_or_default();
            ChatMsg {
                role: m.role,
                content,
                parts: parts.map(Value::Array),
                tool_calls: m.tool_calls.and_then(|tc| serde_json::to_value(tc).ok()),
                tool_call_id: m.tool_call_id,
                reasoning_content: m.reasoning_content,
                reasoning_details: m.reasoning_details.map(Value::Array),
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
        reasoning: chat_reasoning(body.reasoning_effort, body.reasoning),
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
        message: messages,
        model_param_v2: Some(param),
        user_id,
        ..Default::default()
    };

    if let Some(model) = stream_model {
        return chat_stream_response(s, request, ak, model, started).into_response();
    }

    let mut ctx = match run_pipeline(&s, request, ak).await {
        Ok(ctx) => ctx,
        Err(e) => return gateway_error(e),
    };
    log_access("chat_completions", &ctx, started);
    let Some(mut outcome) = ctx.outcome.take() else {
        let response = error_response(500, "pipeline produced no outcome");
        return terminal_response(&ctx, response).await;
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

    let mut resp = if let Some(tc) = outcome.response.tool_calls.take() {
        let calls: Vec<gw_protocol::openai::ToolCall> =
            match serde_json::from_value(Value::Array(openai_tool_calls(tc, &mut 0))) {
                Ok(calls) => calls,
                Err(e) => {
                    let response = error_response(
                        500,
                        format!("engine tool calls do not render as OpenAI tool_calls: {e}"),
                    );
                    return terminal_response(&ctx, response).await;
                }
            };
        ChatCompletionResponse::tool_calls(
            id,
            created,
            model_out,
            outcome.response.message,
            calls,
            usage,
        )
    } else {
        ChatCompletionResponse::text(
            id,
            created,
            model_out,
            outcome.response.message,
            finish_openai(outcome.response.finish_reason),
            usage,
        )
    };
    if !outcome.response.reasoning.is_empty() || outcome.response.reasoning_details.is_some() {
        let message = &mut resp.choices[0].message;
        message.reasoning_content =
            (!outcome.response.reasoning.is_empty()).then_some(outcome.response.reasoning);
        message.reasoning_details = outcome.response.reasoning_details;
    }
    let response = (StatusCode::OK, Json(resp)).into_response();
    terminal_response(&ctx, response).await
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
    let buffer_output = s.handler.cfg().security_for(&ak.tenant).redacts_output();
    if !buffer_output {
        request.stream_tx = Some(tx.clone());
    }
    let handler = s.handler.clone();
    tokio::spawn(async move {
        match handler.run(request, ak).await {
            Ok(mut ctx) => {
                let dlp = ctx.billing_deferred;
                if !dlp {
                    log_access(surface, &ctx, started);
                }
                if let Some(outcome) = ctx.outcome.as_mut() {
                    let usage_totals = (
                        outcome.response.prompt_tokens,
                        outcome.response.completion_tokens,
                        outcome.response.total_tokens,
                    );
                    let common_usage = outcome.response.common_usage;
                    let mut tail = if dlp && outcome.chunks.is_empty() {
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
                    if dlp {
                        let mut delivered = 0i64;
                        let mut complete = true;
                        for chunk in tail {
                            let tokens = stream_chunk_output_tokens(&chunk);
                            if tx.send(chunk).await.is_err() {
                                complete = false;
                                break;
                            }
                            delivered = delivered.saturating_add(tokens);
                        }
                        let delivery = if complete {
                            gw_dag::StreamDelivery::Complete
                        } else if delivered > 0 {
                            gw_dag::StreamDelivery::Partial(delivered)
                        } else {
                            gw_dag::StreamDelivery::None
                        };
                        if let Err(e) =
                            gw_handler::complete_buffered_stream(&mut ctx, delivery).await
                        {
                            tracing::error!(error = %e, "buffered stream settlement failed");
                        }
                        log_access(surface, &ctx, started);
                    } else {
                        for chunk in tail {
                            if tx.send(chunk).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                // 499 client-closed classifies to None: the peer is gone and
                // no frame is rendered.
                if let Some(error) = gw_models::StreamError::from_error(e) {
                    let _ = tx
                        .send(gw_engines::StreamChunk {
                            error: Some(Box::new(error)),
                            ..Default::default()
                        })
                        .await;
                }
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

/// Chat/legacy SSE error frame: the OpenAI envelope plus the upstream's
/// original status when one was received.
fn stream_error_body(err: gw_models::StreamError) -> Value {
    let mut body = openai_error_body(err.class, err.message);
    if let Some(os) = err.original_status {
        body["error"]["original_status_code"] = os.into();
    }
    body
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
        tool_index: usize,
    }
    impl SseEncodeState for St {
        fn queue(&mut self) -> &mut VecDeque<Event> {
            &mut self.queue
        }
        fn apply(&mut self, chunk: Option<gw_engines::StreamChunk>) -> bool {
            match chunk {
                Some(gw_engines::StreamChunk {
                    error: Some(err), ..
                }) => {
                    self.queue
                        .push_back(Event::default().data(stream_error_body(*err).to_string()));
                    self.queue.push_back(Event::default().data("[DONE]"));
                    true
                }
                Some(mut c) => {
                    if !c.reasoning.is_empty() || c.reasoning_details.is_some() {
                        let chunk = ChatCompletionChunk::reasoning(
                            &self.id,
                            self.created,
                            &self.model,
                            std::mem::take(&mut c.reasoning),
                            c.reasoning_details.take(),
                        );
                        if let Ok(payload) = serde_json::to_string(&chunk) {
                            self.queue.push_back(Event::default().data(payload));
                        }
                    }
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
                        let chunk = ChatCompletionChunk::tool_calls(
                            &self.id,
                            self.created,
                            &self.model,
                            openai_tool_calls(tc, &mut self.tool_index),
                        );
                        if let Ok(payload) = serde_json::to_string(&chunk) {
                            self.queue.push_back(Event::default().data(payload));
                        }
                    }
                    if let Some(fr) = c.finish_reason {
                        // held back until usage arrives so the final frame carries both
                        self.pending_finish = Some(finish_openai(fr));
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
            tool_index: 0,
        },
    )
}

/// The reasoning and message text of a buffered response as chunks, moved out.
fn text_chunks(resp: &mut gw_models::GatewayResponse) -> Vec<gw_engines::StreamChunk> {
    let mut chunks = Vec::new();
    if !resp.reasoning.is_empty() || resp.reasoning_details.is_some() {
        chunks.push(gw_engines::StreamChunk {
            reasoning: std::mem::take(&mut resp.reasoning),
            reasoning_details: resp.reasoning_details.take(),
            ..Default::default()
        });
    }
    if !resp.message.is_empty() {
        chunks.push(gw_engines::StreamChunk {
            delta: std::mem::take(&mut resp.message),
            ..Default::default()
        });
    }
    chunks
}

/// A native event's type as its SSE event name. An upstream-controlled name
/// that is missing or CR/LF-bearing cannot become one (axum asserts): the
/// caller drops the frame and keeps the stream alive.
fn native_event_kind(event: &Value) -> Option<&str> {
    event["type"]
        .as_str()
        .filter(|kind| !kind.is_empty() && !kind.contains(['\r', '\n']))
}

/// Chunks for an engine that returned a buffered response.
fn synth_chunks(outcome: &mut gw_engines::EngineOutcome) -> Vec<gw_engines::StreamChunk> {
    let resp = &mut outcome.response;
    let mut chunks = if outcome.chunks.is_empty() {
        text_chunks(resp)
    } else {
        std::mem::take(&mut outcome.chunks)
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
fn redacted_stream_tail(outcome: &mut gw_engines::EngineOutcome) -> Vec<gw_engines::StreamChunk> {
    let resp = &mut outcome.response;
    if resp.anthropic_content.is_some() {
        let chunks = gw_engines::anthropic_native_chunks(resp, None);
        resp.anthropic_content = None;
        return chunks;
    }
    let mut chunks = text_chunks(resp);
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

fn stream_chunk_output_tokens(chunk: &gw_engines::StreamChunk) -> i64 {
    let encoder = gw_models::token_estimate::default_encoder();
    let mut tokens = encoder.encode_len(&chunk.delta) as i64;
    if !chunk.reasoning.is_empty() {
        tokens = tokens.saturating_add(encoder.encode_len(&chunk.reasoning) as i64);
    }
    if let Some(tool_calls) = &chunk.tool_calls {
        tokens = tokens.saturating_add(encoder.encode_len(&tool_calls.to_string()) as i64);
    }
    if let Some(event) = &chunk.native_event {
        // Anthropic deltas are objects keyed by kind; Responses deltas are strings
        if let Some(value) = event["delta"].as_str() {
            tokens = tokens.saturating_add(encoder.encode_len(value) as i64);
        }
        for field in ["text", "thinking", "partial_json"] {
            if let Some(value) = event["delta"][field].as_str() {
                tokens = tokens.saturating_add(encoder.encode_len(value) as i64);
            }
        }
        if let Some(name) = event["content_block"]["name"].as_str() {
            tokens = tokens.saturating_add(encoder.encode_len(name) as i64);
        }
    }
    gw_state::clamp_tokens(tokens)
}

/// POST /v1/messages (Anthropic-compatible surface, stream + non-stream)
async fn messages(
    State(s): State<AppState>,
    headers: HeaderMap,
    AnthJson(mut body): AnthJson<MessagesRequest>,
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
        reasoning: (body.thinking.is_some() || body.output_config.is_some()).then(|| {
            Box::new(ReasoningParam {
                thinking: body.thinking,
                output_config: body.output_config,
                ..Default::default()
            })
        }),
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
        preserve_anthropic_wire: true,
        message: body
            .messages
            .into_iter()
            .map(|m| match m.content {
                Value::String(s) => ChatMsg::text(m.role, s),
                Value::Array(blocks) => {
                    let text = gw_protocol::anthropic::blocks_text(&blocks);
                    let mut msg = ChatMsg::text(m.role, text);
                    msg.parts = Some(Value::Array(blocks));
                    msg
                }
                _ => ChatMsg::text(m.role, String::new()),
            })
            .collect(),
        model_param_v2: Some(param),
        user_id,
        ..Default::default()
    };

    let thinking_audit = s.handler.state().thinking_signatures.clone();
    let (thinking_context, thinking_verdict) = thinking_audit.review_request(&request, &ak.ak);
    if thinking_verdict == ReviewVerdict::Mismatch {
        return anthropic_error(
            400,
            "protected thinking proof does not match the response previously served for this tool call",
        );
    }

    if let Some(model) = stream_model {
        return messages_stream_response(
            s,
            request,
            ak,
            model,
            started,
            thinking_audit,
            thinking_context,
        )
        .into_response();
    }

    let mut ctx = match run_pipeline(&s, request, ak).await {
        Ok(ctx) => ctx,
        Err(e) => return anthropic_gateway_error(e),
    };
    log_access("messages", &ctx, started);
    let Some(mut outcome) = ctx.outcome.take() else {
        let response = anthropic_error(500, "pipeline produced no outcome");
        return terminal_response(&ctx, response).await;
    };
    let usage = anthropic_usage(
        outcome.response.prompt_tokens,
        outcome.response.completion_tokens,
        outcome.response.common_usage,
    );
    let content = match outcome.response.anthropic_content.take() {
        Some(Value::Array(blocks)) => blocks,
        _ => {
            let mut blocks = Vec::new();
            if !outcome.response.reasoning.is_empty() {
                blocks.push(json!({
                    "type":"thinking",
                    "thinking":Value::String(std::mem::take(&mut outcome.response.reasoning)),
                    "signature":""
                }));
            }
            if !outcome.response.message.is_empty() {
                blocks.push(json!({
                    "type":"text",
                    "text":Value::String(std::mem::take(&mut outcome.response.message))
                }));
            }
            blocks.extend(anthropic_tool_blocks(outcome.response.tool_calls.take()));
            blocks
        }
    };
    if let Some(context) = thinking_context.as_ref() {
        thinking_audit.remember_content(context, &content);
    }
    // built by hand: json! would deep-copy the content blocks
    let mut body = serde_json::Map::with_capacity(7);
    body.insert("id".into(), next_id("msg").into());
    body.insert("type".into(), "message".into());
    body.insert("role".into(), "assistant".into());
    body.insert("model".into(), Value::String(outcome.response.model));
    body.insert("content".into(), Value::Array(content));
    body.insert(
        "stop_reason".into(),
        finish_anthropic(outcome.response.finish_reason).into(),
    );
    body.insert("usage".into(), json!(usage));
    let response = (StatusCode::OK, Json(Value::Object(body))).into_response();
    terminal_response(&ctx, response).await
}

/// tool_use blocks for an engine's tool_calls: native blocks pass through;
/// OpenAI-shaped calls convert via [`gw_protocol::anthropic::tool_calls_to_tool_use`].
fn anthropic_tool_blocks(tool_calls: Option<Value>) -> Vec<Value> {
    let Some(Value::Array(blocks)) = tool_calls else {
        return Vec::new();
    };
    if blocks.iter().any(|b| b["type"] == "tool_use") {
        blocks
            .into_iter()
            .filter(|b| b["type"] == "tool_use")
            .collect()
    } else {
        gw_protocol::anthropic::tool_calls_to_tool_use(blocks)
    }
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
    thinking_audit: ThinkingSignatureAudit,
    thinking_context: Option<gw_state::AuditContext>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>> + use<>> {
    let rx = spawn_stream_pipeline(&s, request, ak, "messages", started);

    #[derive(Clone, Copy, PartialEq)]
    enum BlockKind {
        Text,
        Thinking,
    }

    struct St {
        queue: VecDeque<Event>,
        id: String,
        model: String,
        started_msg: bool,
        text_idx: Option<usize>,
        thinking_idx: Option<usize>,
        next_idx: usize,
        /// OpenAI-shaped tool-call fragments, accumulated until the stream ends.
        tool_frags: Option<Value>,
        pending_finish: Option<String>,
        thinking_capture: Option<ThinkingStreamCapture>,
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

        /// A text or (for a non-Anthropic model's reasoning prose) unsigned
        /// thinking block; thinking precedes text, so opening text closes it.
        fn open_block(&mut self, kind: BlockKind) -> usize {
            if let Some(idx) = *self.slot(kind) {
                return idx;
            }
            if kind == BlockKind::Text {
                self.close_block(BlockKind::Thinking);
            }
            let idx = self.next_idx;
            self.next_idx += 1;
            *self.slot(kind) = Some(idx);
            let content_block = match kind {
                BlockKind::Text => json!({"type":"text","text":""}),
                BlockKind::Thinking => json!({"type":"thinking","thinking":"","signature":""}),
            };
            self.queue.push_back(Self::ev(
                "content_block_start",
                json!({"type":"content_block_start","index":idx,"content_block":content_block}),
            ));
            idx
        }

        fn close_block(&mut self, kind: BlockKind) {
            if kind == BlockKind::Text {
                self.close_block(BlockKind::Thinking);
            }
            if let Some(idx) = self.slot(kind).take() {
                self.queue.push_back(Self::ev(
                    "content_block_stop",
                    json!({"type":"content_block_stop","index":idx}),
                ));
            }
        }

        fn slot(&mut self, kind: BlockKind) -> &mut Option<usize> {
            match kind {
                BlockKind::Text => &mut self.text_idx,
                BlockKind::Thinking => &mut self.thinking_idx,
            }
        }

        /// The wire pattern clients expect for a tool_use block: empty `input`
        /// in the start frame, the arguments as one input_json_delta, stop.
        fn emit_tool_block(&mut self, block: &Value) {
            self.close_block(BlockKind::Text);
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
                for block in anthropic_tool_blocks(Some(frags)) {
                    self.emit_tool_block(&block);
                }
            }
            self.close_block(BlockKind::Text);
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
                    error: Some(err), ..
                }) => {
                    let err = *err;
                    self.queue.push_back(St::ev(
                        "error",
                        anthropic_error_body(err.class, err.message),
                    ));
                    true
                }
                Some(mut c) => {
                    if let Some(mut event) = c.native_event.take() {
                        if let Some(capture) = self.thinking_capture.as_mut() {
                            capture.observe(&event);
                        }
                        if native_event_kind(&event).is_none() {
                            return false;
                        }
                        if event["type"] == "message_start" {
                            self.started_msg = true;
                            if let Some(message) =
                                event.get_mut("message").and_then(Value::as_object_mut)
                            {
                                message.insert("id".to_owned(), self.id.clone().into());
                            }
                        }
                        let done = event["type"] == "message_stop";
                        let data = event.to_string();
                        let kind = event["type"].as_str().unwrap_or_default();
                        self.queue
                            .push_back(Event::default().event(kind).data(data));
                        return done;
                    }
                    if !c.reasoning.is_empty() {
                        self.ensure_message_start();
                        let idx = self.open_block(BlockKind::Thinking);
                        self.queue.push_back(St::ev(
                            "content_block_delta",
                            json!({"type":"content_block_delta","index":idx,
                                   "delta":{"type":"thinking_delta","thinking":c.reasoning}}),
                        ));
                    }
                    if !c.delta.is_empty() {
                        self.ensure_message_start();
                        let idx = self.open_block(BlockKind::Text);
                        self.queue.push_back(St::ev(
                            "content_block_delta",
                            json!({"type":"content_block_delta","index":idx,
                                   "delta":{"type":"text_delta","text":c.delta}}),
                        ));
                    }
                    if let Some(tc) = c.tool_calls.take() {
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
                            gw_engines::merge_tool_call_fragments(&mut self.tool_frags, &tc);
                        }
                    }
                    if let Some(fr) = c.finish_reason {
                        self.pending_finish = Some(finish_anthropic(fr));
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
            thinking_idx: None,
            next_idx: 0,
            tool_frags: None,
            pending_finish: None,
            thinking_capture: thinking_audit.stream_capture(thinking_context),
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

/// The shared tail of the message-less typed-param families (embeddings,
/// images, moderations, rerank): pipeline, access log, native payload.
#[allow(clippy::too_many_arguments)] // mirrors run_family; all call sites are literal
async fn family_response(
    s: &AppState,
    ak: AkInfo,
    model: String,
    mt: gw_consts::Protocol,
    typed: TypedParams,
    user_id: Option<String>,
    surface: &'static str,
    engine: &str,
    started: Instant,
) -> Response {
    match run_family(s, ak, model, mt, typed, vec![], user_id).await {
        Ok(mut ctx) => {
            log_access(surface, &ctx, started);
            let response = response_v2_or_500(ctx.outcome.take(), engine);
            terminal_response(&ctx, response).await
        }
        Err(resp) => resp,
    }
}

/// An `input`-style field that may be a string or an array of strings
/// (the OpenAI embeddings/moderations shape).
fn string_or_string_array(v: Option<Value>) -> Vec<String> {
    match v {
        Some(Value::String(x)) => vec![x],
        Some(Value::Array(a)) => a
            .into_iter()
            .filter_map(|v| match v {
                Value::String(s) => Some(s),
                _ => None,
            })
            .collect(),
        _ => vec![],
    }
}

/// The engine's native payload; a blocked outcome is the client's 400.
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
    ApiJson(mut body): ApiJson<Value>,
) -> Response {
    let started = Instant::now();
    let model = gw_engines::engine::take_string(&mut body, "/model").unwrap_or_default();
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
    let mut ctx = match run_family(
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
    let Some(outcome) = ctx.outcome.take() else {
        let response = error_response(500, "pipeline produced no outcome");
        return terminal_response(&ctx, response).await;
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
    let response = (StatusCode::OK, Json(resp)).into_response();
    terminal_response(&ctx, response).await
}

/// POST /v1/responses — native passthrough: the whole body rides as `raw`
/// through ResponsesEngine and its native response is returned as-is.
async fn responses(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    ApiJson(body): ApiJson<Value>,
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
        model_param_v2: Some(param),
        user_id,
        ..Default::default()
    };

    if let Some(model) = stream_model {
        return responses_stream_response(s, request, ak, model, started).into_response();
    }

    let mut ctx = match run_pipeline(&s, request, ak).await {
        Ok(ctx) => ctx,
        Err(e) => return gateway_error(e),
    };
    log_access("responses", &ctx, started);
    let response = response_v2_or_500(ctx.outcome.take(), "responses");
    terminal_response(&ctx, response).await
}

/// Streaming /v1/responses: the vendor's event sequence forwarded verbatim; a
/// synthesized created/delta/completed sequence only for a buffered non-native
/// reply. Live for real vendors, buffered-then-redacted when outbound DLP is on.
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
        native: bool,
        status: String,
        seq: u64,
    }
    impl St {
        fn ensure_created(&mut self) {
            if self.created {
                return;
            }
            self.created = true;
            self.seq += 1;
            self.queue.push_back(Event::default().event("response.created").data(
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
                    error: Some(err), ..
                }) => {
                    let err = *err;
                    self.ensure_created();
                    // the official Responses error event is flat: no nested
                    // `error` object, sequence_number continues the stream
                    self.queue.push_back(
                        Event::default().event("error").data(
                            json!({
                                "type": "error",
                                "code": err.class.code(),
                                "message": err.message,
                                "param": null,
                                "sequence_number": self.seq,
                            })
                            .to_string(),
                        ),
                    );
                    self.queue.push_back(Event::default().data("[DONE]"));
                    true
                }
                Some(mut c) => {
                    if let Some(event) = c.native_event.take() {
                        let Some(kind) = native_event_kind(&event) else {
                            return false;
                        };
                        self.created = true;
                        self.native = true;
                        self.queue
                            .push_back(Event::default().event(kind).data(event.to_string()));
                        return false;
                    }
                    self.ensure_created();
                    if !c.delta.is_empty() {
                        self.seq += 1;
                        self.queue.push_back(
                            Event::default().event("response.output_text.delta").data(
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
                    if !self.native {
                        self.seq += 1;
                        self.queue.push_back(
                            Event::default().event("response.completed").data(
                                json!({"type":"response.completed","response":{
                                    "model": self.model, "status": self.status,
                                    "usage": responses_usage(pt, ct, tt, c.common_usage),
                                }})
                                .to_string(),
                            ),
                        );
                    }
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
            native: false,
            seq: 0,
            status: "completed".to_owned(),
        },
    )
}

/// POST /v1/embeddings (OpenAI-compatible surface)
async fn embeddings(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    ApiJson(mut body): ApiJson<Value>,
) -> Response {
    let started = Instant::now();
    let model = gw_engines::engine::take_string(&mut body, "/model").unwrap_or_default();
    let input = string_or_string_array(body.get_mut("input").map(Value::take));
    if model.is_empty() || input.is_empty() {
        return error_response(400, "model and input are required");
    }
    let typed = TypedParams::Embeddings(EmbeddingParams {
        input,
        dimensions: body["dimensions"].as_i64(),
    });
    family_response(
        &s,
        ak,
        model,
        gw_consts::Protocol::Embeddings,
        typed,
        user_hint(&headers, &body["user"]),
        "embeddings",
        "embeddings",
        started,
    )
    .await
}

/// POST /v1/images/generations (OpenAI-compatible image generation surface)
async fn images_generations(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    ApiJson(mut body): ApiJson<Value>,
) -> Response {
    let started = Instant::now();
    let model = gw_engines::engine::take_string(&mut body, "/model").unwrap_or_default();
    let prompt = gw_engines::engine::take_string(&mut body, "/prompt").unwrap_or_default();
    if model.is_empty() || prompt.is_empty() {
        return error_response(400, "model and prompt are required");
    }
    let typed = TypedParams::Image(ImageParams {
        prompt,
        n: body["n"].as_i64().unwrap_or(1),
        size: body["size"].as_str().map(str::to_owned),
        ..Default::default()
    });
    family_response(
        &s,
        ak,
        model,
        gw_consts::Protocol::Image,
        typed,
        user_hint(&headers, &body["user"]),
        "images",
        "image",
        started,
    )
    .await
}

/// POST /v1/images/edits — same engine as generations; presence of `image`
/// routes to the edit endpoint; the image arrives as base64 JSON.
async fn images_edits(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    ApiJson(mut body): ApiJson<Value>,
) -> Response {
    let started = Instant::now();
    let model = gw_engines::engine::take_string(&mut body, "/model").unwrap_or_default();
    let prompt = gw_engines::engine::take_string(&mut body, "/prompt").unwrap_or_default();
    let image = gw_engines::engine::take_string(&mut body, "/image").unwrap_or_default();
    if model.is_empty() || prompt.is_empty() || image.is_empty() {
        return error_response(400, "model, prompt and image are required");
    }
    let typed = TypedParams::Image(ImageParams {
        prompt,
        n: body["n"].as_i64().unwrap_or(1),
        size: body["size"].as_str().map(str::to_owned),
        image: Some(image),
        mask: gw_engines::engine::take_string(&mut body, "/mask"),
    });
    family_response(
        &s,
        ak,
        model,
        gw_consts::Protocol::Image,
        typed,
        user_hint(&headers, &body["user"]),
        "images_edits",
        "image",
        started,
    )
    .await
}

/// POST /v1/audio/speech (TTS, returns audio bytes; OpenAI-compatible surface)
async fn audio_speech(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    ApiJson(mut body): ApiJson<Value>,
) -> Response {
    let started = Instant::now();
    let model = gw_engines::engine::take_string(&mut body, "/model").unwrap_or_default();
    let input = gw_engines::engine::take_string(&mut body, "/input").unwrap_or_default();
    if model.is_empty() || input.is_empty() {
        return error_response(400, "model and input are required");
    }
    let format = body["response_format"].as_str().unwrap_or("mp3");
    let content_type = match format {
        "wav" => "audio/wav",
        "pcm" => "audio/pcm",
        "opus" => "audio/opus",
        "aac" => "audio/aac",
        "flac" => "audio/flac",
        _ => "audio/mpeg",
    };
    let typed = TypedParams::AudioTts(TtsParams {
        input,
        voice: body["voice"].as_str().map(str::to_owned),
        response_format: Some(format.to_owned()),
    });
    let mut ctx = match run_family(
        &s,
        ak,
        model,
        gw_consts::Protocol::Tts,
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
    if let Some(o) = ctx.outcome.take_if(|o| o.block.block) {
        let response = error_response(400, o.response.message);
        return terminal_response(&ctx, response).await;
    }
    let payload = ctx.outcome.take().and_then(|o| o.response.response_v2);
    let Some(b64) = payload.as_ref().and_then(|v| v["audio_b64"].as_str()) else {
        let response = error_response(500, "tts engine returned no audio");
        return terminal_response(&ctx, response).await;
    };
    let response = match base64::engine::general_purpose::STANDARD.decode(b64) {
        Ok(bytes) => (StatusCode::OK, [("content-type", content_type)], bytes).into_response(),
        Err(e) => error_response(500, format!("bad audio payload: {e}")),
    };
    terminal_response(&ctx, response).await
}

/// POST /v1/audio/transcriptions (STT; JSON carries b64 audio, not multipart).
async fn audio_transcriptions(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    ApiJson(body): ApiJson<Value>,
) -> Response {
    audio_transcribe(s, headers, ak, body, false).await
}

/// POST /v1/audio/translations — the transcriptions shape, translated to
/// English by the upstream (OpenAI translations semantics).
async fn audio_translations(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    ApiJson(body): ApiJson<Value>,
) -> Response {
    audio_transcribe(s, headers, ak, body, true).await
}

/// The shared STT body: transcriptions and translations differ only in the
/// upstream path the `translate` flag selects.
async fn audio_transcribe(
    s: AppState,
    headers: HeaderMap,
    ak: AkInfo,
    mut body: Value,
    translate: bool,
) -> Response {
    let started = Instant::now();
    let model = gw_engines::engine::take_string(&mut body, "/model").unwrap_or_default();
    let audio = gw_engines::engine::take_string(&mut body, "/audio_b64").unwrap_or_default();
    if model.is_empty() || audio.is_empty() {
        return error_response(400, "model and audio_b64 are required");
    }
    let typed = TypedParams::AudioStt(SttParams {
        audio_b64: audio,
        language: body["language"].as_str().map(str::to_owned),
        translate,
    });
    let mut ctx = match run_family(
        &s,
        ak,
        model,
        gw_consts::Protocol::Stt,
        typed,
        vec![],
        user_hint(&headers, &body["user"]),
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(resp) => return resp,
    };
    let surface = if translate {
        "audio_translations"
    } else {
        "audio_transcriptions"
    };
    log_access(surface, &ctx, started);
    let response = match ctx.outcome.take() {
        Some(o) if o.block.block => error_response(400, o.response.message),
        // the vendor body verbatim (text plus usage/segments/language when sent)
        Some(o) => {
            let body = o
                .response
                .response_v2
                .unwrap_or_else(|| json!({ "text": o.response.message }));
            (StatusCode::OK, Json(body)).into_response()
        }
        None => error_response(500, "stt engine returned no outcome"),
    };
    terminal_response(&ctx, response).await
}

/// POST /v1/moderations — OpenAI moderations shape; input may be a string or
/// an array of strings.
async fn moderations(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    ApiJson(mut body): ApiJson<Value>,
) -> Response {
    let started = Instant::now();
    let model = gw_engines::engine::take_string(&mut body, "/model").unwrap_or_default();
    let input = string_or_string_array(body.get_mut("input").map(Value::take));
    if model.is_empty() || input.is_empty() {
        return error_response(400, "model and input are required");
    }
    let typed = TypedParams::Moderation(gw_models::ModerationParams { input });
    family_response(
        &s,
        ak,
        model,
        gw_consts::Protocol::Moderations,
        typed,
        user_hint(&headers, &body["user"]),
        "moderations",
        "moderations",
        started,
    )
    .await
}

/// POST /v1/rerank — Cohere/Jina-compatible: `{model, query, documents, top_n?}`.
async fn rerank(
    State(s): State<AppState>,
    headers: HeaderMap,
    Authed(ak): Authed,
    ApiJson(mut body): ApiJson<Value>,
) -> Response {
    let started = Instant::now();
    let model = gw_engines::engine::take_string(&mut body, "/model").unwrap_or_default();
    let query = gw_engines::engine::take_string(&mut body, "/query").unwrap_or_default();
    let documents = string_or_string_array(body.get_mut("documents").map(Value::take));
    if model.is_empty() || query.is_empty() || documents.is_empty() {
        return error_response(400, "model, query, and documents are required");
    }
    let typed = TypedParams::Rerank(gw_models::RerankParams {
        query,
        documents,
        top_n: body["top_n"].as_i64(),
    });
    family_response(
        &s,
        ak,
        model,
        gw_consts::Protocol::Rerank,
        typed,
        user_hint(&headers, &body["user"]),
        "rerank",
        "rerank",
        started,
    )
    .await
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
    ApiJson(mut body): ApiJson<Value>,
) -> Response {
    let mut model = gw_engines::engine::take_string(&mut body, "/model").unwrap_or_default();
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
    ApiJson(mut body): ApiJson<Value>,
) -> Response {
    let purpose = body["purpose"].as_str().unwrap_or("batch").to_owned();
    let Some(content) = gw_engines::engine::take_string(&mut body, "/file") else {
        return error_response(400, "file content (string) is required");
    };
    if content.is_empty() {
        return error_response(400, "file content must not be empty");
    }
    let f = match s
        .handler
        .state()
        .store
        .file_put(&ak.tenant, &purpose, content)
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

    #[derive(Debug)]
    struct InvalidTtsPayload;

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for InvalidTtsPayload {
        async fn send(
            &self,
            _req: gw_engines::transport::UpstreamRequest,
        ) -> gw_models::GResult<gw_engines::transport::UpstreamResponse> {
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::Json(bytes::Bytes::from_static(
                    br#"{"audio_b64":"%%%"}"#,
                )),
                headers: Default::default(),
            })
        }
    }

    #[derive(Debug)]
    struct DelayedDlpStream {
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl gw_engines::transport::Transport for DelayedDlpStream {
        async fn send(
            &self,
            _req: gw_engines::transport::UpstreamRequest,
        ) -> gw_models::GResult<gw_engines::transport::UpstreamResponse> {
            self.release.notified().await;
            Ok(gw_engines::transport::UpstreamResponse {
                status: 200,
                body: gw_engines::transport::UpstreamBody::Sse(
                    b"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5}}\n\ndata: [DONE]\n\n"
                        .to_vec(),
                ),
                headers: Default::default(),
            })
        }
    }

    #[test]
    fn unsealed_content_passes_plaintext_and_nulls_unopenable() {
        assert_eq!(
            unsealed_content(false, "hello".into()),
            Value::String("hello".into())
        );
        assert_eq!(
            unsealed_content(true, "not-real-ciphertext".into()),
            Value::Null,
            "an unopenable sealed row must never leak ciphertext"
        );
    }

    fn test_app() -> Router {
        let cfg = Arc::new(GatewayConfig::embedded_default().unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        app(AppState::new(
            cfg,
            state,
            Arc::new(gw_engines::MockTransport),
        ))
    }

    fn retained_view_app(transport: SharedTransport) -> (Router, Arc<dyn gw_state::Store>) {
        let yaml = "listen: {host: h, port: 1}\nsecurity: {blocklist: [forbiddenword]}\nmodels: [{name: gpt-5-responses, protocol: responses}, {name: tts-1, protocol: tts}]\naccounts: [{name: a1, provider: openai, protocols: [responses, tts]}]\ntenants: [{name: t1, retention: {content: redacted, days: 1}}]\naccess_keys: [{ak: retained-ak, tenant: t1, owner: user-1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let store = state.store.clone();
        (app(AppState::new(cfg, state, transport)), store)
    }

    async fn retained_terminal(store: &dyn gw_state::Store) -> Value {
        let rows = store
            .content_list_user(Some("t1"), "user-1", 10, true)
            .await
            .unwrap();
        let terminal = rows
            .iter()
            .find(|row| row.kind == "terminal")
            .expect("terminal row");
        serde_json::from_str(&terminal.content).unwrap()
    }

    fn rt(name: &str) -> RtModel {
        RtModel {
            requested: name.to_owned(),
            served: name.to_owned(),
            from_config: true,
        }
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
    async fn retained_non_chat_block_uses_view_status() {
        let (router, store) = retained_view_app(Arc::new(gw_engines::MockTransport));
        let request = Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("content-type", "application/json")
            .header("authorization", "Bearer retained-ak")
            .body(Body::from(
                r#"{"model":"gpt-5-responses","input":"forbiddenword"}"#,
            ))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let terminal = retained_terminal(store.as_ref()).await;
        assert_eq!(terminal["state"], "error");
        assert_eq!(terminal["code"], "validation_exception");
        assert_eq!(terminal["http_status"], 400);
        assert_eq!(terminal["stream_committed"], false);
    }

    #[tokio::test]
    async fn retained_tts_decode_failure_uses_view_status() {
        let (router, store) = retained_view_app(Arc::new(InvalidTtsPayload));
        let request = Request::builder()
            .method("POST")
            .uri("/v1/audio/speech")
            .header("content-type", "application/json")
            .header("authorization", "Bearer retained-ak")
            .body(Body::from(
                r#"{"model":"tts-1","input":"read this","voice":"alloy"}"#,
            ))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let terminal = retained_terminal(store.as_ref()).await;
        assert_eq!(terminal["state"], "error");
        assert_eq!(terminal["code"], "internal_server_exception");
        assert_eq!(terminal["http_status"], 500);
        assert_eq!(terminal["stream_committed"], false);
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

    #[tokio::test]
    async fn dlp_disconnect_before_replay_refunds_and_marks_terminal() {
        let yaml = "listen: {host: h, port: 1}\nsecurity: {dlp_redact: true}\nmodels: [{name: m, protocol: openai-chat}]\naccounts: [{name: a, provider: openai, protocols: [openai-chat]}]\ntenants: [{name: t, retention: {content: redacted, days: 1}}]\naccess_keys: [{ak: k, tenant: t, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let release = Arc::new(tokio::sync::Notify::new());
        let app_state = AppState::new(
            cfg,
            state.clone(),
            Arc::new(DelayedDlpStream {
                release: release.clone(),
            }),
        );
        let ak = state.auth.authenticate("k").await.unwrap();
        let request = GatewayRequest {
            is_online: true,
            stream: true,
            request_id: "req-dlp-disconnect".into(),
            message: vec![ChatMsg::text("user", "hi")],
            model_param_v2: Some(ModelParamV2::with_name(
                gw_consts::Protocol::OpenaiChat,
                "m",
            )),
            ..Default::default()
        };
        drop(spawn_stream_pipeline(
            &app_state,
            request,
            ak,
            "test",
            Instant::now(),
        ));
        release.notify_one();

        let terminal = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let rows = state.store.content_for("req-dlp-disconnect").await.unwrap();
                if let Some(row) = rows.into_iter().find(|row| row.kind == "terminal") {
                    break serde_json::from_str::<Value>(&row.content).unwrap();
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("terminal result");
        assert_eq!(terminal["state"], "client_closed");
        assert_eq!(terminal["stream_committed"], false);
        assert_eq!(state.store.ledger_snapshot(10).await.unwrap().0, 0);
        assert_eq!(state.governance.quota_used("k").await, 0);
    }

    #[tokio::test]
    async fn admin_keys_expose_lifecycle_and_usage_series_is_bucketed() {
        let yaml = "listen: {host: h, port: 1}\nadmin: {token_env: GW_TEST_CP_ADMIN}\nmodels: [{name: m, protocol: openai-chat}]\naccess_keys: [{ak: active, product: p, qps: 1, daily_token_quota: 100}, {ak: banned, product: p, qps: 1, daily_token_quota: 100, banned: true}, {ak: expired, product: p, qps: 1, daily_token_quota: 100, expires_at_epoch_secs: 1}]";
        // SAFETY: this test owns a unique env var name.
        unsafe { std::env::set_var("GW_TEST_CP_ADMIN", "cp-root") };
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let app_state = AppState::new(cfg, state, Arc::new(gw_engines::MockTransport));
        let store = app_state.handler.state().store.clone();
        for (created_at_epoch_secs, tokens, cost_micros) in [(3_700, 10, 25), (7_300, 20, 50)] {
            store
                .ledger_add(&gw_state::BillingRecord {
                    ak: "active".into(),
                    product: "p".into(),
                    tenant: "default".into(),
                    user_id: "alice".into(),
                    request_id: format!("r-{created_at_epoch_secs}"),
                    created_at_epoch_secs,
                    model: "m".into(),
                    served_model: "m".into(),
                    protocol: "openai-chat".into(),
                    account: "a".into(),
                    prompt_tokens: tokens,
                    completion_tokens: 0,
                    total_tokens: tokens,
                    cost_micros,
                    vendor_cost_micros: cost_micros / 2,
                    billed_units: 0,
                    ptu_spillover: false,
                    estimated: false,
                })
                .await
                .unwrap();
        }
        let router = app(app_state);
        let get = |uri: &'static str| {
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("authorization", "Bearer cp-root")
                .body(Body::empty())
                .unwrap()
        };

        let resp = router.clone().oneshot(get("/admin/keys")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let keys = body["keys"].as_array().unwrap();
        let key = |name: &str| keys.iter().find(|k| k["ak"] == name).unwrap();
        assert_eq!(key("active")["status"], "active");
        assert_eq!(key("active")["available"], true);
        assert_eq!(key("banned")["status"], "banned");
        assert_eq!(key("expired")["status"], "expired");

        let resp = router
            .oneshot(get(
                "/admin/usage/series?bucket=hour&since=3600&until=10799&user=alice",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let series = body_json(resp).await;
        let points = series["series"].as_array().unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0]["total_tokens"], 10);
        assert_eq!(points[1]["total_tokens"], 20);
        assert_eq!(points[1]["cost_micros"], 50);
    }

    #[tokio::test]
    async fn tenant_scope_redacts_vendor_cost_and_ak_filter_hides_foreign_keys() {
        let yaml = "listen: {host: h, port: 1}\nadmin: {token_env: GW_TEST_TSCOPE_G}\nmodels: [{name: m, protocol: openai-chat}]\ntenants: [{name: acme, admin_token_env: GW_TEST_TSCOPE_T, models: [m]}, {name: labs, models: [m]}]\naccess_keys: [{ak: k-acme, product: p, tenant: acme, qps: 1, daily_token_quota: 100}, {ak: k-labs, product: p, tenant: labs, qps: 1, daily_token_quota: 100}]";
        // SAFETY: this test owns unique env var names.
        unsafe {
            std::env::set_var("GW_TEST_TSCOPE_G", "root-token");
            std::env::set_var("GW_TEST_TSCOPE_T", "acme-token");
        }
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let app_state = AppState::new(cfg, state, Arc::new(gw_engines::MockTransport));
        let store = app_state.handler.state().store.clone();
        store
            .ledger_add(&gw_state::BillingRecord {
                ak: "k-acme".into(),
                product: "p".into(),
                tenant: "acme".into(),
                user_id: "alice".into(),
                request_id: "r-1".into(),
                created_at_epoch_secs: 3_700,
                model: "m".into(),
                served_model: "m".into(),
                protocol: "openai-chat".into(),
                account: "a".into(),
                prompt_tokens: 10,
                completion_tokens: 0,
                total_tokens: 10,
                cost_micros: 40,
                vendor_cost_micros: 25,
                billed_units: 0,
                ptu_spillover: false,
                estimated: false,
            })
            .await
            .unwrap();
        let router = app(app_state);
        let get = |uri: &'static str, token: &'static str| {
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };

        let resp = router
            .clone()
            .oneshot(get("/admin/usage/users?since=0&until=10000", "acme-token"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let usage = body_json(resp).await;
        assert_eq!(usage["usage"][0]["cost_micros"], 40);
        assert_eq!(
            usage["usage"][0]["vendor_cost_micros"], 0,
            "tenant scope never sees platform vendor cost"
        );
        let resp = router
            .clone()
            .oneshot(get("/admin/usage/users?since=0&until=10000", "root-token"))
            .await
            .unwrap();
        let usage = body_json(resp).await;
        assert_eq!(usage["usage"][0]["vendor_cost_micros"], 25);
        let resp = router
            .clone()
            .oneshot(get("/admin/usage", "acme-token"))
            .await
            .unwrap();
        let usage = body_json(resp).await;
        assert_eq!(usage["usage"][0]["cost_micros"], 40);
        assert_eq!(
            usage["usage"][0]["vendor_cost_micros"], 0,
            "ledger rollup redacts vendor cost under a tenant scope too"
        );
        let resp = router
            .clone()
            .oneshot(get("/admin/usage", "root-token"))
            .await
            .unwrap();
        let usage = body_json(resp).await;
        assert_eq!(usage["usage"][0]["vendor_cost_micros"], 25);
        let resp = router
            .clone()
            .oneshot(get(
                "/admin/usage/series?bucket=hour&since=3600&until=7199",
                "acme-token",
            ))
            .await
            .unwrap();
        let series = body_json(resp).await;
        assert_eq!(series["series"][0]["cost_micros"], 40);
        assert_eq!(series["series"][0]["vendor_cost_micros"], 0);

        let resp = router
            .clone()
            .oneshot(get("/admin/keys?ak=k-acme", "acme-token"))
            .await
            .unwrap();
        let body = body_json(resp).await;
        assert_eq!(body["count"], 1);
        assert_eq!(body["keys"][0]["ak"], "k-acme");
        let resp = router
            .clone()
            .oneshot(get("/admin/keys?ak=k-labs", "acme-token"))
            .await
            .unwrap();
        let body = body_json(resp).await;
        assert_eq!(body["count"], 0, "foreign key invisible through ak filter");
        let resp = router
            .oneshot(get("/admin/keys?ak=k-labs", "root-token"))
            .await
            .unwrap();
        let body = body_json(resp).await;
        assert_eq!(body["count"], 1);
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
    async fn models_status_classifies_and_scopes() {
        let yaml = "listen: {host: h, port: 1}\nadmin: {token_env: GW_TEST_AVAIL_ADMIN}\nmodels: [{name: m-ok, protocol: openai-chat, provider: openai}, {name: m-bad, protocol: openai-chat, provider: downp}, {name: rt-m, protocol: realtime}]\naccounts: [{name: a-up, provider: openai, protocols: ['openai-chat']}, {name: a-down, provider: downp, protocols: ['openai-chat']}]\ntenants: [{name: t1, models: [m-ok], admin_token_env: GW_TEST_AVAIL_T1}]\naccess_keys: [{ak: k1, product: p, qps: 100, daily_token_quota: 100000}]\nstability: {availability_min_samples: 3, failure_threshold: 100}";
        // SAFETY: unique var names for this test; no concurrent reader of them.
        unsafe {
            std::env::set_var("GW_TEST_AVAIL_ADMIN", "root-tok");
            std::env::set_var("GW_TEST_AVAIL_T1", "t1-tok");
        }
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let app_state = AppState::new(cfg, state, Arc::new(gw_engines::MockTransport));
        let avail = app_state.handler.state().avail.clone();
        let router = app(app_state);
        let chat = |model: &str| {
            chat_req(
                Some("k1"),
                &format!(r#"{{"model":"{model}","messages":[{{"role":"user","content":"x"}}]}}"#),
            )
        };
        for _ in 0..4 {
            let ok = router.clone().oneshot(chat("m-ok")).await.unwrap();
            assert_eq!(ok.status(), StatusCode::OK);
            let bad = router.clone().oneshot(chat("m-bad")).await.unwrap();
            assert_eq!(bad.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
        avail.flush().await;

        let status = |token: &'static str| {
            Request::builder()
                .method("GET")
                .uri("/admin/models/status")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };
        let resp = router.clone().oneshot(status("root-tok")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let j = body_json(resp).await;
        let rows = j["models"].as_array().unwrap();
        let of = |name: &str| {
            rows.iter()
                .find(|r| r["model"] == name)
                .unwrap_or_else(|| panic!("row for {name}"))
                .clone()
        };
        assert_eq!(of("m-ok")["state"], "available");
        assert_eq!(of("m-ok")["requests"], 4);
        assert_eq!(of("m-bad")["state"], "unavailable");
        assert_eq!(of("m-bad")["errors"], 4);
        let rt_row = rows
            .iter()
            .find(|r| r["model"] == "rt-m")
            .expect("realtime models are listed (turn-sampled)");
        assert_eq!(rt_row["state"], "no_data", "no turns billed yet");

        let resp = router.clone().oneshot(status("t1-tok")).await.unwrap();
        let j = body_json(resp).await;
        let names: Vec<_> = j["models"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["model"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(names, ["m-ok"], "tenant admin sees only entitled models");
    }

    #[tokio::test]
    async fn sustained_pool_exhaustion_converges_to_unavailable() {
        let yaml = "listen: {host: h, port: 1}\nadmin: {token_env: GW_TEST_OUT_ADMIN}\nmodels: [{name: m-out, protocol: openai-chat, provider: downp}]\naccounts: [{name: a-down, provider: downp, protocols: ['openai-chat']}]\naccess_keys: [{ak: k1, product: p, qps: 100, daily_token_quota: 100000}]\nstability: {failure_threshold: 2, cooldown_seconds: 300, availability_min_samples: 5}";
        // SAFETY: unique var name for this test; no concurrent reader of it.
        unsafe {
            std::env::set_var("GW_TEST_OUT_ADMIN", "out-tok");
        }
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let app_state = AppState::new(cfg, state, Arc::new(gw_engines::MockTransport));
        let avail = app_state.handler.state().avail.clone();
        let router = app(app_state);
        for _ in 0..6 {
            let resp = router
                .clone()
                .oneshot(chat_req(
                    Some("k1"),
                    r#"{"model":"m-out","messages":[{"role":"user","content":"x"}]}"#,
                ))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
        avail.flush().await;
        let resp = router
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/admin/models/status")
                    .header("authorization", "Bearer out-tok")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let j = body_json(resp).await;
        let row = &j["models"][0];
        assert_eq!(row["model"], "m-out");
        assert_eq!(row["errors"], 6, "cooldown-era 503s sample too");
        assert_eq!(row["state"], "unavailable");
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
    async fn content_list_is_tenant_scoped_metadata_newest_first() {
        let yaml = "listen: {host: h, port: 1}\nadmin: {token_env: GW_TEST_CLIST_ADMIN}\nmodels: [{name: gpt-4o, protocol: openai-chat}]\ntenants: [{name: t1, admin_token_env: GW_TEST_CLIST_T1}, {name: t2, admin_token_env: GW_TEST_CLIST_T2}]\naccess_keys: [{ak: k1, tenant: t1, product: p, qps: 10, daily_token_quota: 1000}]";
        // SAFETY: unique var names for this test; no concurrent reader of them.
        unsafe {
            std::env::set_var("GW_TEST_CLIST_ADMIN", "root-tok");
            std::env::set_var("GW_TEST_CLIST_T1", "t1-tok");
            std::env::set_var("GW_TEST_CLIST_T2", "t2-tok");
        }
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let app_state = AppState::new(cfg, state, Arc::new(gw_engines::MockTransport));
        let store = app_state.handler.state().store.clone();
        let rec = |req: &str, user: &str, tenant: &str, at: i64| gw_state::ContentRecord {
            created_at_epoch_secs: at,
            request_id: req.into(),
            ak: "k1".into(),
            user_id: user.into(),
            tenant: tenant.into(),
            kind: "prompt".into(),
            content: "hello".into(),
            sealed: false,
            expires_at_epoch_secs: 0,
        };
        store
            .content_add(&rec("r1", "u1", "t1", 100))
            .await
            .unwrap();
        store
            .content_add(&rec("r2", "u1", "t1", 200))
            .await
            .unwrap();
        store
            .content_add(&rec("rx", "u2", "t1", 250))
            .await
            .unwrap();
        store
            .content_add(&rec("r3", "u1", "t2", 300))
            .await
            .unwrap();
        let mut terminal = rec("rt", "u1", "t1", 400);
        terminal.kind = "terminal".into();
        terminal.content =
            r#"{"state":"success","http_status":200,"stream_committed":false}"#.into();
        store.content_terminal_put(&terminal).await.unwrap();
        let router = app(app_state);

        let list = |uri: &str, token: &str| {
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };
        let ids = |j: &Value| -> Vec<String> {
            j["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["request_id"].as_str().unwrap().to_owned())
                .collect()
        };
        let resp = router
            .clone()
            .oneshot(list("/admin/audit/content?user=u1", "root-tok"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let j = body_json(resp).await;
        assert_eq!(
            ids(&j),
            ["rt", "r3", "r2", "r1"],
            "global scope: the user's rows across tenants, newest first"
        );
        assert!(
            j["rows"][0].get("content").is_none(),
            "metadata only: no content bodies"
        );
        assert_eq!(j["rows"][0]["user_id"], "u1");
        assert_eq!(j["rows"][0]["kind"], "terminal");
        assert_eq!(j["rows"][0]["created_at_epoch_secs"], 400);

        let resp = router
            .clone()
            .oneshot(list("/admin/audit/content?user=u1", "t1-tok"))
            .await
            .unwrap();
        assert_eq!(
            ids(&body_json(resp).await),
            ["rt", "r2", "r1"],
            "tenant admin sees only its own tenant's rows"
        );

        let resp = router
            .clone()
            .oneshot(list(
                "/admin/audit/content?user=u1&limit=1&include=bodies",
                "t1-tok",
            ))
            .await
            .unwrap();
        let j = body_json(resp).await;
        assert_eq!(j["rows"][0]["kind"], "terminal");
        let terminal: Value =
            serde_json::from_str(j["rows"][0]["content"].as_str().unwrap()).expect("terminal json");
        assert_eq!(
            terminal["state"], "success",
            "the owner-scoped archive exposes the terminal body"
        );

        let resp = router
            .clone()
            .oneshot(list("/admin/audit/content?user=u2", "t2-tok"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            ids(&body_json(resp).await).is_empty(),
            "foreign tenant token sees nothing"
        );

        let resp = router
            .clone()
            .oneshot(list("/admin/audit/content?user=u1&limit=1", "root-tok"))
            .await
            .unwrap();
        assert_eq!(ids(&body_json(resp).await), ["rt"], "limit respected");

        let resp = router
            .oneshot(list("/admin/audit/content", "root-tok"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "user is required");
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
        assert_eq!(
            entries.len(),
            3,
            "prompt, response, and terminal rows read back"
        );
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
        let terminal = entries
            .iter()
            .find(|entry| entry["kind"] == "terminal")
            .expect("terminal entry");
        let terminal: Value = serde_json::from_str(terminal["content"].as_str().unwrap()).unwrap();
        assert_eq!(terminal["state"], "success");
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

        let mut frame = json!({"type":"input_text","text":"hello there"});
        assert_eq!(
            rt_inbound_policy(&app, &ak, "", &mut frame).await,
            Err("blocked by moderator".to_owned())
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

    #[derive(Debug)]
    struct FrameMaskModerator;

    #[async_trait::async_trait]
    impl gw_handler::moderation::Moderator for FrameMaskModerator {
        async fn review(&self, text: &str) -> Result<gw_handler::moderation::Verdict, String> {
            match text.find("secret") {
                Some(i) => Ok(gw_handler::moderation::Verdict::Mask(
                    std::iter::once(i..i + "secret".len()).collect(),
                )),
                None => Ok(gw_handler::moderation::Verdict::Allow),
            }
        }
    }

    #[tokio::test]
    async fn realtime_moderation_masks_the_frame() {
        let yaml = "listen: {host: h, port: 1}\nsecurity: {moderate: true}\nmodels: [{name: rt, protocol: realtime}]\naccess_keys: [{ak: k1, product: p, qps: 10, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let handler = OnlineHandler::new(
            gw_state::SharedConfig::new(cfg, state),
            Arc::new(gw_engines::MockTransport),
        )
        .with_moderator(Arc::new(FrameMaskModerator));
        let offline = OfflineHandler::new(handler.clone());
        let app = AppState {
            handler,
            offline,
            loader: None,
            config_store: None,
        };
        let ak = app.handler.state().auth.authenticate("k1").await.unwrap();
        let mut frame = json!({"type":"input_text","text":"tell secret now"});
        assert_eq!(rt_inbound_policy(&app, &ak, "", &mut frame).await, Ok(0));
        assert_eq!(
            frame["text"], "tell [MASKED] now",
            "the mask lands in the frame before it is forwarded"
        );
        let events = app
            .handler
            .state()
            .store
            .security_events(None, 10)
            .await
            .unwrap();
        assert!(
            events
                .iter()
                .any(|e| e.rule == "moderation" && e.action == "mask")
        );
    }

    #[tokio::test]
    async fn moderations_rerank_and_translations_roundtrip() {
        let post = |path: &str, body: &str| {
            Request::builder()
                .method("POST")
                .uri(path.to_owned())
                .header("content-type", "application/json")
                .header("authorization", "Bearer ak-demo-123")
                .body(Body::from(body.to_owned()))
                .unwrap()
        };
        let resp = test_app()
            .oneshot(post(
                "/v1/moderations",
                r#"{"model":"text-moderation","input":["fine text","really unsafe text"]}"#,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let j = body_json(resp).await;
        assert_eq!(j["results"][0]["flagged"], false);
        assert_eq!(j["results"][1]["flagged"], true);

        let resp = test_app()
            .oneshot(post(
                "/v1/rerank",
                r#"{"model":"rerank-mini","query":"rust gateway","documents":["a rust gateway","cooking pasta"],"top_n":1}"#,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let j = body_json(resp).await;
        let results = j["results"].as_array().unwrap();
        assert_eq!(results.len(), 1, "top_n honored");
        assert_eq!(results[0]["index"], 0, "the matching document ranks first");

        let resp = test_app()
            .oneshot(post(
                "/v1/audio/translations",
                r#"{"model":"whisper-1","audio_b64":"AAAA"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let j = body_json(resp).await;
        assert!(
            j["text"].as_str().unwrap().contains("translated"),
            "the translations path reached the translations mock: {j}"
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

        let a1 = realtime_gate(&s, &ak, &rt("gpt-4o"), "")
            .await
            .expect("admit");
        assert_eq!(used().await, REALTIME_TURN_RESERVE, "reserved up front");

        bill_realtime_turn(
            &a1,
            &rt("gpt-4o"),
            gw_consts::Protocol::Realtime,
            "acc",
            30,
            70,
            false,
        )
        .await;
        assert_eq!(used().await, 100, "settled to actual (30 + 70)");

        let a2 = realtime_gate(&s, &ak, &rt("gpt-4o"), "")
            .await
            .expect("admit");
        assert_eq!(used().await, 100 + REALTIME_TURN_RESERVE);
        gov().quota_settle(&a2.ak.ak, -a2.reserved, a2.at).await;
        assert_eq!(used().await, 100, "dropped turn refunded whole");

        let a3 = realtime_gate(&s, &ak, &rt("gpt-4o"), "")
            .await
            .expect("admit");
        assert_eq!(used().await, 100 + REALTIME_TURN_RESERVE);
        let ledger_before = s.handler.state().store.ledger_snapshot(1).await.unwrap().0;
        bill_realtime_turn(
            &a3,
            &rt("gpt-4o"),
            gw_consts::Protocol::Realtime,
            "acc",
            0,
            0,
            false,
        )
        .await;
        assert_eq!(used().await, 100, "zero-usage turn refunds its reserve");
        let ledger_after = s.handler.state().store.ledger_snapshot(1).await.unwrap().0;
        assert_eq!(
            ledger_before, ledger_after,
            "zero-usage turn writes no ledger row"
        );

        gov().quota_consume(&ak.ak, ak.daily_token_quota).await;
        let denied = realtime_gate(&s, &ak, &rt("gpt-4o"), "")
            .await
            .err()
            .expect("exhausted daily quota must deny");
        assert_eq!(denied.0, ErrClass::ServiceQuotaExceeded);
    }

    #[tokio::test]
    async fn realtime_billing_applies_token_rate() {
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: rt-m, protocol: realtime, input_price_per_1k_micros: 1000, output_price_per_1k_micros: 1000, token_rate: {completion: 0.5}}]\naccounts: [{name: a1, provider: openai, protocols: ['realtime']}]\naccess_keys: [{ak: k1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let s = AppState::new(cfg, state, Arc::new(gw_engines::MockTransport));
        let ak = s.handler.state().auth.authenticate("k1").await.unwrap();
        let a = realtime_gate(&s, &ak, &rt("rt-m"), "")
            .await
            .expect("admit");
        bill_realtime_turn(
            &a,
            &rt("rt-m"),
            gw_consts::Protocol::Realtime,
            "acc",
            100,
            100,
            false,
        )
        .await;
        let (_, ledger) = s
            .handler
            .state()
            .store
            .ledger_snapshot(usize::MAX)
            .await
            .unwrap();
        let rec = &ledger[0];
        assert_eq!(rec.prompt_tokens, 100);
        assert_eq!(rec.completion_tokens, 100);
        assert_eq!(rec.total_tokens, 150, "100 + 100*0.5 weighted");
        assert_eq!(rec.cost_micros, 150, "billable 100+50 at 1000/1k each");
        assert_eq!(
            s.handler.state().governance.quota_used(&ak.ak).await,
            150,
            "quota settles the weighted total"
        );
    }

    #[tokio::test]
    async fn realtime_turn_denied_when_served_model_reloaded_away() {
        let yaml = "listen: {host: h, port: 1}\nmodels: [{name: rt-pub, protocol: realtime, variants: [{model: rt-canary, weight: 1}]}, {name: rt-canary, protocol: realtime}]\naccounts: [{name: a1, provider: openai, protocols: ['realtime']}]\naccess_keys: [{ak: k1, product: p, qps: 100, daily_token_quota: 100000}]";
        let cfg = Arc::new(GatewayConfig::from_yaml(yaml).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let s = AppState::new(cfg, state, Arc::new(gw_engines::MockTransport));
        let ak = s.handler.state().auth.authenticate("k1").await.unwrap();
        let pinned = RtModel {
            requested: "rt-pub".into(),
            served: "rt-canary".into(),
            from_config: true,
        };
        assert!(realtime_gate(&s, &ak, &pinned, "").await.is_ok());
        let without = "listen: {host: h, port: 1}\nmodels: [{name: rt-pub, protocol: realtime}]\naccounts: [{name: a1, provider: openai, protocols: ['realtime']}]\naccess_keys: [{ak: k1, product: p, qps: 100, daily_token_quota: 100000}]";
        s.handler
            .reload(GatewayConfig::from_yaml(without).unwrap())
            .await
            .unwrap();
        let denied = realtime_gate(&s, &ak, &pinned, "").await.err();
        assert!(
            denied
                .as_ref()
                .is_some_and(|(class, e)| *class == ErrClass::ResourceNotFound
                    && e.contains("no longer configured")),
            "a pinned variant removed by reload must deny the turn, not bill zero: {denied:?}"
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

        let a1 = realtime_gate(&s, &ak, &rt("gpt-4o"), "")
            .await
            .expect("first admits");
        assert_eq!(a1.tpm_reserved, Some(REALTIME_TURN_RESERVE));
        let daily_before = gov.quota_used(&ak.ak).await;

        assert!(
            realtime_gate(&s, &ak, &rt("gpt-4o"), "").await.is_err(),
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

        let admit = realtime_gate(&s, &ak, &rt("rt"), "").await.expect("admit");
        s.handler
            .reload(GatewayConfig::from_yaml(&price(2_000_000)).unwrap())
            .await
            .unwrap();
        bill_realtime_turn(
            &admit,
            &rt("rt"),
            gw_consts::Protocol::Realtime,
            "acc",
            100,
            100,
            false,
        )
        .await;

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
        let admit = realtime_gate(&s, &shared, &rt("rt"), "alice")
            .await
            .expect("admit");
        assert_eq!(admit.user, "alice", "ownerless key attributes to the hint");
        bill_realtime_turn(
            &admit,
            &rt("rt"),
            gw_consts::Protocol::Realtime,
            "acc",
            40,
            60,
            false,
        )
        .await;

        let owned = s
            .handler
            .state()
            .auth
            .authenticate("k-owned")
            .await
            .unwrap();
        let admit = realtime_gate(&s, &owned, &rt("rt"), "mallory")
            .await
            .expect("admit");
        assert_eq!(
            admit.user, "bob",
            "owner is authoritative over a spoofed hint"
        );
        bill_realtime_turn(
            &admit,
            &rt("rt"),
            gw_consts::Protocol::Realtime,
            "acc",
            10,
            20,
            false,
        )
        .await;

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
    fn partial_stream_estimate_covers_visible_output_only() {
        let text = gw_engines::StreamChunk {
            delta: "hello".into(),
            ..Default::default()
        };
        let tool = gw_engines::StreamChunk {
            tool_calls: Some(json!([{"function":{"name":"lookup","arguments":"{}"}}])),
            ..Default::default()
        };
        let native = gw_engines::StreamChunk {
            native_event: Some(json!({
                "type":"content_block_delta",
                "delta":{"type":"text_delta","text":"answer"}
            })),
            ..Default::default()
        };
        assert!(stream_chunk_output_tokens(&text) > 0);
        assert!(stream_chunk_output_tokens(&tool) > 0);
        assert!(stream_chunk_output_tokens(&native) > 0);
        assert_eq!(stream_chunk_output_tokens(&Default::default()), 0);
    }

    #[test]
    fn finish_reason_mapping_both_directions() {
        assert_eq!(finish_openai("end_turn".into()), "stop");
        assert_eq!(finish_openai("stop_sequence".into()), "stop");
        assert_eq!(finish_openai(String::new()), "stop");
        assert_eq!(finish_openai("max_tokens".into()), "length");
        assert_eq!(finish_openai("tool_use".into()), "tool_calls");
        assert_eq!(finish_openai("refusal".into()), "refusal");

        assert_eq!(finish_anthropic("stop".into()), "end_turn");
        assert_eq!(finish_anthropic(String::new()), "end_turn");
        assert_eq!(finish_anthropic("length".into()), "max_tokens");
        assert_eq!(finish_anthropic("tool_calls".into()), "tool_use");
        assert_eq!(finish_anthropic("content_filter".into()), "content_filter");

        for (o, a) in [
            ("stop", "end_turn"),
            ("length", "max_tokens"),
            ("tool_calls", "tool_use"),
        ] {
            assert_eq!(finish_anthropic(o.into()), a, "openai→anthropic {o}");
            assert_eq!(finish_openai(a.into()), o, "anthropic→openai {a}");
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
