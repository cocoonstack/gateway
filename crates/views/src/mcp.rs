//! `/mcp/{server}`: the Model Context Protocol proxy. A key reaches only the
//! servers it is entitled to and the tools its allowlist names; `tools/list`
//! is filtered to that allowlist, a tenant under `security.moderate` has the
//! results of `tools/call`, `resources/read` and `prompts/get` reviewed before
//! they leave, sessions are bound to the key that opened them, and every
//! call, denial and intervention is a security event.

use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::StreamExt as _;
use gw_config::{McpServerConf, SecurityConf};
use gw_handler::{RtModeration, plugins};
use gw_state::{AkInfo, GatewayState, SecurityEvent, Snapshot, admission};
use serde_json::{Value, json};

use crate::{AppState, authenticate, error_response};

const FORWARDED_HEADERS: [&str; 5] = [
    "accept",
    "content-type",
    "mcp-session-id",
    "mcp-protocol-version",
    "last-event-id",
];
const RETURNED_HEADERS: [&str; 2] = ["content-type", "mcp-session-id"];
/// Methods whose results carry prose an agent reads; reviewed under `security.moderate`.
const REVIEWED_METHODS: [&str; 3] = ["tools/call", "resources/read", "prompts/get"];
/// Result fields that carry base64 binary, never prose: skipped so the review
/// neither reads nor rewrites an image, audio clip or blob resource.
const OPAQUE_KEYS: [&str; 2] = ["blob", "data"];
const JSONRPC_TOOL_DENIED: i64 = -32000;
const JSONRPC_RESULT_BLOCKED: i64 = -32001;
const UNREVIEWABLE: &str = "the result could not be reviewed";

/// The JSON-RPC envelope of one POST, as far as the proxy needs it.
#[derive(Default)]
struct Call {
    method: String,
    id: Value,
    tool: Option<String>,
}

/// One piece of a buffered reply: a JSON-RPC message the proxy may rewrite, a
/// `data` payload it could not parse, or framing it passes through.
enum Segment {
    Message(Value),
    Opaque,
    Raw(String),
}

pub(crate) async fn proxy(
    State(s): State<AppState>,
    Path(server): Path<String>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let ak = match authenticate(&s, &headers).await {
        Ok(ak) => ak,
        Err((status, msg)) => return error_response(status, msg),
    };
    let snap = s.handler.config.load();
    // an unentitled server answers like an unknown one, so names cannot be probed
    let Some(conf) = snap
        .cfg
        .find_mcp_server(&server)
        .filter(|_| ak.mcp.reaches(&server))
    else {
        return error_response(404, format!("unknown mcp server: {server}"));
    };
    let gov = snap.state.governance.as_ref();
    if let Err(e) = admission::check_tenant_rate(gov, &snap.cfg, &ak.tenant).await {
        return error_response(429, e);
    }
    if let Err(e) = admission::check_ak_rate(gov, &ak).await {
        return error_response(429, e);
    }
    let session = headers.get("mcp-session-id").and_then(|v| v.to_str().ok());
    if let Some(sid) = session
        && s.mcp_sessions
            .get(sid)
            .is_some_and(|owner| owner != ak.ak_id)
    {
        return error_response(404, "unknown mcp session");
    }
    let sec = snap.cfg.security_for(&ak.tenant);
    // a listen stream carries server-pushed content the proxy cannot review
    if method == Method::GET && sec.moderate {
        return error_response(
            403,
            "mcp listen streams are unavailable under content review",
        );
    }
    let call = if method == Method::POST {
        match parse_call(&body) {
            Ok(call) => call,
            Err(msg) => return error_response(400, msg),
        }
    } else {
        Call::default()
    };
    let allowed = ak.mcp.allowed_tools(&server);
    if let Some(tool) = call.tool.as_deref()
        && allowed.is_some_and(|list| !list.iter().any(|t| t == tool))
    {
        audit(
            &snap.state,
            &ak,
            format!("mcp:{server}"),
            format!("deny:{tool}"),
            1,
        )
        .await;
        count(&server, "tools/call", "denied");
        return jsonrpc_error(
            call.id,
            JSONRPC_TOOL_DENIED,
            format!("tool `{tool}` is not permitted for this key"),
        );
    }
    let stream_guard = if method == Method::GET {
        let Some(guard) = snap
            .state
            .streams
            .open(&ak.ak, snap.cfg.max_live_streams_per_key)
        else {
            return error_response(429, "too many open mcp streams for this key");
        };
        Some(guard)
    } else {
        None
    };
    let label = method_label(&call.method);
    if let Some(tool) = call.tool.as_deref() {
        audit(
            &snap.state,
            &ak,
            format!("mcp:{server}"),
            format!("call:{tool}"),
            1,
        )
        .await;
    }
    // a reviewed tenant gets no stream resumption: a replayed result would skip the review
    let resumable = !sec.moderate;
    let mut sent = send(&s, conf, &method, &headers, &body, resumable).await;
    // a token the server stopped honoring is fetched anew once
    if conf.oauth.is_some() && matches!(&sent, Ok(r) if r.status() == StatusCode::UNAUTHORIZED) {
        s.mcp_auth.invalidate(&conf.name);
        sent = send(&s, conf, &method, &headers, &body, resumable).await;
    }
    let reply = match sent {
        Ok(reply) => reply,
        Err(e) => {
            tracing::warn!(server, error = %e, "mcp upstream request failed");
            count(&server, label, "upstream_error");
            return error_response(502, format!("mcp server `{server}` is unavailable"));
        }
    };
    let status = reply.status();
    count(&server, label, crate::status_label(status));
    let mut out = HeaderMap::new();
    for name in RETURNED_HEADERS {
        if let Some(v) = reply.headers().get(name) {
            out.insert(name, v.clone());
        }
    }
    if let Some(sid) = out.get("mcp-session-id").and_then(|v| v.to_str().ok())
        && s.mcp_sessions.get(sid).is_none()
    {
        s.mcp_sessions.insert(sid.to_owned(), ak.ak_id.clone());
    }
    let sse = out
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("text/event-stream"));
    let filtered = call.method == "tools/list" && allowed.is_some();
    let reviewed = sec.moderate && REVIEWED_METHODS.contains(&call.method.as_str());
    if !filtered && !reviewed {
        let stream = reply.bytes_stream().map(move |chunk| {
            let _held = &stream_guard;
            chunk
        });
        return (status, out, Body::from_stream(stream)).into_response();
    }
    let bytes = match read_capped(reply, conf.max_reply_bytes).await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::warn!(server, error = %e, "mcp reply not read");
            count(&server, label, "reply_unreadable");
            return error_response(
                502,
                format!("mcp server `{server}` reply could not be read"),
            );
        }
    };
    let body = match allowed {
        Some(list) if filtered => match filter_tool_list(&bytes, sse, list) {
            Some(body) => body,
            None => {
                count(&server, label, "reply_unreadable");
                return error_response(
                    502,
                    format!("mcp server `{server}` reply could not be filtered"),
                );
            }
        },
        _ => moderate_result(&s, &snap, sec, &ak, &server, label, call.id, &bytes, sse).await,
    };
    (status, out, Body::from(body)).into_response()
}

async fn send(
    s: &AppState,
    conf: &McpServerConf,
    method: &Method,
    headers: &HeaderMap,
    body: &Bytes,
    resumable: bool,
) -> Result<reqwest::Response, String> {
    let bearer = s
        .mcp_auth
        .bearer(&s.mcp, conf)
        .await
        .map_err(|e| format!("credentials: {e}"))?;
    let mut upstream = s.mcp.request(method.clone(), &conf.endpoint);
    if *method != Method::GET {
        upstream = upstream.timeout(Duration::from_secs(conf.timeout_seconds));
    }
    for name in FORWARDED_HEADERS {
        if let Some(v) = headers.get(name)
            && (resumable || name != "last-event-id")
        {
            upstream = upstream.header(name, v);
        }
    }
    if let Some(token) = bearer {
        upstream = upstream.bearer_auth(token);
    }
    if *method == Method::POST {
        upstream = upstream.body(body.clone());
    }
    upstream.send().await.map_err(|e| e.to_string())
}

/// Collect a reply body up to `cap` bytes; a larger one is refused rather than held.
async fn read_capped(reply: reqwest::Response, cap: usize) -> Result<Vec<u8>, String> {
    let mut stream = reply.bytes_stream();
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| e.to_string())?;
        if out.len() + chunk.len() > cap {
            return Err(format!("reply exceeds max_reply_bytes ({cap})"));
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

fn parse_call(body: &[u8]) -> Result<Call, String> {
    let v: Value =
        serde_json::from_slice(body).map_err(|e| format!("body is not JSON-RPC: {e}"))?;
    let Value::Object(mut obj) = v else {
        return Err("JSON-RPC batches are not supported; send one message per request".to_owned());
    };
    let method = match obj.remove("method") {
        Some(Value::String(method)) => method,
        _ => String::new(),
    };
    let tool = match obj.get_mut("params").and_then(|p| p.get_mut("name")) {
        Some(Value::String(name)) if method == "tools/call" => Some(std::mem::take(name)),
        _ if method == "tools/call" => {
            return Err("tools/call needs a string params.name".to_owned());
        }
        _ => None,
    };
    Ok(Call {
        method,
        id: obj.remove("id").unwrap_or(Value::Null),
        tool,
    })
}

/// Keep only the allowlisted tools in every `tools/list` result; `None` when a message could not be parsed.
fn filter_tool_list(bytes: &[u8], sse: bool, allowed: &[String]) -> Option<Vec<u8>> {
    let mut segments = parse_segments(bytes, sse);
    if segments.iter().any(|seg| matches!(seg, Segment::Opaque)) {
        return None;
    }
    for tools in segments.iter_mut().filter_map(|seg| match seg {
        Segment::Message(msg) => msg
            .get_mut("result")
            .and_then(|r| r.get_mut("tools"))
            .and_then(Value::as_array_mut),
        _ => None,
    }) {
        tools.retain(|t| {
            t["name"]
                .as_str()
                .is_some_and(|n| allowed.iter().any(|a| a == n))
        });
    }
    Some(serialize_segments(segments, sse, bytes.len()))
}

/// Review a reply's prose: deny → one JSON-RPC error replaces the reply, mask → in-place rewrite, both recorded.
#[allow(clippy::too_many_arguments)]
async fn moderate_result(
    s: &AppState,
    snap: &Snapshot,
    sec: &SecurityConf,
    ak: &AkInfo,
    server: &str,
    label: &'static str,
    id: Value,
    bytes: &[u8],
    sse: bool,
) -> Vec<u8> {
    let mut segments = parse_segments(bytes, sse);
    if segments.iter().any(|seg| matches!(seg, Segment::Opaque)) {
        return blocked(snap, ak, server, label, id, UNREVIEWABLE, sse).await;
    }
    let texts: Vec<&mut String> = segments.iter_mut().flat_map(review_slots).collect();
    let review = plugins::slot_text(texts.iter().map(|s| s.as_str()));
    if review.is_empty() {
        return serialize_segments(segments, sse, bytes.len());
    }
    match s.handler.moderate_rt(sec, &review).await {
        RtModeration::Allow => {}
        RtModeration::Mask(spans) => {
            let hits = plugins::apply_mask_slots(&spans, texts);
            if hits == 0 {
                return blocked(snap, ak, server, label, id, UNREVIEWABLE, sse).await;
            }
            audit(&snap.state, ak, "moderation", "mask", hits as i64).await;
            count(server, label, "masked");
        }
        RtModeration::Deny(reason) => {
            return blocked(snap, ak, server, label, id, &reason, sse).await;
        }
    }
    serialize_segments(segments, sse, bytes.len())
}

/// The whole reply becomes one JSON-RPC error, so nothing unreviewed leaves.
async fn blocked(
    snap: &Snapshot,
    ak: &AkInfo,
    server: &str,
    label: &'static str,
    id: Value,
    reason: &str,
    sse: bool,
) -> Vec<u8> {
    audit(&snap.state, ak, "moderation", "block", 1).await;
    count(server, label, "blocked");
    let error = jsonrpc_error_value(id, JSONRPC_RESULT_BLOCKED, reason);
    let mut segments = vec![Segment::Message(error)];
    if sse {
        segments.push(Segment::Raw("\n".to_owned()));
    }
    serialize_segments(segments, sse, 0)
}

/// Every prose slot of a message: string leaves under `result` (a notification's `params`), identifiers and binary skipped.
fn review_slots(seg: &mut Segment) -> Vec<&mut String> {
    let mut slots = Vec::new();
    if let Segment::Message(msg) = seg {
        let root = if msg.get("result").is_some() {
            msg.get_mut("result")
        } else if msg.get("error").is_some() {
            msg.get_mut("error")
        } else {
            msg.get_mut("params")
        };
        if let Some(root) = root {
            collect_prose(root, &mut slots);
        }
    }
    slots
}

fn collect_prose<'a>(v: &'a mut Value, out: &mut Vec<&'a mut String>) {
    match v {
        Value::String(s) => out.push(s),
        Value::Array(items) => items.iter_mut().for_each(|x| collect_prose(x, out)),
        Value::Object(map) => map
            .iter_mut()
            .filter(|(k, _)| !OPAQUE_KEYS.contains(&k.as_str()))
            .for_each(|(_, x)| collect_prose(x, out)),
        _ => {}
    }
}

/// A bare JSON body is one message; an event stream is its events, each event's `data` lines joined by newlines, framing kept verbatim.
fn parse_segments(bytes: &[u8], sse: bool) -> Vec<Segment> {
    let text = String::from_utf8_lossy(bytes);
    let text = text.strip_prefix('\u{feff}').unwrap_or(&text);
    if !sse {
        return vec![match serde_json::from_str(text) {
            Ok(msg) => Segment::Message(msg),
            Err(_) => Segment::Opaque,
        }];
    }
    let mut segments = Vec::new();
    let mut data: Option<String> = None;
    let flush = |data: &mut Option<String>, segments: &mut Vec<Segment>| {
        if let Some(payload) = data.take() {
            segments.push(match serde_json::from_str(&payload) {
                Ok(msg) => Segment::Message(msg),
                Err(_) => Segment::Opaque,
            });
        }
    };
    for line in text.split_inclusive('\n') {
        let field = line.trim_end_matches(['\r', '\n']);
        if let Some(d) = field.strip_prefix("data:") {
            let d = d.strip_prefix(' ').unwrap_or(d);
            match &mut data {
                Some(acc) => {
                    acc.push('\n');
                    acc.push_str(d);
                }
                None => data = Some(d.to_owned()),
            }
            continue;
        }
        if field.is_empty() {
            flush(&mut data, &mut segments);
        }
        segments.push(Segment::Raw(line.to_owned()));
    }
    flush(&mut data, &mut segments);
    segments
}

fn serialize_segments(segments: Vec<Segment>, sse: bool, hint: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(hint);
    for seg in segments {
        match seg {
            Segment::Raw(s) => out.extend_from_slice(s.as_bytes()),
            Segment::Opaque => {}
            Segment::Message(msg) => {
                if sse {
                    out.extend_from_slice(b"data: ");
                }
                out.extend(serde_json::to_vec(&msg).unwrap_or_default());
                if sse {
                    out.push(b'\n');
                }
            }
        }
    }
    out
}

fn method_label(method: &str) -> &'static str {
    match method {
        "initialize" => "initialize",
        "tools/list" => "tools/list",
        "tools/call" => "tools/call",
        "resources/list" | "resources/read" | "resources/templates/list" => "resources",
        "prompts/list" | "prompts/get" => "prompts",
        "ping" => "ping",
        "" => "stream",
        _ => "other",
    }
}

fn count(server: &str, method: &'static str, result: impl Into<metrics::SharedString>) {
    metrics::counter!(
        "gateway_mcp_requests_total",
        "server" => server.to_owned(),
        "method" => method,
        "result" => result.into(),
    )
    .increment(1);
}

async fn audit(
    state: &GatewayState,
    ak: &AkInfo,
    rule: impl Into<String>,
    action: impl Into<String>,
    hits: i64,
) {
    SecurityEvent {
        created_at_epoch_secs: gw_state::epoch_secs(),
        request_id: gw_handler::new_request_id(),
        ak: ak.ak.clone(),
        user_id: ak.owner.clone().unwrap_or_default(),
        tenant: ak.tenant.clone(),
        surface: "mcp".to_owned(),
        rule: rule.into(),
        action: action.into(),
        hits,
    }
    .record(state.store.as_ref())
    .await;
}

fn jsonrpc_error(id: Value, code: i64, message: String) -> Response {
    (
        StatusCode::OK,
        axum::Json(jsonrpc_error_value(id, code, &message)),
    )
        .into_response()
}

fn jsonrpc_error_value(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::Router;
    use axum::routing::post;
    use gw_config::GatewayConfig;
    use gw_handler::moderation::{Moderator, Verdict};
    use gw_state::GatewayState;
    use tower::ServiceExt;

    use super::*;

    struct Stub {
        fetches: AtomicUsize,
        expires_in: u64,
        reject_first_token: bool,
        multiline: bool,
    }

    async fn stub(
        State(st): State<Arc<Stub>>,
        headers: HeaderMap,
        axum::Json(req): axum::Json<Value>,
    ) -> Response {
        let auth = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        if st.reject_first_token && auth == "Bearer tok-1" {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        let reply = match req["method"].as_str() {
            Some("tools/list") => json!({"jsonrpc":"2.0","id":req["id"],"result":{"tools":[
                {"name":"add","inputSchema":{"type":"object"}},{"name":"echo","inputSchema":{"type":"object"}}]}}),
            Some("tools/call") => {
                json!({"jsonrpc":"2.0","id":req["id"],"result":{"content":[{"type":"text","text":
                format!("called {} with {}", req["params"]["name"], req["params"]["arguments"])},
                {"type":"image","data":"AAAA"}],"structuredContent":{"echoed":req["params"]["arguments"]["s"]}}})
            }
            Some("resources/read") => {
                json!({"jsonrpc":"2.0","id":req["id"],"result":{"contents":[{"uri":req["params"]["uri"],"text":"contact bob@example.com"}]}})
            }
            Some("ping") => json!({"jsonrpc":"2.0","id":req["id"],"result":{"auth":auth,
                "session":headers.get("mcp-session-id").and_then(|v| v.to_str().ok()),
                "last_event_id":headers.get("last-event-id").and_then(|v| v.to_str().ok())}}),
            _ => {
                json!({"jsonrpc":"2.0","id":req["id"],"result":{"protocolVersion":"2025-06-18","capabilities":{}}})
            }
        };
        let sse = headers
            .get("accept")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|a| a == "text/event-stream");
        let mut out = HeaderMap::new();
        out.insert("mcp-session-id", "sess-1".parse().unwrap());
        if sse {
            out.insert("content-type", "text/event-stream".parse().unwrap());
            let body = if st.multiline {
                let text = serde_json::to_string_pretty(&reply).unwrap();
                let data: String = text
                    .lines()
                    .enumerate()
                    .map(|(i, l)| {
                        if i == 0 {
                            format!("data: {l}\r\n")
                        } else {
                            format!("data:{l}\r\n")
                        }
                    })
                    .collect();
                format!("event: message\r\nid: 7\r\n{data}\r\n")
            } else {
                format!("event: message\ndata: {reply}\n\n")
            };
            return (StatusCode::OK, out, body).into_response();
        }
        (StatusCode::OK, out, axum::Json(reply)).into_response()
    }

    async fn stub_listen() -> Response {
        let mut out = HeaderMap::new();
        out.insert("content-type", "text/event-stream".parse().unwrap());
        (StatusCode::OK, out, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\",\"params\":{\"data\":\"hello\"}}\n\n").into_response()
    }

    async fn token(State(st): State<Arc<Stub>>, body: String) -> Response {
        assert!(body.contains("grant_type=client_credentials"), "{body}");
        assert!(body.contains("client_id=gw"), "{body}");
        let n = st.fetches.fetch_add(1, Ordering::Relaxed) + 1;
        axum::Json(json!({"access_token": format!("tok-{n}"), "expires_in": st.expires_in}))
            .into_response()
    }

    async fn spawn_stub_with(expires_in: u64, reject_first_token: bool) -> (String, Arc<Stub>) {
        spawn_stub_full(expires_in, reject_first_token, false).await
    }

    async fn spawn_stub_full(
        expires_in: u64,
        reject_first_token: bool,
        multiline: bool,
    ) -> (String, Arc<Stub>) {
        let st = Arc::new(Stub {
            fetches: AtomicUsize::new(0),
            expires_in,
            reject_first_token,
            multiline,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/mcp", post(stub).get(stub_listen))
            .route("/token", post(token))
            .with_state(st.clone());
        tokio::spawn(axum::serve(listener, app).into_future());
        (format!("http://{addr}"), st)
    }

    async fn spawn_stub() -> String {
        format!("{}/mcp", spawn_stub_with(3600, false).await.0)
    }

    fn app_yaml(base: &str) -> String {
        format!(
            "listen: {{host: h, port: 1}}\nmax_live_streams_per_key: 2\nmcp_servers: [{{name: tools, endpoint: {base}/mcp, api_key_env: GW_TEST_MCP_TOKEN, max_reply_bytes: 4096}}, {{name: locked, endpoint: {base}/mcp}}, {{name: oauth, endpoint: {base}/mcp, oauth: {{token_url: {base}/token, client_id: gw, client_secret_env: GW_TEST_MCP_SECRET}}}}]\ntenants: [{{name: reviewed, security: {{moderate: true}}}}]\naccess_keys: [{{ak: k-add, product: p, qps: 100, daily_token_quota: 1000, mcp_servers: [tools], mcp_tools: {{tools: [add]}}}}, {{ak: k-all, product: p, qps: 100, daily_token_quota: 1000, mcp_servers: [tools]}}, {{ak: k-oauth, product: p, qps: 100, daily_token_quota: 1000, mcp_servers: [oauth]}}, {{ak: k-mod, tenant: reviewed, product: p, qps: 100, daily_token_quota: 1000, mcp_servers: [tools]}}]"
        )
    }

    async fn app_with(endpoint: &str) -> (Router, Arc<GatewayState>) {
        let base = endpoint.trim_end_matches("/mcp");
        let cfg = Arc::new(GatewayConfig::from_yaml(&app_yaml(base)).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let app = crate::app(AppState::new(
            cfg,
            state.clone(),
            Arc::new(gw_engines::MockTransport),
        ));
        (app, state)
    }

    fn rpc(ak: &str, server: &str, body: &str, accept: &str) -> axum::http::Request<Body> {
        rpc_session(ak, server, body, accept, &format!("sess-{ak}"))
    }

    fn rpc_session(
        ak: &str,
        server: &str,
        body: &str,
        accept: &str,
        session: &str,
    ) -> axum::http::Request<Body> {
        axum::http::Request::builder()
            .method("POST")
            .uri(format!("/mcp/{server}"))
            .header("authorization", format!("Bearer {ak}"))
            .header("content-type", "application/json")
            .header("accept", accept)
            .header("mcp-session-id", session)
            .header("last-event-id", "41")
            .body(Body::from(body.to_owned()))
            .unwrap()
    }

    async fn text(resp: Response) -> String {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn tools_list_is_filtered_to_the_allowlist() {
        let (app, _) = app_with(&spawn_stub().await).await;
        let list = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#;
        let resp = app
            .clone()
            .oneshot(rpc("k-add", "tools", list, "application/json"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()["mcp-session-id"], "sess-1");
        let v: Value = serde_json::from_str(&text(resp).await).unwrap();
        assert_eq!(
            v["result"]["tools"],
            json!([{"name":"add","inputSchema":{"type":"object"}}])
        );
        let resp = app
            .oneshot(rpc("k-all", "tools", list, "application/json"))
            .await
            .unwrap();
        let v: Value = serde_json::from_str(&text(resp).await).unwrap();
        assert_eq!(v["result"]["tools"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn tools_list_is_filtered_inside_an_event_stream() {
        let (app, _) = app_with(&spawn_stub().await).await;
        let list = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#;
        let resp = app
            .oneshot(rpc("k-add", "tools", list, "text/event-stream"))
            .await
            .unwrap();
        assert_eq!(resp.headers()["content-type"], "text/event-stream");
        let body = text(resp).await;
        assert!(body.starts_with("event: message\ndata: "), "{body}");
        let msg: Value =
            serde_json::from_str(body["event: message\ndata: ".len()..].trim()).unwrap();
        assert_eq!(msg["result"]["tools"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn multi_line_data_events_are_assembled_before_filtering() {
        let (base, _) = spawn_stub_full(3600, false, true).await;
        let (app, _) = app_with(&format!("{base}/mcp")).await;
        let list = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#;
        let resp = app
            .oneshot(rpc("k-add", "tools", list, "text/event-stream"))
            .await
            .unwrap();
        let body = text(resp).await;
        assert!(
            body.starts_with("event: message\r\nid: 7\r\ndata: "),
            "{body}"
        );
        let data = body.lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
        let msg: Value = serde_json::from_str(data).unwrap();
        assert_eq!(
            msg["result"]["tools"],
            json!([{"name":"add","inputSchema":{"type":"object"}}])
        );
    }

    #[tokio::test]
    async fn tool_calls_are_gated_and_audited() {
        let (app, state) = app_with(&spawn_stub().await).await;
        let echo = r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"echo","arguments":{"s":"x"}}}"#;
        let resp = app
            .clone()
            .oneshot(rpc("k-add", "tools", echo, "application/json"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let v: Value = serde_json::from_str(&text(resp).await).unwrap();
        assert_eq!(v["id"], 7);
        assert_eq!(v["error"]["code"], JSONRPC_TOOL_DENIED);
        let add = r#"{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"add","arguments":{"a":2,"b":3}}}"#;
        let resp = app
            .oneshot(rpc("k-add", "tools", add, "application/json"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let v: Value = serde_json::from_str(&text(resp).await).unwrap();
        assert_eq!(
            v["result"]["content"][0]["text"],
            "called \"add\" with {\"a\":2,\"b\":3}"
        );
        let events = state.store.security_events(None, 10).await.unwrap();
        let actions: Vec<(&str, &str, &str)> = events
            .iter()
            .map(|e| (e.surface.as_str(), e.rule.as_str(), e.action.as_str()))
            .collect();
        assert!(
            actions.contains(&("mcp", "mcp:tools", "deny:echo")),
            "{actions:?}"
        );
        assert!(
            actions.contains(&("mcp", "mcp:tools", "call:add")),
            "{actions:?}"
        );
    }

    #[tokio::test]
    async fn unentitled_unknown_and_malformed_requests_are_refused() {
        let (app, _) = app_with(&spawn_stub().await).await;
        let ping = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
        for server in ["locked", "nope"] {
            let resp = app
                .clone()
                .oneshot(rpc("k-add", server, ping, "application/json"))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{server}");
            assert!(text(resp).await.contains("unknown mcp server"), "{server}");
        }
        let resp = app
            .clone()
            .oneshot(rpc("k-add", "tools", "[]", "application/json"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let positional = r#"{"jsonrpc":"2.0","id":9,"method":"tools/call","params":["add",{}]}"#;
        let resp = app
            .clone()
            .oneshot(rpc("k-add", "tools", positional, "application/json"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let resp = app
            .oneshot(rpc("bogus", "tools", ping, "application/json"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn sessions_are_bound_to_the_key_that_opened_them() {
        let (app, _) = app_with(&spawn_stub().await).await;
        let ping = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
        let resp = app
            .clone()
            .oneshot(rpc_session(
                "k-all",
                "tools",
                ping,
                "application/json",
                "fresh",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = app
            .clone()
            .oneshot(rpc_session(
                "k-add",
                "tools",
                ping,
                "application/json",
                "sess-1",
            ))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "another key's session"
        );
        let resp = app
            .oneshot(rpc_session(
                "k-all",
                "tools",
                ping,
                "application/json",
                "sess-1",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "the owner keeps using it");
    }

    #[tokio::test]
    async fn listen_streams_are_capped_per_key() {
        let (app, _) = app_with(&spawn_stub().await).await;
        let listen = |ak: &str| {
            axum::http::Request::builder()
                .method("GET")
                .uri("/mcp/tools")
                .header("authorization", format!("Bearer {ak}"))
                .header("accept", "text/event-stream")
                .body(Body::empty())
                .unwrap()
        };
        let first = app.clone().oneshot(listen("k-all")).await.unwrap();
        let second = app.clone().oneshot(listen("k-all")).await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(second.status(), StatusCode::OK);
        let third = app.clone().oneshot(listen("k-all")).await.unwrap();
        assert_eq!(third.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(text(first).await.contains("notifications/message"));
        let again = app.oneshot(listen("k-all")).await.unwrap();
        assert_eq!(
            again.status(),
            StatusCode::OK,
            "a finished stream frees its slot"
        );
    }

    async fn ping_auth(app: &Router, server: &str) -> String {
        let ping = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
        let resp = app
            .clone()
            .oneshot(rpc("k-oauth", server, ping, "application/json"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let v: Value = serde_json::from_str(&text(resp).await).unwrap();
        v["result"]["auth"].as_str().unwrap().to_owned()
    }

    #[tokio::test]
    async fn oauth_tokens_are_fetched_once_and_reused() {
        let (base, st) = spawn_stub_with(3600, false).await;
        let (app, _) = app_with(&format!("{base}/mcp")).await;
        assert_eq!(ping_auth(&app, "oauth").await, "Bearer tok-1");
        assert_eq!(ping_auth(&app, "oauth").await, "Bearer tok-1");
        assert_eq!(st.fetches.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn short_lived_and_refused_oauth_tokens_are_fetched_anew() {
        let (base, st) = spawn_stub_with(1, false).await;
        let (app, _) = app_with(&format!("{base}/mcp")).await;
        assert_eq!(ping_auth(&app, "oauth").await, "Bearer tok-1");
        assert_eq!(
            ping_auth(&app, "oauth").await,
            "Bearer tok-1",
            "a 1 s token is cached for half its life"
        );
        assert_eq!(st.fetches.load(Ordering::Relaxed), 1);

        let (base, st) = spawn_stub_with(3600, true).await;
        let (app, _) = app_with(&format!("{base}/mcp")).await;
        assert_eq!(ping_auth(&app, "oauth").await, "Bearer tok-2");
        assert_eq!(st.fetches.load(Ordering::Relaxed), 2);
    }

    #[derive(Debug)]
    struct EmailMasker;

    #[async_trait::async_trait]
    impl Moderator for EmailMasker {
        async fn review(&self, text: &str) -> Result<Verdict, String> {
            if text.contains("ssn") {
                return Ok(Verdict::Deny("blocked by guardrail: SSN".into()));
            }
            let spans: Vec<_> = text
                .match_indices("bob@example.com")
                .map(|(i, m)| i..i + m.len())
                .collect();
            Ok(if spans.is_empty() {
                Verdict::Allow
            } else {
                Verdict::Mask(spans)
            })
        }
    }

    async fn reviewed_app(endpoint: &str) -> (Router, Arc<GatewayState>) {
        let base = endpoint.trim_end_matches("/mcp");
        let cfg = Arc::new(GatewayConfig::from_yaml(&app_yaml(base)).unwrap());
        let state = Arc::new(GatewayState::from_config(&cfg));
        let app_state = AppState::new(cfg, state.clone(), Arc::new(gw_engines::MockTransport));
        let app_state = AppState {
            handler: app_state.handler.with_moderator(Arc::new(EmailMasker)),
            ..app_state
        };
        (crate::app(app_state), state)
    }

    #[tokio::test]
    async fn reviewed_results_are_masked_everywhere_or_blocked_whole() {
        let (app, state) = reviewed_app(&spawn_stub().await).await;
        let echo = r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"echo","arguments":{"s":"mail bob@example.com twice bob@example.com"}}}"#;
        let resp = app
            .clone()
            .oneshot(rpc("k-mod", "tools", echo, "application/json"))
            .await
            .unwrap();
        let v: Value = serde_json::from_str(&text(resp).await).unwrap();
        let masked = v["result"]["content"][0]["text"].as_str().unwrap();
        assert!(masked.contains("mail [MASKED] twice [MASKED]"), "{masked}");
        assert_eq!(v["result"]["content"][1]["type"], "image");
        assert_eq!(v["result"]["content"][1]["data"], "AAAA");
        assert_eq!(
            v["result"]["structuredContent"]["echoed"], "mail [MASKED] twice [MASKED]",
            "structured output is reviewed too"
        );

        let resp = app
            .clone()
            .oneshot(rpc("k-mod", "tools", echo, "text/event-stream"))
            .await
            .unwrap();
        let body = text(resp).await;
        assert!(
            body.contains("[MASKED]") && !body.contains("bob@"),
            "{body}"
        );

        let read = r#"{"jsonrpc":"2.0","id":5,"method":"resources/read","params":{"uri":"file:///notes"}}"#;
        let resp = app
            .clone()
            .oneshot(rpc("k-mod", "tools", read, "application/json"))
            .await
            .unwrap();
        let v: Value = serde_json::from_str(&text(resp).await).unwrap();
        assert_eq!(v["result"]["contents"][0]["text"], "contact [MASKED]");
        assert_eq!(v["result"]["contents"][0]["uri"], "file:///notes");

        let leak = r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"echo","arguments":{"s":"my ssn is 123"}}}"#;
        let resp = app
            .clone()
            .oneshot(rpc("k-mod", "tools", leak, "text/event-stream"))
            .await
            .unwrap();
        let body = text(resp).await;
        assert_eq!(
            body.lines().filter(|l| l.starts_with("data:")).count(),
            1,
            "{body}"
        );
        let v: Value = serde_json::from_str(body.trim().strip_prefix("data: ").unwrap()).unwrap();
        assert_eq!(v["id"], 4);
        assert_eq!(v["error"]["code"], JSONRPC_RESULT_BLOCKED);
        assert_eq!(v["error"]["message"], "blocked by guardrail: SSN");
        assert!(!body.contains("ssn"), "{body}");

        let resp = app
            .oneshot(rpc("k-all", "tools", leak, "application/json"))
            .await
            .unwrap();
        let v: Value = serde_json::from_str(&text(resp).await).unwrap();
        assert!(
            v["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("ssn"),
            "a tenant without moderate passes results through"
        );

        let events = state.store.security_events(None, 10).await.unwrap();
        let mods: Vec<(&str, i64)> = events
            .iter()
            .filter(|e| e.surface == "mcp" && e.rule == "moderation")
            .map(|e| (e.action.as_str(), e.hits))
            .collect();
        assert_eq!(
            mods,
            vec![("block", 1), ("mask", 1), ("mask", 4), ("mask", 4)]
        );
    }

    async fn tricky(axum::Json(req): axum::Json<Value>) -> Response {
        let id = req["id"].clone();
        let reply = match req["params"]["name"].as_str() {
            Some("err_status") => {
                let body = json!({"jsonrpc":"2.0","id":id,"result":{"content":[{"type":"text","text":"leak bob@example.com"}]}});
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    mcp_headers(),
                    axum::Json(body),
                )
                    .into_response();
            }
            Some("err_body") => {
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-1,"message":"see bob@example.com"}})
            }
            _ => {
                json!({"jsonrpc":"2.0","id":id,"result":{"structuredContent":{"name":"contact bob@example.com"}}})
            }
        };
        (StatusCode::OK, mcp_headers(), axum::Json(reply)).into_response()
    }

    fn mcp_headers() -> HeaderMap {
        let mut out = HeaderMap::new();
        out.insert("mcp-session-id", "sess-1".parse().unwrap());
        out
    }

    async fn spawn_tricky() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().route("/mcp", post(tricky));
        tokio::spawn(axum::serve(listener, app).into_future());
        format!("http://{addr}")
    }

    async fn call_tool(app: &Router, name: &str) -> Value {
        let body = format!(
            r#"{{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{{"name":"{name}","arguments":{{}}}}}}"#
        );
        let resp = app
            .clone()
            .oneshot(rpc("k-mod", "tools", &body, "application/json"))
            .await
            .unwrap();
        serde_json::from_str(&text(resp).await).unwrap()
    }

    #[tokio::test]
    async fn review_covers_error_status_error_body_and_structured_prose() {
        let (app, _) = reviewed_app(&format!("{}/mcp", spawn_tricky().await)).await;

        let v = call_tool(&app, "err_status").await;
        assert_eq!(
            v["result"]["content"][0]["text"], "leak [MASKED]",
            "a non-2xx reply is still reviewed: {v}"
        );

        let v = call_tool(&app, "err_body").await;
        assert_eq!(
            v["error"]["message"], "see [MASKED]",
            "a JSON-RPC error's prose is reviewed: {v}"
        );

        let v = call_tool(&app, "structured").await;
        assert_eq!(
            v["result"]["structuredContent"]["name"], "contact [MASKED]",
            "structured prose under any key is reviewed: {v}"
        );
    }

    #[tokio::test]
    async fn a_reviewed_tenant_cannot_open_a_listen_stream() {
        let (app, _) = reviewed_app(&spawn_stub().await).await;
        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .method("GET")
                    .uri("/mcp/tools")
                    .header("authorization", "Bearer k-mod")
                    .header("accept", "text/event-stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn reviewed_tenants_get_no_stream_resumption() {
        let (app, _) = reviewed_app(&spawn_stub().await).await;
        let ping = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
        let resp = app
            .clone()
            .oneshot(rpc("k-mod", "tools", ping, "application/json"))
            .await
            .unwrap();
        let v: Value = serde_json::from_str(&text(resp).await).unwrap();
        assert!(v["result"]["last_event_id"].is_null(), "{v}");
        let resp = app
            .oneshot(rpc("k-all", "tools", ping, "application/json"))
            .await
            .unwrap();
        let v: Value = serde_json::from_str(&text(resp).await).unwrap();
        assert_eq!(v["result"]["last_event_id"], "41");
    }

    #[test]
    fn unparsable_data_fails_closed_and_framing_survives() {
        let sse = b"event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[\ndata: {\"name\":\"add\"},{\"name\":\"exfil\"}]}}\n\n: comment\nid: 9\ndata: not json\n\n";
        let segments = parse_segments(sse, true);
        assert!(
            matches!(segments[1], Segment::Message(_)),
            "joined data lines parse"
        );
        assert!(segments.iter().any(|s| matches!(s, Segment::Opaque)));
        assert!(filter_tool_list(sse, true, &["add".to_owned()]).is_none());
        let bom = "\u{feff}{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[{\"name\":\"add\"},{\"name\":\"x\"}]}}";
        let filtered = filter_tool_list(bom.as_bytes(), false, &["add".to_owned()]).unwrap();
        let v: Value = serde_json::from_slice(&filtered).unwrap();
        assert_eq!(v["result"]["tools"].as_array().unwrap().len(), 1);
        let plain =
            b"event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[]}}\n\n";
        assert_eq!(
            String::from_utf8(filter_tool_list(plain, true, &[]).unwrap()).unwrap(),
            "event: message\ndata: {\"id\":1,\"jsonrpc\":\"2.0\",\"result\":{\"tools\":[]}}\n\n"
        );
    }

    #[test]
    fn config_rejects_a_key_naming_an_unknown_server() {
        let yaml = "listen: {host: h, port: 1}\nmcp_servers: [{name: tools, endpoint: http://x/mcp}]\naccess_keys: [{ak: k, product: p, qps: 1, daily_token_quota: 1, mcp_tools: {ghost: [a]}}]";
        assert!(matches!(
            GatewayConfig::from_yaml(yaml),
            Err(gw_config::ConfigError::UnknownMcpServer { .. })
        ));
    }

    #[test]
    fn config_rejects_a_broken_oauth_block() {
        for oauth in [
            "api_key_env: T, oauth: {token_url: http://x/token, client_id: c}",
            "oauth: {token_url: '', client_id: c}",
            "oauth: {token_url: http://x/token, client_id: c, grant: refresh_token}",
        ] {
            let yaml = format!(
                "listen: {{host: h, port: 1}}\nmcp_servers: [{{name: tools, endpoint: http://x/mcp, {oauth}}}]"
            );
            assert!(
                matches!(
                    GatewayConfig::from_yaml(&yaml),
                    Err(gw_config::ConfigError::BadMcpServer { .. })
                ),
                "{oauth}"
            );
        }
    }
}
