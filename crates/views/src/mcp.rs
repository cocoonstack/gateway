//! `/mcp/{server}`: the Model Context Protocol proxy. A key reaches only the
//! servers it is entitled to and the tools its allowlist names; `tools/list`
//! is filtered to that allowlist, a tenant under `security.moderate` has its
//! tool results reviewed, and every call, denial and intervention is a
//! security event.

use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use gw_config::McpServerConf;
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
const JSONRPC_TOOL_DENIED: i64 = -32000;
const JSONRPC_RESULT_BLOCKED: i64 = -32001;

/// The JSON-RPC envelope of one POST, as far as the proxy needs it.
#[derive(Default)]
struct Call {
    method: String,
    id: Value,
    tool: Option<String>,
}

/// One piece of a buffered reply: a JSON-RPC message the proxy may rewrite, or bytes it passes through.
enum Segment {
    Message(Value),
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
    let Some(conf) = snap.cfg.find_mcp_server(&server) else {
        return error_response(404, format!("unknown mcp server: {server}"));
    };
    if !ak.mcp.reaches(&server) {
        return error_response(
            403,
            format!("mcp server `{server}` is not entitled for this key"),
        );
    }
    if let Err(e) = admission::check_ak_rate(snap.state.governance.as_ref(), &ak).await {
        return error_response(429, e);
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
    let label = method_label(&call.method);
    let mut reply = match send(&s, conf, &method, &headers, &body).await {
        Ok(reply) => reply,
        Err(e) => {
            count(&server, label, "upstream_error");
            return error_response(502, format!("mcp server `{server}`: {e}"));
        }
    };
    // a token the server stopped honoring is fetched anew once
    if reply.status() == StatusCode::UNAUTHORIZED && conf.oauth.is_some() {
        s.mcp_auth.invalidate(&conf.name);
        reply = match send(&s, conf, &method, &headers, &body).await {
            Ok(reply) => reply,
            Err(e) => {
                count(&server, label, "upstream_error");
                return error_response(502, format!("mcp server `{server}`: {e}"));
            }
        };
    }
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
    let status = reply.status();
    count(&server, label, crate::status_label(status));
    let mut out = HeaderMap::new();
    for name in RETURNED_HEADERS {
        if let Some(v) = reply.headers().get(name) {
            out.insert(name, v.clone());
        }
    }
    let sse = out
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("text/event-stream"));
    let sec = snap.cfg.security_for(&ak.tenant);
    let filtered = status.is_success() && call.method == "tools/list" && allowed.is_some();
    let reviewed = status.is_success() && call.tool.is_some() && sec.moderate;
    if !filtered && !reviewed {
        return (status, out, Body::from_stream(reply.bytes_stream())).into_response();
    }
    let bytes = match reply.bytes().await {
        Ok(bytes) => bytes,
        Err(e) => return error_response(502, format!("mcp server `{server}`: {e}")),
    };
    let body = match allowed {
        Some(list) if filtered => filter_tool_list(&bytes, sse, list),
        _ => moderate_result(&s, &snap, &ak, &server, call.id, &bytes, sse).await,
    };
    (status, out, Body::from(body)).into_response()
}

async fn send(
    s: &AppState,
    conf: &McpServerConf,
    method: &Method,
    headers: &HeaderMap,
    body: &Bytes,
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
        if let Some(v) = headers.get(name) {
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

fn parse_call(body: &[u8]) -> Result<Call, String> {
    let v: Value =
        serde_json::from_slice(body).map_err(|e| format!("body is not JSON-RPC: {e}"))?;
    let Value::Object(mut obj) = v else {
        return Err("JSON-RPC batches are not supported; send one message per request".to_owned());
    };
    let method = obj
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let tool = (method == "tools/call")
        .then(|| {
            obj.get("params")
                .and_then(|p| p["name"].as_str())
                .map(str::to_owned)
        })
        .flatten();
    Ok(Call {
        method,
        id: obj.remove("id").unwrap_or(Value::Null),
        tool,
    })
}

/// Keep only the allowlisted tools in every `tools/list` result the reply carries.
fn filter_tool_list(bytes: &[u8], sse: bool, allowed: &[String]) -> Vec<u8> {
    let mut segments = parse_segments(bytes, sse);
    for tools in segments.iter_mut().filter_map(|seg| match seg {
        Segment::Message(msg) => msg
            .get_mut("result")
            .and_then(|r| r.get_mut("tools"))
            .and_then(Value::as_array_mut),
        Segment::Raw(_) => None,
    }) {
        tools.retain(|t| {
            t["name"]
                .as_str()
                .is_some_and(|n| allowed.iter().any(|a| a == n))
        });
    }
    serialize_segments(segments, sse)
}

/// Review a `tools/call` result's text: deny → JSON-RPC error, mask → in-place rewrite, both recorded.
async fn moderate_result(
    s: &AppState,
    snap: &Snapshot,
    ak: &AkInfo,
    server: &str,
    id: Value,
    bytes: &[u8],
    sse: bool,
) -> Vec<u8> {
    let mut segments = parse_segments(bytes, sse);
    let texts: Vec<&mut String> = segments.iter_mut().flat_map(tool_texts).collect();
    let review = plugins::slot_text(texts.iter().map(|s| s.as_str()));
    if review.is_empty() {
        return serialize_segments(segments, sse);
    }
    let sec = snap.cfg.security_for(&ak.tenant);
    match s.handler.moderate_rt(sec, &review).await {
        RtModeration::Allow => {}
        RtModeration::Mask(spans) => {
            let hits = plugins::apply_mask_slots(&spans, texts);
            if hits > 0 {
                audit(
                    &snap.state,
                    ak,
                    "moderation".to_owned(),
                    "mask".to_owned(),
                    hits as i64,
                )
                .await;
                count(server, "tools/call", "masked");
            }
        }
        RtModeration::Deny(reason) => {
            audit(
                &snap.state,
                ak,
                "moderation".to_owned(),
                "block".to_owned(),
                1,
            )
            .await;
            count(server, "tools/call", "blocked");
            for seg in &mut segments {
                if let Segment::Message(msg) = seg
                    && msg.get("result").is_some()
                {
                    *msg = jsonrpc_error_value(id.clone(), JSONRPC_RESULT_BLOCKED, &reason);
                }
            }
        }
    }
    serialize_segments(segments, sse)
}

/// The text items of a `tools/call` result, in wire order; nothing for other messages.
fn tool_texts(seg: &mut Segment) -> impl Iterator<Item = &mut String> {
    let content = match seg {
        Segment::Message(msg) => msg
            .get_mut("result")
            .and_then(|r| r.get_mut("content"))
            .and_then(Value::as_array_mut),
        Segment::Raw(_) => None,
    };
    content
        .into_iter()
        .flatten()
        .filter(|c| c["type"] == "text")
        .filter_map(|c| match c.get_mut("text") {
            Some(Value::String(s)) => Some(s),
            _ => None,
        })
}

/// A bare JSON body is one message; an event stream is its `data:` lines, everything else verbatim.
fn parse_segments(bytes: &[u8], sse: bool) -> Vec<Segment> {
    let text = String::from_utf8_lossy(bytes);
    if !sse {
        return vec![match serde_json::from_str(&text) {
            Ok(msg) => Segment::Message(msg),
            Err(_) => Segment::Raw(text.into_owned()),
        }];
    }
    text.split_inclusive('\n')
        .map(|line| {
            match line
                .strip_prefix("data:")
                .and_then(|data| serde_json::from_str(data.trim()).ok())
            {
                Some(msg) => Segment::Message(msg),
                None => Segment::Raw(line.to_owned()),
            }
        })
        .collect()
}

fn serialize_segments(segments: Vec<Segment>, sse: bool) -> Vec<u8> {
    let mut out = Vec::new();
    for seg in segments {
        match seg {
            Segment::Raw(s) => out.extend_from_slice(s.as_bytes()),
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

async fn audit(state: &GatewayState, ak: &AkInfo, rule: String, action: String, hits: i64) {
    SecurityEvent {
        created_at_epoch_secs: gw_state::epoch_secs(),
        request_id: gw_handler::new_request_id(),
        ak: ak.ak.clone(),
        user_id: ak.owner.clone().unwrap_or_default(),
        tenant: ak.tenant.clone(),
        surface: "mcp".to_owned(),
        rule,
        action,
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
                {"type":"image","data":"AAAA"}]}})
            }
            Some("ping") => json!({"jsonrpc":"2.0","id":req["id"],"result":{"auth":auth}}),
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
            let body = format!("event: message\ndata: {reply}\n\n");
            return (StatusCode::OK, out, body).into_response();
        }
        (StatusCode::OK, out, axum::Json(reply)).into_response()
    }

    async fn token(State(st): State<Arc<Stub>>, body: String) -> Response {
        assert!(body.contains("grant_type=client_credentials"), "{body}");
        assert!(body.contains("client_id=gw"), "{body}");
        let n = st.fetches.fetch_add(1, Ordering::Relaxed) + 1;
        axum::Json(json!({"access_token": format!("tok-{n}"), "expires_in": st.expires_in}))
            .into_response()
    }

    async fn spawn_stub_with(expires_in: u64, reject_first_token: bool) -> (String, Arc<Stub>) {
        let st = Arc::new(Stub {
            fetches: AtomicUsize::new(0),
            expires_in,
            reject_first_token,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/mcp", post(stub))
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
            "listen: {{host: h, port: 1}}\nmcp_servers: [{{name: tools, endpoint: {base}/mcp, api_key_env: GW_TEST_MCP_TOKEN}}, {{name: locked, endpoint: {base}/mcp}}, {{name: oauth, endpoint: {base}/mcp, oauth: {{token_url: {base}/token, client_id: gw, client_secret_env: GW_TEST_MCP_SECRET}}}}]\ntenants: [{{name: reviewed, security: {{moderate: true}}}}]\naccess_keys: [{{ak: k-add, product: p, qps: 100, daily_token_quota: 1000, mcp_servers: [tools], mcp_tools: {{tools: [add]}}}}, {{ak: k-all, product: p, qps: 100, daily_token_quota: 1000, mcp_servers: [tools]}}, {{ak: k-oauth, product: p, qps: 100, daily_token_quota: 1000, mcp_servers: [oauth]}}, {{ak: k-mod, tenant: reviewed, product: p, qps: 100, daily_token_quota: 1000, mcp_servers: [tools]}}]"
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
        axum::http::Request::builder()
            .method("POST")
            .uri(format!("/mcp/{server}"))
            .header("authorization", format!("Bearer {ak}"))
            .header("content-type", "application/json")
            .header("accept", accept)
            .header("mcp-session-id", "sess-1")
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
    async fn entitlement_unknown_server_and_batches_are_refused() {
        let (app, _) = app_with(&spawn_stub().await).await;
        let ping = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
        let resp = app
            .clone()
            .oneshot(rpc("k-add", "locked", ping, "application/json"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        let resp = app
            .clone()
            .oneshot(rpc("k-add", "nope", ping, "application/json"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let resp = app
            .clone()
            .oneshot(rpc("k-add", "tools", "[]", "application/json"))
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
    async fn a_tool_call_without_params_is_forwarded_not_panicked() {
        let (app, _) = app_with(&spawn_stub().await).await;
        let bare = r#"{"jsonrpc":"2.0","id":9,"method":"tools/call"}"#;
        let resp = app
            .oneshot(rpc("k-add", "tools", bare, "application/json"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
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
    async fn expiring_and_refused_oauth_tokens_are_fetched_anew() {
        let (base, st) = spawn_stub_with(1, false).await;
        let (app, _) = app_with(&format!("{base}/mcp")).await;
        assert_eq!(ping_auth(&app, "oauth").await, "Bearer tok-1");
        assert_eq!(ping_auth(&app, "oauth").await, "Bearer tok-2");
        assert_eq!(st.fetches.load(Ordering::Relaxed), 2);

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
    async fn tool_results_are_masked_or_blocked_under_moderation() {
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

        let leak = r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"echo","arguments":{"s":"my ssn is 123"}}}"#;
        let resp = app
            .clone()
            .oneshot(rpc("k-mod", "tools", leak, "application/json"))
            .await
            .unwrap();
        let v: Value = serde_json::from_str(&text(resp).await).unwrap();
        assert_eq!(v["id"], 4);
        assert_eq!(v["error"]["code"], JSONRPC_RESULT_BLOCKED);
        assert_eq!(v["error"]["message"], "blocked by guardrail: SSN");
        assert!(v.get("result").is_none());

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
        assert_eq!(mods, vec![("block", 1), ("mask", 2), ("mask", 2)]);
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
