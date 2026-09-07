//! `/mcp/{server}`: the Model Context Protocol proxy. A key reaches only the
//! servers it is entitled to and the tools its allowlist names; `tools/list`
//! is filtered to that allowlist and every call or denial is a security event.

use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use gw_state::{AkInfo, GatewayState, SecurityEvent, admission};
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

/// The JSON-RPC envelope of one POST, as far as the proxy needs it.
#[derive(Default)]
struct Call {
    method: String,
    id: Value,
    tool: Option<String>,
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
        audit(&snap.state, &ak, &server, &format!("deny:{tool}")).await;
        count(&server, "tools/call", "denied");
        return jsonrpc_error(
            call.id,
            JSONRPC_TOOL_DENIED,
            format!("tool `{tool}` is not permitted for this key"),
        );
    }
    let mut upstream = s
        .mcp
        .request(method.clone(), &conf.endpoint)
        .timeout(Duration::from_secs(conf.timeout_seconds));
    for name in FORWARDED_HEADERS {
        if let Some(v) = headers.get(name) {
            upstream = upstream.header(name, v);
        }
    }
    if let Some(key) = conf.api_key() {
        upstream = upstream.bearer_auth(key);
    }
    if method == Method::POST {
        upstream = upstream.body(body);
    }
    let reply = match upstream.send().await {
        Ok(reply) => reply,
        Err(e) => {
            count(&server, method_label(&call.method), "upstream_error");
            return error_response(502, format!("mcp server `{server}`: {e}"));
        }
    };
    if let Some(tool) = call.tool.as_deref() {
        audit(&snap.state, &ak, &server, &format!("call:{tool}")).await;
    }
    let status = reply.status();
    count(
        &server,
        method_label(&call.method),
        crate::status_label(status).as_ref(),
    );
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
    let body = match allowed {
        Some(list) if call.method == "tools/list" && status.is_success() => {
            match reply.bytes().await {
                Ok(bytes) => Body::from(filter_tool_list(&bytes, sse, list)),
                Err(e) => return error_response(502, format!("mcp server `{server}`: {e}")),
            }
        }
        _ => Body::from_stream(reply.bytes_stream()),
    };
    (status, out, body).into_response()
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
        .then(|| obj["params"]["name"].as_str().map(str::to_owned))
        .flatten();
    Ok(Call {
        method,
        id: obj.remove("id").unwrap_or(Value::Null),
        tool,
    })
}

/// Keep only the allowlisted tools in every `tools/list` result the reply carries.
fn filter_tool_list(bytes: &[u8], sse: bool, allowed: &[String]) -> Vec<u8> {
    let keep = |mut msg: Value| {
        if let Some(tools) = msg["result"]["tools"].as_array_mut() {
            tools.retain(|t| {
                t["name"]
                    .as_str()
                    .is_some_and(|n| allowed.iter().any(|a| a == n))
            });
        }
        msg
    };
    if !sse {
        return match serde_json::from_slice::<Value>(bytes) {
            Ok(msg) => serde_json::to_vec(&keep(msg)).unwrap_or_else(|_| bytes.to_vec()),
            Err(_) => bytes.to_vec(),
        };
    }
    let text = String::from_utf8_lossy(bytes);
    let mut out = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        match line.strip_prefix("data:") {
            Some(data) => match serde_json::from_str::<Value>(data.trim()) {
                Ok(msg) => {
                    out.push_str("data: ");
                    out.push_str(&keep(msg).to_string());
                    out.push('\n');
                }
                Err(_) => out.push_str(line),
            },
            None => out.push_str(line),
        }
    }
    out.into_bytes()
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

fn count(server: &str, method: &'static str, result: &str) {
    metrics::counter!(
        "gateway_mcp_requests_total",
        "server" => server.to_owned(),
        "method" => method,
        "result" => result.to_owned(),
    )
    .increment(1);
}

async fn audit(state: &GatewayState, ak: &AkInfo, server: &str, action: &str) {
    SecurityEvent {
        created_at_epoch_secs: gw_state::epoch_secs(),
        request_id: gw_handler::new_request_id(),
        ak: ak.ak.clone(),
        user_id: ak.owner.clone().unwrap_or_default(),
        tenant: ak.tenant.clone(),
        surface: "mcp".to_owned(),
        rule: format!("mcp:{server}"),
        action: action.to_owned(),
        hits: 1,
    }
    .record(state.store.as_ref())
    .await;
}

fn jsonrpc_error(id: Value, code: i64, message: String) -> Response {
    let body = json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}});
    (StatusCode::OK, axum::Json(body)).into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::Router;
    use axum::routing::post;
    use gw_config::GatewayConfig;
    use gw_state::GatewayState;
    use tower::ServiceExt;

    use super::*;

    async fn stub(headers: HeaderMap, axum::Json(req): axum::Json<Value>) -> Response {
        let reply = match req["method"].as_str() {
            Some("tools/list") => json!({"jsonrpc":"2.0","id":req["id"],"result":{"tools":[
                {"name":"add","inputSchema":{"type":"object"}},{"name":"echo","inputSchema":{"type":"object"}}]}}),
            Some("tools/call") => {
                json!({"jsonrpc":"2.0","id":req["id"],"result":{"content":[{"type":"text","text":
                format!("called {} with {}", req["params"]["name"], req["params"]["arguments"])}]}})
            }
            _ => {
                json!({"jsonrpc":"2.0","id":req["id"],"result":{"protocolVersion":"2025-06-18","capabilities":{}}})
            }
        };
        let sse = headers
            .get("accept")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|a| a == "text/event-stream");
        let auth = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let mut out = HeaderMap::new();
        out.insert("mcp-session-id", "sess-1".parse().unwrap());
        out.insert("x-stub-auth", auth.parse().unwrap());
        if sse {
            out.insert("content-type", "text/event-stream".parse().unwrap());
            let body = format!("event: message\ndata: {reply}\n\n");
            return (StatusCode::OK, out, body).into_response();
        }
        (StatusCode::OK, out, axum::Json(reply)).into_response()
    }

    async fn spawn_stub() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(axum::serve(listener, Router::new().route("/mcp", post(stub))).into_future());
        format!("http://{addr}/mcp")
    }

    async fn app_with(endpoint: &str) -> (Router, Arc<GatewayState>) {
        let yaml = format!(
            "listen: {{host: h, port: 1}}\nmcp_servers: [{{name: tools, endpoint: {endpoint}, api_key_env: GW_TEST_MCP_TOKEN}}, {{name: locked, endpoint: {endpoint}}}]\naccess_keys: [{{ak: k-add, product: p, qps: 100, daily_token_quota: 1000, mcp_servers: [tools], mcp_tools: {{tools: [add]}}}}, {{ak: k-all, product: p, qps: 100, daily_token_quota: 1000, mcp_servers: [tools]}}]"
        );
        let cfg = Arc::new(GatewayConfig::from_yaml(&yaml).unwrap());
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

    #[test]
    fn config_rejects_a_key_naming_an_unknown_server() {
        let yaml = "listen: {host: h, port: 1}\nmcp_servers: [{name: tools, endpoint: http://x/mcp}]\naccess_keys: [{ak: k, product: p, qps: 1, daily_token_quota: 1, mcp_tools: {ghost: [a]}}]";
        assert!(matches!(
            GatewayConfig::from_yaml(yaml),
            Err(gw_config::ConfigError::UnknownMcpServer { .. })
        ));
    }
}
