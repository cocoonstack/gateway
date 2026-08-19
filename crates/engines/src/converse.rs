//! Bedrock Converse — the vendor-neutral Bedrock chat API — driven through the
//! Anthropic Messages engine: the Messages body the engine builds is transcoded
//! into a Converse request, and Converse replies (buffered JSON, streamed
//! events) are transcoded back into the Messages shapes the engine, the views
//! and the native `/v1/messages` stream already understand.

use gw_protocol::object;
use serde_json::{Map, Value, json};

/// A Messages body as a Converse body; Claude-only knobs and passthrough extras
/// ride in `additionalModelRequestFields` (the knobs drop on non-Claude ids).
pub(crate) fn request(mut body: Map<String, Value>, claude: bool) -> Value {
    let mut out = Map::with_capacity(6);
    if let Some(system) = body.remove("system") {
        let blocks = match system {
            Value::String(text) => vec![object([("text", text.into())])],
            Value::Array(blocks) => blocks.into_iter().flat_map(content_block).collect(),
            _ => Vec::new(),
        };
        if !blocks.is_empty() {
            out.insert("system".into(), Value::Array(blocks));
        }
    }
    let messages: Vec<Value> = match body.remove("messages") {
        Some(Value::Array(messages)) => messages.into_iter().map(message).collect(),
        _ => Vec::new(),
    };
    out.insert("messages".into(), Value::Array(messages));

    let mut inference = Map::new();
    for (from, to) in [
        ("max_tokens", "maxTokens"),
        ("temperature", "temperature"),
        ("top_p", "topP"),
        ("stop_sequences", "stopSequences"),
    ] {
        if let Some(v) = body.remove(from) {
            inference.insert(to.into(), v);
        }
    }
    if !inference.is_empty() {
        out.insert("inferenceConfig".into(), Value::Object(inference));
    }

    let tools: Vec<Value> = match body.remove("tools") {
        Some(Value::Array(tools)) => tools
            .into_iter()
            .flat_map(|tool| tool_spec(tool, claude))
            .collect(),
        _ => Vec::new(),
    };
    let (disable_tools, tool_choice) = body
        .remove("tool_choice")
        .map(converse_tool_choice)
        .unwrap_or_default();
    if !tools.is_empty() && !disable_tools {
        let mut config = Map::with_capacity(2);
        config.insert("tools".into(), Value::Array(tools));
        if let Some(choice) = tool_choice {
            config.insert("toolChoice".into(), choice);
        }
        out.insert("toolConfig".into(), Value::Object(config));
    }

    if !claude {
        body.remove("thinking");
        body.remove("output_config");
    }
    body.remove("model");
    body.remove("stream");
    body.remove("metadata");
    if !body.is_empty() {
        out.insert("additionalModelRequestFields".into(), Value::Object(body));
    }
    Value::Object(out)
}

/// A buffered Converse reply as a Messages reply.
pub(crate) fn reply(mut v: Value, model: &str) -> Value {
    let content: Vec<Value> = match v["output"]["message"]["content"].take() {
        Value::Array(blocks) => blocks.into_iter().filter_map(anthropic_block).collect(),
        _ => Vec::new(),
    };
    let usage = usage(&v["usage"]);
    object([
        ("id", "msg_converse".into()),
        ("type", "message".into()),
        ("role", "assistant".into()),
        ("model", model.into()),
        ("content", Value::Array(content)),
        ("stop_reason", stop_reason(v["stopReason"].as_str()).into()),
        ("stop_sequence", Value::Null),
        ("usage", usage),
    ])
}

/// Converse stream events as the Anthropic sequence: implicit text/reasoning
/// blocks get a synthesized `content_block_start`, the trailing `metadata`
/// becomes the `message_delta` usage overlay plus `message_stop`.
#[derive(Debug)]
pub(crate) struct Events {
    model: String,
    open: Vec<u64>,
}

impl Events {
    pub(crate) fn new(model: String) -> Self {
        Self {
            model,
            open: Vec::new(),
        }
    }

    pub(crate) fn map(&mut self, mut ev: Value) -> Vec<Value> {
        let index = ev["contentBlockIndex"].as_u64().unwrap_or(0);
        match ev["type"].as_str().unwrap_or_default() {
            "messageStart" => vec![json!({
                "type": "message_start",
                "message": {"id": "msg_converse", "type": "message", "role": "assistant",
                    "model": self.model, "content": [], "stop_reason": null,
                    "usage": {"input_tokens": 0, "output_tokens": 0}}
            })],
            "contentBlockStart" => {
                let mut start = ev["start"].take();
                let block = if let Some(tool) = start.get_mut("toolUse") {
                    object([
                        ("type", "tool_use".into()),
                        ("id", tool["toolUseId"].take()),
                        ("name", tool["name"].take()),
                        ("input", json!({})),
                    ])
                } else {
                    json!({"type": "text", "text": ""})
                };
                self.open.push(index);
                vec![block_event(
                    "content_block_start",
                    index,
                    "content_block",
                    block,
                )]
            }
            "contentBlockDelta" => {
                let mut delta = ev["delta"].take();
                let mut events = Vec::with_capacity(2);
                let (start_block, anthropic_delta) = if let Some(text) = delta.get_mut("text") {
                    (
                        json!({"type": "text", "text": ""}),
                        typed("text_delta", "text", text.take()),
                    )
                } else if let Some(input) = delta["toolUse"].get_mut("input") {
                    (
                        Value::Null,
                        typed("input_json_delta", "partial_json", input.take()),
                    )
                } else if let Some(reasoning) = delta.get_mut("reasoningContent") {
                    if let Some(sig) = reasoning.get_mut("signature") {
                        (
                            json!({"type": "thinking", "thinking": ""}),
                            typed("signature_delta", "signature", sig.take()),
                        )
                    } else if let Some(data) = reasoning.get_mut("redactedContent") {
                        self.open.push(index);
                        let block = typed("redacted_thinking", "data", data.take());
                        return vec![block_event(
                            "content_block_start",
                            index,
                            "content_block",
                            block,
                        )];
                    } else {
                        (
                            json!({"type": "thinking", "thinking": ""}),
                            typed("thinking_delta", "thinking", reasoning["text"].take()),
                        )
                    }
                } else {
                    return Vec::new();
                };
                if !start_block.is_null() && !self.open.contains(&index) {
                    self.open.push(index);
                    events.push(block_event(
                        "content_block_start",
                        index,
                        "content_block",
                        start_block,
                    ));
                }
                events.push(block_event(
                    "content_block_delta",
                    index,
                    "delta",
                    anthropic_delta,
                ));
                events
            }
            "contentBlockStop" => {
                self.open.retain(|i| *i != index);
                vec![json!({"type": "content_block_stop", "index": index})]
            }
            "messageStop" => vec![json!({
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason(ev["stopReason"].as_str()), "stop_sequence": null}
            })],
            "metadata" => vec![
                object([
                    ("type", "message_delta".into()),
                    ("delta", json!({})),
                    ("usage", usage(&ev["usage"])),
                ]),
                json!({"type": "message_stop"}),
            ],
            _ => Vec::new(),
        }
    }
}

/// `{"type": <kind>, <key>: <value>}` — the shape of every Anthropic delta and block.
fn typed(kind: &str, key: &str, value: Value) -> Value {
    object([("type", kind.into()), (key, value)])
}

fn block_event(kind: &str, index: u64, key: &str, payload: Value) -> Value {
    object([
        ("type", kind.into()),
        ("index", index.into()),
        (key, payload),
    ])
}

fn message(mut m: Value) -> Value {
    let content = match m["content"].take() {
        Value::String(text) => vec![object([("text", text.into())])],
        Value::Array(blocks) => blocks.into_iter().flat_map(content_block).collect(),
        _ => Vec::new(),
    };
    object([
        ("role", m["role"].take()),
        ("content", Value::Array(content)),
    ])
}

/// One Messages content block as Converse blocks; a `cache_control` marker
/// becomes a following `cachePoint`.
fn content_block(mut block: Value) -> Vec<Value> {
    let cache_control = block.get_mut("cache_control").map(Value::take);
    let mapped = match block["type"].as_str() {
        Some("text") => object([("text", block["text"].take())]),
        Some("image") => {
            let mut source = block["source"].take();
            let format = source["media_type"]
                .as_str()
                .and_then(|m| m.strip_prefix("image/"))
                .unwrap_or("png");
            let image = object([
                ("format", format.into()),
                ("source", object([("bytes", source["data"].take())])),
            ]);
            object([("image", image)])
        }
        Some("tool_use") => {
            let tool = object([
                ("toolUseId", block["id"].take()),
                ("name", block["name"].take()),
                ("input", block["input"].take()),
            ]);
            object([("toolUse", tool)])
        }
        Some("tool_result") => {
            let mut content = match block["content"].take() {
                Value::String(text) => vec![object([("text", text.into())])],
                Value::Array(blocks) => blocks
                    .into_iter()
                    .flat_map(content_block)
                    .filter(|b| b.get("cachePoint").is_none())
                    .collect(),
                _ => Vec::new(),
            };
            if content.is_empty() {
                content.push(json!({"json": {}}));
            }
            let mut result = object([
                ("toolUseId", block["tool_use_id"].take()),
                ("content", Value::Array(content)),
            ]);
            if block["is_error"] == true {
                result["status"] = "error".into();
            }
            object([("toolResult", result)])
        }
        Some("thinking") => {
            let text = object([
                ("text", block["thinking"].take()),
                ("signature", block["signature"].take()),
            ]);
            object([("reasoningContent", object([("reasoningText", text)]))])
        }
        Some("redacted_thinking") => object([(
            "reasoningContent",
            object([("redactedContent", block["data"].take())]),
        )]),
        _ => return Vec::new(),
    };
    let mut blocks = vec![mapped];
    if let Some(control) = cache_control {
        blocks.push(cache_point(control));
    }
    blocks
}

/// `strict` reaches Bedrock only for Claude — the other families reject the
/// field outright ("This model doesn't support the strict field").
fn tool_spec(mut tool: Value, claude: bool) -> Vec<Value> {
    let cache_control = tool.get_mut("cache_control").map(Value::take);
    let mut spec = Map::with_capacity(4);
    spec.insert("name".into(), tool["name"].take());
    if let Some(d) = tool.get_mut("description").filter(|d| !d.is_null()) {
        spec.insert("description".into(), d.take());
    }
    let schema = match tool["input_schema"].take() {
        Value::Null => json!({"type": "object", "properties": {}}),
        schema => schema,
    };
    spec.insert("inputSchema".into(), object([("json", schema)]));
    if claude && let Some(strict) = tool.get_mut("strict").filter(|strict| !strict.is_null()) {
        spec.insert("strict".into(), strict.take());
    }
    let mut tools = vec![object([("toolSpec", Value::Object(spec))])];
    if let Some(control) = cache_control {
        tools.push(cache_point(control));
    }
    tools
}

fn cache_point(mut control: Value) -> Value {
    let mut point = Map::with_capacity(2);
    point.insert("type".into(), "default".into());
    if let Some(ttl) = control.get_mut("ttl").filter(|ttl| !ttl.is_null()) {
        point.insert("ttl".into(), ttl.take());
    }
    object([("cachePoint", Value::Object(point))])
}

fn converse_tool_choice(choice: Value) -> (bool, Option<Value>) {
    let Value::Object(mut fields) = choice else {
        return (false, None);
    };
    match fields.remove("type").as_ref().and_then(Value::as_str) {
        Some("none") => (true, None),
        Some("any") => (false, Some(json!({"any": {}}))),
        Some("auto") => (false, Some(json!({"auto": {}}))),
        Some("tool") => {
            let name = fields.remove("name").unwrap_or(Value::Null);
            (false, Some(object([("tool", object([("name", name)]))])))
        }
        _ => (false, None),
    }
}

fn anthropic_block(mut block: Value) -> Option<Value> {
    if let Some(text) = block.get_mut("text") {
        return Some(typed("text", "text", text.take()));
    }
    if let Some(tool) = block.get_mut("toolUse") {
        return Some(object([
            ("type", "tool_use".into()),
            ("id", tool["toolUseId"].take()),
            ("name", tool["name"].take()),
            ("input", tool["input"].take()),
        ]));
    }
    if let Some(reasoning) = block.get_mut("reasoningContent") {
        if let Some(text) = reasoning.get_mut("reasoningText") {
            return Some(object([
                ("type", "thinking".into()),
                ("thinking", text["text"].take()),
                ("signature", text["signature"].take()),
            ]));
        }
        if let Some(data) = reasoning.get_mut("redactedContent") {
            return Some(typed("redacted_thinking", "data", data.take()));
        }
    }
    None
}

fn usage(u: &Value) -> Value {
    let mut out = Map::with_capacity(5);
    for (from, to) in [
        ("inputTokens", "input_tokens"),
        ("outputTokens", "output_tokens"),
        ("cacheReadInputTokens", "cache_read_input_tokens"),
        ("cacheWriteInputTokens", "cache_creation_input_tokens"),
    ] {
        if let Some(n) = u.get(from).filter(|n| !n.is_null()) {
            out.insert(to.into(), n.clone());
        }
    }
    if let Some(tokens) = u["cacheDetails"].as_array().and_then(|details| {
        details
            .iter()
            .find(|detail| detail["ttl"] == "1h")
            .and_then(|detail| detail["inputTokens"].as_i64())
    }) {
        out.insert(
            "cache_creation".into(),
            json!({"ephemeral_1h_input_tokens": tokens}),
        );
    }
    Value::Object(out)
}

fn stop_reason(converse: Option<&str>) -> &'static str {
    match converse {
        Some("tool_use") => "tool_use",
        Some("max_tokens") => "max_tokens",
        Some("stop_sequence") => "stop_sequence",
        Some("guardrail_intervened" | "content_filtered") => "refusal",
        Some("malformed_model_output") => "malformed_model_output",
        Some("malformed_tool_use") => "malformed_tool_use",
        Some("model_context_window_exceeded") => "model_context_window_exceeded",
        _ => "end_turn",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_body_transcodes_to_converse() {
        let body: Map<String, Value> = serde_json::from_value(json!({
            "model": "x", "stream": true, "max_tokens": 64, "temperature": 0.2,
            "stop_sequences": ["END"],
            "system": [{"type": "text", "text": "be brief", "cache_control": {"type": "ephemeral", "ttl": "1h"}}],
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": [
                    {"type": "thinking", "thinking": "plan", "signature": "sig"},
                    {"type": "tool_use", "id": "t1", "name": "get_weather", "input": {"city": "Paris"}}]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "t1", "content": "sunny"},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AAAA"}}]}
            ],
            "tools": [{"name": "get_weather", "input_schema": {"type": "object"}, "strict": true,
                       "cache_control": {"type": "ephemeral", "ttl": "1h"}}],
            "tool_choice": {"type": "any"},
            "thinking": {"type": "enabled", "budget_tokens": 1024},
            "seed": 7
        }))
        .unwrap();
        let out = request(body, true);
        assert_eq!(
            out["system"],
            json!([{"text": "be brief"}, {"cachePoint": {"type": "default", "ttl": "1h"}}])
        );
        assert_eq!(
            out["messages"][0],
            json!({"role": "user", "content": [{"text": "hi"}]})
        );
        assert_eq!(
            out["messages"][1]["content"],
            json!([
                {"reasoningContent": {"reasoningText": {"text": "plan", "signature": "sig"}}},
                {"toolUse": {"toolUseId": "t1", "name": "get_weather", "input": {"city": "Paris"}}}
            ])
        );
        assert_eq!(
            out["messages"][2]["content"],
            json!([
                {"toolResult": {"toolUseId": "t1", "content": [{"text": "sunny"}]}},
                {"image": {"format": "png", "source": {"bytes": "AAAA"}}}
            ])
        );
        assert_eq!(
            out["inferenceConfig"],
            json!({"maxTokens": 64, "temperature": 0.2, "stopSequences": ["END"]})
        );
        assert_eq!(
            out["toolConfig"],
            json!({"tools": [
                       {"toolSpec": {"name": "get_weather", "inputSchema": {"json": {"type": "object"}}, "strict": true}},
                       {"cachePoint": {"type": "default", "ttl": "1h"}}
                   ],
                   "toolChoice": {"any": {}}})
        );
        assert_eq!(
            out["additionalModelRequestFields"],
            json!({"thinking": {"type": "enabled", "budget_tokens": 1024}, "seed": 7})
        );
        assert!(out.get("model").is_none() && out.get("stream").is_none());

        let body: Map<String, Value> =
            serde_json::from_value(json!({"messages": [], "thinking": {"type": "adaptive"}}))
                .unwrap();
        assert!(
            request(body, false)
                .get("additionalModelRequestFields")
                .is_none()
        );
    }

    #[test]
    fn anthropic_tool_choices_map_to_converse() {
        let body = serde_json::from_value(json!({
            "messages": [],
            "tools": [{"name": "get_weather", "input_schema": {"type": "object"}, "strict": true}],
            "tool_choice": {"type": "tool", "name": "get_weather"}
        }))
        .unwrap();
        let out = request(body, false);
        assert_eq!(
            out["toolConfig"]["toolChoice"],
            json!({"tool": {"name": "get_weather"}})
        );
        assert!(
            out["toolConfig"]["tools"][0]["toolSpec"]
                .get("strict")
                .is_none(),
            "non-Claude Bedrock models reject strict"
        );

        let body = serde_json::from_value(json!({
            "messages": [],
            "tools": [{"name": "get_weather", "input_schema": {"type": "object"}}],
            "tool_choice": {"type": "none"}
        }))
        .unwrap();
        assert!(request(body, false).get("toolConfig").is_none());
    }

    #[test]
    fn converse_reply_becomes_a_messages_reply() {
        let v = json!({
            "output": {"message": {"role": "assistant", "content": [
                {"reasoningContent": {"reasoningText": {"text": "hmm", "signature": "s"}}},
                {"text": "hello"},
                {"toolUse": {"toolUseId": "t1", "name": "f", "input": {"a": 1}}}]}},
            "stopReason": "tool_use",
            "usage": {"inputTokens": 10, "outputTokens": 4, "totalTokens": 14,
                      "cacheReadInputTokens": 3, "cacheWriteInputTokens": 5,
                      "cacheDetails": [{"ttl": "1h", "inputTokens": 3}, {"ttl": "5m", "inputTokens": 2}]}
        });
        let m = reply(v, "eu.amazon.nova-micro-v1:0");
        assert_eq!(m["stop_reason"], "tool_use");
        assert_eq!(m["content"][0]["type"], "thinking");
        assert_eq!(m["content"][1]["text"], "hello");
        assert_eq!(m["content"][2]["id"], "t1");
        assert_eq!(m["usage"]["input_tokens"], 10);
        assert_eq!(m["usage"]["cache_read_input_tokens"], 3);
        assert_eq!(m["usage"]["cache_creation_input_tokens"], 5);
        assert_eq!(m["usage"]["cache_creation"]["ephemeral_1h_input_tokens"], 3);
        let usage = crate::usage_extract::extract_common_usage(&m["usage"], true).unwrap();
        assert_eq!((usage.write_cache, usage.write_cache_1h), (5, 3));
        assert_eq!(m["model"], "eu.amazon.nova-micro-v1:0");
    }

    #[test]
    fn converse_failure_stop_reasons_survive() {
        for reason in [
            "malformed_model_output",
            "malformed_tool_use",
            "model_context_window_exceeded",
        ] {
            assert_eq!(stop_reason(Some(reason)), reason);
        }
    }

    #[test]
    fn stream_events_become_the_anthropic_sequence() {
        let mut ev = Events::new("m".into());
        let mut out = Vec::new();
        for e in [
            json!({"type": "messageStart", "role": "assistant"}),
            json!({"type": "contentBlockDelta", "contentBlockIndex": 0, "delta": {"reasoningContent": {"text": "th"}}}),
            json!({"type": "contentBlockDelta", "contentBlockIndex": 0, "delta": {"reasoningContent": {"signature": "sig"}}}),
            json!({"type": "contentBlockStop", "contentBlockIndex": 0}),
            json!({"type": "contentBlockDelta", "contentBlockIndex": 1, "delta": {"text": "he"}}),
            json!({"type": "contentBlockDelta", "contentBlockIndex": 1, "delta": {"text": "llo"}}),
            json!({"type": "contentBlockStop", "contentBlockIndex": 1}),
            json!({"type": "contentBlockStart", "contentBlockIndex": 2, "start": {"toolUse": {"toolUseId": "t1", "name": "f"}}}),
            json!({"type": "contentBlockDelta", "contentBlockIndex": 2, "delta": {"toolUse": {"input": "{\"a\":1}"}}}),
            json!({"type": "contentBlockStop", "contentBlockIndex": 2}),
            json!({"type": "messageStop", "stopReason": "tool_use"}),
            json!({"type": "metadata", "usage": {"inputTokens": 5, "outputTokens": 9}}),
        ] {
            out.extend(ev.map(e));
        }
        let types: Vec<&str> = out.iter().map(|e| e["type"].as_str().unwrap()).collect();
        assert_eq!(
            types,
            [
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_delta",
                "content_block_stop",
                "content_block_start",
                "content_block_delta",
                "content_block_delta",
                "content_block_stop",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_delta",
                "message_stop"
            ]
        );
        assert_eq!(out[1]["content_block"]["type"], "thinking");
        assert_eq!(out[3]["delta"]["signature"], "sig");
        assert_eq!(out[5]["content_block"]["type"], "text");
        assert_eq!(out[10]["delta"]["partial_json"], "{\"a\":1}");
        assert_eq!(out[12]["delta"]["stop_reason"], "tool_use");
        assert_eq!(out[13]["usage"]["output_tokens"], 9);
    }
}
