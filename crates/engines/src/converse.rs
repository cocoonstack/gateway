//! Bedrock Converse — the vendor-neutral Bedrock chat API — driven through the
//! Anthropic Messages engine: the Messages body the engine builds is transcoded
//! into a Converse request, and Converse replies (buffered JSON, streamed
//! events) are transcoded back into the Messages shapes the engine, the views
//! and the native `/v1/messages` stream already understand.

use serde_json::{Map, Value, json};

/// A Messages request body as a Converse body. Claude-only knobs (`thinking`,
/// `output_config`) and any passthrough extras ride in
/// `additionalModelRequestFields`; on non-Claude ids the Claude knobs drop.
pub(crate) fn request(mut body: Map<String, Value>, claude: bool) -> Value {
    let mut out = Map::with_capacity(6);
    if let Some(system) = body.remove("system") {
        let blocks = match system {
            Value::String(text) => vec![json!({"text": text})],
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
        Some(Value::Array(tools)) => tools.into_iter().map(tool_spec).collect(),
        _ => Vec::new(),
    };
    let tool_choice = body.remove("tool_choice");
    if !tools.is_empty() {
        let mut config = Map::with_capacity(2);
        config.insert("tools".into(), Value::Array(tools));
        if let Some(choice) = tool_choice.and_then(|c| match c["type"].as_str() {
            Some("any") => Some(json!({"any": {}})),
            Some("tool") => Some(json!({"tool": {"name": c["name"]}})),
            Some("auto") => Some(json!({"auto": {}})),
            _ => None,
        }) {
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
    json!({
        "id": "msg_converse",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content,
        "stop_reason": stop_reason(v["stopReason"].as_str()),
        "stop_sequence": null,
        "usage": usage,
    })
}

/// Converse stream events (typed by the EventStream adapter) as the Anthropic
/// event sequence. Converse opens text and reasoning blocks implicitly, so
/// their `content_block_start` is synthesized on the first delta; usage
/// arrives in a trailing `metadata` event, which becomes the `message_delta`
/// usage overlay followed by `message_stop`.
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
                    json!({"type": "tool_use", "id": tool["toolUseId"].take(),
                           "name": tool["name"].take(), "input": {}})
                } else {
                    json!({"type": "text", "text": ""})
                };
                self.open.push(index);
                vec![json!({"type": "content_block_start", "index": index, "content_block": block})]
            }
            "contentBlockDelta" => {
                let mut delta = ev["delta"].take();
                let mut events = Vec::with_capacity(2);
                let (start_block, anthropic_delta) = if let Some(text) = delta.get_mut("text") {
                    (
                        json!({"type": "text", "text": ""}),
                        json!({"type": "text_delta", "text": text.take()}),
                    )
                } else if let Some(input) = delta["toolUse"].get_mut("input") {
                    (
                        Value::Null,
                        json!({"type": "input_json_delta", "partial_json": input.take()}),
                    )
                } else if let Some(reasoning) = delta.get_mut("reasoningContent") {
                    if let Some(sig) = reasoning.get_mut("signature") {
                        (
                            json!({"type": "thinking", "thinking": ""}),
                            json!({"type": "signature_delta", "signature": sig.take()}),
                        )
                    } else if let Some(data) = reasoning.get_mut("redactedContent") {
                        self.open.push(index);
                        return vec![json!({"type": "content_block_start", "index": index,
                            "content_block": {"type": "redacted_thinking", "data": data.take()}})];
                    } else {
                        (
                            json!({"type": "thinking", "thinking": ""}),
                            json!({"type": "thinking_delta",
                                   "thinking": reasoning["text"].take()}),
                        )
                    }
                } else {
                    return Vec::new();
                };
                if !start_block.is_null() && !self.open.contains(&index) {
                    self.open.push(index);
                    events.push(json!({"type": "content_block_start", "index": index,
                                       "content_block": start_block}));
                }
                events.push(json!({"type": "content_block_delta", "index": index,
                                   "delta": anthropic_delta}));
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
                json!({"type": "message_delta", "delta": {}, "usage": usage(&ev["usage"])}),
                json!({"type": "message_stop"}),
            ],
            _ => Vec::new(),
        }
    }
}

fn message(mut m: Value) -> Value {
    let content = match m["content"].take() {
        Value::String(text) => vec![json!({"text": text})],
        Value::Array(blocks) => blocks.into_iter().flat_map(content_block).collect(),
        _ => Vec::new(),
    };
    json!({"role": m["role"], "content": content})
}

/// One Messages content block as Converse blocks; a `cache_control` marker
/// becomes a following `cachePoint`.
fn content_block(mut block: Value) -> Vec<Value> {
    let cached = block.get("cache_control").is_some();
    let mapped = match block["type"].as_str() {
        Some("text") => json!({"text": block["text"].take()}),
        Some("image") => {
            let mut source = block["source"].take();
            let format = source["media_type"]
                .as_str()
                .and_then(|m| m.strip_prefix("image/"))
                .unwrap_or("png")
                .to_owned();
            json!({"image": {"format": format, "source": {"bytes": source["data"].take()}}})
        }
        Some("tool_use") => json!({"toolUse": {"toolUseId": block["id"].take(),
            "name": block["name"].take(), "input": block["input"].take()}}),
        Some("tool_result") => {
            let content = match block["content"].take() {
                Value::String(text) => vec![json!({"text": text})],
                Value::Array(blocks) => blocks
                    .into_iter()
                    .flat_map(content_block)
                    .filter(|b| b.get("cachePoint").is_none())
                    .collect(),
                _ => Vec::new(),
            };
            let mut result = json!({"toolUseId": block["tool_use_id"].take(),
                "content": if content.is_empty() { vec![json!({"json": {}})] } else { content }});
            if block["is_error"] == true {
                result["status"] = "error".into();
            }
            json!({"toolResult": result})
        }
        Some("thinking") => json!({"reasoningContent": {"reasoningText": {
            "text": block["thinking"].take(), "signature": block["signature"].take()}}}),
        Some("redacted_thinking") => {
            json!({"reasoningContent": {"redactedContent": block["data"].take()}})
        }
        _ => return Vec::new(),
    };
    if cached {
        vec![mapped, json!({"cachePoint": {"type": "default"}})]
    } else {
        vec![mapped]
    }
}

fn tool_spec(mut tool: Value) -> Value {
    let mut spec = Map::with_capacity(3);
    spec.insert("name".into(), tool["name"].take());
    if let Some(d) = tool.get_mut("description").filter(|d| !d.is_null()) {
        spec.insert("description".into(), d.take());
    }
    spec.insert(
        "inputSchema".into(),
        json!({"json": match tool["input_schema"].take() {
            Value::Null => json!({"type": "object", "properties": {}}),
            schema => schema,
        }}),
    );
    json!({"toolSpec": spec})
}

fn anthropic_block(mut block: Value) -> Option<Value> {
    if let Some(text) = block.get_mut("text") {
        return Some(json!({"type": "text", "text": text.take()}));
    }
    if let Some(tool) = block.get_mut("toolUse") {
        return Some(json!({"type": "tool_use", "id": tool["toolUseId"].take(),
            "name": tool["name"].take(), "input": tool["input"].take()}));
    }
    if let Some(reasoning) = block.get_mut("reasoningContent") {
        if let Some(text) = reasoning.get_mut("reasoningText") {
            return Some(json!({"type": "thinking", "thinking": text["text"].take(),
                "signature": text["signature"].take()}));
        }
        if let Some(data) = reasoning.get_mut("redactedContent") {
            return Some(json!({"type": "redacted_thinking", "data": data.take()}));
        }
    }
    None
}

fn usage(u: &Value) -> Value {
    let mut out = Map::with_capacity(4);
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
    Value::Object(out)
}

fn stop_reason(converse: Option<&str>) -> &'static str {
    match converse {
        Some("tool_use") => "tool_use",
        Some("max_tokens") => "max_tokens",
        Some("stop_sequence") => "stop_sequence",
        Some("guardrail_intervened" | "content_filtered") => "refusal",
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
            "system": [{"type": "text", "text": "be brief", "cache_control": {"type": "ephemeral"}}],
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": [
                    {"type": "thinking", "thinking": "plan", "signature": "sig"},
                    {"type": "tool_use", "id": "t1", "name": "get_weather", "input": {"city": "Paris"}}]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "t1", "content": "sunny"},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AAAA"}}]}
            ],
            "tools": [{"name": "get_weather", "input_schema": {"type": "object"}}],
            "tool_choice": {"type": "any"},
            "thinking": {"type": "enabled", "budget_tokens": 1024},
            "seed": 7
        }))
        .unwrap();
        let out = request(body, true);
        assert_eq!(
            out["system"],
            json!([{"text": "be brief"}, {"cachePoint": {"type": "default"}}])
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
            json!({"maxTokens": 64, "temperature": 0.2})
        );
        assert_eq!(
            out["toolConfig"],
            json!({"tools": [{"toolSpec": {"name": "get_weather", "inputSchema": {"json": {"type": "object"}}}}],
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
    fn converse_reply_becomes_a_messages_reply() {
        let v = json!({
            "output": {"message": {"role": "assistant", "content": [
                {"reasoningContent": {"reasoningText": {"text": "hmm", "signature": "s"}}},
                {"text": "hello"},
                {"toolUse": {"toolUseId": "t1", "name": "f", "input": {"a": 1}}}]}},
            "stopReason": "tool_use",
            "usage": {"inputTokens": 10, "outputTokens": 4, "totalTokens": 14, "cacheReadInputTokens": 3}
        });
        let m = reply(v, "eu.amazon.nova-micro-v1:0");
        assert_eq!(m["stop_reason"], "tool_use");
        assert_eq!(m["content"][0]["type"], "thinking");
        assert_eq!(m["content"][1]["text"], "hello");
        assert_eq!(m["content"][2]["id"], "t1");
        assert_eq!(m["usage"]["input_tokens"], 10);
        assert_eq!(m["usage"]["cache_read_input_tokens"], 3);
        assert!(m["usage"].get("cache_creation_input_tokens").is_none());
        assert_eq!(m["model"], "eu.amazon.nova-micro-v1:0");
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
