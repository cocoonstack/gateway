//! Anthropic-compatible wire types.
//!
//! Full messages surface: system, content blocks (text / tool_use / tool_result),
//! and tools. The streaming event sequence is emitted by the views layer.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
pub struct MessagesRequest {
    pub model: String,
    #[serde(default)]
    pub max_tokens: Option<i64>,
    #[serde(default)]
    pub messages: Vec<InMessage>,
    #[serde(default)]
    pub stream: bool,
    /// string or [{type:"text",text}] blocks
    #[serde(default)]
    pub system: Option<Value>,
    #[serde(default)]
    pub tools: Option<Vec<Value>>,
    #[serde(default)]
    pub tool_choice: Option<Value>,
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    #[serde(default)]
    pub stop_sequences: Option<Vec<String>>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

impl MessagesRequest {
    /// Flatten the system prompt (string or text blocks) to plain text.
    pub fn system_text(&self) -> Option<String> {
        let sys = self.system.as_ref()?;
        let text = match sys {
            Value::String(s) => s.clone(),
            Value::Array(blocks) => blocks
                .iter()
                .filter_map(|b| b["text"].as_str())
                .collect::<Vec<_>>()
                .join(""),
            _ => return None,
        };
        (!text.is_empty()).then_some(text)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct InMessage {
    pub role: String,
    /// string or [{type:"text"|"image"|"tool_result"|...}] blocks
    pub content: Value,
}

/// Joined text of the `text`/untyped blocks in an Anthropic content array.
pub fn blocks_text(blocks: &[Value]) -> String {
    blocks
        .iter()
        .filter(|b| b["type"] == "text" || b.get("type").is_none())
        .filter_map(|b| b["text"].as_str())
        .collect()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AnthUsage {
    pub input_tokens: i64,
    pub output_tokens: i64,
    /// Prompt-cache accounting (the real API always sends these; 0 = none).
    #[serde(default)]
    pub cache_read_input_tokens: i64,
    #[serde(default)]
    pub cache_creation_input_tokens: i64,
}

/// Convert OpenAI-shaped tool calls (`{id, function: {name, arguments}}`) into
/// Anthropic `tool_use` content blocks. `arguments` is a JSON string on the
/// OpenAI wire; it parses into the block's structured `input` (kept verbatim
/// when unparseable). Entries without a `function` are skipped.
pub fn tool_calls_to_tool_use(calls: &[Value]) -> Vec<Value> {
    calls
        .iter()
        .filter(|c| c.get("function").is_some())
        .map(|c| {
            let f = &c["function"];
            let input = f["arguments"]
                .as_str()
                .and_then(|s| serde_json::from_str::<Value>(s).ok())
                .unwrap_or_else(|| f["arguments"].clone());
            serde_json::json!({"type": "tool_use", "id": c["id"], "name": f["name"], "input": input})
        })
        .collect()
}

/// Convert Anthropic `tool_use` blocks into OpenAI-shaped tool calls
/// (`{id, type: "function", function: {name, arguments}}`), the inverse of
/// [`tool_calls_to_tool_use`]. The block's structured `input` becomes the
/// JSON-encoded `arguments` string. Entries already carrying a `function`
/// pass through unchanged, so mixed engine output renders once.
pub fn tool_use_to_tool_calls(blocks: &[Value]) -> Vec<Value> {
    blocks
        .iter()
        .filter_map(|b| {
            if b.get("function").is_some() {
                return Some(b.clone());
            }
            if b["type"] != "tool_use" {
                return None;
            }
            let arguments = match &b["input"] {
                Value::Null => "{}".to_owned(),
                input => input.to_string(),
            };
            Some(serde_json::json!({
                "id": b["id"], "type": "function",
                "function": {"name": b["name"], "arguments": arguments}
            }))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn tool_use_blocks_convert_to_tool_calls() {
        let blocks = vec![
            json!({"type":"tool_use","id":"toolu_1","name":"shell","input":{"command":"ls -la"}}),
            json!({"type":"text","text":"I'll list the files."}),
            json!({"id":"call_9","type":"function","function":{"name":"kept","arguments":"{}"}}),
            json!({"type":"tool_use","id":"toolu_2","name":"noop"}),
        ];
        let calls = tool_use_to_tool_calls(&blocks);
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0]["id"], "toolu_1");
        assert_eq!(calls[0]["type"], "function");
        assert_eq!(calls[0]["function"]["name"], "shell");
        assert_eq!(
            calls[0]["function"]["arguments"],
            "{\"command\":\"ls -la\"}"
        );
        assert_eq!(calls[1]["id"], "call_9");
        assert_eq!(calls[2]["function"]["arguments"], "{}");
        let back = tool_calls_to_tool_use(&calls);
        assert_eq!(back[0]["input"], json!({"command":"ls -la"}));
    }

    #[test]
    fn tool_calls_convert_to_tool_use_blocks() {
        let calls = vec![
            json!({"id":"call_1","type":"function",
                   "function":{"name":"get_weather","arguments":"{\"city\":\"sf\"}"}}),
            json!({"id":"call_2","function":{"name":"raw","arguments":"not json"}}),
            json!({"type":"tool_use","id":"native"}),
        ];
        let blocks = tool_calls_to_tool_use(&calls);
        assert_eq!(blocks.len(), 2, "no-function entries are skipped");
        assert_eq!(blocks[0]["type"], "tool_use");
        assert_eq!(blocks[0]["id"], "call_1");
        assert_eq!(blocks[0]["name"], "get_weather");
        assert_eq!(blocks[0]["input"]["city"], "sf");
        assert_eq!(
            blocks[1]["input"], "not json",
            "unparseable arguments ride verbatim"
        );
    }

    #[test]
    fn content_blocks_flatten_to_text() {
        let m: InMessage = serde_json::from_str(
            r#"{"role":"user","content":[{"type":"text","text":"a"},{"type":"text","text":"b"}]}"#,
        )
        .unwrap();
        let Value::Array(blocks) = &m.content else {
            panic!("blocks form")
        };
        assert_eq!(blocks_text(blocks), "ab");
    }

    #[test]
    fn system_flattening() {
        let r: MessagesRequest =
            serde_json::from_str(r#"{"model":"m","system":"be brief","messages":[]}"#).unwrap();
        assert_eq!(r.system_text().unwrap(), "be brief");
        let r: MessagesRequest = serde_json::from_str(
            r#"{"model":"m","system":[{"type":"text","text":"a"},{"type":"text","text":"b"}],"messages":[]}"#,
        )
        .unwrap();
        assert_eq!(r.system_text().unwrap(), "ab");
    }
}
