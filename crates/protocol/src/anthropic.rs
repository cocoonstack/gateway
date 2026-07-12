//! Anthropic-compatible wire types.
//!
//! Full messages surface: system, content blocks (text / tool_use / tool_result),
//! tools, and the standard streaming event sequence (message_start →
//! content_block_* → message_delta → message_stop).

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

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

impl InMessage {
    /// Flatten the content to plain text (string form or text blocks).
    pub fn text(&self) -> String {
        match &self.content {
            Value::String(s) => s.clone(),
            Value::Array(blocks) => blocks
                .iter()
                .filter(|b| b["type"] == "text" || b.get("type").is_none())
                .filter_map(|b| b["text"].as_str())
                .collect::<Vec<_>>()
                .join(""),
            _ => String::new(),
        }
    }
}

/// Output content block: text or tool_use.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AnthUsage {
    pub input_tokens: i64,
    pub output_tokens: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagesResponse {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String, // "message"
    pub role: String, // "assistant"
    pub model: String,
    pub content: Vec<ContentBlock>,
    pub stop_reason: String,
    pub usage: AnthUsage,
}

impl MessagesResponse {
    pub fn new(
        id: impl Into<String>,
        model: impl Into<String>,
        content: Vec<ContentBlock>,
        stop_reason: impl Into<String>,
        usage: AnthUsage,
    ) -> Self {
        Self {
            id: id.into(),
            kind: "message".to_owned(),
            role: "assistant".to_owned(),
            model: model.into(),
            content,
            stop_reason: stop_reason.into(),
            usage,
        }
    }

    pub fn text(
        id: impl Into<String>,
        model: impl Into<String>,
        text: impl Into<String>,
        stop_reason: impl Into<String>,
        usage: AnthUsage,
    ) -> Self {
        Self::new(
            id,
            model,
            vec![ContentBlock::Text { text: text.into() }],
            stop_reason,
            usage,
        )
    }
}

/// One streaming event: `(event_name, data_payload)`. The standard sequence for
/// a text reply — used by the /v1/messages SSE surface and mirrored by the mock.
pub fn stream_events(
    id: &str,
    model: &str,
    text_deltas: &[String],
    stop_reason: &str,
    usage: &AnthUsage,
) -> Vec<(&'static str, Value)> {
    let mut ev = Vec::with_capacity(text_deltas.len() + 5);
    ev.push((
        "message_start",
        json!({"type":"message_start","message":{
            "id":id,"type":"message","role":"assistant","model":model,
            "content":[],"stop_reason":null,
            "usage":{"input_tokens":usage.input_tokens,"output_tokens":0}}}),
    ));
    ev.push((
        "content_block_start",
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
    ));
    for d in text_deltas {
        ev.push((
            "content_block_delta",
            json!({"type":"content_block_delta","index":0,
                   "delta":{"type":"text_delta","text":d}}),
        ));
    }
    ev.push((
        "content_block_stop",
        json!({"type":"content_block_stop","index":0}),
    ));
    ev.push((
        "message_delta",
        json!({"type":"message_delta","delta":{"stop_reason":stop_reason},
               "usage":{"output_tokens":usage.output_tokens}}),
    ));
    ev.push(("message_stop", json!({"type":"message_stop"})));
    ev
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_string_and_blocks() {
        let m: InMessage = serde_json::from_str(r#"{"role":"user","content":"hi"}"#).unwrap();
        assert_eq!(m.text(), "hi");
        let m: InMessage = serde_json::from_str(
            r#"{"role":"user","content":[{"type":"text","text":"a"},{"type":"text","text":"b"}]}"#,
        )
        .unwrap();
        assert_eq!(m.text(), "ab");
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

    #[test]
    fn content_block_tagging() {
        let b = ContentBlock::ToolUse {
            id: "tu-1".into(),
            name: "get_weather".into(),
            input: json!({"city":"sf"}),
        };
        let v = serde_json::to_value(&b).unwrap();
        assert_eq!(v["type"], "tool_use");
        assert_eq!(v["name"], "get_weather");
        let t: ContentBlock = serde_json::from_value(json!({"type":"text","text":"x"})).unwrap();
        assert!(matches!(t, ContentBlock::Text { text } if text == "x"));
    }

    #[test]
    fn stream_event_sequence_shape() {
        let ev = stream_events(
            "msg-1",
            "anthropic-messages",
            &["he".into(), "llo".into()],
            "end_turn",
            &AnthUsage {
                input_tokens: 3,
                output_tokens: 5,
            },
        );
        let names: Vec<&str> = ev.iter().map(|(n, _)| *n).collect();
        assert_eq!(
            names,
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop"
            ]
        );
        assert_eq!(ev[0].1["message"]["usage"]["input_tokens"], 3);
        assert_eq!(ev[5].1["usage"]["output_tokens"], 5);
    }
}
