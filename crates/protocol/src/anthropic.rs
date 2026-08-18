//! Anthropic-compatible wire types.
//!
//! Full messages surface: system, content blocks (text / tool_use / tool_result),
//! and tools. The streaming event sequence is emitted by the views layer.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

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
    #[serde(default)]
    pub thinking: Option<Value>,
    #[serde(default)]
    pub output_config: Option<Value>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

impl MessagesRequest {
    /// Take the system prompt (string or text blocks) as plain text.
    pub fn system_text(&mut self) -> Option<String> {
        let text = match self.system.take()? {
            Value::String(s) => s,
            Value::Array(blocks) => blocks.iter().filter_map(|b| b["text"].as_str()).collect(),
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

/// An OpenAI `image_url` part as an Anthropic `image` block (data URI → base64
/// source, else URL source); other parts pass through.
pub fn image_url_to_image(mut part: Value) -> Value {
    if part["type"] != "image_url" {
        return part;
    }
    let url = match part["image_url"].get_mut("url").map(Value::take) {
        Some(Value::String(url)) => url,
        _ => return part,
    };
    let source = match url
        .strip_prefix("data:")
        .and_then(|rest| rest.split_once(";base64,"))
    {
        Some((media_type, data)) => {
            serde_json::json!({"type": "base64", "media_type": media_type, "data": data})
        }
        None => serde_json::json!({"type": "url", "url": url}),
    };
    serde_json::json!({"type": "image", "source": source})
}

/// Inverse of [`image_url_to_image`]: an Anthropic `image` block as an OpenAI
/// `image_url` part (base64 sources become data URIs).
pub fn image_to_image_url(mut block: Value) -> Value {
    if block["type"] != "image" {
        return block;
    }
    let source = block["source"].take();
    let url = match source["type"].as_str() {
        Some("base64") => format!(
            "data:{};base64,{}",
            source["media_type"].as_str().unwrap_or("image/png"),
            source["data"].as_str().unwrap_or_default()
        ),
        _ => match source.get("url").and_then(Value::as_str) {
            Some(url) => url.to_owned(),
            None => return block,
        },
    };
    serde_json::json!({"type": "image_url", "image_url": {"url": url}})
}

/// OpenAI-shaped tool calls as Anthropic `tool_use` blocks; the `arguments`
/// string parses into `input` (kept verbatim when unparseable).
pub fn tool_calls_to_tool_use(calls: Vec<Value>) -> Vec<Value> {
    calls
        .into_iter()
        .filter_map(|mut c| {
            let Some(Value::Object(mut f)) = c.get_mut("function").map(Value::take) else {
                return None;
            };
            let input = match f.remove("arguments") {
                Some(Value::String(s)) => serde_json::from_str(&s).unwrap_or(Value::String(s)),
                other => other.unwrap_or_default(),
            };
            let mut block = Map::new();
            block.insert("type".into(), "tool_use".into());
            block.insert("id".into(), c["id"].take());
            block.insert("name".into(), f.remove("name").unwrap_or_default());
            block.insert("input".into(), input);
            Some(Value::Object(block))
        })
        .collect()
}

/// Inverse of [`tool_calls_to_tool_use`]: `input` becomes the JSON-encoded
/// `arguments`, each call takes the next `index`; other entries pass through.
pub fn tool_use_to_tool_calls(blocks: Vec<Value>, index: &mut usize) -> Vec<Value> {
    blocks
        .into_iter()
        .map(|mut b| {
            if b["type"] != "tool_use" {
                return b;
            }
            let arguments = match &b["input"] {
                Value::Null => "{}".to_owned(),
                input => input.to_string(),
            };
            let call = serde_json::json!({
                "index": *index, "id": b["id"].take(), "type": "function",
                "function": {"name": b["name"].take(), "arguments": arguments}
            });
            *index += 1;
            call
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[test]
    fn image_parts_convert_both_ways() {
        let part = json!({"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}});
        let block = image_url_to_image(part.clone());
        assert_eq!(
            block,
            json!({"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}})
        );
        assert_eq!(image_to_image_url(block), part);
        let remote = json!({"type":"image_url","image_url":{"url":"https://x/y.png"}});
        assert_eq!(
            image_url_to_image(remote.clone()),
            json!({"type":"image","source":{"type":"url","url":"https://x/y.png"}})
        );
        assert_eq!(
            image_to_image_url(image_url_to_image(remote.clone())),
            remote
        );
        let text = json!({"type":"text","text":"hi"});
        assert_eq!(image_url_to_image(text.clone()), text);
        assert_eq!(image_to_image_url(text.clone()), text);
    }

    use super::*;

    #[test]
    fn tool_use_blocks_convert_to_tool_calls() {
        let blocks = vec![
            json!({"type":"tool_use","id":"toolu_1","name":"shell","input":{"command":"ls -la"}}),
            json!({"type":"text","text":"kept as is"}),
            json!({"id":"call_9","type":"function","function":{"name":"kept","arguments":"{}"}}),
            json!({"type":"tool_use","id":"toolu_2","name":"noop"}),
        ];
        let mut index = 0;
        let calls = tool_use_to_tool_calls(blocks, &mut index);
        assert_eq!(calls.len(), 4);
        assert_eq!(calls[0]["index"], 0);
        assert_eq!(calls[0]["id"], "toolu_1");
        assert_eq!(calls[0]["type"], "function");
        assert_eq!(calls[0]["function"]["name"], "shell");
        assert_eq!(
            calls[0]["function"]["arguments"],
            "{\"command\":\"ls -la\"}"
        );
        assert_eq!(calls[1]["type"], "text");
        assert_eq!(calls[2]["id"], "call_9");
        assert_eq!(calls[3]["index"], 1);
        assert_eq!(calls[3]["function"]["arguments"], "{}");
        assert_eq!(index, 2);
        let back = tool_calls_to_tool_use(calls);
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
        let blocks = tool_calls_to_tool_use(calls);
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
        let mut r: MessagesRequest =
            serde_json::from_str(r#"{"model":"m","system":"be brief","messages":[]}"#).unwrap();
        assert_eq!(r.system_text().unwrap(), "be brief");
        let mut r: MessagesRequest = serde_json::from_str(
            r#"{"model":"m","system":[{"type":"text","text":"a"},{"type":"text","text":"b"}],"messages":[]}"#,
        )
        .unwrap();
        assert_eq!(r.system_text().unwrap(), "ab");
    }
}
