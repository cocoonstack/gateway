//! OpenAI-compatible wire types.
//!
//! Full chat surface: multimodal content (string | parts array), tools /
//! tool_choice / tool_calls, sampling params, logprobs & response_format
//! passthrough, streaming chunks.

use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `content` as a plain string or the multimodal parts array; parts stay
/// untyped so unknown modalities pass through.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageContent {
    Text(String),
    Parts(Vec<Value>),
}

impl MessageContent {
    /// Plain text (text parts joined; non-text parts skipped) plus the
    /// surrendered raw parts; the Text form moves its string out.
    pub fn into_text_and_parts(self) -> (String, Option<Vec<Value>>) {
        match self {
            MessageContent::Text(s) => (s, None),
            MessageContent::Parts(parts) => (parts_text(&parts), Some(parts)),
        }
    }
}

impl Default for MessageContent {
    fn default() -> Self {
        MessageContent::Text(String::new())
    }
}

/// One function call requested by the model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String, // "function"
    pub function: FunctionCall,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionCall {
    pub name: String,
    /// JSON-encoded arguments string (OpenAI wire format).
    pub arguments: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: Cow<'static, str>,
    /// `null` when the assistant message carries only tool_calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<MessageContent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
    /// present on role:"tool" result messages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// assistant reasoning prose (DeepSeek-style; the widest-adopted field).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    /// assistant reasoning units in emission order (OpenRouter-style
    /// `reasoning.text` / `reasoning.encrypted` / `reasoning.summary`), the
    /// signed/encrypted proof a later turn replays.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_details: Option<Vec<Value>>,
}

impl ChatMessage {
    pub fn text(role: impl Into<Cow<'static, str>>, content: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            content: Some(MessageContent::Text(content.into())),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    #[serde(default)]
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub stream: bool,
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub max_tokens: Option<i64>,
    #[serde(default)]
    pub stop: Option<Value>, // string | [string]
    pub presence_penalty: Option<f64>,
    pub frequency_penalty: Option<f64>,
    #[serde(default)]
    pub tools: Option<Vec<Value>>,
    #[serde(default)]
    pub tool_choice: Option<Value>,
    #[serde(default)]
    pub response_format: Option<Value>,
    pub logprobs: Option<bool>,
    pub top_logprobs: Option<i64>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    /// OpenRouter-style `{effort, max_tokens, enabled}`.
    #[serde(default)]
    pub reasoning: Option<Value>,
    /// Consumed here: the gateway always streams usage, and non-OpenAI wires
    /// reject the field.
    #[serde(default)]
    pub stream_options: Option<Value>,
    /// unrecognized fields ride along untouched and are passed through to vendors.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Usage {
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
    /// Cache/reasoning detail, emitted only when the vendor reported some.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_tokens_details: Option<PromptTokensDetails>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_tokens_details: Option<CompletionTokensDetails>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct PromptTokensDetails {
    pub cached_tokens: i64,
    /// Anthropic cache writes ride inside `prompt_tokens`; surfaced so a client
    /// can reconcile the write premium it was billed.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub cache_creation_input_tokens: i64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct CompletionTokensDetails {
    pub reasoning_tokens: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Choice {
    pub index: i32,
    pub message: ChatMessage,
    pub finish_reason: Cow<'static, str>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionResponse {
    pub id: String,
    pub object: Cow<'static, str>,
    pub created: i64,
    pub model: String,
    pub choices: Vec<Choice>,
    pub usage: Usage,
}

impl ChatCompletionResponse {
    pub fn text(
        id: impl Into<String>,
        created: i64,
        model: impl Into<String>,
        content: impl Into<String>,
        finish_reason: impl Into<Cow<'static, str>>,
        usage: Usage,
    ) -> Self {
        Self::with_message(
            id,
            created,
            model,
            ChatMessage::text("assistant", content),
            finish_reason.into(),
            usage,
        )
    }

    /// Tool-call assistant turn (finish_reason=tool_calls); accompanying text rides in `content`.
    pub fn tool_calls(
        id: impl Into<String>,
        created: i64,
        model: impl Into<String>,
        content: String,
        calls: Vec<ToolCall>,
        usage: Usage,
    ) -> Self {
        Self::with_message(
            id,
            created,
            model,
            ChatMessage {
                role: "assistant".into(),
                content: (!content.is_empty()).then_some(MessageContent::Text(content)),
                tool_calls: Some(calls),
                ..Default::default()
            },
            "tool_calls".into(),
            usage,
        )
    }

    fn with_message(
        id: impl Into<String>,
        created: i64,
        model: impl Into<String>,
        message: ChatMessage,
        finish_reason: Cow<'static, str>,
        usage: Usage,
    ) -> Self {
        Self {
            id: id.into(),
            object: Cow::Borrowed("chat.completion"),
            created,
            model: model.into(),
            choices: vec![Choice {
                index: 0,
                message,
                finish_reason,
            }],
            usage,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChunkDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_details: Option<Vec<Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkChoice {
    pub index: i32,
    pub delta: ChunkDelta,
    pub finish_reason: Option<Cow<'static, str>>,
}

/// Borrows `id`/`model` from the stream state: chunks are built and serialized
/// once per SSE frame, so owning them would allocate twice per frame.
#[derive(Debug, Clone, Serialize)]
pub struct ChatCompletionChunk<'a> {
    pub id: &'a str,
    pub object: &'static str,
    pub created: i64,
    pub model: &'a str,
    pub choices: Vec<ChunkChoice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
}

impl<'a> ChatCompletionChunk<'a> {
    pub fn content(id: &'a str, created: i64, model: &'a str, text: impl Into<String>) -> Self {
        Self::with_delta(
            id,
            created,
            model,
            ChunkDelta {
                content: Some(text.into()),
                ..Default::default()
            },
            None,
            None,
        )
    }

    pub fn tool_calls(id: &'a str, created: i64, model: &'a str, calls: Vec<Value>) -> Self {
        Self::with_delta(
            id,
            created,
            model,
            ChunkDelta {
                tool_calls: Some(calls),
                ..Default::default()
            },
            None,
            None,
        )
    }

    /// Reasoning delta: prose and/or completed units.
    pub fn reasoning(
        id: &'a str,
        created: i64,
        model: &'a str,
        text: String,
        details: Option<Vec<Value>>,
    ) -> Self {
        Self::with_delta(
            id,
            created,
            model,
            ChunkDelta {
                reasoning_content: (!text.is_empty()).then_some(text),
                reasoning_details: details,
                ..Default::default()
            },
            None,
            None,
        )
    }

    pub fn finish(id: &'a str, created: i64, model: &'a str, usage: Option<Usage>) -> Self {
        Self::with_delta(
            id,
            created,
            model,
            ChunkDelta::default(),
            Some(Cow::Borrowed("stop")),
            usage,
        )
    }

    fn with_delta(
        id: &'a str,
        created: i64,
        model: &'a str,
        delta: ChunkDelta,
        finish_reason: Option<Cow<'static, str>>,
        usage: Option<Usage>,
    ) -> Self {
        Self {
            id,
            object: "chat.completion.chunk",
            created,
            model,
            choices: vec![ChunkChoice {
                index: 0,
                delta,
                finish_reason,
            }],
            usage,
        }
    }
}

fn parts_text(parts: &[Value]) -> String {
    parts
        .iter()
        .filter(|p| p["type"] == "text")
        .filter_map(|p| p["text"].as_str())
        .collect()
}

fn is_zero(n: &i64) -> bool {
    *n == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrip_keeps_extra_fields() {
        let j = r#"{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"seed":7,"stream_options":{"include_usage":true}}"#;
        let req: ChatCompletionRequest = serde_json::from_str(j).unwrap();
        assert_eq!(req.model, "gpt-4o");
        assert!(!req.stream);
        assert!(
            req.extra.get("stream_options").is_none(),
            "an OpenAI-surface knob must not reach a foreign wire"
        );
        let (text, parts) = req.messages[0]
            .content
            .clone()
            .unwrap()
            .into_text_and_parts();
        assert_eq!((text.as_str(), parts), ("hi", None));
        assert_eq!(req.extra.get("seed").unwrap().as_i64().unwrap(), 7);
    }

    #[test]
    fn multimodal_parts_parse_and_flatten() {
        let j = r#"{"model":"m","messages":[{"role":"user","content":[
            {"type":"text","text":"look: "},
            {"type":"image_url","image_url":{"url":"data:image/png;base64,xx"}},
            {"type":"text","text":"what is it?"}]}]}"#;
        let req: ChatCompletionRequest = serde_json::from_str(j).unwrap();
        let c = req.messages[0].content.clone().unwrap();
        let (text, parts) = c.into_text_and_parts();
        assert_eq!(text, "look: what is it?");
        assert_eq!(parts.unwrap().len(), 3);
    }

    #[test]
    fn tools_and_tool_calls_roundtrip() {
        let j = r#"{"model":"m","messages":[{"role":"user","content":"x"}],
            "tools":[{"type":"function","function":{"name":"get_weather","parameters":{}}}],
            "tool_choice":"auto"}"#;
        let req: ChatCompletionRequest = serde_json::from_str(j).unwrap();
        assert_eq!(req.tools.as_ref().unwrap().len(), 1);

        let resp = ChatCompletionResponse::tool_calls(
            "id",
            1,
            "m",
            String::new(),
            vec![ToolCall {
                id: "call-1".into(),
                kind: "function".into(),
                function: FunctionCall {
                    name: "get_weather".into(),
                    arguments: "{}".into(),
                },
            }],
            Usage::default(),
        );
        let v = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["choices"][0]["finish_reason"], "tool_calls");
        assert!(v["choices"][0]["message"].get("content").is_none());
        assert_eq!(
            v["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
            "get_weather"
        );

        let with_text = ChatCompletionResponse::tool_calls(
            "id",
            1,
            "m",
            "Looking first.".into(),
            vec![],
            Usage::default(),
        );
        let v = serde_json::to_value(&with_text).unwrap();
        assert_eq!(v["choices"][0]["message"]["content"], "Looking first.");
    }

    #[test]
    fn chunk_shapes() {
        let c = ChatCompletionChunk::content("id1", 1, "m", "hel");
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["choices"][0]["delta"]["content"], "hel");
        assert!(v.get("usage").is_none());
    }
}
