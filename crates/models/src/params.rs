//! Typed per-family model params.
//!
//! Rather than one param struct per vendor across 65 vendors (heavy field
//! duplication), this collapses into **protocol-family params** + `raw`
//! passthrough: vendors in the same family share one typed param set,
//! vendor-specific fields pass through verbatim in `ModelParamV2.raw`
//! (serde_json::Value); byte-level vendor parity lands with live integration.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Typed params, one variant per protocol family (consts::Protocol).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "family", rename_all = "snake_case")]
pub enum TypedParams {
    /// azure / ali (openai-compatible) / deepseek / mistral / moonshot / zhipu / ...
    Chat(ChatParams),
    /// embedding-model vendors sharing this family.
    Embeddings(EmbeddingParams),
    /// dalle / wanx / flux / stability / ideogram / ...
    Image(ImageParams),
    /// openai / azure / elevenlabs / cosyvoice / minimax / ... (text-to-speech)
    AudioTts(TtsParams),
    /// whisper / azure / google / ... (speech-to-text)
    AudioStt(SttParams),
    /// sora / veo / kling / runway / vidu / minimax (video generation)
    Video(VideoParams),
    /// bing / brave / serp / google custom search
    Search(SearchParams),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChatParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<i64>,
    /// string or [string] (OpenAI accepts both forms).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f64>,
    /// tools definition (openai or anthropic wire shape, passed through per protocol).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_logprobs: Option<i64>,
    /// system prompt (anthropic passes it directly; openai carries it via messages).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmbeddingParams {
    pub input: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ImageParams {
    pub prompt: String,
    #[serde(default = "one")]
    pub n: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<String>, // e.g. "1280*1280", etc.
    /// base64 source image — present → this is an edit (POST /v1/images/edits),
    /// absent → a generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// optional base64 edit mask (transparent pixels = editable region).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mask: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TtsParams {
    pub input: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voice: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<String>, // mp3/wav/pcm
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SttParams {
    /// base64 audio (the real API uses multipart upload; this local milestone carries it as b64)
    pub audio_b64: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VideoParams {
    pub prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchParams {
    pub query: String,
    #[serde(default = "three")]
    pub count: i64,
}

fn one() -> i64 {
    1
}
fn three() -> i64 {
    3
}

/// Vendor-untyped passthrough helper: anything not covered by the typed layer.
pub fn passthrough(raw: Value) -> Value {
    raw
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_params_tagged_serde() {
        let p = TypedParams::Chat(ChatParams {
            temperature: Some(0.7),
            ..Default::default()
        });
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["family"], "chat");
        assert_eq!(v["temperature"], 0.7);
        let back: TypedParams = serde_json::from_value(v).unwrap();
        assert!(matches!(back, TypedParams::Chat(c) if c.temperature == Some(0.7)));
    }
}
