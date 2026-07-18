//! The wire protocols the gateway speaks to upstream vendors.
//!
//! One engine per protocol. A model entry in the config binds a public model
//! name to a protocol — directly or via its provider's kind — so adding a
//! model or vendor is pure configuration, never a code change.

use std::fmt;

/// A vendor wire protocol served by a dedicated engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Protocol {
    /// OpenAI chat completions (native + the many compatible vendors)
    OpenaiChat,
    /// OpenAI legacy text completions ({prompt} in, {choices[].text} out)
    Completions,
    /// OpenAI Responses API (output items, input/output token usage)
    Responses,
    /// Anthropic messages
    AnthropicMessages,
    /// Google Gemini generateContent
    Gemini,
    /// vector embeddings (OpenAI shape)
    Embeddings,
    /// image generation
    Image,
    /// text-to-speech
    Tts,
    /// speech-to-text
    Stt,
    /// other audio work (sound effects, isolation, alignment, cloning)
    Audio,
    /// video generation (async task type)
    Video,
    /// web search
    Search,
    /// realtime bidirectional streaming (served on the WebSocket surface)
    Realtime,
    /// request body passed through as-is
    Passthrough,
    /// Baidu Ernie chat
    Ernie,
    /// MiniMax v1 chat (sender_type/reply/base_resp shape)
    MinimaxV1,
    /// Cohere Command on AWS Bedrock (SigV4)
    AwsCohere,
    /// Llama on AWS Bedrock (SigV4)
    AwsLlama,
    /// Alibaba DashScope native (input.messages/parameters/output.choices)
    Dashscope,
    /// content moderation (OpenAI moderations shape)
    Moderations,
    /// document rerank (Cohere/Jina-compatible shape)
    Rerank,
}

impl Protocol {
    pub const ALL: &'static [Protocol] = &[
        Protocol::OpenaiChat,
        Protocol::Completions,
        Protocol::Responses,
        Protocol::AnthropicMessages,
        Protocol::Gemini,
        Protocol::Embeddings,
        Protocol::Image,
        Protocol::Tts,
        Protocol::Stt,
        Protocol::Audio,
        Protocol::Video,
        Protocol::Search,
        Protocol::Realtime,
        Protocol::Passthrough,
        Protocol::Ernie,
        Protocol::MinimaxV1,
        Protocol::AwsCohere,
        Protocol::AwsLlama,
        Protocol::Dashscope,
        Protocol::Moderations,
        Protocol::Rerank,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Protocol::OpenaiChat => "openai-chat",
            Protocol::Completions => "completions",
            Protocol::Responses => "responses",
            Protocol::AnthropicMessages => "anthropic-messages",
            Protocol::Gemini => "gemini",
            Protocol::Embeddings => "embeddings",
            Protocol::Image => "image",
            Protocol::Tts => "tts",
            Protocol::Stt => "stt",
            Protocol::Audio => "audio",
            Protocol::Video => "video",
            Protocol::Search => "search",
            Protocol::Realtime => "realtime",
            Protocol::Passthrough => "passthrough",
            Protocol::Ernie => "ernie",
            Protocol::MinimaxV1 => "minimax-v1",
            Protocol::AwsCohere => "aws-cohere",
            Protocol::AwsLlama => "aws-llama",
            Protocol::Dashscope => "dashscope",
            Protocol::Moderations => "moderations",
            Protocol::Rerank => "rerank",
        }
    }

    pub fn from_wire(s: &str) -> Option<Protocol> {
        Protocol::ALL.iter().copied().find(|p| p.as_str() == s)
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_roundtrip_is_total() {
        for &p in Protocol::ALL {
            assert_eq!(Protocol::from_wire(p.as_str()), Some(p));
        }
        assert_eq!(Protocol::ALL.len(), 21);
        assert!(Protocol::from_wire("nope").is_none());
    }
}
