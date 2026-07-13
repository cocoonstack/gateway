//! Model engines.
//!
//! Layer L3. Defines the `ModelEngine` trait, the `Transport` seam (the default
//! build ships only `MockTransport`; no real upstream calls are made), the SSE
//! decoder, usage normalization, the dispatch factory,
//! and engine implementations.

pub mod bespoke;
pub mod claude_engine;
pub mod echo;
pub mod engine;
pub mod factory;
pub mod families;
pub mod http_transport;
pub mod openai_engine;
pub mod pump;
pub mod sigv4;
pub mod sse;
pub mod transport;
pub mod usage_extract;

pub use bespoke::{CohereEngine, DashScopeEngine, ErnieEngine, LlamaEngine, MinimaxV1Engine};
pub use claude_engine::ClaudeEngine;
pub use echo::EchoEngine;
pub use engine::{EngineOutcome, ModelEngine, StreamChunk, vendor_error};
pub use factory::{get_engine, is_implemented};
pub use families::{
    AudioEngine, AudioKind, CompletionsEngine, EmbeddingsEngine, ImageEngine, PassthroughEngine,
    ResponsesEngine, SearchEngine, VertexEngine, VideoEngine,
};
pub use openai_engine::{OpenAiEngine, merge_tool_call_fragments};
pub use sse::SseDecoder;
pub use transport::{
    MockTransport, SharedTransport, Transport, UpstreamBody, UpstreamRequest, UpstreamResponse,
};
pub use usage_extract::extract_common_usage;
