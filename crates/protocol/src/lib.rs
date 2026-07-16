//! External wire protocols the gateway speaks (OpenAI/Anthropic compatible).
//!
//! Layer L1: pure serde types, no I/O.

pub mod anthropic;
pub mod openai;
