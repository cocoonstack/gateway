//! Core domain models for the gateway.
//!
//! Layer L1: depends only on `gw-consts`. Holds the request/response types the
//! whole pipeline threads through, the unified error model, and the usage view.

pub mod block;
pub mod cost;
pub mod error;
pub mod params;
pub mod request;
pub mod response;
pub mod token_estimate;
pub mod usage;

pub use block::Block;
pub use cost::{TokenInput, TokenRate, cost_micros, platform_total};
pub use error::{GResult, GatewayError};
pub use params::{
    ChatParams, EmbeddingParams, ImageParams, SearchParams, SttParams, TtsParams, TypedParams,
    VideoParams,
};
pub use request::domain::{Account, ChatMsg};
pub use request::{GatewayRequest, ModelParamV2};
pub use response::GatewayResponse;
pub use response::StreamChunk;
pub use token_estimate::{HeuristicEncoder, TokenEncoder, estimate_prompt_tokens};
pub use usage::CommonUsage;
