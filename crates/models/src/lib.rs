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
pub mod usage;

pub use block::Block;
pub use cost::{
    TokenInput, TokenRate, cost_micros, long_context_scale, weighted_completion, weighted_prompt,
};
pub use error::{GResult, GatewayError};
pub use params::{
    ChatParams, DecisionParams, EmbeddingParams, ImageParams, ModerationParams, ReasoningParam,
    RerankParams, SearchParams, SttParams, TtsParams, TypedParams, VideoParams,
};
pub use request::domain::{Account, ChatMsg};
pub use request::{BatchItem, GatewayRequest, ModelParamV2};
pub use response::{GatewayResponse, StreamChunk, StreamError};
pub use usage::CommonUsage;

/// The proleptic-Gregorian (year, month, day) of a UTC epoch day.
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m as u32, d as u32)
}
