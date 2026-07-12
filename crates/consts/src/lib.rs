//! Global constants for the gateway.
//!
//! Layer L0: depends on nothing internal. Holds the error-code model and the
//! `Protocol` enum the engine factory dispatches on.

pub mod error_code;
pub mod protocol;

pub use error_code::{ErrCode, ErrorException};
pub use protocol::Protocol;

/// Chat roles.
pub mod role {
    pub const USER: &str = "user";
    pub const AI: &str = "assistant";
    pub const MODEL: &str = "model";
    pub const SYSTEM: &str = "system";
    pub const DEVELOPER: &str = "developer";
    pub const BOT: &str = "bot"; // google vertex
    pub const STORAGE: &str = "storage";
    // alternate names for the same roles.
    pub const QUESTION: &str = USER;
    pub const ANSWER: &str = AI;
}

/// Account tiers for PTU (provisioned) vs pay-as-you-go routing.
pub mod account_tier {
    pub const PTU: &str = "ptu";
    pub const PAYGO: &str = "paygo";
}
