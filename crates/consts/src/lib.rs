//! Global constants for the gateway.
//!
//! Layer L0: depends on nothing internal. Holds the error-code model and the
//! `Protocol` enum the engine factory dispatches on.

pub mod error_code;
pub mod protocol;

pub use error_code::ErrCode;
pub use protocol::Protocol;

/// The per-minute governance window (QPM/TPM limits).
pub const MINUTE: std::time::Duration = std::time::Duration::from_secs(60);

/// Chat roles.
pub mod role {
    pub const USER: &str = "user";
    pub const AI: &str = "assistant";
    pub const MODEL: &str = "model";
    pub const SYSTEM: &str = "system";
    pub const STORAGE: &str = "storage";
}

/// Account tiers for PTU (provisioned) vs pay-as-you-go routing.
pub mod account_tier {
    pub const PTU: &str = "ptu";
    pub const PAYGO: &str = "paygo";
}
