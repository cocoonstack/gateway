//! The moderation seam: an optional external content-review pass, plugged into
//! the pre-stage. The default [`AllowModerator`] is a deterministic no-op, so
//! the hot path pays nothing until a real moderator is wired in and a tenant
//! turns `moderate` on. External review is latency-bearing and pull-driven, so
//! this ships as a trait, not a built-in integration.

use std::ops::Range;
use std::sync::Arc;

/// A moderator's decision on one request's text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Allow,
    /// Redact these byte ranges of the reviewed text, then serve. Offsets
    /// address the exact string `review` received; the caller maps them back
    /// onto the request's text slots.
    Mask(Vec<Range<usize>>),
    /// Serve via the tenant's fallback model. Denies when no fallback is
    /// configured or the surface can't switch models mid-session (realtime).
    Degrade,
    /// Deny with a user-facing reason.
    Deny(String),
}

/// A pluggable content moderator. `review` sees the request's concatenated
/// inbound text; a real impl calls an external service. An `Err` is a moderator
/// failure — the caller resolves it per its fail-open/closed posture.
#[async_trait::async_trait]
pub trait Moderator: Send + Sync + std::fmt::Debug {
    async fn review(&self, text: &str) -> Result<Verdict, String>;
}

/// The default: allow everything (a real moderator replaces it via
/// [`crate::OnlineHandler::with_moderator`]).
#[derive(Debug, Default)]
pub struct AllowModerator;

#[async_trait::async_trait]
impl Moderator for AllowModerator {
    async fn review(&self, _text: &str) -> Result<Verdict, String> {
        Ok(Verdict::Allow)
    }
}

/// The default moderator handle.
pub fn default_moderator() -> Arc<dyn Moderator> {
    Arc::new(AllowModerator)
}
