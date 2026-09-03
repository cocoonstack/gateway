//! The moderation seam: an optional external content-review pass in the pre-stage.
//! The default [`AllowModerator`] is a no-op, so the hot path pays nothing.

use std::ops::Range;
use std::sync::Arc;

/// A moderator's decision on one request's text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Allow,
    /// Redact these byte ranges of the reviewed text, then serve; offsets address the string `review` saw.
    Mask(Vec<Range<usize>>),
    /// Serve via the tenant's fallback model; denies when there is none or the surface cannot switch.
    Degrade,
    /// Deny with a user-facing reason.
    Deny(String),
}

/// A pluggable content moderator over the request's concatenated inbound text; `Err` is a failure.
#[async_trait::async_trait]
pub trait Moderator: Send + Sync + std::fmt::Debug {
    async fn review(&self, text: &str) -> Result<Verdict, String>;
}

/// The default: allow everything.
#[derive(Debug, Default)]
pub struct AllowModerator;

#[async_trait::async_trait]
impl Moderator for AllowModerator {
    async fn review(&self, _text: &str) -> Result<Verdict, String> {
        Ok(Verdict::Allow)
    }
}

pub fn default_moderator() -> Arc<dyn Moderator> {
    Arc::new(AllowModerator)
}
