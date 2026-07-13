//! The unified gateway error model.
//!
//! Every error carries a stable numeric code, an HTTP status to surface, and
//! a user-facing message, collapsed into a single `Result<T, GatewayError>`.

use std::fmt;

use gw_consts::ErrCode;

/// Result alias used across the gateway.
pub type GResult<T> = Result<T, GatewayError>;

/// A gateway error: a stable numeric code, an HTTP status to surface, a
/// user-facing message, and an optional underlying cause.
#[derive(Debug)]
pub struct GatewayError {
    pub code: ErrCode,
    pub http_status: u16,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl GatewayError {
    pub fn new(code: ErrCode, http_status: u16, message: impl Into<String>) -> Self {
        Self {
            code,
            http_status,
            message: message.into(),
            source: None,
        }
    }

    /// Internal 500 with the system-error code.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrCode::SYSTEM_ERROR, 500, message)
    }

    /// 400 bad request.
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(ErrCode::REQ_PARAM, 400, message)
    }

    /// Client went away before the response was committed. Status 499 stays
    /// below the 5xx failover threshold so the disconnect neither re-bills nor
    /// faults the account. (Once delivery has begun, a break returns an
    /// aborted outcome instead — billed from the delivered content.)
    pub fn client_closed(message: impl Into<String>) -> Self {
        Self::new(ErrCode::SYSTEM_ERROR, 499, message)
    }

    /// Attach an underlying cause.
    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }
}

impl fmt::Display for GatewayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl std::error::Error for GatewayError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_and_code() {
        let e = GatewayError::bad_request("invalid parameter");
        assert_eq!(e.http_status, 400);
        assert_eq!(e.code, ErrCode::REQ_PARAM);
        assert_eq!(e.to_string(), "[3002] invalid parameter");
    }
}
