//! Error codes.
//!
//! This module models two related things:
//!   * `ErrorException{ code, msg }` values (SYSTEM_ERROR, ReqJsonErr, ...)
//!   * an `ErrCode` numeric set used for metrics/logging
//!
//! Numeric codes are stable so the gateway's externally observable error
//! codes remain consistent across releases.

use std::fmt;

/// Stable numeric error code, backed by `i64`.
///
/// Kept as a newtype over i64 (rather than a closed enum) because callers
/// construct `ErrorException` from arbitrary upstream/business codes; a closed
/// enum would lose the long tail. Named constants below cover the known set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct ErrCode(pub i64);

impl ErrCode {
    pub const SUCCESS: ErrCode = ErrCode(200); // used for metrics tracking

    pub const SYSTEM_ERROR: ErrCode = ErrCode(1000);
    pub const DB_WRITE: ErrCode = ErrCode(2000);
    pub const DB_READ: ErrCode = ErrCode(2001);
    pub const RPC: ErrCode = ErrCode(2002);
    pub const ID_GEN: ErrCode = ErrCode(2003);
    pub const INTERNAL_UNKNOWN: ErrCode = ErrCode(2011);
    pub const BUILD_REQ: ErrCode = ErrCode(2004);
    pub const FED_RESP_UNKNOWN: ErrCode = ErrCode(2005);
    pub const FED_RESP_RPC_FAILED: ErrCode = ErrCode(2006);
    pub const FED_RESP_NIL: ErrCode = ErrCode(2007);
    pub const FED_RESP_STATUS_NOT_ZERO: ErrCode = ErrCode(2008);
    pub const PARSE_FED_RESP: ErrCode = ErrCode(2009);
    pub const GEN_RES_NOT_NULL: ErrCode = ErrCode(2010);

    pub const REQ_JSON: ErrCode = ErrCode(3001);
    pub const REQ_PARAM: ErrCode = ErrCode(3002);
    pub const REQ_NON_CHAT: ErrCode = ErrCode(3003);
    pub const PERMISSION_CHECK: ErrCode = ErrCode(3007);

    pub const EMPTY_RESP: ErrCode = ErrCode(4003);
    pub const STOP_LIMIT_MSG: ErrCode = ErrCode(4004);

    #[inline]
    pub const fn value(self) -> i64 {
        self.0
    }
}

impl fmt::Display for ErrCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An error exception: a numeric code plus a user-facing message.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ErrorException {
    pub code: i64,
    pub msg: String,
}

impl ErrorException {
    pub fn new(code: i64, msg: impl Into<String>) -> Self {
        Self {
            code,
            msg: msg.into(),
        }
    }

    pub fn ml_unknown(code: ErrCode) -> Self {
        Self::new(
            code.value(),
            "an unknown error occurred, please retry later",
        )
    }
}

impl fmt::Display for ErrorException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.msg)
    }
}

impl std::error::Error for ErrorException {}

/// Well-known `ErrorException` values.
pub mod exceptions {
    use super::ErrorException;

    pub fn system_error() -> ErrorException {
        ErrorException::new(1000, "internal error, please retry later")
    }
    pub fn req_json_err() -> ErrorException {
        ErrorException::new(3001, "invalid request json")
    }
    pub fn req_param_err() -> ErrorException {
        ErrorException::new(3002, "invalid parameter")
    }
    pub fn permission_check_err() -> ErrorException {
        ErrorException::new(3007, "signature verification failed")
    }
    pub fn empty_resp_err() -> ErrorException {
        ErrorException::new(
            4003,
            "this content cannot be answered, please try a different request",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_codes_preserved() {
        assert_eq!(ErrCode::REQ_JSON.value(), 3001);
        assert_eq!(ErrCode::SYSTEM_ERROR.value(), 1000);
    }

    #[test]
    fn exception_display() {
        assert_eq!(
            exceptions::req_json_err().to_string(),
            "[3001] invalid request json"
        );
    }
}
