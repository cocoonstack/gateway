//! Client-facing error classification: the Bedrock-derived closed set from
//! the gateway error contract. Every rendered error carries exactly one
//! class; the numeric `ErrCode` stays internal (logs/metrics only).

use crate::ErrCode;

/// One classification from the contract's closed set. `ModelStreamError` is
/// in-stream only and never renders at the HTTP phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrClass {
    Validation,
    /// Reserved by the contract for hard quota/balance exhaustion; no
    /// construction site classifies into it yet (billing wires it later).
    ServiceQuotaExceeded,
    UnrecognizedClient,
    AccessDenied,
    ResourceNotFound,
    ModelTimeout,
    Conflict,
    RequestEntityTooLarge,
    ModelError,
    Throttling,
    InternalServer,
    ServiceUnavailable,
    ModelStreamError,
}

impl ErrClass {
    /// PascalCase exception name, the `x-amzn-errortype` header value.
    pub const fn name(self) -> &'static str {
        match self {
            ErrClass::Validation => "ValidationException",
            ErrClass::ServiceQuotaExceeded => "ServiceQuotaExceededException",
            ErrClass::UnrecognizedClient => "UnrecognizedClientException",
            ErrClass::AccessDenied => "AccessDeniedException",
            ErrClass::ResourceNotFound => "ResourceNotFoundException",
            ErrClass::ModelTimeout => "ModelTimeoutException",
            ErrClass::Conflict => "ConflictException",
            ErrClass::RequestEntityTooLarge => "RequestEntityTooLargeException",
            ErrClass::ModelError => "ModelErrorException",
            ErrClass::Throttling => "ThrottlingException",
            ErrClass::InternalServer => "InternalServerException",
            ErrClass::ServiceUnavailable => "ServiceUnavailableException",
            ErrClass::ModelStreamError => "ModelStreamErrorException",
        }
    }

    /// snake_case of [`name`](Self::name), the body `code` field.
    pub const fn code(self) -> &'static str {
        match self {
            ErrClass::Validation => "validation_exception",
            ErrClass::ServiceQuotaExceeded => "service_quota_exceeded_exception",
            ErrClass::UnrecognizedClient => "unrecognized_client_exception",
            ErrClass::AccessDenied => "access_denied_exception",
            ErrClass::ResourceNotFound => "resource_not_found_exception",
            ErrClass::ModelTimeout => "model_timeout_exception",
            ErrClass::Conflict => "conflict_exception",
            ErrClass::RequestEntityTooLarge => "request_entity_too_large_exception",
            ErrClass::ModelError => "model_error_exception",
            ErrClass::Throttling => "throttling_exception",
            ErrClass::InternalServer => "internal_server_exception",
            ErrClass::ServiceUnavailable => "service_unavailable_exception",
            ErrClass::ModelStreamError => "model_stream_error_exception",
        }
    }

    /// External HTTP status rendered to clients.
    pub const fn status(self) -> u16 {
        match self {
            ErrClass::Validation | ErrClass::ServiceQuotaExceeded => 400,
            ErrClass::UnrecognizedClient => 401,
            ErrClass::AccessDenied => 403,
            ErrClass::ResourceNotFound => 404,
            ErrClass::ModelTimeout => 408,
            ErrClass::Conflict => 409,
            ErrClass::RequestEntityTooLarge => 413,
            // ModelStreamError never renders at the HTTP phase; 424 nominal
            ErrClass::ModelError | ErrClass::ModelStreamError => 424,
            ErrClass::Throttling => 429,
            ErrClass::InternalServer => 500,
            ErrClass::ServiceUnavailable => 503,
        }
    }

    /// Coarse `type` for the OpenAI envelope, for SDKs that key on it.
    pub const fn openai_type(self) -> &'static str {
        match self {
            ErrClass::Validation
            | ErrClass::ServiceQuotaExceeded
            | ErrClass::Conflict
            | ErrClass::RequestEntityTooLarge => "invalid_request_error",
            ErrClass::UnrecognizedClient => "authentication_error",
            ErrClass::AccessDenied => "permission_denied_error",
            ErrClass::ResourceNotFound => "not_found_error",
            ErrClass::ModelTimeout => "timeout_error",
            ErrClass::ModelError | ErrClass::ModelStreamError => "model_error",
            ErrClass::Throttling => "rate_limit_error",
            ErrClass::InternalServer | ErrClass::ServiceUnavailable => "server_error",
        }
    }

    /// `type` for the Anthropic envelope — the discriminator its SDKs
    /// dispatch exceptions on.
    pub const fn anthropic_type(self) -> &'static str {
        match self {
            ErrClass::Validation | ErrClass::ServiceQuotaExceeded | ErrClass::Conflict => {
                "invalid_request_error"
            }
            ErrClass::UnrecognizedClient => "authentication_error",
            ErrClass::AccessDenied => "permission_error",
            ErrClass::ResourceNotFound => "not_found_error",
            ErrClass::RequestEntityTooLarge => "request_too_large",
            ErrClass::Throttling => "rate_limit_error",
            ErrClass::ServiceUnavailable => "overloaded_error",
            ErrClass::ModelTimeout
            | ErrClass::ModelError
            | ErrClass::ModelStreamError
            | ErrClass::InternalServer => "api_error",
        }
    }

    /// Classify an internal error for rendering. `None` for 499
    /// (client-closed): the peer is gone, nothing is rendered.
    ///
    /// Upstream-family codes classify by code first: their `status` is the
    /// real vendor status (or a pinned 502), which must not be mistaken for
    /// the client's own auth/validation failure.
    pub fn classify(code: ErrCode, status: u16) -> Option<ErrClass> {
        if status == 499 {
            return None;
        }
        let class = match code {
            ErrCode::FED_RESP_TIMEOUT => ErrClass::ModelTimeout,
            ErrCode::FED_RESP_UNKNOWN
            | ErrCode::FED_RESP_RPC_FAILED
            | ErrCode::FED_RESP_NIL
            | ErrCode::FED_RESP_STATUS_NOT_ZERO
            | ErrCode::PARSE_FED_RESP
            | ErrCode::GEN_RES_NOT_NULL
            // a terminal upstream 429/503 keeps its transient retry
            // semantics instead of collapsing into the 424 "don't retry"
            | ErrCode::EMPTY_RESP => match status {
                408 => ErrClass::ModelTimeout,
                429 => ErrClass::Throttling,
                503 => ErrClass::ServiceUnavailable,
                _ => ErrClass::ModelError,
            },
            ErrCode::STOP_LIMIT_MSG => ErrClass::Throttling,
            ErrCode::PERMISSION_CHECK => ErrClass::AccessDenied,
            ErrCode::REQ_JSON | ErrCode::REQ_NON_CHAT => ErrClass::Validation,
            _ => return Self::from_status(status),
        };
        Some(class)
    }

    /// Classify by external-facing status alone (ad-hoc error sites and the
    /// open `ErrCode` tail). `None` for 499.
    pub fn from_status(status: u16) -> Option<ErrClass> {
        let class = match status {
            401 => ErrClass::UnrecognizedClient,
            403 => ErrClass::AccessDenied,
            404 => ErrClass::ResourceNotFound,
            408 => ErrClass::ModelTimeout,
            409 => ErrClass::Conflict,
            413 => ErrClass::RequestEntityTooLarge,
            424 => ErrClass::ModelError,
            429 => ErrClass::Throttling,
            499 => return None,
            503 => ErrClass::ServiceUnavailable,
            s if s >= 500 => ErrClass::InternalServer,
            // 400, 405, 415, 422, 501 and the remaining 4xx tail: the request
            // as sent cannot be served
            _ => ErrClass::Validation,
        };
        Some(class)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_is_snake_of_name() {
        for c in [
            ErrClass::Validation,
            ErrClass::ServiceQuotaExceeded,
            ErrClass::UnrecognizedClient,
            ErrClass::AccessDenied,
            ErrClass::ResourceNotFound,
            ErrClass::ModelTimeout,
            ErrClass::Conflict,
            ErrClass::RequestEntityTooLarge,
            ErrClass::ModelError,
            ErrClass::Throttling,
            ErrClass::InternalServer,
            ErrClass::ServiceUnavailable,
            ErrClass::ModelStreamError,
        ] {
            let mut snake = String::new();
            for (i, ch) in c.name().chars().enumerate() {
                if ch.is_uppercase() {
                    if i > 0 {
                        snake.push('_');
                    }
                    snake.extend(ch.to_lowercase());
                } else {
                    snake.push(ch);
                }
            }
            assert_eq!(c.code(), snake, "{}", c.name());
        }
    }

    #[test]
    fn upstream_family_classifies_by_code_not_status() {
        // a vendor 401 is the upstream's failure, not the client's
        let c = ErrClass::classify(ErrCode::FED_RESP_STATUS_NOT_ZERO, 401).unwrap();
        assert_eq!(c, ErrClass::ModelError);
        assert_eq!(c.status(), 424);
        // terminal vendor throttle/capacity keep their transient semantics
        let c = ErrClass::classify(ErrCode::FED_RESP_STATUS_NOT_ZERO, 429).unwrap();
        assert_eq!(c, ErrClass::Throttling);
        let c = ErrClass::classify(ErrCode::FED_RESP_STATUS_NOT_ZERO, 503).unwrap();
        assert_eq!(c, ErrClass::ServiceUnavailable);
        // deadline family
        let c = ErrClass::classify(ErrCode::FED_RESP_TIMEOUT, 502).unwrap();
        assert_eq!((c, c.status()), (ErrClass::ModelTimeout, 408));
    }

    #[test]
    fn client_closed_is_not_rendered() {
        assert!(ErrClass::classify(ErrCode::SYSTEM_ERROR, 499).is_none());
        assert!(ErrClass::from_status(499).is_none());
    }

    #[test]
    fn own_faults_classify_by_status() {
        assert_eq!(
            ErrClass::classify(ErrCode::SYSTEM_ERROR, 503),
            Some(ErrClass::ServiceUnavailable)
        );
        assert_eq!(
            ErrClass::classify(ErrCode::SYSTEM_ERROR, 500),
            Some(ErrClass::InternalServer)
        );
        assert_eq!(
            ErrClass::classify(ErrCode::REQ_PARAM, 404),
            Some(ErrClass::ResourceNotFound)
        );
        assert_eq!(
            ErrClass::classify(ErrCode::REQ_PARAM, 400),
            Some(ErrClass::Validation)
        );
    }
}
