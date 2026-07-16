//! The content-safety verdict (`Block`).

/// Content-safety verdict.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Block {
    pub block: bool,
    pub message: String,
    pub err_code: i32,
}

impl Block {
    /// A clean (not blocked) verdict.
    pub fn allow() -> Self {
        Self::default()
    }

    /// A blocking verdict.
    pub fn blocked(message: impl Into<String>, err_code: i32) -> Self {
        Self {
            block: true,
            message: message.into(),
            err_code,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocked_carries_message_and_code() {
        let b = Block::blocked("nope", 4003);
        assert!(b.block);
        assert_eq!((b.message.as_str(), b.err_code), ("nope", 4003));
        assert!(!Block::allow().block);
    }
}
