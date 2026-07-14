//! The content-safety verdict (`Block`).

/// Content-safety verdict. Invariant: a blocked verdict is always a hit; a hit
/// is not necessarily a block.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Block {
    pub block: bool,
    pub hit: bool,
    pub message: String,
    pub err_code: i32,
}

impl Block {
    /// A clean (not hit, not blocked) verdict.
    pub fn allow() -> Self {
        Self::default()
    }

    /// A blocking verdict (implies hit, per the invariant above).
    pub fn blocked(message: impl Into<String>, err_code: i32) -> Self {
        Self {
            block: true,
            hit: true,
            message: message.into(),
            err_code,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocked_implies_hit() {
        let b = Block::blocked("nope", 4003);
        assert!(b.block && b.hit);
        assert!(!Block::allow().hit);
    }
}
