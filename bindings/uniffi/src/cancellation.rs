use std::sync::Arc;

/// A handle that cancels an original read/Scan or a foreground `Admin` loop.
/// Repeated cancellation is safe. It can occur before or during the call.
#[derive(Debug, uniffi::Object)]
pub struct CancellationToken {
    pub(crate) inner: tokio_util::sync::CancellationToken,
}

#[uniffi::export]
impl CancellationToken {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: tokio_util::sync::CancellationToken::new(),
        })
    }

    /// Requests cancellation of each read or loop that holds this token.
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_is_visible_and_idempotent() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
        token.cancel();
        token.cancel();
        assert!(token.is_cancelled());
        assert!(token.inner.is_cancelled());
    }
}
