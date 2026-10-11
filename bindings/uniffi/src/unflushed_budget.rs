use std::sync::Arc;

use crate::error::{Error, SlateDbError};

/// One bound on the bytes every database built with it holds unflushed, by
/// the engine's estimate. A write that finds the sum at or above the capacity
/// waits, and the database with the most bytes a flush can freeze is asked to
/// flush.
#[derive(uniffi::Object)]
pub struct UnflushedBudget {
    pub(crate) inner: Arc<slatedb::UnflushedBudget>,
}

#[uniffi::export]
impl UnflushedBudget {
    /// A budget of `capacity` bytes.
    #[uniffi::constructor]
    pub fn new(capacity: u64) -> Result<Arc<Self>, Error> {
        let capacity = usize::try_from(capacity)
            .map_err(|_| SlateDbError::ValueTooLargeForUsize { field: "capacity" })?;
        Ok(Arc::new(Self {
            inner: Arc::new(slatedb::UnflushedBudget::new(capacity)),
        }))
    }

    /// The bytes the sharing databases may hold unflushed.
    pub fn capacity(&self) -> u64 {
        self.inner.capacity() as u64
    }

    /// The bytes the sharing databases last published as unflushed.
    pub fn used(&self) -> u64 {
        self.inner.used() as u64
    }
}
