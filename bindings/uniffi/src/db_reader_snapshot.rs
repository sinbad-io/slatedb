use std::sync::Arc;

use crate::config::{ReadOptions, ScanOptions};
use crate::error::Error;
use crate::iterator::DbIterator;
use crate::types::KeyRange;
use crate::validation::validate_key;

/// One reader state across all operations, without a garbage-collection pin.
#[derive(uniffi::Object)]
pub struct DbReaderSnapshot {
    inner: Arc<slatedb::DbReaderSnapshot>,
}

impl DbReaderSnapshot {
    pub(crate) fn new(inner: Arc<slatedb::DbReaderSnapshot>) -> Self {
        Self { inner }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbReaderSnapshot {
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        validate_key(&key)?;
        Ok(self.inner.get(key).await?.map(|value| value.to_vec()))
    }

    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: ReadOptions,
    ) -> Result<Option<Vec<u8>>, Error> {
        validate_key(&key)?;
        let options = options.into();
        Ok(self
            .inner
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    pub async fn scan_with_options(
        &self,
        range: KeyRange,
        options: ScanOptions,
    ) -> Result<Arc<DbIterator>, Error> {
        let range = range.into_bounds()?;
        let options = options.try_into()?;
        Ok(Arc::new(DbIterator::new(
            self.inner.scan_with_options(range, &options).await?,
        )))
    }

    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        subrange: KeyRange,
        options: ScanOptions,
    ) -> Result<Arc<DbIterator>, Error> {
        let subrange = subrange.into_bounds()?;
        let options = options.try_into()?;
        Ok(Arc::new(DbIterator::new(
            self.inner
                .scan_prefix_with_options(prefix, subrange, &options)
                .await?,
        )))
    }
}
