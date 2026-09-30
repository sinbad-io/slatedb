use tokio::sync::Mutex;

use crate::error::Error;
use crate::types::KeyValue;
use crate::validation::validate_key;

/// Upper bound on the number of rows `next_batch` preallocates room for, so a
/// caller passing a very large `max` cannot force a large allocation up front.
const MAX_BATCH_PREALLOC: u32 = 1024;

/// Async iterator returned by scan APIs.
#[derive(uniffi::Object)]
pub struct DbIterator {
    inner: Mutex<slatedb::DbIterator>,
}

impl DbIterator {
    pub(crate) fn new(inner: slatedb::DbIterator) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbIterator {
    /// Returns the next key/value pair from the iterator.
    pub async fn next(&self) -> Result<Option<KeyValue>, Error> {
        let mut guard = self.inner.lock().await;
        Ok(guard.next().await?.map(KeyValue::from))
    }

    /// Returns up to `max` key/value pairs from the iterator in one call.
    ///
    /// Locks the iterator once and pulls rows until it yields `max` items or the
    /// iterator is exhausted. A returned vector shorter than `max` (including an
    /// empty vector) means the iterator is exhausted. `max == 0` returns an empty
    /// vector without advancing.
    ///
    /// This exists so that callers crossing a foreign-function boundary can drain
    /// a scan with one call per batch instead of one call per row.
    pub async fn next_batch(&self, max: u32) -> Result<Vec<KeyValue>, Error> {
        let mut guard = self.inner.lock().await;
        let mut out = Vec::with_capacity(max.min(MAX_BATCH_PREALLOC) as usize);
        for _ in 0..max {
            match guard.next().await? {
                Some(kv) => out.push(KeyValue::from(kv)),
                None => break,
            }
        }
        Ok(out)
    }

    /// Seeks the iterator to the first entry at or after `key`.
    pub async fn seek(&self, key: Vec<u8>) -> Result<(), Error> {
        validate_key(&key)?;
        let mut guard = self.inner.lock().await;
        guard.seek(key).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;

    use super::DbIterator;

    const ROWS: u32 = 6;

    async fn seeded_db() -> slatedb::Db {
        let db = slatedb::Db::builder("test", Arc::new(InMemory::new()))
            .build()
            .await
            .expect("failed to open db");
        for i in 0..ROWS {
            db.put(format!("key{i:02}"), format!("value{i:02}"))
                .await
                .expect("failed to put row");
        }
        db
    }

    async fn scan_all(db: &slatedb::Db) -> DbIterator {
        DbIterator::new(db.scan(..).await.expect("failed to scan"))
    }

    /// Drains an iterator one row at a time; the oracle for the batch results.
    async fn drain_rows(iter: &DbIterator) -> Vec<crate::types::KeyValue> {
        let mut rows = Vec::new();
        while let Some(row) = iter.next().await.expect("next() failed") {
            rows.push(row);
        }
        rows
    }

    #[tokio::test]
    async fn next_batch_matches_next() {
        let db = seeded_db().await;
        let want = drain_rows(&scan_all(&db).await).await;
        assert_eq!(want.len(), ROWS as usize);

        // 3 and 6 divide the row count exactly; 4 and 7 leave a short final
        // batch; 1 must behave like repeated next().
        for max in [1u32, 3, 4, 6, 7, 1000] {
            let iter = scan_all(&db).await;
            let mut got = Vec::new();
            loop {
                let batch = iter.next_batch(max).await.expect("next_batch() failed");
                let exhausted = batch.len() < max as usize;
                got.extend(batch);
                if exhausted {
                    break;
                }
            }
            assert_eq!(got, want, "next_batch({max}) disagreed with next()");
        }
    }

    #[tokio::test]
    async fn next_batch_zero_max_does_not_advance() {
        let db = seeded_db().await;
        let iter = scan_all(&db).await;

        assert!(iter
            .next_batch(0)
            .await
            .expect("next_batch(0) failed")
            .is_empty());

        let rows = iter
            .next_batch(1000)
            .await
            .expect("next_batch(1000) failed");
        assert_eq!(rows.len(), ROWS as usize);
    }

    #[tokio::test]
    async fn next_batch_returns_empty_once_exhausted() {
        let db = seeded_db().await;
        let iter = scan_all(&db).await;

        let rows = iter
            .next_batch(1000)
            .await
            .expect("next_batch(1000) failed");
        assert_eq!(rows.len(), ROWS as usize);
        assert!(iter
            .next_batch(1000)
            .await
            .expect("next_batch(1000) after exhaustion failed")
            .is_empty());
    }

    // New-API source controls: Close/ScanOptions token are not present in the
    // published point binding. These are separate from old-API Go behavior.
    #[tokio::test]
    async fn scan_close_cancels_original_before_waiting_for_iterator_mutex() {
        use std::future::Future;
        let db = seeded_db().await;
        let parent = crate::cancellation::CancellationToken::new();
        let options: slatedb::config::ScanOptions = crate::config::ScanOptions {
            cancellation_token: Some(parent.clone()),
            ..Default::default()
        }
        .try_into()
        .unwrap();
        let inner = db.scan_with_options(.., &options).await.unwrap();
        let original = inner
            .cancellation_token()
            .expect("scan must retain its own child");
        let iter = DbIterator::new(inner);
        let held = iter.inner.lock().await;
        let mut close = Box::pin(iter.close());
        let pending =
            std::future::poll_fn(|cx| std::task::Poll::Ready(close.as_mut().poll(cx).is_pending()))
                .await;
        let signaled = original.is_cancelled();
        let parent_cancelled = parent.is_cancelled();
        drop(held);
        tokio::time::timeout(std::time::Duration::from_secs(2), close)
            .await
            .unwrap()
            .unwrap();
        iter.close().await.unwrap();
        assert!(pending, "Close did not retain the original locked iterator");
        assert!(
            signaled,
            "Close waited for the mutex before cancelling original work"
        );
        assert!(!parent_cancelled, "Close cancelled caller or sibling scope");
        let sibling = db.scan_with_options(.., &options).await.unwrap();
        let sibling = DbIterator::new(sibling);
        assert_eq!(sibling.next_batch(100).await.unwrap().len(), ROWS as usize);
        sibling.close().await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn scan_close_retains_original_read_error_on_every_call() {
        let db = seeded_db().await;
        let parent = crate::cancellation::CancellationToken::new();
        let options: slatedb::config::ScanOptions = crate::config::ScanOptions {
            cancellation_token: Some(parent.clone()),
            ..Default::default()
        }
        .try_into()
        .unwrap();
        let iter = DbIterator::new(db.scan_with_options(.., &options).await.unwrap());
        parent.cancel();
        let read = iter.next_batch(3).await;
        let first = iter.close().await;
        let second = iter.close().await;
        assert!(
            matches!(read, Err(crate::error::Error::Cancelled { .. })),
            "{read:?}"
        );
        assert!(
            matches!(first, Err(crate::error::Error::Cancelled { .. })),
            "{first:?}"
        );
        assert!(
            matches!(second, Err(crate::error::Error::Cancelled { .. })),
            "{second:?}"
        );
        db.close().await.unwrap();
    }
}
