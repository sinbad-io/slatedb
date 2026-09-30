#![cfg(feature = "foyer")]

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig,
};
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use slatedb::bytes::Bytes;
use slatedb::config::{DbReaderOptions, FlushOptions, FlushType, ReadOptions, Settings};
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
use slatedb::db_cache::{CacheTarget, CachedEntry, DbCache};
use slatedb::{
    Db, DbCacheManagerOps, DbReader, DbReaderMode, DbReaderSnapshot, DbSnapshot, ErrorKind,
    VersionedManifest,
};
use tempfile::TempDir;
use tokio::sync::Notify;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const KEY: &[u8] = b"point-key";
const VALUE: &[u8] = b"point-value";
const INACTIVE: u64 = u64::MAX;

#[derive(Debug)]
struct ReadHold {
    offset: AtomicU64,
    released: AtomicBool,
    started: AtomicUsize,
    active: AtomicUsize,
    dropped: AtomicUsize,
    changed: Notify,
}

impl ReadHold {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            offset: AtomicU64::new(INACTIVE),
            released: AtomicBool::new(false),
            started: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            changed: Notify::new(),
        })
    }

    fn release(&self) {
        self.released.store(true, SeqCst);
        self.changed.notify_waiters();
    }

    async fn wait_started(&self, count: usize) {
        loop {
            let changed = self.changed.notified();
            if self.started.load(SeqCst) >= count {
                return;
            }
            changed.await;
        }
    }

    async fn wait_empty(&self) {
        loop {
            let changed = self.changed.notified();
            if self.active.load(SeqCst) == 0 {
                return;
            }
            changed.await;
        }
    }
}

struct ActiveRead(Arc<ReadHold>);

impl Drop for ActiveRead {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, SeqCst);
        self.0.active.fetch_sub(1, SeqCst);
        self.0.changed.notify_waiters();
    }
}

#[derive(Debug)]
struct HeldStore {
    inner: Arc<dyn ObjectStore>,
    hold: Arc<ReadHold>,
    ranged_gets: AtomicUsize,
}

impl std::fmt::Display for HeldStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HeldStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for HeldStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let mut retained = None;
        if location.as_ref().contains("/compacted/") {
            if let Some(GetRange::Bounded(range)) = &options.range {
                self.ranged_gets.fetch_add(1, SeqCst);
                if range.start == self.hold.offset.load(SeqCst) && !self.hold.released.load(SeqCst)
                {
                    self.hold.active.fetch_add(1, SeqCst);
                    retained = Some(ActiveRead(self.hold.clone()));
                    self.hold.started.fetch_add(1, SeqCst);
                    self.hold.changed.notify_waiters();
                    loop {
                        let changed = self.hold.changed.notified();
                        if self.hold.released.load(SeqCst) {
                            break;
                        }
                        changed.await;
                    }
                }
            }
        }
        let result = self.inner.get_opts(location, options).await;
        drop(retained);
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Clone, Copy, Debug)]
enum Part {
    Filter,
    Index,
    Data,
}

fn settings() -> Settings {
    Settings {
        flush_interval: None,
        compactor_options: None,
        garbage_collector_options: None,
        min_filter_keys: 1,
        ..Settings::default()
    }
}

async fn cache(hybrid: bool, directory: &std::path::Path) -> Arc<dyn DbCache> {
    if !hybrid {
        return Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: 4 * 1024 * 1024,
            shards: 1,
        }));
    }
    let cache = HybridCacheBuilder::new()
        .with_name("point_cancellation")
        .memory(1024 * 1024)
        .with_weighter(|_, v: &CachedEntry| v.size())
        .storage()
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_engine_config(
            BlockEngineConfig::new(
                FsDeviceBuilder::new(directory)
                    .with_capacity(4 * 1024 * 1024)
                    .build()
                    .unwrap(),
            )
            .with_block_size(64 * 1024),
        )
        .build()
        .await
        .unwrap();
    Arc::new(FoyerHybridCache::new_with_cache(cache))
}

#[derive(Clone)]
enum OriginalOwner {
    Writer(Arc<Db>),
    Reader(Arc<DbReader>),
}

#[derive(Clone)]
enum OriginalView {
    Writer(Arc<DbSnapshot>),
    Reader(Arc<DbReaderSnapshot>),
}

impl OriginalOwner {
    fn manifest(&self) -> VersionedManifest {
        match self {
            Self::Writer(db) => db.manifest(),
            Self::Reader(db) => db.manifest(),
        }
    }

    async fn warm_sst(
        &self,
        id: slatedb::manifest::SsTableId,
        targets: &[CacheTarget],
    ) -> Result<(), slatedb::Error> {
        match self {
            Self::Writer(db) => db.warm_sst(id, targets).await,
            Self::Reader(db) => db.warm_sst(id, targets).await,
        }
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, slatedb::Error> {
        self.get_with_options(key, &ReadOptions::default()).await
    }

    async fn get_with_options(
        &self,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, slatedb::Error> {
        match self {
            Self::Writer(db) => db.get_with_options(key, options).await,
            Self::Reader(db) => db.get_with_options(key, options).await,
        }
    }

    async fn snapshot(&self) -> Result<OriginalView, slatedb::Error> {
        match self {
            Self::Writer(db) => db.snapshot().await.map(OriginalView::Writer),
            Self::Reader(db) => db.snapshot().map(OriginalView::Reader),
        }
    }

    async fn close(&self) -> Result<(), slatedb::Error> {
        match self {
            Self::Writer(db) => db.close().await,
            Self::Reader(db) => db.close().await,
        }
    }
}

impl OriginalView {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, slatedb::Error> {
        match self {
            Self::Writer(view) => view.get(key).await,
            Self::Reader(view) => view.get(key).await,
        }
    }

    async fn get_with_options(
        &self,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, slatedb::Error> {
        match self {
            Self::Writer(view) => view.get_with_options(key, options).await,
            Self::Reader(view) => view.get_with_options(key, options).await,
        }
    }
}

async fn fixture(
    hybrid: bool,
    part: Part,
    reader: bool,
) -> (OriginalOwner, Arc<HeldStore>, Arc<dyn DbCache>, TempDir) {
    let directory = TempDir::new().unwrap();
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let seed = Db::builder("point-cancellation", inner.clone())
        .with_settings(settings())
        .with_db_cache_disabled()
        .build()
        .await
        .unwrap();
    seed.put(KEY, VALUE).await.unwrap();
    seed.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    seed.close().await.unwrap();

    let store = Arc::new(HeldStore {
        inner,
        hold: ReadHold::new(),
        ranged_gets: AtomicUsize::new(0),
    });
    let cache = cache(hybrid, directory.path()).await;
    let db = if reader {
        OriginalOwner::Reader(Arc::new(
            DbReader::builder("point-cancellation", store.clone() as Arc<dyn ObjectStore>)
                .with_reader_mode(DbReaderMode::FollowLatest)
                .with_options(DbReaderOptions {
                    manifest_poll_interval: Duration::from_secs(3600),
                    skip_wal_replay: true,
                    ..DbReaderOptions::default()
                })
                .with_db_cache(cache.clone(), 1)
                .build()
                .await
                .unwrap(),
        ))
    } else {
        OriginalOwner::Writer(Arc::new(
            Db::builder("point-cancellation", store.clone() as Arc<dyn ObjectStore>)
                .with_settings(settings())
                .with_db_cache(cache.clone(), 1)
                .build()
                .await
                .unwrap(),
        ))
    };
    let manifest = db.manifest();
    assert_eq!(manifest.l0().len(), 1);
    let table = &manifest.l0()[0].sst;
    let warm = match part {
        Part::Filter => Vec::new(),
        Part::Index => vec![CacheTarget::Filters],
        Part::Data => vec![CacheTarget::Filters, CacheTarget::Index],
    };
    db.warm_sst(table.id, &warm).await.unwrap();
    let offset = match part {
        Part::Filter => {
            assert!(table.info.filter_len > 0);
            table.info.filter_offset
        }
        Part::Index => table.info.index_offset,
        Part::Data => 0,
    };
    store.hold.offset.store(offset, SeqCst);
    (db, store, cache, directory)
}

async fn cancellation_joins_original_load(hybrid: bool, part: Part) {
    for (reader, snapshot) in [(false, false), (false, true), (true, false), (true, true)] {
        let (db, store, cache, _directory) = fixture(hybrid, part, reader).await;
        let token = CancellationToken::new();
        let options = ReadOptions {
            cancellation_token: Some(token.clone()),
            ..ReadOptions::default()
        };
        let original_snapshot = db.snapshot().await.unwrap();
        let mut original = tokio::spawn({
            let db = db.clone();
            let original_snapshot = original_snapshot.clone();
            async move {
                if snapshot {
                    original_snapshot.get_with_options(KEY, &options).await
                } else {
                    db.get_with_options(KEY, &options).await
                }
            }
        });
        let started = timeout(Duration::from_secs(2), store.hold.wait_started(1)).await;
        token.cancel();
        let cancelled = timeout(Duration::from_secs(1), &mut original).await;
        let active_at_return = store.hold.active.load(SeqCst);
        let dropped_at_return = store.hold.dropped.load(SeqCst);
        store.hold.release();
        let (finished_before_release, result) = match cancelled {
            Ok(result) => (true, result.unwrap()),
            Err(_) => (
                false,
                timeout(Duration::from_secs(2), original)
                    .await
                    .expect("original read did not join after fixture release")
                    .unwrap(),
            ),
        };
        timeout(Duration::from_secs(2), store.hold.wait_empty())
            .await
            .expect("original object read survived cleanup");
        let valid_same_view = original_snapshot.get(KEY).await;
        drop(original_snapshot);
        db.close().await.unwrap();
        cache.close().await.unwrap();
        assert!(
            started.is_ok(),
            "fixture never held the selected original read"
        );
        assert!(
            finished_before_release,
            "cancelled read waited for external fixture release"
        );
        assert_eq!(
            active_at_return, 0,
            "native read returned while original object loader remained active"
        );
        assert_eq!(
            dropped_at_return, 1,
            "original loader was not dropped before cancellation returned"
        );
        assert!(
            matches!(result, Err(ref e) if matches!(e.kind(), ErrorKind::Cancelled)),
            "cancellation returned a value or wrong error: {result:?}"
        );
        assert_eq!(valid_same_view.unwrap(), Some(Bytes::from_static(VALUE)));
    }
}

#[tokio::test]
async fn cancelled_memory_filter_load_is_joined() {
    cancellation_joins_original_load(false, Part::Filter).await;
}

#[tokio::test]
async fn cancelled_hybrid_filter_load_is_joined() {
    cancellation_joins_original_load(true, Part::Filter).await;
}

#[tokio::test]
async fn cancelled_memory_index_load_is_joined() {
    cancellation_joins_original_load(false, Part::Index).await;
}

#[tokio::test]
async fn cancelled_hybrid_index_load_is_joined() {
    cancellation_joins_original_load(true, Part::Index).await;
}

#[tokio::test]
async fn cancelled_memory_data_load_is_joined() {
    cancellation_joins_original_load(false, Part::Data).await;
}

#[tokio::test]
async fn cancelled_hybrid_data_load_is_joined() {
    cancellation_joins_original_load(true, Part::Data).await;
}

async fn cancellable_warm_cache_stays_warm(hybrid: bool) {
    let (db, store, cache, _directory) = fixture(hybrid, Part::Data, false).await;
    store.hold.release();
    let warm = db.get(KEY).await;
    if hybrid {
        cache.flush_scope(1).await.unwrap();
    }
    let reads_before = store.ranged_gets.load(SeqCst);
    let token = CancellationToken::new();
    let options = ReadOptions {
        cancellation_token: Some(token.clone()),
        ..ReadOptions::default()
    };
    let result = db.get_with_options(KEY, &options).await;
    token.cancel();
    let cancelled = db.get_with_options(KEY, &options).await;
    let reads_after = store.ranged_gets.load(SeqCst);
    let next = db.get(KEY).await;
    db.close().await.unwrap();
    cache.close().await.unwrap();
    assert_eq!(warm.unwrap(), Some(Bytes::from_static(VALUE)));
    assert_eq!(result.unwrap(), Some(Bytes::from_static(VALUE)));
    assert_eq!(next.unwrap(), Some(Bytes::from_static(VALUE)));
    assert_eq!(
        reads_before, reads_after,
        "cancellable cache hit issued an object read"
    );
    assert!(matches!(cancelled, Err(ref e) if matches!(e.kind(), ErrorKind::Cancelled)));
}

#[tokio::test]
async fn cancellable_memory_hit_preserves_cache_and_view() {
    cancellable_warm_cache_stays_warm(false).await;
}

#[tokio::test]
async fn cancellable_hybrid_disk_hit_preserves_cache_and_view() {
    cancellable_warm_cache_stays_warm(true).await;
}

async fn open_original(
    store: Arc<dyn ObjectStore>,
    cache: Arc<dyn DbCache>,
    reader: bool,
    max_retries: Option<u32>,
) -> OriginalOwner {
    if reader {
        OriginalOwner::Reader(Arc::new(
            DbReader::builder("point-cancellation", store)
                .with_reader_mode(DbReaderMode::FollowLatest)
                .with_options(DbReaderOptions {
                    manifest_poll_interval: Duration::from_secs(3600),
                    skip_wal_replay: true,
                    object_store_max_retries: max_retries,
                    ..DbReaderOptions::default()
                })
                .with_db_cache(cache, 1)
                .build()
                .await
                .unwrap(),
        ))
    } else {
        OriginalOwner::Writer(Arc::new(
            Db::builder("point-cancellation", store)
                .with_settings(Settings {
                    object_store_max_retries: max_retries,
                    ..settings()
                })
                .with_db_cache(cache, 1)
                .build()
                .await
                .unwrap(),
        ))
    }
}

async fn seeded_store() -> Arc<dyn ObjectStore> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let seed = Db::builder("point-cancellation", store.clone())
        .with_settings(settings())
        .with_db_cache_disabled()
        .build()
        .await
        .unwrap();
    seed.put(KEY, VALUE).await.unwrap();
    seed.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    seed.close().await.unwrap();
    store
}

async fn concurrent_read_cost(hybrid: bool) {
    for cancellable in [false, true] {
        let (db, store, cache, _directory) = fixture(hybrid, Part::Data, false).await;
        let before = store.ranged_gets.load(SeqCst);
        let start = std::time::Instant::now();
        let mut reads = Vec::new();
        let polled = ReadHold::new();
        for _ in 0..4 {
            let db = db.clone();
            let polled = polled.clone();
            reads.push(tokio::spawn(async move {
                let options = ReadOptions {
                    cancellation_token: cancellable.then(CancellationToken::new),
                    ..ReadOptions::default()
                };
                let mut read = Box::pin(db.get_with_options(KEY, &options));
                let mut first = true;
                futures::future::poll_fn(|cx| {
                    let result = std::future::Future::poll(read.as_mut(), cx);
                    if first {
                        first = false;
                        polled.started.fetch_add(1, SeqCst);
                        polled.changed.notify_waiters();
                    }
                    result
                })
                .await
            }));
        }
        let all_polled = timeout(Duration::from_secs(2), polled.wait_started(4)).await;
        let expected = if cancellable { 4 } else { 1 };
        let started = timeout(Duration::from_secs(2), store.hold.wait_started(expected)).await;
        let active = store.hold.active.load(SeqCst);
        store.hold.release();
        let mut results = Vec::new();
        for read in reads {
            results.push(
                timeout(Duration::from_secs(2), read)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        timeout(Duration::from_secs(2), store.hold.wait_empty())
            .await
            .unwrap();
        let gets = store.ranged_gets.load(SeqCst) - before;
        let elapsed = start.elapsed();
        let warm_before = store.ranged_gets.load(SeqCst);
        let warm = db
            .get_with_options(
                KEY,
                &ReadOptions {
                    cancellation_token: Some(CancellationToken::new()),
                    ..ReadOptions::default()
                },
            )
            .await;
        let warm_gets = store.ranged_gets.load(SeqCst) - warm_before;
        db.close().await.unwrap();
        cache.close().await.unwrap();
        println!("concurrent-cost hybrid={hybrid} cancellable={cancellable} readers=4 object_gets={gets} held_loaders={active} elapsed_us={} warm_gets={warm_gets}", elapsed.as_micros());
        assert!(
            all_polled.is_ok(),
            "not all four original reads were polled"
        );
        assert!(
            started.is_ok(),
            "the selected original loaders did not start"
        );
        assert_eq!(active, expected, "concurrent loader ownership changed");
        assert_eq!(
            gets, expected,
            "object GET cost differs from the selected path"
        );
        assert_eq!(
            warm_gets, 0,
            "the next token read did not reuse its cache entry"
        );
        assert_eq!(warm.unwrap(), Some(Bytes::from_static(VALUE)));
        for result in results {
            assert_eq!(result.unwrap(), Some(Bytes::from_static(VALUE)));
        }
    }
}

#[tokio::test]
async fn cancellable_memory_reads_expose_owned_get_cost() {
    concurrent_read_cost(false).await;
}

#[tokio::test]
async fn cancellable_hybrid_reads_expose_owned_get_cost() {
    concurrent_read_cost(true).await;
}

#[derive(Debug)]
struct CancelErrorStore {
    inner: Arc<dyn ObjectStore>,
    token: CancellationToken,
    armed: AtomicBool,
    errors: AtomicUsize,
}

impl std::fmt::Display for CancelErrorStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CancelErrorStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CancelErrorStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if location.as_ref().contains("/compacted/")
            && options.range.is_some()
            && self.armed.load(SeqCst)
        {
            self.errors.fetch_add(1, SeqCst);
            self.token.cancel();
            return Err(object_store::Error::Generic {
                store: "point-cancellation-fixture",
                source: Box::new(std::io::Error::other("original point storage failure")),
            });
        }
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

async fn observed_storage_error_survives_cancel(hybrid: bool) {
    for (reader, snapshot) in [(false, false), (false, true), (true, false), (true, true)] {
        let directory = TempDir::new().unwrap();
        let token = CancellationToken::new();
        let store = Arc::new(CancelErrorStore {
            inner: seeded_store().await,
            token: token.clone(),
            armed: AtomicBool::new(false),
            errors: AtomicUsize::new(0),
        });
        let cache = cache(hybrid, directory.path()).await;
        let db = open_original(store.clone(), cache.clone(), reader, Some(0)).await;
        let original_snapshot = db.snapshot().await.unwrap();
        store.armed.store(true, SeqCst);
        let options = ReadOptions {
            cancellation_token: Some(token),
            ..ReadOptions::default()
        };
        let result = if snapshot {
            original_snapshot.get_with_options(KEY, &options).await
        } else {
            db.get_with_options(KEY, &options).await
        };
        let observed = store.errors.load(SeqCst);
        store.armed.store(false, SeqCst);
        let next = original_snapshot.get(KEY).await;
        drop(original_snapshot);
        db.close().await.unwrap();
        cache.close().await.unwrap();
        assert_eq!(
            observed, 1,
            "the original token read did not observe exactly one storage error"
        );
        let error = result.expect_err("the storage failure returned a value");
        assert!(
            matches!(error.kind(), ErrorKind::Unavailable),
            "storage error was masked by cancellation: {error:?}"
        );
        assert!(
            format!("{error:?}").contains("original point storage failure"),
            "original storage cause was lost: {error:?}"
        );
        assert_eq!(next.unwrap(), Some(Bytes::from_static(VALUE)));
    }
}

#[tokio::test]
async fn memory_point_read_preserves_observed_storage_error() {
    observed_storage_error_survives_cancel(false).await;
}

#[tokio::test]
async fn hybrid_point_read_preserves_observed_storage_error() {
    observed_storage_error_survives_cancel(true).await;
}

struct JoinedCache {
    inner: Arc<dyn DbCache>,
    hold: Arc<ReadHold>,
    armed: AtomicBool,
    original: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[async_trait]
impl DbCache for JoinedCache {
    async fn get_block(
        &self,
        key: &slatedb::db_cache::CachedKey,
    ) -> Result<Option<CachedEntry>, slatedb::Error> {
        if !self.armed.swap(false, SeqCst) {
            return self.inner.get_block(key).await;
        }
        let inner = self.inner.clone();
        let key = key.clone();
        let hold = self.hold.clone();
        let (send, receive) = tokio::sync::oneshot::channel();
        // This fixture holds the return from a real cache lookup in one original task.
        // It models Foyer's detached lookup boundary, not a physical disk stall.
        let original = tokio::spawn(async move {
            let result = inner.get_block(&key).await;
            hold.active.fetch_add(1, SeqCst);
            let retained = ActiveRead(hold.clone());
            hold.started.fetch_add(1, SeqCst);
            hold.changed.notify_waiters();
            loop {
                let changed = hold.changed.notified();
                if hold.released.load(SeqCst) {
                    break;
                }
                changed.await;
            }
            drop(retained);
            let _ = send.send(result);
        });
        let previous = self.original.lock().unwrap().replace(original);
        assert!(
            previous.is_none(),
            "the fixture replaced an original lookup"
        );
        receive
            .await
            .expect("original cache lookup sender disappeared")
    }
    async fn get_index(
        &self,
        key: &slatedb::db_cache::CachedKey,
    ) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.inner.get_index(key).await
    }
    async fn get_filter(
        &self,
        key: &slatedb::db_cache::CachedKey,
    ) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.inner.get_filter(key).await
    }
    async fn get_stats(
        &self,
        key: &slatedb::db_cache::CachedKey,
    ) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.inner.get_stats(key).await
    }
    async fn insert(&self, key: slatedb::db_cache::CachedKey, value: CachedEntry) {
        self.inner.insert(key, value).await
    }
    async fn remove(&self, key: &slatedb::db_cache::CachedKey) {
        self.inner.remove(key).await
    }
    fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
    async fn flush_scope(&self, scope: u64) -> Result<(), slatedb::Error> {
        self.inner.flush_scope(scope).await
    }
}

async fn cancelled_cache_lookup_is_joined(hybrid: bool) {
    for (reader, snapshot) in [(false, false), (false, true), (true, false), (true, true)] {
        let directory = TempDir::new().unwrap();
        let inner = cache(hybrid, directory.path()).await;
        let cache = Arc::new(JoinedCache {
            inner: inner.clone(),
            hold: ReadHold::new(),
            armed: AtomicBool::new(false),
            original: std::sync::Mutex::new(None),
        });
        let db = open_original(seeded_store().await, cache.clone(), reader, None).await;
        let warm = db.get(KEY).await;
        if hybrid {
            cache.flush_scope(1).await.unwrap();
        }
        let token = CancellationToken::new();
        let options = ReadOptions {
            cancellation_token: Some(token.clone()),
            ..ReadOptions::default()
        };
        let original_snapshot = db.snapshot().await.unwrap();
        cache.armed.store(true, SeqCst);
        let mut original = tokio::spawn({
            let db = db.clone();
            let original_snapshot = original_snapshot.clone();
            async move {
                if snapshot {
                    original_snapshot.get_with_options(KEY, &options).await
                } else {
                    db.get_with_options(KEY, &options).await
                }
            }
        });
        let started = timeout(Duration::from_secs(2), cache.hold.wait_started(1)).await;
        token.cancel();
        let early = timeout(Duration::from_millis(50), &mut original).await;
        let active_before_release = cache.hold.active.load(SeqCst);
        cache.hold.release();
        let (returned_early, result) = match early {
            Ok(result) => (true, result.unwrap()),
            Err(_) => (
                false,
                timeout(Duration::from_secs(2), original)
                    .await
                    .unwrap()
                    .unwrap(),
            ),
        };
        let original_lookup = cache.original.lock().unwrap().take();
        if let Some(original_lookup) = original_lookup {
            timeout(Duration::from_secs(2), original_lookup)
                .await
                .unwrap()
                .unwrap();
        }
        let active_after_return = cache.hold.active.load(SeqCst);
        let next = original_snapshot.get(KEY).await;
        drop(original_snapshot);
        db.close().await.unwrap();
        inner.close().await.unwrap();
        assert!(
            started.is_ok(),
            "original cache lookup never reached the held return"
        );
        assert!(
            !returned_early,
            "cancelled read returned before its original cache lookup joined"
        );
        assert_eq!(
            active_before_release, 1,
            "cache lookup did not remain active until release"
        );
        assert_eq!(
            active_after_return, 0,
            "cache lookup survived the original read"
        );
        assert!(
            matches!(result, Err(ref e) if matches!(e.kind(), ErrorKind::Cancelled)),
            "joined cancellation returned a value or wrong error: {result:?}"
        );
        assert_eq!(warm.unwrap(), Some(Bytes::from_static(VALUE)));
        assert_eq!(next.unwrap(), Some(Bytes::from_static(VALUE)));
    }
}

#[tokio::test]
async fn cancelled_memory_cache_lookup_keeps_original_join() {
    cancelled_cache_lookup_is_joined(false).await;
}

#[tokio::test]
async fn cancelled_hybrid_cache_lookup_keeps_original_join() {
    cancelled_cache_lookup_is_joined(true).await;
}

struct LookaheadCache {
    inner: Arc<dyn DbCache>,
    armed: AtomicBool,
    selected: Arc<ReadHold>,
    pending: Arc<ReadHold>,
    filters: std::sync::Mutex<Vec<slatedb::db_cache::CachedKey>>,
    original: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

async fn hold_cache_return(hold: Arc<ReadHold>) {
    hold.active.fetch_add(1, SeqCst);
    let retained = ActiveRead(hold.clone());
    hold.started.fetch_add(1, SeqCst);
    hold.changed.notify_waiters();
    loop {
        let changed = hold.changed.notified();
        if hold.released.load(SeqCst) {
            break;
        }
        changed.await;
    }
    drop(retained);
}

#[async_trait]
impl DbCache for LookaheadCache {
    async fn get_block(
        &self,
        key: &slatedb::db_cache::CachedKey,
    ) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.inner.get_block(key).await
    }

    async fn get_index(
        &self,
        key: &slatedb::db_cache::CachedKey,
    ) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.inner.get_index(key).await
    }

    async fn get_filter(
        &self,
        key: &slatedb::db_cache::CachedKey,
    ) -> Result<Option<CachedEntry>, slatedb::Error> {
        if !self.armed.load(SeqCst) {
            return self.inner.get_filter(key).await;
        }
        let ordinal = {
            let mut filters = self.filters.lock().unwrap();
            filters.push(key.clone());
            filters.len()
        };
        match ordinal {
            2 => {
                let result = self.inner.get_filter(key).await;
                hold_cache_return(self.selected.clone()).await;
                result
            }
            3 => {
                let inner = self.inner.clone();
                let key = key.clone();
                let hold = self.pending.clone();
                let (send, receive) = tokio::sync::oneshot::channel();
                // Hold the return from the original Foyer lookup in its own task.
                // This models its task boundary, not a physical disk stall.
                let original = tokio::spawn(async move {
                    let result = inner.get_filter(&key).await;
                    hold_cache_return(hold).await;
                    let _ = send.send(result);
                });
                let previous = self.original.lock().unwrap().replace(original);
                assert!(
                    previous.is_none(),
                    "lookahead fixture replaced its original lookup"
                );
                receive.await.expect("lookahead cache sender disappeared")
            }
            _ => self.inner.get_filter(key).await,
        }
    }

    async fn get_stats(
        &self,
        key: &slatedb::db_cache::CachedKey,
    ) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.inner.get_stats(key).await
    }

    async fn insert(&self, key: slatedb::db_cache::CachedKey, value: CachedEntry) {
        self.inner.insert(key, value).await
    }

    async fn remove(&self, key: &slatedb::db_cache::CachedKey) {
        self.inner.remove(key).await
    }

    fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }

    async fn flush_scope(&self, scope: u64) -> Result<(), slatedb::Error> {
        self.inner.flush_scope(scope).await
    }
}

async fn lookahead_store(tombstone: bool, extra_older: usize) -> Arc<dyn ObjectStore> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let seed = Db::builder("point-cancellation", store.clone())
        .with_settings(settings())
        .with_db_cache_disabled()
        .build()
        .await
        .unwrap();
    for _ in 0..=extra_older {
        seed.put(KEY, b"old-value").await.unwrap();
        seed.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
    }
    if tombstone {
        seed.delete(KEY).await.unwrap();
    } else {
        seed.put(KEY, VALUE).await.unwrap();
    }
    seed.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    // The newest table covers KEY but does not contain it.
    seed.put(b"point-j", b"before").await.unwrap();
    seed.put(b"point-l", b"after").await.unwrap();
    seed.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    seed.close().await.unwrap();
    store
}

async fn point_joins_started_lookahead(
    reader: bool,
    snapshot: bool,
    cancel: bool,
    tombstone: bool,
    extra_older: usize,
) {
    let directory = TempDir::new().unwrap();
    let inner = cache(true, directory.path()).await;
    let cache = Arc::new(LookaheadCache {
        inner: inner.clone(),
        armed: AtomicBool::new(false),
        selected: ReadHold::new(),
        pending: ReadHold::new(),
        filters: std::sync::Mutex::new(Vec::new()),
        original: std::sync::Mutex::new(None),
    });
    let db = open_original(
        lookahead_store(tombstone, extra_older).await,
        cache.clone(),
        reader,
        Some(0),
    )
    .await;
    let manifest = db.manifest();
    assert_eq!(
        manifest.l0().len(),
        3 + extra_older,
        "fixture lost an original L0 table"
    );
    for table in manifest.l0() {
        db.warm_sst(table.sst.id, &[CacheTarget::Filters, CacheTarget::Index])
            .await
            .unwrap();
    }
    cache.flush_scope(1).await.unwrap();
    let original_snapshot = db.snapshot().await.unwrap();
    let token = CancellationToken::new();
    let options = ReadOptions {
        cancellation_token: Some(token.clone()),
        ..ReadOptions::default()
    };
    cache.armed.store(true, SeqCst);
    let mut original = tokio::spawn({
        let db = db.clone();
        let original_snapshot = original_snapshot.clone();
        async move {
            if snapshot {
                original_snapshot.get_with_options(KEY, &options).await
            } else {
                db.get_with_options(KEY, &options).await
            }
        }
    });
    let selected_started = timeout(Duration::from_secs(2), cache.selected.wait_started(1)).await;
    let pending_started = timeout(Duration::from_secs(2), cache.pending.wait_started(1)).await;
    if cancel {
        token.cancel();
    }
    cache.selected.release();
    let selected_joined = timeout(Duration::from_secs(2), cache.selected.wait_empty()).await;
    let early = timeout(Duration::from_millis(50), &mut original).await;
    let active_before_release = cache.pending.active.load(SeqCst);
    cache.pending.release();
    let (returned_early, result) = match early {
        Ok(result) => (true, result.unwrap()),
        Err(_) => (
            false,
            timeout(Duration::from_secs(2), original)
                .await
                .unwrap()
                .unwrap(),
        ),
    };
    let original_lookup = cache.original.lock().unwrap().take();
    let retained_original = original_lookup.is_some();
    if let Some(original_lookup) = original_lookup {
        timeout(Duration::from_secs(2), original_lookup)
            .await
            .unwrap()
            .unwrap();
    }
    let active_after_join = cache.pending.active.load(SeqCst);
    let filters = cache.filters.lock().unwrap().clone();
    cache.armed.store(false, SeqCst);
    let next = original_snapshot.get(KEY).await;
    drop(original_snapshot);
    db.close().await.unwrap();
    inner.close().await.unwrap();

    assert!(
        selected_started.is_ok(),
        "selected source never entered its original cache lookup"
    );
    assert!(
        pending_started.is_ok(),
        "older source never entered its original cache lookup"
    );
    assert!(
        selected_joined.is_ok(),
        "selected source did not leave its cache hold"
    );
    assert!(
        retained_original,
        "fixture lost the original pending cache task"
    );
    assert_eq!(
        filters.len(),
        (3 + extra_older).min(5),
        "point lookup started sources outside its original lookahead window"
    );
    let distinct: std::collections::HashSet<_> = filters.iter().collect();
    assert_eq!(
        distinct.len(),
        filters.len(),
        "fixture revisited an original filter"
    );

    assert_eq!(
        active_before_release, 1,
        "older original lookup did not remain held"
    );
    assert_eq!(
        active_after_join, 0,
        "older original lookup survived cleanup"
    );
    let expected = (!tombstone).then(|| Bytes::from_static(VALUE));
    assert_eq!(
        next.unwrap(),
        expected,
        "the same original view changed after cleanup"
    );
    if cancel {
        assert!(
            matches!(result, Err(ref e) if matches!(e.kind(), ErrorKind::Cancelled)),
            "cancelled lookahead returned a value or wrong error: {result:?}"
        );
    } else {
        assert_eq!(
            result.unwrap(),
            expected,
            "lookahead changed the selected row"
        );
    }
    assert!(
        !returned_early,
        "point read returned before its older original cache lookup joined"
    );
}

#[tokio::test]
async fn writer_hit_joins_started_lookahead() {
    point_joins_started_lookahead(false, false, false, false, 0).await;
}

#[tokio::test]
async fn writer_snapshot_hit_joins_started_lookahead() {
    point_joins_started_lookahead(false, true, false, false, 0).await;
}

#[tokio::test]
async fn reader_hit_joins_started_lookahead() {
    point_joins_started_lookahead(true, false, false, false, 0).await;
}

#[tokio::test]
async fn reader_snapshot_hit_joins_started_lookahead() {
    point_joins_started_lookahead(true, true, false, false, 0).await;
}

#[tokio::test]
async fn writer_cancellation_joins_started_lookahead() {
    point_joins_started_lookahead(false, false, true, false, 0).await;
}

#[tokio::test]
async fn writer_snapshot_cancellation_joins_started_lookahead() {
    point_joins_started_lookahead(false, true, true, false, 0).await;
}

#[tokio::test]
async fn reader_cancellation_joins_started_lookahead() {
    point_joins_started_lookahead(true, false, true, false, 0).await;
}

#[tokio::test]
async fn reader_snapshot_cancellation_joins_started_lookahead() {
    point_joins_started_lookahead(true, true, true, false, 0).await;
}

#[tokio::test]
async fn writer_tombstone_joins_started_lookahead() {
    point_joins_started_lookahead(false, false, false, true, 0).await;
}

#[tokio::test]
async fn writer_hit_joins_started_lookahead_without_opening_the_rest() {
    point_joins_started_lookahead(false, false, false, false, 3).await;
}

struct KeepMergeOperand;

impl slatedb::MergeOperator for KeepMergeOperand {
    fn merge(
        &self,
        _key: &Bytes,
        _existing: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, slatedb::MergeOperatorError> {
        Ok(operand)
    }
}

async fn lookahead_merge_store() -> Arc<dyn ObjectStore> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let seed = Db::builder("point-cancellation", store.clone())
        .with_settings(settings())
        .with_merge_operator(Arc::new(KeepMergeOperand))
        .with_db_cache_disabled()
        .build()
        .await
        .unwrap();
    seed.put(KEY, b"old-value").await.unwrap();
    seed.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    seed.merge(KEY, b"operand").await.unwrap();
    seed.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    seed.put(b"point-j", b"before").await.unwrap();
    seed.put(b"point-l", b"after").await.unwrap();
    seed.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    seed.close().await.unwrap();
    store
}

async fn merge_rejection_joins_started_lookahead(reader: bool, snapshot: bool) {
    let directory = TempDir::new().unwrap();
    let inner = cache(true, directory.path()).await;
    let cache = Arc::new(LookaheadCache {
        inner: inner.clone(),
        armed: AtomicBool::new(false),
        selected: ReadHold::new(),
        pending: ReadHold::new(),
        filters: std::sync::Mutex::new(Vec::new()),
        original: std::sync::Mutex::new(None),
    });
    let db = open_original(
        lookahead_merge_store().await,
        cache.clone(),
        reader,
        Some(0),
    )
    .await;
    let manifest = db.manifest();
    assert_eq!(manifest.l0().len(), 3, "fixture lost an original L0 table");
    for table in manifest.l0() {
        db.warm_sst(table.sst.id, &[CacheTarget::Filters, CacheTarget::Index])
            .await
            .unwrap();
    }
    cache.flush_scope(1).await.unwrap();
    let original_snapshot = db.snapshot().await.unwrap();
    let token = CancellationToken::new();
    let options = ReadOptions {
        cancellation_token: Some(token.clone()),
        ..ReadOptions::default()
    };
    cache.armed.store(true, SeqCst);
    let mut original = tokio::spawn({
        let db = db.clone();
        let original_snapshot = original_snapshot.clone();
        async move {
            if snapshot {
                original_snapshot.get_with_options(KEY, &options).await
            } else {
                db.get_with_options(KEY, &options).await
            }
        }
    });
    let selected_started = timeout(Duration::from_secs(2), cache.selected.wait_started(1)).await;
    let pending_started = timeout(Duration::from_secs(2), cache.pending.wait_started(1)).await;
    cache.selected.release();
    let selected_joined = timeout(Duration::from_secs(2), cache.selected.wait_empty()).await;
    let early = timeout(Duration::from_millis(50), &mut original).await;
    let active_before_release = cache.pending.active.load(SeqCst);
    cache.pending.release();
    let (returned_early, result) = match early {
        Ok(result) => (true, result.unwrap()),
        Err(_) => (
            false,
            timeout(Duration::from_secs(2), original)
                .await
                .unwrap()
                .unwrap(),
        ),
    };
    let original_lookup = cache.original.lock().unwrap().take();
    let retained_original = original_lookup.is_some();
    if let Some(original_lookup) = original_lookup {
        timeout(Duration::from_secs(2), original_lookup)
            .await
            .unwrap()
            .unwrap();
    }
    let active_after_join = cache.pending.active.load(SeqCst);
    let filters = cache.filters.lock().unwrap().clone();
    cache.armed.store(false, SeqCst);
    let next = original_snapshot.get(b"point-j").await;
    let same_error = original_snapshot.get(KEY).await;
    drop(original_snapshot);
    db.close().await.unwrap();
    inner.close().await.unwrap();

    assert!(
        selected_started.is_ok(),
        "selected source never entered its original cache lookup"
    );
    assert!(
        pending_started.is_ok(),
        "older source never entered its original cache lookup"
    );
    assert!(
        selected_joined.is_ok(),
        "selected source did not leave its cache hold"
    );
    assert!(
        retained_original,
        "fixture lost the original pending cache task"
    );
    assert_eq!(
        filters.len(),
        (3).min(5),
        "point lookup started sources outside its original lookahead window"
    );
    let distinct: std::collections::HashSet<_> = filters.iter().collect();
    assert_eq!(
        distinct.len(),
        filters.len(),
        "fixture revisited an original filter"
    );

    assert_eq!(
        active_before_release, 1,
        "older original lookup did not remain held"
    );
    assert_eq!(
        active_after_join, 0,
        "older original lookup survived cleanup"
    );
    assert_eq!(
        next.unwrap(),
        Some(Bytes::from_static(b"before")),
        "the same original view failed after cleanup"
    );
    for result in [result, same_error] {
        let error = result.expect_err("merge operand returned bytes without its required operator");
        assert!(
            matches!(error.kind(), ErrorKind::Invalid),
            "merge error category changed: {error:?}"
        );
        assert!(
            error.to_string().contains("merge operator missing"),
            "original merge error was lost: {error:?}"
        );
    }
    assert!(
        !returned_early,
        "merge rejection returned before its older original cache lookup joined"
    );
}

#[tokio::test]
async fn multisource_boundary_writer_merge_rejection_joins_started_lookahead() {
    merge_rejection_joins_started_lookahead(false, false).await;
}

#[tokio::test]
async fn multisource_boundary_writer_snapshot_merge_rejection_joins_started_lookahead() {
    merge_rejection_joins_started_lookahead(false, true).await;
}

#[tokio::test]
async fn multisource_boundary_reader_merge_rejection_joins_started_lookahead() {
    merge_rejection_joins_started_lookahead(true, false).await;
}

#[tokio::test]
async fn multisource_boundary_reader_snapshot_merge_rejection_joins_started_lookahead() {
    merge_rejection_joins_started_lookahead(true, true).await;
}

#[derive(Debug)]
struct LookaheadErrorStore {
    inner: Arc<dyn ObjectStore>,
    target: std::sync::Mutex<Option<Path>>,
    armed: AtomicBool,
    errors: AtomicUsize,
    changed: Notify,
}

impl std::fmt::Display for LookaheadErrorStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LookaheadErrorStore({})", self.inner)
    }
}

impl LookaheadErrorStore {
    async fn wait_error(&self) {
        loop {
            let changed = self.changed.notified();
            if self.errors.load(SeqCst) != 0 {
                return;
            }
            changed.await;
        }
    }
}

#[async_trait]
impl ObjectStore for LookaheadErrorStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if self.armed.load(SeqCst)
            && options.range.is_some()
            && self.target.lock().unwrap().as_ref() == Some(location)
        {
            self.errors.fetch_add(1, SeqCst);
            self.changed.notify_waiters();
            return Err(object_store::Error::Generic {
                store: "lookahead-error-fixture",
                source: Box::new(std::io::Error::other("original lookahead storage failure")),
            });
        }
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

async fn lookahead_error_order(reader: bool, snapshot: bool, selected_error: bool) {
    for tombstone in [false, true] {
        // A tombstone has no value to decode; the selected-error case needs only a value.
        if selected_error && tombstone {
            continue;
        }
        let directory = TempDir::new().unwrap();
        let store = Arc::new(LookaheadErrorStore {
            inner: lookahead_store(tombstone, 0).await,
            target: std::sync::Mutex::new(None),
            armed: AtomicBool::new(false),
            errors: AtomicUsize::new(0),
            changed: Notify::new(),
        });
        let inner = cache(true, directory.path()).await;
        let cache = Arc::new(LookaheadCache {
            inner: inner.clone(),
            armed: AtomicBool::new(false),
            selected: ReadHold::new(),
            pending: ReadHold::new(),
            filters: std::sync::Mutex::new(Vec::new()),
            original: std::sync::Mutex::new(None),
        });
        let db = open_original(store.clone(), cache.clone(), reader, Some(0)).await;
        let manifest = db.manifest();
        assert_eq!(manifest.l0().len(), 3);
        for table in manifest.l0() {
            db.warm_sst(table.sst.id, &[CacheTarget::Filters, CacheTarget::Index])
                .await
                .unwrap();
        }
        cache.flush_scope(1).await.unwrap();
        let target = &manifest.l0()[if selected_error { 1 } else { 2 }];
        *store.target.lock().unwrap() = Some(Path::from(format!(
            "point-cancellation/compacted/{}.sst",
            target.sst.id.value()
        )));
        let view = db.snapshot().await.unwrap();
        let token = CancellationToken::new();
        let options = ReadOptions {
            cancellation_token: Some(token.clone()),
            ..ReadOptions::default()
        };
        store.armed.store(true, SeqCst);
        cache.armed.store(true, SeqCst);
        let mut original = tokio::spawn({
            let db = db.clone();
            let view = view.clone();
            async move {
                if snapshot {
                    view.get_with_options(KEY, &options).await
                } else {
                    db.get_with_options(KEY, &options).await
                }
            }
        });
        let selected_started =
            timeout(Duration::from_secs(2), cache.selected.wait_started(1)).await;
        let pending_started = timeout(Duration::from_secs(2), cache.pending.wait_started(1)).await;
        if selected_error {
            cache.selected.release();
        } else {
            cache.pending.release();
        }
        let error_observed = timeout(Duration::from_secs(2), store.wait_error()).await;
        let early = if selected_error {
            Some(timeout(Duration::from_millis(50), &mut original).await)
        } else {
            None
        };
        let pending_held = cache.pending.active.load(SeqCst);
        cache.selected.release();
        cache.pending.release();
        let (returned_early, result) = match early {
            Some(Ok(result)) => (true, result.unwrap()),
            _ => (
                false,
                timeout(Duration::from_secs(2), original)
                    .await
                    .unwrap()
                    .unwrap(),
            ),
        };
        let lookup = cache.original.lock().unwrap().take();
        let retained_original = lookup.is_some();
        if let Some(lookup) = lookup {
            timeout(Duration::from_secs(2), lookup)
                .await
                .unwrap()
                .unwrap();
        }
        let active_after = cache.pending.active.load(SeqCst);
        let filters = cache.filters.lock().unwrap().clone();
        let errors = store.errors.load(SeqCst);
        store.armed.store(false, SeqCst);
        cache.armed.store(false, SeqCst);
        let next = view.get(KEY).await;
        drop(view);
        db.close().await.unwrap();
        inner.close().await.unwrap();
        assert!(
            selected_started.is_ok() && pending_started.is_ok(),
            "original selected and sibling cache lookups did not both start"
        );
        assert!(
            error_observed.is_ok(),
            "the exact selected SST did not produce its storage error"
        );
        assert!(
            retained_original,
            "original sibling lookup was not retained"
        );
        assert_eq!(active_after, 0, "original sibling survived fixture cleanup");
        assert_eq!(filters.len(), 3, "source window changed");
        assert_eq!(errors, 1, "fixture retry policy or selected SST changed");
        assert!(
            !token.is_cancelled(),
            "unused child cancellation escaped to original caller token"
        );
        let expected = (!tombstone).then(|| Bytes::from_static(VALUE));
        assert_eq!(
            next.unwrap(),
            expected,
            "same original snapshot changed after error"
        );
        if selected_error {
            let error = result.expect_err("selected storage failure returned a value");
            assert!(
                matches!(error.kind(), ErrorKind::Unavailable),
                "selected error category changed: {error:?}"
            );
            assert!(
                format!("{error:?}").contains("original lookahead storage failure"),
                "selected error cause changed: {error:?}"
            );
            assert_eq!(
                pending_held, 1,
                "original sibling was not held when error returned"
            );
            assert!(
                !returned_early,
                "selected storage error returned before its older original cache lookup joined"
            );
        } else {
            assert_eq!(
                result.unwrap(),
                expected,
                "unused sibling error replaced the selected row"
            );
        }
    }
}

#[tokio::test]
async fn multisource_boundary_writer_selected_error_joins_started_lookahead() {
    lookahead_error_order(false, false, true).await;
}
#[tokio::test]
async fn multisource_boundary_writer_snapshot_selected_error_joins_started_lookahead() {
    lookahead_error_order(false, true, true).await;
}
#[tokio::test]
async fn multisource_boundary_reader_selected_error_joins_started_lookahead() {
    lookahead_error_order(true, false, true).await;
}
#[tokio::test]
async fn multisource_boundary_reader_snapshot_selected_error_joins_started_lookahead() {
    lookahead_error_order(true, true, true).await;
}
#[tokio::test]
async fn multisource_boundary_writer_unused_lookahead_error_keeps_selected_row() {
    lookahead_error_order(false, false, false).await;
}
#[tokio::test]
async fn multisource_boundary_writer_snapshot_unused_lookahead_error_keeps_selected_row() {
    lookahead_error_order(false, true, false).await;
}
#[tokio::test]
async fn multisource_boundary_reader_unused_lookahead_error_keeps_selected_row() {
    lookahead_error_order(true, false, false).await;
}
#[tokio::test]
async fn multisource_boundary_reader_snapshot_unused_lookahead_error_keeps_selected_row() {
    lookahead_error_order(true, true, false).await;
}

// Entries come from actual warming. Returning them synchronously makes the
// queued-but-unpolled boundary deterministic; this is not a physical cache test.
struct ReadyLookaheadCache {
    inner: Arc<dyn DbCache>,
    entries: std::sync::Mutex<std::collections::HashMap<slatedb::db_cache::CachedKey, CachedEntry>>,
    armed: AtomicBool,
    filters: std::sync::Mutex<Vec<slatedb::db_cache::CachedKey>>,
}

#[async_trait]
impl DbCache for ReadyLookaheadCache {
    async fn get_block(
        &self,
        key: &slatedb::db_cache::CachedKey,
    ) -> Result<Option<CachedEntry>, slatedb::Error> {
        if self.armed.load(SeqCst) {
            return Ok(self.entries.lock().unwrap().get(key).cloned());
        }
        self.inner.get_block(key).await
    }
    async fn get_index(
        &self,
        key: &slatedb::db_cache::CachedKey,
    ) -> Result<Option<CachedEntry>, slatedb::Error> {
        if self.armed.load(SeqCst) {
            return Ok(self.entries.lock().unwrap().get(key).cloned());
        }
        self.inner.get_index(key).await
    }
    async fn get_filter(
        &self,
        key: &slatedb::db_cache::CachedKey,
    ) -> Result<Option<CachedEntry>, slatedb::Error> {
        if self.armed.load(SeqCst) {
            self.filters.lock().unwrap().push(key.clone());
            return Ok(self.entries.lock().unwrap().get(key).cloned());
        }
        self.inner.get_filter(key).await
    }
    async fn get_stats(
        &self,
        key: &slatedb::db_cache::CachedKey,
    ) -> Result<Option<CachedEntry>, slatedb::Error> {
        if self.armed.load(SeqCst) {
            return Ok(self.entries.lock().unwrap().get(key).cloned());
        }
        self.inner.get_stats(key).await
    }
    async fn insert(&self, key: slatedb::db_cache::CachedKey, value: CachedEntry) {
        self.entries
            .lock()
            .unwrap()
            .insert(key.clone(), value.clone());
        self.inner.insert(key, value).await;
    }
    async fn remove(&self, key: &slatedb::db_cache::CachedKey) {
        self.entries.lock().unwrap().remove(key);
        self.inner.remove(key).await;
    }
    fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
    async fn flush_scope(&self, scope: u64) -> Result<(), slatedb::Error> {
        self.inner.flush_scope(scope).await
    }
}

async fn ready_hit_does_not_start_queued_lookahead(reader: bool, snapshot: bool) {
    for tombstone in [false, true] {
        let directory = TempDir::new().unwrap();
        let inner = cache(true, directory.path()).await;
        let store = Arc::new(HeldStore {
            inner: lookahead_store(tombstone, 3).await,
            hold: ReadHold::new(),
            ranged_gets: AtomicUsize::new(0),
        });
        let cache = Arc::new(ReadyLookaheadCache {
            inner: inner.clone(),
            entries: std::sync::Mutex::new(std::collections::HashMap::new()),
            armed: AtomicBool::new(false),
            filters: std::sync::Mutex::new(Vec::new()),
        });
        let db = open_original(store.clone(), cache.clone(), reader, Some(0)).await;
        let manifest = db.manifest();
        assert_eq!(manifest.l0().len(), 6);
        for table in manifest.l0() {
            db.warm_sst(table.sst.id, &CacheTarget::all())
                .await
                .unwrap();
        }
        cache.flush_scope(1).await.unwrap();
        let view = db.snapshot().await.unwrap();
        let token = CancellationToken::new();
        let options = ReadOptions {
            cancellation_token: Some(token.clone()),
            ..ReadOptions::default()
        };
        let before = store.ranged_gets.load(SeqCst);
        cache.armed.store(true, SeqCst);
        let result = if snapshot {
            view.get_with_options(KEY, &options).await
        } else {
            db.get_with_options(KEY, &options).await
        };
        let filters = cache.filters.lock().unwrap().clone();
        let gets = store.ranged_gets.load(SeqCst) - before;
        cache.armed.store(false, SeqCst);
        let next = view.get(KEY).await;
        drop(view);
        db.close().await.unwrap();
        inner.close().await.unwrap();
        let expected = (!tombstone).then(|| Bytes::from_static(VALUE));
        assert_eq!(result.unwrap(), expected, "ready selected row changed");
        assert_eq!(next.unwrap(), expected, "same original snapshot changed");
        assert_eq!(
            filters.len(),
            2,
            "a queued but unpolled older source started its cache lookup"
        );
        assert_ne!(filters[0], filters[1], "the selected filter was read twice");
        assert_eq!(
            gets, 0,
            "ready cache control unexpectedly performed object reads"
        );
        assert!(
            !token.is_cancelled(),
            "window cleanup cancelled original caller token"
        );
    }
}

#[tokio::test]
async fn multisource_boundary_writer_ready_hit_does_not_start_queued_lookahead() {
    ready_hit_does_not_start_queued_lookahead(false, false).await;
}
#[tokio::test]
async fn multisource_boundary_writer_snapshot_ready_hit_does_not_start_queued_lookahead() {
    ready_hit_does_not_start_queued_lookahead(false, true).await;
}
#[tokio::test]
async fn multisource_boundary_reader_ready_hit_does_not_start_queued_lookahead() {
    ready_hit_does_not_start_queued_lookahead(true, false).await;
}
#[tokio::test]
async fn multisource_boundary_reader_snapshot_ready_hit_does_not_start_queued_lookahead() {
    ready_hit_does_not_start_queued_lookahead(true, true).await;
}

#[path = "point_read_cancellation/scan_coverage.rs"]
mod scan_coverage;
