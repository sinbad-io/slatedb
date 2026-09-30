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
