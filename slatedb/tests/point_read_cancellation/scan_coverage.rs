use super::*;
use slatedb::config::ScanOptions;
use slatedb::{DbIterator, IterationOrder};

type Rows = Vec<(Bytes, Bytes)>;

async fn original_scan(
    owner: &OriginalOwner,
    snapshot: Option<&OriginalView>,
    options: &ScanOptions,
    exact: bool,
) -> Result<DbIterator, slatedb::Error> {
    let range = if exact {
        (
            std::ops::Bound::Included(KEY),
            std::ops::Bound::Included(KEY),
        )
    } else {
        (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
    };
    match snapshot {
        Some(OriginalView::Writer(view)) => view.scan_with_options(range, options).await,
        Some(OriginalView::Reader(view)) => view.scan_with_options(range, options).await,
        None => match owner {
            OriginalOwner::Writer(db) => db.scan_with_options(range, options).await,
            OriginalOwner::Reader(db) => db.scan_with_options(range, options).await,
        },
    }
}

async fn complete_scan(
    owner: &OriginalOwner,
    snapshot: Option<&OriginalView>,
    options: &ScanOptions,
    exact: bool,
) -> Result<Rows, slatedb::Error> {
    let mut scan = original_scan(owner, snapshot, options, exact).await?;
    let mut rows = Vec::new();
    let result = loop {
        match scan.next().await {
            Ok(Some(row)) => rows.push((row.key, row.value)),
            Ok(None) => break Ok(rows),
            Err(error) => break Err(error),
        }
    };
    let closed = scan.close().await;
    match result {
        Err(error) => Err(error),
        Ok(rows) => closed.map(|()| rows),
    }
}

// Keep every original task handle, including one whose bounded wait expires.
// A timeout is a failed fixture and never an accepted ownership observation.
async fn join_original_scans(
    scans: Vec<tokio::task::JoinHandle<Result<Rows, slatedb::Error>>>,
) -> (Vec<Result<Rows, String>>, bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut results = Vec::new();
    let mut all_joined = true;
    for mut scan in scans {
        match tokio::time::timeout_at(deadline, &mut scan).await {
            Ok(Ok(result)) => results.push(result.map_err(|error| error.to_string())),
            Ok(Err(error)) => results.push(Err(format!("original task failed: {error}"))),
            Err(_) => {
                scan.abort();
                all_joined &= timeout(Duration::from_secs(2), &mut scan).await.is_ok();
                results.push(Err("original scan exceeded the fixture join bound".into()));
            }
        }
    }
    (results, all_joined)
}

async fn cancelled_scan_metadata(hybrid: bool, part: Part) {
    for (reader, snapshot) in [(false, false), (false, true), (true, false), (true, true)] {
        let (owner, store, cache, _directory) = fixture(hybrid, part, reader).await;
        let view = owner.snapshot().await.unwrap();
        let parent = CancellationToken::new();
        let options = ScanOptions {
            cancellation_token: Some(parent.clone()),
            ..ScanOptions::default()
        };
        let mut original = tokio::spawn({
            let owner = owner.clone();
            let view = view.clone();
            async move { complete_scan(&owner, snapshot.then_some(&view), &options, true).await }
        });
        let started = timeout(Duration::from_secs(2), store.hold.wait_started(1)).await;
        parent.cancel();
        let before_release = timeout(Duration::from_secs(1), &mut original).await;
        let returned = before_release.is_ok();
        let active_at_return = store.hold.active.load(SeqCst);
        let dropped_at_return = store.hold.dropped.load(SeqCst);
        store.hold.release();
        let result = match before_release {
            Ok(result) => result.map_err(|error| error.to_string()),
            Err(_) => match timeout(Duration::from_secs(2), &mut original).await {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(_) => {
                    original.abort();
                    let joined = timeout(Duration::from_secs(2), original).await;
                    assert!(joined.is_ok(), "original metadata Scan task did not join");
                    Err("metadata Scan exceeded its join bound".into())
                }
            },
        };
        let empty = timeout(Duration::from_secs(2), store.hold.wait_empty()).await;
        let same_view = complete_scan(&owner, Some(&view), &ScanOptions::default(), true).await;
        drop(view);
        let owner_closed = owner.close().await;
        let cache_closed = cache.close().await;
        assert!(
            started.is_ok(),
            "Scan never reached the actual {part:?} object read"
        );
        assert!(
            returned,
            "cancelled Scan waited for external object release"
        );
        assert_eq!(
            active_at_return, 0,
            "Scan returned with an original object read live"
        );
        assert_eq!(dropped_at_return, 1);
        assert!(empty.is_ok());
        assert!(matches!(result.unwrap(), Err(ref error) if error.kind() == ErrorKind::Cancelled));
        assert_eq!(
            same_view.unwrap(),
            vec![(Bytes::from_static(KEY), Bytes::from_static(VALUE))]
        );
        owner_closed.unwrap();
        cache_closed.unwrap();
    }
}

#[tokio::test]
async fn scan_memory_filter_cancellation_joins_original_load() {
    cancelled_scan_metadata(false, Part::Filter).await;
}

#[tokio::test]
async fn scan_hybrid_filter_cancellation_joins_original_load() {
    cancelled_scan_metadata(true, Part::Filter).await;
}

#[tokio::test]
async fn scan_memory_index_cancellation_joins_original_load() {
    cancelled_scan_metadata(false, Part::Index).await;
}

#[tokio::test]
async fn scan_hybrid_index_cancellation_joins_original_load() {
    cancelled_scan_metadata(true, Part::Index).await;
}

async fn scan_cost(hybrid: bool) {
    for tokenized in [false, true] {
        for descending in [false, true] {
            let directory = TempDir::new().unwrap();
            let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let seed = Db::builder("point-cancellation", inner.clone())
                .with_settings(settings())
                .with_db_cache_disabled()
                .build()
                .await
                .unwrap();
            let mut expected = Vec::new();
            for n in 0u8..64 {
                let key = Bytes::from(format!("scan-{n:03}"));
                let value = Bytes::from(vec![n; 2048]);
                seed.put(&key, &value).await.unwrap();
                expected.push((key, value));
            }
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
            let owner = open_original(store.clone(), cache.clone(), false, Some(0)).await;
            let manifest = owner.manifest();
            assert_eq!(manifest.l0().len(), 1, "cost fixture must use one SST");
            owner
                .warm_sst(
                    manifest.l0()[0].sst.id,
                    &[CacheTarget::Filters, CacheTarget::Index],
                )
                .await
                .unwrap();
            store.hold.offset.store(0, SeqCst);
            if descending {
                expected.reverse();
            }
            let mut windows = Vec::new();
            for cold in [true, false] {
                // Cold means data blocks are absent. Metadata is warm in both
                // windows. Hybrid warm includes a flush, not forced eviction.
                if !cold && hybrid {
                    cache.flush_scope(1).await.unwrap();
                }
                let first_polls = ReadHold::new();
                let before = store.ranged_gets.load(SeqCst);
                let start = std::time::Instant::now();
                let mut originals = Vec::new();
                for _ in 0..4 {
                    let owner = owner.clone();
                    let first_polls = first_polls.clone();
                    originals.push(tokio::spawn(async move {
                        let options = ScanOptions {
                            cancellation_token: tokenized.then(CancellationToken::new),
                            cache_blocks: true,
                            max_fetch_tasks: 2,
                            order: if descending {
                                IterationOrder::Descending
                            } else {
                                IterationOrder::Ascending
                            },
                            ..ScanOptions::default()
                        };
                        let mut scan = Box::pin(complete_scan(&owner, None, &options, false));
                        let mut first = true;
                        futures::future::poll_fn(|cx| {
                            let result = std::future::Future::poll(scan.as_mut(), cx);
                            if first {
                                first = false;
                                first_polls.started.fetch_add(1, SeqCst);
                                first_polls.changed.notify_waiters();
                            }
                            result
                        })
                        .await
                    }));
                }
                let polled = timeout(Duration::from_secs(2), first_polls.wait_started(4))
                    .await
                    .is_ok();
                let held = if cold {
                    timeout(Duration::from_secs(2), store.hold.wait_started(1))
                        .await
                        .is_ok()
                } else {
                    true
                };
                let held_loaders = store.hold.active.load(SeqCst);
                // Release on every outcome before joining every original.
                store.hold.release();
                let (rows, all_joined) = join_original_scans(originals).await;
                let elapsed = start.elapsed();
                let empty = timeout(Duration::from_secs(2), store.hold.wait_empty())
                    .await
                    .is_ok();
                let gets = store.ranged_gets.load(SeqCst) - before;
                println!("scan-cost hybrid={hybrid} tokenized={tokenized} descending={descending} cold_data={cold} warm_metadata=true callers=4 rows_per_caller=64 value_bytes=2048 fetch_tasks=2 object_ranged_gets={gets} held_loaders={held_loaders} elapsed_us={} original_tasks_joined={all_joined} original_loaders_empty={empty}", elapsed.as_micros());
                windows.push((polled, held, all_joined, empty, rows));
            }
            let owner_closed = owner.close().await;
            let cache_closed = cache.close().await;
            for (polled, held, joined, empty, rows) in windows {
                assert!(
                    polled && held,
                    "four original calls did not reach the measured window"
                );
                assert!(
                    joined && empty,
                    "the measured window retained original work"
                );
                assert_eq!(rows.len(), 4);
                for rows in rows {
                    let rows = rows.unwrap();
                    assert_eq!(rows.len(), expected.len());
                    for (index, ((key, value), (expected_key, expected_value))) in
                        rows.iter().zip(&expected).enumerate()
                    {
                        assert_eq!(key, expected_key, "row {index} is out of order");
                        assert!(
                            value == expected_value,
                            "row {index} has different value bytes (actual length {}, expected {})",
                            value.len(),
                            expected_value.len()
                        );
                    }
                }
            }
            owner_closed.unwrap();
            cache_closed.unwrap();
        }
    }
}

// These are bounded local mechanics observations. There is deliberately no
// GET-count or latency equality assertion between legacy and tokenized paths.
#[tokio::test]
async fn scan_four_concurrent_memory_cold_and_warm_cost() {
    scan_cost(false).await;
}

#[tokio::test]
async fn scan_four_concurrent_hybrid_cold_and_warm_cost() {
    scan_cost(true).await;
}
