use super::*;
use crate::db_state::SsTableId;
use crate::sst_iter::tests::{held_scan_prefetch, scan_fixture_parts, settle_scan_fixture_tasks};
use std::time::Duration;

// Reuse the original SST fixture and its actual prefetch task identities.
// The held cache callback models a lookup stall, not a physical disk stall.
#[rstest::rstest]
#[tokio::test(flavor = "current_thread")]
async fn scan_sorted_run_seek_joins_before_opening_next_sst(
    #[values(false, true)] cancel_parent: bool,
) {
    sorted_run_boundary(true, cancel_parent).await;
}

#[tokio::test(flavor = "current_thread")]
async fn scan_sorted_run_close_joins_without_opening_next_sst() {
    sorted_run_boundary(false, false).await;
}

async fn sorted_run_boundary(seek: bool, cancel_parent: bool) {
    let (current, cache, parent, originals) = held_scan_prefetch().await;
    let (table_store, options) = scan_fixture_parts(&current);
    let mut writer =
        table_store.table_writer(SsTableId::from(ulid::Ulid::new()), Some(Bytes::new()));
    for key in [b"k100", b"k101", b"k102"] {
        writer
            .add(RowEntry::new_value(key, b"replacement", 1))
            .await
            .unwrap();
    }
    let next = SsTableView::identity(writer.close().await.unwrap().0);
    let mut iter = SortedRunIterator {
        table_store,
        sst_iter_options: options,
        sst_tracing_context: None,
        db_stats: None,
        view: SortedRunView::Owned(VecDeque::from([next]), BytesRange::from(..)),
        current_iter: Some(current),
        descending_state: None,
        initialized: true,
    };
    let index_before = cache.counts().2;
    if cancel_parent {
        parent.cancel();
    }
    let mut call = Box::pin(async {
        if seek {
            iter.seek(b"k101").await
        } else {
            iter.close().await
        }
    });
    // The first poll must reach the original held owner. No time-only
    // success path can pass because the callback is already observed live.
    let first = futures::poll!(call.as_mut());
    let returned_early = first.is_ready();
    let held = cache.counts();
    cache.release();
    let result = match first {
        std::task::Poll::Ready(result) => Ok(result),
        std::task::Poll::Pending => {
            tokio::time::timeout(Duration::from_secs(2), call.as_mut()).await
        }
    };
    drop(call);
    settle_scan_fixture_tasks(&originals).await;
    let mut rows = Vec::new();
    if matches!(&result, Ok(Ok(()))) && seek && !cancel_parent {
        let drained = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(row) = iter.next().await? {
                rows.push((row.key, row.value));
            }
            Ok::<_, SlateDBError>(())
        })
        .await;
        let closed = iter.close().await;
        drained.unwrap().unwrap();
        closed.unwrap();
    } else {
        iter.close().await.unwrap();
    }
    assert!(
        !returned_early,
        "replacement/Close dropped the original prefetch"
    );
    assert_eq!(held.0, 1);
    assert_eq!(held.1, 0, "original lookup was not held at the boundary");
    assert_eq!(
        held.2, index_before,
        "replacement SST opened before the join"
    );
    assert_eq!(cache.counts().1, 1);
    if cancel_parent {
        assert!(matches!(result.unwrap(), Err(SlateDBError::Cancelled)));
        assert_eq!(
            cache.counts().2,
            index_before,
            "cancelled Seek opened a replacement"
        );
    } else {
        result.unwrap().unwrap();
        assert!(!parent.is_cancelled(), "iterator canceled the parent");
        if seek {
            assert_eq!(
                rows,
                [b"k101", b"k102"].map(|key| {
                    (
                        Bytes::copy_from_slice(key),
                        crate::types::ValueDeletable::Value(Bytes::from_static(b"replacement")),
                    )
                })
            );
            assert!(
                cache.counts().2 > index_before,
                "positive Seek never opened the next SST"
            );
        } else {
            assert!(rows.is_empty());
            assert_eq!(cache.counts().2, index_before);
        }
    }
}
