use async_trait::async_trait;
use bytes::Bytes;
use futures::future::join;
use futures::StreamExt;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::bytes_range::BytesRange;
use crate::db_iter::GetIterator;
use crate::db_state::{SortedRun, SsTableView};
use crate::db_stats::DbStats;
use crate::error::SlateDBError;
use crate::iter::{EmptyIterator, IterationOrder, RowEntryIterator};
use crate::manifest::{LsmTreeState, Segment};
use crate::merge_iterator::MergeIterator;
use crate::reader::{ReadTrace, SstTraceLevel};
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::{SstIterator, SstIteratorOptions, SstTracingContext};
use crate::tablestore::TableStore;
use crate::types::RowEntry;

/// Inputs needed to construct a per-segment iterator. The scan path's
/// [`SegmentMergeIterator`] holds an unbound template and clones it with the
/// selected segment in [`Self::for_segment`] before building each child. The
/// get path binds its single selected segment before constructing its L0 and
/// sorted-run point iterators.
#[derive(Clone)]
pub(crate) struct SegmentScanContext {
    pub(crate) table_store: Arc<TableStore>,
    pub(crate) range: BytesRange,
    pub(crate) sst_iter_options: SstIteratorOptions,
    pub(crate) max_parallel: usize,
    /// Stats for point-query SSTs (optional — propagated from
    /// `ReadOptions`).
    pub(crate) point_lookup_stats: Option<DbStats>,
    /// Stats for range-query sorted runs.
    pub(crate) db_stats: DbStats,
    pub(crate) read_trace: ReadTrace,
}

impl SegmentScanContext {
    /// Returns a context whose SST options carry the selected segment.
    fn for_segment(&self, segment: &Bytes) -> Self {
        let mut context = self.clone();
        context.sst_iter_options.segment = Some(segment.clone());
        context
    }
}

/// Per-segment iterator bundle built from a single [`LsmTreeState`] for
/// a range scan. Holds L0 and sorted-run iterators as separate arms so
/// the per-segment merge can preserve key-by-key ordering across both
/// tiers. Point lookups skip this entirely and build their own flat
/// chain via [`crate::db_iter::GetIterator::from_lsm_tree`].
struct RangeTreeIterators {
    l0: VecDeque<Box<dyn RowEntryIterator>>,
    sr: VecDeque<Box<dyn RowEntryIterator>>,
}

impl RangeTreeIterators {
    async fn build(tree: &LsmTreeState, ctx: &SegmentScanContext) -> Result<Self, SlateDBError> {
        let mut ctx = ctx.clone();
        ctx.sst_iter_options.cancellation_token = ctx
            .sst_iter_options
            .cancellation_token
            .as_ref()
            .map(tokio_util::sync::CancellationToken::child_token);
        // Retire the original construction as a whole, including token-free
        // reads. This flag never enters the cache path or cancels a caller.
        let retiring = Arc::new(AtomicBool::new(false));
        // Range scans need both L0 and SR iterators, so build them in parallel
        let (l0, sr) = join(
            build_l0_range_iters(&tree.l0, &ctx, retiring.clone()),
            build_sr_range_iters(&tree.compacted, &ctx, retiring),
        )
        .await;
        match (l0, sr) {
            (Ok(l0), Ok(sr)) => Ok(Self { l0, sr }),
            (Err(error), Ok(mut sr)) | (Ok(mut sr), Err(error)) => {
                let mut error = error;
                for iter in &mut sr {
                    if let Err(cleanup) = iter.close().await {
                        if matches!(error, SlateDBError::Cancelled) {
                            error = cleanup;
                        }
                    }
                }
                Err(error)
            }
            (Err(l0), Err(sr)) => Err(if matches!(l0, SlateDBError::Cancelled) {
                sr
            } else {
                l0
            }),
        }
    }
}

/// One child of [`SegmentMergeIterator`]. `Pending` stores the
/// segment's LSM state directly; promotion (atomically: build + init +
/// seek-to-`pending_seek`) transitions it to `Built`. `Built` therefore
/// always means "ready to serve `next` / `seek` immediately" — the
/// variant alone encodes readiness, no separate flag required.
///
/// Internal to this module — production callers pass `&[Segment]` to
/// [`SegmentMergeIterator::new`] and the iterator wraps each segment in
/// `Pending`. Test fixtures that bypass the build path inject already-
/// initialized iterators wrapped as `Built` via direct field construction.
enum SegmentIterState {
    Pending(Arc<LsmTreeState>),
    Built(Box<dyn RowEntryIterator>),
}

/// Iterator that walks a sequence of per-segment children in
/// iteration order. Segments partition the key space (RFC-0024
/// antichain invariant), so a key-by-key merge across them is a no-op
/// — chaining is sufficient and avoids the heap inflation a global
/// merge would incur.
///
/// Each child is paired with the prefix of the segment it covers.
/// Prefixes drive two things:
///
/// 1. **Lazy promotion.** Children start as `Pending(LsmTreeState)`;
///    only the front child is built. The next is built on demand when
///    the current exhausts, so SST opens for unreached segments never
///    happen.
/// 2. **Binary-search seek.** A `seek(key)` discards every child
///    whose interval is entirely on the already-emitted side of `key`
///    in `O(log M)` and then seeks inside the matching child — far
///    cheaper than walking children one at a time when a scan jumps
///    far ahead.
///
/// Children are stored in iteration order: prefix-ascending for
/// `Ascending`, prefix-descending for `Descending`. `next` always pops
/// from the front, so the chain logic itself is order-agnostic — only
/// the construction order and the [`Self::count_before`] predicate
/// differ.
///
/// The unsegmented mode is treated as a singleton segment with empty
/// prefix. Empty is a prefix of every byte string, so the binary-
/// search code uniformly yields that single child for every query —
/// no special case in the iterator.
///
/// Build logic is inline (per-segment SST opens + per-tree merge
/// construction) rather than callback-driven, so the iterator owns the
/// segment metadata directly and the call site just hands it specs.
///
/// Range-scan only — point lookups bypass this iterator and use
/// [`crate::db_iter::GetIterator::from_lsm_tree`] directly. Construction
/// goes through the [`build_segment_iter`] helper, which dispatches
/// between the two.
struct SegmentMergeIterator {
    children: VecDeque<(Bytes, SegmentIterState)>,
    /// Shared inputs for building `Pending` children. `None` is
    /// permitted only when every child is already `Built` (test
    /// configurations and degenerate empty chains).
    context: Option<SegmentScanContext>,
    /// Iteration order. Children are stored in this order, and
    /// `count_before` flips its predicate accordingly.
    order: IterationOrder,
    /// Most recent seek target, applied to each child on promotion so
    /// a seek into a future segment is honored when iteration reaches
    /// it.
    pending_seek: Option<Bytes>,
    initialized: bool,
}

impl SegmentMergeIterator {
    /// Build a chain over `segments`. Each segment becomes a `Pending`
    /// child wrapping its `LsmTreeState`; promotion is lazy so segments
    /// the query never reaches incur no SST opens. `segments` must be
    /// in prefix-ascending order (the antichain invariant from the
    /// manifest is preserved by `select_trees`); the constructor
    /// reverses them when `order` is `Descending` so the front of the
    /// deque is always the next segment to emit.
    fn new(segments: &[Segment], context: SegmentScanContext) -> Self {
        let order = context.sst_iter_options.order;
        let mut children: VecDeque<(Bytes, SegmentIterState)> = segments
            .iter()
            .map(|s| (s.prefix.clone(), SegmentIterState::Pending(s.tree.clone())))
            .collect();
        if matches!(order, IterationOrder::Descending) {
            children = children.into_iter().rev().collect();
        }
        Self {
            children,
            context: Some(context),
            order,
            pending_seek: None,
            initialized: false,
        }
    }

    /// Number of leading children whose intervals are entirely on the
    /// already-emitted side of `key` and can be discarded.
    ///
    /// **Ascending.** Children are prefix-ascending; we drop those
    /// whose interval `[prefix, prefix++)` ends `<= key`. Equivalent
    /// to "prefix < key and key does not extend prefix" — the
    /// extension special-case keeps the unique segment containing
    /// `key` (antichain invariant: at most one prefix is itself a
    /// prefix of `key`, and it is the largest prefix `<= key`).
    ///
    /// **Descending.** Children are prefix-descending; we drop those
    /// whose interval `[prefix, prefix++)` starts `> key`. No
    /// extension special-case is needed: if `prefix` is a prefix of
    /// `key` then `prefix <= key` lexicographically, so the predicate
    /// already keeps that child.
    fn count_before(&self, key: &[u8]) -> usize {
        match self.order {
            IterationOrder::Ascending => {
                let after = self.children.partition_point(|(p, _)| p.as_ref() <= key);
                if after > 0 && key.starts_with(self.children[after - 1].0.as_ref()) {
                    after - 1
                } else {
                    after
                }
            }
            IterationOrder::Descending => self.children.partition_point(|(p, _)| p.as_ref() > key),
        }
    }

    /// Returns the front child as a mutable iterator handle, building
    /// it first if it's still `Pending`. The transition is atomic
    /// (build + init + seek-to-`pending_seek`), so the returned handle
    /// is always ready to serve `next`/`seek` immediately. Returns
    /// `None` only when the chain is exhausted.
    async fn ensure_front_built(
        &mut self,
    ) -> Result<Option<&mut Box<dyn RowEntryIterator>>, SlateDBError> {
        if matches!(
            self.children.front(),
            Some((_, SegmentIterState::Pending(_)))
        ) {
            if self.context.as_ref().is_some_and(|context| {
                context
                    .sst_iter_options
                    .cancellation_token
                    .as_ref()
                    .is_some_and(|token| token.is_cancelled())
            }) {
                return Err(SlateDBError::Cancelled);
            }
            let (prefix, lazy) = self
                .children
                .pop_front()
                .expect("front exists per match above");
            let SegmentIterState::Pending(tree) = lazy else {
                unreachable!("matched Pending above");
            };
            let context = self
                .context
                .as_ref()
                .expect("Pending children require a SegmentScanContext");
            let context = context.for_segment(&prefix);
            let RangeTreeIterators { l0, sr } = RangeTreeIterators::build(&tree, &context).await?;
            let iters: VecDeque<Box<dyn RowEntryIterator>> = l0.into_iter().chain(sr).collect();
            let merge = MergeIterator::new_with_order(iters, context.sst_iter_options.order)?;
            // Per-segment merge runs with dedup disabled so the outer
            // `max_seq` filter can drop out-of-window entries before any
            // dedup decision is made (see comments in `db_iter::DbIterator::new`).
            let mut child: Box<dyn RowEntryIterator> = Box::new(merge.with_dedup(false));
            if let Err(error) = child.init().await {
                let cleanup = child.close().await;
                return Err(if matches!(error, SlateDBError::Cancelled) {
                    cleanup.err().unwrap_or(error)
                } else {
                    error
                });
            }
            if let Some(seek_key) = self.pending_seek.as_ref() {
                if let Err(error) = child.seek(seek_key).await {
                    let cleanup = child.close().await;
                    return Err(if matches!(error, SlateDBError::Cancelled) {
                        cleanup.err().unwrap_or(error)
                    } else {
                        error
                    });
                }
            }
            self.children
                .push_front((prefix, SegmentIterState::Built(child)));
        }
        Ok(self.children.front_mut().map(|(_, lazy)| match lazy {
            SegmentIterState::Built(child) => child,
            SegmentIterState::Pending(_) => unreachable!("just transitioned to Built"),
        }))
    }
}

#[async_trait]
impl RowEntryIterator for SegmentMergeIterator {
    async fn close(&mut self) -> Result<(), SlateDBError> {
        let mut failure = None;
        while let Some((_, child)) = self.children.front_mut() {
            if let SegmentIterState::Built(child) = child {
                if let Err(error) = child.close().await {
                    if failure.is_none() && !matches!(error, SlateDBError::Cancelled) {
                        failure = Some(error);
                    }
                }
            }
            self.children.pop_front();
        }
        self.initialized = true;
        failure.map_or(Ok(()), Err)
    }

    async fn init(&mut self) -> Result<(), SlateDBError> {
        // Setting the flag is the only thing `init` needs to do. The
        // first child is built lazily on the first `next`/`seek`.
        self.initialized = true;
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if !self.initialized {
            return Err(SlateDBError::IteratorNotInitialized);
        }
        loop {
            let Some(child) = self.ensure_front_built().await? else {
                return Ok(None);
            };
            if let Some(entry) = child.next().await? {
                return Ok(Some(entry));
            }
            child.close().await?;
            self.children.pop_front();
        }
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        if !self.initialized {
            return Err(SlateDBError::IteratorNotInitialized);
        }
        self.pending_seek = Some(Bytes::copy_from_slice(next_key));
        let drop_count = self.count_before(next_key);
        for _ in 0..drop_count {
            if let Some((_, SegmentIterState::Built(child))) = self.children.front_mut() {
                child.close().await?;
            }
            self.children.pop_front();
        }
        // If the new front is `Pending`, the build path applies
        // `pending_seek` (= `next_key`) atomically. If it's already
        // `Built`, we apply the seek explicitly after building.
        let was_pending = matches!(
            self.children.front(),
            Some((_, SegmentIterState::Pending(_)))
        );
        if let Some(child) = self.ensure_front_built().await? {
            if !was_pending {
                child.seek(next_key).await?;
            }
        }
        Ok(())
    }
}

/// Build the on-disk segment iterator for a query, dispatching on
/// whether `context.range` is a point lookup or a range scan.
///
/// **Point lookups** select at most one segment (antichain invariant on
/// segment prefixes). The matching segment's L0 + sorted runs are
/// walked as a flat lazy chain via
/// [`crate::db_iter::GetIterator::from_lsm_tree`], so a bloom-positive
/// hit in the newest L0 SST returns without opening any older SSTs.
/// When no segment matches, an [`EmptyIterator`] stands in.
///
/// **Range scans** thread the segments into [`SegmentMergeIterator`],
/// which lazily promotes each segment as the scan reaches it.
pub(crate) fn build_segment_iter(
    segments: &[Segment],
    context: SegmentScanContext,
    max_seq: Option<u64>,
) -> Result<Box<dyn RowEntryIterator>, SlateDBError> {
    if let Some(point_key) = context.range.as_point() {
        match segments.last() {
            Some(segment) => {
                let context = context.for_segment(&segment.prefix);
                Ok(Box::new(GetIterator::from_lsm_tree(
                    point_key.clone(),
                    &segment.tree,
                    &context,
                    max_seq,
                )?))
            }
            None => Ok(Box::new(EmptyIterator::new())),
        }
    } else {
        Ok(Box::new(SegmentMergeIterator::new(segments, context)))
    }
}

pub(crate) fn build_l0_point_iters(
    l0: &VecDeque<SsTableView>,
    ctx: &SegmentScanContext,
) -> Result<VecDeque<Box<dyn RowEntryIterator>>, SlateDBError> {
    let mut iters = VecDeque::new();
    let options = ctx.sst_iter_options.clone();
    for sst in l0.iter().cloned() {
        let iter = SstIterator::new_owned_with_stats(
            ctx.range.clone(),
            sst,
            ctx.table_store.clone(),
            options.clone(),
            Some(SstTracingContext::new(
                SstTraceLevel::L0,
                ctx.read_trace.clone(),
            )),
            ctx.point_lookup_stats.clone(),
        )?;
        if let Some(iter) = iter {
            iters.push_back(Box::new(iter) as Box<dyn RowEntryIterator>);
        }
    }
    Ok(iters)
}

pub(crate) fn build_sr_point_iters(
    key: &Bytes,
    compacted: &[SortedRun],
    ctx: &SegmentScanContext,
) -> Result<VecDeque<Box<dyn RowEntryIterator>>, SlateDBError> {
    let mut iters = VecDeque::new();
    let options = ctx.sst_iter_options.clone();
    for sr in compacted.iter() {
        for handle in sr.tables_covering_point_key(key.as_ref()) {
            let iter = SstIterator::new_owned_with_stats(
                ctx.range.clone(),
                handle.clone(),
                ctx.table_store.clone(),
                options.clone(),
                Some(SstTracingContext::new(
                    SstTraceLevel::SortedRun(sr.id),
                    ctx.read_trace.clone(),
                )),
                ctx.point_lookup_stats.clone(),
            )?;
            if let Some(iter) = iter {
                iters.push_back(Box::new(iter) as Box<dyn RowEntryIterator>);
            }
        }
    }
    Ok(iters)
}

async fn build_l0_range_iters(
    l0: &VecDeque<SsTableView>,
    ctx: &SegmentScanContext,
    retiring: Arc<AtomicBool>,
) -> Result<VecDeque<Box<dyn RowEntryIterator>>, SlateDBError> {
    let table_store = ctx.table_store.clone();
    let range = ctx.range.clone();
    let opts = ctx.sst_iter_options.clone();
    let sst_tracing_context = Some(SstTracingContext::new(
        SstTraceLevel::L0,
        ctx.read_trace.clone(),
    ));
    let stats = ctx.db_stats.clone();
    build_range_iters(
        l0.iter().cloned(),
        ctx.max_parallel,
        ctx.sst_iter_options.cancellation_token.clone(),
        retiring,
        move |sst| {
            let table_store = table_store.clone();
            let range = range.clone();
            let opts = opts.clone();
            let sst_tracing_context = sst_tracing_context.clone();
            let stats = stats.clone();
            async move {
                SstIterator::new_owned_initialized_with_stats(
                    range,
                    sst,
                    table_store,
                    opts,
                    sst_tracing_context,
                    Some(stats),
                )
                .await
                .map(|maybe| maybe.map(|i| Box::new(i) as Box<dyn RowEntryIterator>))
            }
        },
    )
    .await
}

async fn build_sr_range_iters(
    compacted: &[SortedRun],
    ctx: &SegmentScanContext,
    retiring: Arc<AtomicBool>,
) -> Result<VecDeque<Box<dyn RowEntryIterator>>, SlateDBError> {
    let range = ctx.range.clone();
    let overlapping: Vec<_> = compacted
        .iter()
        .filter(|sr| sr.overlaps_range(&range))
        .cloned()
        .collect();
    let table_store = ctx.table_store.clone();
    let opts = ctx.sst_iter_options.clone();
    let stats = ctx.db_stats.clone();
    let read_trace = ctx.read_trace.clone();
    build_range_iters(
        overlapping.into_iter(),
        ctx.max_parallel,
        ctx.sst_iter_options.cancellation_token.clone(),
        retiring,
        move |sr| {
            let table_store = table_store.clone();
            let range = range.clone();
            let opts = opts.clone();
            let sst_tracing_context = Some(SstTracingContext::new(
                SstTraceLevel::SortedRun(sr.id),
                read_trace.clone(),
            ));
            let stats = stats.clone();
            async move {
                SortedRunIterator::new_owned_initialized_with_stats(
                    range,
                    sr,
                    table_store,
                    opts,
                    sst_tracing_context,
                    Some(stats),
                )
                .await
                .map(|iter| Some(Box::new(iter) as Box<dyn RowEntryIterator>))
            }
        },
    )
    .await
}

async fn build_range_iters<I, F, Fut>(
    inputs: I,
    max_parallel: usize,
    cancellation_token: Option<tokio_util::sync::CancellationToken>,
    retiring: Arc<AtomicBool>,
    build: F,
) -> Result<VecDeque<Box<dyn RowEntryIterator>>, SlateDBError>
where
    I: IntoIterator,
    I::Item: Send,
    F: Fn(I::Item) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Option<Box<dyn RowEntryIterator>>, SlateDBError>> + Send,
{
    let build = &build;
    let mut results =
        futures::stream::iter(inputs.into_iter().enumerate().map(move |(index, input)| {
            let cancellation_token = cancellation_token.clone();
            let retiring = retiring.clone();
            async move {
                if retiring.load(Ordering::Acquire)
                    || cancellation_token
                        .as_ref()
                        .is_some_and(|token| token.is_cancelled())
                {
                    return (index, Err(SlateDBError::Cancelled));
                }
                let result = build(input).await;
                if result.is_err() {
                    retiring.store(true, Ordering::Release);
                    if let Some(token) = cancellation_token {
                        token.cancel();
                    }
                }
                (index, result)
            }
        }))
        .buffer_unordered(max_parallel.max(1))
        .collect::<Vec<_>>()
        .await;
    let failure = results
        .iter()
        .filter_map(|(index, result)| result.as_ref().err().map(|error| (*index, error)))
        .min_by_key(|(index, error)| (matches!(error, SlateDBError::Cancelled), *index))
        .map(|(_, error)| error.clone());
    if let Some(mut error) = failure {
        results.sort_by_key(|(index, _)| *index);
        for (_, result) in &mut results {
            if let Ok(Some(iter)) = result {
                if let Err(cleanup) = iter.close().await {
                    if matches!(error, SlateDBError::Cancelled) {
                        error = cleanup;
                    }
                }
            }
        }
        Err(error)
    } else {
        // Preserve the existing successful factory completion order.
        Ok(results
            .into_iter()
            .filter_map(|(_, result)| result.ok().flatten())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_cache::DbCache;
    use crate::types::{RowEntry, ValueDeletable};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Default)]
    struct RangeBuildProbe {
        started: parking_lot::Mutex<Vec<usize>>,
        held: AtomicBool,
        failed: AtomicBool,
        released: AtomicBool,
        joined: AtomicUsize,
        closed: AtomicUsize,
        changed: tokio::sync::Notify,
    }

    impl RangeBuildProbe {
        async fn wait(&self, flag: &AtomicBool) {
            loop {
                let changed = self.changed.notified();
                if flag.load(Ordering::SeqCst) {
                    return;
                }
                changed.await;
            }
        }

        fn release(&self) {
            self.released.store(true, Ordering::SeqCst);
            self.changed.notify_waiters();
        }
    }

    struct BuiltRangeProbe {
        record: Arc<RangeBuildProbe>,
        entry: Option<RowEntry>,
    }

    #[async_trait]
    impl RowEntryIterator for BuiltRangeProbe {
        async fn init(&mut self) -> Result<(), SlateDBError> {
            Ok(())
        }

        async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
            Ok(self.entry.take())
        }

        async fn seek(&mut self, _: &[u8]) -> Result<(), SlateDBError> {
            Ok(())
        }

        async fn close(&mut self) -> Result<(), SlateDBError> {
            self.record.closed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn scan_token_free_factory_error_does_not_start_queued_factory() {
        let record = Arc::new(RangeBuildProbe::default());
        let result = build_range_iters(0..3, 1, None, Arc::new(AtomicBool::new(false)), |id| {
            record.started.lock().push(id);
            let record = record.clone();
            async move {
                if id == 0 {
                    Err(SlateDBError::ChecksumMismatch { path: None })
                } else {
                    Ok(Some(Box::new(BuiltRangeProbe {
                        record,
                        entry: None,
                    }) as Box<dyn RowEntryIterator>))
                }
            }
        })
        .await;
        assert!(matches!(result, Err(SlateDBError::ChecksumMismatch { .. })));
        assert_eq!(*record.started.lock(), vec![0]);
        assert_eq!(record.closed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn scan_token_free_factory_error_joins_started_without_new_factory() {
        let record = Arc::new(RangeBuildProbe::default());
        let original_record = record.clone();
        let mut original = tokio::spawn(async move {
            build_range_iters(0..3, 2, None, Arc::new(AtomicBool::new(false)), |id| {
                original_record.started.lock().push(id);
                let record = original_record.clone();
                async move {
                    if id == 0 {
                        record.wait(&record.held).await;
                        record.failed.store(true, Ordering::SeqCst);
                        record.changed.notify_waiters();
                        return Err(SlateDBError::ChecksumMismatch { path: None });
                    }
                    if id == 1 {
                        record.held.store(true, Ordering::SeqCst);
                        record.changed.notify_waiters();
                        record.wait(&record.released).await;
                        record.joined.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(Some(Box::new(BuiltRangeProbe {
                        record,
                        entry: None,
                    }) as Box<dyn RowEntryIterator>))
                }
            })
            .await
        });
        let started =
            tokio::time::timeout(Duration::from_secs(2), record.wait(&record.failed)).await;
        let early = tokio::time::timeout(Duration::from_millis(25), &mut original).await;
        record.release();
        let (returned_early, result) = match early {
            Ok(result) => (true, result),
            Err(_) => match tokio::time::timeout(Duration::from_secs(2), &mut original).await {
                Ok(result) => (false, result),
                Err(_) => {
                    original.abort();
                    let _ = original.await;
                    panic!("original factory did not join after releasing its held work");
                }
            },
        };
        assert!(
            started.is_ok(),
            "original held factory and failure were not observed"
        );
        assert!(
            !returned_early,
            "construction returned while its original factory was held"
        );
        assert!(matches!(
            result.unwrap(),
            Err(SlateDBError::ChecksumMismatch { .. })
        ));
        assert_eq!(*record.started.lock(), vec![0, 1]);
        assert_eq!(record.joined.load(Ordering::SeqCst), 1);
        assert_eq!(record.closed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn scan_token_free_factory_success_keeps_unordered_completion() {
        let record = Arc::new(RangeBuildProbe::default());
        let mut build = Box::pin(build_range_iters(
            0..2,
            2,
            None,
            Arc::new(AtomicBool::new(false)),
            |id| {
                record.started.lock().push(id);
                let record = record.clone();
                async move {
                    if id == 0 {
                        record.wait(&record.released).await;
                    }
                    Ok(Some(Box::new(BuiltRangeProbe {
                        record,
                        entry: Some(RowEntry::new_value(b"key", &[id as u8], 1)),
                    }) as Box<dyn RowEntryIterator>))
                }
            },
        ));
        assert!(futures::poll!(build.as_mut()).is_pending());
        let started = record.started.lock().clone();
        record.release();
        let mut iters = tokio::time::timeout(Duration::from_secs(2), build)
            .await
            .unwrap()
            .unwrap();
        let mut order = Vec::new();
        for iter in &mut iters {
            order.push(
                iter.next()
                    .await
                    .unwrap()
                    .unwrap()
                    .value
                    .as_bytes()
                    .unwrap()[0],
            );
            iter.close().await.unwrap();
        }
        assert_eq!(
            started,
            vec![0, 1],
            "successful construction must remain concurrent"
        );
        assert_eq!(order, vec![1, 0], "successful completion order changed");
        assert_eq!(record.closed.load(Ordering::SeqCst), 2);
    }

    // This is an actual TableStore/cache construction boundary, not a provider
    // failure or a physical disk stall. One original index callback is held
    // while the other arm reaches a deliberately missing in-memory SST.
    struct RangeBuildCache {
        inner: crate::db_cache::test_utils::TestCache,
        record: Arc<RangeBuildProbe>,
        failure: crate::db_state::SsTableId,
        held: crate::db_state::SsTableId,
        unopened: Vec<crate::db_state::SsTableId>,
        unwanted: AtomicUsize,
        point_cleanup: bool,
    }

    #[async_trait]
    impl crate::db_cache::DbCache for RangeBuildCache {
        async fn get_block(
            &self,
            key: &crate::db_cache::CachedKey,
        ) -> Result<Option<crate::db_cache::CachedEntry>, crate::Error> {
            self.inner.get_block(key).await
        }
        async fn get_index(
            &self,
            key: &crate::db_cache::CachedKey,
        ) -> Result<Option<crate::db_cache::CachedEntry>, crate::Error> {
            if self.unopened.contains(&key.sst_id) {
                self.unwanted.fetch_add(1, Ordering::SeqCst);
            }
            if self.point_cleanup {
                if key.sst_id == self.held {
                    self.record.wait(&self.record.held).await;
                }
                if key.sst_id == self.failure {
                    self.record.held.store(true, Ordering::SeqCst);
                    self.record.changed.notify_waiters();
                    self.record.wait(&self.record.released).await;
                    self.record.joined.fetch_add(1, Ordering::SeqCst);
                    return Err(crate::Error::unavailable("use missing original SST".into()));
                }
                return self.inner.get_index(key).await;
            }
            if key.sst_id == self.failure {
                self.record.wait(&self.record.held).await;
                self.record.failed.store(true, Ordering::SeqCst);
                self.record.changed.notify_waiters();
                return Err(crate::Error::unavailable(
                    "original range index error".into(),
                ));
            }
            if key.sst_id == self.held {
                self.record.held.store(true, Ordering::SeqCst);
                self.record.changed.notify_waiters();
                self.record.wait(&self.record.released).await;
                self.record.joined.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get_index(key).await
        }
        async fn get_filter(
            &self,
            key: &crate::db_cache::CachedKey,
        ) -> Result<Option<crate::db_cache::CachedEntry>, crate::Error> {
            self.inner.get_filter(key).await
        }
        async fn get_stats(
            &self,
            key: &crate::db_cache::CachedKey,
        ) -> Result<Option<crate::db_cache::CachedEntry>, crate::Error> {
            self.inner.get_stats(key).await
        }
        async fn insert(
            &self,
            key: crate::db_cache::CachedKey,
            value: crate::db_cache::CachedEntry,
        ) {
            self.inner.insert(key, value).await;
        }
        async fn remove(&self, key: &crate::db_cache::CachedKey) {
            self.inner.remove(key).await;
        }
        fn entry_count(&self) -> u64 {
            self.inner.entry_count()
        }
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn scan_token_free_range_failure_retires_both_original_arms(
        #[values(false, true)] failure_in_l0: bool,
    ) {
        use crate::block_cache_policy::BlockCachePolicy;
        use crate::db_state::SsTableId;
        use crate::format::sst::SsTableFormat;
        use crate::tablestore::TableStoreKind;
        use slatedb_common::metrics::{DefaultMetricsRecorder, MetricLevel, MetricsRecorderHelper};
        let ids: Vec<_> = (1..=4)
            .map(|id| SsTableId::from(ulid::Ulid::from_parts(id, 0)))
            .collect();
        let record = Arc::new(RangeBuildProbe::default());
        let cache = Arc::new(RangeBuildCache {
            inner: crate::db_cache::test_utils::TestCache::new(),
            record: record.clone(),
            failure: ids[if failure_in_l0 { 0 } else { 2 }].clone(),
            held: ids[if failure_in_l0 { 2 } else { 0 }].clone(),
            unopened: vec![ids[1].clone(), ids[3].clone()],
            unwanted: AtomicUsize::new(0),
            point_cleanup: false,
        });
        let table_store = Arc::new(TableStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            SsTableFormat::default(),
            object_store::path::Path::from("scan-construction-arms"),
            Some(cache.clone()),
            TableStoreKind::Main,
            BlockCachePolicy::default(),
        ));
        let mut tables = Vec::new();
        for id in ids {
            let mut writer = table_store.table_writer(id, Some(Bytes::new()));
            writer
                .add(RowEntry::new_value(b"key", b"original", 1))
                .await
                .unwrap();
            tables.push(SsTableView::identity(writer.close().await.unwrap().0));
        }
        // The production cache-error fallback remains unchanged. Its next
        // real object read sees this missing fixture SST, without transport retries.
        table_store.delete_sst(&cache.failure).await.unwrap();
        let tree = LsmTreeState {
            l0: tables[..2].iter().cloned().collect(),
            compacted: vec![
                SortedRun::new(1, [tables[2].clone()]),
                SortedRun::new(2, [tables[3].clone()]),
            ],
            ..LsmTreeState::default()
        };
        let helper = MetricsRecorderHelper::new(
            Arc::new(DefaultMetricsRecorder::new()),
            MetricLevel::default(),
        );
        let context = SegmentScanContext {
            table_store,
            range: BytesRange::from(..),
            max_parallel: 1,
            sst_iter_options: SstIteratorOptions {
                segment: Some(Bytes::new()),
                ..SstIteratorOptions::default()
            },
            point_lookup_stats: None,
            db_stats: DbStats::new(&helper),
            read_trace: ReadTrace::new(None),
        };
        let mut original =
            tokio::spawn(async move { RangeTreeIterators::build(&tree, &context).await });
        let started =
            tokio::time::timeout(Duration::from_secs(2), record.wait(&record.failed)).await;
        let early = tokio::time::timeout(Duration::from_millis(25), &mut original).await;
        cache.record.release();
        let (returned_early, result) = match early {
            Ok(result) => (true, result),
            Err(_) => match tokio::time::timeout(Duration::from_secs(2), &mut original).await {
                Ok(result) => (false, result),
                Err(_) => {
                    original.abort();
                    let _ = original.await;
                    panic!("original range construction did not join");
                }
            },
        };
        let error = match result.unwrap() {
            Err(error) => error,
            Ok(mut iters) => {
                for iter in iters.l0.iter_mut().chain(&mut iters.sr) {
                    iter.close().await.unwrap();
                }
                panic!("range construction lost original cache error");
            }
        };
        assert!(
            started.is_ok(),
            "did not witness both original construction arms"
        );
        assert!(!returned_early, "failed arm dropped the held original arm");
        assert!(
            matches!(error, SlateDBError::ObjectStoreError(ref error)
            if matches!(error.as_ref(), object_store::Error::NotFound { .. })),
            "{error:?}"
        );
        assert_eq!(record.joined.load(Ordering::SeqCst), 1);
        assert_eq!(
            cache.unwanted.load(Ordering::SeqCst),
            0,
            "failure admitted a queued table on one construction arm"
        );
    }

    #[tokio::test]
    async fn scan_exact_key_returns_joined_unused_storage_error() {
        use crate::block_cache_policy::BlockCachePolicy;
        use crate::db_state::SsTableId;
        use crate::format::sst::SsTableFormat;
        use crate::tablestore::TableStoreKind;
        use slatedb_common::metrics::{DefaultMetricsRecorder, MetricLevel, MetricsRecorderHelper};
        let ids: Vec<_> = (1..=3)
            .map(|id| SsTableId::from(ulid::Ulid::from_parts(id, 0)))
            .collect();
        let record = Arc::new(RangeBuildProbe::default());
        let cache = Arc::new(RangeBuildCache {
            inner: crate::db_cache::test_utils::TestCache::new(),
            record: record.clone(),
            failure: ids[2].clone(),
            held: ids[1].clone(),
            unopened: Vec::new(),
            unwanted: AtomicUsize::new(0),
            point_cleanup: true,
        });
        let table_store = Arc::new(TableStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            SsTableFormat::default(),
            object_store::path::Path::from("scan-exact-key-cleanup"),
            Some(cache.clone()),
            TableStoreKind::Main,
            BlockCachePolicy::default(),
        ));
        let mut tables = VecDeque::new();
        for (id, seq) in ids.into_iter().zip([10, 4, 1]) {
            let mut writer = table_store.table_writer(id, Some(Bytes::new()));
            writer
                .add(RowEntry::new_value(b"key", b"original", seq))
                .await
                .unwrap();
            tables.push_back(SsTableView::identity(writer.close().await.unwrap().0));
        }
        table_store.delete_sst(&cache.failure).await.unwrap();
        let tree = LsmTreeState {
            l0: tables,
            ..LsmTreeState::default()
        };
        let helper = MetricsRecorderHelper::new(
            Arc::new(DefaultMetricsRecorder::new()),
            MetricLevel::default(),
        );
        let range = BytesRange::from(Bytes::from_static(b"key")..=Bytes::from_static(b"key"));
        let context = SegmentScanContext {
            table_store,
            range: range.clone(),
            max_parallel: 2,
            sst_iter_options: SstIteratorOptions {
                segment: Some(Bytes::new()),
                inline_point_read: false,
                ..SstIteratorOptions::default()
            },
            point_lookup_stats: None,
            db_stats: DbStats::new(&helper),
            read_trace: ReadTrace::new(None),
        };
        let inner =
            GetIterator::from_lsm_tree(Bytes::from_static(b"key"), &tree, &context, Some(5))
                .unwrap();
        let mut iter = crate::db_iter::DbIterator::new(
            range,
            None,
            Vec::<Box<dyn RowEntryIterator>>::new(),
            Box::new(inner),
            Some(5),
            None,
            IterationOrder::Ascending,
            ReadTrace::new(None),
        )
        .await
        .unwrap();
        let mut original = tokio::spawn(async move {
            let result = iter.next_entry().await;
            let closed = iter.close_inner().await;
            (result, closed)
        });
        let started = tokio::time::timeout(Duration::from_secs(2), record.wait(&record.held)).await;
        let early = tokio::time::timeout(Duration::from_millis(25), &mut original).await;
        record.release();
        let (returned_early, result) = match early {
            Ok(result) => (true, result),
            Err(_) => match tokio::time::timeout(Duration::from_secs(2), &mut original).await {
                Ok(result) => (false, result),
                Err(_) => {
                    original.abort();
                    let _ = original.await;
                    panic!("original exact-key Scan did not join");
                }
            },
        };
        let (result, closed) = result.unwrap();
        assert!(
            started.is_ok(),
            "original unused index lookup never started"
        );
        assert!(
            !returned_early,
            "exact-key Scan returned before original unused lookup joined"
        );
        assert_eq!(record.joined.load(Ordering::SeqCst), 1);
        assert!(
            matches!(result, Err(SlateDBError::ObjectStoreError(ref error))
            if matches!(error.as_ref(), object_store::Error::NotFound { .. })),
            "{result:?}"
        );
        assert!(
            matches!(closed, Err(SlateDBError::ObjectStoreError(ref error))
            if matches!(error.as_ref(), object_store::Error::NotFound { .. })),
            "{closed:?}"
        );
    }

    /// Vec-backed iterator over fixed RowEntries. `seek` discards
    /// entries on the already-emitted side of `next_key` according to
    /// the configured order.
    struct VecIter {
        entries: VecDeque<RowEntry>,
        order: IterationOrder,
        initialized: bool,
    }

    impl VecIter {
        fn with_order(entries: Vec<RowEntry>, order: IterationOrder) -> Self {
            Self {
                entries: entries.into(),
                order,
                initialized: false,
            }
        }
    }

    #[async_trait]
    impl RowEntryIterator for VecIter {
        async fn close(&mut self) -> Result<(), SlateDBError> {
            Ok(())
        }

        async fn init(&mut self) -> Result<(), SlateDBError> {
            self.initialized = true;
            Ok(())
        }

        async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
            if !self.initialized {
                return Err(SlateDBError::IteratorNotInitialized);
            }
            Ok(self.entries.pop_front())
        }

        async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
            if !self.initialized {
                return Err(SlateDBError::IteratorNotInitialized);
            }
            while let Some(front) = self.entries.front() {
                let satisfied = match self.order {
                    IterationOrder::Ascending => front.key.as_ref() >= next_key,
                    IterationOrder::Descending => front.key.as_ref() <= next_key,
                };
                if satisfied {
                    break;
                }
                self.entries.pop_front();
            }
            Ok(())
        }
    }

    fn entry(key: &[u8], val: &[u8], seq: u64) -> RowEntry {
        RowEntry {
            key: Bytes::copy_from_slice(key),
            value: ValueDeletable::Value(Bytes::copy_from_slice(val)),
            seq,
            create_ts: None,
            expire_ts: None,
        }
    }

    fn tombstone(key: &[u8], seq: u64) -> RowEntry {
        RowEntry {
            key: Bytes::copy_from_slice(key),
            value: ValueDeletable::Tombstone,
            seq,
            create_ts: None,
            expire_ts: None,
        }
    }

    /// Tests inject pre-built, already-initialized iterators. Production
    /// wires up `Pending(LsmTreeState)` plus a `SegmentScanContext`; the chain
    /// logic is identical for already-`Built` children either way.
    async fn built_child(
        prefix: &[u8],
        entries: Vec<RowEntry>,
        order: IterationOrder,
    ) -> (Bytes, SegmentIterState) {
        let mut iter = VecIter::with_order(entries, order);
        iter.init().await.unwrap();
        (
            Bytes::copy_from_slice(prefix),
            SegmentIterState::Built(Box::new(iter)),
        )
    }

    async fn make_iter(specs: Vec<(&[u8], Vec<RowEntry>)>) -> SegmentMergeIterator {
        make_iter_with_order(specs, IterationOrder::Ascending).await
    }

    async fn make_iter_with_order(
        specs: Vec<(&[u8], Vec<RowEntry>)>,
        order: IterationOrder,
    ) -> SegmentMergeIterator {
        let mut children = VecDeque::new();
        for (prefix, entries) in specs {
            children.push_back(built_child(prefix, entries, order).await);
        }
        // Tests bypass the production `new` (which takes Vec<Segment>
        // and lazily promotes) by constructing the struct directly with
        // already-`Built` children. Same-module access lets us skip the
        // `SegmentScanContext` entirely. For descending tests the
        // caller is expected to pass `specs` in iteration order
        // (largest prefix first), matching what production does
        // internally via the `new` reverse step.
        SegmentMergeIterator {
            children,
            context: None,
            order,
            pending_seek: None,
            initialized: false,
        }
    }

    async fn drain(iter: &mut SegmentMergeIterator) -> Vec<Bytes> {
        let mut out = Vec::new();
        while let Some(e) = iter.next().await.unwrap() {
            out.push(e.key);
        }
        out
    }

    #[tokio::test]
    async fn empty_chain_returns_none() {
        let mut iter = make_iter(vec![]).await;
        iter.init().await.unwrap();
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn next_before_init_errors() {
        let mut iter = make_iter(vec![]).await;
        let err = iter.next().await.unwrap_err();
        assert!(matches!(err, SlateDBError::IteratorNotInitialized));
    }

    #[tokio::test]
    async fn single_child_passes_through() {
        let mut iter = make_iter(vec![(
            b"a/",
            vec![entry(b"a/1", b"x", 1), entry(b"a/2", b"y", 2)],
        )])
        .await;
        iter.init().await.unwrap();
        assert_eq!(
            drain(&mut iter).await,
            vec![Bytes::from_static(b"a/1"), Bytes::from_static(b"a/2")],
        );
    }

    #[tokio::test]
    async fn unsegmented_child_with_empty_prefix() {
        // Unsegmented mode is a singleton segment with empty prefix.
        // Empty starts every byte string, so seek's binary search
        // treats it as covering the whole keyspace.
        let mut iter = make_iter(vec![(
            b"",
            vec![entry(b"alpha", b"x", 1), entry(b"omega", b"y", 2)],
        )])
        .await;
        iter.init().await.unwrap();
        iter.seek(b"foo").await.unwrap();
        assert_eq!(drain(&mut iter).await, vec![Bytes::from_static(b"omega")]);
    }

    #[tokio::test]
    async fn two_children_chain_in_order() {
        let mut iter = make_iter(vec![
            (b"a/", vec![entry(b"a/1", b"x", 1), entry(b"a/2", b"y", 2)]),
            (b"b/", vec![entry(b"b/1", b"z", 3)]),
        ])
        .await;
        iter.init().await.unwrap();
        assert_eq!(
            drain(&mut iter).await,
            vec![
                Bytes::from_static(b"a/1"),
                Bytes::from_static(b"a/2"),
                Bytes::from_static(b"b/1"),
            ],
        );
    }

    #[tokio::test]
    async fn empty_intermediate_child_is_skipped() {
        let mut iter = make_iter(vec![
            (b"a/", vec![entry(b"a/1", b"x", 1)]),
            (b"b/", vec![]),
            (b"c/", vec![entry(b"c/1", b"y", 2)]),
        ])
        .await;
        iter.init().await.unwrap();
        assert_eq!(
            drain(&mut iter).await,
            vec![Bytes::from_static(b"a/1"), Bytes::from_static(b"c/1")],
        );
    }

    #[tokio::test]
    async fn seek_within_current_child() {
        // Seek lands inside the current child's interval; only the
        // current child needs to advance.
        let mut iter = make_iter(vec![
            (
                b"a/",
                vec![
                    entry(b"a/1", b"x", 1),
                    entry(b"a/2", b"y", 2),
                    entry(b"a/3", b"z", 3),
                ],
            ),
            (b"b/", vec![entry(b"b/1", b"w", 4)]),
        ])
        .await;
        iter.init().await.unwrap();
        iter.seek(b"a/2").await.unwrap();
        assert_eq!(
            drain(&mut iter).await,
            vec![
                Bytes::from_static(b"a/2"),
                Bytes::from_static(b"a/3"),
                Bytes::from_static(b"b/1"),
            ],
        );
    }

    #[tokio::test]
    async fn seek_into_later_child_skips_intervening() {
        // Seek key sits inside the third child's interval. The binary
        // search drops the first two children outright; only the third
        // is initialized.
        let mut iter = make_iter(vec![
            (b"a/", vec![entry(b"a/1", b"x", 1), entry(b"a/2", b"y", 2)]),
            (b"b/", vec![entry(b"b/1", b"z", 3)]),
            (b"c/", vec![entry(b"c/1", b"u", 4), entry(b"c/2", b"v", 5)]),
        ])
        .await;
        iter.init().await.unwrap();
        iter.seek(b"c/2").await.unwrap();
        assert_eq!(drain(&mut iter).await, vec![Bytes::from_static(b"c/2")]);
    }

    #[tokio::test]
    async fn seek_into_gap_advances_to_next_segment() {
        // Seek key falls between segments, not inside any interval.
        // Drops every preceding child; the first child whose prefix is
        // `>= seek_key` becomes the new front.
        let mut iter = make_iter(vec![
            (b"a/", vec![entry(b"a/1", b"x", 1)]),
            (b"c/", vec![entry(b"c/1", b"y", 2)]),
        ])
        .await;
        iter.init().await.unwrap();
        iter.seek(b"b/key").await.unwrap();
        assert_eq!(drain(&mut iter).await, vec![Bytes::from_static(b"c/1")]);
    }

    #[tokio::test]
    async fn seek_lazily_initializes_promoted_children() {
        // After seek drops the first child, exhausting the new front
        // re-applies the stashed seek key when the next child is
        // promoted — verifies pending_seek persists across promotions.
        let mut iter = make_iter(vec![
            (b"a/", vec![entry(b"a/1", b"x", 1)]),
            (b"b/", vec![entry(b"b/1", b"y", 2)]),
            (b"c/", vec![entry(b"c/1", b"z", 3), entry(b"c/2", b"w", 4)]),
        ])
        .await;
        iter.init().await.unwrap();
        iter.seek(b"b/").await.unwrap();
        assert_eq!(
            drain(&mut iter).await,
            vec![
                Bytes::from_static(b"b/1"),
                Bytes::from_static(b"c/1"),
                Bytes::from_static(b"c/2"),
            ],
        );
    }

    #[tokio::test]
    async fn tombstones_propagate() {
        // The chain is type-blind — tombstones flow through unchanged
        // and are interpreted by downstream consumers.
        let mut iter = make_iter(vec![(
            b"a/",
            vec![
                entry(b"a/1", b"x", 1),
                tombstone(b"a/2", 2),
                entry(b"a/3", b"z", 3),
            ],
        )])
        .await;
        iter.init().await.unwrap();

        let first = iter.next().await.unwrap().unwrap();
        assert!(matches!(first.value, ValueDeletable::Value(_)));
        let second = iter.next().await.unwrap().unwrap();
        assert!(matches!(second.value, ValueDeletable::Tombstone));
        let third = iter.next().await.unwrap().unwrap();
        assert!(matches!(third.value, ValueDeletable::Value(_)));
    }

    #[tokio::test]
    async fn descending_two_children_chain_largest_prefix_first() {
        // Descending: caller supplies specs in iteration order
        // (largest prefix first), and within each child entries are
        // already in descending order. The chain emits them in that
        // order, never crossing back to a smaller prefix once
        // exhausted.
        let mut iter = make_iter_with_order(
            vec![
                (b"b/", vec![entry(b"b/2", b"z", 3), entry(b"b/1", b"y", 2)]),
                (b"a/", vec![entry(b"a/2", b"w", 4), entry(b"a/1", b"x", 1)]),
            ],
            IterationOrder::Descending,
        )
        .await;
        iter.init().await.unwrap();
        assert_eq!(
            drain(&mut iter).await,
            vec![
                Bytes::from_static(b"b/2"),
                Bytes::from_static(b"b/1"),
                Bytes::from_static(b"a/2"),
                Bytes::from_static(b"a/1"),
            ],
        );
    }

    #[tokio::test]
    async fn descending_seek_into_earlier_child_skips_intervening() {
        // Descending: seek to `a/2` lands in the *last* segment
        // (smallest prefix). The two larger-prefix segments must be
        // dropped from the front. Verifies count_before's descending
        // predicate.
        let mut iter = make_iter_with_order(
            vec![
                (b"c/", vec![entry(b"c/2", b"z", 5), entry(b"c/1", b"y", 4)]),
                (b"b/", vec![entry(b"b/1", b"x", 3)]),
                (b"a/", vec![entry(b"a/2", b"w", 2), entry(b"a/1", b"v", 1)]),
            ],
            IterationOrder::Descending,
        )
        .await;
        iter.init().await.unwrap();
        iter.seek(b"a/2").await.unwrap();
        assert_eq!(
            drain(&mut iter).await,
            vec![Bytes::from_static(b"a/2"), Bytes::from_static(b"a/1")],
        );
    }

    #[tokio::test]
    async fn descending_seek_into_gap_advances_to_smaller_segment() {
        // Descending: seek key falls between segments (no segment
        // contains it). The first child whose prefix is `<= seek_key`
        // becomes the new front — here `a/`, since `b/key` is between
        // `c/` and `a/` in lex order and the only candidates whose
        // prefix is `<= b/key` are `a/` and `b/`-prefixed (none here).
        let mut iter = make_iter_with_order(
            vec![
                (b"c/", vec![entry(b"c/1", b"z", 3)]),
                (b"a/", vec![entry(b"a/1", b"y", 1)]),
            ],
            IterationOrder::Descending,
        )
        .await;
        iter.init().await.unwrap();
        iter.seek(b"b/key").await.unwrap();
        assert_eq!(drain(&mut iter).await, vec![Bytes::from_static(b"a/1")]);
    }

    #[tokio::test]
    async fn descending_seek_within_segment_via_prefix_extension() {
        // Descending: seek key extends a segment's prefix. That child
        // contains the key, so we keep it and seek inside. The
        // descending predicate `prefix > key` is false when prefix is
        // a prefix of key (since prefix <= key lexicographically), so
        // no extension special-case is needed.
        let mut iter = make_iter_with_order(
            vec![
                (b"c/", vec![entry(b"c/1", b"z", 3)]),
                (
                    b"b/",
                    vec![
                        entry(b"b/3", b"w", 6),
                        entry(b"b/2", b"v", 5),
                        entry(b"b/1", b"u", 4),
                    ],
                ),
                (b"a/", vec![entry(b"a/1", b"t", 1)]),
            ],
            IterationOrder::Descending,
        )
        .await;
        iter.init().await.unwrap();
        iter.seek(b"b/2").await.unwrap();
        assert_eq!(
            drain(&mut iter).await,
            vec![
                Bytes::from_static(b"b/2"),
                Bytes::from_static(b"b/1"),
                Bytes::from_static(b"a/1"),
            ],
        );
    }

    #[tokio::test]
    async fn descending_seek_drops_all_children() {
        // Seek key is smaller than every segment's prefix in
        // descending iteration → drop everything. next() returns
        // None.
        let mut iter = make_iter_with_order(
            vec![
                (b"c/", vec![entry(b"c/1", b"z", 2)]),
                (b"b/", vec![entry(b"b/1", b"y", 1)]),
            ],
            IterationOrder::Descending,
        )
        .await;
        iter.init().await.unwrap();
        // All prefixes (b/, c/) are > "a/anything" lex-wise, so
        // count_before drops them all in descending mode.
        iter.seek(b"a/zzz").await.unwrap();
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn ascending_seek_drops_all_children() {
        // Symmetric to the descending variant: seek key is larger
        // than every segment's interval → drop everything.
        let mut iter = make_iter(vec![
            (b"a/", vec![entry(b"a/1", b"x", 1)]),
            (b"b/", vec![entry(b"b/1", b"y", 2)]),
        ])
        .await;
        iter.init().await.unwrap();
        iter.seek(b"z/").await.unwrap();
        assert!(iter.next().await.unwrap().is_none());
    }
}
