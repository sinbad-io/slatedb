use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use log::info;
use parking_lot::{Mutex, MutexGuard};
use tokio::sync::futures::Notified;
use tokio::sync::{oneshot, Notify};

use crate::batch_write::{BatchWriterFlush, BatchWriterMessage};
use crate::utils::{format_bytes_si, SafeSender};

/// How long a member's flush may run before a crossing picks another member.
const FLIGHT: Duration = Duration::from_secs(30);

/// One bound on the bytes every database sharing it holds unflushed, by the
/// engine's estimate: the active and immutable memtables and the WAL buffers
/// that `max_unflushed_bytes` bounds per database. A write that finds the sum
/// at or above the capacity waits, and the member whose active memtable and
/// current WAL buffer hold the most is asked to flush, one member at a time.
pub struct UnflushedBudget {
    capacity: usize,
    used: AtomicUsize,
    room: Notify,
    members: Mutex<Members>,
}

#[derive(Default)]
struct Members {
    next_id: u64,
    by_id: HashMap<u64, Member>,
    flushing: Option<(u64, DateTime<Utc>)>,
}

struct Member {
    published: usize,
    freezable: usize,
    writer: Option<SafeSender<BatchWriterMessage>>,
}

impl UnflushedBudget {
    /// A budget of `capacity` bytes.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            used: AtomicUsize::new(0),
            room: Notify::new(),
            members: Mutex::new(Members::default()),
        }
    }

    /// The bytes the sharing databases may hold unflushed.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The bytes the sharing databases last published as unflushed.
    pub fn used(&self) -> usize {
        self.used.load(Ordering::SeqCst)
    }

    pub(crate) fn crossed(&self) -> bool {
        self.used() >= self.capacity
    }

    /// Resolves once a publish or a departure lowers the sum under the
    /// capacity after this future is created.
    pub(crate) fn room(&self) -> Notified<'_> {
        self.room.notified()
    }

    pub(crate) fn join(self: &Arc<Self>) -> BudgetMember {
        let mut members = self.members.lock();
        let id = members.next_id;
        members.next_id += 1;
        members.by_id.insert(
            id,
            Member {
                published: 0,
                freezable: 0,
                writer: None,
            },
        );
        BudgetMember {
            budget: Arc::clone(self),
            id,
            order: Mutex::new(()),
        }
    }

    /// Sends a freezing flush to the member with the most freezable bytes,
    /// unless another member's flush is in flight; a flight ends when that
    /// member publishes fewer bytes, leaves, or has run for [`FLIGHT`], and a
    /// member whose flight ran out is not picked by the next crossing; a
    /// flight that started after `now` by the caller's clock has not run out.
    pub(crate) fn flush_largest(&self, now: DateTime<Utc>) {
        let mut members = self.members.lock();
        let expired = match members.flushing {
            Some((_, since))
                if now
                    .signed_duration_since(since)
                    .to_std()
                    .map_or(true, |elapsed| elapsed < FLIGHT) =>
            {
                return
            }
            Some((id, _)) => Some(id),
            None => None,
        };
        members.flushing = None;
        while let Some(id) = members
            .by_id
            .iter()
            .filter(|(id, m)| Some(**id) != expired && m.freezable > 0 && m.writer.is_some())
            .max_by_key(|(_, m)| m.freezable)
            .map(|(id, _)| *id)
        {
            let (done, _) = oneshot::channel();
            let flush = BatchWriterMessage::Flush(BatchWriterFlush {
                freeze_memtable: true,
                done,
            });
            let Some(member) = members.by_id.get_mut(&id) else {
                return;
            };
            if member
                .writer
                .as_ref()
                .is_none_or(|writer| writer.send(flush).is_err())
            {
                member.writer = None;
                continue;
            }
            info!(
                "unflushed budget crossed; flushing the member with the most freezable bytes [used={}, capacity={}, freezable={}, members={}]",
                format_bytes_si(self.used() as u64),
                format_bytes_si(self.capacity as u64),
                format_bytes_si(member.freezable as u64),
                members.by_id.len(),
            );
            members.flushing = Some((id, now));
            return;
        }
    }

    fn publish(&self, id: u64, bytes: UnflushedBytes) {
        let mut members = self.members.lock();
        let Some(member) = members.by_id.get_mut(&id) else {
            return;
        };
        let before = std::mem::replace(&mut member.published, bytes.total());
        member.freezable = bytes.freezable();
        if bytes.total() >= before {
            self.used
                .fetch_add(bytes.total() - before, Ordering::SeqCst);
            return;
        }
        if members.flushing.is_some_and(|(f, _)| f == id) {
            members.flushing = None;
        }
        self.lower(before - bytes.total());
    }

    fn leave(&self, id: u64) {
        let mut members = self.members.lock();
        let Some(member) = members.by_id.remove(&id) else {
            return;
        };
        if members.flushing.is_some_and(|(f, _)| f == id) {
            members.flushing = None;
        }
        self.lower(member.published);
    }

    fn lower(&self, by: usize) {
        if self.used.fetch_sub(by, Ordering::SeqCst) - by < self.capacity {
            self.room.notify_waiters();
        }
    }
}

/// A database's unflushed bytes by the engine's estimate.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct UnflushedBytes {
    pub(crate) active_memtable: usize,
    pub(crate) imm_memtables: usize,
    pub(crate) wal: usize,
    pub(crate) current_wal: usize,
}

impl UnflushedBytes {
    pub(crate) fn total(&self) -> usize {
        self.active_memtable
            .saturating_add(self.imm_memtables)
            .saturating_add(self.wal)
    }

    fn freezable(&self) -> usize {
        self.active_memtable.saturating_add(self.current_wal)
    }
}

/// A database's place in an [`UnflushedBudget`].
pub(crate) struct BudgetMember {
    budget: Arc<UnflushedBudget>,
    id: u64,
    order: Mutex<()>,
}

impl BudgetMember {
    pub(crate) fn budget(&self) -> &UnflushedBudget {
        &self.budget
    }

    /// Holds this member's publish order: the count measured under it is the
    /// count published last.
    pub(crate) fn publishing(&self) -> Publishing<'_> {
        Publishing {
            member: self,
            _order: self.order.lock(),
        }
    }

    /// Lets the budget send `writer` a flush.
    pub(crate) fn accept_flushes(&self, writer: SafeSender<BatchWriterMessage>) {
        if let Some(member) = self.budget.members.lock().by_id.get_mut(&self.id) {
            member.writer = Some(writer);
        }
    }

    /// Withdraws this member and its bytes; later publishes are ignored.
    pub(crate) fn leave(&self) {
        let _order = self.order.lock();
        self.budget.leave(self.id);
    }
}

/// Withdraws a member from its budget when dropped, unless its open finished:
/// an open that fails after joining leaves no bytes behind.
pub(crate) struct LeaveUnlessOpened(Option<Arc<BudgetMember>>);

impl LeaveUnlessOpened {
    pub(crate) fn new(member: Option<Arc<BudgetMember>>) -> Self {
        Self(member)
    }

    pub(crate) fn opened(mut self) {
        self.0 = None;
    }
}

impl Drop for LeaveUnlessOpened {
    fn drop(&mut self) {
        if let Some(member) = self.0.take() {
            member.leave();
        }
    }
}

pub(crate) struct Publishing<'a> {
    member: &'a BudgetMember,
    _order: MutexGuard<'a, ()>,
}

impl Publishing<'_> {
    pub(crate) fn publish(&self, bytes: UnflushedBytes) {
        self.member.budget.publish(self.member.id, bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::SlateDBError;
    use crate::utils::WatchableOnceCell;

    fn writer(
        closed: bool,
    ) -> (
        SafeSender<BatchWriterMessage>,
        async_channel::Receiver<BatchWriterMessage>,
    ) {
        let cell = WatchableOnceCell::new();
        if closed {
            cell.write(Err(SlateDBError::Closed));
        }
        SafeSender::unbounded_channel(cell.reader())
    }

    #[test]
    fn test_flush_largest_passes_over_a_member_whose_writer_is_gone() {
        let budget = Arc::new(UnflushedBudget::new(1 << 20));
        let gone = budget.join();
        let live = budget.join();
        let (gone_writer, gone_rx) = writer(true);
        drop(gone_rx);
        let (live_writer, live_rx) = writer(false);
        gone.accept_flushes(gone_writer);
        live.accept_flushes(live_writer);
        let bytes = |active_memtable| UnflushedBytes {
            active_memtable,
            ..Default::default()
        };
        gone.publishing().publish(bytes(2 << 20));
        live.publishing().publish(bytes(1 << 20));

        budget.flush_largest(DateTime::UNIX_EPOCH);
        assert!(
            matches!(live_rx.try_recv(), Ok(BatchWriterMessage::Flush(_))),
            "the next largest member is flushed when the largest one's writer is gone"
        );
        assert!(budget.members.lock().by_id[&gone.id].writer.is_none());
    }
}
