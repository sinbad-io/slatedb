use super::GcTask;
use crate::manifest::Manifest;
use crate::{
    error::SlateDBError,
    manifest::store::ManifestStore,
    wal::{WalFileRange, WalGc, WalGcPolicy, WalGcRequest},
};
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

/// Runs one or both WAL retention policies through a [`WalGc`]. When the WAL and WAL fence
/// policies share a schedule, a single task carries both so the implementation can serve them
/// from one scan.
#[derive(Clone)]
pub(crate) struct WalGcTask {
    manifest_store: Arc<ManifestStore>,
    wal_gc: Arc<dyn WalGc>,
    wal: Option<WalGcPolicy>,
    fence: Option<WalGcPolicy>,
}

impl std::fmt::Debug for WalGcTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalGcTask")
            .field("wal", &self.wal)
            .field("fence", &self.fence)
            .finish()
    }
}

impl WalGcTask {
    pub(super) fn new(
        manifest_store: Arc<ManifestStore>,
        wal_gc: Arc<dyn WalGc>,
        wal: Option<WalGcPolicy>,
        fence: Option<WalGcPolicy>,
    ) -> Self {
        Self {
            manifest_store,
            wal_gc,
            wal,
            fence,
        }
    }

    fn referenced_wal_ranges(
        latest_manifest_id: u64,
        active_manifests: &BTreeMap<u64, Manifest>,
    ) -> Vec<WalFileRange> {
        active_manifests
            .iter()
            .map(|(manifest_id, manifest)| {
                if *manifest_id == latest_manifest_id {
                    // Keep the current compaction boundary and everything after it. Retaining the
                    // boundary matches the existing GC protocol and protects concurrent writers.
                    WalFileRange(
                        Bound::Included(manifest.core.replay_after_wal_id),
                        Bound::Unbounded,
                    )
                } else {
                    // A checkpoint only references WALs that must be replayed for its manifest.
                    WalFileRange(
                        Bound::Excluded(manifest.core.replay_after_wal_id),
                        Bound::Excluded(manifest.core.next_wal_sst_id),
                    )
                }
            })
            .collect()
    }
}

impl GcTask for WalGcTask {
    /// Resolve the WAL ranges referenced by the current manifest and active checkpoints, then
    /// delegate collection to the configured WAL implementation.
    async fn collect(&self, _utc_now: DateTime<Utc>) -> Result<(), SlateDBError> {
        let latest_manifest = self.manifest_store.read_latest_manifest().await?;
        let active_manifests = self
            .manifest_store
            .read_referenced_manifests(latest_manifest.id, &latest_manifest.manifest)
            .await?;
        let referenced_ranges = Self::referenced_wal_ranges(latest_manifest.id, &active_manifests);

        self.wal_gc
            .collect(WalGcRequest::new(referenced_ranges, self.wal, self.fence))
            .await
            .map_err(Into::into)
    }

    fn resource(&self) -> &str {
        match (self.wal.is_some(), self.fence.is_some()) {
            (true, true) => "WAL and WAL fence",
            (false, true) => "WAL fence",
            _ => "WAL",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::Checkpoint;
    use crate::manifest::store::StoredManifest;
    use crate::manifest::ManifestCore;
    use crate::wal::WalError;
    use async_trait::async_trait;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb_common::clock::DefaultSystemClock;
    use std::sync::Mutex;
    use std::time::Duration;
    use uuid::Uuid;

    #[derive(Default)]
    struct RecordingWalGc {
        calls: Mutex<Vec<WalGcRequest>>,
    }

    impl RecordingWalGc {
        fn calls(&self) -> Vec<WalGcRequest> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl WalGc for RecordingWalGc {
        async fn collect(&self, request: WalGcRequest) -> Result<(), WalError> {
            self.calls.lock().unwrap().push(request);
            Ok(())
        }
    }

    #[test]
    fn test_referenced_wal_ranges() {
        let mut checkpoint_core = ManifestCore::new();
        checkpoint_core.replay_after_wal_id = 2;
        checkpoint_core.next_wal_sst_id = 6;

        let mut current_core = ManifestCore::new();
        current_core.replay_after_wal_id = 5;
        current_core.next_wal_sst_id = 8;

        let active_manifests = BTreeMap::from([
            (1, Manifest::initial(checkpoint_core)),
            (2, Manifest::initial(current_core)),
        ]);

        assert_eq!(
            WalGcTask::referenced_wal_ranges(2, &active_manifests),
            vec![
                WalFileRange(Bound::Excluded(2), Bound::Excluded(6)),
                WalFileRange(Bound::Included(5), Bound::Unbounded),
            ]
        );
    }

    #[tokio::test]
    async fn test_collect_calls_wal_gc_with_ranges_and_policies() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from("/test/wal-gc-ranges"),
            object_store,
        ));

        let mut checkpoint_core = ManifestCore::new();
        checkpoint_core.replay_after_wal_id = 2;
        checkpoint_core.next_wal_sst_id = 6;
        let mut stored_manifest = StoredManifest::create_new_db(
            manifest_store.clone(),
            checkpoint_core,
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();
        let checkpoint_manifest_id = stored_manifest.id();

        let mut dirty = stored_manifest.prepare_dirty().unwrap();
        dirty.value.core.replay_after_wal_id = 5;
        dirty.value.core.next_wal_sst_id = 8;
        dirty.value.core.checkpoints.push(Checkpoint {
            id: Uuid::new_v4(),
            manifest_id: checkpoint_manifest_id,
            expire_time: None,
            create_time: Utc::now(),
            name: None,
        });
        stored_manifest.update(dirty).await.unwrap();

        let wal_gc = Arc::new(RecordingWalGc::default());
        let wal = WalGcPolicy {
            min_age: Duration::ZERO,
            dry_run: false,
        };
        let fence = WalGcPolicy {
            min_age: Duration::from_secs(60),
            dry_run: true,
        };
        let task = WalGcTask::new(manifest_store, wal_gc.clone(), Some(wal), Some(fence));

        task.collect(Utc::now()).await.unwrap();

        let calls = wal_gc.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].referenced_ranges,
            vec![
                WalFileRange(Bound::Excluded(2), Bound::Excluded(6)),
                WalFileRange(Bound::Included(5), Bound::Unbounded),
            ]
        );
        assert_eq!(calls[0].wal, Some(wal));
        assert_eq!(calls[0].fence, Some(fence));
    }
}
