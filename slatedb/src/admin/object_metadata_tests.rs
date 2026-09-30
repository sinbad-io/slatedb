use super::AdminBuilder;
use crate::test_utils::FlakyObjectStore;
use crate::ObjectMetadata;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Debug)]
struct ListingOnly {
    prefix: Path,
    objects: Vec<ObjectMeta>,
    pause: bool,
    entered: Arc<Notify>,
    dropped: Arc<AtomicBool>,
}

impl ListingOnly {
    fn new(objects: Vec<ObjectMeta>, pause: bool) -> Self {
        Self {
            prefix: Path::from("tenant/cell"),
            objects,
            pause,
            entered: Arc::new(Notify::new()),
            dropped: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl fmt::Display for ListingOnly {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ListingOnly")
    }
}

struct ListingDrop(Arc<AtomicBool>);

impl Drop for ListingDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl ObjectStore for ListingOnly {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        panic!("an object observation must not put an object")
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        panic!("an object observation must not start an upload")
    }

    async fn get_opts(
        &self,
        _location: &Path,
        _opts: GetOptions,
    ) -> object_store::Result<GetResult> {
        panic!("an object observation must use the listing metadata")
    }

    fn delete_stream(
        &self,
        _locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        panic!("an object observation must not delete an object")
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        assert_eq!(prefix, Some(&self.prefix));
        let objects = stream::iter(self.objects.clone().into_iter().map(Ok));
        if !self.pause {
            return objects.boxed();
        }
        let entered = self.entered.clone();
        let dropped = self.dropped.clone();
        objects
            .chain(stream::once(async move {
                let _drop = ListingDrop(dropped);
                entered.notify_one();
                std::future::pending::<object_store::Result<ObjectMeta>>().await
            }))
            .boxed()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> object_store::Result<ListResult> {
        panic!("an object observation must read the full listing")
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _opts: CopyOptions,
    ) -> object_store::Result<()> {
        panic!("an object observation must not copy an object")
    }
}

fn metadata(path: &str) -> ObjectMeta {
    ObjectMeta {
        location: Path::from(path),
        last_modified: DateTime::<Utc>::from_timestamp(1_700_000_000, 123_456_789).unwrap(),
        size: u64::from(u32::MAX) + 19,
        e_tag: Some("etag-unchanged".to_owned()),
        version: Some("generation-7".to_owned()),
    }
}

#[tokio::test]
async fn main_object_observation_keeps_every_metadata_field() {
    let first = metadata("tenant/cell/data/first.sst");
    let mut second = metadata("tenant/cell/wal/second.sst");
    second.size = 0;
    second.e_tag = None;
    second.version = None;
    let store = Arc::new(ListingOnly::new(vec![first.clone(), second.clone()], false));
    let admin = AdminBuilder::new("tenant/cell", store).build();

    let mut observed = admin.list_main_objects().await.unwrap();
    observed.sort_by(|a, b| a.location.cmp(&b.location));
    let mut expected = vec![
        ObjectMetadata {
            location: Path::from("tenant/cell/data/first.sst"),
            last_modified: DateTime::<Utc>::from_timestamp(1_700_000_000, 123_456_789).unwrap(),
            size: 4_294_967_314,
            e_tag: Some("etag-unchanged".to_owned()),
            version: Some("generation-7".to_owned()),
        },
        ObjectMetadata {
            location: Path::from("tenant/cell/wal/second.sst"),
            last_modified: DateTime::<Utc>::from_timestamp(1_700_000_000, 123_456_789).unwrap(),
            size: 0,
            e_tag: None,
            version: None,
        },
    ];
    expected.sort_by(|a, b| a.location.cmp(&b.location));
    assert_eq!(observed, expected);
}

#[tokio::test]
async fn main_object_observation_keeps_scope_and_does_not_change_objects() {
    let store = Arc::new(InMemory::new());
    for name in [
        "tenant/cell/manifest/first",
        "tenant/cell/data/first.sst",
        "tenant/cell/wal/first.sst",
        "tenant/cell/.deleting",
        "tenant/cell-neighbor/data/first.sst",
        "tenant/cell2/data/first.sst",
        "neighbor/cell/data/first.sst",
    ] {
        store
            .put(
                &Path::from(name),
                Bytes::copy_from_slice(name.as_bytes()).into(),
            )
            .await
            .unwrap();
    }
    let before: Vec<_> = store.list(None).try_collect().await.unwrap();
    let admin = AdminBuilder::new("tenant/cell", store.clone()).build();

    let observed = admin.list_main_objects().await.unwrap();
    let names: std::collections::BTreeSet<_> = observed
        .iter()
        .map(|object| object.location.to_string())
        .collect();
    assert_eq!(
        names,
        std::collections::BTreeSet::from([
            "tenant/cell/.deleting".to_owned(),
            "tenant/cell/data/first.sst".to_owned(),
            "tenant/cell/manifest/first".to_owned(),
            "tenant/cell/wal/first.sst".to_owned(),
        ])
    );
    assert_eq!(
        store.list(None).try_collect::<Vec<_>>().await.unwrap(),
        before
    );
    for object in before {
        let bytes = store
            .get(&object.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), object.location.as_ref().as_bytes());
    }
}

#[tokio::test]
async fn main_object_observation_keeps_a_separate_wal_outside_its_scope() {
    let main = Arc::new(InMemory::new());
    let wal = Arc::new(InMemory::new());
    let path = Path::from("tenant/cell/wal/same-name.sst");
    main.put(&path, Bytes::from_static(b"main").into())
        .await
        .unwrap();
    wal.put(&path, Bytes::from_static(b"separate-wal").into())
        .await
        .unwrap();
    let expected = ObjectMetadata::new(main.head(&path).await.unwrap());
    let admin = AdminBuilder::new("tenant/cell", main)
        .with_wal_object_store(wal.clone())
        .build();

    assert_eq!(admin.list_main_objects().await.unwrap(), vec![expected]);
    assert_eq!(
        wal.get(&path).await.unwrap().bytes().await.unwrap(),
        Bytes::from_static(b"separate-wal")
    );
}

#[tokio::test]
async fn main_object_observation_refuses_a_late_listing_error() {
    let store = Arc::new(InMemory::new());
    for name in ["tenant/cell/a", "tenant/cell/b"] {
        store
            .put(&Path::from(name), Bytes::from_static(b"data").into())
            .await
            .unwrap();
    }
    let flaky = Arc::new(FlakyObjectStore::new(store.clone(), 0).with_list_failures(1, 1));
    let mut admin = AdminBuilder::new("tenant/cell", flaky.clone()).build();
    admin.object_store_max_retries = Some(0);

    let error = admin.list_main_objects().await.unwrap_err();
    assert!(
        error.to_string().contains("injected list failure"),
        "{error}"
    );
    assert_eq!(flaky.list_attempts(), 1);
    assert_eq!(
        store.list(None).try_collect::<Vec<_>>().await.unwrap().len(),
        2
    );
}

#[tokio::test]
async fn main_object_observation_of_an_empty_prefix_creates_no_intent() {
    let store = Arc::new(InMemory::new());
    let admin = AdminBuilder::new("tenant/cell", store.clone()).build();

    assert!(admin.list_main_objects().await.unwrap().is_empty());
    assert!(store
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn main_object_observation_cancellation_drops_the_original_listing() {
    let store = Arc::new(ListingOnly::new(vec![metadata("tenant/cell/a")], true));
    let admin = AdminBuilder::new("tenant/cell", store.clone()).build();
    let task = tokio::spawn(async move { admin.list_main_objects().await });

    let entered = tokio::time::timeout(Duration::from_secs(1), store.entered.notified()).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    entered.expect("the original listing did not reach its held item");
    assert!(store.dropped.load(Ordering::SeqCst));
}
