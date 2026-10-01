<a href="https://slatedb.io">
  <img src="https://github.com/slatedb/slatedb/blob/main/website/public/img/slatedb-gh-banner.png?raw=true" alt="SlateDB" width="100%">
</a>

<a href="https://crates.io/crates/slatedb">![Crates.io Version](https://img.shields.io/crates/v/slatedb?style=flat-square)</a>
![GitHub License](https://img.shields.io/github/license/slatedb/slatedb?style=flat-square)
<a href="https://slatedb.io">![slatedb.io](https://img.shields.io/badge/site-slatedb.io-00A1FF?style=flat-square)</a>
<a href="https://discord.gg/mHYmGy5MgA">![Discord](https://img.shields.io/discord/1232385660460204122?style=flat-square)</a>
<a href="https://docs.rs/slatedb/latest/slatedb/">![Docs](https://img.shields.io/badge/docs-docs.rs-00A1FF?style=flat-square)</a>
<a href="https://github.dosu.com/slatedb/slatedb/ask">![Dosu](https://img.shields.io/badge/bot-dosu.dev-dd404a?style=flat-square)</a>
<a href="https://opencollective.com/commonhaus-foundation/projects/slatedb">![OpenCollective](https://img.shields.io/badge/donate-opencollective.com-287233?style=flat-square)</a>

## Introduction

[SlateDB](https://slatedb.io) is an embedded storage engine built as a [log-structured merge-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree). Unlike traditional LSM-tree storage engines, SlateDB writes data to object storage (S3, GCS, ABS, MinIO, Tigris, and so on). Leveraging object storage allows SlateDB to provide bottomless storage capacity, high durability, and easy replication. The trade-off is that object storage has a higher latency and higher API cost than local disk.

To mitigate high write API costs (PUTs), SlateDB batches writes. Rather than writing every `put()` call to object storage, MemTables are flushed periodically to object storage as a string-sorted table (SST). The flush interval is configurable.

Write operations return a `WriteHandle` after updating the in-memory WAL and MemTable. Call `handle.await_durable().await` to wait for one write to become durable, or call `db.flush().await` to flush all pending writes.

To mitigate read latency and read API costs (GETs), SlateDB will use standard LSM-tree caching techniques: in-memory block caches, compression, bloom filters, and local SST disk caches.

Checkout [slatedb.io](https://slatedb.io) to learn more.

## Othermemory point-read candidate

The published point baseline `ff1e3b67` adds cooperative cancellation to point reads. It retains the original call and its started work. Its shared Darwin debug roll completed on 2026-09-30 with library SHA256 `32d5feef367d3982986a7a23c0d727dc2ac316d3e198391920d462ac43750153`. That narrow result does not complete native acceptance.

| Component | Reproducible source / evidence | Limit |
| --- | --- | --- |
| Point core and original multi-source joins | `17e7451c5c57abd3a23545be1894685ea78c0fc4`: 58 selected native controls pass; four window omissions fail at their named assertions | Complete native suites remain separate |
| Existing UniFFI token and point adapters | `5b4966722f99709d1be85e13d990d8945281150e`: private Linux debug library SHA256 `5c23cdf5c08199a1d03682401b3e2df7b061c82fdfddcabc2c59be9aaa0fe6d1` | Selected point scope only |
| Generated Go and callback/reader contract | `02ff5c6c41788fa2935f70b9570581d246a01e7d`: 28 named nodes pass ordinary and race; both selected-package vets pass | Complete Go modules and explicit binding omissions remain separate |

Cancellable concurrent reads made four modeled GETs versus one on the legacy deduplicated path. Cache hits retain the original lookup; a local cache stall can still delay cancellation. No regional performance neutrality or fixed wall-clock interruption is claimed. The [dated run report](docs/runs/RUN_2026-09-30_native-point-cancellation.md) preserves original failures, exact commands, tool/library hashes, generated-byte provenance and process closure. Fork publication does not authorize a dependent merge; the shared binding roll is a separate, locked operation.

## Othermemory Scan successor

This prepared successor adds owned Scan cancellation and async iterator cleanup. It keeps the existing owners and joins their started work. It does not add a registry. Seek retires its original prefetch before replacement. Constructor errors join all started children. Selected storage errors keep their precedence.

The [dated Scan report](docs/runs/RUN_2026-10-01_native-scan-ownership.md) records the tested source, actual generated files and retained failures. The candidate library reported 2,193 passes and one declared ignore. Four ownership omissions failed at their intended assertions. A separate application run completed 286 passing nodes and 11 classified skips in each ordinary and race mode. Both scoped vets passed.

Four concurrent cold hybrid scans made 64 GETs without tokens and 256 with tokens. Memory-cache scans made 64 and 67. Warm scans made no GETs. These are cache mechanics, not real-store latency. A held Foyer lookup can exceed a request budget before it joins.

This source is prepared for review. Publication, complete native workspace/DST acceptance and the shared Darwin Scan roll remain separate. The existing point15 roll is the rollback baseline. A dependent application merge remains blocked until its required gates pass.

## Get Started

Add the following to your `Cargo.toml`:

```toml
[dependencies]
slatedb = "*"
tokio = "*"
```

Then you can use SlateDB in your Rust code:

```rust
use slatedb::{Db, Error};
use slatedb::object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Setup
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let kv_store = Db::open("/tmp/test_kv_store", object_store).await?;

    // Put
    let key = b"test_key";
    let value = b"test_value";
    kv_store.put(key, value).await?;

    // Get
    assert_eq!(
        kv_store.get(key).await?,
        Some("test_value".into())
    );

    // Delete
    kv_store.delete(key).await?;
    assert!(kv_store.get(key).await?.is_none());

    kv_store.put(b"test_key1", b"test_value1").await?;
    kv_store.put(b"test_key2", b"test_value2").await?;
    kv_store.put(b"test_key3", b"test_value3").await?;
    kv_store.put(b"test_key4", b"test_value4").await?;

    // Scan over unbound range
    let mut iter = kv_store.scan(..).await?;
    let mut count = 1;
    while let Ok(Some(item)) = iter.next().await {
        assert_eq!(
            item.key,
            format!("test_key{count}").into_bytes()
        );
        assert_eq!(
            item.value,
            format!("test_value{count}").into_bytes()
        );
        count += 1;
    }

    // Scan over bound range
    let mut iter = kv_store.scan("test_key1"..="test_key2").await?;
    let item = iter.next().await?.expect("missing test_key1");
    assert_eq!(item.key.as_ref(), b"test_key1");
    assert_eq!(item.value.as_ref(), b"test_value1");
    let item = iter.next().await?.expect("missing test_key2");
    assert_eq!(item.key.as_ref(), b"test_key2");
    assert_eq!(item.value.as_ref(), b"test_value2");

    // Seek ahead to next key
    let mut iter = kv_store.scan(..).await?;
    let next_key = b"test_key4";
    iter.seek(next_key).await?;
    let item = iter.next().await?.expect("missing test_key4");
    assert_eq!(item.key.as_ref(), b"test_key4");
    assert_eq!(item.value.as_ref(), b"test_value4");
    assert_eq!(iter.next().await?, None);

    // Close
    kv_store.close().await?;

    Ok(())
}
```

SlateDB uses the [`object_store`](https://docs.rs/object_store/latest/object_store/) crate to interact with object storage, and therefore supports any object storage that implements the `ObjectStore` trait. You can use the crate in your project to interact with any object storage that implements the `ObjectStore` trait. SlateDB also re-exports the [`object_store`](https://docs.rs/object_store/latest/object_store/) crate for your convenience.

## Documentation

Visit [slatedb.io](https://slatedb.io) to learn more.

## Bindings

- [Go](https://github.com/slatedb/slatedb/tree/main/bindings/go) (official)
- [Java](https://github.com/slatedb/slatedb/tree/main/bindings/java) (official)
- [Node.js / TypeScript](https://github.com/slatedb/slatedb/tree/main/bindings/node) (official)
- [.NET](https://github.com/Pulsy-Global/slatedb-dotnet)
- [.NET](https://github.com/dotstream/slatedb-csharp-binding)
- [Python](https://github.com/slatedb/slatedb/tree/main/bindings/python) (official)
- [Ruby](https://github.com/catkins/slatedb-rb)
- [TypeScript](https://github.com/gadget-inc/slatedb-node)

## Features

- [x] Basic API (get, put, delete)
- [x] SSTs on object storage
- [x] Range queries ([#8](https://github.com/slatedb/slatedb/issues/8))
- [x] Block cache ([#15](https://github.com/slatedb/slatedb/issues/15))
- [x] Disk cache ([#9](https://github.com/slatedb/slatedb/issues/9))
- [x] Compression ([#10](https://github.com/slatedb/slatedb/issues/10))
- [x] Bloom filters ([#11](https://github.com/slatedb/slatedb/issues/11))
- [x] Manifest persistence ([#14](https://github.com/slatedb/slatedb/issues/14))
- [x] Compaction ([#7](https://github.com/slatedb/slatedb/issues/7))
- [x] Transactions ([#785](https://github.com/slatedb/slatedb/issues/785))
- [x] Merge operator ([#328](https://github.com/slatedb/slatedb/issues/328))
- [x] Clones ([#49](https://github.com/slatedb/slatedb/issues/49))
- [ ] Range deletions ([#577](https://github.com/slatedb/slatedb/issues/577))
- [x] Change data capture (CDC) ([#249](https://github.com/slatedb/slatedb/issues/249))
- [x] Database split/merge ([RFC](https://github.com/slatedb/slatedb/blob/main/rfcs/0004-checkpoints.md#manifest-projection-and-union))

## Projects

Check out [CONTRIBUTING.md](CONTRIBUTING.md) for fun (and useful) projects to work on.

## Release Schedule

SlateDB follows Semantic Versioning. We release new versions approximately every 2 months at the end of each even month (February, April, and so on). We guarantee forward/backward compatibility for storage formats between adjacent versions, but we do not currently guarantee API compatibility at this time (we reserve the right to break compile-time API compatibility).

## Adopters

See who's using SlateDB.

- [4og.io](https://4og.io)
- [Dropbox](https://www.dropbox.com)
- [Embucket](https://www.embucket.com)
- [Gadget](https://gadget.dev)
- [Goldsky](https://goldsky.com)
- [HelixDB](https://github.com/HelixDB/helix-db)
- [Malstrom](https://github.com/MalstromDevelopers/malstrom)
- [Massive](https://massive.com)
- [Merklemap](https://merklemap.com)
- [OpenData](https://www.opendata.dev)
- [Prisma](https://www.prisma.io)
- [Responsive](https://responsive.dev)
- [s2-lite](https://github.com/s2-streamstore/s2)
- [Storrito](https://storrito.com)
- [Taquba](https://github.com/micllam/taquba)
- [Tasklet](https://tasklet.ai)
- [Tensorlake](https://www.tensorlake.ai)
- [Triplox](https://github.com/fiv0/triplox)
- [Volga](https://github.com/volga-project/volga)
- [WombatKV](https://github.com/Venkat2811/wombatkv)
- [ZeroFS](https://zerofs.net)
- [LixRay](https://lixray.com)

## Talks

- [SlateDB - Object native LSM tree](https://www.youtube.com/watch?v=KEtVKZMPY0Q) (Almog Gavra, 2026)
- [Internals of SlateDB: An Embedded Key-Value Store Built on Object Storage](https://www.youtube.com/watch?v=gcTRXZeKbNg) (Vignesh Chandramohan, 2025)
- [Internals of SlateDB — by Vignesh Chandramohan](https://www.youtube.com/watch?v=qqF_zFWqFYk) (Vignesh Chandramohan, 2025)
- [Database Internals - SlateDB](https://www.youtube.com/watch?v=wEAcNoJOBFI) (Chris Riccomini, 2024)
- [Building a Cloud Native LSM on Object Storage](https://www.p99conf.io/session/building-a-cloud-native-lsm-on-object-storage/) (Rohan Desai/Chris Riccomini, 2024)

## Infrastructure Sponsors

Thanks to the following companies for donating services and infrastructure to the SlateDB project.

- **[Pulumi](https://www.pulumi.com/)** - Open source platform for automating, securing, and managing cloud resources, configuration, and secrets.
- **[Tigris](https://www.tigrisdata.com/)** – Globally distributed S3-compatible object storage service.

## License

SlateDB is licensed under the Apache License, Version 2.0.

## Foundation

SlateDB is a member of the [Commonhaus Foundation](https://www.commonhaus.org/).

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/commonhaus/artwork/blob/main/foundation/brand/png/CF_logo_horizontal_single_reverse_200px.png?raw=true">
  <img src="https://github.com/commonhaus/artwork/blob/main/foundation/brand/png/CF_logo_horizontal_single_default_200px.png?raw=true">
</picture>
