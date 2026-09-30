# Native point-read cancellation: first baseline

Date: 2026-09-30 UTC. This run proves that the proposed API is absent. It does not prove that an active native read stops.

The source is published fork commit `2b86b808dbc682849074ec022c2ad7be7312f8f8`, tree `9d9893aba22fe6a83195d9841e2cfa1e5a8755ac`. The only added source is `slatedb/tests/point_read_cancellation.rs`, SHA256 `751ee8b38e4f84de53741787aa115841486d062683e0df0eab87b9f7567a506c`. The archive contains 557 files. Cargo.lock is `f8da9e3a3d365476e09898d6aae2a004674cffb83b07af3b498680729fe4c015`.

The worker used Rust 1.91.1 on Linux x86_64. Cargo ran offline with two workers and the `foyer` feature. Its retained private target came from the prior K baseline. The cache census reported 84 absent locked packages. The actual offline Linux build succeeded for the existing controls without fetching them. This does not certify other feature or target graphs.

The finite dispatcher was `/private/tmp/om-fh-state/f/native-read-cancellation/native-point-baseline-k-v1/dispatch-owned.py --root-authorized-slot`. Its manifest is `e33a573070de40fb2deb7d4c2a7dd8032608f6ef67d177bc03a20c1bd836557a`. The local bound was 1200 seconds. The worker bound was 990 seconds, including 90 seconds for cleanup. TERM was 940 seconds and KILL was 990 seconds. No limit was reached.

The commands were:

```sh
cargo test --locked --offline --jobs 2 --target x86_64-unknown-linux-gnu --message-format=json -p slatedb --features foyer --lib tablestore::tests::dedups_concurrent -- --nocapture --test-threads=1
cargo test --locked --offline --jobs 2 --target x86_64-unknown-linux-gnu --message-format=json -p slatedb --features foyer --test point_read_cancellation --no-run
```

The first command passed both existing Foyer tests. They make concurrent callers share the original index or block load. The command took 151.538322862 seconds, including compilation. The two tests took 0.01 seconds. The sampled process-group peak was 3,281,575,936 bytes.

The second command exited 101 in 35.327142656 seconds. It reported exactly five errors in the new fixture. They were two absent `ReadOptions.cancellation_token` fields, two absent `ErrorKind::Cancelled` variants, and one absent `DbReaderSnapshot.get_with_options` method. No unrelated compiler error qualified as an expected result. Its sampled process-group peak was 1,438,445,568 bytes.

The eight new test parents did not execute. Six parents contain 24 held-load cases across Db, DbSnapshot, DbReader and DbReaderSnapshot. Two parents cover warm memory and hybrid-disk cache hits. These later controls must prove that the original loader ends before a cancelled read returns. They must also prove that the same original view remains usable. Missing symbols alone do not prove these behaviors.

The original local supervisor was 7629 with exec session 13352. Its seven children were 7633,7660,7674,7907,7936,9387,9418. The worker owner was 127705 and the two original Cargo processes were 127706 and 129269. Every original process joined and every original group was absent. The exact worker lock and owner inodes were `[2049,1648609]` and `[2049,1648610]`. Release completed at 11:55:26.224952Z. An independent read then found the gate absent.

The worker took 189.352048561 seconds and the full dispatcher took 230.063738833 seconds. The 557 source files, lock, fixture, Rust tools, canonical 044 source, release library 16b51 and four library dependencies were unchanged. OOM-kill count stayed 0. No application checkout, production library, binding, provider, or shared native roll changed.

Evidence is under `/private/tmp/om-fh-state/f/native-read-cancellation/native-point-baseline-k-v1/dispatch-20260930T115141Z-7629`. The outer receipt is `aa57780f24b8eb918391c47ee8ed77f68caca769b23195b5c051fb024e52d515`. The worker receipt is `caf9b8ccd5f847d44298cbdd0daf4aa5b3fff67f90b18b211851c4173b428282`. The raw archive is `efed9619993941710295bc24e61fc767c5ff35d1d5d3ee8c8baf22c04a76fc80`. Both command logs and all transport logs match their receipt hashes.

The application napkin gives 15–30ms for one object GET. Compilation and local Foyer controls are not object-store latency measurements, so no ratio to that rate is claimed. The observed vector query debt remains: a 30-second request took up to 121765.887ms, about 4.06 times its budget. This baseline does not reduce that measured overrun.

The next source cut adds the proposed API shape without a cancellation handler. This preserves a compiling behavioral baseline for the same held-load tests. A separate candidate will keep cache hits, own each cancellable cache miss in the original read, and retain the original call through cancellation. Scans, the Go callback bridge, concurrent-read cost, fork publication and the required shared roll remain separate work. Public erasure stays unsupported.

## Prepared behavior controls

These controls are source only. The original 514 fixture lines remain byte-for-byte unchanged. Six added parents bring the fixture to 14 parents. They cover four concurrent reads, storage-error precedence, and a held cache lookup for memory and hybrid caches. No added parent executed during preparation.

The concurrent controls wait until all four original reads receive a poll. They then observe the selected loader count before release. Legacy reads must share one object GET. Token reads must own four object GETs. Each arm reports elapsed time and GET counts. The next token read must reuse the cache with zero object GETs. This measures a deliberate cost, not performance parity.

The error controls cancel the token in the same object-store poll that returns a named storage error. All four public point surfaces must retain that storage error. Each control then reads the same original snapshot without a token.

The cache controls hold the return from a real Foyer lookup in one retained fixture task. They model Foyer's detached lookup boundary, not a physical disk stall. Cancellation cannot return while that task remains held. After release, the read must return cancellation and the same original snapshot must still work. Every original fixture task joins before assertions. A local disk lookup can delay cancellation until it returns. These controls do not bound that latency.

The API-only source also sets the new token field to None in two existing full literals. One is the legacy UniFFI conversion. The other is a native unit-test literal. Neither change adds a cancellation handler or a wire API.

## Prepared point-read source

The candidate remains unbuilt. It adds a token only to the existing point-read configuration. The original Reader call stays alive until its cache lookups return. Filter, index, and data cache misses use the same token inside their original object futures. Cancellation drops those owned futures before return. An error from the original storage poll takes precedence over concurrent cancellation.

Foyer 0.22.6 `HybridCache.get` calls `memory.get_or_fetch_inner` with a storage spawner in `foyer/src/hybrid/cache.rs:678–720`. Thus even a disk cache hit can own a task outside the waiting future. The candidate does not select cancellation around that whole cache future. It awaits the cache call, then checks the token. No dependency code, cache map, retry policy, or scan ownership changes.

Token reads keep memory and disk cache hits. On a cold miss, each caller loads its own bytes and inserts a valid result through the existing cache. Legacy reads keep the shared-loader path. Read-owned inserts can still feed the cache's existing background flush. The cache owner retains that existing lifetime. The point-read change does not claim to join all cache maintenance.

The prepared source does not add a Go token callback or a new UniFFI method. No library, generated binding, or installed artifact changed. Actual held behavior, deliberate omissions, complete affected tests, source publication, and a separate shared roll still remain.
