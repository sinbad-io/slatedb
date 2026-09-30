# Native point-read cancellation: first baseline

Date: 2026-09-30 UTC. This run proves that the proposed API is absent. It does not prove that an active native read stops.

The source is published fork commit `2b86b808dbc682849074ec022c2ad7be7312f8f8`, tree `9d9893aba22fe6a83195d9841e2cfa1e5a8755ac`. The only added source is `slatedb/tests/point_read_cancellation.rs`, SHA256 `751ee8b38e4f84de53741787aa115841486d062683e0df0eab87b9f7567a506c`. The archive contains 557 files. Cargo.lock is `f8da9e3a3d365476e09898d6aae2a004674cffb83b07af3b498680729fe4c015`.

The worker used Rust1.91.1 on Linux x86_64. Cargo ran offline with two workers and the `foyer` feature. Its retained private target came from the prior K baseline. The cache census reported 84 absent locked packages. The actual offline Linux build succeeded for the existing controls without fetching them. This does not certify other feature or target graphs.

The finite dispatcher was `/private/tmp/om-fh-state/f/native-read-cancellation/native-point-baseline-k-v1/dispatch-owned.py --root-authorized-slot`. Its manifest is `e33a573070de40fb2deb7d4c2a7dd8032608f6ef67d177bc03a20c1bd836557a`. The local bound was1200 seconds. The worker bound was990 seconds, including90 seconds for cleanup. TERM was940 seconds and KILL was990 seconds. No limit was reached.

The commands were:

```sh
cargo test --locked --offline --jobs 2 --target x86_64-unknown-linux-gnu --message-format=json -p slatedb --features foyer --lib tablestore::tests::dedups_concurrent -- --nocapture --test-threads=1
cargo test --locked --offline --jobs 2 --target x86_64-unknown-linux-gnu --message-format=json -p slatedb --features foyer --test point_read_cancellation --no-run
```

The first command passed both existing Foyer tests. They make concurrent callers share the original index or block load. The command took151.538322862 seconds, including compilation. The two tests took0.01 seconds. The sampled process-group peak was3,281,575,936 bytes.

The second command exited101 in35.327142656 seconds. It reported exactly five errors in the new fixture. They were two absent `ReadOptions.cancellation_token` fields, two absent `ErrorKind::Cancelled` variants, and one absent `DbReaderSnapshot.get_with_options` method. No unrelated compiler error qualified as an expected result. Its sampled process-group peak was1,438,445,568 bytes.

The eight new test parents did not execute. Six parents contain24 held-load cases across Db, DbSnapshot, DbReader and DbReaderSnapshot. Two parents cover warm memory and hybrid-disk cache hits. These later controls must prove that the original loader ends before a cancelled read returns. They must also prove that the same original view remains usable. Missing symbols alone do not prove these behaviors.

The original local supervisor was7629 with exec session13352. Its seven children were7633,7660,7674,7907,7936,9387,9418. The worker owner was127705 and the two original Cargo processes were127706 and129269. Every original process joined and every original group was absent. The exact worker lock and owner inodes were `[2049,1648609]` and `[2049,1648610]`. Release completed at11:55:26.224952Z. An independent read then found the gate absent.

The worker took189.352048561 seconds and the full dispatcher took230.063738833 seconds. The 557 source files, lock, fixture, Rust tools, canonical044 source, release library16b51 and four library dependencies were unchanged. OOM-kill count stayed0. No application checkout, production library, binding, provider, or shared native roll changed.

Evidence is under `/private/tmp/om-fh-state/f/native-read-cancellation/native-point-baseline-k-v1/dispatch-20260930T115141Z-7629`. The outer receipt is `aa57780f24b8eb918391c47ee8ed77f68caca769b23195b5c051fb024e52d515`. The worker receipt is `caf9b8ccd5f847d44298cbdd0daf4aa5b3fff67f90b18b211851c4173b428282`. The raw archive is `efed9619993941710295bc24e61fc767c5ff35d1d5d3ee8c8baf22c04a76fc80`. Both command logs and all transport logs match their receipt hashes.

The application napkin gives15–30ms for one object GET. Compilation and local Foyer controls are not object-store latency measurements, so no ratio to that rate is claimed. The observed vector query debt remains: a30-second request took up to121765.887ms, about4.06 times its budget. This baseline does not reduce that measured overrun.

The next source cut adds the proposed API shape without a cancellation handler. This preserves a compiling behavioral baseline for the same held-load tests. A separate candidate will keep cache hits, own each cancellable cache miss in the original read, and retain the original call through cancellation. Scans, the Go callback bridge, concurrent-read cost, fork publication and the required shared roll remain separate work. Public erasure stays unsupported.
