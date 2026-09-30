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

The next source cut adds the proposed API shape without a cancellation handler. This prepares an API-only behavioral baseline for the same held-load tests. A separate candidate will keep cache hits, own each cancellable cache miss in the original read, and retain the original call through cancellation. Scans, the Go callback bridge, concurrent-read cost, fork publication and the required shared roll remain separate work. Public erasure stays unsupported.

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


## Behavior attempt 1: setup failure

The API-only behavior baseline did not compile. Source 9e79e1b4c96fcbbb9f6fd7dad3cd4d256f407946 added ErrorKind::Cancelled but omitted one exhaustive WAL conversion. The sole compiler error was E0004 at slatedb/src/wal/mod.rs:394:15. No behavior parent executed. No candidate, legacy candidate control, or omission executed. The separate candidate source 3e525c67e7c94713c0d59386cefd626d9993ba2e remains untested.

The exact command was:

```sh
/usr/bin/python3 /private/tmp/om-fh-state/f/native-read-cancellation/native-point-behavior-k-v1/dispatch-owned.py --root-authorized-slot
```

The local supervisor was original 22473 with exec session 64771. Its children were 22478, 22503, 22518, 22842, 22855, 22986 and 23003. The worker owner was 136107 and the sole Cargo original was 136108. Cargo exited 101 after 15.012796686 seconds. Its sampled group peak was 854,028,288 bytes. Every original process joined and every original group was absent.

The exact lock and owner inodes were [2049,3484497] and [2049,3484498]. Release completed at 12:24:08.513721Z. Independent readback found the gate absent. The worker took 17.046976943 seconds. The full dispatcher took 74.186689375 seconds. Both 558-file source inventories, Cargo.lock, tools, canonical044 and the original library with four dependencies stayed unchanged. No OOM event occurred. K returned to the coordinator.

The packet bounds remained 1200 seconds locally and 990 seconds remotely, with 900 seconds for work and 90 seconds for cleanup. No deadline increased. No retry ran. This compiler duration is a setup measurement, not an object-store latency result. The napkin's 15–30ms object GET rate does not describe compilation. The observed 121765.887ms request remains unresolved.

Evidence is under /private/tmp/om-fh-state/f/native-read-cancellation/native-point-behavior-k-v1/dispatch-20260930T122259Z-22473. The outer receipt is b565e764ecfa0399d60f6f80e35bbee0415de60ba9f686e81fc3645910f8b743. The worker receipt is 8952c72ae436d84382ace29c31e9d81c171105d3043a045f67918e8698f9cd66. The raw archive is c73b61e4465f500a54751176f803b699d9bcbf5eea711315180e4773b91ea64b. The compiler log is 441c923cde84672aee9493ddcbd088aa4e056361cc116df1b4f4db6268890d52. All seven transport logs and the original compiler log match their receipt hashes.

The successor adds one explicit Cancelled-to-WalError::InternalError arm. The existing WAL error categories remain unchanged. Cancellation still applies only to point reads. The audit covers every tracked Rust ErrorKind reference. Display already handles Cancelled. CloseReason and UniFFI keep their existing fallback behavior. Neither the source audit nor this fix proves compilation or cancellation behavior.


## Behavior attempt 2: partial timeout

The WAL correction compiled. The v2 baseline was 25d1c363106a769ffa0a089eb39998f1b4180c41. The candidate was 31be743c07f49908a8fefc5a37820bf7320cd6ff. Compilation finished in 41.29 seconds, with no compiler error. The command then printed twelve FAILED parent lines. It did not finish the full test run.

The exact command was:

```sh
/usr/bin/python3 /private/tmp/om-fh-state/f/native-read-cancellation/native-point-behavior-k-v2/dispatch-owned.py --root-authorized-slot
```

The command reached its unchanged 240-second cap during hybrid_point_read_preserves_observed_storage_error. The memory error parent did not start. Normal Rust capture did not flush the twelve failure details before interruption. Thus these parent lines do not prove the required named assertion failures. No isolated parent, candidate, legacy control, or omission ran. The candidate remains untested.

Original Cargo138076 joined after TERM with exit -15. Its duration was 240.045077841 seconds, and its sampled group peak was 1,487,912,960 bytes. The worker owner138075 released at 12:38:25.016775Z. Its lock and owner inodes were [2049,3484497] and [2049,3484498]. Independent readback found the gate absent. Local original26965/session12957 and all seven transport children joined. Their IDs were 26967, 26988, 27001, 27550, 27568, 29391 and 29413.

The worker took 242.057319548 seconds. The full dispatcher took 327.382194166 seconds. Both source inventories, Cargo.lock, Rust tools, canonical044, library16b51 and all four dependencies stayed unchanged. OOM stayed 0. K returned to the coordinator. No retry or deadline extension ran.

The partial log does not contain an object-store latency measurement. Compilation and a fixture timeout do not compare with the napkin's 15–30ms GET rate. The actual 121765.887ms vector request debt remains unresolved.

Evidence is under /private/tmp/om-fh-state/f/native-read-cancellation/native-point-behavior-k-v2/dispatch-20260930T123302Z-26965. The outer receipt is 1def3890fff0b61e4042790b997a6f73537146a0e5f997d3e22e32e98fc29511. The worker receipt is 7f5d26ec08d4b7c99586844f5059d814c8bdf9a2b08065f805d8b13d200d9fa9. The raw archive is 2de98fa82422217e03fe67f842f88c93c072028c455ee81d259dedb9e4154ae5. The command log is 39d2064fbac3fc9e9075814c35ff2e053b7902033c3dc1dd5d41ff516695ec45. All original log hashes match. PARTIAL-RESULT.json lists the twelve printed failures and the missing results.

Source inspection found a fixture setup error. CancelErrorStore returns Generic on every armed range read. The default retry wrapper retries Generic without a limit. The fixture therefore withholds the terminal error that its assertion intends to inspect. The accepted successor sets object_store_max_retries to Some(0) only for the two error-precedence parents. This exposes one returned storage error without changing the production retry policy. The original 514 fixture lines and every behavior assertion remain intact.

## Behavior attempt 3: baseline proof and stale candidate artifact

The v3 baseline completed all fourteen expected assertion failures. Each parent then failed alone at its required assertion. No test was ignored. Source f3ef5d5693e825d7f20a1c5b8e072595c7e4fb15 compiled in 39.25 seconds. Its full command took 50.065385879 seconds, including 10.60 seconds of tests. The sampled group peak was 1,459,838,976 bytes.

The exact command was:

```sh
/usr/bin/python3 /private/tmp/om-fh-state/f/native-read-cancellation/native-point-behavior-k-v3/dispatch-owned.py --root-authorized-slot
```

The candidate stage did not compile its production source. Cargo reported fresh=true for both the slatedb library and point_read_cancellation binary. Both records named the exact output paths from the baseline compile. Cargo finished that stage in 0.43 seconds and ran the existing binary. It printed fourteen failures, and each failed again alone. Those failures do not classify candidate production behavior. Candidate source 96674bce719249457073c245af168de4acdad102 remains unproved. The nine legacy controls and six omissions did not start.

Both source archives were extracted before the baseline compile into the same retained private Cargo target. Their source hashes stayed correct. Archive source timestamps alone did not force a fresh candidate artifact. The successor must invalidate only the slatedb package artifacts in this private target. Cargo supports package-scoped cleanup with -p and an explicit target directory. The dependency cache remains retained. See the [Cargo clean contract](https://doc.rust-lang.org/cargo/commands/cargo-clean.html).

The successor must record fresh compilation from the exact candidate source for each candidate or omission transition. Source hashes alone cannot prove the tested artifact. No production edit, test assertion change, retry change, or longer deadline follows from this failure. The completed baseline remains evidence for a later candidate-only continuation.

Local original34413/session29564 and its seven transport children joined. The child IDs were 34415, 34433, 34449, 34983, 35005, 35768 and 35794. Worker owner141270 retained thirty original Cargo groups. All groups joined and disappeared. Its exact lock and owner inodes were [2049,3484497] and [2049,3484498]. Release completed at 12:52:20.717848Z. Independent readback found no gate.

The worker took 101.205655624 seconds. The full dispatcher took 184.770851750 seconds. Both 558-file source inventories, Cargo.lock, packet inputs, tools, canonical044, library16b51 and four dependencies stayed unchanged. OOM stayed 0. No deadline extension or retry ran. K returned to the coordinator.

Evidence is under /private/tmp/om-fh-state/f/native-read-cancellation/native-point-behavior-k-v3/dispatch-20260930T124921Z-34413. The outer receipt is 3f7b0327022818acf0e82428c9d43184ff8440e7eb5ccde12877579550a505a9. The worker receipt is 326d319dcd49b2af7ed117aeaab568ea7c08b1e75d59352597f741dcf7b0cf57. The raw archive is f421144809314913f82ad0dfbacb36a13f80061bde4cf0d379a2ff3d51c33f59. The baseline log is 1f433e92fbd8195264ca8db39392f5e6a35d6f102ba555cd13f4100c1c8b25f7. The candidate log is 80204e20892f36e03ad929cfffa417de08207521b1d8cbe1a3def85b06fd49d5. All seven local and thirty worker command logs match their receipt hashes.

These local failure timings are not object-store latency measurements. The napkin's 15–30ms GET rate still does not describe compilation or held fixtures. The regional 121765.887ms request debt remains open. No native publication, binding change, scan closure, shared roll, or public erasure follows from this attempt.


## Behavior attempt 4: candidate and omission proof

Candidate 96674bce719249457073c245af168de4acdad102 passed all fourteen parents. All nine original compatibility controls passed. Six separately compiled omissions each failed at the required assertion. No selected test was ignored. The packet retained the prior fourteen baseline failures and fourteen isolated failures. It read and hashed the original receipt and all fifteen baseline logs before work.

The exact command was:

```sh
/usr/bin/python3 /private/tmp/om-fh-state/f/native-read-cancellation/native-point-behavior-k-v4/dispatch-owned.py --root-authorized-slot
```

The manifest was dcf4296cd1dcdc3ee128c76911aa237119fa048ff0266fef5d54e9bdb48eec05. No source, fixture, omission, assertion, retry policy, or deadline changed from v3. Both source archives and inventories stayed pinned. Rust 1.91.1 ran offline with two Cargo jobs and the foyer feature.

Before each source transition, Cargo cleaned only the slatedb package in the original private target. The command was:

```sh
cargo clean --locked --offline -p slatedb --target x86_64-unknown-linux-gnu --target-dir /home/codex/inbox/f-native14-dispatch-20260930T054850Z-88285/target-41fb7b9-rust1.91.1-amd64
```

Each candidate and omission then reported fresh=false for the exact library source and integration test. Every output stayed inside that original target. The receipt records each artifact hash. The initial candidate test executable was 28b0baa43fb22ff1b90d0e039af2d18fc59af0b8e92f69251248ab55b08e6c78. Its rlib was abc7e712c838ed5a7e2ef81271d04926b27b968e26e3601be62c559f9ad1e86b. The first legacy group also compiled its exact unit-test source. The second group used the same recorded artifact. Dependency caches stayed retained.

The main candidate command took 43.663568978 seconds. Cargo reported 42.15 seconds of compilation, followed by 1.34 seconds of tests. Its sampled group peak was 1,484,210,176 bytes. Seven original point-inline controls took 85.214686098 seconds, including compilation. They reported 0.04 seconds of tests. Their sampled peak was 3,380,342,784 bytes. The two original shared-loader controls took 0.630211513 seconds and reported 0.01 seconds of tests.

The fourteen parents include all 24 held object-load cases across four public point surfaces. They also cover warm caches, concurrent read cost, observed storage errors, and original cache-lookup joins. The cache-lookup fixture holds the return from a real Foyer lookup in an original task. It models that ownership boundary, not a physical disk stall. Cancellation waits for this original lookup before return. A real local cache stall can still delay cancellation.

The concurrent arms each started four original readers. Their measured results were:

| Cache | Token | Object GETs | Held loaders | Elapsed microseconds | Next warm GETs |
| --- | --- | ---: | ---: | ---: | ---: |
| Memory | Absent | 1 | 1 | 2532 | 0 |
| Memory | Present | 4 | 4 | 3503 | 0 |
| Hybrid | Absent | 1 | 1 | 1903 | 0 |
| Hybrid | Present | 4 | 4 | 2668 | 0 |

Thus cancellable misses made four times as many object GETs for four concurrent readers. The fixture elapsed ratios were 1.383 and 1.402. These local held-store controls do not measure real object-store latency. Their 1.903–3.503ms batches are below the napkin's 15–30ms rate for one real GET because the fixture uses local storage. They are also not warm single-read latency measurements. No performance-neutral claim follows from them. The regional 121765.887ms request, about 4.06 times its 30-second budget, still needs end-to-end proof.

Each omission compiled and ran one parent. Removing object cancellation, caller-owned loaders, DbReader forwarding, or DbReaderSnapshot forwarding made cancellation wait for fixture release. Removing the cache join returned before the original cache lookup ended. Reversing error precedence masked the named storage error with cancellation. All six original commands exited 101 at these assertions. They had no compiler errors. The exact names, diagnostics, fresh artifact records, and raw logs remain in the receipt.

The local original was 38917 with exec session 18595. Its seven children were 38919, 38935, 38946, 39370, 39389, 42362 and 42379. Worker owner146342 retained sixteen original Cargo groups. Their IDs were 146343, 146356, 146513, 146569, 146587, 146600, 146669, 146682, 146761, 146774, 146844, 146857, 146938, 146951, 147020 and 147033. Every original joined and every group was absent.

The exact lock and owner inodes were [2049,3484497] and [2049,3484498]. Release completed at 13:07:07.518141Z. Independent terminal readback found the gate absent. The worker took 397.419603624 seconds. The full dispatcher took 471.523919834 seconds. Bounds remained 1200 seconds locally and 990 remotely, including 90 seconds for cleanup. No extension or retry ran.

Both source inventories, packet inputs, Cargo.lock, Rust tools, canonical044, library16b51 and four dependencies stayed unchanged. OOM stayed 0. All twenty-three command logs matched their receipt hashes. K returned to the coordinator. A local evidence-reader initially used logs.stdout for the archive and failed before adjudication. It then used the recorded logs.bin path and completed every hash comparison. This did not rerun a remote command or change the original evidence.

Evidence is under /private/tmp/om-fh-state/f/native-read-cancellation/native-point-behavior-k-v4/dispatch-20260930T125921Z-38917. The outer receipt is 6330905a0146518e39549eecdc59a9e866385d281808880b49211d1dfea33929. The worker receipt is 1c4999a752ddd275e729095fb720d7df16c5fa66526c0d322b7fe6d2d7342bfa. The raw archive is 00bfe10e14f9a66f591124fd25eaf1e6b1c2129d9a059aafd206c1ae9553fc8b. RESULTS.json records the separate candidate proof and retained baseline proof.

This is the first passing native point behavior proof. Complete affected native tests, the Go token bridge, scan closure, library generation, fork publication, and the required shared roll remain separate. The old compiler failure, fixture timeout, and stale-artifact attempt remain recorded above. No public erasure or regional deadline acceptance follows from these results.

## Remaining point-window boundary baseline

Test-only native source `564ec174a114f455224346732b4091cac542576c` kept
production `96674bce719249457073c245af168de4acdad102` unchanged. Packet
`4af068b43f249dd31cc6ff146129c8604c33461c49cb9d35910b00fb2a34cdf9` selected
only the sixteen new `multisource_boundary_` parents. The ten earlier lookup
failures and their isolated receipts were retained rather than rerun.

The first authorized transport attempt, original 98922/session 78688, passed
its exact K identity and free-gate check, then its second SSH connection timed
out before directory creation. No upload, remote gate, compiler or test ran.
Both originals joined; receipt
`5dcb7a59e6debb8f0360e8fb474bc6d1a0c1bbf7c3e03d77da158113e4609abb` remains
a transport-only failure. The separately authorized exact three-source SSH
repair was verified before the next named baseline grant. No test assertion
or deadline was changed to address transport.

The completed command was:

```sh
/usr/bin/python3 /private/tmp/om-fh-state/f/native-read-cancellation/multisource-boundary-baseline-k-v1/dispatch-owned.py --root-authorized-slot
```

All sixteen parents compiled. Four merge-rejection cases and four selected
storage-error cases failed at their required original-cache-task join
assertions. Each failed again alone with the same diagnostic. Four unused
sibling-error cases preserved the selected value/tombstone; four ready-cache
queued-source cases passed without opening sources outside the original
window. No selected case was ignored. These distinguish physically started
fixture tasks from queued-but-unpolled work; the fixture models the delivery
boundary around actual Foyer calls, not a real disk stall.

The original family command took 41.199411 seconds, with 39.91 seconds reported
for compilation and 1.05 seconds for the tests. Peak sampled group RSS was
1,513,418,752 bytes. The eight isolated commands each took 0.624709–0.632518
seconds including Cargo startup. These local compile/held-fixture times are
not latency measurements against the napkin 15–30ms object GET rate. The
regional 121765.887ms request-budget overrun remains separate and unresolved.

Local original 39720/session 40352 and all seven children joined. Worker owner
166258 retained ten original Cargo groups; all joined and disappeared. The
exact lock/owner inodes `[2049,3484497]` and `[2049,3484498]` released at
17:32:12.078016Z after55.174651079 seconds. The local dispatcher took
99.151986458 seconds. Limits remained 900 seconds local and 660 remote,
including 90 seconds for cleanup. Independent readback found no worker gate.

All 558 archived source files, Cargo.lock, Rust 1.91.1 tools, canonical044
library 16b51 and its four dependencies stayed unchanged. OOM stayed 0. The
initial library/integration artifacts were `fresh=false`; every isolated run
used the same actual artifact hashes. All local/remote log hashes matched.
No source, library, binding, Scan behavior, shared roll or erasure activation
changed in this baseline. Candidate17e7451 remained a separate continuation.

Evidence: `/private/tmp/om-fh-state/f/native-read-cancellation/multisource-boundary-baseline-k-v1/dispatch-20260930T173038Z-39720`.
Outer receipt: `4111c23184e5ad6d5181b75e8bdc1acceea20b44d110492dc052d5424c2db3b8`.
Worker receipt: `7211a36dbd2e302bd01e361f6fc6f36a1af7df5292a21029c38bfd8453143163`.
Archive: `72fa7f0e94404e0d17d39adb951f9bc01f66c9c928f134e066f08dd82fa58f8b`.

## Original point window candidate

Candidate `17e7451c5c57abd3a23545be1894685ea78c0fc4` passed all forty point
parents, nine existing GetIterator nodes, seven original inline controls and
two original dedup controls. All 58 nodes passed; none failed or skipped.
Four separately compiled omissions failed at their exact named assertions.
This closes the tested original lookahead-task lifetime gap for point reads.
It does not establish Scan cancellation, a Go bridge, or a regional deadline.

The packet first rechecked the accepted sixteen-parent boundary receipt and
all nine original family/isolated logs, with the same source and native pins.
It retained the earlier ten-parent baseline independently. The exact command
was:

```sh
/usr/bin/python3 /private/tmp/om-fh-state/f/native-read-cancellation/multisource-window-candidate-k-prepared/dispatch-owned.py --root-authorized-slot
```

Manifest: `3d688b3a2f0250f3bf219c7e02e770ef0c8f4eeaebf591ad12ec2840adc69f55`.
Rust 1.91.1 ran locked/offline with 2 Cargo jobs and the foyer feature. Limits
remained 1200s local and 990s remote, including 90s cleanup. Every source
transition invalidated only slatedb artifacts in the same private target.
The candidate library and integration executable each reported `fresh=false`
from the exact candidate source; their actual output hashes are retained.
The first lib-test build was also fresh, and later lib selections used that
same executable. Dependency caches and installed native files were preserved.

The point command took 42.041885s, reporting 37.95s compilation and 3.88s tests,
with sampled group RSS 1,476,132,864 bytes. The GetIterator command took
73.934148s including compilation, with 0.01s tests and peak 3,461,054,464 bytes.
The inline and dedup commands took 1.049121s and 0.630701s respectively. The
GetIterator census comprises five single tests plus four existing combinations
of two booleans; a preparation-only function-count assertion was corrected
before execution without changing a fixture or runtime assertion.

Each omitted join was rebuilt separately. Removing hit joining, selected-error
joining or merge-window joining returned before the held older original
lookup joined. Reusing the caller token let unused-child cancellation escape
to that original token. Each omission compiled, exited 101 and produced its
required assertion; none was a compiler/setup failure. Each tested source
mutation was restored after its original process joined, then the complete
candidate inventory was rechecked.

The concurrent fixture retained these raw observations:

| Cache | Token | Object GETs | Held loaders | Elapsed microseconds | Next warm GETs |
| --- | --- | ---: | ---: | ---: | ---: |
| Memory | Absent | 1 | 1 | 2159 | 0 |
| Memory | Present | 4 | 4 | 1909 | 0 |
| Hybrid | Absent | 1 | 1 | 2380 | 0 |
| Hybrid | Present | 4 | 4 | 3408 | 0 |

Four concurrent token readers still issue 4 GETs versus the legacy 1 GET. These
1.909–3.408ms local held-store batches are below the napkin 15–30ms rate for
a real object GET because the fixture is local. They do not show production
latency neutrality or improved latency; the memory ordering is fixture noise.
The held-cache control uses an original task around actual Foyer calls, not
an actual physical disk stall. Cancellation still waits for an original
cache lookup to finish. The observed 121765.887ms regional request, about 4.06
times its 30s budget, remains an end-to-end acceptance debt.

Local original 66913/session 42193 and seven original children joined. Worker
167159 retained thirteen Cargo process groups, all joined/gone. It released
its exact owner/inodes at 17:46:00.433274Z after 282.198959370s; the local
dispatcher took 336.833596542s. Independent readback found the gate absent.
All source inventories, Rust tools, canonical044/library 16b51 and four
dependencies matched before/after; OOM stayed 0. Every local and remote
command log matched its recorded hash. No retry or deadline extension ran.

Evidence: `/private/tmp/om-fh-state/f/native-read-cancellation/multisource-window-candidate-k-prepared/dispatch-20260930T174031Z-66913`.
Outer receipt: `ecb99230949abbc2f1583e6dc436eaa4e5694770562efcb94e4cbbc577b11f22`.
Worker receipt: `1dd1495059beb8de085c2a13956534361c4daa34f89a130cc29377716224bf1c`.
Raw archive: `e0bc31c5ff833ec5128bed4f376cee85cd752a4a3974ffea6057229062ad0b1b`.
Independent summary: `54cb81c126679d3000a7e943a47a0ca549c408359a70a3989dfdb8c922db077c`.

The next point prerequisites are a private library/generated binding with
actual Go callback/reader error controls, full affected verification, fork
publication and the separately locked shared roll. None ran in this packet.
Scan lifetime and protected erasure/material receipt completion stay separate.

## Private point binding and Go bridge, 18:16Z

The original-window native core remains tested at `17e7451c5c57abd3a23545be1894685ea78c0fc4`, documented at `38ac27dcdb9523d1c90b5310a80170dba6a8886f`: 58 named passes and four independently compiling intended omission failures. Cancellable concurrent reads made four modeled GETs versus one for the legacy path; this cost remains, and neither local timing nor modeled hybrid-cache holds prove physical disk or regional object-store latency. Against `docs/napkin.md`, these are correctness and compile observations, not a new regional performance gate.

The private binding source `5b4966722f99709d1be85e13d990d8945281150e` applies only the previously reviewed token field/conversion, original ReaderSnapshot delegate, Debug implementation and appended typed Cancelled variant, plus the already-red Go fixture. The core and Cargo.lock remain byte-identical to the accepted native core. The binding uses the existing token and original call, not a detached read goroutine or another owner registry.

An initial core-only library build at 17e succeeded, but generation correctly produced the old, identical bindings; the reused verifier returned status1 because it expected a changed binding. No Go behavior ran. Original generation21164 and all four helpers joined and its exact local gate released. Receipt `72606b4059576119161106309d86ad196065246d1ae75acd6a938a6cee869088` is preserved. This exposed that the reviewed binding patch was still scratch-only. It is not a native behavioral failure.

After applying that patch, K built a genuine debug library using Rust1.91.1, locked/offline Cargo, two jobs and the existing private target. Only slatedb and slatedb-uniffi package artifacts were invalidated; both emitted artifacts had fresh=false and exact source paths/hashes. Canonical installed044 and its16b51 release library were unchanged. Cargo170717 took45.070998s with sampled process-group peak1,775,198,208B; remote170703 completed50.150368s and exact-released18:03:44.950084Z. All seven local and three remote originals joined. Raw receipt `889242e249952d7463c123d1794afb21cf630680393eec9771212265594d1ada`, outer `4e05af75199a5947f4d0e14d1192eaddc3ed640d539fceb9ca80e96fc95ce37a`, immutable archive `d85a7acd2e6cfb4d9c3c4f6da325d0d371e4969975d6059b1e1618091bcbd56e`.

The library is `5c23cdf5c08199a1d03682401b3e2df7b061c82fdfddcabc2c59be9aaa0fe6d1`,270,548,000bytes. All four runtime dependency hashes match the original044 environment. The exact retained binary is in the private K attempt, not installed over canonical native paths. Build command:

```sh
cargo clean --locked --offline -p slatedb -p slatedb-uniffi --target x86_64-unknown-linux-gnu --target-dir "$PRIVATE_TARGET"
cargo build --locked --offline --jobs 2 --target x86_64-unknown-linux-gnu --message-format=json -p slatedb-uniffi --lib
```

Local generation reused the proven7020d7006ac3fbbe248f82657ff93f8270f99ae8 tool, version0.7.0+v0.31.0, binary457e6a26a3e6852c88ff22ca89471c9a4eec96501e6e696fe1bb1a9daf416d6c. It read those exact collected Linux library bytes, used the unchanged async-error workaround and gofmt, and performed no compiler build. Original47403/four helpers completed in3.892617s with exact local release. Receipt `2b439ee7402fc946c2cc2012f3a71e4cff003a918f4efcc47432cd89f5a3328f`. Generated Go is `5cb39d045492fe02b30b1ff5e5a6e45f0bd1c7c5630ca5f269b459f4696b3442`; header `95ba0f723396c8d15909fcf55bd7f529558574d8c52f0e149c8f5bf928e56821`. The header's generated trailing whitespace is preserved byte-for-byte rather than hand-edited.

The first Go candidate reached zero tests: UniFFI represents optional Arc<CancellationToken> as **CancellationToken, but the prepared helper supplied a single pointer. The exact compiler diagnostic is preserved in raw `dbe442573851723f3cde4fe5372addab0b2c1276a4f86a95a09679a202389fdf`; original171271/all helpers joined and released18:12:24Z. The successor changes only optional-token shape: a real token supplies &token, while the fixture's absent token stays nil. Assertions were not relaxed and the native library was not rebuilt.

Actual candidate source: application `2d64afc9c4b801487fc97ea56dcebe57890f0017`; generated Go/fixture source `02ff5c6c41788fa2935f70b9570581d246a01e7d`. The three binding parents cover Db, DbSnapshot, DbReader and DbReaderSnapshot: pre-cancel avoids Decode; cancellation while the original Decode is held joins before return; original decoder errors survive cancellation; every case reads again through the same view. The app parents exercise the four real Get paths, with retained original callbacks; three separate callback controls cover stopped, running and completed AfterFunc lifetimes.

| Actual command group | Parents | Pass nodes | Fail | Skip |
| --- | ---: | ---: | ---: | ---: |
| Ordinary binding | 3 | 15 | 0 | 0 |
| Ordinary app/callback | 5 | 13 | 0 | 0 |
| Race binding | 3 | 15 | 0 | 0 |
| Race app/callback | 5 | 13 | 0 | 0 |

Both complete selected-package vets returned0. Go1.27.1, GOMAXPROCS2, `-p 2`, CGO1, module downloads disabled; every original test binary's loader trace named only the retained5c23 library. Commands use exact test patterns retained in the immutable receipt:

```sh
go test -p 2 -count=1 -json -timeout=90s -exec="$CHECKED_EXEC" -run "$EXACT_BINDING_OR_APP_CONTRACT" .
go test -p 2 -count=1 -json -timeout=90s -exec="$CHECKED_EXEC" -race -run "$EXACT_BINDING_OR_APP_CONTRACT" .
go vet -p 2 .
```

Original local60289 and remote171824, seven originals each, all joined with logs and loader hashes independently rechecked. Remote61.957617s/local96.308240s; exact K release18:16:13.562422Z. Source/archive/workspace/dependencies/installed044 remained unchanged. Raw `facd671853894778e6081427ee84db527f3064d3848c3b5e25ea569ebdf3f96d`, outer `413858b1df5361d926866884dcae9a039855a34d3035a8f5217acc090a1af067`, archive `a56ac36809d2a9616773c5ca74022d2bed9fef4f65ba43f32c41034ad672dfee`. Packet `f/native-read-cancellation/point-go-candidate-k-v2`, manifest477a37ff0c3ced67e3f40b386103604de0745c9f160cff22599de013565d5c76; RESULTS.json retains all exact local paths and counts.

This is focused point binding/callback evidence. Complete affected Go/native suites, explicit binding omission controls, iterator Scan/Seek/Close cancellation, publication and the required shared checkout roll remain separate. No dependent merge or U-j/public erasure activation follows from these results. The isolated application branch predates K's d2a26ab80 actual-holder acknowledgement fix; any later coherent union must preserve that fix rather than revive identity-only retirement.
