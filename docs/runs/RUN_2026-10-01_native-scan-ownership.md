# Native Scan ownership and generated binding, 2026-10-01

The native Scan source completed the admitted library and selected target scopes. The Go adapter also completed its full module scope in a separate application.

## Exact source and artifacts

The tested Rust source is `0b0554ecb6da046d91119a4769e4db4c70f64116`, tree `49e831ee752c7527ee49af721a5d518902d5d2ad`. It extends published point source `ff1e3b6795f812f2f563c9da48d36a4ee66469bb`. This successor changes no Rust implementation, test, Cargo manifest, lockfile, or toolchain input. It adds the two actual generated binding files and this report.

Original 123803 built Linux debug library `9bd5c3af8988d8461b3f1321088efd4622f0679d5e67596196a2c37bb708aa83`, 271,503,712 bytes. Rust 1.91.1 and the unchanged Cargo lock `f8da9e3a3d365476e09898d6aae2a004674cffb83b07af3b498680729fe4c015` supplied that build. This is not a Darwin library.

Original 90711 used the existing generator at `7020d7006ac3fbbe248f82657ff93f8270f99ae8` against those library bytes. The output hashes are:

| File | SHA256 |
| --- | --- |
| `bindings/go/uniffi/slatedb.go` | `a6938dd9d32a027d2ef9c2808fb7a96bf9149ba00bbb311dff0ce86159182d04` |
| `bindings/go/uniffi/slatedb.h` | `353aafe6e9ef71806b7a58d5a919182f4470fe3c96ffce0c54fdaa2866ecbb2b` |

Generated whitespace stays unchanged. No compiler or generator ran during source preparation for this successor.

## Actual scopes and original failures

The candidate library reported 2,193 passes and one declared ignore, with 2,194 exact compiled names. Selected point 44, model 1 and binding 83 scopes also passed. The combined point regression population remains 58. The final four ownership omissions each failed at their intended assertion. Eighteen historical controls retained 13 expected reds and 5 positives.

The earlier failures retain their original status. Original 95949 stopped on incorrect predictions about historical behavior. Original 104353 then stopped because the parser omitted 22 literal ` - should panic` terminals. Original 119359 showed that one caller-line omission was not decisive because another cleanup route still joined the owner. Original 121552 corrected the omission at the actual pending-window owner. It carried completed scopes without replay.

| Original | Status | Receipt SHA256 |
| --- | --- | --- |
| baseline-95949 | 1 | `2865818bc36c72e593adaaa7a5fe2b60999a0bb3a2863016f7354feed2ad24bc` |
| continuation-104353 | 1 | `b651331293589b0ff47537570da3a7bf2c2db13bc877f1e68b67aab1d3eb6415` |
| parser-119359 | 1 | `0ecd70b1cff3b05ae0ef8ff358878006027365c211161da69bf5195f61a8b3ff` |
| owner-121552 | 0 | `dbbf25696f46fea964368a86de569fbbb9719e01da7ed3bfa87366f3d3fc54e8` |
| library-123803 | 0 | `3aabfa739a43165d20a5e18f649a3f7a768a0e22b61bbc8c1794b033bb63b1ff` |
| generation-90711 | 0 | `981f3400e47b6a033fe792643e3c1ecd00d3ad4098b2992cee1d9a629ce2f446` |

[The raw archive](evidence/native-scan-20261001/raw.tar.gz), SHA256 `4051a958a598d84359b417d1965f101ff5633e5bc6e1eafd9f302bc3fc8d0d58`, retains these receipts and command logs. Its `commands.json` records the exact commands and working directories. Each receipt records its environment, source pins, original process groups and release. No original failed receipt is replaced by a later result.

The separate application run used Go 1.27.1 and this library/binding pair. Original 146643 completed 297 exact nodes in each ordinary and race mode: 286 passes, ten capability exclusions and one helper skip. Both Slate and binding vets passed. Its application source is `a836ae0a4372798555b7f892978736d8b97786be`. Receipt `36fa1f04e142aa315499ab174906df0a04db398c945673cf5ff650aa989b27c6` records that bounded module result.

## Cost and napkin bracket

The fixture ran 16 windows and 64 original Scan calls. Each window used four concurrent readers and compared every returned row. Its fields are hybrid, tokenized, descending, cold data, ranged GET count, observed held loaders and elapsed microseconds:

```json
[["true", "false", "false", "true", "64", "1", "24362"], ["true", "false", "false", "false", "0", "0", "30881"], ["true", "false", "true", "true", "64", "1", "26119"], ["true", "false", "true", "false", "0", "0", "32593"], ["true", "true", "false", "true", "256", "4", "48609"], ["true", "true", "false", "false", "0", "0", "31732"], ["true", "true", "true", "true", "256", "4", "48028"], ["true", "true", "true", "false", "0", "0", "34681"], ["false", "false", "false", "true", "64", "1", "22505"], ["false", "false", "false", "false", "0", "0", "15745"], ["false", "false", "true", "true", "64", "1", "25006"], ["false", "false", "true", "false", "0", "0", "17537"], ["false", "true", "false", "true", "67", "4", "25659"], ["false", "true", "false", "false", "0", "0", "17684"], ["false", "true", "true", "true", "67", "4", "27959"], ["false", "true", "true", "false", "0", "0", "19657"]]
```

Cold hybrid reads made 64 GETs without tokens and 256 with tokens. Cold memory-cache reads made 64 GETs without tokens and 67 with tokens. Warm windows made no GETs. Metadata stays warm in both windows. Cold means that data blocks are absent. Hybrid warm flushes the cache without forcing eviction. The measurements use memory stores and modeled cache work. They do not measure S3 request latency or a physical disk stall.

The application napkin gives 15–30ms for an S3 GET and 4µs for a warm native point read. These fixture times measure complete concurrent windows. They cannot be compared as per-call latency. For scale,192 serial S3 GETs represent 2.88–5.76s at that base rate. Concurrency and actual storage latency remain unmeasured. The point path keeps its separate4-versus1 GET result. No unchanged-cost claim follows.

## Astra reflection and remaining gates

An iterator owns its started work until that work joins. Cancellation retires queued work without starting it. Constructor failure, Seek replacement, buffered delivery and final Close need distinct controls. A selected storage error must survive later cancellation. A held cache lookup can delay completion beyond a request budget.

The false omission result exposed an instrument problem. Deleting a caller cleanup left another cleanup route intact. The final control omits the pending-window owner itself. This explains the old PASS without changing production code or weakening its assertions.

Complete native workspace and deterministic simulation tests remain open. The private module proof does not replace them. The shared Darwin Scan build and binding roll also remain open. The point15 source/library remains the rollback baseline. Real-S3/K7, F3 reader-under-kill, the repository gate and physical erasure remain separate requirements. Source publication alone does not admit a dependent merge.
