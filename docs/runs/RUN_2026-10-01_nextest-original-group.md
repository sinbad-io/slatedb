# Nextest test group controls, 2026-10-01

This is a source preparation record. All four Python controls are UNRUN. No Linux process, compiler, native test, or worker ran for this record.

The base is published Scan source `9f2fbca81ccb91259ea5b1e5e267579e22b9faa2`. Nextest is pinned to `0.9.98`. Its `ci` and `dst` profiles remain unchanged. Their test timeouts remain 120 seconds and 1,800 seconds.

The old wrapper enables Bash job control. The test then runs in another process group. Its timeout trap calls `sudo gdb` under `set -e` before cleanup. The earlier worker had no sudo grant. A debugger failure can bypass the kill and wait.

The tests-first source has four controls. Two retain success and failure exit codes. Two hold the original test and its descendant, then send TERM or KILL to the group that Nextest owns. The selected group must contain both processes. The script must keep the original test PID.

The Linux fixture uses child adoption only to collect its own failed-baseline orphans. It records their PID and start identity before each signal. Cleanup joins the original wrapper, kills only recorded children, and reaps adopted children. The fixture refuses other operating systems. No skipped control counts as a pass.

The proposed source repair replaces the wrapper body with `exec "$@"`. It removes the extra process group and the sudo dependency. It also removes timeout backtraces. That diagnostic loss remains open. A missing backtrace cannot turn a timeout into a pass.

The future command is:

```sh
SLATEDB_TIMEOUT_WRAPPER=/exact/source/scripts/backtrace-on-timeout.sh \
  /usr/bin/python3 /exact/tests/scripts/tests/timeout_wrapper_test.py
```

The complete four-control command has a 90-second fixture deadline and a 120-second supervisor allowance. The published old wrapper must first produce the two group failures. Both exit-code controls must pass. Each failed member must then fail alone with the same assertion. Only then can the repaired wrapper run all four controls.

This models the process-group operation from the pinned Nextest source. It does not execute Nextest itself. The later native gate must retain actual Nextest processes and prove that no original test group or descendant remains. The outer command group alone cannot prove that. The gate remains held if that evidence is missing.

The full workspace, documentation tests, and DST commands remain required. There is no test filter change or retry. There is no shared native roll. The Scan Rust source, generated Go, header, and library remain unchanged.

This record contains no latency, throughput, or storage measurement. The napkin's object-store latency rates do not apply to a process-group control. The previous Scan cost and real-S3 limits remain unchanged.

Astra reflection: a wrapper added for diagnostics created an ownership gap. The existing Nextest group already provides the needed lifetime. Removing the extra owner is the narrow repair. Actual Linux evidence still decides whether the repair closes the gap.
