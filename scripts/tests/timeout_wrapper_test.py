#!/usr/bin/env python3
"""Linux controls for the original Nextest test group. No Cargo build runs here."""
import ctypes
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest


def identity(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    except FileNotFoundError:
        return None
    return {"pid": pid, "state": fields[0], "ppid": int(fields[1]),
            "pgid": int(fields[2]), "start": int(fields[19])}


def held(role, record, ready):
    row = identity(os.getpid())
    row["role"] = role
    fd = os.open(record, os.O_WRONLY | os.O_APPEND)
    try:
        os.write(fd, (json.dumps(row) + "\n").encode())
    finally:
        os.close(fd)
    if role == "test":
        subprocess.Popen([sys.executable, __file__, "--held", "descendant", record, ready])
    else:
        Path(ready).write_text("ready\n")
    while True:
        signal.pause()


class TimeoutWrapper(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if sys.platform != "linux":
            raise RuntimeError("These controls require Linux process identities and adoption.")
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(36, 1, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")
        cls.wrapper = Path(os.environ.get("SLATEDB_TIMEOUT_WRAPPER",
                           Path(__file__).resolve().parents[1] / "backtrace-on-timeout.sh")).resolve(strict=True)
        cls.deadline = time.monotonic() + 90

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="slatedb-timeout-test-")
        self.root = Path(self.tmp.name)
        self.record = self.root / "pids.jsonl"
        self.record.touch()
        self.ready = self.root / "ready"
        self.children = []
        self.env = dict(os.environ)
        bindir = self.root / "bin"
        bindir.mkdir()
        sudo = bindir / "sudo"
        sudo.write_text("#!/bin/sh\nprintf 'sudo is unavailable\\n' >&2\nexit 1\n")
        sudo.chmod(0o755)
        self.env["PATH"] = str(bindir) + ":" + self.env.get("PATH", "")

    def rows(self):
        return [json.loads(line) for line in self.record.read_text().splitlines()]

    def wait_until(self, predicate, seconds):
        end = min(self.deadline, time.monotonic() + seconds)
        while time.monotonic() < end:
            if predicate():
                return True
            time.sleep(0.01)
        return predicate()

    def spawn(self, args):
        log = (self.root / f"command-{len(self.children)}.log").open("wb")
        p = subprocess.Popen(["/bin/bash", str(self.wrapper), *args], env=self.env,
                             stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                             start_new_session=True)
        self.children.append((p, log))
        return p

    def reap_adopted(self):
        for row in self.rows():
            if row["pid"] in [p.pid for p, _ in self.children]:
                continue
            try:
                os.waitpid(row["pid"], os.WNOHANG)
            except ChildProcessError:
                pass

    def absent(self):
        self.reap_adopted()
        return all(identity(row["pid"]) is None for row in self.rows())

    def tearDown(self):
        # Every signal targets an original identity from this fixture.
        for row in self.rows():
            now = identity(row["pid"])
            if now is not None:
                if now["start"] != row["start"]:
                    raise AssertionError("A fixture PID changed identity before cleanup.")
                os.kill(row["pid"], signal.SIGKILL)
        for p, log in self.children:
            if p.poll() is None:
                os.killpg(p.pid, signal.SIGKILL)
            p.wait(timeout=max(0.1, min(5, self.deadline - time.monotonic())))
            log.close()
        if not self.wait_until(self.absent, 5):
            raise AssertionError("An original fixture child remains after cleanup.")
        remaining = Path(f"/proc/{os.getpid()}/task/{os.getpid()}/children").read_text().strip()
        if remaining:
            raise AssertionError("The fixture still owns children: " + remaining)
        self.tmp.cleanup()

    def test_success_keeps_exit_status(self):
        p = self.spawn([sys.executable, "-c", "raise SystemExit(0)"])
        self.assertEqual(p.wait(timeout=5), 0)

    def test_failure_keeps_exit_status(self):
        p = self.spawn([sys.executable, "-c", "raise SystemExit(7)"])
        self.assertEqual(p.wait(timeout=5), 7)

    def assert_timeout_group(self, sig):
        p = self.spawn([sys.executable, __file__, "--held", "test", str(self.record), str(self.ready)])
        self.assertTrue(self.wait_until(lambda: self.ready.exists(), 5), "The original descendant did not start.")
        rows = self.rows()
        self.assertEqual({r["role"] for r in rows}, {"test", "descendant"})
        self.assertEqual(len(rows), 2)
        os.killpg(p.pid, sig)
        p.wait(timeout=5)
        absent = self.wait_until(self.absent, 1)
        print(json.dumps({"signal": int(sig), "wrapper_pid": p.pid,
                          "originals": rows, "after": [identity(r["pid"]) for r in rows]}, sort_keys=True), flush=True)
        self.assertTrue(absent, "The original test or descendant survived the Nextest group signal.")
        self.assertTrue(all(r["pgid"] == p.pid for r in rows), "The wrapper created another test group.")
        self.assertEqual(next(r["pid"] for r in rows if r["role"] == "test"), p.pid)
        self.assertEqual(p.returncode, -sig)

    def test_term_reaches_original_test_and_descendant(self):
        self.assert_timeout_group(signal.SIGTERM)

    def test_kill_reaches_original_test_and_descendant(self):
        self.assert_timeout_group(signal.SIGKILL)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--held":
        held(*sys.argv[2:])
    else:
        unittest.main(verbosity=2)
