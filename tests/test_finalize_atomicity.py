"""Regression tests for atomic finalize + old-database recovery.

Bug fixed: finalizing a window used to commit every export row and the
window close in *separate* transactions. A SQLite write failure halfway
left a still-open window with partial export rows behind; those leftovers
were listed as exports and, on retry, occupied quota slots that late
error traces needed. Databases carrying such leftovers kept both problems
forever.

These tests use a fault-injecting connection wrapper to fail the export
batch deterministically, and hand-crafted databases to emulate pre-fix
leftovers.

Run:  python3 -I tests/test_finalize_atomicity.py
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

# Isolated mode (-I) puts neither cwd nor PYTHONPATH on sys.path; the test
# file lives in tests/, so add the project root explicitly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.server import ApiError, AppState, create_server


class FakeClock:
    def __init__(self, start_ms: int):
        self.now = start_ms

    def __call__(self) -> int:
        return self.now

    def advance(self, ms: int) -> None:
        self.now += ms


class FlakyConnection:
    """Delegates to a real sqlite3.Connection but fails export inserts.

    ``fail_after`` = number of export-batch INSERT statements allowed to
    succeed before every further one raises. ``None`` disables injection.
    Failing after 1 success emulates "died halfway through the batch".
    """

    def __init__(self, real: sqlite3.Connection):
        self._real = real
        self.fail_after: int | None = None
        self._inserts = 0

    def execute(self, sql, parameters=()):
        stmt = sql.lstrip().upper()
        if stmt.startswith("INSERT") and "EXPORTS" in stmt:
            if self.fail_after is not None and self._inserts >= self.fail_after:
                raise sqlite3.OperationalError("simulated disk full")
            self._inserts += 1
        return self._real.execute(sql, parameters)

    def __getattr__(self, name):  # commit/rollback/close/... pass through
        return getattr(self._real, name)


def make_state(tmpdir: str, name: str = "t.db"):
    state = AppState(os.path.join(tmpdir, name))
    clock = FakeClock(int(time.time() * 1000))
    state.now_ms = clock
    return state, clock


def arm_failure(state: AppState, fail_after: int | None) -> FlakyConnection:
    conn = state.storage._conn
    if not isinstance(conn, FlakyConnection):
        conn = FlakyConnection(conn)
        state.storage._conn = conn
    conn.fail_after = fail_after
    return conn


def span(state: AppState, span_id, trace_id, service, start, duration=100,
         status="ok"):
    return state.submit_span({
        "span_id": span_id, "trace_id": trace_id, "service": service,
        "operation": "op", "start_ms": start, "duration_ms": duration,
        "status": status,
    })


class FinalizeAtomicityTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="tslab-atomic-")

    # --------------------------------------- 1 write failure rolls back all
    def test_01_failed_finalize_leaves_no_partial_exports_then_retry(self):
        state, clock = make_state(self.tmpdir)
        snap = state.create_window({
            "collect_ms": 4000, "grace_ms": 2000, "slow_ms": 500,
            "default_quota": 5, "quotas": {},
        })
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        for i in range(3):  # three traces, all comfortably kept
            span(state, f"s{i}", f"T-{i}", "gateway", base + i * 100)
        clock.advance(6100)  # past grace

        # Fail after the first export insert: exactly the old "halfway" case.
        arm_failure(state, fail_after=1)
        with self.assertRaises(ApiError) as ctx:
            state.finalize(wid)
        self.assertEqual(ctx.exception.status, 500)
        self.assertIn("导出写入失败", ctx.exception.message)

        # Nothing may have leaked: window open, zero export rows, spans kept.
        self.assertEqual(state.storage.get_window(wid)["status"], "open")
        self.assertEqual(state.storage.get_exports(wid), [])
        self.assertEqual(state.storage.count_window_spans(wid), 3)

        # While the write keeps failing, touching the window retries the
        # finalize and reports the error again instead of corrupting state.
        with self.assertRaises(ApiError):
            state.current_snapshot()
        self.assertEqual(state.storage.get_window(wid)["status"], "open")
        self.assertEqual(state.storage.get_exports(wid), [])

        # Heal the "disk": the next touch finalizes the window atomically.
        arm_failure(state, None)
        snap = state.current_snapshot()
        self.assertEqual(snap["window"]["status"], "closed")
        self.assertEqual(snap["span_count"], 3)
        self.assertEqual({e["trace_id"] for e in snap["exports"]},
                         {"T-0", "T-1", "T-2"})
        # Repeated finalize returns the original result, no duplicates.
        again = state.finalize(wid)
        self.assertEqual(len(again["exports"]), 3)
        self.assertEqual(
            [e["trace_id"] for e in again["exports"]],
            [e["trace_id"] for e in snap["exports"]],
        )

    # -------------------- 2 failure in grace: late span + quota change win
    def test_02_retry_resettles_with_late_span_and_new_quota(self):
        state, clock = make_state(self.tmpdir)
        snap = state.create_window({
            "collect_ms": 2000, "grace_ms": 5000, "slow_ms": 500,
            "default_quota": 1, "quotas": {"payment": 1},
        })
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        span(state, "n1", "T-normal", "payment", base + 100, 50)
        clock.advance(2500)  # inside grace

        # Manual finalize during grace fails on the very first export write.
        arm_failure(state, fail_after=0)
        with self.assertRaises(ApiError):
            state.finalize(wid)
        self.assertEqual(state.storage.get_window(wid)["status"], "open")
        self.assertEqual(state.storage.get_exports(wid), [])

        # Still in grace: a late error span and a quota change both work.
        late = span(state, "e1-late", "T-late-err", "payment", base + 1500,
                    80, status="error")
        self.assertTrue(late["span_accepted"])
        self.assertEqual(late["window"]["phase"], "grace")
        state.set_quota(wid, {"service": "payment", "quota": 2})

        # Retry after healing: settlement uses the spans and quotas in
        # effect NOW — both traces fit under the raised quota.
        arm_failure(state, None)
        snap = state.finalize(wid)
        dec = {t["trace_id"]: t["decision"] for t in snap["traces"]}
        self.assertEqual(dec["T-late-err"], "keep")
        self.assertEqual(dec["T-normal"], "keep")
        self.assertEqual({e["trace_id"] for e in snap["exports"]},
                         {"T-late-err", "T-normal"})

    # ----------------------------- 3 old DB: orphan exports purged on open
    def test_03_old_database_leftover_exports_are_recovered(self):
        db = os.path.join(self.tmpdir, "old.db")
        state, clock = make_state(self.tmpdir, "old.db")
        snap = state.create_window({
            "collect_ms": 2000, "grace_ms": 5000, "slow_ms": 500,
            "default_quota": 1, "quotas": {"payment": 1},
        })
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        span(state, "n1", "T-normal", "payment", base + 100, 50)
        clock.advance(2500)  # grace; a late error trace is still coming

        # Emulate the pre-fix bug by hand: a committed export row for the
        # still-open window (what a halfway-died finalize left behind).
        state.storage._conn.execute(
            "INSERT INTO exports(window_id, trace_id, exported_at_ms, "
            "span_count) VALUES(?,?,?,?)",
            (wid, "T-normal", clock(), 1),
        )
        state.storage._conn.commit()
        state.storage.close()

        # Reopen the old database: the leftover must be recovered away.
        state2 = AppState(db)
        state2.now_ms = clock
        self.assertEqual(state2.storage.recovered_orphan_exports, 1)
        snap = state2.current_snapshot()
        self.assertEqual(snap["window"]["status"], "open")
        self.assertEqual(snap["exports"], [],
                         "残留记录不得再列为导出")
        # T-normal must be an ordinary tentative trace again — not treated
        # as already exported — so its quota slot is up for re-settlement.
        t_normal = next(t for t in snap["traces"] if t["trace_id"] == "T-normal")
        self.assertEqual(t_normal["decision"], "waiting")
        self.assertNotIn("已导出", t_normal["reason"])
        hist = state2.history()
        self.assertEqual(
            next(w["export_count"] for w in hist["windows"] if w["id"] == wid),
            0,
        )

        # The late error span arrives; finalize must re-settle from spans
        # and quotas only — the error trace wins the single payment slot.
        span(state2, "e1-late", "T-late-err", "payment", base + 1500, 80,
             status="error")
        snap = state2.finalize(wid)
        dec = {t["trace_id"]: t["decision"] for t in snap["traces"]}
        self.assertEqual(dec["T-late-err"], "keep")
        self.assertEqual(dec["T-normal"], "drop_quota")
        self.assertEqual({e["trace_id"] for e in snap["exports"]},
                         {"T-late-err"})
        state2.storage.close()

    # --------------------- 4 closed windows are untouched by the recovery
    def test_04_closed_window_exports_survive_reopen_and_refinalize(self):
        db = os.path.join(self.tmpdir, "closed.db")
        state, clock = make_state(self.tmpdir, "closed.db")
        snap = state.create_window({
            "collect_ms": 2000, "grace_ms": 1000, "slow_ms": 500,
            "default_quota": 3, "quotas": {},
        })
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        span(state, "k1", "T-keep", "gateway", base + 50, 60)
        clock.advance(3100)
        first = state.finalize(wid)
        self.assertEqual(len(first["exports"]), 1)
        state.storage.close()

        state2 = AppState(db)
        state2.now_ms = clock
        self.assertEqual(state2.storage.recovered_orphan_exports, 0,
                         "已关闭窗口的导出不是残留，绝不能被清理")
        snap = state2.window_detail(wid)
        self.assertEqual(snap["window"]["status"], "closed")
        self.assertEqual([e["trace_id"] for e in snap["exports"]], ["T-keep"])
        # Repeated finalize returns the original result, still no dupes.
        again = state2.finalize(wid)
        self.assertEqual([e["trace_id"] for e in again["exports"]],
                         ["T-keep"])
        state2.storage.close()


class FinalizeHttpTests(unittest.TestCase):
    """The webpage's "结束并导出" button against a write-failing database."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="tslab-atomic-http-")
        self.httpd = create_server("127.0.0.1", 0,
                                   os.path.join(self.tmpdir, "t.db"))
        self.clock = FakeClock(int(time.time() * 1000))
        self.httpd.state.now_ms = self.clock
        self.thread = threading.Thread(target=self.httpd.serve_forever,
                                       daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.httpd.state.storage.close()

    def request(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data,
                                     method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    def test_05_finalize_button_reports_error_then_retry_succeeds(self):
        st, snap = self.request("POST", "/api/windows", {
            "collect_ms": 2000, "grace_ms": 1000, "slow_ms": 500,
            "default_quota": 3, "quotas": {}})
        self.assertEqual(st, 201)
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        self.request("POST", "/api/spans", {
            "span_id": "h1", "trace_id": "T-h", "service": "gateway",
            "operation": "op", "start_ms": base + 50, "duration_ms": 60,
            "status": "ok"})
        self.clock.advance(3100)

        # Disk "fails": the button gets a 500 with an actionable message.
        arm_failure(self.httpd.state, fail_after=0)
        st, err = self.request("POST", f"/api/windows/{wid}/finalize", {})
        self.assertEqual(st, 500)
        self.assertIn("导出写入失败", err["error"])
        self.assertIn("重试", err["error"])
        # The page keeps showing an open window with zero exports.
        st, snap = self.request("GET", "/api/window/current")
        self.assertEqual(st, 500, "到期自动收尾同样要报错")
        arm_failure(self.httpd.state, None)
        st, snap = self.request("GET", "/api/window/current")
        self.assertEqual(st, 200)
        self.assertEqual(snap["window"]["status"], "closed")
        self.assertEqual([e["trace_id"] for e in snap["exports"]], ["T-h"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
