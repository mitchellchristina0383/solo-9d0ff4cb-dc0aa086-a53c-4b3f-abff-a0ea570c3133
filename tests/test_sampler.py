"""End-to-end self tests for the tail-sampling workbench.

The full HTTP stack is exercised on an ephemeral port with an isolated
temporary SQLite database. A controllable fake clock lets us deterministically
walk a window through collecting -> grace -> closed without real sleeping.

Run:  python3 -m unittest -v tests.test_sampler
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

# Isolated mode (-I) puts neither cwd nor PYTHONPATH on sys.path; the test
# file lives in tests/, so add the project root explicitly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.server import AppState, create_server


class FakeClock:
    def __init__(self, start_ms: int):
        self.now = start_ms

    def __call__(self) -> int:
        return self.now

    def advance(self, ms: int) -> None:
        self.now += ms


class WorkbenchHarness:
    def __init__(self):
        self.tmpdir = tempfile.mkdtemp(prefix="tslab-")
        self.db = os.path.join(self.tmpdir, "test.db")
        self.httpd: ThreadingHTTPServer = create_server("127.0.0.1", 0, self.db)
        self.clock = FakeClock(int(time.time() * 1000))
        self.httpd.state.now_ms = self.clock  # type: ignore[attr-defined]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.httpd.state.storage.close()

    def request(self, method: str, path: str, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    # convenience wrappers ------------------------------------------------
    def create_window(self, collect=4000, grace=2000, slow=500,
                      default_quota=3, quotas=None):
        return self.request("POST", "/api/windows", {
            "collect_ms": collect, "grace_ms": grace, "slow_ms": slow,
            "default_quota": default_quota, "quotas": quotas or {},
        })

    def span(self, span_id, trace_id, service, start, duration=100,
             status="ok", operation="op"):
        return self.request("POST", "/api/spans", {
            "span_id": span_id, "trace_id": trace_id, "service": service,
            "operation": operation, "start_ms": start,
            "duration_ms": duration, "status": status,
        })

    def current(self):
        return self.request("GET", "/api/window/current")[1]

    def decisions(self):
        snap = self.current()
        return {t["trace_id"]: t for t in snap["traces"]}

    def finalize(self, wid):
        return self.request("POST", f"/api/windows/{wid}/finalize", {})


class TailSamplingTests(unittest.TestCase):
    def setUp(self):
        self.h = WorkbenchHarness()

    def tearDown(self):
        self.h.stop()

    # ------------------------------------------------------------- 1 health
    def test_01_health_and_empty_state(self):
        status, body = self.h.request("GET", "/api/health")
        self.assertEqual(status, 200)
        self.assertTrue(body["ok"])
        status, snap = self.h.request("GET", "/api/window/current")
        self.assertEqual(status, 200)
        self.assertIsNone(snap["window"])

    # ----------------------------------------------------- 2 out-of-order span
    def test_02_out_of_order_spans_rebuild_complete_trace(self):
        _, snap = self.h.create_window(collect=4000, grace=2000)
        base = snap["window"]["start_ms"]
        # Submit in deliberately shuffled order: s3 -> s1 -> s2.
        self.assertEqual(self.h.span("s3", "T1", "checkout", base + 300, 120)[0], 200)
        self.assertEqual(self.h.span("s1", "T1", "gateway", base, 450)[0], 200)
        self.assertEqual(self.h.span("s2", "T1", "orders", base + 100, 300)[0], 200)
        d = self.h.decisions()["T1"]
        self.assertEqual(d["span_count"], 3)
        # Latency must be computed from the full trace, not arrival order.
        # Root span runs base..base+450, so end-to-end latency is 450ms.
        self.assertEqual(d["latency_ms"], 450)
        self.assertEqual(
            [s["span_id"] for s in d["spans"]], ["s1", "s2", "s3"],
            "span 必须按时间戳重排，而不是按到达顺序展示",
        )
        self.assertEqual(d["decision"], "waiting")

    # ----------------------------------------------- 3 late error flips call
    def test_03_late_error_span_flips_tentative_decision(self):
        _, snap = self.h.create_window(collect=3000, grace=3000, slow=500,
                                       default_quota=1)
        base = snap["window"]["start_ms"]
        # Normal-looking trace plus a competitor that would take the only slot.
        self.h.span("a1", "T-late", "gateway", base + 50, 100)
        self.h.span("a2", "T-late", "auth", base + 80, 60)
        self.h.span("c1", "T-comp", "gateway", base + 200, 50)
        # Collection window ends; enter grace.
        self.h.clock.advance(3100)
        self.assertEqual(self.current_phase(), "grace")
        # Late error span (timestamp inside collection window, arrives in grace).
        code, _ = self.h.span("a3-late", "T-late", "auth", base + 2500, 150,
                              status="error")
        self.assertEqual(code, 200)
        d = self.h.decisions()["T-late"]
        self.assertTrue(d["spans"][-1]["late"])
        self.assertEqual(d["tier"], 0, "迟到错误必须把链路提升为错误优先 tier")
        # End grace: error trace must win the single quota slot.
        self.h.clock.advance(3000)
        self.h.request("GET", "/api/window/current")  # tick auto-finalize
        d = self.h.decisions()
        self.assertEqual(d["T-late"]["decision"], "keep")
        self.assertEqual(d["T-comp"]["decision"], "drop_quota")
        self.assertEqual(
            set(s["trace_id"] for s in
                self.h.current()["exports"]),
            {"T-late"},
        )

    # ------------------------------------------------------ 4 quota hard cap
    def test_04_quota_exhaustion_priority_wins(self):
        _, snap = self.h.create_window(
            collect=4000, grace=1000, slow=400, default_quota=1,
            quotas={"payment": 1},
        )
        base = snap["window"]["start_ms"]
        self.h.span("n1", "T-normal-a", "payment", base + 100, 50)
        self.h.span("n2", "T-normal-b", "payment", base + 200, 50)
        self.h.span("e1", "T-error", "payment", base + 300, 80, status="error")
        self.h.span("t1", "T-timeout", "payment", base + 400, 80,
                    status="timeout")
        self.h.clock.advance(5100)
        self.h.request("GET", "/api/window/current")
        d = self.h.decisions()
        self.assertEqual(d["T-error"]["decision"], "keep",
                         "错误链路优先抢占唯一配额")
        for tid in ("T-normal-a", "T-normal-b", "T-timeout"):
            self.assertEqual(d[tid]["decision"], "drop_quota")
            self.assertIn("配额已耗尽", d[tid]["reason"])
        snap = self.h.current()
        pay = next(q for q in snap["quotas"] if q["service"] == "payment")
        self.assertEqual((pay["used"], pay["quota"]), (1, 1),
                         "最终占用绝不能突破配额")

    # ----------------------------------------------- 5 multi-service atomicity
    def test_05_cross_service_trace_needs_all_quotas(self):
        _, snap = self.h.create_window(
            collect=4000, grace=1000, slow=500, default_quota=5,
            quotas={"gateway": 5, "orders": 5, "inventory": 0},
        )
        base = snap["window"]["start_ms"]
        for sid, svc, off in (("a1", "gateway", 0), ("a2", "orders", 50),
                              ("a3", "inventory", 100)):
            self.h.span(sid, "T-multi-A", svc, base + off, 80)
        self.h.span("b1", "T-multi-B", "gateway", base + 200, 90)
        self.h.span("b2", "T-multi-B", "orders", base + 250, 70)
        self.h.clock.advance(5100)
        self.h.request("GET", "/api/window/current")
        d = self.h.decisions()
        self.assertEqual(d["T-multi-A"]["decision"], "drop_quota")
        self.assertEqual(d["T-multi-A"]["blocked_service"], "inventory")
        self.assertEqual(d["T-multi-B"]["decision"], "keep")
        # No partial export: only the fully eligible trace is exported.
        self.assertEqual(
            {e["trace_id"] for e in self.h.current()["exports"]},
            {"T-multi-B"},
        )

    # --------------------------------------------- 6 restart resumes window
    def test_06_restart_resumes_unfinished_window(self):
        _, snap = self.h.create_window(collect=4000, grace=2000)
        wid = snap["window"]["id"]
        base = snap["window"]["start_ms"]
        self.h.span("r1", "T-r", "gateway", base + 100, 100)
        before = self.h.current()
        self.assertEqual(before["window"]["status"], "open")
        # Simulate process restart: brand new AppState over the same DB file.
        self.h.httpd.state.storage.close()
        self.h.httpd.state = AppState(self.h.db)
        self.h.httpd.state.now_ms = self.h.clock
        after = self.h.current()
        self.assertEqual(after["window"]["id"], wid)
        self.assertEqual(after["window"]["status"], "open")
        self.assertEqual(after["span_count"], 1, "span 必须在重启后从 SQLite 恢复")
        self.assertEqual(after["traces"][0]["trace_id"], "T-r")
        # Close the window after the restart; the decision must still work.
        self.h.clock.advance(6100)
        self.h.request("GET", "/api/window/current")
        self.assertEqual(self.h.current()["window"]["status"], "closed")
        self.assertEqual(self.h.decisions()["T-r"]["decision"], "keep")

    # --------------------------------------------- 7 no duplicate export
    def test_07_finalize_is_idempotent(self):
        _, snap = self.h.create_window(collect=2000, grace=1000,
                                       default_quota=2)
        wid = snap["window"]["id"]
        base = snap["window"]["start_ms"]
        self.h.span("i1", "T-i", "gateway", base + 50, 60)
        self.h.clock.advance(3100)
        self.h.finalize(wid)
        first_exports = list(self.h.current()["exports"])
        self.assertEqual(len(first_exports), 1)
        # Re-finalize must not create a second export row.
        self.h.finalize(wid)
        self.h.request("POST", f"/api/windows/{wid}/finalize", {})
        self.assertEqual(len(self.h.current()["exports"]), 1)

    # --------------------------------------------- 8 closed window rejects span
    def test_08_closed_window_rejects_new_spans(self):
        _, snap = self.h.create_window(collect=1000, grace=1000)
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        self.h.clock.advance(2100)
        self.h.finalize(wid)
        code, body = self.h.span("late1", "T-x", "gateway", base + 50, 10)
        self.assertEqual(code, 409)
        self.assertIn("窗口", body["error"])

    # --------------------------------------------- 9 duplicate span idempotent
    def test_09_duplicate_span_id_is_idempotent(self):
        _, snap = self.h.create_window()
        base = snap["window"]["start_ms"]
        self.h.span("dup1", "T-d", "gateway", base + 10, 10)
        _, body = self.h.span("dup1", "T-d", "gateway", base + 10, 10)
        self.assertTrue(body["duplicate"])
        self.assertEqual(self.h.current()["span_count"], 1)

    # --------------------------------------------- 10 unknown service default
    def test_10_unknown_service_uses_default_quota(self):
        _, snap = self.h.create_window(
            collect=2000, grace=1000, default_quota=1, quotas={"gateway": 5})
        base = snap["window"]["start_ms"]
        self.h.span("u1", "T-u1", "billing", base + 10, 10)
        self.h.span("u2", "T-u2", "billing", base + 20, 10)
        self.h.clock.advance(3100)
        self.h.request("GET", "/api/window/current")
        d = self.h.decisions()
        keeps = [t for t in d.values() if t["decision"] == "keep"]
        self.assertEqual(len(keeps), 1, "未配置配额的服务走默认配额 1")

    # --------------------------------------------- 11 slow trace priority
    def test_11_slow_trace_outranks_normal_trace(self):
        _, snap = self.h.create_window(
            collect=3000, grace=1000, slow=400, default_quota=1)
        base = snap["window"]["start_ms"]
        self.h.span("f1", "T-fast", "gateway", base + 100, 50)
        self.h.span("w1", "T-slow", "gateway", base + 200, 800)  # latency 800
        self.h.clock.advance(4100)
        self.h.request("GET", "/api/window/current")
        d = self.h.decisions()
        self.assertEqual(d["T-slow"]["decision"], "keep")
        self.assertEqual(d["T-fast"]["decision"], "drop_quota")

    # --------------------------------------------- 12 quota update live
    def test_12_quota_update_changes_decision(self):
        _, snap = self.h.create_window(
            collect=3000, grace=1000, default_quota=5,
            quotas={"gateway": 1})
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        self.h.span("q1", "T-q1", "gateway", base + 10, 10)
        self.h.span("q2", "T-q2", "gateway", base + 20, 10)
        code, _ = self.h.request("POST", f"/api/windows/{wid}/quotas",
                                 {"service": "gateway", "quota": 2})
        self.assertEqual(code, 200)
        self.h.clock.advance(4100)
        self.h.request("GET", "/api/window/current")
        d = self.h.decisions()
        self.assertEqual(d["T-q1"]["decision"], "keep")
        self.assertEqual(d["T-q2"]["decision"], "keep",
                         "运行中调大配额后两条链路都应保留")

    # --------------------------------------------- 13 validation errors
    def test_13_input_validation(self):
        self.h.create_window()
        base = self.h.current()["window"]["start_ms"]
        code, body = self.h.request("POST", "/api/spans", {
            "span_id": "bad", "trace_id": "t", "service": "s",
            "operation": "o", "start_ms": base, "duration_ms": -1,
            "status": "ok",
        })
        self.assertEqual(code, 400)
        code, _ = self.h.request("POST", "/api/spans", {
            "span_id": "bad2", "trace_id": "t", "service": "s",
            "operation": "o", "start_ms": base, "duration_ms": 5,
            "status": "weird",
        })
        self.assertEqual(code, 400)
        # No active window after closing.
        self.h.request("POST", "/api/reset", {})
        code, _ = self.h.span("x1", "Tx", "svc",
                              self.h.clock() + 100, 10)
        self.assertEqual(code, 409)

    # --------------------------------------------- 14 demo endpoint
    def test_14_demo_scenario_seeds_window(self):
        code, body = self.h.request("POST", "/api/demo/quota-exhausted", {})
        self.assertEqual(code, 200)
        self.assertIn("window_id", body)
        snap = self.h.current()
        self.assertGreaterEqual(snap["span_count"], 5)
        # Creating a second active window must be rejected.
        code, body = self.h.create_window()
        self.assertEqual(code, 409)

    def current_phase(self):
        return self.h.current()["window"]["phase"]

    # ------------------------------------------- 15 zero-grace boundary span
    def test_15_zero_grace_accepts_span_at_boundary_then_closes(self):
        _, snap = self.h.create_window(collect=2000, grace=0,
                                       slow=500, default_quota=3)
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        self.h.clock.advance(600)
        self.h.span("b1", "T-b", "gateway", base + 500, 10)
        # Exactly at the collection/grace end instant: the span must still
        # be accepted as late before the window is allowed to finalize.
        self.h.clock.advance(1400)
        code, body = self.h.span("b2", "T-b", "gateway", base + 1500, 10)
        self.assertEqual(code, 200)
        self.assertEqual(body["window"]["phase"], "grace")
        self.assertTrue(body["traces"][0]["spans"][-1]["late"])
        # One millisecond later the window is due and auto-finalizes.
        self.h.clock.advance(1)
        self.h.request("GET", "/api/window/current")
        self.assertEqual(self.h.current()["window"]["status"], "closed")
        self.assertEqual(self.h.decisions()["T-b"]["decision"], "keep")


if __name__ == "__main__":
    unittest.main(verbosity=2)
