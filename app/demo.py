"""Scripted demo scenarios reachable from the UI's demo buttons.

Each scenario creates a short window (a few seconds) so the full lifecycle
collecting -> grace -> closed can be watched live. Late spans are posted by
a background daemon thread during the grace period.
"""

from __future__ import annotations

import threading
import time

from .server import ApiError


def _post(state, span: dict) -> bool:
    try:
        snap = state.submit_span(span)
        return bool(snap.get("span_accepted", True))
    except ApiError as e:
        return False


def _schedule_late(state, spans: list[dict], delay: float) -> None:
    def worker():
        time.sleep(delay)
        for s in spans:
            _post(state, s)

    threading.Thread(target=worker, daemon=True).start()


def run_demo(state, name: str) -> dict:
    if name not in SCENARIOS:
        raise ApiError(404, f"未知演示场景 {name}，可选: {sorted(SCENARIOS)}")
    with state.storage.lock:
        active = state.storage.get_active_window()
        if active is not None:
            raise ApiError(409, f"窗口 #{active['id']} 仍在进行，请先结束或等待其关闭")
    return SCENARIOS[name](state)


# --------------------------------------------------------------- scenario 1
def demo_out_of_order(state) -> dict:
    """Spans of one trace arrive shuffled; the complete trace is rebuilt."""
    snap = state.create_window({
        "collect_ms": 4000, "grace_ms": 2000, "slow_ms": 500,
        "default_quota": 5, "quotas": {},
    })
    wid = snap["window"]["id"]
    base = snap["window"]["start_ms"]
    spans = [
        dict(span_id="oo-s3", trace_id="T-oo", service="checkout",
             operation="charge", start_ms=base + 300, duration_ms=120,
             status="ok"),
        dict(span_id="oo-s1", trace_id="T-oo", service="gateway",
             operation="POST /order", start_ms=base, duration_ms=450,
             status="ok"),
        dict(span_id="oo-s2", trace_id="T-oo", service="orders",
             operation="create", start_ms=base + 100, duration_ms=300,
             status="ok"),
    ]
    # Deliberately submit s3, then s1, then s2 (not time order).
    for s in (spans[0], spans[1], spans[2]):
        _post(state, s)
    return {"ok": True, "window_id": wid,
            "message": "已乱序提交同一链路的 3 个 span（s3→s1→s2），窗口结束后按完整链路决策"}


# --------------------------------------------------------------- scenario 2
def demo_late_error(state) -> dict:
    """A late error span during grace flips a tentatively-normal trace."""
    snap = state.create_window({
        "collect_ms": 3000, "grace_ms": 3000, "slow_ms": 500,
        "default_quota": 5, "quotas": {},
    })
    w = snap["window"]
    wid, base = w["id"], w["start_ms"]
    _post(state, dict(span_id="le-s1", trace_id="T-late", service="gateway",
                      operation="GET /pay", start_ms=base + 50,
                      duration_ms=200, status="ok"))
    _post(state, dict(span_id="le-s2", trace_id="T-late", service="payment",
                      operation="auth", start_ms=base + 150,
                      duration_ms=100, status="ok"))
    # Competitor normal trace that would consume attention first.
    _post(state, dict(span_id="le-c1", trace_id="T-competitor",
                      service="gateway", operation="GET /x",
                      start_ms=base + 200, duration_ms=80, status="ok"))
    # The late error span has a timestamp inside the collection window but
    # is only delivered after the collection window ends (during grace).
    late_span = dict(span_id="le-s3-late", trace_id="T-late",
                     service="payment", operation="settle",
                     start_ms=base + 2500, duration_ms=150, status="error")
    grace_arrival = max(0.0, (base + 3200 - state.now_ms()) / 1000.0)
    _schedule_late(state, [late_span], grace_arrival)
    return {"ok": True, "window_id": wid,
            "message": "宽限期内将到达一个迟到的错误 span，注意 T-late 的暂定决策如何被翻转"}


# --------------------------------------------------------------- scenario 3
def demo_quota(state) -> dict:
    """Quotas are a hard cap; high-priority traces win the last slots."""
    snap = state.create_window({
        "collect_ms": 4000, "grace_ms": 2000, "slow_ms": 400,
        "default_quota": 1,
        "quotas": {"gateway": 2, "orders": 1, "payment": 1},
    })
    wid, base = snap["window"]["id"], snap["window"]["start_ms"]
    # Three traces touch payment, whose quota is 1.
    _post(state, dict(span_id="q-n1", trace_id="T-normal-a",
                      service="payment", operation="n1", start_ms=base + 100,
                      duration_ms=50, status="ok"))
    _post(state, dict(span_id="q-n2", trace_id="T-normal-b",
                      service="payment", operation="n2", start_ms=base + 200,
                      duration_ms=50, status="ok"))
    _post(state, dict(span_id="q-e1", trace_id="T-error",
                      service="payment", operation="e1", start_ms=base + 300,
                      duration_ms=80, status="error"))
    # orders quota 1: one slow trace takes it, a second trace must drop.
    _post(state, dict(span_id="q-s1", trace_id="T-slow", service="orders",
                      operation="s1", start_ms=base + 150,
                      duration_ms=600, status="ok"))
    _post(state, dict(span_id="q-s2", trace_id="T-slow-2", service="orders",
                      operation="s2", start_ms=base + 160,
                      duration_ms=700, status="ok"))
    return {"ok": True, "window_id": wid,
            "message": "payment/orders 配额仅 1：错误链路抢占保留位，其余整条丢弃"}


# --------------------------------------------------------------- scenario 4
def demo_multi_service(state) -> dict:
    """A cross-service trace is kept only if ALL involved quotas allow it."""
    snap = state.create_window({
        "collect_ms": 4000, "grace_ms": 2000, "slow_ms": 500,
        "default_quota": 5,
        "quotas": {"gateway": 5, "orders": 5, "inventory": 0},
    })
    wid, base = snap["window"]["id"], snap["window"]["start_ms"]
    # Trace A spans gateway -> orders -> inventory (inventory quota = 0).
    _post(state, dict(span_id="m-a1", trace_id="T-multi-A", service="gateway",
                      operation="g", start_ms=base, duration_ms=300,
                      status="ok"))
    _post(state, dict(span_id="m-a2", trace_id="T-multi-A", service="orders",
                      operation="o", start_ms=base + 50, duration_ms=200,
                      status="ok"))
    _post(state, dict(span_id="m-a3", trace_id="T-multi-A",
                      service="inventory", operation="i", start_ms=base + 100,
                      duration_ms=100, status="ok"))
    # Trace B stays within healthy services and can be retained.
    _post(state, dict(span_id="m-b1", trace_id="T-multi-B", service="gateway",
                      operation="g2", start_ms=base + 200, duration_ms=120,
                      status="ok"))
    _post(state, dict(span_id="m-b2", trace_id="T-multi-B", service="orders",
                      operation="o2", start_ms=base + 250, duration_ms=80,
                      status="ok"))
    return {"ok": True, "window_id": wid,
            "message": "T-multi-A 经过 inventory（配额 0），整条链路丢弃，绝不只导出部分 span"}


SCENARIOS = {
    "out-of-order": demo_out_of_order,
    "late-error": demo_late_error,
    "quota-exhausted": demo_quota,
    "multi-service": demo_multi_service,
}
