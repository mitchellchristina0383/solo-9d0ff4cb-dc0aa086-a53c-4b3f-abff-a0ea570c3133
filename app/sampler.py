"""Tail-sampling decision logic.

The core rule required by the exercise:

* Spans are grouped into complete traces within one collection window.
* A decision is made on the *whole trace* after the window (plus a grace
  period for late spans), never on individual spans.
* Error / timeout traces get priority, then slow traces, then the rest.
* A trace spanning several services must fit under *every* involved
  service's retention quota simultaneously; if any one quota is exhausted
  the whole trace (all of its spans) is dropped — partial export is never
  allowed.
* Quotas are a hard cap: even an error trace is dropped once the relevant
  quotas are full.

This module contains pure functions; the server layer feeds it span rows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

# Decision codes -----------------------------------------------------------
WAITING = "waiting"      # window still open / grace in progress, no final call
KEEP = "keep"            # retained and (for closed windows) exported
DROP_QUOTA = "drop_quota"              # a required service quota was full
# Every drop is fundamentally a quota rejection: a service had no free slot
# when this trace's turn came. The human-readable reason distinguishes the
# two flavours (hard quota 0 vs. slots consumed by higher-priority traces).

# Priority tiers (lower number = retained first)
TIER_ERROR = 0   # status error / timeout anywhere in the trace
TIER_SLOW = 1    # not error, but exceeds the slow-request threshold
TIER_NORMAL = 2  # everything else

ERROR_STATUSES = {"error", "timeout"}


@dataclass
class Trace:
    trace_id: str
    spans: list[dict[str, Any]] = field(default_factory=list)

    @property
    def services(self) -> list[str]:
        seen: list[str] = []
        for s in self.spans:
            if s["service"] not in seen:
                seen.append(s["service"])
        return sorted(seen)

    @property
    def start_ms(self) -> int:
        return min(s["start_ms"] for s in self.spans)

    @property
    def end_ms(self) -> int:
        return max(s["start_ms"] + s["duration_ms"] for s in self.spans)

    @property
    def latency_ms(self) -> int:
        return self.end_ms - self.start_ms

    @property
    def has_error(self) -> bool:
        return any(s["status"] in ERROR_STATUSES for s in self.spans)

    @property
    def has_timeout(self) -> bool:
        return any(s["status"] == "timeout" for s in self.spans)

    @property
    def late_span_count(self) -> int:
        return sum(1 for s in self.spans if s.get("late"))

    def tier(self, slow_ms: int) -> int:
        if self.has_error:
            return TIER_ERROR
        if self.latency_ms >= slow_ms:
            return TIER_SLOW
        return TIER_NORMAL

    def reason_detail(self, slow_ms: int) -> str:
        if self.has_timeout:
            return "含超时 span（最高优先）"
        if self.has_error:
            return "含错误 span（最高优先）"
        if self.latency_ms >= slow_ms:
            return f"慢请求 {self.latency_ms}ms ≥ 阈值 {slow_ms}ms"
        return f"普通链路 {self.latency_ms}ms"


def build_traces(span_rows: Iterable[Any]) -> dict[str, Trace]:
    """Group persisted span rows by trace_id, preserving span order."""
    traces: dict[str, Trace] = {}
    for row in span_rows:
        d = dict(row)
        t = traces.setdefault(d["trace_id"], Trace(d["trace_id"]))
        t.spans.append(d)
    return traces


def evaluate_window(
    traces: dict[str, Trace],
    quotas: dict[str, int],
    default_quota: int,
    slow_ms: int,
    already_exported: set[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Run the final, window-level allocation.

    Traces are processed in deterministic priority order:
    error/timeout → slow → normal, oldest trace first inside a tier.
    Each accepted trace consumes one slot from *every* service it touches.

    ``already_exported`` guarantees that a trace exported earlier (for
    example after an earlier decision pass) can never be exported twice:
    it stays "keep" but consumes no new quota slots here.
    """
    already_exported = already_exported or set()

    def sort_key(t: Trace) -> tuple[int, int, str]:
        return (t.tier(slow_ms), t.start_ms, t.trace_id)

    ordered = sorted(traces.values(), key=sort_key)

    remaining = dict(quotas)          # explicit per-service quotas
    used: dict[str, int] = {}
    decisions: dict[str, dict[str, Any]] = {}

    def effective_quota(service: str) -> int:
        return remaining.get(service, default_quota)

    # Traces exported by an earlier decision pass still occupy their quota
    # slots: restore their occupancy first so re-evaluation (e.g. after a
    # restart or an idempotent re-finalize) can never resurrect a dropped
    # trace or exceed a quota.
    for t in ordered:
        if t.trace_id in already_exported:
            for svc in t.services:
                used[svc] = used.get(svc, 0) + 1

    for t in ordered:
        services = t.services
        # Already-exported traces keep their keep decision idempotently and
        # are never exported a second time (their slots were restored above).
        if t.trace_id in already_exported:
            decisions[t.trace_id] = {
                "decision": KEEP,
                "tier": t.tier(slow_ms),
                "reason": "已导出（幂等保留，不重复导出）",
                "blocked_service": None,
            }
            continue

        # A quota of 0 (or a fully consumed service) blocks the whole trace.
        blocking = next(
            (svc for svc in services if used.get(svc, 0) >= effective_quota(svc)),
            None,
        )
        if blocking is not None:
            decisions[t.trace_id] = {
                "decision": DROP_QUOTA,
                "tier": t.tier(slow_ms),
                "reason": (
                    f"服务 {blocking} 配额已耗尽 "
                    f"（{used.get(blocking, 0)}/{effective_quota(blocking)}），"
                    "跨服务链路整体丢弃，不做部分导出"
                ),
                "blocked_service": blocking,
            }
            continue

        # Accept: atomically reserve one slot on every involved service.
        for svc in services:
            used[svc] = used.get(svc, 0) + 1
        tier = t.tier(slow_ms)
        if tier == TIER_ERROR:
            reason = "错误/超时链路优先保留"
        elif tier == TIER_SLOW:
            reason = f"慢请求优先保留（{t.latency_ms}ms ≥ {slow_ms}ms）"
        else:
            reason = "配额内的普通链路，予以保留"
        decisions[t.trace_id] = {
            "decision": KEEP,
            "tier": tier,
            "reason": reason,
            "blocked_service": None,
        }

    # Traces that lost to higher-priority traffic (never got a slot because
    # a higher tier consumed it) are reported distinctly from hard quota
    # zero rejections where useful: both stay drops, but the reason text
    # explains the competition.
    for t in ordered:
        d = decisions[t.trace_id]
        if d["decision"] == DROP_QUOTA and t.tier(slow_ms) != TIER_NORMAL:
            # Even a priority trace can lose when a quota is full — make
            # that explicit rather than implying it was merely out-ranked.
            d["reason"] = "虽为高优先链路，但" + d["reason"]

    # Annotate final per-service occupancy for the UI.
    occupancy = {
        svc: {
            "used": used.get(svc, 0),
            "quota": remaining.get(svc, default_quota),
        }
        for svc in sorted({s for t in traces.values() for s in t.services})
    }
    return {"decisions": decisions, "occupancy": occupancy}


def tentative_evaluation(
    traces: dict[str, Trace],
    quotas: dict[str, int],
    default_quota: int,
    slow_ms: int,
) -> dict[str, Any]:
    """Preview the allocation while the window is still open / in grace.

    Decisions are marked ``waiting`` so the UI can show *why* a trace is
    currently tentatively kept or dropped and that the call can still
    change when late spans arrive.
    """
    result = evaluate_window(traces, quotas, default_quota, slow_ms)
    for d in result["decisions"].values():
        if d["decision"] == KEEP:
            d["decision"] = WAITING
            d["reason"] = "等待窗口结束：暂定保留 — " + d["reason"]
        else:  # DROP_QUOTA, still reversible until the window is exported
            d["decision"] = WAITING
            d["reason"] = "等待窗口结束：暂定丢弃 — " + d["reason"]
    return result


def trace_payload(trace: Trace, slow_ms: int, decision: dict[str, Any]) -> dict[str, Any]:
    """Serialize one trace plus its spans for the API / UI."""
    return {
        "trace_id": trace.trace_id,
        "services": trace.services,
        "span_count": len(trace.spans),
        "latency_ms": trace.latency_ms,
        "has_error": trace.has_error,
        "has_timeout": trace.has_timeout,
        "is_slow": trace.latency_ms >= slow_ms,
        "tier": trace.tier(slow_ms),
        "tier_label": ("错误/超时", "慢请求", "普通")[trace.tier(slow_ms)],
        "late_span_count": trace.late_span_count,
        "start_ms": trace.start_ms,
        "end_ms": trace.end_ms,
        "decision": decision["decision"],
        "reason": decision["reason"],
        "blocked_service": decision.get("blocked_service"),
        "spans": [
            {
                "span_id": s["span_id"],
                "service": s["service"],
                "operation": s["operation"],
                "start_ms": s["start_ms"],
                "duration_ms": s["duration_ms"],
                "status": s["status"],
                "received_ms": s["received_ms"],
                "late": bool(s.get("late")),
            }
            for s in sorted(trace.spans, key=lambda x: (x["start_ms"], x["id"]))
        ],
    }
