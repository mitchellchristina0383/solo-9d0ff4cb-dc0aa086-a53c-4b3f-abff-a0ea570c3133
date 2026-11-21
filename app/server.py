"""HTTP server for the local tail-sampling workbench.

Zero third-party dependencies: built on http.server + sqlite3.

Endpoints
---------
GET  /                         single-page UI (static/index.html)
GET  /static/*                 UI assets
GET  /api/health               liveness probe
GET  /api/window/current       full snapshot of the active/last window
GET  /api/windows              window history
GET  /api/windows/<id>         one closed/active window with full details
POST /api/windows              create a window (collect/grace/slow/quota)
POST /api/windows/<id>/quotas  upsert per-service retention quota
POST /api/windows/<id>/finalize close the window immediately (run decision)
POST /api/spans                submit one span (timestamps supplied by client)
POST /api/demo/<name>          seed a scripted demo scenario
POST /api/simulate-restart     drop in-memory state and reload from SQLite
POST /api/reset                wipe all data (demo convenience)
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from . import sampler
from .storage import Storage

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

VALID_STATUSES = {"ok", "error", "timeout"}
MAX_BODY_BYTES = 1_000_000


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class AppState:
    """Holds the database and the pluggable clock (real time by default)."""

    def __init__(self, db_path: str):
        self.storage = Storage(db_path)
        self.db_path = db_path
        # now_ms is indirected so tests can drive deterministic scenarios;
        # production simply reads the wall clock.
        self.now_ms = lambda: int(time.time() * 1000)
        self.startup_count = int(self.storage.get_meta("startup_count") or 0) + 1
        self.storage.set_meta("startup_count", str(self.startup_count))
        if self.storage.recovered_orphan_exports:
            print(
                f"recovery: 已清理 {self.storage.recovered_orphan_exports} 条"
                "属于未关闭窗口的残留导出记录（旧库中断写入遗留），"
                "这些窗口将按当前 span 与配额重新结算",
                flush=True,
            )

    # ------------------------------------------------------------- windows
    def phase_of(self, w) -> str:
        now = self.now_ms()
        collect_end = w["start_ms"] + w["collect_ms"]
        grace_end = collect_end + w["grace_ms"]
        if w["status"] == "closed":
            return "closed"
        if now < collect_end:
            return "collecting"
        if now <= grace_end:
            # The boundary instant itself still belongs to the grace
            # period, so a span arriving exactly at grace_end is accepted
            # before the window is finalized (matters when grace_ms == 0).
            return "grace"
        return "due"  # open but strictly past grace end: finalize on touch

    def advance(self) -> None:
        """Background/tick work: finalize any open window past its grace."""
        with self.storage.lock:
            w = self.storage.get_active_window()
            if w is not None and self.phase_of(w) == "due":
                self._finalize_locked(w["id"])

    def create_window(self, payload: dict) -> dict:
        collect_ms = int(payload["collect_ms"])
        grace_ms = int(payload["grace_ms"])
        slow_ms = int(payload["slow_ms"])
        default_quota = int(payload["default_quota"])
        if min(collect_ms, grace_ms, slow_ms, default_quota) < 0:
            raise ApiError(400, "窗口时长、阈值和配额不能为负数")
        if collect_ms < 500:
            raise ApiError(400, "收集窗口至少 500ms")
        if default_quota < 0:
            raise ApiError(400, "默认配额不能为负数")
        with self.storage.lock:
            active = self.storage.get_active_window()
            if active is not None:
                raise ApiError(409, f"已有进行中的窗口 #{active['id']}，请先结束它")
            start_ms = (
                int(payload["start_ms"]) if payload.get("start_ms") is not None
                else self.now_ms()
            )
            wid = self.storage.create_window(
                start_ms, collect_ms, grace_ms, slow_ms, default_quota
            )
            for svc, q in (payload.get("quotas") or {}).items():
                q = int(q)
                if q < 0:
                    raise ApiError(400, f"服务 {svc} 配额不能为负数")
                self.storage.upsert_quota(wid, str(svc), q)
            return self._snapshot_locked(self.storage.get_window(wid))

    def set_quota(self, window_id: int, payload: dict) -> dict:
        service = str(payload.get("service") or "").strip()
        if not service:
            raise ApiError(400, "缺少 service")
        quota = int(payload.get("quota"))
        if quota < 0:
            raise ApiError(400, "配额不能为负数")
        with self.storage.lock:
            w = self.storage.get_window(window_id)
            if w is None:
                raise ApiError(404, "窗口不存在")
            if w["status"] != "open":
                raise ApiError(409, "窗口已关闭，不能再修改配额")
            self.storage.upsert_quota(window_id, service, quota)
            return self._snapshot_locked(self.storage.get_window(window_id))

    def finalize(self, window_id: int) -> dict:
        with self.storage.lock:
            w = self.storage.get_window(window_id)
            if w is None:
                raise ApiError(404, "窗口不存在")
            if w["status"] == "closed":
                return self._snapshot_locked(w)  # idempotent
            self._finalize_locked(window_id)
            return self._snapshot_locked(self.storage.get_window(window_id))

    def _finalize_locked(self, window_id: int) -> None:
        """Run the tail decision once and persist it atomically.

        The whole export batch and the window close commit in a single
        transaction (see ``Storage.finalize_window``). A write failure
        raises: nothing is exported, the window stays open and every
        received span and quota is preserved, so while the window is still
        collecting / in grace, late spans and quota changes keep working
        and the finalize can simply be retried — the retry re-settles from
        the spans and quotas in effect at that moment.
        """
        w = self.storage.get_window(window_id)
        span_rows = self.storage.get_spans(window_id)
        traces = sampler.build_traces(span_rows)
        quotas = self.storage.get_quotas(window_id)
        already = self.storage.get_exported_trace_ids(window_id)
        result = sampler.evaluate_window(
            traces, quotas, w["default_quota"], w["slow_ms"], already
        )
        now = self.now_ms()
        batch = [
            (trace_id, len(traces[trace_id].spans))
            for trace_id, d in result["decisions"].items()
            if d["decision"] == sampler.KEEP and trace_id not in already
        ]
        try:
            self.storage.finalize_window(window_id, batch, now)
        except sqlite3.Error as e:
            raise ApiError(
                500,
                f"导出写入失败：{e}。窗口未关闭、未产生任何导出，"
                "已接收的 span 与配额保持不变；收集期/宽限期内仍可继续补传 "
                "span 和调整配额，请重试结束窗口。",
            )

    # ---------------------------------------------------------------- spans
    def submit_span(self, payload: dict) -> dict:
        required = ("span_id", "trace_id", "service", "operation",
                    "start_ms", "duration_ms", "status")
        for key in required:
            if payload.get(key) is None:
                raise ApiError(400, f"缺少字段 {key}")
        span = {
            "span_id": str(payload["span_id"]).strip(),
            "trace_id": str(payload["trace_id"]).strip(),
            "service": str(payload["service"]).strip(),
            "operation": str(payload["operation"]).strip(),
            "start_ms": int(payload["start_ms"]),
            "duration_ms": int(payload["duration_ms"]),
            "status": str(payload["status"]).strip().lower(),
        }
        if not all(span[k] for k in ("span_id", "trace_id", "service", "operation")):
            raise ApiError(400, "span_id/trace_id/service/operation 不能为空")
        if span["duration_ms"] < 0:
            raise ApiError(400, "duration_ms 不能为负数")
        if span["status"] not in VALID_STATUSES:
            raise ApiError(400, f"status 必须是 {sorted(VALID_STATUSES)} 之一")
        now = self.now_ms()
        if span["start_ms"] > now + 2000:
            raise ApiError(400, "start_ms 不能是未来时间（容差 2s）")
        with self.storage.lock:
            self.advance()
            w = self.storage.get_active_window()
            if w is None:
                raise ApiError(409, "当前没有进行中的窗口，请先创建窗口")
            collect_end = w["start_ms"] + w["collect_ms"]
            grace_end = collect_end + w["grace_ms"]
            if span["start_ms"] < w["start_ms"]:
                raise ApiError(
                    400,
                    f"span 时间戳 {span['start_ms']} 早于窗口开始 {w['start_ms']}",
                )
            if span["start_ms"] > grace_end:
                raise ApiError(
                    400,
                    "span 时间戳已超过收集窗口+宽限期，该窗口已不可接收新 span",
                )
            late = span["start_ms"] >= collect_end or now >= collect_end
            inserted = self.storage.insert_span(span, w["id"], now, late)
            snap = self._snapshot_locked(self.storage.get_window(w["id"]))
            snap["span_accepted"] = inserted
            if not inserted:
                snap["duplicate"] = True
            return snap

    # ------------------------------------------------------------ snapshots
    def _snapshot_locked(self, w) -> dict:
        """Build the full JSON representation of one window."""
        span_rows = self.storage.get_spans(w["id"])
        traces = sampler.build_traces(span_rows)
        quotas = self.storage.get_quotas(w["id"])
        exported = self.storage.get_exported_trace_ids(w["id"])
        phase = self.phase_of(w)

        if w["status"] == "closed":
            result = sampler.evaluate_window(
                traces, quotas, w["default_quota"], w["slow_ms"], exported
            )
        else:
            result = sampler.tentative_evaluation(
                traces, quotas, w["default_quota"], w["slow_ms"]
            )

        trace_list = []
        for trace_id in sorted(traces, key=lambda tid: (
                traces[tid].tier(w["slow_ms"]), traces[tid].start_ms, tid)):
            t = traces[trace_id]
            trace_list.append(
                sampler.trace_payload(t, w["slow_ms"], result["decisions"][trace_id])
            )

        collect_end = w["start_ms"] + w["collect_ms"]
        grace_end = collect_end + w["grace_ms"]
        services_seen = sorted({s["service"] for s in span_rows})
        quota_view = []
        for svc in sorted(set(services_seen) | set(quotas)):
            q = quotas.get(svc, w["default_quota"])
            quota_view.append({
                "service": svc,
                "quota": q,
                "explicit": svc in quotas,
                "used": result["occupancy"].get(svc, {"used": 0})["used"],
            })

        return {
            "window": {
                "id": w["id"],
                "start_ms": w["start_ms"],
                "collect_ms": w["collect_ms"],
                "grace_ms": w["grace_ms"],
                "slow_ms": w["slow_ms"],
                "default_quota": w["default_quota"],
                "status": w["status"],
                "phase": phase,
                "collect_end_ms": collect_end,
                "grace_end_ms": grace_end,
                "finalized_at_ms": w["finalized_at_ms"],
                "server_now_ms": self.now_ms(),
            },
            "quotas": quota_view,
            "traces": trace_list,
            "exports": [dict(r) for r in self.storage.get_exports(w["id"])],
            "span_count": len(span_rows),
            "kept_trace_count": sum(1 for t in trace_list
                                    if t["decision"] == sampler.KEEP),
            "dropped_trace_count": sum(
                1 for t in trace_list if t["decision"] == sampler.DROP_QUOTA
            ),
        }

    def current_snapshot(self) -> dict:
        with self.storage.lock:
            self.advance()
            w = self.storage.get_active_window()
            if w is None:
                rows = self.storage.list_windows(limit=1)
                if not rows:
                    return {"window": None, "message": "尚未创建窗口"}
                w = rows[0]
            return self._snapshot_locked(w)

    def window_detail(self, window_id: int) -> dict:
        with self.storage.lock:
            w = self.storage.get_window(window_id)
            if w is None:
                raise ApiError(404, "窗口不存在")
            return self._snapshot_locked(w)

    def history(self) -> dict:
        with self.storage.lock:
            out = []
            for w in self.storage.list_windows():
                out.append({
                    "id": w["id"],
                    "start_ms": w["start_ms"],
                    "status": w["status"],
                    "collect_ms": w["collect_ms"],
                    "grace_ms": w["grace_ms"],
                    "slow_ms": w["slow_ms"],
                    "default_quota": w["default_quota"],
                    "span_count": self.storage.count_window_spans(w["id"]),
                    "export_count": len(self.storage.get_exports(w["id"])),
                })
            return {"windows": out, "startup_count": self.startup_count}

    def reset(self) -> dict:
        with self.storage.lock:
            self.storage.reset()
            self.startup_count = 1
            self.storage.set_meta("startup_count", "1")
        return {"ok": True, "message": "所有数据已清空"}

    def simulate_restart(self) -> dict:
        """Re-open the Storage from the same file to prove resumability."""
        with self.storage.lock:
            self.storage.close()
            self.storage = Storage(self.db_path)
            self.startup_count = int(
                self.storage.get_meta("startup_count") or 0
            ) + 1
            self.storage.set_meta("startup_count", str(self.startup_count))
            return {"ok": True, "startup_count": self.startup_count,
                    "message": "已从 SQLite 重新加载全部状态（模拟进程重启）"}


# --------------------------------------------------------------------- HTTP
class Handler(BaseHTTPRequestHandler):
    server_version = "TailSamplingWorkbench/1.0"

    def log_message(self, fmt, *args):  # quieter logs
        pass

    @property
    def state(self) -> AppState:
        return self.server.state  # type: ignore[attr-defined]

    def _send_json(self, status: int, payload) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        if length > MAX_BODY_BYTES:
            raise ApiError(413, "请求体过大")
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ApiError(400, "请求体不是合法 JSON")
        if not isinstance(data, dict):
            raise ApiError(400, "请求体必须是 JSON 对象")
        return data

    def _send_static(self, rel: str) -> None:
        # Prevent path traversal: resolve inside STATIC_DIR.
        safe = os.path.normpath(os.path.join(STATIC_DIR, rel))
        if not safe.startswith(STATIC_DIR + os.sep) and safe != STATIC_DIR:
            self._send_json(403, {"error": "forbidden"})
            return
        if os.path.isdir(safe) or not os.path.isfile(safe):
            self._send_json(404, {"error": "not found"})
            return
        ctype = {
            ".html": "text/html; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".svg": "image/svg+xml",
        }.get(os.path.splitext(safe)[1], "application/octet-stream")
        with open(safe, "rb") as fh:
            body = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ------------------------------------------------------------- routing
    def do_GET(self) -> None:
        try:
            path = urlparse(self.path).path
            if path == "/":
                self._send_static("index.html")
            elif path.startswith("/static/"):
                self._send_static(path[len("/static/"):])
            elif path == "/api/health":
                self._send_json(200, {"ok": True,
                                      "startup_count": self.state.startup_count})
            elif path == "/api/window/current":
                self._send_json(200, self.state.current_snapshot())
            elif path == "/api/windows":
                self._send_json(200, self.state.history())
            elif path.startswith("/api/windows/"):
                wid = int(path.split("/")[3])
                self._send_json(200, self.state.window_detail(wid))
            else:
                self._send_json(404, {"error": "not found"})
        except ApiError as e:
            self._send_json(e.status, {"error": e.message})
        except ValueError:
            self._send_json(400, {"error": "非法的 id 参数"})
        except Exception as e:  # pragma: no cover - defensive
            self._send_json(500, {"error": f"服务器内部错误: {e}"})

    def do_POST(self) -> None:
        try:
            path = urlparse(self.path).path.rstrip("/")
            payload = self._read_json()
            if path == "/api/windows":
                self._send_json(201, self.state.create_window(payload))
            elif path.startswith("/api/windows/") and path.endswith("/quotas"):
                wid = int(path.split("/")[3])
                self._send_json(200, self.state.set_quota(wid, payload))
            elif path.startswith("/api/windows/") and path.endswith("/finalize"):
                wid = int(path.split("/")[3])
                self._send_json(200, self.state.finalize(wid))
            elif path == "/api/spans":
                snap = self.state.submit_span(payload)
                self._send_json(200 if snap.get("span_accepted", True) else 200,
                                snap)
            elif path.startswith("/api/demo/"):
                from .demo import run_demo
                name = path[len("/api/demo/"):]
                self._send_json(200, run_demo(self.state, name))
            elif path == "/api/simulate-restart":
                self._send_json(200, self.state.simulate_restart())
            elif path == "/api/reset":
                self._send_json(200, self.state.reset())
            else:
                self._send_json(404, {"error": "not found"})
        except ApiError as e:
            self._send_json(e.status, {"error": e.message})
        except (TypeError, ValueError) as e:
            self._send_json(400, {"error": f"参数类型错误: {e}"})
        except Exception as e:  # pragma: no cover - defensive
            self._send_json(500, {"error": f"服务器内部错误: {e}"})


def create_server(host: str, port: int, db_path: str) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.state = AppState(db_path)  # type: ignore[attr-defined]
    return httpd


def main() -> None:
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    db_path = os.environ.get("DB_PATH", "/data/tracing.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    httpd = create_server(host, port, db_path)
    print(f"tail-sampling workbench listening on http://{host}:{port} "
          f"(db={db_path})", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.state.storage.close()
        httpd.server_close()


if __name__ == "__main__":
    main()
