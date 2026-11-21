"""SQLite persistence layer for the tail-sampling workbench.

All state needed to resume an unfinished window after a restart lives here:
window definitions, per-service quotas, received spans and export records.
The trace-level keep/drop decisions themselves are *derived* from spans and
quotas and therefore do not need a separate table.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS windows (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    start_ms        INTEGER NOT NULL,
    collect_ms      INTEGER NOT NULL,
    grace_ms        INTEGER NOT NULL,
    slow_ms         INTEGER NOT NULL,
    default_quota   INTEGER NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('open','closed')),
    finalized_at_ms INTEGER
);

CREATE TABLE IF NOT EXISTS quotas (
    window_id    INTEGER NOT NULL,
    service      TEXT NOT NULL,
    quota        INTEGER NOT NULL,
    PRIMARY KEY (window_id, service),
    FOREIGN KEY (window_id) REFERENCES windows (id)
);

CREATE TABLE IF NOT EXISTS spans (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    span_id       TEXT NOT NULL UNIQUE,
    trace_id      TEXT NOT NULL,
    service       TEXT NOT NULL,
    operation     TEXT NOT NULL,
    start_ms      INTEGER NOT NULL,
    duration_ms   INTEGER NOT NULL,
    status        TEXT NOT NULL,
    received_ms   INTEGER NOT NULL,
    late          INTEGER NOT NULL DEFAULT 0,
    window_id     INTEGER NOT NULL,
    FOREIGN KEY (window_id) REFERENCES windows (id)
);
CREATE INDEX IF NOT EXISTS idx_spans_trace ON spans (window_id, trace_id);
CREATE INDEX IF NOT EXISTS idx_spans_window ON spans (window_id);

CREATE TABLE IF NOT EXISTS exports (
    window_id      INTEGER NOT NULL,
    trace_id       TEXT NOT NULL,
    exported_at_ms INTEGER NOT NULL,
    span_count     INTEGER NOT NULL,
    PRIMARY KEY (window_id, trace_id),
    FOREIGN KEY (window_id) REFERENCES windows (id)
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class Storage:
    """Thin, thread-safe wrapper around a single SQLite database file."""

    def __init__(self, path: str):
        self.path = path
        # check_same_thread=False is safe because every call is guarded by
        # the re-entrant lock; the HTTP server is single threaded anyway.
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    # ------------------------------------------------------------------ meta
    def get_meta(self, key: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key=?", (key,)
            ).fetchone()
            return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            self._conn.commit()

    # --------------------------------------------------------------- windows
    def create_window(
        self,
        start_ms: int,
        collect_ms: int,
        grace_ms: int,
        slow_ms: int,
        default_quota: int,
    ) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO windows(start_ms, collect_ms, grace_ms, slow_ms, "
                "default_quota, status) VALUES(?,?,?,?,?,'open')",
                (start_ms, collect_ms, grace_ms, slow_ms, default_quota),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def get_active_window(self) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM windows WHERE status='open' "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()

    def get_window(self, window_id: int) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM windows WHERE id=?", (window_id,)
            ).fetchone()

    def list_windows(self, limit: int = 50) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM windows ORDER BY id DESC LIMIT ?", (limit,)
                ).fetchall()
            )

    def close_window(self, window_id: int, finalized_at_ms: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE windows SET status='closed', finalized_at_ms=? WHERE id=?",
                (finalized_at_ms, window_id),
            )
            self._conn.commit()

    # ---------------------------------------------------------------- quotas
    def upsert_quota(self, window_id: int, service: str, quota: int) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO quotas(window_id, service, quota) VALUES(?,?,?) "
                "ON CONFLICT(window_id, service) DO UPDATE SET quota=excluded.quota",
                (window_id, service, quota),
            )
            self._conn.commit()

    def get_quotas(self, window_id: int) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT service, quota FROM quotas WHERE window_id=?",
                (window_id,),
            ).fetchall()
            return {r["service"]: r["quota"] for r in rows}

    # ----------------------------------------------------------------- spans
    def insert_span(self, span: dict[str, Any], window_id: int, now_ms: int,
                    late: bool) -> bool:
        """Insert a span. Returns False if span_id already exists (idempotent)."""
        with self._lock:
            exists = self._conn.execute(
                "SELECT 1 FROM spans WHERE span_id=?", (span["span_id"],)
            ).fetchone()
            if exists:
                return False
            self._conn.execute(
                "INSERT INTO spans(span_id, trace_id, service, operation, "
                "start_ms, duration_ms, status, received_ms, late, window_id) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    span["span_id"], span["trace_id"], span["service"],
                    span["operation"], span["start_ms"], span["duration_ms"],
                    span["status"], now_ms, 1 if late else 0, window_id,
                ),
            )
            self._conn.commit()
            return True

    def get_spans(self, window_id: int) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM spans WHERE window_id=? ORDER BY start_ms, id",
                    (window_id,),
                ).fetchall()
            )

    def count_window_spans(self, window_id: int) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS c FROM spans WHERE window_id=?", (window_id,)
            ).fetchone()
            return int(row["c"])

    # --------------------------------------------------------------- exports
    def save_export(self, window_id: int, trace_id: str, exported_at_ms: int,
                    span_count: int) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO exports(window_id, trace_id, "
                "exported_at_ms, span_count) VALUES(?,?,?,?)",
                (window_id, trace_id, exported_at_ms, span_count),
            )
            self._conn.commit()

    def get_exported_trace_ids(self, window_id: int) -> set[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT trace_id FROM exports WHERE window_id=?", (window_id,)
            ).fetchall()
            return {r["trace_id"] for r in rows}

    def get_exports(self, window_id: int) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM exports WHERE window_id=? ORDER BY exported_at_ms",
                    (window_id,),
                ).fetchall()
            )

    # ----------------------------------------------------------------- reset
    def reset(self) -> None:
        with self._lock:
            for table in ("exports", "spans", "quotas", "windows", "meta"):
                self._conn.execute(f"DELETE FROM {table}")
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def export_state(self) -> str:
        """Debug helper: dump the whole database as JSON."""
        with self._lock:
            payload: dict[str, Any] = {}
            for table in ("windows", "quotas", "spans", "exports", "meta"):
                rows = self._conn.execute(f"SELECT * FROM {table}").fetchall()
                payload[table] = [dict(r) for r in rows]
            return json.dumps(payload, ensure_ascii=False, indent=2)
