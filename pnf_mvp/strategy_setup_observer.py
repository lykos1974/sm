"""Isolated, non-executing recorder of close-confirmed ideal-entry availability."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any


def _number(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


class StrategySetupObserver:
    def __init__(self, database_path, symbols, statuses, minimum_quality_score):
        self.database_path = Path(database_path).resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.symbols = {str(item).upper() for item in symbols}
        self.statuses = {str(item).upper() for item in statuses}
        self.minimum_quality_score = float(minimum_quality_score)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.database_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._init_schema()
        self._interrupt_open_occurrences()

    def _init_schema(self):
        with self._conn:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS setup_occurrences (
                    occurrence_id TEXT PRIMARY KEY, setup_key TEXT NOT NULL,
                    symbol TEXT NOT NULL, strategy TEXT NOT NULL, side TEXT NOT NULL,
                    status TEXT NOT NULL, quality_score REAL NOT NULL,
                    reference_close_ms INTEGER NOT NULL, available_wall_ns INTEGER NOT NULL,
                    available_monotonic_ns INTEGER NOT NULL, last_seen_close_ms INTEGER NOT NULL,
                    last_seen_wall_ns INTEGER NOT NULL, expires_wall_ns INTEGER,
                    lifecycle TEXT NOT NULL, close_reason TEXT, ideal_entry REAL NOT NULL,
                    invalidation REAL, tp1 REAL, tp2 REAL, current_column_index INTEGER,
                    setup_json TEXT NOT NULL, structure_json TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_setup_observer_open_key
                ON setup_occurrences(symbol,setup_key) WHERE lifecycle='OPEN';
                CREATE INDEX IF NOT EXISTS idx_setup_observer_window
                ON setup_occurrences(symbol,available_wall_ns,expires_wall_ns);
            """)

    def _interrupt_open_occurrences(self):
        now = time.time_ns()
        with self._conn:
            self._conn.execute(
                "UPDATE setup_occurrences SET lifecycle='INTERRUPTED',expires_wall_ns=?,"
                "close_reason='OBSERVER_RESTART' WHERE lifecycle='OPEN'", (now,),
            )

    def _eligible(self, symbol, setup):
        status = str(setup.get("status") or "").upper()
        score = _number(setup.get("quality_score"))
        return (symbol.upper() in self.symbols and status in self.statuses and
                score is not None and score >= self.minimum_quality_score and
                _number(setup.get("ideal_entry")) is not None)

    @staticmethod
    def _setup_key(symbol, setup, structure):
        payload = {
            "symbol": symbol.upper(), "strategy": setup.get("strategy"),
            "side": str(setup.get("side") or "").upper(),
            "status": str(setup.get("status") or "").upper(),
            "ideal_entry": _number(setup.get("ideal_entry")),
            "invalidation": _number(setup.get("invalidation")),
            "tp1": _number(setup.get("tp1")), "tp2": _number(setup.get("tp2")),
            "current_column_index": structure.get("current_column_index"),
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def observe(self, symbol, reference_close_ms, setups, structure,
                observed_wall_ns, observed_monotonic_ns):
        symbol = symbol.upper()
        if symbol not in self.symbols:
            return {"opened": 0, "continued": 0, "withdrawn": 0}
        eligible = {self._setup_key(symbol, item, structure): item
                    for item in setups if self._eligible(symbol, item)}
        with self._lock, self._conn:
            open_rows = {row[0]: row[1] for row in self._conn.execute(
                "SELECT setup_key,occurrence_id FROM setup_occurrences "
                "WHERE symbol=? AND lifecycle='OPEN'", (symbol,),
            )}
            withdrawn = set(open_rows) - set(eligible)
            for key in withdrawn:
                self._conn.execute(
                    "UPDATE setup_occurrences SET lifecycle='WITHDRAWN',expires_wall_ns=?,"
                    "close_reason='NO_LONGER_ELIGIBLE' WHERE occurrence_id=?",
                    (int(observed_wall_ns), open_rows[key]),
                )
            continued = set(open_rows) & set(eligible)
            for key in continued:
                self._conn.execute(
                    "UPDATE setup_occurrences SET last_seen_close_ms=?,last_seen_wall_ns=? "
                    "WHERE occurrence_id=?",
                    (int(reference_close_ms), int(observed_wall_ns), open_rows[key]),
                )
            opened = set(eligible) - set(open_rows)
            for key in opened:
                item = eligible[key]
                self._conn.execute("""
                    INSERT INTO setup_occurrences (
                        occurrence_id,setup_key,symbol,strategy,side,status,quality_score,
                        reference_close_ms,available_wall_ns,available_monotonic_ns,
                        last_seen_close_ms,last_seen_wall_ns,lifecycle,ideal_entry,
                        invalidation,tp1,tp2,current_column_index,setup_json,structure_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    uuid.uuid4().hex, key, symbol, str(item.get("strategy") or ""),
                    str(item.get("side") or "").upper(), str(item.get("status") or "").upper(),
                    float(item.get("quality_score")), int(reference_close_ms),
                    int(observed_wall_ns), int(observed_monotonic_ns), int(reference_close_ms),
                    int(observed_wall_ns), "OPEN", float(item.get("ideal_entry")),
                    _number(item.get("invalidation")), _number(item.get("tp1")),
                    _number(item.get("tp2")), structure.get("current_column_index"),
                    json.dumps(item, sort_keys=True, ensure_ascii=False),
                    json.dumps(structure, sort_keys=True, ensure_ascii=False),
                ))
        return {"opened": len(opened), "continued": len(continued),
                "withdrawn": len(withdrawn)}

    def close(self, reason="OBSERVER_STOP"):
        with self._lock:
            now = time.time_ns()
            with self._conn:
                self._conn.execute(
                    "UPDATE setup_occurrences SET lifecycle='INTERRUPTED',expires_wall_ns=?,"
                    "close_reason=? WHERE lifecycle='OPEN'", (now, str(reason)),
                )
            self._conn.close()
