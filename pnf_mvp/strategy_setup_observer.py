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
    def __init__(self, database_path, symbols, statuses, minimum_quality_score,
                 maximum_lifetime_candles=3, candle_interval_ms=60_000):
        self.database_path = Path(database_path).resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.symbols = {str(item).upper() for item in symbols}
        self.statuses = {str(item).upper() for item in statuses}
        self.minimum_quality_score = float(minimum_quality_score)
        self.maximum_lifetime_candles = int(maximum_lifetime_candles)
        self.candle_interval_ms = int(candle_interval_ms)
        if self.maximum_lifetime_candles <= 0 or self.candle_interval_ms <= 0:
            raise ValueError("observer lifetime and candle interval must be positive")
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
                    scheduled_expires_wall_ns INTEGER,
                    lifecycle TEXT NOT NULL, close_reason TEXT, ideal_entry REAL NOT NULL,
                    invalidation REAL, tp1 REAL, tp2 REAL, current_column_index INTEGER,
                    setup_json TEXT NOT NULL, structure_json TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_setup_observer_open_key
                ON setup_occurrences(symbol,setup_key) WHERE lifecycle='OPEN';
                CREATE INDEX IF NOT EXISTS idx_setup_observer_window
                ON setup_occurrences(symbol,available_wall_ns,expires_wall_ns);
            """)
            columns = {row[1] for row in self._conn.execute(
                "PRAGMA table_info(setup_occurrences)"
            )}
            if "scheduled_expires_wall_ns" not in columns:
                self._conn.execute(
                    "ALTER TABLE setup_occurrences ADD COLUMN scheduled_expires_wall_ns INTEGER"
                )
            lifetime_ms = self.maximum_lifetime_candles * self.candle_interval_ms
            self._conn.execute(
                "UPDATE setup_occurrences SET scheduled_expires_wall_ns="
                "(reference_close_ms+?)*1000000 WHERE scheduled_expires_wall_ns IS NULL",
                (lifetime_ms,),
            )
            self._conn.execute(
                "UPDATE setup_occurrences SET lifecycle='EXPIRED',"
                "expires_wall_ns=scheduled_expires_wall_ns,"
                "close_reason='MIGRATED_THREE_CANDLE_CAP' "
                "WHERE expires_wall_ns IS NOT NULL AND scheduled_expires_wall_ns IS NOT NULL "
                "AND expires_wall_ns>scheduled_expires_wall_ns"
            )

    def _interrupt_open_occurrences(self):
        now = time.time_ns()
        with self._conn:
            self._conn.execute(
                "UPDATE setup_occurrences SET "
                "lifecycle=CASE WHEN scheduled_expires_wall_ns IS NOT NULL "
                "AND scheduled_expires_wall_ns<=? THEN 'EXPIRED' ELSE 'INTERRUPTED' END,"
                "expires_wall_ns=CASE WHEN scheduled_expires_wall_ns IS NOT NULL "
                "AND scheduled_expires_wall_ns<=? THEN scheduled_expires_wall_ns ELSE ? END,"
                "close_reason=CASE WHEN scheduled_expires_wall_ns IS NOT NULL "
                "AND scheduled_expires_wall_ns<=? THEN 'THREE_CANDLE_EXPIRY' "
                "ELSE 'OBSERVER_RESTART' END WHERE lifecycle='OPEN'", (now, now, now, now),
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
            return {"opened": 0, "continued": 0, "withdrawn": 0, "expired": 0}
        eligible = {self._setup_key(symbol, item, structure): item
                    for item in setups if self._eligible(symbol, item)}
        with self._lock, self._conn:
            expired = self._conn.execute(
                "UPDATE setup_occurrences SET lifecycle='EXPIRED',"
                "expires_wall_ns=scheduled_expires_wall_ns,"
                "close_reason='THREE_CANDLE_EXPIRY' WHERE symbol=? AND lifecycle='OPEN' "
                "AND scheduled_expires_wall_ns IS NOT NULL AND scheduled_expires_wall_ns<=?",
                (symbol, int(observed_wall_ns)),
            ).rowcount
            open_rows = {row[0]: row[1] for row in self._conn.execute(
                "SELECT setup_key,occurrence_id FROM setup_occurrences "
                "WHERE symbol=? AND lifecycle='OPEN'", (symbol,),
            )}
            historical_keys = {row[0] for row in self._conn.execute(
                "SELECT DISTINCT setup_key FROM setup_occurrences WHERE symbol=?", (symbol,),
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
            opened = set(eligible) - set(open_rows) - historical_keys
            inserted = 0
            for key in opened:
                item = eligible[key]
                scheduled_expiry = (
                    int(reference_close_ms) +
                    self.maximum_lifetime_candles * self.candle_interval_ms
                ) * 1_000_000
                if scheduled_expiry <= int(observed_wall_ns):
                    continue
                self._conn.execute("""
                    INSERT INTO setup_occurrences (
                        occurrence_id,setup_key,symbol,strategy,side,status,quality_score,
                        reference_close_ms,available_wall_ns,available_monotonic_ns,
                        last_seen_close_ms,last_seen_wall_ns,scheduled_expires_wall_ns,
                        lifecycle,ideal_entry,
                        invalidation,tp1,tp2,current_column_index,setup_json,structure_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    uuid.uuid4().hex, key, symbol, str(item.get("strategy") or ""),
                    str(item.get("side") or "").upper(), str(item.get("status") or "").upper(),
                    float(item.get("quality_score")), int(reference_close_ms),
                    int(observed_wall_ns), int(observed_monotonic_ns), int(reference_close_ms),
                    int(observed_wall_ns), scheduled_expiry, "OPEN",
                    float(item.get("ideal_entry")),
                    _number(item.get("invalidation")), _number(item.get("tp1")),
                    _number(item.get("tp2")), structure.get("current_column_index"),
                    json.dumps(item, sort_keys=True, ensure_ascii=False),
                    json.dumps(structure, sort_keys=True, ensure_ascii=False),
                ))
                inserted += 1
        return {"opened": inserted, "continued": len(continued),
                "withdrawn": len(withdrawn), "expired": int(expired)}

    def close(self, reason="OBSERVER_STOP"):
        with self._lock:
            now = time.time_ns()
            with self._conn:
                self._conn.execute(
                    "UPDATE setup_occurrences SET "
                    "lifecycle=CASE WHEN scheduled_expires_wall_ns IS NOT NULL "
                    "AND scheduled_expires_wall_ns<=? THEN 'EXPIRED' ELSE 'INTERRUPTED' END,"
                    "expires_wall_ns=CASE WHEN scheduled_expires_wall_ns IS NOT NULL "
                    "AND scheduled_expires_wall_ns<=? THEN scheduled_expires_wall_ns ELSE ? END,"
                    "close_reason=CASE WHEN scheduled_expires_wall_ns IS NOT NULL "
                    "AND scheduled_expires_wall_ns<=? THEN 'THREE_CANDLE_EXPIRY' ELSE ? END "
                    "WHERE lifecycle='OPEN'", (now, now, now, now, str(reason)),
                )
            self._conn.close()
