import sqlite3
import threading
from pathlib import Path
from typing import Optional, List, Dict


class Storage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.Lock()
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_db(self):
        with self._write_lock:
            with self._connect() as conn:
                conn.execute("""
            CREATE TABLE IF NOT EXISTS candles (
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                close_time INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                PRIMARY KEY (symbol, interval, open_time)
            )
            """)
                conn.execute("""
            CREATE TABLE IF NOT EXISTS collector_state (
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                last_open_time INTEGER,
                last_close_time INTEGER,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (symbol, interval)
            )
            """)
                conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_candles_symbol_interval_close
            ON candles(symbol, interval, close_time)
            """)
                conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_candles_symbol_interval_open
            ON candles(symbol, interval, open_time)
            """)
                conn.commit()

    def upsert_candle(
        self,
        symbol: str,
        interval: str,
        open_time: int,
        close_time: int,
        open_price: float,
        high: float,
        low: float,
        close: float,
        volume: float,
    ) -> None:
        with self._write_lock:
            with self._connect() as conn:
                conn.execute("""
            INSERT INTO candles(symbol, interval, open_time, close_time, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, interval, open_time) DO UPDATE SET
                close_time=excluded.close_time,
                open=excluded.open,
                high=excluded.high,
                low=excluded.low,
                close=excluded.close,
                volume=excluded.volume
                """, (symbol, interval, open_time, close_time, open_price, high, low, close, volume))
                conn.commit()

    def save_collector_state(
        self,
        symbol: str,
        interval: str,
        last_open_time: Optional[int],
        last_close_time: Optional[int],
        updated_at: int,
    ) -> None:
        with self._write_lock:
            with self._connect() as conn:
                conn.execute("""
            INSERT INTO collector_state(symbol, interval, last_open_time, last_close_time, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(symbol, interval) DO UPDATE SET
                last_open_time=excluded.last_open_time,
                last_close_time=excluded.last_close_time,
                updated_at=excluded.updated_at
                """, (symbol, interval, last_open_time, last_close_time, updated_at))
                conn.commit()

    def get_last_open_time(self, symbol: str, interval: str) -> Optional[int]:
        with self._connect() as conn:
            row = conn.execute("""
            SELECT MAX(open_time) AS last_open_time
            FROM candles
            WHERE symbol=? AND interval=?
            """, (symbol, interval)).fetchone()
            return row["last_open_time"] if row and row["last_open_time"] is not None else None

    def count_candles(self, symbol: str, interval: str) -> int:
        with self._connect() as conn:
            row = conn.execute("""
            SELECT COUNT(*) AS n
            FROM candles
            WHERE symbol=? AND interval=?
            """, (symbol, interval)).fetchone()
            return int(row["n"])

    def get_latest_close(self, symbol: str, interval: str):
        with self._connect() as conn:
            row = conn.execute("""
            SELECT close
            FROM candles
            WHERE symbol=? AND interval=?
            ORDER BY close_time DESC
            LIMIT 1
            """, (symbol, interval)).fetchone()
            return float(row["close"]) if row else None

    def get_summary_rows(self, interval: str) -> List[Dict]:
        with self._connect() as conn:
            rows = conn.execute("""
            SELECT
                c.symbol,
                COUNT(*) AS candles,
                MAX(c.close_time) AS last_close_time,
                (
                    SELECT c2.close
                    FROM candles c2
                    WHERE c2.symbol = c.symbol AND c2.interval = c.interval
                    ORDER BY c2.close_time DESC
                    LIMIT 1
                ) AS last_close
            FROM candles c
            WHERE c.interval = ?
            GROUP BY c.symbol, c.interval
            ORDER BY c.symbol
            """, (interval,)).fetchall()
            return [dict(r) for r in rows]
