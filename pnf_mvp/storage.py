import csv
import json
import os
import sqlite3
from pathlib import Path


class Storage:
    def __init__(self, db_path):
        self.db_path = db_path
        Path(os.path.dirname(db_path) or ".").mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        cur = self.conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS symbols (
                symbol TEXT PRIMARY KEY,
                exchange TEXT,
                asset_type TEXT,
                base_quote TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS candles (
                symbol TEXT,
                interval TEXT,
                open_time INTEGER,
                close_time INTEGER,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                PRIMARY KEY(symbol, interval, open_time)
            );

            CREATE TABLE IF NOT EXISTS pnf_state (
                symbol TEXT,
                profile_name TEXT,
                state_json TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(symbol, profile_name)
            );

            CREATE TABLE IF NOT EXISTS pnf_columns (
                symbol TEXT,
                profile_name TEXT,
                idx INTEGER,
                kind TEXT,
                top REAL,
                bottom REAL,
                start_ts INTEGER,
                end_ts INTEGER,
                PRIMARY KEY(symbol, profile_name, idx)
            );

            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                profile_name TEXT,
                signal_type TEXT,
                trigger REAL,
                column_idx INTEGER,
                note TEXT,
                ts INTEGER
            );

            CREATE TABLE IF NOT EXISTS scanner_snapshot (
                symbol TEXT,
                profile_name TEXT,
                market_state TEXT,
                signal TEXT,
                last_price REAL,
                score INTEGER,
                updated_at TEXT,
                PRIMARY KEY(symbol, profile_name)
            );

            CREATE INDEX IF NOT EXISTS idx_candles_symbol_interval_open_time
            ON candles(symbol, interval, open_time);

            CREATE INDEX IF NOT EXISTS idx_candles_symbol_interval_close_time
            ON candles(symbol, interval, close_time);

            CREATE INDEX IF NOT EXISTS idx_pnf_columns_symbol_profile_idx
            ON pnf_columns(symbol, profile_name, idx);

            CREATE INDEX IF NOT EXISTS idx_signals_symbol_profile_ts
            ON signals(symbol, profile_name, ts);

            CREATE INDEX IF NOT EXISTS idx_signals_symbol_profile_column
            ON signals(symbol, profile_name, signal_type, column_idx);
            """
        )

        self._dedupe_signals(cur)

        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_signals_symbol_profile_type_column
            ON signals(symbol, profile_name, signal_type, column_idx)
            """
        )

        self.conn.commit()

    def _dedupe_signals(self, cur):
        cur.execute(
            """
            DELETE FROM signals
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM signals
                GROUP BY symbol, profile_name, signal_type, column_idx
            )
            """
        )

    def upsert_symbol(self, symbol, exchange, asset_type, base_quote):
        self.conn.execute(
            """
            INSERT INTO symbols(symbol, exchange, asset_type, base_quote)
            VALUES(?,?,?,?)
            ON CONFLICT(symbol) DO UPDATE SET
              exchange=excluded.exchange,
              asset_type=excluded.asset_type,
              base_quote=excluded.base_quote,
              updated_at=CURRENT_TIMESTAMP
            """,
            (symbol, exchange, asset_type, base_quote),
        )
        self.conn.commit()

    def insert_candle(self, symbol, interval, open_time, close_time, open_price, high, low, close, volume):
        self.conn.execute(
            """
            INSERT INTO candles(symbol, interval, open_time, close_time, open, high, low, close, volume)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, interval, open_time) DO UPDATE SET
              close_time=excluded.close_time,
              open=excluded.open,
              high=excluded.high,
              low=excluded.low,
              close=excluded.close,
              volume=excluded.volume
            """,
            (symbol, interval, open_time, close_time, open_price, high, low, close, volume),
        )
        self.conn.commit()

    def load_recent_candles(self, symbol, limit=None):
        if limit is None:
            cur = self.conn.execute(
                """
                SELECT close_time, close, high, low
                FROM candles
                WHERE symbol=? AND interval='1m'
                ORDER BY open_time ASC
                """,
                (symbol,),
            )
            return [dict(r) for r in cur.fetchall()]

        cur = self.conn.execute(
            """
            SELECT close_time, close, high, low
            FROM candles
            WHERE symbol=? AND interval='1m'
            ORDER BY open_time DESC
            LIMIT ?
            """,
            (symbol, limit),
        )
        rows = list(reversed([dict(r) for r in cur.fetchall()]))
        return rows

    def load_candles_after(self, symbol, after_close_ts=None):
        if after_close_ts is None:
            return self.load_recent_candles(symbol, None)

        cur = self.conn.execute(
            """
            SELECT close_time, close, high, low
            FROM candles
            WHERE symbol=? AND interval='1m' AND close_time > ?
            ORDER BY open_time ASC
            """,
            (symbol, int(after_close_ts)),
        )
        return [dict(r) for r in cur.fetchall()]

    def save_state(self, symbol, profile, state):
        self.conn.execute(
            """
            INSERT INTO pnf_state(symbol, profile_name, state_json)
            VALUES(?,?,?)
            ON CONFLICT(symbol, profile_name) DO UPDATE SET
              state_json=excluded.state_json,
              updated_at=CURRENT_TIMESTAMP
            """,
            (symbol, profile.name, json.dumps(state)),
        )
        self.conn.commit()

    def replace_columns(self, symbol, profile, columns):
        self.conn.execute("DELETE FROM pnf_columns WHERE symbol=? AND profile_name=?", (symbol, profile.name))
        self.conn.executemany(
            """
            INSERT INTO pnf_columns(symbol, profile_name, idx, kind, top, bottom, start_ts, end_ts)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            [(symbol, profile.name, c.idx, c.kind, c.top, c.bottom, c.start_ts, c.end_ts) for c in columns],
        )
        self.conn.commit()

    def insert_signal(self, symbol, profile, signal):
        self.conn.execute(
            """
            INSERT OR IGNORE INTO signals(symbol, profile_name, signal_type, trigger, column_idx, note, ts)
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                symbol,
                profile.name,
                signal["type"],
                signal["trigger"],
                signal["column_idx"],
                signal["note"],
                signal["timestamp"],
            ),
        )
        self.conn.commit()

    def load_recent_signals(self, limit=20):
        cur = self.conn.execute(
            """
            SELECT symbol, profile_name, signal_type, trigger, column_idx, note, ts
            FROM signals
            ORDER BY ts DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]

    def upsert_scanner_snapshot(self, symbol, profile_name, market_state, signal, last_price, score, updated_at):
        self.conn.execute(
            """
            INSERT INTO scanner_snapshot(symbol, profile_name, market_state, signal, last_price, score, updated_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(symbol, profile_name) DO UPDATE SET
              market_state=excluded.market_state,
              signal=excluded.signal,
              last_price=excluded.last_price,
              score=excluded.score,
              updated_at=excluded.updated_at
            """,
            (symbol, profile_name, market_state, signal, last_price, score, updated_at),
        )
        self.conn.commit()

    def load_state(self, symbol, profile_name):
        cur = self.conn.execute(
            """
            SELECT state_json
            FROM pnf_state
            WHERE symbol=? AND profile_name=?
            """,
            (symbol, profile_name),
        )
        row = cur.fetchone()
        if not row:
            return None

        try:
            return json.loads(row["state_json"])
        except Exception:
            return None

    def load_columns(self, symbol, profile_name):
        cur = self.conn.execute(
            """
            SELECT idx, kind, top, bottom, start_ts, end_ts
            FROM pnf_columns
            WHERE symbol=? AND profile_name=?
            ORDER BY idx
            """,
            (symbol, profile_name),
        )
        return [dict(r) for r in cur.fetchall()]

    def load_scanner_snapshot(self, symbol, profile_name):
        cur = self.conn.execute(
            """
            SELECT market_state, signal, last_price, score, updated_at
            FROM scanner_snapshot
            WHERE symbol=? AND profile_name=?
            """,
            (symbol, profile_name),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def export_columns_csv(self, symbol, profile_name):
        out_dir = Path("exports")
        out_dir.mkdir(exist_ok=True)
        path = out_dir / f"{symbol}_{profile_name}_columns.csv"
        cur = self.conn.execute(
            "SELECT idx, kind, top, bottom, start_ts, end_ts FROM pnf_columns WHERE symbol=? AND profile_name=? ORDER BY idx",
            (symbol, profile_name),
        )
        rows = [dict(r) for r in cur.fetchall()]
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["idx", "kind", "top", "bottom", "start_ts", "end_ts"])
            writer.writeheader()
            writer.writerows(rows)
        return str(path.resolve())
