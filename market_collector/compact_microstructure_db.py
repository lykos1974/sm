"""Create and verify a compact copy of a microstructure evidence database.

The source is opened read-only. The destination is built through a temporary
file and renamed only after count and content-hash equivalence checks pass.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
from pathlib import Path


TABLES = ("sessions", "book_ticker", "agg_trades", "stream_anomalies")


def _hash_rows(rows) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(row, separators=(",", ":"), ensure_ascii=False).encode())
        digest.update(b"\n")
    return digest.hexdigest()


def _source_hashes(conn: sqlite3.Connection) -> dict[str, str]:
    return {
        "sessions": _hash_rows(conn.execute(
            "SELECT session_id,symbol,endpoint,started_wall_ns,started_monotonic_ns,"
            "ended_wall_ns,end_reason,status FROM sessions ORDER BY session_id"
        )),
        "book_ticker": _hash_rows(conn.execute(
            "SELECT session_id,update_id,receive_wall_ns,receive_monotonic_ns,bid_price,"
            "bid_qty,ask_price,ask_qty FROM book_ticker ORDER BY id"
        )),
        "agg_trades": _hash_rows(conn.execute(
            "SELECT session_id,agg_trade_id,event_time_ms,trade_time_ms,first_trade_id,"
            "last_trade_id,price,quantity,buyer_is_maker,receive_wall_ns,"
            "receive_monotonic_ns,raw_json FROM agg_trades ORDER BY agg_trade_id"
        )),
        "stream_anomalies": _hash_rows(conn.execute(
            "SELECT session_id,receive_wall_ns,anomaly_type,previous_id,current_id,details "
            "FROM stream_anomalies ORDER BY id"
        )),
    }


def _destination_hashes(conn: sqlite3.Connection) -> dict[str, str]:
    return {
        "sessions": _hash_rows(conn.execute(
            "SELECT session_key,symbol,endpoint,started_wall_ns,started_monotonic_ns,"
            "ended_wall_ns,end_reason,status FROM sessions ORDER BY session_key"
        )),
        "book_ticker": _hash_rows(conn.execute(
            "SELECT s.session_key,b.update_id,b.receive_wall_ns,b.receive_monotonic_ns,"
            "b.bid_price,b.bid_qty,b.ask_price,b.ask_qty FROM book_ticker b "
            "JOIN sessions s ON s.id=b.session_id ORDER BY b.id"
        )),
        "agg_trades": _hash_rows(conn.execute(
            "SELECT s.session_key,t.agg_trade_id,t.event_time_ms,t.trade_time_ms,"
            "t.first_trade_id,t.last_trade_id,t.price,t.quantity,t.buyer_is_maker,"
            "t.receive_wall_ns,t.receive_monotonic_ns,t.raw_json FROM agg_trades t "
            "JOIN sessions s ON s.id=t.session_id ORDER BY t.agg_trade_id"
        )),
        "stream_anomalies": _hash_rows(conn.execute(
            "SELECT s.session_key,a.receive_wall_ns,a.anomaly_type,a.previous_id,a.current_id,"
            "a.details FROM stream_anomalies a JOIN sessions s ON s.id=a.session_id "
            "ORDER BY a.id"
        )),
    }


def _schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=OFF;
        PRAGMA synchronous=OFF;
        CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID;
        INSERT INTO metadata VALUES('schema_version','2');
        INSERT INTO metadata VALUES('scope','public_data_no_orders');
        INSERT INTO metadata VALUES('book_raw_json','omitted_structured_fields_preserved');
        CREATE TABLE sessions(
            id INTEGER PRIMARY KEY, session_key TEXT NOT NULL UNIQUE,
            symbol TEXT NOT NULL, endpoint TEXT NOT NULL,
            started_wall_ns INTEGER NOT NULL, started_monotonic_ns INTEGER NOT NULL,
            ended_wall_ns INTEGER, end_reason TEXT, status TEXT NOT NULL);
        CREATE TABLE book_ticker(
            id INTEGER PRIMARY KEY, session_id INTEGER NOT NULL,
            update_id INTEGER NOT NULL, receive_wall_ns INTEGER NOT NULL,
            receive_monotonic_ns INTEGER NOT NULL, bid_price TEXT NOT NULL,
            bid_qty TEXT NOT NULL, ask_price TEXT NOT NULL, ask_qty TEXT NOT NULL,
            FOREIGN KEY(session_id) REFERENCES sessions(id));
        CREATE TABLE agg_trades(
            agg_trade_id INTEGER PRIMARY KEY, session_id INTEGER NOT NULL,
            event_time_ms INTEGER NOT NULL, trade_time_ms INTEGER NOT NULL,
            first_trade_id INTEGER NOT NULL, last_trade_id INTEGER NOT NULL,
            price TEXT NOT NULL, quantity TEXT NOT NULL,
            buyer_is_maker INTEGER NOT NULL, receive_wall_ns INTEGER NOT NULL,
            receive_monotonic_ns INTEGER NOT NULL, raw_json TEXT NOT NULL,
            FOREIGN KEY(session_id) REFERENCES sessions(id));
        CREATE TABLE stream_anomalies(
            id INTEGER PRIMARY KEY, session_id INTEGER NOT NULL,
            receive_wall_ns INTEGER NOT NULL, anomaly_type TEXT NOT NULL,
            previous_id INTEGER, current_id INTEGER, details TEXT NOT NULL,
            FOREIGN KEY(session_id) REFERENCES sessions(id));
        CREATE TABLE runtime_diagnostics(
            id INTEGER PRIMARY KEY, session_id INTEGER NOT NULL,
            receive_wall_ns INTEGER NOT NULL, diagnostic_type TEXT NOT NULL,
            duration_ms REAL NOT NULL, socket_wait_ms REAL,
            pending_events INTEGER NOT NULL, details TEXT NOT NULL,
            FOREIGN KEY(session_id) REFERENCES sessions(id));
        """
    )


def compact(source: str | Path, destination: str | Path) -> dict[str, object]:
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    if destination.exists():
        raise FileExistsError(destination)
    temporary = destination.with_suffix(destination.suffix + ".building")
    if temporary.exists():
        raise FileExistsError(temporary)
    src = sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)
    dst = sqlite3.connect(temporary)
    try:
        if src.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("source integrity_check failed")
        source_hashes = _source_hashes(src)
        _schema(dst)
        session_map = {}
        with dst:
            for new_id, row in enumerate(src.execute(
                "SELECT session_id,symbol,endpoint,started_wall_ns,started_monotonic_ns,"
                "ended_wall_ns,end_reason,status FROM sessions ORDER BY rowid"
            ), 1):
                session_map[row[0]] = new_id
                dst.execute("INSERT INTO sessions VALUES(?,?,?,?,?,?,?,?,?)", (new_id, *row))
            book_rows = (
                (row[0], session_map[row[1]], *row[2:]) for row in src.execute(
                    "SELECT id,session_id,update_id,receive_wall_ns,receive_monotonic_ns,"
                    "bid_price,bid_qty,ask_price,ask_qty FROM book_ticker ORDER BY id"
                )
            )
            dst.executemany("INSERT INTO book_ticker VALUES(?,?,?,?,?,?,?,?,?)", book_rows)
            trade_rows = (
                (row[1], session_map[row[0]], *row[2:]) for row in src.execute(
                    "SELECT session_id,agg_trade_id,event_time_ms,trade_time_ms,first_trade_id,"
                    "last_trade_id,price,quantity,buyer_is_maker,receive_wall_ns,"
                    "receive_monotonic_ns,raw_json FROM agg_trades ORDER BY agg_trade_id"
                )
            )
            dst.executemany("INSERT INTO agg_trades VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", trade_rows)
            anomaly_rows = (
                (row[0], session_map[row[1]], *row[2:]) for row in src.execute(
                    "SELECT id,session_id,receive_wall_ns,anomaly_type,previous_id,current_id,"
                    "details FROM stream_anomalies ORDER BY id"
                )
            )
            dst.executemany("INSERT INTO stream_anomalies VALUES(?,?,?,?,?,?,?)", anomaly_rows)
            has_runtime = src.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_diagnostics'"
            ).fetchone()
            if has_runtime:
                runtime_rows = (
                    (row[0], session_map[row[1]], *row[2:]) for row in src.execute(
                        "SELECT id,session_id,receive_wall_ns,diagnostic_type,duration_ms,"
                        "socket_wait_ms,pending_events,details FROM runtime_diagnostics ORDER BY id"
                    )
                )
                dst.executemany("INSERT INTO runtime_diagnostics VALUES(?,?,?,?,?,?,?,?)", runtime_rows)
            dst.executescript(
                "CREATE INDEX idx_book_receive ON book_ticker(receive_wall_ns);"
                "CREATE INDEX idx_trade_time ON agg_trades(trade_time_ms);"
            )
        destination_hashes = _destination_hashes(dst)
        if source_hashes != destination_hashes:
            raise RuntimeError("content equivalence hash mismatch")
        counts = {
            table: int(dst.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in TABLES
        }
        dst.execute("PRAGMA journal_mode=DELETE")
        dst.execute("VACUUM")
        dst.close()
        src.close()
        os.replace(temporary, destination)
        source_bytes, destination_bytes = source.stat().st_size, destination.stat().st_size
        return {
            "source": str(source), "destination": str(destination),
            "source_bytes": source_bytes, "destination_bytes": destination_bytes,
            "reduction_percent": 100 * (1 - destination_bytes / source_bytes),
            "counts": counts, "equivalence_sha256": source_hashes,
        }
    except BaseException:
        dst.close()
        src.close()
        if temporary.exists():
            temporary.unlink()
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", nargs="?", default="microstructure_btcusdt.db")
    parser.add_argument("destination", nargs="?", default="microstructure_btcusdt_compact.db")
    args = parser.parse_args()
    try:
        report = compact(args.source, args.destination)
    except (FileNotFoundError, FileExistsError, RuntimeError, sqlite3.Error) as exc:
        print(f"COMPACTION_FAIL {type(exc).__name__}: {exc}")
        return 1
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print("COMPACTION_PASS source_unchanged=true equivalence_verified=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
