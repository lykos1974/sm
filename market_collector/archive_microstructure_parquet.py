"""Export a closed compact microstructure SQLite database to verified Parquet/ZSTD."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency. Run: python -m pip install -r requirements-archive.txt"
    ) from exc


SPECS = {
    "sessions": (
        "SELECT session_key,symbol,endpoint,started_wall_ns,started_monotonic_ns,"
        "ended_wall_ns,end_reason,status FROM sessions ORDER BY session_key"
    ),
    "book_ticker": (
        "SELECT b.id,s.session_key,b.update_id,b.receive_wall_ns,b.receive_monotonic_ns,"
        "b.bid_price,b.bid_qty,b.ask_price,b.ask_qty FROM book_ticker b "
        "JOIN sessions s ON s.id=b.session_id ORDER BY b.id"
    ),
    "agg_trades": (
        "SELECT t.agg_trade_id,s.session_key,t.event_time_ms,t.trade_time_ms,"
        "t.first_trade_id,t.last_trade_id,t.price,t.quantity,t.buyer_is_maker,"
        "t.receive_wall_ns,t.receive_monotonic_ns,t.raw_json FROM agg_trades t "
        "JOIN sessions s ON s.id=t.session_id ORDER BY t.agg_trade_id"
    ),
    "stream_anomalies": (
        "SELECT a.id,s.session_key,a.receive_wall_ns,a.anomaly_type,a.previous_id,"
        "a.current_id,a.details FROM stream_anomalies a "
        "JOIN sessions s ON s.id=a.session_id ORDER BY a.id"
    ),
    "runtime_diagnostics": (
        "SELECT r.id,s.session_key,r.receive_wall_ns,r.diagnostic_type,r.duration_ms,"
        "r.socket_wait_ms,r.pending_events,r.details FROM runtime_diagnostics r "
        "JOIN sessions s ON s.id=r.session_id ORDER BY r.id"
    ),
}


def _update_hash(digest, rows) -> None:
    for row in rows:
        digest.update(json.dumps(row, separators=(",", ":"), ensure_ascii=False).encode())
        digest.update(b"\n")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _event_day(conn: sqlite3.Connection) -> tuple[str, int, int]:
    row = conn.execute(
        "SELECT MIN(ts),MAX(ts) FROM ("
        "SELECT receive_wall_ns AS ts FROM book_ticker UNION ALL "
        "SELECT receive_wall_ns FROM agg_trades)"
    ).fetchone()
    if not row or row[0] is None:
        raise RuntimeError("no market evidence to archive")
    first = datetime.fromtimestamp(row[0] / 1_000_000_000, timezone.utc).date()
    last = datetime.fromtimestamp(row[1] / 1_000_000_000, timezone.utc).date()
    if first != last:
        raise RuntimeError("source spans multiple UTC dates; daily archive refused")
    return first.isoformat(), int(row[0]), int(row[1])


def _write_table(
    conn: sqlite3.Connection, name: str, query: str, path: Path, chunk_rows: int
) -> dict[str, object]:
    cursor = conn.execute(query)
    columns = [item[0] for item in cursor.description]
    source_hash = hashlib.sha256()
    count = 0
    writer = None
    try:
        while True:
            rows = cursor.fetchmany(chunk_rows)
            if not rows:
                break
            normalized = [tuple(row) for row in rows]
            _update_hash(source_hash, normalized)
            table = pa.Table.from_pylist([dict(zip(columns, row)) for row in normalized])
            if writer is None:
                writer = pq.ParquetWriter(
                    path, table.schema, compression="zstd", compression_level=9,
                    use_dictionary=True, write_statistics=True,
                )
            writer.write_table(table)
            count += len(normalized)
        if writer is None:
            empty = pa.table({column: pa.array([], type=pa.string()) for column in columns})
            writer = pq.ParquetWriter(path, empty.schema, compression="zstd")
            writer.write_table(empty)
    finally:
        if writer is not None:
            writer.close()
    destination_hash = hashlib.sha256()
    verified_count = 0
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=chunk_rows, columns=columns):
        values = batch.to_pydict()
        rows = [tuple(values[column][index] for column in columns) for index in range(batch.num_rows)]
        _update_hash(destination_hash, rows)
        verified_count += batch.num_rows
    if count != verified_count or source_hash.hexdigest() != destination_hash.hexdigest():
        raise RuntimeError(f"Parquet round-trip mismatch: {name}")
    return {
        "rows": count,
        "evidence_sha256": source_hash.hexdigest(),
        "file_sha256": _file_sha256(path),
        "bytes": path.stat().st_size,
        "columns": columns,
    }


def archive(
    source: str | Path, destination_root: str | Path = "microstructure_archive",
    chunk_rows: int = 100_000,
) -> dict[str, object]:
    source = Path(source).resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    conn = sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)
    try:
        if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("source integrity_check failed")
        version = conn.execute(
            "SELECT value FROM metadata WHERE key='schema_version'"
        ).fetchone()
        if not version or version[0] != "2":
            raise RuntimeError("only verified compact schema version 2 can be archived")
        open_sessions = conn.execute(
            "SELECT COUNT(*) FROM sessions WHERE status!='CLOSED' OR ended_wall_ns IS NULL"
        ).fetchone()[0]
        if open_sessions:
            raise RuntimeError("open session present; archive refused")
        day, first_ns, last_ns = _event_day(conn)
        symbol = conn.execute("SELECT DISTINCT symbol FROM sessions").fetchall()
        if len(symbol) != 1:
            raise RuntimeError("archive must contain exactly one symbol")
        symbol = symbol[0][0]
        final_dir = Path(destination_root).resolve() / symbol / day
        building = final_dir.with_name(final_dir.name + ".building")
        if final_dir.exists() or building.exists():
            raise FileExistsError(final_dir if final_dir.exists() else building)
        building.mkdir(parents=True)
        try:
            tables = {}
            for name, query in SPECS.items():
                tables[name] = _write_table(
                    conn, name, query, building / f"{name}.parquet", chunk_rows
                )
            manifest = {
                "archive_schema_version": 1,
                "source_schema_version": 2,
                "symbol": symbol,
                "utc_date": day,
                "first_receive_wall_ns": first_ns,
                "last_receive_wall_ns": last_ns,
                "source_database": source.name,
                "source_bytes": source.stat().st_size,
                "parquet_compression": "zstd",
                "tables": tables,
            }
            manifest_path = building / "manifest.json"
            manifest_path.write_text(
                json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
            )
            manifest["manifest_sha256"] = _file_sha256(manifest_path)
            total_bytes = sum(path.stat().st_size for path in building.iterdir())
            manifest["archive_bytes"] = total_bytes
            os.replace(building, final_dir)
            return {"archive": str(final_dir), **manifest}
        except BaseException:
            shutil.rmtree(building, ignore_errors=True)
            raise
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", nargs="?", default="microstructure_btcusdt_compact.db")
    parser.add_argument("--destination-root", default="microstructure_archive")
    parser.add_argument("--chunk-rows", type=int, default=100_000)
    args = parser.parse_args()
    if args.chunk_rows <= 0:
        parser.error("chunk-rows must be positive")
    try:
        report = archive(args.source, args.destination_root, args.chunk_rows)
    except (FileNotFoundError, FileExistsError, RuntimeError, sqlite3.Error) as exc:
        print(f"ARCHIVE_FAIL {type(exc).__name__}: {exc}")
        return 1
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print("ARCHIVE_PASS source_unchanged=true parquet_round_trip_verified=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
