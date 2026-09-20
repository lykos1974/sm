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


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _quality_summary(conn: sqlite3.Connection) -> dict[str, object]:
    sessions = {
        row[0]: {
            "status": row[1], "book_rows": 0, "trade_rows": 0,
            "trade_latency_ms": {}, "trades_over_250ms": 0,
            "session_start_gaps": 0, "in_session_anomalies": 0,
            "event_loop_stalls": 0, "recorded_stale_trades": 0,
        }
        for row in conn.execute("SELECT session_key,status FROM sessions")
    }
    for key, count in conn.execute(
        "SELECT s.session_key,COUNT(*) FROM book_ticker b "
        "JOIN sessions s ON s.id=b.session_id GROUP BY s.session_key"
    ):
        sessions[key]["book_rows"] = count
    latency_values: dict[str, list[float]] = {key: [] for key in sessions}
    for key, receive_ns, event_ms in conn.execute(
        "SELECT s.session_key,t.receive_wall_ns,t.event_time_ms FROM agg_trades t "
        "JOIN sessions s ON s.id=t.session_id ORDER BY t.agg_trade_id"
    ):
        latency_values[key].append(receive_ns / 1_000_000 - event_ms)
    for key, values in latency_values.items():
        sessions[key]["trade_rows"] = len(values)
        sessions[key]["trades_over_250ms"] = sum(value > 250 for value in values)
        sessions[key]["trade_latency_ms"] = {
            "p50": _percentile(values, 0.50), "p95": _percentile(values, 0.95),
            "p99": _percentile(values, 0.99), "max": _percentile(values, 1),
        }
    for key, kind, current_id, first_id in conn.execute(
        "SELECT s.session_key,a.anomaly_type,a.current_id,"
        "(SELECT MIN(t.agg_trade_id) FROM agg_trades t WHERE t.session_id=a.session_id) "
        "FROM stream_anomalies a JOIN sessions s ON s.id=a.session_id"
    ):
        if kind == "POSSIBLE_AGG_TRADE_ID_GAP" and current_id == first_id:
            sessions[key]["session_start_gaps"] += 1
        else:
            sessions[key]["in_session_anomalies"] += 1
    for key, kind, count in conn.execute(
        "SELECT s.session_key,r.diagnostic_type,COUNT(*) FROM runtime_diagnostics r "
        "JOIN sessions s ON s.id=r.session_id GROUP BY s.session_key,r.diagnostic_type"
    ):
        if kind == "EVENT_LOOP_STALL":
            sessions[key]["event_loop_stalls"] = count
        elif kind == "STALE_AGG_TRADE":
            sessions[key]["recorded_stale_trades"] = count
    totals = {"observation_complete": 0, "contains_indeterminate_intervals": 0,
              "structurally_incomplete": 0}
    for details in sessions.values():
        if details["status"] != "CLOSED" or details["in_session_anomalies"]:
            quality = "STRUCTURALLY_INCOMPLETE"
            totals["structurally_incomplete"] += 1
        elif details["trades_over_250ms"] or details["event_loop_stalls"]:
            quality = "CONTAINS_INDETERMINATE_INTERVALS"
            totals["contains_indeterminate_intervals"] += 1
        else:
            quality = "OBSERVATION_COMPLETE"
            totals["observation_complete"] += 1
        details["quality_status"] = quality
        details["requires_interval_filtering"] = quality != "OBSERVATION_COMPLETE"
    return {
        "trade_stale_threshold_ms": 250,
        "book_ticker_exchange_timestamp_available": False,
        "book_ticker_timestamp_note": "receive time only; uncertain during local or upstream buffering",
        "session_totals": totals,
        "sessions": sessions,
    }


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
                "quality": _quality_summary(conn),
                "tables": tables,
            }
            manifest_path = building / "manifest.json"
            manifest_path.write_text(
                json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
            )
            manifest["manifest_sha256"] = _file_sha256(manifest_path)
            (building / "manifest.sha256").write_text(
                manifest["manifest_sha256"] + "  manifest.json\n", encoding="ascii"
            )
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
