"""UTC-daily supervisor for compact BTCUSDT evidence and verified Parquet archives."""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from archive_microstructure_parquet import archive
from microstructure_collector import (
    ENDPOINT,
    SYMBOL,
    AsyncFlusher,
    CompactStore,
    collect_session,
)
from verify_microstructure_archive import verify


def utc_day(now: datetime | None = None) -> str:
    return (now or datetime.now(timezone.utc)).date().isoformat()


def seconds_until_next_utc_day(now: datetime | None = None) -> float:
    now = now or datetime.now(timezone.utc)
    tomorrow = datetime.combine(now.date() + timedelta(days=1), datetime.min.time(), timezone.utc)
    return max(0.001, (tomorrow - now).total_seconds())


def tree_bytes(*roots: Path) -> int:
    total = 0
    for root in roots:
        if root.exists():
            total += sum(path.stat().st_size for path in root.rglob("*") if path.is_file())
    return total


def recover_aborted_sessions(database: Path) -> int:
    if not database.is_file():
        return 0
    conn = sqlite3.connect(database)
    try:
        version = conn.execute(
            "SELECT value FROM metadata WHERE key='schema_version'"
        ).fetchone()
        if not version or version[0] != "2":
            raise RuntimeError(f"unexpected database schema: {database}")
        with conn:
            cursor = conn.execute(
                "UPDATE sessions SET ended_wall_ns=?,end_reason=?,status='ABORTED' "
                "WHERE status='OPEN' OR ended_wall_ns IS NULL",
                (time.time_ns(), "unclean_process_exit_detected_on_restart"),
            )
        return cursor.rowcount
    finally:
        conn.close()


def archive_and_verify(database: Path, archive_root: Path) -> dict[str, object]:
    report = archive(database, archive_root)
    checked = verify(report["archive"], report["manifest_sha256"])
    return {
        "database": str(database),
        "archive": report["archive"],
        "archive_bytes": report["archive_bytes"],
        "manifest_sha256": report["manifest_sha256"],
        "quality": checked["quality"],
        "verified": True,
        "source_retained": database.is_file(),
    }


async def collect_one_day(
    database: Path, stop: asyncio.Event, endpoint: str,
    flush_events: int, flush_seconds: float, rotate_seconds: float,
) -> None:
    recovered = recover_aborted_sessions(database)
    if recovered:
        print(f"RECOVERED_ABORTED_SESSIONS count={recovered} database={database}", flush=True)
    store = CompactStore(database, SYMBOL)
    flusher = AsyncFlusher(store)
    args = SimpleNamespace(
        symbol=SYMBOL, endpoint=endpoint, flush_events=flush_events,
        flush_seconds=flush_seconds, rotate_seconds=rotate_seconds,
    )
    backoff = 1.0
    try:
        while not stop.is_set():
            try:
                await collect_session(store, flusher, args, stop)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"DISCONNECTED {type(exc).__name__}: {exc}", flush=True)
                if not stop.is_set():
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=backoff)
                    except asyncio.TimeoutError:
                        pass
                    backoff = min(30.0, backoff * 2)
    finally:
        await flusher.flush_all()
        counts = store.counts()
        store.close()
        print(
            f"DAY_STORE_CLOSED database={database} books={counts[0]} "
            f"trades={counts[1]} anomalies={counts[2]}", flush=True,
        )


async def run(args: argparse.Namespace) -> None:
    live_root, archive_root = Path(args.live_root).resolve(), Path(args.archive_root).resolve()
    live_root.mkdir(parents=True, exist_ok=True)
    archive_root.mkdir(parents=True, exist_ok=True)
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except (NotImplementedError, RuntimeError):
            pass
    started = time.monotonic()
    while not shutdown.is_set():
        used = tree_bytes(live_root, archive_root)
        limit = int(args.max_storage_gb * 1024**3)
        if used >= limit:
            raise RuntimeError(f"storage cap reached: {used} >= {limit} bytes")
        day = utc_day()
        database = live_root / SYMBOL / f"{day}.db"
        database.parent.mkdir(parents=True, exist_ok=True)
        day_stop = asyncio.Event()
        remaining_total = None
        if args.max_seconds > 0:
            remaining_total = args.max_seconds - (time.monotonic() - started)
            if remaining_total <= 0:
                shutdown.set()
                break
        until_midnight = seconds_until_next_utc_day()
        wait_seconds = min(until_midnight, remaining_total) if remaining_total else until_midnight

        async def stop_day_after_delay() -> None:
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=wait_seconds)
            except asyncio.TimeoutError:
                pass
            day_stop.set()

        timer = asyncio.create_task(stop_day_after_delay())
        await collect_one_day(
            database, day_stop, args.endpoint, args.flush_events,
            args.flush_seconds, args.rotate_seconds,
        )
        timer.cancel()
        if shutdown.is_set():
            break
        reached_midnight = wait_seconds == until_midnight and utc_day() != day
        reached_limit = remaining_total is not None and wait_seconds == remaining_total
        if reached_midnight:
            result = await asyncio.to_thread(archive_and_verify, database, archive_root)
            print("DAILY_ARCHIVE_VERIFIED " + json.dumps(result, separators=(",", ":")), flush=True)
        if reached_limit:
            shutdown.set()
    print("DAILY_RUNNER_STOPPED current_partial_day_retained=true", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-root", default="microstructure_live")
    parser.add_argument("--archive-root", default="microstructure_daily_archive")
    parser.add_argument("--max-storage-gb", type=float, default=20.0)
    parser.add_argument("--max-seconds", type=float, default=0.0)
    parser.add_argument("--endpoint", default=ENDPOINT)
    parser.add_argument("--flush-events", type=int, default=500)
    parser.add_argument("--flush-seconds", type=float, default=1.0)
    parser.add_argument("--rotate-seconds", type=float, default=86100.0)
    args = parser.parse_args()
    if min(args.max_storage_gb, args.flush_events, args.flush_seconds, args.rotate_seconds) <= 0:
        parser.error("storage, flush, and rotation values must be positive")
    if args.max_seconds < 0:
        parser.error("max-seconds cannot be negative")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass
    except (RuntimeError, sqlite3.Error) as exc:
        print(f"DAILY_RUNNER_FAIL {type(exc).__name__}: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
