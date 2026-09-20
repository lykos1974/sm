"""Read-only Binance Spot BTCUSDT quote/trade evidence collector.

This standalone process uses only public market-data streams. It never imports
the candle collector or scanner, sends no orders, and writes only to its own
SQLite database.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import websockets
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency. Run: python -m pip install -r requirements-microstructure.txt"
    ) from exc


SCHEMA_VERSION = 1
SYMBOL = "BTCUSDT"
ENDPOINT = "wss://stream.binance.com:9443"


@dataclass(frozen=True)
class ReceivedMessage:
    receive_wall_ns: int
    receive_monotonic_ns: int
    stream: str
    payload: dict[str, Any]
    raw_json: str
    socket_wait_ms: float | None = None


def _decimal_text(value: Any, field: str, allow_zero: bool = False) -> str:
    text = str(value)
    try:
        number = float(text)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {field}") from exc
    if not (number >= 0 if allow_zero else number > 0):
        raise ValueError(f"invalid {field}")
    return text


def parse_combined(
    raw: str, expected_symbol: str = SYMBOL, socket_wait_ms: float | None = None
) -> ReceivedMessage:
    wall_ns, monotonic_ns = time.time_ns(), time.monotonic_ns()
    outer = json.loads(raw)
    payload = outer.get("data")
    if not isinstance(payload, dict):
        raise ValueError("combined message has no object payload")
    if str(payload.get("s") or "").upper() != expected_symbol:
        raise ValueError("unexpected symbol")
    return ReceivedMessage(
        wall_ns, monotonic_ns, str(outer.get("stream") or ""), payload, raw,
        socket_wait_ms,
    )


class Store:
    def __init__(self, path: str | Path, symbol: str = SYMBOL):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.symbol = symbol
        self.conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()
        self.pending: list[tuple[str, tuple[Any, ...]]] = []
        self.active_session_id: str | None = None
        self.last_flush = time.monotonic()
        row = self.conn.execute(
            "SELECT MAX(agg_trade_id) FROM agg_trades WHERE symbol=?", (symbol,)
        ).fetchone()
        self.last_agg_id = int(row[0]) if row and row[0] is not None else None

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata(
                    key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS sessions(
                    session_id TEXT PRIMARY KEY, symbol TEXT NOT NULL,
                    endpoint TEXT NOT NULL, started_wall_ns INTEGER NOT NULL,
                    started_monotonic_ns INTEGER NOT NULL, ended_wall_ns INTEGER,
                    end_reason TEXT, status TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS book_ticker(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL, symbol TEXT NOT NULL,
                    update_id INTEGER NOT NULL, receive_wall_ns INTEGER NOT NULL,
                    receive_monotonic_ns INTEGER NOT NULL,
                    bid_price TEXT NOT NULL, bid_qty TEXT NOT NULL,
                    ask_price TEXT NOT NULL, ask_qty TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    FOREIGN KEY(session_id) REFERENCES sessions(session_id));
                CREATE INDEX IF NOT EXISTS idx_book_receive
                    ON book_ticker(symbol,receive_wall_ns);
                CREATE TABLE IF NOT EXISTS agg_trades(
                    symbol TEXT NOT NULL, agg_trade_id INTEGER NOT NULL,
                    session_id TEXT NOT NULL, event_time_ms INTEGER NOT NULL,
                    trade_time_ms INTEGER NOT NULL, first_trade_id INTEGER NOT NULL,
                    last_trade_id INTEGER NOT NULL, price TEXT NOT NULL,
                    quantity TEXT NOT NULL, buyer_is_maker INTEGER NOT NULL,
                    receive_wall_ns INTEGER NOT NULL,
                    receive_monotonic_ns INTEGER NOT NULL, raw_json TEXT NOT NULL,
                    PRIMARY KEY(symbol,agg_trade_id),
                    FOREIGN KEY(session_id) REFERENCES sessions(session_id));
                CREATE INDEX IF NOT EXISTS idx_trade_time
                    ON agg_trades(symbol,trade_time_ms);
                CREATE TABLE IF NOT EXISTS stream_anomalies(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL, symbol TEXT NOT NULL,
                    receive_wall_ns INTEGER NOT NULL, anomaly_type TEXT NOT NULL,
                    previous_id INTEGER, current_id INTEGER, details TEXT NOT NULL,
                    FOREIGN KEY(session_id) REFERENCES sessions(session_id));
                CREATE TABLE IF NOT EXISTS runtime_diagnostics(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL, receive_wall_ns INTEGER NOT NULL,
                    diagnostic_type TEXT NOT NULL, duration_ms REAL NOT NULL,
                    socket_wait_ms REAL, pending_events INTEGER NOT NULL,
                    details TEXT NOT NULL,
                    FOREIGN KEY(session_id) REFERENCES sessions(session_id));
                """
            )
            current = self.conn.execute(
                "SELECT value FROM metadata WHERE key='schema_version'"
            ).fetchone()
            if current and int(current[0]) != SCHEMA_VERSION:
                raise RuntimeError("unsupported database schema")
            self.conn.execute(
                "INSERT OR IGNORE INTO metadata VALUES('schema_version',?)",
                (str(SCHEMA_VERSION),),
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO metadata VALUES('scope','public_data_no_orders')"
            )

    def start_session(self, endpoint: str) -> str:
        session_id = uuid.uuid4().hex
        self.active_session_id = session_id
        with self.conn:
            self.conn.execute(
                "INSERT INTO sessions VALUES(?,?,?,?,?,?,?,?)",
                (session_id, self.symbol, endpoint, time.time_ns(),
                 time.monotonic_ns(), None, None, "OPEN"),
            )
        return session_id

    def record_runtime(
        self, session_id: str, kind: str, duration_ms: float,
        socket_wait_ms: float | None = None, details: str = "{}",
    ) -> None:
        self.pending.append(("runtime", (
            session_id, time.time_ns(), kind, duration_ms, socket_wait_ms,
            len(self.pending), details,
        )))

    def end_session(self, session_id: str, reason: str) -> None:
        self.flush()
        with self.conn:
            self.conn.execute(
                "UPDATE sessions SET ended_wall_ns=?,end_reason=?,status='CLOSED' "
                "WHERE session_id=? AND status='OPEN'",
                (time.time_ns(), reason[:1000], session_id),
            )
        if self.active_session_id == session_id:
            self.active_session_id = None

    def queue(self, session_id: str, msg: ReceivedMessage) -> None:
        data, stream = msg.payload, msg.stream.lower()
        if stream.endswith("@bookticker"):
            bid = _decimal_text(data.get("b"), "bid")
            ask = _decimal_text(data.get("a"), "ask")
            if float(bid) > float(ask):
                raise ValueError("crossed bookTicker")
            self.pending.append(("book", (
                session_id, self.symbol, int(data["u"]), msg.receive_wall_ns,
                msg.receive_monotonic_ns, bid,
                _decimal_text(data.get("B"), "bid_qty", True), ask,
                _decimal_text(data.get("A"), "ask_qty", True), msg.raw_json,
            )))
            return
        if not stream.endswith("@aggtrade"):
            raise ValueError("unexpected stream")
        agg_id = int(data["a"])
        event_latency_ms = msg.receive_wall_ns / 1_000_000 - int(data["E"])
        if event_latency_ms > 250:
            self.record_runtime(
                session_id, "STALE_AGG_TRADE", event_latency_ms,
                msg.socket_wait_ms,
                json.dumps({"agg_trade_id": agg_id}, separators=(",", ":")),
            )
        if self.last_agg_id is not None and agg_id > self.last_agg_id + 1:
            details = json.dumps(
                {"missing_first": self.last_agg_id + 1, "missing_last": agg_id - 1},
                separators=(",", ":"),
            )
            self.pending.append(("anomaly", (
                session_id, self.symbol, msg.receive_wall_ns,
                "POSSIBLE_AGG_TRADE_ID_GAP", self.last_agg_id, agg_id, details,
            )))
        elif self.last_agg_id is not None and agg_id <= self.last_agg_id:
            self.pending.append(("anomaly", (
                session_id, self.symbol, msg.receive_wall_ns,
                "DUPLICATE_OR_OUT_OF_ORDER_AGG_TRADE", self.last_agg_id,
                agg_id, "{}",
            )))
        self.last_agg_id = max(agg_id, self.last_agg_id or agg_id)
        self.pending.append(("trade", (
            self.symbol, agg_id, session_id, int(data["E"]), int(data["T"]),
            int(data["f"]), int(data["l"]),
            _decimal_text(data.get("p"), "price"),
            _decimal_text(data.get("q"), "quantity"),
            1 if bool(data.get("m")) else 0, msg.receive_wall_ns,
            msg.receive_monotonic_ns, msg.raw_json,
        )))

    def flush_if_due(self, max_events: int, max_seconds: float) -> None:
        if len(self.pending) >= max_events or time.monotonic() - self.last_flush >= max_seconds:
            self.flush()

    def flush_due(self, max_events: int, max_seconds: float) -> bool:
        return bool(self.pending) and (
            len(self.pending) >= max_events
            or time.monotonic() - self.last_flush >= max_seconds
        )

    def detach_pending(self) -> list[tuple[str, tuple[Any, ...]]]:
        events, self.pending = self.pending, []
        return events

    def restore_pending(self, events: list[tuple[str, tuple[Any, ...]]]) -> None:
        self.pending = events + self.pending

    def write_batch(self, events: list[tuple[str, tuple[Any, ...]]]) -> None:
        if not events:
            self.last_flush = time.monotonic()
            return
        flush_started_ns = time.monotonic_ns()
        with self.conn:
            for kind, row in events:
                if kind == "book":
                    self.conn.execute(
                        "INSERT INTO book_ticker(session_id,symbol,update_id,receive_wall_ns,"
                        "receive_monotonic_ns,bid_price,bid_qty,ask_price,ask_qty,raw_json) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?)", row)
                elif kind == "trade":
                    self.conn.execute(
                        "INSERT OR IGNORE INTO agg_trades VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        row)
                elif kind == "anomaly":
                    self.conn.execute(
                        "INSERT INTO stream_anomalies(session_id,symbol,receive_wall_ns,"
                        "anomaly_type,previous_id,current_id,details) VALUES(?,?,?,?,?,?,?)",
                        row)
                else:
                    self.conn.execute(
                        "INSERT INTO runtime_diagnostics(session_id,receive_wall_ns,"
                        "diagnostic_type,duration_ms,socket_wait_ms,pending_events,details) "
                        "VALUES(?,?,?,?,?,?,?)", row)
        flush_duration_ms = (time.monotonic_ns() - flush_started_ns) / 1_000_000
        if flush_duration_ms > 50 and self.active_session_id is not None:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO runtime_diagnostics(session_id,receive_wall_ns,"
                    "diagnostic_type,duration_ms,socket_wait_ms,pending_events,details) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (self.active_session_id, time.time_ns(), "SLOW_SQLITE_FLUSH",
                     flush_duration_ms, None, len(events), "{}"),
                )
        self.last_flush = time.monotonic()

    def flush(self) -> None:
        if not self.pending:
            self.last_flush = time.monotonic()
            return
        events = self.detach_pending()
        try:
            self.write_batch(events)
        except BaseException:
            self.restore_pending(events)
            raise

    def counts(self) -> tuple[int, int, int]:
        return tuple(int(self.conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
                     for name in ("book_ticker", "agg_trades", "stream_anomalies"))

    def close(self) -> None:
        self.flush()
        self.conn.close()


class AsyncFlusher:
    """Keep SQLite writes off the WebSocket event loop, one atomic batch at a time."""

    def __init__(self, store: Store):
        self.store = store
        self.task: asyncio.Task[None] | None = None
        self.inflight: list[tuple[str, tuple[Any, ...]]] | None = None

    def _finish_completed(self) -> None:
        if self.task is None or not self.task.done():
            return
        try:
            self.task.result()
        except BaseException:
            if self.inflight:
                self.store.restore_pending(self.inflight)
            self.task = None
            self.inflight = None
            raise
        self.task = None
        self.inflight = None

    def submit_if_due(self, max_events: int, max_seconds: float) -> None:
        self._finish_completed()
        if self.task is not None or not self.store.flush_due(max_events, max_seconds):
            return
        self.inflight = self.store.detach_pending()
        self.task = asyncio.create_task(
            asyncio.to_thread(self.store.write_batch, self.inflight)
        )

    async def flush_all(self) -> None:
        if self.task is not None:
            try:
                await self.task
            except BaseException:
                if self.inflight:
                    self.store.restore_pending(self.inflight)
                self.task = None
                self.inflight = None
                raise
            self.task = None
            self.inflight = None
        events = self.store.detach_pending()
        if events:
            try:
                await asyncio.to_thread(self.store.write_batch, events)
            except BaseException:
                self.store.restore_pending(events)
                raise


def stream_url(endpoint: str = ENDPOINT, symbol: str = SYMBOL) -> str:
    name = symbol.lower()
    return endpoint.rstrip("/") + f"/stream?streams={name}@bookTicker/{name}@aggTrade"


async def collect_session(
    store: Store, flusher: AsyncFlusher, args: argparse.Namespace, stop: asyncio.Event
) -> None:
    url = stream_url(args.endpoint, args.symbol)
    session_id = store.start_session(url)
    started, reason = time.monotonic(), "normal_stop"
    monitor_task = None
    try:
        async with websockets.connect(
            url, open_timeout=20, close_timeout=10, ping_interval=None,
            max_queue=100_000, max_size=1_000_000,
        ) as socket:
            print(f"CONNECTED session={session_id} symbol={args.symbol}", flush=True)

            async def monitor_event_loop() -> None:
                expected = time.monotonic() + 0.1
                while not stop.is_set():
                    await asyncio.sleep(0.1)
                    now = time.monotonic()
                    lag_ms = max(0.0, (now - expected) * 1000)
                    if lag_ms > 50:
                        store.record_runtime(session_id, "EVENT_LOOP_STALL", lag_ms)
                    expected = now + 0.1

            monitor_task = asyncio.create_task(monitor_event_loop())
            while not stop.is_set():
                if time.monotonic() - started >= args.rotate_seconds:
                    reason = "proactive_24h_rotation"
                    return
                try:
                    wait_started_ns = time.monotonic_ns()
                    raw = await asyncio.wait_for(socket.recv(), timeout=1)
                except asyncio.TimeoutError:
                    flusher.submit_if_due(args.flush_events, args.flush_seconds)
                    continue
                if not isinstance(raw, str):
                    raise ValueError("unexpected binary message")
                socket_wait_ms = (time.monotonic_ns() - wait_started_ns) / 1_000_000
                store.queue(
                    session_id, parse_combined(raw, args.symbol, socket_wait_ms)
                )
                flusher.submit_if_due(args.flush_events, args.flush_seconds)
    except BaseException as exc:
        reason = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        if monitor_task is not None:
            monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await monitor_task
        await flusher.flush_all()
        store.end_session(session_id, reason)
        print(f"SESSION_CLOSED session={session_id} reason={reason}", flush=True)


async def run(args: argparse.Namespace) -> None:
    stop = asyncio.Event()
    auto_stop_task = None
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    if args.max_seconds > 0:
        async def stop_after_delay() -> None:
            await asyncio.sleep(args.max_seconds)
            print(f"AUTO_STOP max_seconds={args.max_seconds:g}", flush=True)
            stop.set()

        auto_stop_task = asyncio.create_task(stop_after_delay())
    store, backoff = Store(args.database, args.symbol), 1.0
    flusher = AsyncFlusher(store)
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
        if auto_stop_task is not None:
            auto_stop_task.cancel()
        await flusher.flush_all()
        counts = store.counts()
        store.close()
        print(f"STOPPED database={store.path} books={counts[0]} trades={counts[1]} anomalies={counts[2]}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=SYMBOL)
    parser.add_argument("--database", default="microstructure_btcusdt.db")
    parser.add_argument("--endpoint", default=ENDPOINT)
    parser.add_argument("--flush-events", type=int, default=500)
    parser.add_argument("--flush-seconds", type=float, default=1.0)
    parser.add_argument("--rotate-seconds", type=float, default=86100.0)
    parser.add_argument(
        "--max-seconds",
        type=float,
        default=0.0,
        help="stop automatically after this many seconds (0 means run until stopped)",
    )
    args = parser.parse_args()
    args.symbol = args.symbol.upper()
    if args.symbol != SYMBOL:
        parser.error("this first reviewed collector is locked to BTCUSDT")
    if min(args.flush_events, args.flush_seconds, args.rotate_seconds) <= 0:
        parser.error("flush and rotation values must be positive")
    if args.max_seconds < 0:
        parser.error("max-seconds cannot be negative")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
