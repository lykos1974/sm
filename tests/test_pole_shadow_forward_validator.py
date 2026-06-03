import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pole_shadow_forward_validator as shadow


def _conn(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def test_shadow_tables_and_duplicate_protection(tmp_path: Path) -> None:
    db = tmp_path / "shadow.sqlite"
    conn = _conn(db)
    shadow.init_shadow_tables(conn)
    setup = shadow.ShadowSetup(
        symbol="TEST",
        profile_name="TEST_bs1_rev3",
        pattern_name="LOW_POLE",
        direction="LONG",
        setup_key="TEST|1",
        pole_column_index=2,
        reversal_column_index=3,
        confirmation_column_index=4,
        signal_ts=30,
        entry_after_ts=40,
        box_size=1.0,
    )

    assert shadow.insert_pending_setup(conn, setup) is True
    assert shadow.insert_pending_setup(conn, setup) is False

    tables = {row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert {"pole_shadow_trades", "pole_shadow_events"}.issubset(tables)
    states = [row["state"] for row in conn.execute("SELECT state FROM pole_shadow_trades")]
    assert states == ["PENDING_ENTRY"]
    events = [row["event_type"] for row in conn.execute("SELECT event_type FROM pole_shadow_events ORDER BY id")]
    assert events == ["SETUP_REGISTERED", "DUPLICATE_ACTIVE_SYMBOL_BLOCKED"]


def test_next_column_open_entry_and_break_even_exit(tmp_path: Path) -> None:
    db = tmp_path / "shadow.sqlite"
    conn = _conn(db)
    shadow.init_shadow_tables(conn)
    setup = shadow.ShadowSetup(
        symbol="TEST",
        profile_name="TEST_bs1_rev3",
        pattern_name="LOW_POLE",
        direction="LONG",
        setup_key="TEST|be",
        pole_column_index=2,
        reversal_column_index=3,
        confirmation_column_index=4,
        signal_ts=30,
        entry_after_ts=40,
        box_size=1.0,
    )
    assert shadow.insert_pending_setup(conn, setup)
    candles = [
        shadow.Candle(close_time=41, open=100.0, high=101.0, low=99.5, close=100.5),
        shadow.Candle(close_time=42, open=100.5, high=106.0, low=104.0, close=105.0),
        shadow.Candle(close_time=43, open=105.0, high=105.0, low=100.0, close=101.0),
    ]

    shadow.open_pending_trades(conn, {"TEST": candles})
    opened = conn.execute("SELECT * FROM pole_shadow_trades").fetchone()
    assert opened["state"] == "OPEN"
    assert opened["entry_model"] == "NEXT_COLUMN_OPEN_ENTRY"
    assert opened["entry_price"] == 100.0
    assert opened["initial_stop_price"] == 97.0
    assert opened["target_price"] == 107.5
    assert opened["break_even_trigger_price"] == 106.0

    shadow.update_open_trades(conn, {"TEST": candles})
    closed = conn.execute("SELECT * FROM pole_shadow_trades").fetchone()
    assert closed["state"] == "BREAK_EVEN_EXIT"
    assert closed["realized_r"] == 0.0
    events = [row["event_type"] for row in conn.execute("SELECT event_type FROM pole_shadow_events ORDER BY id")]
    assert events == ["SETUP_REGISTERED", "ENTRY_FILLED_SHADOW", "BREAK_EVEN_ARMED", "BREAK_EVEN_EXIT"]


def test_daily_summary_file_is_shadow_only(tmp_path: Path) -> None:
    db = tmp_path / "shadow.sqlite"
    output = tmp_path / "summary.md"
    conn = _conn(db)
    shadow.init_shadow_tables(conn)
    shadow.record_event(conn, trade_id=None, setup_key=None, symbol="TEST", event_type="NOOP")
    conn.commit()

    summary = shadow.write_daily_summary(conn, output)

    assert summary["mode"] == "SHADOW_ONLY_NO_EXCHANGE_NO_DEMO_NO_API_KEYS"
    text = output.read_text()
    assert "No exchange orders, demo orders, API keys" in text
    assert "NEXT_COLUMN_OPEN_ENTRY" in text
    assert "fixed 3-box stop" in text
    assert "fixed 2.5R target" in text


def test_process_once_uses_dedicated_shadow_db_without_live_order_code(tmp_path: Path) -> None:
    candle_db = tmp_path / "candles.sqlite"
    with sqlite3.connect(str(candle_db)) as conn:
        conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, open REAL, high REAL, low REAL, close REAL)")
        conn.executemany(
            "INSERT INTO candles VALUES (?,?,?,?,?,?,?)",
            [("TEST", "1m", idx, 100.0, 101.0, 99.0, 100.0) for idx in range(1, 5)],
        )
    shadow_db = tmp_path / "dedicated-shadow.sqlite"

    summary = shadow.process_once(
        argparse.Namespace(
            candle_db=str(candle_db),
            shadow_db=str(shadow_db),
            symbol=["TEST"],
            interval="1m",
            profile_name="TEST_bs1_rev3",
            box_size=1.0,
            reversal_boxes=3,
            candle_limit=50,
        )
    )

    assert shadow_db.exists()
    assert summary["mode"] == "SHADOW_ONLY_NO_EXCHANGE_NO_DEMO_NO_API_KEYS"
