import sqlite3
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
PNF_MVP = ROOT / "pnf_mvp"
if str(PNF_MVP) not in sys.path:
    sys.path.insert(0, str(PNF_MVP))

from pnf_engine import PnFColumn, PnFProfile
from storage import Storage


def _checkpoint_inputs(watermark=120_000):
    profile = PnFProfile("BTCUSDT", 100.0, 3)
    state = {
        "last_price": 50_100.0,
        "last_processed_close_ts": watermark,
        "signals": [],
    }
    columns = [PnFColumn(0, "X", 50_100.0, 50_000.0, 60_000, watermark)]
    snapshot = {
        "state": "RANGE",
        "signal": "NONE",
        "score": 50,
        "updated": "2026-09-21T00:02:00Z",
    }
    return profile, state, columns, snapshot


def test_checkpoint_round_trip_is_complete(tmp_path):
    store = Storage(str(tmp_path / "scanner.db"))
    profile, state, columns, snapshot = _checkpoint_inputs()

    store.save_checkpoint("BTCUSDT", profile, state, columns, [], snapshot)
    loaded_state, loaded_columns = store.load_checkpoint("BTCUSDT", profile.name)

    assert loaded_state == state
    assert loaded_columns == [
        {
            "idx": 0,
            "kind": "X",
            "top": 50_100.0,
            "bottom": 50_000.0,
            "start_ts": 60_000,
            "end_ts": 120_000,
        }
    ]


def test_checkpoint_refuses_watermark_regression_without_partial_write(tmp_path):
    store = Storage(str(tmp_path / "scanner.db"))
    profile, state, columns, snapshot = _checkpoint_inputs(120_000)
    store.save_checkpoint("BTCUSDT", profile, state, columns, [], snapshot)

    stale_state = {**state, "last_price": 49_900.0, "last_processed_close_ts": 60_000}
    stale_columns = [PnFColumn(0, "O", 50_000.0, 49_900.0, 60_000, 60_000)]
    with pytest.raises(ValueError, match="Stale checkpoint refused"):
        store.save_checkpoint(
            "BTCUSDT", profile, stale_state, stale_columns, [], snapshot
        )

    loaded_state, loaded_columns = store.load_checkpoint("BTCUSDT", profile.name)
    assert loaded_state == state
    assert loaded_columns[0]["kind"] == "X"
    assert loaded_columns[0]["end_ts"] == 120_000


def test_checkpoint_rejects_non_contiguous_columns_before_database_change(tmp_path):
    store = Storage(str(tmp_path / "scanner.db"))
    profile, state, _, snapshot = _checkpoint_inputs()
    bad_columns = [PnFColumn(1, "X", 50_100.0, 50_000.0, 60_000, 120_000)]

    with pytest.raises(ValueError, match="Non-contiguous"):
        store.save_checkpoint("BTCUSDT", profile, state, bad_columns, [], snapshot)

    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM pnf_state").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM pnf_columns").fetchone()[0] == 0
