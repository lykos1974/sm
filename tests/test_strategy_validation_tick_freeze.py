import contextlib
import io
import math
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest import TestCase


REPO_ROOT = Path(__file__).resolve().parents[1]
PNF_MVP_ROOT = REPO_ROOT / "pnf_mvp"
if str(PNF_MVP_ROOT) not in sys.path:
    sys.path.insert(0, str(PNF_MVP_ROOT))

from strategy_validation import StrategyValidationStore  # noqa: E402


STRUCTURE = {
    "trend_state": "BULLISH",
    "trend_regime": "BULLISH_REGIME",
    "immediate_slope": "BEARISH_PULLBACK",
    "breakout_context": "POST_BREAKOUT_PULLBACK",
    "is_extended_move": False,
    "active_leg_boxes": 2,
    "current_column_index": 1,
    "current_column_kind": "O",
    "current_column_top": 101.0,
    "current_column_bottom": 100.0,
}


def setup(side="LONG"):
    return {
        "strategy": "pullback_retest",
        "side": side,
        "status": "CANDIDATE",
        "zone_low": 100.0,
        "zone_high": 100.0,
        "ideal_entry": 100.0,
        "invalidation": 98.0 if side == "LONG" else 102.0,
        "risk": 2.0,
        "tp1": 104.0 if side == "LONG" else 96.0,
        "tp2": 106.0 if side == "LONG" else 94.0,
        "rr1": 2.0,
        "rr2": 3.0,
        "quality_score": 70.0,
    }


def provenance(size=0.01, source="exchange-info:BTCUSDT", symbol=None):
    item = {"tick_size": size, "source": source}
    if symbol is not None:
        item["symbol"] = symbol
    return {"BTCUSDT": item}


def fetch_row(db_path, setup_id):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return dict(
            conn.execute(
                "SELECT * FROM strategy_setups WHERE setup_id = ?", (setup_id,)
            ).fetchone()
        )


def close_store(store):
    store.flush()
    store._conn.close()


class TickFreezeTests(TestCase):
    def _register(self, store, symbol="BTCUSDT", side="LONG", reference_ts=1):
        with contextlib.redirect_stdout(io.StringIO()):
            return store.register_setup(symbol, setup(side), STRUCTURE, reference_ts)

    def _update(self, store, **candle):
        with contextlib.redirect_stdout(io.StringIO()):
            store.update_pending_with_candle("BTCUSDT", **candle)

    def test_tick_is_persisted_at_registration_for_every_lifecycle(self):
        scenarios = {
            "PENDING": [],
            "ACTIVE": [dict(close_ts=2, high_price=101.0, low_price=99.99, close_price=100.0)],
            "EXPIRED": [
                dict(close_ts=2, high_price=101.0, low_price=100.0, close_price=101.0),
                dict(close_ts=3, high_price=101.0, low_price=100.0, close_price=101.0),
                dict(close_ts=4, high_price=101.0, low_price=100.0, close_price=101.0),
            ],
            "AMBIGUOUS": [dict(close_ts=2, high_price=104.5, low_price=99.99, close_price=103.0)],
            "STOPPED": [dict(close_ts=2, high_price=101.0, low_price=97.5, close_price=99.0)],
        }
        for lifecycle, candles in scenarios.items():
            with self.subTest(lifecycle=lifecycle), tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "validation.db"
                store = StrategyValidationStore(
                    str(db_path),
                    allow_multiple_trades_per_symbol=True,
                    commit_every=1,
                    symbol_tick_provenance=provenance(),
                )
                try:
                    setup_id = self._register(store)
                    registered = fetch_row(db_path, setup_id)
                    self.assertEqual(registered["activation_tick_size"], 0.01)
                    self.assertEqual(
                        registered["activation_tick_source"],
                        "exchange-info:BTCUSDT",
                    )
                    for candle in candles:
                        self._update(store, **candle)
                    observed = fetch_row(db_path, setup_id)
                finally:
                    close_store(store)
                self.assertEqual(observed["activation_tick_size"], 0.01)
                self.assertEqual(
                    observed["activation_tick_source"], "exchange-info:BTCUSDT"
                )

    def test_restart_and_settings_change_do_not_change_existing_setup_tick(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            original = StrategyValidationStore(
                str(db_path),
                allow_multiple_trades_per_symbol=True,
                commit_every=1,
                symbol_tick_provenance=provenance(0.01, "v1:BTCUSDT"),
            )
            setup_id = self._register(original)
            close_store(original)

            changed = StrategyValidationStore(
                str(db_path),
                allow_multiple_trades_per_symbol=True,
                commit_every=1,
                symbol_tick_provenance=provenance(0.1, "v2:BTCUSDT"),
            )
            try:
                self._update(
                    changed,
                    close_ts=2,
                    high_price=101.0,
                    low_price=99.99,
                    close_price=100.0,
                )
                observed = fetch_row(db_path, setup_id)
            finally:
                close_store(changed)

        self.assertEqual(observed["activation_status"], "ACTIVE")
        self.assertEqual(observed["activation_tick_size"], 0.01)
        self.assertEqual(observed["activation_tick_source"], "v1:BTCUSDT")

    def test_non_finite_non_positive_and_missing_ticks_are_rejected(self):
        invalid_values = (None, 0, -0.01, math.nan, math.inf, -math.inf)
        with tempfile.TemporaryDirectory() as temp_dir:
            for index, value in enumerate(invalid_values):
                with self.subTest(value=value), self.assertRaisesRegex(
                    ValueError, "finite positive tick_size"
                ):
                    StrategyValidationStore(
                        str(Path(temp_dir) / f"invalid-{index}.db"),
                        symbol_tick_provenance=provenance(value),
                    )

    def test_source_symbol_mismatch_and_unknown_symbol_registration_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "symbol mismatch"):
                StrategyValidationStore(
                    str(Path(temp_dir) / "mismatch.db"),
                    symbol_tick_provenance=provenance(symbol="ETHUSDT"),
                )

            store = StrategyValidationStore(
                str(Path(temp_dir) / "unknown.db"),
                symbol_tick_provenance=provenance(),
            )
            try:
                with self.assertRaisesRegex(ValueError, "explicit tick provenance"):
                    self._register(store, symbol="ETHUSDT")
            finally:
                close_store(store)

    def test_override_cannot_change_frozen_tick_or_supply_unknown_symbol(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            original = StrategyValidationStore(
                str(db_path),
                allow_multiple_trades_per_symbol=True,
                commit_every=1,
                symbol_tick_provenance=provenance(),
            )
            setup_id = self._register(original)
            close_store(original)

            changed = StrategyValidationStore(
                str(db_path),
                allow_multiple_trades_per_symbol=True,
                commit_every=1,
                symbol_tick_provenance={},
            )
            try:
                before = fetch_row(db_path, setup_id)
                for tick_size, source, message in (
                    (0.1, "exchange-info:BTCUSDT", "unknown-symbol override"),
                    (0.01, "other-source:BTCUSDT", "unknown-symbol override"),
                ):
                    with self.subTest(tick_size=tick_size, source=source), self.assertRaisesRegex(
                        ValueError, message
                    ):
                        with contextlib.redirect_stdout(io.StringIO()):
                            changed.update_pending_with_candle(
                                "BTCUSDT",
                                2,
                                101.0,
                                99.99,
                                100.0,
                                tick_size=tick_size,
                                tick_size_source=source,
                            )
                self.assertEqual(fetch_row(db_path, setup_id), before)
            finally:
                close_store(changed)

            configured = StrategyValidationStore(
                str(db_path),
                allow_multiple_trades_per_symbol=True,
                commit_every=1,
                symbol_tick_provenance=provenance(),
            )
            try:
                for tick_size, source in (
                    (0.1, "exchange-info:BTCUSDT"),
                    (0.01, "other-source:BTCUSDT"),
                ):
                    with self.subTest(configured=(tick_size, source)), self.assertRaisesRegex(
                        ValueError, "does not match frozen"
                    ):
                        with contextlib.redirect_stdout(io.StringIO()):
                            configured.update_pending_with_candle(
                                "BTCUSDT",
                                2,
                                101.0,
                                99.99,
                                100.0,
                                tick_size=tick_size,
                                tick_size_source=source,
                            )
            finally:
                close_store(configured)


if __name__ == "__main__":
    import unittest

    unittest.main()
