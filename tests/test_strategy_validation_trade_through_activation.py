import contextlib
import io
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
    "support_level": 98.0,
    "resistance_level": 102.0,
}


def setup(side):
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


def fetch_row(db_path, setup_id):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return dict(
            conn.execute(
                "SELECT * FROM strategy_setups WHERE setup_id = ?", (setup_id,)
            ).fetchone()
        )


class TradeThroughActivationTests(TestCase):
    def _store(self, db_path):
        return StrategyValidationStore(
            str(db_path),
            allow_multiple_trades_per_symbol=True,
            commit_every=1,
            symbol_tick_provenance={
                "BTCUSDT": {"tick_size": 0.01, "source": "test:BTCUSDT"}
            },
        )

    def _register(self, store, side):
        with contextlib.redirect_stdout(io.StringIO()):
            return store.register_setup("BTCUSDT", setup(side), STRUCTURE, 1)

    def _update(self, store, **candle):
        with contextlib.redirect_stdout(io.StringIO()):
            store.update_pending_with_candle(
                "BTCUSDT",
                **candle,
            )

    def _run_one(self, side, candle):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            store = self._store(db_path)
            try:
                setup_id = self._register(store, side)
                self._update(store, **candle)
                store.flush()
                return fetch_row(db_path, setup_id)
            finally:
                store._conn.close()

    def test_long_requires_one_full_tick_trade_through(self):
        equality = self._run_one(
            "LONG", dict(close_ts=2, high_price=101.0, low_price=100.0, close_price=99.0)
        )
        sub_tick = self._run_one(
            "LONG", dict(close_ts=2, high_price=101.0, low_price=99.995, close_price=99.0)
        )
        full_tick = self._run_one(
            "LONG", dict(close_ts=2, high_price=101.0, low_price=99.99, close_price=101.0)
        )
        self.assertEqual(equality["activation_status"], "PENDING")
        self.assertEqual(sub_tick["activation_status"], "PENDING")
        self.assertEqual(full_tick["activation_status"], "ACTIVE")

    def test_short_requires_one_full_tick_trade_through(self):
        equality = self._run_one(
            "SHORT", dict(close_ts=2, high_price=100.0, low_price=99.0, close_price=101.0)
        )
        sub_tick = self._run_one(
            "SHORT", dict(close_ts=2, high_price=100.005, low_price=99.0, close_price=101.0)
        )
        full_tick = self._run_one(
            "SHORT", dict(close_ts=2, high_price=100.01, low_price=99.0, close_price=99.0)
        )
        self.assertEqual(equality["activation_status"], "PENDING")
        self.assertEqual(sub_tick["activation_status"], "PENDING")
        self.assertEqual(full_tick["activation_status"], "ACTIVE")

    def test_gap_through_fills_at_ideal_entry_without_improvement(self):
        for side, candle in {
            "LONG": dict(close_ts=2, high_price=99.5, low_price=98.5, close_price=99.0),
            "SHORT": dict(close_ts=2, high_price=101.5, low_price=100.5, close_price=101.0),
        }.items():
            with self.subTest(side=side):
                observed = self._run_one(side, candle)
                self.assertEqual(observed["activation_status"], "ACTIVE")
                self.assertEqual(observed["activated_price"], 100.0)

    def test_activation_candle_stop_touch_wins_even_when_target_touched(self):
        cases = {
            "LONG": dict(close_ts=2, high_price=106.5, low_price=97.5, close_price=101.0),
            "SHORT": dict(close_ts=2, high_price=102.5, low_price=93.5, close_price=99.0),
        }
        for side, candle in cases.items():
            with self.subTest(side=side):
                observed = self._run_one(side, candle)
                self.assertEqual(observed["resolution_status"], "STOPPED")
                self.assertEqual(observed["resolved_price"], observed["invalidation"])
                self.assertEqual(observed["resolution_note"], "activation_candle_stop_touch")

    def test_activation_candle_target_only_is_persisted_ambiguous_with_bounds(self):
        cases = (
            ("LONG", dict(close_ts=2, high_price=104.5, low_price=99.99, close_price=103.0), "tp1", 2.0),
            ("LONG", dict(close_ts=2, high_price=106.5, low_price=99.99, close_price=105.0), "tp2", 3.0),
            ("SHORT", dict(close_ts=2, high_price=100.01, low_price=95.5, close_price=97.0), "tp1", 2.0),
            ("SHORT", dict(close_ts=2, high_price=100.01, low_price=93.5, close_price=95.0), "tp2", 3.0),
        )
        for side, candle, target_name, optimistic_r in cases:
            with self.subTest(side=side, target=target_name):
                observed = self._run_one(side, candle)
                self.assertEqual(observed["resolution_status"], "AMBIGUOUS")
                self.assertIsNone(observed["resolved_price"])
                self.assertEqual(
                    observed["resolution_note"],
                    f"activation_candle_target_touch_without_stop:{target_name}",
                )
                self.assertEqual(observed["ambiguous_pessimistic_r"], -1.0)
                self.assertEqual(observed["ambiguous_optimistic_r"], optimistic_r)

    def test_tick_and_source_are_persisted_as_activation_provenance(self):
        observed = self._run_one(
            "LONG", dict(close_ts=2, high_price=101.0, low_price=99.99, close_price=101.0)
        )
        self.assertEqual(observed["activation_tick_size"], 0.01)
        self.assertEqual(observed["activation_tick_source"], "test:BTCUSDT")

    def test_missing_or_invalid_tick_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = StrategyValidationStore(
                str(Path(temp_dir) / "validation.db"),
                allow_multiple_trades_per_symbol=True,
                commit_every=1,
                symbol_tick_provenance={},
            )
            try:
                with self.assertRaisesRegex(ValueError, "explicit tick provenance"):
                    self._register(store, "LONG")
            finally:
                store._conn.close()

    def test_tick_configuration_is_symbol_scoped_and_requires_source(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            for provenance in (
                {"BTCUSDT": 0.01},
                {"BTCUSDT": {"tick_size": 0.01}},
                {"BTCUSDT": {"tick_size": 0, "source": "test"}},
            ):
                with self.subTest(provenance=provenance), self.assertRaises(ValueError):
                    StrategyValidationStore(
                        str(db_path), symbol_tick_provenance=provenance
                    )

    def test_exact_three_candle_expiry_is_unchanged(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            store = self._store(db_path)
            try:
                setup_id = self._register(store, "LONG")
                for close_ts in (2, 3):
                    self._update(
                        store,
                        close_ts=close_ts,
                        high_price=101.0,
                        low_price=100.0,
                        close_price=99.0,
                    )
                before = fetch_row(db_path, setup_id)
                self._update(
                    store,
                    close_ts=4,
                    high_price=101.0,
                    low_price=100.0,
                    close_price=99.0,
                )
                after = fetch_row(db_path, setup_id)
            finally:
                store._conn.close()
        self.assertEqual(before["resolution_status"], "PENDING")
        self.assertEqual(before["bars_observed"], 2)
        self.assertEqual(after["resolution_status"], "EXPIRED")
        self.assertEqual(after["bars_observed"], 3)

    def test_live_paths_remain_isolated_from_historical_ohlc_activation(self):
        for relative_path in (
            "live_binance_forward_trader.py",
            "live_mexc_forward_trader.py",
            "mexc_pole_live_trader.py",
        ):
            source = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
            self.assertNotIn("StrategyValidationStore", source)
            self.assertNotIn("activation_candle_target_touch_without_stop", source)


if __name__ == "__main__":
    import unittest

    unittest.main()
