import contextlib
import inspect
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
    "support_level": 99.0,
    "resistance_level": 105.0,
}


def setup(side):
    if side == "LONG":
        invalidation, tp1, tp2 = 98.0, 104.0, 106.0
    else:
        invalidation, tp1, tp2 = 102.0, 96.0, 94.0
    return {
        "strategy": "pullback_retest",
        "side": side,
        "status": "CANDIDATE",
        "zone_low": 100.0,
        "zone_high": 100.0,
        "ideal_entry": 100.0,
        "invalidation": invalidation,
        "risk": 2.0,
        "tp1": tp1,
        "tp2": tp2,
        "rr1": 2.0,
        "rr2": 3.0,
        "pullback_quality": "HEALTHY",
        "risk_quality": "NORMAL",
        "reward_quality": "STRONG",
        "quality_score": 70.0,
        "quality_grade": "A",
        "reason": "activation-diagnostic",
        "reject_reason": None,
    }


def row(db_path, setup_id):
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


class StrategyValidationActivationDiagnosticTests(TestCase):
    def _store(self, db_path):
        return StrategyValidationStore(
            str(db_path),
            allow_multiple_trades_per_symbol=True,
            commit_every=1,
            symbol_tick_provenance={
                "BTCUSDT": {
                    "provider": "TEST",
                    "venue": "TEST_SPOT",
                    "instrument_type": "SPOT",
                    "native_symbol": "BTCUSDT",
                    "source_symbol": "BTCUSDT",
                    "tick_size": 0.01,
                    "provenance_timestamp": "2026-09-23T00:00:00Z",
                    "provenance_version": "test-v1",
                    "source": "test:BTCUSDT",
                }
            },
        )

    def _register(self, store, side):
        with contextlib.redirect_stdout(io.StringIO()):
            return store.register_setup("BTCUSDT", setup(side), STRUCTURE, 1)

    def _update(self, store, candle):
        with contextlib.redirect_stdout(io.StringIO()):
            store.update_pending_with_candle("BTCUSDT", **candle)

    def test_long_activation_uses_one_tick_trade_through_not_close(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._store(Path(temp_dir) / "validation.db")
            try:
                self.assertFalse(store._should_activate("LONG", 101.0, 100.0, 100.0, 0.01))
                self.assertFalse(store._should_activate("LONG", 101.0, 99.995, 100.0, 0.01))
                self.assertTrue(store._should_activate("LONG", 101.0, 99.99, 100.0, 0.01))
            finally:
                close_store(store)

    def test_short_activation_uses_one_tick_trade_through_not_close(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._store(Path(temp_dir) / "validation.db")
            try:
                self.assertFalse(store._should_activate("SHORT", 100.0, 99.0, 100.0, 0.01))
                self.assertFalse(store._should_activate("SHORT", 100.005, 99.0, 100.0, 0.01))
                self.assertTrue(store._should_activate("SHORT", 100.01, 99.0, 100.0, 0.01))
            finally:
                close_store(store)

    def test_candle_api_requires_tick_provenance_and_needs_no_open_improvement(self):
        parameters = inspect.signature(
            StrategyValidationStore.update_pending_with_candle
        ).parameters
        self.assertEqual(
            list(parameters),
            [
                "self",
                "symbol",
                "close_ts",
                "high_price",
                "low_price",
                "close_price",
                "tick_size",
                "tick_size_source",
            ],
        )
        self.assertNotIn("open_price", parameters)

    def test_long_gap_through_activates_at_ideal_when_close_finishes_above(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            store = self._store(db_path)
            try:
                setup_id = self._register(store, "LONG")
                self._update(
                    store,
                    dict(close_ts=2, high_price=101.0, low_price=98.5, close_price=101.0),
                )
                observed = row(db_path, setup_id)
            finally:
                close_store(store)
        self.assertEqual(observed["activation_status"], "ACTIVE")
        self.assertEqual(observed["activated_price"], 100.0)
        self.assertEqual(observed["bars_observed"], 1)

    def test_short_gap_through_activates_at_ideal_when_close_finishes_below(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            store = self._store(db_path)
            try:
                setup_id = self._register(store, "SHORT")
                self._update(
                    store,
                    dict(close_ts=2, high_price=101.5, low_price=99.0, close_price=99.0),
                )
                observed = row(db_path, setup_id)
            finally:
                close_store(store)
        self.assertEqual(observed["activation_status"], "ACTIVE")
        self.assertEqual(observed["activated_price"], 100.0)
        self.assertEqual(observed["bars_observed"], 1)

    def test_close_cross_without_trade_through_does_not_activate(self):
        scenarios = {
            "LONG": dict(
                close_ts=2, high_price=101.0, low_price=100.0, close_price=99.0
            ),
            "SHORT": dict(
                close_ts=2, high_price=100.0, low_price=99.0, close_price=101.0
            ),
        }
        for side, candle in scenarios.items():
            with self.subTest(side=side), tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "validation.db"
                store = self._store(db_path)
                try:
                    setup_id = self._register(store, side)
                    self._update(store, candle)
                    observed = row(db_path, setup_id)
                finally:
                    close_store(store)
                self.assertEqual(observed["activation_status"], "PENDING")
                self.assertIsNone(observed["activated_price"])

    def test_long_activation_candle_reuses_full_ohlc_for_outcome(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            store = self._store(db_path)
            try:
                setup_id = self._register(store, "LONG")
                self._update(
                    store,
                    dict(close_ts=2, high_price=101.0, low_price=97.5, close_price=100.0),
                )
                observed = row(db_path, setup_id)
            finally:
                close_store(store)
        self.assertEqual(observed["activation_status"], "ACTIVE")
        self.assertEqual(observed["activated_ts"], 2)
        self.assertEqual(observed["activated_price"], 100.0)
        self.assertEqual(observed["resolution_status"], "STOPPED")
        self.assertEqual(observed["resolved_ts"], 2)

    def test_short_activation_candle_reuses_full_ohlc_for_outcome(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            store = self._store(db_path)
            try:
                setup_id = self._register(store, "SHORT")
                self._update(
                    store,
                    dict(close_ts=2, high_price=102.5, low_price=99.0, close_price=100.0),
                )
                observed = row(db_path, setup_id)
            finally:
                close_store(store)
        self.assertEqual(observed["activation_status"], "ACTIVE")
        self.assertEqual(observed["activated_ts"], 2)
        self.assertEqual(observed["activated_price"], 100.0)
        self.assertEqual(observed["resolution_status"], "STOPPED")
        self.assertEqual(observed["resolved_ts"], 2)

    def test_three_touch_only_long_candles_expire_without_activation(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            store = self._store(db_path)
            try:
                setup_id = self._register(store, "LONG")
                for close_ts in (2, 3, 4):
                    self._update(
                        store,
                        dict(
                            close_ts=close_ts,
                            high_price=101.0,
                            low_price=100.0,
                            close_price=101.0,
                        ),
                    )
                observed = row(db_path, setup_id)
            finally:
                close_store(store)
        self.assertEqual(observed["activation_status"], "PENDING")
        self.assertEqual(observed["bars_observed"], 3)
        self.assertEqual(observed["resolution_status"], "EXPIRED")
        self.assertEqual(observed["resolved_ts"], 4)

    def test_three_touch_only_short_candles_expire_without_activation(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            store = self._store(db_path)
            try:
                setup_id = self._register(store, "SHORT")
                for close_ts in (2, 3, 4):
                    self._update(
                        store,
                        dict(
                            close_ts=close_ts,
                            high_price=100.0,
                            low_price=99.0,
                            close_price=99.0,
                        ),
                    )
                observed = row(db_path, setup_id)
            finally:
                close_store(store)
        self.assertEqual(observed["activation_status"], "PENDING")
        self.assertEqual(observed["bars_observed"], 3)
        self.assertEqual(observed["resolution_status"], "EXPIRED")
        self.assertEqual(observed["resolved_ts"], 4)

    def _run_sequence(self, db_path, side, candles, restart_each_candle):
        store = self._store(db_path)
        setup_id = self._register(store, side)
        if restart_each_candle:
            close_store(store)
        for candle in candles:
            if restart_each_candle:
                store = self._store(db_path)
            self._update(store, candle)
            if restart_each_candle:
                close_store(store)
        if not restart_each_candle:
            close_store(store)
        return row(db_path, setup_id)

    def test_continuous_batch_and_per_candle_restart_are_equivalent(self):
        scenarios = {
            "LONG": [
                dict(close_ts=2, high_price=101.0, low_price=100.0, close_price=101.0),
                dict(close_ts=3, high_price=104.5, low_price=99.0, close_price=100.0),
                dict(close_ts=4, high_price=106.5, low_price=99.5, close_price=103.0),
            ],
            "SHORT": [
                dict(close_ts=2, high_price=100.0, low_price=99.0, close_price=99.0),
                dict(close_ts=3, high_price=101.0, low_price=95.5, close_price=100.0),
                dict(close_ts=4, high_price=100.5, low_price=93.5, close_price=97.0),
            ],
        }
        fields = (
            "activation_status",
            "activated_ts",
            "activated_price",
            "bars_observed",
            "tp1_hit",
            "tp1_hit_ts",
            "resolution_status",
            "resolved_ts",
            "resolved_price",
            "resolution_note",
            "max_favorable_excursion",
            "max_adverse_excursion",
        )
        for side, candles in scenarios.items():
            with self.subTest(side=side), tempfile.TemporaryDirectory() as temp_dir:
                temp_dir = Path(temp_dir)
                continuous = self._run_sequence(
                    temp_dir / "continuous.db", side, candles, False
                )
                restarted = self._run_sequence(
                    temp_dir / "restarted.db", side, candles, True
                )
                self.assertEqual(
                    {field: continuous[field] for field in fields},
                    {field: restarted[field] for field in fields},
                )

    def test_historical_and_live_execution_paths_remain_isolated(self):
        app_source = (PNF_MVP_ROOT / "app.py").read_text(encoding="utf-8")
        backfill_source = (PNF_MVP_ROOT / "strategy_historical_backfill.py").read_text(
            encoding="utf-8"
        )
        binance_source = (REPO_ROOT / "live_binance_forward_trader.py").read_text(
            encoding="utf-8"
        )
        mexc_pole_source = (REPO_ROOT / "mexc_pole_live_trader.py").read_text(
            encoding="utf-8"
        )
        independent_labeler = (
            REPO_ROOT / "research_v2" / "labeling" / "label_setup_dataset.py"
        ).read_text(encoding="utf-8")
        microstructure_evaluator = (
            REPO_ROOT / "market_collector" / "microstructure_fill_evaluator.py"
        ).read_text(encoding="utf-8")

        self.assertIn("validation_store.update_pending_with_candle", app_source)
        self.assertIn("validation_store.update_pending_with_candle", backfill_source)
        self.assertIn("strategy_validation_execution", app_source)
        self.assertIn("strategy_validation_execution", backfill_source)
        self.assertNotIn("StrategyValidationStore", binance_source)
        self.assertNotIn("StrategyValidationStore", mexc_pole_source)
        self.assertIn('entry_status == "FILLED"', binance_source)
        self.assertIn("verify_entry_filled_or_open", mexc_pole_source)
        self.assertIn("return low <= ideal_entry", independent_labeler)
        self.assertIn("return high >= ideal_entry", independent_labeler)
        self.assertIn('"fill_model": "aggressor-confirmed trade-through by one tick"', microstructure_evaluator)
        self.assertIn('"classification": "TOUCHED_NO_FILL_PROOF"', microstructure_evaluator)

    def test_legacy_mexc_forward_path_treats_order_sent_as_open_without_fill_proof(self):
        source = (REPO_ROOT / "live_mexc_forward_trader.py").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            'OPEN_TRADE_STATUSES = {"OPEN", "ORDER_SENT", "POSITION_OPEN", "EXIT_PENDING"}',
            source,
        )
        self.assertIn(
            "WHERE status IN ('OPEN','ORDER_SENT','POSITION_OPEN','EXIT_PENDING')",
            source,
        )
        self.assertNotIn("verify_entry_filled_or_open", source)


if __name__ == "__main__":
    import unittest

    unittest.main()
