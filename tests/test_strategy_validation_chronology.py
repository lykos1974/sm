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


def setup():
    return {
        "strategy": "pullback_retest",
        "side": "LONG",
        "status": "CANDIDATE",
        "zone_low": 100.0,
        "zone_high": 100.0,
        "ideal_entry": 100.0,
        "invalidation": 98.0,
        "risk": 2.0,
        "tp1": 104.0,
        "tp2": 106.0,
        "rr1": 2.0,
        "rr2": 3.0,
        "pullback_quality": "HEALTHY",
        "risk_quality": "NORMAL",
        "reward_quality": "STRONG",
        "quality_score": 70.0,
        "quality_grade": "A",
        "reason": "chronology-test",
        "reject_reason": None,
    }


def fetch_row(db_path, setup_id):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return dict(conn.execute("SELECT * FROM strategy_setups WHERE setup_id = ?", (setup_id,)).fetchone())
    finally:
        conn.close()


class StrategyValidationChronologyTests(TestCase):
    def _store(self):
        temp_dir = tempfile.TemporaryDirectory()
        db_path = Path(temp_dir.name) / "validation.db"
        store = StrategyValidationStore(str(db_path), allow_multiple_trades_per_symbol=True, commit_every=1)
        return temp_dir, db_path, store

    def test_activation_candle_is_evaluated_for_stop(self):
        temp_dir, db_path, store = self._store()
        try:
            setup_id = store.register_setup("BTCUSDT", setup(), STRUCTURE, 1)
            store.update_pending_with_candle(
                "BTCUSDT", 2, high_price=101.0, low_price=97.5, close_price=100.0
            )
            store.flush()
            row = fetch_row(db_path, setup_id)
        finally:
            store.flush()
            store._conn.close()
            temp_dir.cleanup()

        self.assertEqual(row["activation_status"], "ACTIVE")
        self.assertEqual(row["resolution_status"], "STOPPED")
        self.assertEqual(row["resolved_ts"], 2)

    def test_unfilled_setup_expires_after_exactly_three_candles(self):
        temp_dir, db_path, store = self._store()
        try:
            setup_id = store.register_setup("BTCUSDT", setup(), STRUCTURE, 1)
            store.update_pending_with_candle("BTCUSDT", 2, 103.0, 101.0, 102.0)
            store.update_pending_with_candle("BTCUSDT", 3, 103.0, 101.0, 102.0)
            before_expiry = fetch_row(db_path, setup_id)
            store.update_pending_with_candle("BTCUSDT", 4, 103.0, 101.0, 102.0)
            store.flush()
            expired = fetch_row(db_path, setup_id)
        finally:
            store.flush()
            store._conn.close()
            temp_dir.cleanup()

        self.assertEqual(before_expiry["bars_observed"], 2)
        self.assertEqual(before_expiry["resolution_status"], "PENDING")
        self.assertEqual(expired["bars_observed"], 3)
        self.assertEqual(expired["resolution_status"], "EXPIRED")
        self.assertEqual(expired["resolved_ts"], 4)

    def test_long_already_armed_be_wins_same_candle_be_and_tp2_touch(self):
        temp_dir, _db_path, store = self._store()
        try:
            be_price = store._breakeven_price("LONG", 100.0)
            result = store._resolve_long_after_tp1(
                low_price=be_price - 0.5,
                high_price=106.5,
                close_price=103.0,
                be_price=be_price,
                tp2=106.0,
            )
        finally:
            store._conn.close()
            temp_dir.cleanup()

        self.assertEqual(
            result,
            ("TP1_PARTIAL_THEN_BE", be_price, "same_candle_be_and_tp2_be_first"),
        )

    def test_short_already_armed_be_wins_same_candle_be_and_tp2_touch(self):
        temp_dir, _db_path, store = self._store()
        try:
            be_price = store._breakeven_price("SHORT", 100.0)
            result = store._resolve_short_after_tp1(
                low_price=93.5,
                high_price=be_price + 0.5,
                close_price=97.0,
                be_price=be_price,
                tp2=94.0,
            )
        finally:
            store._conn.close()
            temp_dir.cleanup()

        self.assertEqual(
            result,
            ("TP1_PARTIAL_THEN_BE", be_price, "same_candle_be_and_tp2_be_first"),
        )

    def test_after_tp1_boundary_touches_and_single_touch_outcomes(self):
        temp_dir, _db_path, store = self._store()
        try:
            long_be = store._breakeven_price("LONG", 100.0)
            short_be = store._breakeven_price("SHORT", 100.0)
            cases = [
                (
                    store._resolve_long_after_tp1(long_be, 106.0, 103.0, long_be, 106.0),
                    ("TP1_PARTIAL_THEN_BE", long_be, "same_candle_be_and_tp2_be_first"),
                ),
                (
                    store._resolve_short_after_tp1(94.0, short_be, 97.0, short_be, 94.0),
                    ("TP1_PARTIAL_THEN_BE", short_be, "same_candle_be_and_tp2_be_first"),
                ),
                (
                    store._resolve_long_after_tp1(long_be, 105.0, 102.0, long_be, 106.0),
                    ("TP1_PARTIAL_THEN_BE", long_be, "tp1_partial_then_breakeven"),
                ),
                (
                    store._resolve_long_after_tp1(long_be + 0.5, 106.0, 105.0, long_be, 106.0),
                    ("TP2", 106.0, "tp2_hit_after_tp1"),
                ),
                (
                    store._resolve_short_after_tp1(95.0, short_be, 98.0, short_be, 94.0),
                    ("TP1_PARTIAL_THEN_BE", short_be, "tp1_partial_then_breakeven"),
                ),
                (
                    store._resolve_short_after_tp1(94.0, short_be - 0.5, 95.0, short_be, 94.0),
                    ("TP2", 94.0, "tp2_hit_after_tp1"),
                ),
                (
                    store._resolve_long_after_tp1(long_be + 0.5, 105.0, 103.0, long_be, 106.0),
                    (None, None, None),
                ),
                (
                    store._resolve_short_after_tp1(95.0, short_be - 0.5, 97.0, short_be, 94.0),
                    (None, None, None),
                ),
            ]
        finally:
            store._conn.close()
            temp_dir.cleanup()

        for actual, expected in cases:
            self.assertEqual(actual, expected)

    def test_same_ohlc_extremes_are_not_reordered_by_close(self):
        temp_dir, _db_path, store = self._store()
        try:
            be_price = store._breakeven_price("LONG", 100.0)
            outcomes = {
                store._resolve_long_after_tp1(
                    be_price - 0.5, 106.5, close_price, be_price, 106.0
                )
                for close_price in (98.0, 103.0, 107.0)
            }
        finally:
            store._conn.close()
            temp_dir.cleanup()

        self.assertEqual(
            outcomes,
            {("TP1_PARTIAL_THEN_BE", be_price, "same_candle_be_and_tp2_be_first")},
        )

    def test_already_armed_trade_persists_be_first_resolution_end_to_end(self):
        scenarios = {
            "LONG": {
                "invalidation": 98.0,
                "tp1": 104.0,
                "tp2": 106.0,
                "tp1_candle": (104.5, 100.5, 104.0),
                "both_candle": (106.5, 99.5, 103.0),
            },
            "SHORT": {
                "invalidation": 102.0,
                "tp1": 96.0,
                "tp2": 94.0,
                "tp1_candle": (99.5, 95.5, 96.0),
                "both_candle": (100.5, 93.5, 97.0),
            },
        }

        for side, values in scenarios.items():
            with self.subTest(side=side):
                temp_dir, db_path, store = self._store()
                try:
                    item = setup()
                    item.update(
                        side=side,
                        invalidation=values["invalidation"],
                        tp1=values["tp1"],
                        tp2=values["tp2"],
                    )
                    setup_id = store.register_setup("BTCUSDT", item, STRUCTURE, 1)
                    store.update_pending_with_candle("BTCUSDT", 2, 101.0, 99.0, 100.0)
                    store.update_pending_with_candle(
                        "BTCUSDT", 3, *values["tp1_candle"]
                    )
                    armed = fetch_row(db_path, setup_id)
                    store.update_pending_with_candle(
                        "BTCUSDT", 4, *values["both_candle"]
                    )
                    store.flush()
                    resolved = fetch_row(db_path, setup_id)
                    be_price = store._breakeven_price(side, 100.0)
                finally:
                    store.flush()
                    store._conn.close()
                    temp_dir.cleanup()

                self.assertEqual(armed["tp1_hit"], 1)
                self.assertEqual(armed["resolution_status"], "PENDING")
                self.assertEqual(resolved["resolution_status"], "TP1_PARTIAL_THEN_BE")
                self.assertEqual(resolved["resolved_ts"], 4)
                self.assertAlmostEqual(resolved["resolved_price"], be_price)
                self.assertEqual(
                    resolved["resolution_note"], "same_candle_be_and_tp2_be_first"
                )

    def test_historical_ohlc_resolver_is_not_used_by_live_traders(self):
        for relative_path in (
            "live_binance_forward_trader.py",
            "live_mexc_forward_trader.py",
            "mexc_pole_live_trader.py",
        ):
            source = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
            self.assertNotIn("StrategyValidationStore", source)
            self.assertNotIn("_resolve_long_after_tp1", source)
            self.assertNotIn("_resolve_short_after_tp1", source)


if __name__ == "__main__":
    import unittest

    unittest.main()
