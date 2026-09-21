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

    def test_be_and_tp2_same_candle_records_current_policy_diagnostic(self):
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

        # Diagnostic lock only: TP2-first is the current policy. Changing it
        # requires an explicit execution-model decision, not a chronology fix.
        self.assertEqual(result, ("TP2", 106.0, "tp2_hit_after_tp1"))


if __name__ == "__main__":
    import unittest

    unittest.main()
