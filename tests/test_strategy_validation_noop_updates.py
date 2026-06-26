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


BASE_STRUCTURE = {
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


def make_setup(*, ideal_entry=100.0, invalidation=98.0, tp1=104.0, tp2=106.0):
    return {
        "strategy": "pullback_retest",
        "side": "LONG",
        "status": "CANDIDATE",
        "zone_low": ideal_entry,
        "zone_high": ideal_entry,
        "ideal_entry": ideal_entry,
        "invalidation": invalidation,
        "risk": abs(ideal_entry - invalidation),
        "tp1": tp1,
        "tp2": tp2,
        "rr1": 2.0,
        "rr2": 3.0,
        "pullback_quality": "HEALTHY",
        "risk_quality": "NORMAL",
        "reward_quality": "STRONG",
        "quality_score": 70.0,
        "quality_grade": "A",
        "reason": "test",
        "reject_reason": None,
    }


def fetch_row(db_path, setup_id):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return dict(conn.execute("SELECT * FROM strategy_setups WHERE setup_id = ?", (setup_id,)).fetchone())
    finally:
        conn.close()


class StrategyValidationNoopUpdateTests(TestCase):
    def _store(self):
        temp_dir = tempfile.TemporaryDirectory()
        db_path = Path(temp_dir.name) / "validation.db"
        store = StrategyValidationStore(str(db_path), allow_multiple_trades_per_symbol=True, commit_every=1)
        return temp_dir, db_path, store

    def _sql_updates(self, store, symbol="BTCUSDT"):
        return store.get_perf_snapshot()["update_pending"].get(symbol, {}).get("sql_update_count", 0)

    def test_pending_not_activated_receives_no_sql_update(self):
        temp_dir, db_path, store = self._store()
        try:
            setup_id = store.register_setup("BTCUSDT", make_setup(ideal_entry=100.0), BASE_STRUCTURE, 1)
            store.flush()
            before = self._sql_updates(store)
            store.update_pending_with_candle("BTCUSDT", 2, high_price=103.0, low_price=101.0, close_price=102.0)
            store.flush()
            after = self._sql_updates(store)
            row = fetch_row(db_path, setup_id)
            perf = store.get_perf_snapshot()["update_pending"]["BTCUSDT"]
        finally:
            store.flush()
            store._conn.close()
            temp_dir.cleanup()
        self.assertEqual(after - before, 0)
        self.assertEqual(row["activation_status"], "PENDING")
        self.assertEqual(row["bars_observed"], 0)
        self.assertIsNone(row["first_outcome_ts"])
        self.assertEqual(perf["noop_skipped_count"], 1)
        self.assertEqual(perf["trades_scanned"], 1)

    def test_activation_writes_exactly_one_meaningful_update(self):
        temp_dir, db_path, store = self._store()
        try:
            setup_id = store.register_setup("BTCUSDT", make_setup(ideal_entry=100.0), BASE_STRUCTURE, 1)
            store.flush()
            before = self._sql_updates(store)
            store.update_pending_with_candle("BTCUSDT", 2, high_price=101.0, low_price=99.0, close_price=100.0)
            store.flush()
            after = self._sql_updates(store)
            row = fetch_row(db_path, setup_id)
            perf = store.get_perf_snapshot()["update_pending"]["BTCUSDT"]
        finally:
            store.flush()
            store._conn.close()
            temp_dir.cleanup()
        self.assertEqual(after - before, 1)
        self.assertEqual(row["activation_status"], "ACTIVE")
        self.assertEqual(row["activated_ts"], 2)
        self.assertEqual(perf["lifecycle_update_count"], 1)
        self.assertEqual(perf["trades_activated"], 1)

    def test_stop_resolution_still_writes(self):
        temp_dir, db_path, store = self._store()
        try:
            setup_id = store.register_setup("BTCUSDT", make_setup(ideal_entry=100.0, invalidation=98.0), BASE_STRUCTURE, 1)
            store.update_pending_with_candle("BTCUSDT", 2, high_price=101.0, low_price=99.0, close_price=100.0)
            before = self._sql_updates(store)
            store.update_pending_with_candle("BTCUSDT", 3, high_price=100.0, low_price=97.5, close_price=98.0)
            store.flush()
            after = self._sql_updates(store)
            row = fetch_row(db_path, setup_id)
        finally:
            store.flush()
            store._conn.close()
            temp_dir.cleanup()
        self.assertEqual(after - before, 1)
        self.assertEqual(row["resolution_status"], "STOPPED")
        self.assertEqual(row["resolved_ts"], 3)

    def test_tp_resolution_still_writes(self):
        temp_dir, db_path, store = self._store()
        try:
            setup_id = store.register_setup("BTCUSDT", make_setup(ideal_entry=100.0, tp1=104.0, tp2=106.0), BASE_STRUCTURE, 1)
            store.update_pending_with_candle("BTCUSDT", 2, high_price=101.0, low_price=99.0, close_price=100.0)
            before = self._sql_updates(store)
            store.update_pending_with_candle("BTCUSDT", 3, high_price=106.5, low_price=100.0, close_price=106.0)
            store.flush()
            after = self._sql_updates(store)
            row = fetch_row(db_path, setup_id)
        finally:
            store.flush()
            store._conn.close()
            temp_dir.cleanup()
        self.assertEqual(after - before, 1)
        self.assertEqual(row["resolution_status"], "TP2")
        self.assertEqual(row["tp1_hit"], 1)

    def test_active_row_with_unchanged_excursion_does_not_write(self):
        temp_dir, db_path, store = self._store()
        try:
            setup_id = store.register_setup("BTCUSDT", make_setup(ideal_entry=100.0, tp1=110.0, tp2=120.0), BASE_STRUCTURE, 1)
            store.update_pending_with_candle("BTCUSDT", 2, high_price=101.0, low_price=99.0, close_price=100.0)
            store.update_pending_with_candle("BTCUSDT", 3, high_price=102.0, low_price=98.5, close_price=101.0)
            before = self._sql_updates(store)
            store.update_pending_with_candle("BTCUSDT", 4, high_price=101.5, low_price=99.0, close_price=101.0)
            store.flush()
            after = self._sql_updates(store)
            row = fetch_row(db_path, setup_id)
            perf = store.get_perf_snapshot()["update_pending"]["BTCUSDT"]
        finally:
            store.flush()
            store._conn.close()
            temp_dir.cleanup()
        self.assertEqual(after - before, 0)
        self.assertEqual(row["resolution_status"], "PENDING")
        self.assertEqual(row["max_favorable_excursion"], 2.0)
        self.assertEqual(row["max_adverse_excursion"], 1.5)
        self.assertGreaterEqual(perf["noop_skipped_count"], 1)

    def test_lifecycle_semantics_preserved_through_activation_tp1_and_tp2(self):
        temp_dir, db_path, store = self._store()
        try:
            setup_id = store.register_setup("BTCUSDT", make_setup(ideal_entry=100.0, tp1=104.0, tp2=106.0), BASE_STRUCTURE, 1)
            store.update_pending_with_candle("BTCUSDT", 2, high_price=101.0, low_price=99.0, close_price=100.0)
            store.update_pending_with_candle("BTCUSDT", 3, high_price=104.5, low_price=100.5, close_price=104.0)
            mid = fetch_row(db_path, setup_id)
            store.update_pending_with_candle("BTCUSDT", 4, high_price=106.5, low_price=104.0, close_price=106.0)
            store.flush()
            row = fetch_row(db_path, setup_id)
        finally:
            store.flush()
            store._conn.close()
            temp_dir.cleanup()
        self.assertEqual(mid["activation_status"], "ACTIVE")
        self.assertEqual(mid["tp1_hit"], 1)
        self.assertEqual(mid["resolution_status"], "PENDING")
        self.assertEqual(row["resolution_status"], "TP2")
        self.assertEqual(row["resolved_ts"], 4)
