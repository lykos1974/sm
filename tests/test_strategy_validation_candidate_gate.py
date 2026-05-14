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


def make_setup(status: str, *, ideal_entry: float) -> dict:
    return {
        "strategy": "pullback_retest",
        "side": "LONG",
        "status": status,
        "zone_low": ideal_entry,
        "zone_high": ideal_entry,
        "ideal_entry": ideal_entry,
        "invalidation": ideal_entry - 2.0,
        "risk": 2.0,
        "tp1": ideal_entry + 4.0,
        "tp2": ideal_entry + 6.0,
        "rr1": 2.0,
        "rr2": 3.0,
        "pullback_quality": "HEALTHY",
        "risk_quality": "NORMAL",
        "reward_quality": "STRONG",
        "quality_score": 70.0,
        "quality_grade": "A",
        "reason": f"test {status}",
        "reject_reason": None,
    }


def count_by_status(db_path: Path) -> dict[str, int]:
    conn = sqlite3.connect(db_path)
    try:
        return {
            str(status): int(count)
            for status, count in conn.execute(
                "SELECT status, COUNT(*) FROM strategy_setups GROUP BY status"
            ).fetchall()
        }
    finally:
        conn.close()


class StrategyValidationCandidateGateTests(TestCase):
    def test_pending_watch_does_not_block_new_candidate(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            store = StrategyValidationStore(
                str(db_path), allow_multiple_trades_per_symbol=False, commit_every=1
            )
            try:
                watch_id = store.register_setup(
                    symbol="BTCUSDT",
                    setup=make_setup("WATCH", ideal_entry=100.0),
                    structure_state=BASE_STRUCTURE,
                    reference_ts=1,
                )
                candidate_id = store.register_setup(
                    symbol="BTCUSDT",
                    setup=make_setup("CANDIDATE", ideal_entry=101.0),
                    structure_state=BASE_STRUCTURE,
                    reference_ts=2,
                )
                store.flush()
            finally:
                store.flush()
                store._conn.close()

            self.assertIsNotNone(watch_id)
            self.assertIsNotNone(candidate_id)
            self.assertEqual(count_by_status(db_path), {"CANDIDATE": 1, "WATCH": 1})

    def test_pending_candidate_blocks_new_candidate_when_multiple_trades_disabled(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            store = StrategyValidationStore(
                str(db_path), allow_multiple_trades_per_symbol=False, commit_every=1
            )
            try:
                first_candidate_id = store.register_setup(
                    symbol="BTCUSDT",
                    setup=make_setup("CANDIDATE", ideal_entry=100.0),
                    structure_state=BASE_STRUCTURE,
                    reference_ts=1,
                )
                second_candidate_id = store.register_setup(
                    symbol="BTCUSDT",
                    setup=make_setup("CANDIDATE", ideal_entry=101.0),
                    structure_state=BASE_STRUCTURE,
                    reference_ts=2,
                )
                store.flush()
            finally:
                store.flush()
                store._conn.close()

            self.assertIsNotNone(first_candidate_id)
            self.assertIsNone(second_candidate_id)
            self.assertEqual(count_by_status(db_path), {"CANDIDATE": 1})

    def test_pending_watch_does_not_change_new_watch_registration_behavior(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            store = StrategyValidationStore(
                str(db_path), allow_multiple_trades_per_symbol=False, commit_every=1
            )
            try:
                first_watch_id = store.register_setup(
                    symbol="BTCUSDT",
                    setup=make_setup("WATCH", ideal_entry=100.0),
                    structure_state=BASE_STRUCTURE,
                    reference_ts=1,
                )
                second_watch_id = store.register_setup(
                    symbol="BTCUSDT",
                    setup=make_setup("WATCH", ideal_entry=101.0),
                    structure_state=BASE_STRUCTURE,
                    reference_ts=2,
                )
                store.flush()
            finally:
                store.flush()
                store._conn.close()

            self.assertIsNotNone(first_watch_id)
            self.assertIsNotNone(second_watch_id)
            self.assertEqual(count_by_status(db_path), {"WATCH": 2})
