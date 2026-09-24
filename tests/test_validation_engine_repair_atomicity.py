import contextlib
import io
import json
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


def setup(side="LONG", quality=70.0):
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
        "quality_score": quality,
    }


def provenance():
    return {
        "BTCUSDT": {
            "provider": "BINANCE",
            "venue": "BINANCE_SPOT",
            "instrument_type": "SPOT",
            "native_symbol": "BTCUSDT",
            "source_symbol": "BTCUSDT",
            "tick_size": 0.01,
            "provenance_timestamp": "2026-09-23T00:00:00Z",
            "provenance_version": "exchange-info-v1",
            "source": "descriptive label only",
        }
    }


def identities():
    return {"BTCUSDT": {
        "provider": "BINANCE", "venue": "BINANCE_SPOT", "instrument_type": "SPOT",
        "native_symbol": "BTCUSDT", "source_symbol": "BTCUSDT",
    }}


def canonical_state(db_path):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        setup_rows = [dict(row) for row in conn.execute(
            "SELECT * FROM strategy_setups ORDER BY setup_id"
        )]
        branch_rows = [dict(row) for row in conn.execute(
            "SELECT * FROM strategy_setup_branches ORDER BY setup_id, branch_key"
        )]
    return json.dumps(
        {"setups": setup_rows, "branches": branch_rows},
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def open_store(db_path, commit_every=1):
    return StrategyValidationStore(
        str(db_path),
        allow_multiple_trades_per_symbol=True,
        commit_every=commit_every,
        symbol_tick_provenance=provenance(),
        symbol_identity_allowlist=identities(),
    )


def register_pair(store):
    with contextlib.redirect_stdout(io.StringIO()):
        first = store.register_setup("BTCUSDT", setup("LONG", 70.0), STRUCTURE, 1)
        second_structure = dict(STRUCTURE, current_column_index=2)
        second = store.register_setup(
            "BTCUSDT", setup("LONG", 71.0), second_structure, 1
        )
    store.flush()
    return first, second


def update_activation_target(store):
    with contextlib.redirect_stdout(io.StringIO()):
        store.update_pending_with_candle(
            "BTCUSDT", 2, high_price=104.5, low_price=99.99, close_price=103.0
        )


class AtomicCandleProcessingTests(TestCase):
    def _prepared_db(self, directory):
        db_path = Path(directory) / "validation.db"
        store = open_store(db_path)
        register_pair(store)
        store._conn.close()
        return db_path

    def test_fault_after_every_intermediate_dml_rolls_back_all_setups(self):
        with tempfile.TemporaryDirectory() as baseline_dir:
            baseline_db = self._prepared_db(baseline_dir)
            baseline = open_store(baseline_db)
            dml_count = 0
            original = baseline._execute_counted

            def count_dml(category, sql, params=None, symbol=None):
                nonlocal dml_count
                cursor = original(category, sql, params, symbol)
                if str(sql).lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE")):
                    dml_count += 1
                return cursor

            baseline._execute_counted = count_dml
            update_activation_target(baseline)
            uninterrupted = canonical_state(baseline_db)
            baseline._conn.close()
            self.assertGreaterEqual(dml_count, 6)

            for fail_at in range(1, dml_count + 1):
                with self.subTest(fail_at=fail_at), tempfile.TemporaryDirectory() as temp_dir:
                    db_path = self._prepared_db(temp_dir)
                    before = canonical_state(db_path)
                    store = open_store(db_path)
                    original = store._execute_counted
                    seen = 0

                    def fail_after_dml(category, sql, params=None, symbol=None):
                        nonlocal seen
                        cursor = original(category, sql, params, symbol)
                        if str(sql).lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE")):
                            seen += 1
                            if seen == fail_at:
                                raise RuntimeError(f"injected-after-dml-{fail_at}")
                        return cursor

                    store._execute_counted = fail_after_dml
                    with self.assertRaisesRegex(RuntimeError, "injected-after-dml"):
                        update_activation_target(store)
                    self.assertEqual(canonical_state(db_path), before)
                    store._conn.close()

                    replay = open_store(db_path)
                    update_activation_target(replay)
                    replay.flush()
                    replay._conn.close()
                    self.assertEqual(canonical_state(db_path), uninterrupted)

    def test_crash_at_every_transaction_boundary_recovers_by_exact_replay(self):
        boundaries = ("after_savepoint", "before_release", "after_release")
        with tempfile.TemporaryDirectory() as baseline_dir:
            baseline_db = self._prepared_db(baseline_dir)
            baseline = open_store(baseline_db)
            update_activation_target(baseline)
            uninterrupted = canonical_state(baseline_db)
            baseline._conn.close()

        for boundary in boundaries:
            with self.subTest(boundary=boundary), tempfile.TemporaryDirectory() as temp_dir:
                db_path = self._prepared_db(temp_dir)
                before = canonical_state(db_path)
                store = open_store(db_path)

                def crash(name):
                    if name == boundary:
                        raise RuntimeError(f"crash-at-{name}")

                store._candle_transaction_boundary = crash
                with self.assertRaisesRegex(RuntimeError, f"crash-at-{boundary}"):
                    update_activation_target(store)
                store._conn.close()
                expected_after_crash = uninterrupted if boundary == "after_release" else before
                self.assertEqual(canonical_state(db_path), expected_after_crash)

                replay = open_store(db_path)
                update_activation_target(replay)
                replay._conn.close()
                self.assertEqual(canonical_state(db_path), uninterrupted)

    def test_fault_after_every_sql_inside_savepoint_recovers_exactly(self):
        with tempfile.TemporaryDirectory() as baseline_dir:
            baseline_db = self._prepared_db(baseline_dir)
            baseline = open_store(baseline_db)
            original = baseline._execute_counted
            sql_points = 0

            def count_sql(category, sql, params=None, symbol=None):
                nonlocal sql_points
                cursor = original(category, sql, params, symbol)
                if baseline._candle_transaction_active:
                    sql_points += 1
                return cursor

            baseline._execute_counted = count_sql
            update_activation_target(baseline)
            uninterrupted = canonical_state(baseline_db)
            baseline._conn.close()

            for fail_at in range(1, sql_points + 1):
                with self.subTest(fail_at=fail_at), tempfile.TemporaryDirectory() as temp_dir:
                    db_path = self._prepared_db(temp_dir)
                    before = canonical_state(db_path)
                    store = open_store(db_path)
                    original = store._execute_counted
                    seen = 0

                    def fail_after_sql(category, sql, params=None, symbol=None):
                        nonlocal seen
                        cursor = original(category, sql, params, symbol)
                        if store._candle_transaction_active:
                            seen += 1
                            if seen == fail_at:
                                raise RuntimeError(f"injected-after-sql-{fail_at}")
                        return cursor

                    store._execute_counted = fail_after_sql
                    with self.assertRaisesRegex(RuntimeError, "injected-after-sql"):
                        update_activation_target(store)
                    store._conn.close()
                    self.assertEqual(canonical_state(db_path), before)

                    replay = open_store(db_path)
                    update_activation_target(replay)
                    replay._conn.close()
                    self.assertEqual(canonical_state(db_path), uninterrupted)

    def test_commit_every_never_commits_inside_candle_transaction(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = self._prepared_db(temp_dir)
            store = open_store(db_path, commit_every=1)
            original = store._record_commit
            observed_inside = []

            def guarded_commit(*args, **kwargs):
                observed_inside.append(bool(store._candle_transaction_active))
                return original(*args, **kwargs)

            store._record_commit = guarded_commit
            update_activation_target(store)
            store._conn.close()
            self.assertEqual(observed_inside, [False])

    def test_migrated_nonterminal_null_watermark_fails_closed(self):
        lifecycle_updates = (
            ("PENDING", "PENDING", 0, 0),
            ("PENDING", "ACTIVE", 0, 0),
            ("PENDING", "ACTIVE", 1, 0),
            ("AMBIGUOUS", "ACTIVE", 0, 1),
        )
        for resolution_status, activation_status, tp1_hit, branch_active in lifecycle_updates:
            with self.subTest(status=resolution_status, activation=activation_status, tp1=tp1_hit), tempfile.TemporaryDirectory() as temp_dir:
                db_path = self._prepared_db(temp_dir)
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        UPDATE strategy_setups
                        SET resolution_status=?, activation_status=?, tp1_hit=?, branch_active=?,
                            last_evaluated_candle_ts=NULL, validation_state_version=NULL
                        """,
                        (resolution_status, activation_status, tp1_hit, branch_active),
                    )
                before = canonical_state(db_path)
                store = open_store(db_path)
                with self.assertRaisesRegex(ValueError, "legacy nonterminal.*watermark"):
                    with contextlib.redirect_stdout(io.StringIO()):
                        store.update_pending_with_candle(
                            "BTCUSDT", 3, 106.5, 97.5, 101.0
                        )
                store._conn.close()
                self.assertEqual(canonical_state(db_path), before)


if __name__ == "__main__":
    import unittest

    unittest.main()
