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

from app import App  # noqa: E402
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


def ticks(size=0.01, source="test:BTCUSDT"):
    return {
        "BTCUSDT": {
            "provider": "TEST",
            "venue": "TEST_SPOT",
            "instrument_type": "SPOT",
            "native_symbol": "BTCUSDT",
            "source_symbol": "BTCUSDT",
            "tick_size": size,
            "provenance_timestamp": "2026-09-23T00:00:00Z",
            "provenance_version": "test-v1",
            "source": source,
        }
    }


def identities():
    return {"BTCUSDT": {
        "provider": "TEST", "venue": "TEST_SPOT", "instrument_type": "SPOT",
        "native_symbol": "BTCUSDT", "source_symbol": "BTCUSDT",
    }}


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


def canonical_result(db_path):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        payload = {
            "setups": [dict(row) for row in conn.execute(
                "SELECT * FROM strategy_setups ORDER BY setup_id"
            )],
            "branches": [dict(row) for row in conn.execute(
                "SELECT * FROM strategy_setup_branches ORDER BY setup_id, branch_key"
            )],
        }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


class CandleIdempotencyTests(TestCase):
    def _store(self, db_path, provenance=None):
        return StrategyValidationStore(
            str(db_path),
            allow_multiple_trades_per_symbol=True,
            commit_every=1,
            symbol_tick_provenance=provenance or ticks(),
            symbol_identity_allowlist=identities(),
        )

    def _register(self, store, side, reference_ts=1):
        with contextlib.redirect_stdout(io.StringIO()):
            return store.register_setup(
                "BTCUSDT", setup(side), STRUCTURE, reference_ts
            )

    def _update(self, store, candle):
        with contextlib.redirect_stdout(io.StringIO()):
            store.update_pending_with_candle("BTCUSDT", **candle)

    def _assert_replay_is_absolute_noop(self, store, db_path, setup_id, candle):
        store.flush()
        before_row = fetch_row(db_path, setup_id)
        before_changes = store._conn.total_changes
        self._update(store, candle)
        store.flush()
        after_row = fetch_row(db_path, setup_id)
        self.assertEqual(after_row, before_row)
        self.assertEqual(store._conn.total_changes, before_changes)

    def test_pending_repeated_and_older_candles_are_noops_long_and_short(self):
        candles = {
            "LONG": dict(close_ts=2, high_price=101.0, low_price=100.0, close_price=101.0),
            "SHORT": dict(close_ts=2, high_price=100.0, low_price=99.0, close_price=99.0),
        }
        for side, candle in candles.items():
            with self.subTest(side=side), tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "validation.db"
                store = self._store(db_path)
                try:
                    setup_id = self._register(store, side)
                    self._update(store, candle)
                    observed = fetch_row(db_path, setup_id)
                    self.assertEqual(observed["bars_observed"], 1)
                    self.assertEqual(observed["last_evaluated_candle_ts"], 2)
                    self._assert_replay_is_absolute_noop(
                        store, db_path, setup_id, candle
                    )
                    older = dict(candle, close_ts=1)
                    self._assert_replay_is_absolute_noop(
                        store, db_path, setup_id, older
                    )
                finally:
                    close_store(store)

    def test_activation_candle_replay_is_noop_long_and_short(self):
        candles = {
            "LONG": dict(close_ts=2, high_price=101.0, low_price=99.0, close_price=100.0),
            "SHORT": dict(close_ts=2, high_price=101.0, low_price=99.0, close_price=100.0),
        }
        for side, candle in candles.items():
            with self.subTest(side=side), tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "validation.db"
                store = self._store(db_path)
                try:
                    setup_id = self._register(store, side)
                    self._update(store, candle)
                    observed = fetch_row(db_path, setup_id)
                    self.assertEqual(observed["activation_status"], "ACTIVE")
                    self.assertEqual(observed["last_evaluated_candle_ts"], 2)
                    self._assert_replay_is_absolute_noop(
                        store, db_path, setup_id, candle
                    )
                finally:
                    close_store(store)

    def test_tp1_be_state_and_resolved_state_retries_are_noops(self):
        scenarios = {
            "LONG": (
                dict(close_ts=2, high_price=101.0, low_price=99.0, close_price=100.0),
                dict(close_ts=3, high_price=104.5, low_price=100.5, close_price=104.0),
                dict(close_ts=4, high_price=106.5, low_price=104.0, close_price=106.0),
            ),
            "SHORT": (
                dict(close_ts=2, high_price=101.0, low_price=99.0, close_price=100.0),
                dict(close_ts=3, high_price=99.5, low_price=95.5, close_price=96.0),
                dict(close_ts=4, high_price=96.0, low_price=93.5, close_price=94.0),
            ),
        }
        for side, (activation, tp1, resolved) in scenarios.items():
            with self.subTest(side=side), tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "validation.db"
                store = self._store(db_path)
                try:
                    setup_id = self._register(store, side)
                    self._update(store, activation)
                    self._update(store, tp1)
                    armed = fetch_row(db_path, setup_id)
                    self.assertEqual(armed["tp1_hit"], 1)
                    self.assertEqual(armed["last_evaluated_candle_ts"], 3)
                    self._assert_replay_is_absolute_noop(
                        store, db_path, setup_id, tp1
                    )
                    self._update(store, resolved)
                    final = fetch_row(db_path, setup_id)
                    self.assertNotEqual(final["resolution_status"], "PENDING")
                    self.assertEqual(final["last_evaluated_candle_ts"], 4)
                    self._assert_replay_is_absolute_noop(
                        store, db_path, setup_id, resolved
                    )
                finally:
                    close_store(store)

    def _run_sequence(self, db_path, side, candles, *, restart, retry):
        store = self._store(db_path)
        setup_id = self._register(store, side)
        if restart:
            close_store(store)
        for candle in candles:
            store = self._store(db_path) if restart else store
            self._update(store, candle)
            if retry:
                self._update(store, candle)
            if restart:
                close_store(store)
        if not restart:
            close_store(store)
        return fetch_row(db_path, setup_id)

    def test_batch_per_candle_restart_and_retry_are_equivalent(self):
        scenarios = {
            "LONG": [
                dict(close_ts=2, high_price=101.0, low_price=100.0, close_price=101.0),
                dict(close_ts=3, high_price=101.0, low_price=99.0, close_price=100.0),
                dict(close_ts=4, high_price=104.5, low_price=100.5, close_price=104.0),
            ],
            "SHORT": [
                dict(close_ts=2, high_price=100.0, low_price=99.0, close_price=99.0),
                dict(close_ts=3, high_price=101.0, low_price=99.0, close_price=100.0),
                dict(close_ts=4, high_price=99.5, low_price=95.5, close_price=96.0),
            ],
        }
        fields = (
            "bars_observed",
            "activation_status",
            "activated_ts",
            "tp1_hit",
            "tp1_hit_ts",
            "resolution_status",
            "last_evaluated_candle_ts",
            "max_favorable_excursion",
            "max_adverse_excursion",
        )
        for side, candles in scenarios.items():
            with self.subTest(side=side), tempfile.TemporaryDirectory() as temp_dir:
                root = Path(temp_dir)
                batch = self._run_sequence(
                    root / "batch.db", side, candles, restart=False, retry=False
                )
                per_candle = self._run_sequence(
                    root / "per-candle.db", side, candles, restart=True, retry=False
                )
                retried = self._run_sequence(
                    root / "retried.db", side, candles, restart=True, retry=True
                )
                expected = {field: batch[field] for field in fields}
                self.assertEqual(
                    {field: per_candle[field] for field in fields}, expected
                )
                self.assertEqual(
                    {field: retried[field] for field in fields}, expected
                )
                self.assertEqual(canonical_result(root / "per-candle.db"), canonical_result(root / "batch.db"))
                self.assertEqual(canonical_result(root / "retried.db"), canonical_result(root / "batch.db"))

    def test_replay_after_validation_flush_and_checkpoint_failure_is_noop(self):
        class ReplayDummy:
            _run_downstream_before_checkpoint = App._run_downstream_before_checkpoint

            def __init__(self, store, candle, fail_checkpoint):
                self.validation_store = store
                self.candle = candle
                self.fail_checkpoint = fail_checkpoint

            def _observe_ideal_entry_setups(self, *_args):
                return None

            def _refresh_validation_for_symbol(self, *_args, **_kwargs):
                with contextlib.redirect_stdout(io.StringIO()):
                    self.validation_store.update_pending_with_candle(
                        "BTCUSDT", **self.candle
                    )

            def _save_engine_snapshot(self, *_args):
                if self.fail_checkpoint:
                    raise RuntimeError("checkpoint failed after validation flush")

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            store = self._store(db_path)
            setup_id = self._register(store, "LONG")
            candle = dict(
                close_ts=2, high_price=101.0, low_price=100.0, close_price=101.0
            )
            first = ReplayDummy(store, candle, True)
            with self.assertRaisesRegex(RuntimeError, "checkpoint failed"):
                first._run_downstream_before_checkpoint(
                    symbol="BTCUSDT",
                    engine=object(),
                    new_candles=[{"close_time": 2}],
                    last_processed=2,
                    snapshot={},
                    validation_engine_steps=[object()],
                    stage_log=lambda _message: None,
                )
            close_store(store)
            after_failure = fetch_row(db_path, setup_id)

            restarted = self._store(db_path)
            changes_before_retry = restarted._conn.total_changes
            retry = ReplayDummy(restarted, candle, False)
            retry._run_downstream_before_checkpoint(
                symbol="BTCUSDT",
                engine=object(),
                new_candles=[{"close_time": 2}],
                last_processed=2,
                snapshot={},
                validation_engine_steps=[object()],
                stage_log=lambda _message: None,
            )
            changes_after_retry = restarted._conn.total_changes
            close_store(restarted)
            after_retry = fetch_row(db_path, setup_id)

        self.assertEqual(after_retry, after_failure)
        self.assertEqual(changes_after_retry, changes_before_retry)


if __name__ == "__main__":
    import unittest

    unittest.main()
