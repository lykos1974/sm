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


def setup(side="LONG", tp1_r=2.0, tp2_r=2.5):
    entry = 100.0
    stop = 98.0 if side == "LONG" else 102.0
    direction = 1 if side == "LONG" else -1
    risk = 2.0
    return {
        "strategy": "pullback_retest",
        "side": side,
        "status": "CANDIDATE",
        "zone_low": entry,
        "zone_high": entry,
        "ideal_entry": entry,
        "invalidation": stop,
        "risk": risk,
        "tp1": entry + direction * tp1_r * risk,
        "tp2": entry + direction * tp2_r * risk,
        "rr1": tp1_r,
        "rr2": tp2_r,
        "quality_score": 70.0,
    }


def provenance(**overrides):
    item = {
        "provider": "BINANCE",
        "venue": "BINANCE_SPOT",
        "instrument_type": "SPOT",
        "native_symbol": "BTCUSDT",
        "source_symbol": "BTCUSDT",
        "tick_size": 0.01,
        "provenance_timestamp": "2026-09-23T00:00:00Z",
        "provenance_version": "exchange-info-v1",
        "source": "exchange-info:ETHUSDT is only a descriptive label",
    }
    item.update(overrides)
    return {"BTCUSDT": item}


def identities(symbol="BTCUSDT", **overrides):
    identity = {
        "provider": "BINANCE",
        "venue": "BINANCE_SPOT",
        "instrument_type": "SPOT",
        "native_symbol": "BTCUSDT",
        "source_symbol": symbol,
    }
    identity.update(overrides)
    return {symbol: identity}


def fetch_one(db_path, query, params=()):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(query, params).fetchone()
        return dict(row) if row else None


def fetch_all(db_path, query, params=()):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(query, params)]


class StructuredProvenanceTests(TestCase):
    def test_explicit_allowlist_accepts_supported_test_identities(self):
        cases = (
            (
                "BTCUSDT",
                identities(),
                provenance(),
            ),
            (
                "BINANCE_FUT:BTCUSDT",
                identities(
                    "BINANCE_FUT:BTCUSDT",
                    venue="BINANCE_FUTURES",
                    instrument_type="PERPETUAL",
                ),
                {
                    "BINANCE_FUT:BTCUSDT": provenance()["BTCUSDT"]
                    | {
                        "venue": "BINANCE_FUTURES",
                        "instrument_type": "PERPETUAL",
                        "source_symbol": "BINANCE_FUT:BTCUSDT",
                    }
                },
            ),
            (
                "MEXC_FUT:BTCUSDT",
                identities(
                    "MEXC_FUT:BTCUSDT",
                    provider="MEXC",
                    venue="MEXC_FUTURES",
                    instrument_type="PERPETUAL",
                ),
                {
                    "MEXC_FUT:BTCUSDT": provenance()["BTCUSDT"]
                    | {
                        "provider": "MEXC",
                        "venue": "MEXC_FUTURES",
                        "instrument_type": "PERPETUAL",
                        "source_symbol": "MEXC_FUT:BTCUSDT",
                    }
                },
            ),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            for index, (symbol, allowlist, ticks) in enumerate(cases):
                with self.subTest(symbol=symbol):
                    store = StrategyValidationStore(
                        str(Path(temp_dir) / f"positive-{index}.db"),
                        symbol_tick_provenance=ticks,
                        symbol_identity_allowlist=allowlist,
                    )
                    store.close()

    def test_unknown_and_incompatible_identities_fail_before_database_creation(self):
        cases = (
            (
                "UNKNOWN:FAKE",
                {},
                {
                    "UNKNOWN:FAKE": provenance()["BTCUSDT"]
                    | {
                        "provider": "UNKNOWN",
                        "venue": "UNKNOWN",
                        "instrument_type": "UNKNOWN",
                        "native_symbol": "FAKE",
                        "source_symbol": "UNKNOWN:FAKE",
                    }
                },
            ),
            (
                "BINANCE_FUT:BTCUSDT",
                identities(
                    "BINANCE_FUT:BTCUSDT",
                    venue="BINANCE_FUTURES",
                    instrument_type="PERPETUAL",
                ),
                {
                    "BINANCE_FUT:BTCUSDT": provenance()["BTCUSDT"]
                    | {
                        "provider": "MEXC",
                        "venue": "MEXC_FUTURES",
                        "instrument_type": "SPOT",
                        "source_symbol": "BINANCE_FUT:BTCUSDT",
                    }
                },
            ),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            for index, (symbol, allowlist, ticks) in enumerate(cases):
                db_path = Path(temp_dir) / f"rejected-{index}.db"
                with self.subTest(symbol=symbol), self.assertRaises(ValueError):
                    StrategyValidationStore(
                        str(db_path),
                        symbol_tick_provenance=ticks,
                        symbol_identity_allowlist=allowlist,
                    )
                self.assertFalse(db_path.exists())
    def test_all_structured_fields_are_required_and_tick_must_be_finite_positive(self):
        required = (
            "provider",
            "venue",
            "instrument_type",
            "native_symbol",
            "source_symbol",
            "tick_size",
            "provenance_timestamp",
            "provenance_version",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            for index, field in enumerate(required):
                value = provenance()["BTCUSDT"]
                value.pop(field)
                with self.subTest(field=field), self.assertRaises(ValueError):
                    StrategyValidationStore(
                        str(Path(temp_dir) / f"missing-{index}.db"),
                        symbol_tick_provenance={"BTCUSDT": value},
                    )
            for index, value in enumerate((None, math.nan, math.inf, -math.inf, 0, -0.01)):
                with self.subTest(tick=value), self.assertRaisesRegex(
                    ValueError, "finite positive"
                ):
                    StrategyValidationStore(
                        str(Path(temp_dir) / f"tick-{index}.db"),
                        symbol_tick_provenance=provenance(tick_size=value),
                    )

    def test_symbol_identity_mismatch_is_rejected_despite_free_form_label(self):
        mismatches = (
            {"source_symbol": "ETHUSDT"},
            {"native_symbol": "ETHUSDT"},
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            for index, mismatch in enumerate(mismatches):
                with self.subTest(mismatch=mismatch), self.assertRaisesRegex(
                    ValueError, "symbol identity mismatch"
                ):
                    StrategyValidationStore(
                        str(Path(temp_dir) / f"mismatch-{index}.db"),
                        symbol_tick_provenance=provenance(**mismatch),
                    )

    def test_registration_freezes_every_structured_field(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            store = StrategyValidationStore(
                str(db_path), symbol_tick_provenance=provenance()
            )
            with contextlib.redirect_stdout(io.StringIO()):
                setup_id = store.register_setup(
                    "BTCUSDT", setup(), STRUCTURE, 1
                )
            store.flush()
            store._conn.close()
            row = fetch_one(
                db_path, "SELECT * FROM strategy_setups WHERE setup_id=?", (setup_id,)
            )
            self.assertEqual(row["activation_tick_provider"], "BINANCE")
            self.assertEqual(row["activation_tick_venue"], "BINANCE_SPOT")
            self.assertEqual(row["activation_tick_instrument_type"], "SPOT")
            self.assertEqual(row["activation_tick_native_symbol"], "BTCUSDT")
            self.assertEqual(row["activation_tick_source_symbol"], "BTCUSDT")
            self.assertEqual(row["activation_tick_provenance_timestamp"], "2026-09-23T00:00:00Z")
            self.assertEqual(row["activation_tick_provenance_version"], "exchange-info-v1")


class ActivationBranchChronologyTests(TestCase):
    def _store_and_setup(self, directory, side="LONG"):
        db_path = Path(directory) / "validation.db"
        store = StrategyValidationStore(
            str(db_path),
            allow_multiple_trades_per_symbol=True,
            commit_every=1,
            symbol_tick_provenance=provenance(),
        )
        with contextlib.redirect_stdout(io.StringIO()):
            setup_id = store.register_setup("BTCUSDT", setup(side), STRUCTURE, 1)
        store.flush()
        return db_path, store, setup_id

    def _update(self, store, ts, high, low, close):
        with contextlib.redirect_stdout(io.StringIO()):
            store.update_pending_with_candle("BTCUSDT", ts, high, low, close)

    def test_prefill_movement_never_arms_tp1_or_be(self):
        cases = (
            ("LONG", 103.5, 99.99, 101.0),
            ("SHORT", 100.01, 96.5, 99.0),
        )
        for side, high, low, close in cases:
            with self.subTest(side=side), tempfile.TemporaryDirectory() as temp_dir:
                db_path, store, setup_id = self._store_and_setup(temp_dir, side)
                self._update(store, 2, high, low, close)
                store._conn.close()
                row = fetch_one(db_path, "SELECT * FROM strategy_setups WHERE setup_id=?", (setup_id,))
                self.assertEqual(row["activation_status"], "ACTIVE")
                self.assertEqual(row["resolution_status"], "PENDING")
                self.assertEqual(row["tp1_hit"], 0)

    def test_target_uncertainty_persists_two_branches_and_continues_active_branch(self):
        cases = (
            ("LONG", (104.0, 99.99, 103.0), (101.0, 97.5, 99.0)),
            ("SHORT", (100.01, 96.0, 97.0), (102.5, 99.0, 101.0)),
        )
        for side, activation, later_stop in cases:
            with self.subTest(side=side), tempfile.TemporaryDirectory() as temp_dir:
                db_path, store, setup_id = self._store_and_setup(temp_dir, side)
                self._update(store, 2, *activation)
                branched = fetch_one(db_path, "SELECT * FROM strategy_setups WHERE setup_id=?", (setup_id,))
                branches = fetch_all(
                    db_path,
                    "SELECT * FROM strategy_setup_branches WHERE setup_id=? ORDER BY branch_key",
                    (setup_id,),
                )
                self.assertEqual(branched["resolution_status"], "AMBIGUOUS")
                self.assertEqual(branched["branch_active"], 1)
                self.assertEqual(branched["tp1_hit"], 0)
                self.assertEqual(len(branches), 2)
                self.assertEqual({row["branch_status"] for row in branches}, {"ACTIVE", "COMPLETED"})

                self._update(store, 3, *later_stop)
                store._conn.close()
                completed = fetch_one(db_path, "SELECT * FROM strategy_setups WHERE setup_id=?", (setup_id,))
                self.assertEqual(completed["resolution_status"], "AMBIGUOUS")
                self.assertEqual(completed["branch_active"], 0)
                self.assertEqual(completed["ambiguous_pessimistic_r"], -1.0)
                self.assertEqual(completed["ambiguous_optimistic_r"], 2.0)

    def test_activation_stop_dominates_and_creates_no_branches(self):
        cases = (
            ("LONG", 105.5, 97.5, 101.0),
            ("SHORT", 102.5, 94.5, 99.0),
        )
        for side, high, low, close in cases:
            with self.subTest(side=side), tempfile.TemporaryDirectory() as temp_dir:
                db_path, store, setup_id = self._store_and_setup(temp_dir, side)
                self._update(store, 2, high, low, close)
                store._conn.close()
                row = fetch_one(db_path, "SELECT * FROM strategy_setups WHERE setup_id=?", (setup_id,))
                self.assertEqual(row["resolution_status"], "STOPPED")
                self.assertEqual(
                    fetch_all(db_path, "SELECT * FROM strategy_setup_branches WHERE setup_id=?", (setup_id,)),
                    [],
                )

    def test_completed_bounds_use_actual_branch_rr_for_long_and_short(self):
        cases = (
            ("LONG", (104.0, 99.99, 103.0), (108.5, 100.0, 108.0)),
            ("SHORT", (100.01, 96.0, 97.0), (100.0, 91.5, 92.0)),
        )
        for side, activation, later_tp2 in cases:
            with self.subTest(side=side), tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "validation.db"
                store = StrategyValidationStore(
                    str(db_path),
                    allow_multiple_trades_per_symbol=True,
                    commit_every=1,
                    symbol_tick_provenance=provenance(),
                )
                configured = setup(side, tp1_r=2.0, tp2_r=4.0)
                with contextlib.redirect_stdout(io.StringIO()):
                    setup_id = store.register_setup("BTCUSDT", configured, STRUCTURE, 1)
                self._update(store, 2, *activation)
                self._update(store, 3, *later_tp2)
                store._conn.close()

                row = fetch_one(db_path, "SELECT * FROM strategy_setups WHERE setup_id=?", (setup_id,))
                branches = fetch_all(
                    db_path,
                    "SELECT * FROM strategy_setup_branches WHERE setup_id=? ORDER BY branch_key",
                    (setup_id,),
                )
                actual = sorted(branch["r_lower"] for branch in branches)
                self.assertEqual(actual, [2.0, 2.75])
                self.assertEqual(row["ambiguous_pessimistic_r"], 2.0)
                self.assertEqual(row["ambiguous_optimistic_r"], 2.75)

    def test_later_same_candle_bounds_use_partial_position_rr_once(self):
        cases = (
            ("LONG", (101.0, 99.99, 100.0), (108.5, 97.5, 101.0)),
            ("SHORT", (100.01, 99.0, 100.0), (102.5, 91.5, 99.0)),
        )
        for side, activation, ambiguous in cases:
            with self.subTest(side=side), tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "validation.db"
                store = StrategyValidationStore(
                    str(db_path),
                    allow_multiple_trades_per_symbol=True,
                    commit_every=1,
                    symbol_tick_provenance=provenance(),
                )
                configured = setup(side, tp1_r=2.0, tp2_r=4.0)
                with contextlib.redirect_stdout(io.StringIO()):
                    setup_id = store.register_setup("BTCUSDT", configured, STRUCTURE, 1)
                self._update(store, 2, *activation)
                self._update(store, 3, *ambiguous)
                store._conn.close()

                row = fetch_one(db_path, "SELECT * FROM strategy_setups WHERE setup_id=?", (setup_id,))
                self.assertEqual(row["resolution_status"], "AMBIGUOUS")
                self.assertEqual(row["ambiguous_pessimistic_r"], -1.0)
                self.assertEqual(row["ambiguous_optimistic_r"], 2.75)


if __name__ == "__main__":
    import unittest

    unittest.main()
