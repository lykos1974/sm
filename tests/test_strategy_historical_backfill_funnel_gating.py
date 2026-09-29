import importlib
import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
PNF_MVP_ROOT = REPO_ROOT / "pnf_mvp"
if str(PNF_MVP_ROOT) not in sys.path:
    sys.path.insert(0, str(PNF_MVP_ROOT))


class FakeEngine:
    columns = []

    def __init__(self, profile):
        self.profile = profile

    def update_from_price(self, close_ts, close_price):
        return None


class FakeStorage:
    def __init__(self, db_path):
        self.db_path = db_path


class FakeValidationStore:
    allow_multiple_trades_per_symbol = False

    def __init__(
        self,
        db_path,
        *,
        allow_multiple_trades_per_symbol=False,
        symbol_tick_provenance=None,
        symbol_identity_allowlist=None,
    ):
        self.db_path = db_path
        self.allow_multiple_trades_per_symbol = allow_multiple_trades_per_symbol

    def update_pending_with_candle(self, **kwargs):
        return None

    def register_setup(self, **kwargs):
        return "setup-id"

    def get_perf_snapshot(self):
        return {"update_pending": {"BTCUSDT": {}}}


def _run_backfill(tmp_path, extra_args, *, setup_map_override=None, structure_override=None, registration_calls=None):
    backfill = importlib.import_module("strategy_historical_backfill")
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(
        json.dumps(
            {
                "database_path": str(tmp_path / "candles.db"),
                "strategy_validation_db_path": str(tmp_path / "validation.db"),
                "symbols": ["BTCUSDT"],
                "profiles": {"BTCUSDT": {"box_size": 100, "reversal_boxes": 3}},
                "max_open_watch_per_symbol": 20,
                "allow_multiple_trades_per_symbol": False,
                "strategy_validation_execution": {
                    "activation_model": "historical_ohlc_one_tick_trade_through_v1",
                    "symbol_ticks": {
                        "BTCUSDT": {
                            "provider": "TEST",
                            "venue": "TEST_SPOT",
                            "instrument_type": "SPOT",
                            "native_symbol": "BTCUSDT",
                            "source_symbol": "BTCUSDT",
                            "tick_size": 0.01,
                            "provenance_timestamp": "2026-09-23T00:00:00Z",
                            "provenance_version": "test-v1",
                        }
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    counters = {"build": 0, "csv": 0, "parquet": 0}

    def fake_build_funnel_row(**kwargs):
        counters["build"] += 1
        return {
            "status": kwargs["setup"].get("status"),
            "registered_to_validation": 0,
            "blocked_by_existing_open_trade": 0,
        }

    def fake_evaluate_setups(symbol, profile, engine):
        structure = structure_override if structure_override is not None else {"breakout_context": "NONE"}
        setup = {"side": "LONG", "status": "REJECT", "strategy": "test"}
        timings = {
            "elapsed_build_structure_s": 0.0,
            "elapsed_eval_long_s": 0.0,
            "elapsed_eval_short_s": 0.0,
        }
        setup_map = setup_map_override if setup_map_override is not None else {"LONG": setup, "SHORT": None}
        return structure, [s for s in setup_map.values() if s], timings, setup_map

    def register_setup(self, **kwargs):
        if registration_calls is not None:
            registration_calls.append(kwargs["setup"]["side"])
        return "setup-id"

    def fake_write_funnel_csv(rows, csv_path):
        counters["csv"] += 1
        return str(Path(csv_path).resolve())

    def fake_write_funnel_parquet(rows, parquet_path):
        counters["parquet"] += 1
        return str(Path(parquet_path).resolve())

    argv = [
        "strategy_historical_backfill.py",
        "--settings",
        str(settings_path),
        "--symbols",
        "BTCUSDT",
        "--reset-validation-db",
        "--perf-json",
        str(tmp_path / "perf.json"),
        "--perf-progress-every",
        "100000",
        *extra_args,
    ]
    with patch.object(sys, "argv", argv), patch.object(backfill, "Storage", FakeStorage), patch.object(
        backfill, "StrategyValidationStore", FakeValidationStore
    ), patch.object(backfill, "PnFEngine", FakeEngine), patch.object(
        backfill, "load_all_closed_candles", return_value=[{"close_time": 1, "close": 1.0, "high": 1.0, "low": 1.0}]
    ), patch.object(backfill, "evaluate_setups", side_effect=fake_evaluate_setups), patch.object(
        backfill, "build_funnel_row", side_effect=fake_build_funnel_row
    ), patch.object(backfill, "write_funnel_csv", side_effect=fake_write_funnel_csv), patch.object(
        backfill, "write_funnel_parquet", side_effect=fake_write_funnel_parquet
    ), patch.object(backfill, "reset_validation_db"), patch.object(backfill, "table_row_count", return_value=0), patch.object(
        backfill, "has_pending_candidate_for_symbol", return_value=False
    ), patch.object(
        FakeValidationStore, "register_setup", register_setup
    ):
        with redirect_stdout(io.StringIO()):
            backfill.main()
    return counters


class FunnelGatingTests(TestCase):
    def test_research_gate_rejects_each_missing_or_conflicting_field(self):
        import strategy_historical_backfill as backfill

        structure = {"breakout_context": "POST_BREAKOUT_PULLBACK", "active_leg_boxes": 2,
                     "is_extended_move": False}
        setup = {"side": "LONG", "status": "CANDIDATE", "pullback_quality": "HEALTHY",
                 "is_baseline_profile_match": 1}
        self.assertTrue(backfill.research_long_baseline_eligible(setup, structure))
        for key, bad in (("side", "SHORT"), ("status", "WATCH"),
                         ("pullback_quality", "BROKEN"), ("is_baseline_profile_match", True),
                         ("is_baseline_profile_match", 0)):
            altered = dict(setup, **{key: bad})
            self.assertFalse(backfill.research_long_baseline_eligible(altered, structure))
            altered.pop(key)
            self.assertFalse(backfill.research_long_baseline_eligible(altered, structure))
        for key, bad in (("breakout_context", "NONE"), ("active_leg_boxes", 3),
                         ("active_leg_boxes", True), ("is_extended_move", True)):
            altered = dict(structure, **{key: bad})
            self.assertFalse(backfill.research_long_baseline_eligible(setup, altered))
            altered.pop(key)
            self.assertFalse(backfill.research_long_baseline_eligible(setup, altered))

    def test_default_mode_retains_short_registration(self):
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            registered = []
            _run_backfill(Path(temp_dir), [], setup_map_override={
                "LONG": None, "SHORT": {"side": "SHORT", "status": "CANDIDATE"}},
                registration_calls=registered)
            self.assertEqual(registered, ["SHORT"])

    def test_research_long_baseline_gate_prevents_short_or_off_profile_registration(self):
        import tempfile

        structure = {"breakout_context": "POST_BREAKOUT_PULLBACK", "active_leg_boxes": 2,
                     "is_extended_move": False}
        good = {"side": "LONG", "status": "CANDIDATE", "pullback_quality": "HEALTHY",
                "is_baseline_profile_match": 1}
        short = dict(good, side="SHORT")
        with tempfile.TemporaryDirectory() as temp_dir:
            registered = []
            _run_backfill(Path(temp_dir), ["--research-long-baseline-only"],
                          setup_map_override={"LONG": good, "SHORT": short},
                          structure_override=structure, registration_calls=registered)
            self.assertEqual(registered, ["LONG"])

        with tempfile.TemporaryDirectory() as temp_dir:
            registered = []
            _run_backfill(Path(temp_dir), ["--research-long-baseline-only"],
                          setup_map_override={"LONG": dict(good, is_baseline_profile_match=0), "SHORT": short},
                          structure_override=structure, registration_calls=registered)
            self.assertEqual(registered, [])

    def test_no_funnel_args_does_not_build_funnel_rows(self):
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            counters = _run_backfill(Path(temp_dir), [])
        self.assertEqual(counters, {"build": 0, "csv": 0, "parquet": 0})

    def test_funnel_csv_builds_rows_and_writes_csv(self):
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            tmp_path = Path(temp_dir)
            counters = _run_backfill(tmp_path, ["--funnel-csv", str(tmp_path / "funnel.csv")])
        self.assertEqual(counters, {"build": 1, "csv": 1, "parquet": 0})

    def test_funnel_parquet_builds_rows_and_writes_parquet(self):
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            tmp_path = Path(temp_dir)
            counters = _run_backfill(tmp_path, ["--funnel-parquet", str(tmp_path / "funnel.parquet")])
        self.assertEqual(counters, {"build": 1, "csv": 0, "parquet": 1})
