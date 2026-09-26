import copy
import sys
from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[1]
PNF_MVP = ROOT / "pnf_mvp"
if str(PNF_MVP) not in sys.path:
    sys.path.insert(0, str(PNF_MVP))

from pnf_engine import PnFEngine, PnFProfile
from structure_engine import build_structure_state


class PnFCausalityInvariantTests(TestCase):
    def _engine(self):
        return PnFEngine(PnFProfile("TEST", 1.0, 3))

    def test_future_prices_do_not_mutate_already_completed_columns(self):
        engine = self._engine()
        for ts, price in enumerate([100, 104, 100, 105, 101], start=1):
            engine.update_from_price(ts, price)
        completed_before = copy.deepcopy(engine.columns[:-1])

        for ts, price in enumerate([106, 102, 107, 103], start=6):
            engine.update_from_price(ts, price)

        self.assertEqual(engine.columns[: len(completed_before)], completed_before)

    def test_state_dict_is_detached_from_mutable_engine_signals(self):
        engine = self._engine()
        engine.signals = [{"type": "DOUBLE_TOP_BREAKOUT", "column_idx": 2}]

        snapshot = engine.state_dict()
        snapshot["signals"][0]["type"] = "MUTATED"

        self.assertEqual(engine.signals[0]["type"], "DOUBLE_TOP_BREAKOUT")

    def test_structure_for_a_prefix_is_not_changed_by_later_engine_updates(self):
        engine = self._engine()
        for ts, price in enumerate([100, 104, 100, 105, 101], start=1):
            engine.update_from_price(ts, price)
        prefix_columns = copy.deepcopy(engine.columns)
        prefix_state = build_structure_state(
            "TEST",
            engine.profile,
            prefix_columns,
            engine.latest_signal_name(),
            engine.market_state(),
            engine.last_price,
        )

        for ts, price in enumerate([106, 102, 107, 103], start=6):
            engine.update_from_price(ts, price)
        rebuilt_prefix_state = build_structure_state(
            "TEST",
            engine.profile,
            prefix_columns,
            prefix_state["latest_signal_name"],
            prefix_state["market_state"],
            prefix_state["last_price"],
        )

        self.assertEqual(rebuilt_prefix_state, prefix_state)
