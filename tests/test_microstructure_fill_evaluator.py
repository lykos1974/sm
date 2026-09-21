import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.path.insert(0, str(path.parent))
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


EVALUATOR = load(
    "microstructure_fill_evaluator_tested",
    ROOT / "market_collector" / "microstructure_fill_evaluator.py",
)


class FillEvaluatorTests(unittest.TestCase):
    def test_order_validation_rejects_non_positive_lifetime(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "orders.csv"
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=sorted(EVALUATOR.REQUIRED_COLUMNS))
                writer.writeheader()
                writer.writerow({
                    "order_id": "x", "symbol": "BTCUSDT", "side": "LONG",
                    "available_wall_ns": 10, "expires_wall_ns": 10,
                    "ideal_entry": "100", "tick_size": "0.01",
                })
            with self.assertRaisesRegex(ValueError, "non-positive order lifetime"):
                EVALUATOR.load_orders(path)

    def test_causal_trade_through_and_quality_overlap(self):
        with tempfile.TemporaryDirectory() as tmp:
            archive = Path(tmp)
            pq.write_table(pa.Table.from_pylist([
                {"agg_trade_id": 1, "receive_wall_ns": 90, "price": "98.00",
                 "quantity": "1", "buyer_is_maker": True},
                {"agg_trade_id": 2, "receive_wall_ns": 110, "price": "99.99",
                 "quantity": "1", "buyer_is_maker": True},
            ]), archive / "agg_trades.parquet")
            pq.write_table(pa.Table.from_pylist([], schema=pa.schema([
                ("start_wall_ns", pa.int64()), ("end_wall_ns", pa.int64()),
                ("reason", pa.string()),
            ])), archive / "quality_intervals.parquet")
            manifest = {"symbol": "BTCUSDT", "first_receive_wall_ns": 1,
                        "last_receive_wall_ns": 1000}
            order = EVALUATOR.Order("o", "BTCUSDT", "LONG", 100, 200,
                                    EVALUATOR.Decimal("100"), EVALUATOR.Decimal("0.01"))
            result = EVALUATOR.evaluate_order(order, archive, manifest)
            self.assertEqual(result["classification"], "FILLED_TRADE_THROUGH")
            self.assertEqual(result["fill_evidence"]["agg_trade_id"], 2)

            pq.write_table(pa.Table.from_pylist([
                {"start_wall_ns": 120, "end_wall_ns": 130, "reason": "STALE_AGG_TRADE"}
            ]), archive / "quality_intervals.parquet")
            result = EVALUATOR.evaluate_order(order, archive, manifest)
            self.assertEqual(result["classification"], "INDETERMINATE_QUALITY_OVERLAP")


if __name__ == "__main__":
    unittest.main()
