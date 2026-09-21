import importlib.util
import sys
import tempfile
import time
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


OBSERVER = load("observer_e2e", ROOT / "pnf_mvp" / "strategy_setup_observer.py")
EXPORTER = load("observer_exporter_e2e", ROOT / "pnf_mvp" / "export_observed_ideal_entry_orders.py")
EVALUATOR = load("fill_evaluator_e2e", ROOT / "market_collector" / "microstructure_fill_evaluator.py")


class ObserverFillEndToEndTests(unittest.TestCase):
    def _archive(self, root, name, start, end, trades):
        path = root / name
        path.mkdir()
        pq.write_table(pa.Table.from_pylist(trades, schema=pa.schema([
            ("agg_trade_id", pa.int64()), ("receive_wall_ns", pa.int64()),
            ("price", pa.string()), ("quantity", pa.string()),
            ("buyer_is_maker", pa.bool_()),
        ])), path / "agg_trades.parquet")
        pq.write_table(pa.Table.from_pylist([], schema=pa.schema([
            ("start_wall_ns", pa.int64()), ("end_wall_ns", pa.int64()),
            ("reason", pa.string()),
        ])), path / "quality_intervals.parquet")
        session = {"session_key": name, "symbol": "BTCUSDT",
                   "started_wall_ns": start, "ended_wall_ns": end, "status": "CLOSED"}
        return EVALUATOR.ArchiveEvidence(
            path, {"symbol": "BTCUSDT", "first_receive_wall_ns": start,
                   "last_receive_wall_ns": end}, name, (session,),
        )

    def test_observer_export_and_multi_archive_fill_share_exact_causal_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            database, orders_csv = root / "observer.db", root / "orders.csv"
            observer = OBSERVER.StrategySetupObserver(
                database, ["BTCUSDT"], ["CANDIDATE"], 65, 3
            )
            reference_close_ms = time.time_ns() // 1_000_000
            available = reference_close_ms * 1_000_000 + 1_000
            expiry = (reference_close_ms + 180_000) * 1_000_000
            setup = {"strategy": "pullback_retest", "side": "LONG",
                     "status": "CANDIDATE", "quality_score": 80,
                     "ideal_entry": 100.0, "invalidation": 98.0,
                     "tp1": 104.0, "tp2": 106.0}
            structure = {"current_column_index": 7}
            observer.observe(
                "BTCUSDT", reference_close_ms, [setup], structure, available, 1
            )
            observer.observe(
                "BTCUSDT", reference_close_ms + 180_000, [setup], structure, expiry, 2
            )
            observer.close()
            self.assertEqual(EXPORTER.export_orders(database, orders_csv, "0.01"), 1)
            order = EVALUATOR.load_orders(orders_csv)[0]
            self.assertEqual((order.available_wall_ns, order.expires_wall_ns),
                             (available, expiry))

            boundary = available + (expiry - available) // 2
            first = self._archive(root, "day-one", available - 1, boundary, [])
            second = self._archive(root, "day-two", boundary, expiry + 1, [{
                "agg_trade_id": 11, "receive_wall_ns": boundary + 1,
                "price": "99.99", "quantity": "0.10", "buyer_is_maker": True,
            }])
            result = EVALUATOR.evaluate_order_across(order, [first, second])
            self.assertEqual(result["classification"], "FILLED_TRADE_THROUGH")
            self.assertEqual(result["fill_evidence"]["agg_trade_id"], 11)


if __name__ == "__main__":
    unittest.main()
