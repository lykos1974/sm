import csv
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

OBSERVER = load("observer_for_tools", ROOT / "pnf_mvp" / "strategy_setup_observer.py")
CHECKER = load("observer_checker", ROOT / "pnf_mvp" / "check_strategy_setup_observer.py")
EXPORTER = load("observer_exporter", ROOT / "pnf_mvp" / "export_observed_ideal_entry_orders.py")

class ObserverToolsTests(unittest.TestCase):
    def test_checker_and_exporter_use_closed_valid_windows_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            database, output = root / "observer.db", root / "orders.csv"
            observer = OBSERVER.StrategySetupObserver(database, ["BTCUSDT"], ["CANDIDATE"], 65)
            setup = {"strategy": "x", "side": "LONG", "status": "CANDIDATE",
                     "quality_score": 80, "ideal_entry": 100, "invalidation": 98,
                     "tp1": 104, "tp2": 106}
            observer.observe("BTCUSDT", 1000, [setup], {"current_column_index": 1}, 10_000, 50)
            observer.observe("BTCUSDT", 2000, [], {"current_column_index": 1}, 20_000, 60)
            observer.close()
            report = CHECKER.check(database)
            self.assertTrue(report["pass"])
            self.assertEqual(report["lifecycle_counts"], {"WITHDRAWN": 1})
            self.assertEqual(EXPORTER.export_orders(database, output, "0.01"), 1)
            with output.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["available_wall_ns"], "10000")
            self.assertEqual(rows[0]["expires_wall_ns"], "20000")
            self.assertEqual(rows[0]["tick_size"], "0.01")

if __name__ == "__main__":
    unittest.main()
