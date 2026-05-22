from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from research_v2.optimizers.validate_geometry_stability import _chronological_split, validate_geometry_stability


class ValidateGeometryStabilityTests(unittest.TestCase):
    def _write(self, path: Path, rows: list[dict[str, str]]) -> None:
        fields = ["reference_ts", "side", "symbol", "resolution_status", "realized_r_multiple"]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    def test_chronological_split_correctness(self) -> None:
        rows = [{"reference_ts": ts} for ts in ["2024-01-03", "2024-01-01", "2024-01-02", "2024-01-04", "2024-01-05"]]
        splits = _chronological_split(rows, config=type("cfg", (), {"train_fraction": 0.6, "validation_fraction": 0.2, "oos_fraction": 0.2})())
        self.assertEqual([r["reference_ts"] for r in splits["train"]], ["2024-01-01", "2024-01-02", "2024-01-03"])
        self.assertEqual([r["reference_ts"] for r in splits["validation"]], ["2024-01-04"])
        self.assertEqual([r["reference_ts"] for r in splits["oos"]], ["2024-01-05"])

    def test_metrics_and_warnings(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            retained = base / "retained.csv"
            excluded = base / "excluded.csv"

            retained_rows = []
            for i in range(60):
                retained_rows.append({"reference_ts": f"2024-01-{i+1:02d}", "side": "LONG" if i % 2 == 0 else "SHORT", "symbol": "ETHUSDT" if i < 50 else "BTCUSDT", "resolution_status": "TP2" if i < 36 else "STOPPED", "realized_r_multiple": "0.5" if i < 36 else "-0.6"})
            for i in range(60, 100):
                retained_rows.append({"reference_ts": f"2024-03-{i-59:02d}", "side": "LONG" if i % 2 == 0 else "SHORT", "symbol": "ETHUSDT" if i < 92 else "BTCUSDT", "resolution_status": "STOPPED", "realized_r_multiple": "-0.4"})

            excluded_rows = []
            for i in range(1, 41):
                excluded_rows.append({"reference_ts": f"2024-02-{i:02d}", "side": "SHORT", "symbol": "ETHUSDT", "resolution_status": "TP2" if i <= 20 else "STOPPED", "realized_r_multiple": "0.1" if i <= 20 else "-0.7"})

            self._write(retained, retained_rows)
            self._write(excluded, excluded_rows)
            out = validate_geometry_stability(retained_rows_csv=str(retained), excluded_rows_csv=str(excluded), output_root=str(base / "out"))

            with Path(out["warnings_csv"]).open("r", encoding="utf-8") as handle:
                warnings = list(csv.DictReader(handle))
            codes = {w["warning_code"] for w in warnings}
            self.assertIn("SYMBOL_CONCENTRATION", codes)
            self.assertIn("TRAIN_ONLY_EDGE_COLLAPSE", codes)
            self.assertIn("SMALL_OOS_OR_VALIDATION_SAMPLE", codes)

            with Path(out["split_metrics_csv"]).open("r", encoding="utf-8") as handle:
                metrics = list(csv.DictReader(handle))
            retained_train = next(r for r in metrics if r["population"] == "retained" and r["split"] == "train")
            self.assertEqual(retained_train["rows"], "60")


if __name__ == "__main__":
    unittest.main()
