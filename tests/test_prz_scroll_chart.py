"""Offline PRZ chart data contract and publication tests."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_v2 import prz_scroll_chart as chart


class PrzScrollChartTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.report = {"schema": "prz-x-break-pole-2024-exploratory-v1",
                       "source_sha256": chart.SOURCE_SHA256, "box_size": "100",
                       "reversal_boxes": 3, "research_only": True, "execution": "OFF",
                       "decision_facts": [
                           *({"type": "PIVOT_CONFIRMED", "pivot_id": f"pnf:{i}:{kind}", "price": price}
                             for i, kind, price in ((0, "O", "1000"), (1, "X", "1500"),
                                                    (2, "O", "1200"), (3, "X", "1400"))),
                           {"type": "ZONE_CREATED", "candidate_id": "zone-1", "direction": "LONG",
                            "state": "WAITING", "zone_known_at": 1500, "connected_column_id": 4,
                            "lower": "1100", "upper": "1200", "pivot_ids":
                            [f"pnf:{i}:{k}" for i,k in ((0,"O"),(1,"X"),(2,"O"),(3,"X"))]},
                           {"type": "ZONE_CONTACT", "candidate_id": "zone-1", "at": 1600},
                           {"type": "ZONE_FULL_TEST", "candidate_id": "zone-1", "at": 1700},
                           {"type": "ZONE_INVALIDATED", "candidate_id": "zone-1", "at": 1800} ]}
        self.columns = [{"idx": i, "kind": "X" if i%2 else "O", "top": 1500,
                         "bottom": 1000, "start": 1000+i*100, "end": 1099+i*100}
                        for i in range(7)]

    def test_zone_lifecycle_geometry_and_browser_controls(self):
        zones = chart.extract_zones(self.report)
        self.assertEqual(len(zones), 1)
        self.assertEqual([e["type"] for e in zones[0]["events"]],
                         ["ZONE_CONTACT", "ZONE_FULL_TEST", "ZONE_INVALIDATED"])
        page = chart.chart_html(self.columns, zones, source_hash="a"*64, report_hash="b"*64)
        for value in ('id="viewport"', 'id="prev"', 'id="next"', 'id="zoom"',
                      'showLong', 'showShort', 'ZONE_FULL_TEST', 'zone-1', 'XABC',
                      'Έρευνα μόνο'):
            self.assertIn(value, page)

    def test_conflicting_or_orphan_events_fail_closed(self):
        self.report["decision_facts"].append(self.report["decision_facts"][4].copy())
        with self.assertRaisesRegex(ValueError, "duplicate zone"):
            chart.extract_zones(self.report)
        self.report["decision_facts"].pop()
        self.report["decision_facts"][-1]["candidate_id"] = "unrelated"
        with self.assertRaisesRegex(ValueError, "orphaned"):
            chart.extract_zones(self.report)

    def test_hash_and_existing_output_protection(self):
        path = self.root / "report.json"
        path.write_text(json.dumps(self.report))
        candles = self.root / "candles.csv"
        candles.write_text("close_time,close\n1,1000\n")
        output = self.root / "chart.html"
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            chart.run(path, candles, output)
        self.assertFalse(output.exists())
        output.write_bytes(b"operator file")
        with self.assertRaisesRegex(ValueError, "new output"):
            chart.run(path, candles, output)
        self.assertEqual(output.read_bytes(), b"operator file")

    def test_atomic_publication_and_failure_cleanup(self):
        path = self.root / "report.json"
        path.write_text(json.dumps(self.report))
        candles = self.root / "candles.csv"
        candles.write_text("synthetic")
        output = self.root / "chart.html"
        with (patch.object(chart, "replay_columns", return_value=self.columns),
              patch.object(chart.os, "link", side_effect=PermissionError("synthetic"))):
            with self.assertRaises(PermissionError):
                chart.run(path, candles, output)
        self.assertFalse(output.exists())
        self.assertEqual(list(self.root.glob(".prz_chart_*.tmp")), [])
        with patch.object(chart, "replay_columns", return_value=self.columns):
            result = chart.run(path, candles, output)
        self.assertEqual(result["zones"], 1)
        self.assertIn("ZONE_CONTACT", output.read_text())


if __name__ == "__main__":
    unittest.main()
