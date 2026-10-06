"""Offline PRZ chart data contract and publication tests."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_v2 import prz_scroll_chart as chart
from research_v2.pnf_multicolumn_sr import derive_zones
from tests.test_pnf_multicolumn_harmonic import pivots as harmonic_pivots


class PrzScrollChartTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.report = {"schema": "prz-x-break-pole-2024-exploratory-v1",
                       "source_sha256": chart.SOURCE_SHA256, "box_size": "100",
                       "reversal_boxes": 3, "research_only": True, "execution": "OFF",
                       "decision_facts": [
                           *({"type": "PIVOT_CONFIRMED", "pivot_id": f"pnf:{i}:{kind}",
                              "column_id": i, "kind": "HIGH" if kind == "X" else "LOW",
                              "price": price, "extreme_at": 1050+i*100,
                              "confirmed_at": 1100+i*100, "confirmation_sequence": i+1}
                             for i, kind, price in ((0, "O", "1000"), (1, "X", "1500"),
                                                    (2, "O", "1200"), (3, "X", "1400"))),
                           {"type": "ZONE_CREATED", "candidate_id": "zone-1", "direction": "LONG",
                            "state": "WAITING", "zone_known_at": 1500, "connected_column_id": 4,
                            "lower": "1100", "upper": "1200", "pivot_ids":
                            [f"pnf:{i}:{k}" for i,k in ((0,"O"),(1,"X"),(2,"O"),(3,"X"))]},
                           {"type": "ZONE_CONTACT", "candidate_id": "zone-1", "at": 1600},
                           {"type": "ZONE_FULL_TEST", "candidate_id": "zone-1", "at": 1700},
                           {"type": "ZONE_INVALIDATED", "candidate_id": "zone-1", "at": 1800} ]}
        self.columns = [{"idx": i, "kind": "X" if i%2 else "O",
                         "top": 1400 if i==3 else 1500,
                         "bottom": 1200 if i==2 else 1000,
                         "start": 1000+i*100, "end": 1099+i*100}
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

    def test_independent_multicolumn_layer_and_strict_column_binding(self):
        facts = self.report["decision_facts"][:4]
        facts[1]["price"] = "6800"
        facts[2]["price"] = "6400"
        facts[3]["price"] = "6900"
        zones = derive_zones(facts)
        self.assertEqual(zones, [])  # 500-point excursion is below six boxes.
        facts[2]["price"] = "6200"
        zones = derive_zones(facts)
        self.assertEqual(len(zones), 1)
        self.assertEqual(zones[0]["type"], "RESISTANCE")
        page = chart.chart_html(self.columns, [], structural_zones=zones,
                                source_hash="a"*64, report_hash="b"*64)
        self.assertIn("δομικές ζώνες", page)
        self.assertIn('id="pickSr"', page)
        self.assertIn('id="window"', page)
        with self.assertRaisesRegex(ValueError, "chronology"):
            chart.chart_html(self.columns[:-3], [], structural_zones=zones,
                             source_hash="a"*64, report_hash="b"*64)

    def test_coarse_replay_emits_single_selected_zone_not_fine_clutter(self):
        candles = self.root / "candles.csv"
        first = 1704067259999
        prices = (60000,68000,65000,69000,65900)
        candles.write_text("close_time,close\n"+"".join(
            f"{first+i*60000},{p}\n" for i,p in enumerate(prices)))
        import hashlib
        digest=hashlib.sha256(candles.read_bytes()).hexdigest()
        with (patch.object(chart,"SOURCE_SHA256",digest),
              patch.object(chart,"SOURCE_MINUTES",len(prices)),
              patch.object(chart,"FIRST_CLOSE_MS",first),
              patch.object(chart,"LAST_CLOSE_MS",first+(len(prices)-1)*60000)):
            fine, coarse = chart.replay_multiscale(candles)
        zones=chart.map_coarse_zones(fine,coarse)
        self.assertEqual(len(zones),1)
        self.assertEqual(zones[0]["type"],"RESISTANCE")
        page=chart.chart_html(fine,[],structural_zones=zones,
                              source_hash=digest,report_hash="b"*64)
        self.assertIn('selectedSr=-1',page)
        self.assertIn('i===selectedSr',page)
        self.assertIn('id="prevSr"',page)

    def test_nonconsecutive_harmonic_is_separate_selectable_layer(self):
        facts = harmonic_pivots()
        fine = [{"idx": i, "kind": "X" if i%2 else "O", "top": 11000,
                 "bottom": 1000, "start": 900+i*100, "end": 999+i*100}
                for i in range(12)]
        mapped = chart.map_harmonic_zones(fine, facts)
        chosen = next(z for z in mapped if z["coarse_pivot_columns"] == [0,3,6,9])
        self.assertEqual(chosen["known_column"], 11)
        page = chart.chart_html(fine, [], harmonic_zones=mapped,
                                source_hash="a"*64, report_hash="b"*64)
        for token in ('id="pickHarmonic"', 'selectedHarmonic=-1',
                      'i===selectedHarmonic', '"coarse_pivot_columns":[0,3,6,9]',
                      'μη διαδοχικές Fibonacci PRZ'):
            self.assertIn(token, page)

    def test_atomic_publication_and_failure_cleanup(self):
        path = self.root / "report.json"
        path.write_text(json.dumps(self.report))
        candles = self.root / "candles.csv"
        candles.write_text("synthetic")
        output = self.root / "chart.html"
        with (patch.object(chart, "replay_multiscale", return_value=(self.columns, [])),
              patch.object(chart.os, "link", side_effect=PermissionError("synthetic"))):
            with self.assertRaises(PermissionError):
                chart.run(path, candles, output)
        self.assertFalse(output.exists())
        self.assertEqual(list(self.root.glob(".prz_chart_*.tmp")), [])
        with patch.object(chart, "replay_multiscale", return_value=(self.columns, [])):
            result = chart.run(path, candles, output)
        self.assertEqual(result["gartley_prz"], 1)
        self.assertEqual(result["structural_zones"], 0)
        self.assertIn("ZONE_CONTACT", output.read_text())


if __name__ == "__main__":
    unittest.main()
