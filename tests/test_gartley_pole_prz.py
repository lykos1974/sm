"""Synthetic offline tests; no market data, database, or exchange access."""
import unittest
import hashlib
import json
import tempfile
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from research_v2.gartley_pole_prz import CausalPrzLedger, Pivot, distance, project


@dataclass
class Column:
    idx: int
    kind: str
    top: int
    bottom: int
    start_ts: int
    end_ts: int


@dataclass(frozen=True)
class Pole:
    event_id: str
    pole_column_id: int
    decision_ts: int
    action: str = "LONG"


def pivots(offset=0, mirror=False):
    values = (1000, 2000, 1382, 1850)
    if mirror:
        values = tuple(4000 - p for p in values)
    kinds = ("LOW", "HIGH", "LOW", "HIGH") if not mirror else ("HIGH", "LOW", "HIGH", "LOW")
    return [Pivot(f"p{i}", i, kind, Decimal(value+offset), i+1, i+10, i+1)
            for i, (kind, value) in enumerate(zip(kinds, values))]


def columns(mirror=False):
    values = [(1600, 1000), (2000, 1400), (1800, 1382), (1850, 1500), (1800, 1600)]
    kinds = "OXOXO"
    if mirror:
        values = [(4000-bottom, 4000-top) for top, bottom in values]
        kinds = "XOXOX"
    return [Column(i, kind, top, bottom, i+1, 10+i) for i, (kind, (top, bottom)) in enumerate(zip(kinds, values))]


class GeometryTests(unittest.TestCase):
    def test_equivalent_abcd_and_mirror(self):
        for mirror in (False, True):
            candidate, reason = project(pivots(mirror=mirror), connected_column_id=4)
            self.assertEqual(reason, "CANDIDATE")
            self.assertEqual(candidate.direction, "SHORT" if mirror else "LONG")
            self.assertEqual(candidate.r_b, Decimal("0.618"))
            self.assertEqual(candidate.d_abcd, Decimal(2768 if mirror else 1232))
            self.assertEqual(candidate.d_bc, candidate.d_abcd)
            self.assertEqual(candidate.width, Decimal(18))
            self.assertEqual(distance(candidate.lower, candidate.lower, candidate.upper), 0)

    def test_ratio_boundaries_and_width(self):
        p = pivots()
        p[2] = Pivot("p2", 2, "LOW", Decimal(1412), 3, 12, 3)  # 0.588
        self.assertNotEqual(project(p, connected_column_id=4)[1], "B_RATIO")
        p[2] = Pivot("p2", 2, "LOW", Decimal(1351), 3, 12, 3)  # 0.649
        self.assertEqual(project(p, connected_column_id=4)[1], "B_RATIO")
        p = pivots()
        p[2] = Pivot("p2", 2, "LOW", Decimal(1412), 3, 12, 3)
        p[3] = Pivot("p3", 3, "HIGH", Decimal(1930), 4, 13, 4)
        self.assertEqual(project(p, connected_column_id=4)[1], "ZONE_WIDTH")

    def test_variant_projection_and_unconfirmed(self):
        p = pivots()
        p[3] = Pivot("p3", 3, "HIGH", Decimal(1800), 4, 13, 4)
        self.assertEqual(project(p, connected_column_id=4)[1], "EXCLUDED_VARIANT")
        self.assertEqual(project(p, connected_column_id=5)[1], "NONCONSECUTIVE_OR_UNCONFIRMED")
        p[3] = Pivot("p3", 3, "HIGH", Decimal(1382), 4, 13, 4)
        self.assertEqual(project(p, connected_column_id=4)[1], "PIVOT_GEOMETRY")

    def test_zero_width_and_b_equality(self):
        p = pivots()
        p[3] = Pivot("p3", 3, "HIGH", Decimal(1832), 4, 13, 4)
        c, reason = project(p, connected_column_id=4)
        self.assertEqual((reason, c.width), ("CANDIDATE", Decimal(0)))
        self.assertEqual(distance(Decimal(1230), c.lower, c.upper), Decimal(16))

    def test_reject_float_nonfinite_and_bool(self):
        ledger = CausalPrzLedger()
        for bad in (True, 1.5, "NaN", "Infinity", "1e999", "1\x00"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                ledger.ingest(columns()[:1], close_ts=10, close=bad)


class ChronologyTests(unittest.TestCase):
    def setUp(self):
        self.ledger = CausalPrzLedger()
        self.cols = columns()

    def create(self, close=1800):
        # Last X is confirmed only when the next O exists. The first four
        # pivots are the consecutive, completed X/A/B/C geometry.
        for count in range(1, 6):
            self.ledger.ingest(self.cols[:count], close_ts=20+count,
                               close=close if count == 5 else 1800)
        return next(iter(self.ledger.candidates.values()))

    def test_no_unconfirmed_c_and_late_creation(self):
        with self.assertRaisesRegex(ValueError, "missing or reordered"):
            CausalPrzLedger().ingest(self.cols, close_ts=25, close=1800)
        for count in range(1, 5):
            self.ledger.ingest(self.cols[:count], close_ts=20+count, close=1800)
        self.assertEqual(self.ledger.candidates, {})
        self.ledger.ingest(self.cols, close_ts=25, close=1214)
        c = next(iter(self.ledger.candidates.values()))
        self.assertEqual(self.ledger.states[c.candidate_id]["phase"], "UNAVAILABLE")

    def test_partial_then_full_then_pole(self):
        c = self.create()
        self.ledger.ingest(self.cols, close_ts=26, close=1220)
        self.assertIsNone(self.ledger.states[c.candidate_id]["full_zone_test_at"])
        self.ledger.ingest(self.cols, close_ts=27, close=1214)
        event = Pole("pole:4", 4, 28)
        result = self.ledger.ingest(self.cols, close_ts=28, close=1400, pole_events=[event])
        annotation = next(r for r in result if r["type"] == "POLE_ANNOTATION")
        self.assertEqual((annotation["category"], annotation["reason"]), ("PRZ_MATCH", "PRIOR_FULL_TEST"))
        self.assertEqual(self.ledger.ingest(self.cols, close_ts=28, close=1400, pole_events=[event]), [])
        later = self.ledger.ingest(self.cols, close_ts=29, close=1500,
                                   pole_events=[Pole("another:4", 4, 29)])
        self.assertEqual(next(r for r in later if r["type"] == "POLE_ANNOTATION")["category"],
                         "PRZ_AVAILABLE_NONMATCH")

    def test_same_timestamp_full_test_and_wrong_leg(self):
        c = self.create()
        event = Pole("pole:4", 4, 26)
        result = self.ledger.ingest(self.cols, close_ts=26, close=1214, pole_events=[event])
        self.assertEqual(next(r for r in result if r["type"] == "POLE_ANNOTATION")["reason"],
                         "SAME_TIMESTAMP_FULL_TEST")
        result = self.ledger.ingest(self.cols, close_ts=27, close=1400,
                                    pole_events=[Pole("pole:5", 5, 27)])
        self.assertEqual(next(r for r in result if r["type"] == "POLE_ANNOTATION")["category"],
                         "PRZ_UNAVAILABLE")

    def test_b_equality_does_not_count_as_violation(self):
        c = self.create()
        self.ledger.ingest(self.cols, close_ts=26, close=1382)
        self.assertIsNone(self.ledger.states[c.candidate_id]["b_violated_at"])
        self.ledger.ingest(self.cols, close_ts=27, close=1214)
        self.assertEqual(self.ledger.states[c.candidate_id]["b_violated_at"], 27)

    def test_reject_status_direction_and_late_pole(self):
        self.create()
        self.ledger.ingest(self.cols, close_ts=26, close=1214)
        for n, action in enumerate(("SHORT", "EXIT_LONG"), 27):
            result = self.ledger.ingest(self.cols, close_ts=n, close=1400,
                                        pole_events=[Pole(f"wrong:{n}", 4, n, action)])
            self.assertEqual(next(r for r in result if r["type"] == "POLE_ANNOTATION")["category"],
                             "PRZ_UNAVAILABLE")

    def test_event_evaluated_before_connected_leg_retirement(self):
        c = self.create()
        self.ledger.ingest(self.cols, close_ts=26, close=1214)
        self.cols.append(Column(5, "X", 1900, 1700, 27, 27))
        fresh = self.ledger.ingest(self.cols, close_ts=27, close=1400,
                                   pole_events=[Pole("closing:4", 4, 27)])
        self.assertEqual(next(r for r in fresh if r["type"] == "POLE_ANNOTATION")["category"],
                         "PRZ_MATCH")
        self.assertEqual(self.ledger.states[c.candidate_id]["phase"], "MATCHED")

    def test_x_invalidation_and_completed_leg_expiry(self):
        c = self.create()
        self.ledger.ingest(self.cols, close_ts=26, close=1000)
        self.assertEqual(self.ledger.states[c.candidate_id]["phase"], "INVALIDATED")
        other = CausalPrzLedger()
        for count in range(1, 6):
            other.ingest(self.cols[:count], close_ts=20+count, close=1800)
        self.cols.append(Column(5, "X", 2000, 1750, 27, 27))
        other.ingest(self.cols, close_ts=27, close=1800)
        self.assertEqual(other.states[c.candidate_id]["phase"], "EXPIRED")

    def test_prefix_invariance_and_conflicting_duplicate(self):
        c = self.create()
        before = list(self.ledger.records)
        self.ledger.ingest(self.cols, close_ts=26, close=1220)
        self.assertEqual(before, self.ledger.records[:len(before)])
        with self.assertRaisesRegex(ValueError, "conflicting"):
            self.ledger.ingest(self.cols, close_ts=26, close=1214)
        with self.assertRaisesRegex(ValueError, "retrospectively"):
            altered = columns()
            altered[3].top += 100
            self.ledger.ingest(altered, close_ts=27, close=1200)

    def test_restart_batch_stream_determinism(self):
        sequence = [(self.cols[:n], 20+n, 1800, ()) for n in range(1, 6)]
        sequence += [(self.cols, 26, 1214, ()),
                     (self.cols, 27, 1400, (Pole("pole:4", 4, 27),))]
        a, b = CausalPrzLedger(), CausalPrzLedger()
        for col, ts, close, events in sequence:
            a.ingest(col, close_ts=ts, close=close, pole_events=events)
        for col, ts, close, events in sequence:
            b.ingest(col, close_ts=ts, close=close, pole_events=events)
        self.assertEqual(a.records, b.records)


class OfflineBoundaryTests(unittest.TestCase):
    def test_bounded_report_and_baseline_output_unchanged(self):
        from research_v2.gartley_pole_prz_smoke import run
        from research_v2.du_plessis_poles_forward_sim import simulate
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "candles.csv"
            source.write_text("close_time,open,close\n" + "".join(
                f"{n*60000-1},{p},{p}\n" for n, p in enumerate(
                    (700, 1000, 700, 1000, 700, 1900, 1300, 1900), 1)))
            expected = hashlib.sha256(source.read_bytes()).hexdigest()
            baseline_before = simulate(source, box_size=100, reversal_boxes=3, max_candles=8)
            output = Path(root) / "report.json"
            report = run(source, expected_sha256=expected, output=output, max_candles=8)
            self.assertEqual(report["execution"], "OFF")
            self.assertEqual(json.loads(output.read_text())["manifest"], report)
            content = json.loads(output.read_text())
            self.assertIn("decision_events", content)
            self.assertIn("future_outcomes_separate", content)
            self.assertEqual(simulate(source, box_size=100, reversal_boxes=3,
                                      max_candles=8), baseline_before)
            with self.assertRaisesRegex(ValueError, "existing"):
                run(source, expected_sha256=expected, output=output, max_candles=8)
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                run(source, expected_sha256="0"*64,
                    output=Path(root)/"other.json", max_candles=8)


if __name__ == "__main__":
    unittest.main()
