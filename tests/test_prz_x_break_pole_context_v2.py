"""Offline causal context tests; no performance, database, or order effects."""
import unittest
import hashlib
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace

from research_v2.du_plessis_poles_preview import PnFEngine, PnFProfile
from pnf_mvp.strategies.du_plessis_poles_v1 import PoleDecisionLedger
from research_v2.gartley_pole_prz import CausalPrzLedger
from research_v2.prz_x_break_pole_context_v2 import XBreakPoleContext


def replay(prices):
    engine = PnFEngine(PnFProfile("offline", 100, 3))
    poles = PoleDecisionLedger()
    zones = CausalPrzLedger(box_size="100")
    context = XBreakPoleContext(enabled=True)
    all_context = []
    for n, price in enumerate(prices, 1):
        ts = n * 60000 - 1
        engine.update_from_price(ts, price)
        pole_events = [e for e in poles.ingest(engine.columns, box_size="100",
                                               reversal_boxes=3, enabled=True)
                       if e.status == "CANDIDATE"]
        facts = zones.ingest(engine.columns, close_ts=ts, close=price,
                             pole_events=pole_events)
        all_context.extend(context.ingest(facts, pole_events=pole_events, at=ts))
    return zones, context, all_context


class ContextTests(unittest.TestCase):
    def test_same_timestamp_or_reordered_facts_never_match(self):
        def signal(ts=5, column=4):
            return SimpleNamespace(event_id="pole:4", action="SHORT", pole_column_id=column,
                                   decision_ts=ts, status="CANDIDATE", mode="EARLY_ENTRY",
                                   pole_type="HIGH_POLE")
        zone = {"type": "ZONE_CREATED", "candidate_id": "c", "direction": "SHORT",
                "connected_column_id": 4, "zone_known_at": 1, "state": "WAITING"}
        for full_at, x_at, expected in ((2, 2, "ORDER_OR_OWNERSHIP_MISMATCH"),
                                        (3, 2, "ORDER_OR_OWNERSHIP_MISMATCH"),
                                        (2, 4, "FULL_TEST_THEN_X_BREAK_THEN_POLE")):
            with self.subTest(full_at=full_at, x_at=x_at):
                observer = XBreakPoleContext(enabled=True)
                observer.ingest([zone], pole_events=(), at=1)
                for ts in range(2, 5):
                    facts = []
                    if ts == full_at:
                        facts += [{"type": "B_VIOLATION", "candidate_id": "c", "at": ts},
                                  {"type": "ZONE_FULL_TEST", "candidate_id": "c", "at": ts}]
                    if ts == x_at:
                        facts.append({"type": "ZONE_INVALIDATED", "candidate_id": "c",
                                      "at": ts, "reason": "X_CROSSED_BEFORE_SIGNAL"})
                    observer.ingest(facts, pole_events=(), at=ts)
                self.assertEqual(observer.ingest([], pole_events=(signal(),), at=5)[0].reason,
                                 expected)

    def test_wrong_leg_and_expiry_cannot_revive(self):
        zone = {"type": "ZONE_CREATED", "candidate_id": "c", "direction": "SHORT",
                "connected_column_id": 4, "zone_known_at": 1, "state": "WAITING"}
        observer = XBreakPoleContext(enabled=True)
        observer.ingest([zone], pole_events=(), at=1)
        observer.ingest([{"type": "B_VIOLATION", "candidate_id": "c", "at": 2},
                         {"type": "ZONE_FULL_TEST", "candidate_id": "c", "at": 2}],
                        pole_events=(), at=2)
        observer.ingest([{"type": "ZONE_INVALIDATED", "candidate_id": "c", "at": 3,
                         "reason": "X_CROSSED_BEFORE_SIGNAL"}], pole_events=(), at=3)
        observer.ingest([{"type": "ZONE_EXPIRED", "candidate_id": "c", "at": 4}],
                        pole_events=(), at=4)
        wrong = SimpleNamespace(event_id="wrong", action="SHORT", pole_column_id=5,
                                decision_ts=5, status="CANDIDATE", mode="EARLY_ENTRY",
                                pole_type="HIGH_POLE")
        self.assertEqual(observer.ingest([], pole_events=(wrong,), at=5)[0].category,
                         "CONTEXT_UNAVAILABLE")
        late = SimpleNamespace(event_id="late", action="SHORT", pole_column_id=4,
                               decision_ts=6, status="CANDIDATE", mode="EARLY_ENTRY",
                               pole_type="HIGH_POLE")
        self.assertEqual(observer.ingest([], pole_events=(late,), at=6)[0].reason,
                         "CONNECTED_LEG_EXPIRED")

    def test_mirrored_real_pnf_full_test_x_cross_then_pole(self):
        for label, prices, action in (
            ("bearish", (2700,3000,2000,2600,2100,2400,2800,3200,2600), "SHORT"),
            ("bullish", (2300,2000,3000,2400,2900,2600,2200,1800,2400), "LONG"),
        ):
            with self.subTest(label=label):
                zones, context, results = replay(prices)
                self.assertEqual(len(results), 1)
                item = results[0]
                self.assertEqual((item.category, item.reason, item.direction),
                                 ("CONTEXT_MATCH", "FULL_TEST_THEN_X_BREAK_THEN_POLE", action))
                self.assertLess(item.zone_known_at, item.full_zone_test_at)
                self.assertLess(item.full_zone_test_at, item.x_cross_at)
                self.assertLess(item.x_cross_at, item.pole_signal_at)
                self.assertTrue(context.seen)
                self.assertFalse(any(r["category"] == "PRZ_MATCH" for r in zones.records
                                     if r["type"] == "POLE_ANNOTATION"))

    def test_no_full_test_no_context(self):
        *_, rows = replay((2700,3000,2000,2600,2100,2400,3200,2600))
        self.assertEqual([(r.category, r.reason) for r in rows],
                         [("CONTEXT_AVAILABLE_NONMATCH", "NO_PRIOR_FULL_TEST")])

    def test_prefix_invariance_and_replay(self):
        prefix = (2700,3000,2000,2600,2100,2400,2800,3200,2600)
        first = replay(prefix)[2]
        self.assertEqual(first, replay(prefix)[2])
        longer = replay(prefix + (2200,3100,2700))[2]
        self.assertEqual(longer[:len(first)], first)

    def test_default_off(self):
        context = XBreakPoleContext()
        self.assertEqual(context.ingest([], pole_events=(), at=1), [])
        self.assertEqual(context.seen, set())

    def test_duplicate_event_and_malformed_chronology_fail_closed(self):
        context = XBreakPoleContext(enabled=True)
        context.ingest([], pole_events=(), at=1)
        self.assertEqual(context.ingest([], pole_events=(), at=1), [])
        with self.assertRaises(ValueError):
            context.ingest([{"type": "unknown"}], pole_events=(), at=1)
        self.assertEqual(context.last_at, 1)
        self.assertEqual(context.candidates, {})

    def test_bounded_smoke_and_unchanged_baseline(self):
        from research_v2.prz_x_break_pole_context_v2_smoke import run
        from research_v2.du_plessis_poles_forward_sim import simulate
        prices = (2700,3000,2000,2600,2100,2400,2800,3200,2600)
        with tempfile.TemporaryDirectory() as root:
            candles = Path(root) / "research.csv"
            candles.write_text("close_time,open,close\n" + "".join(
                f"{n*60000-1},{price},{price}\n" for n, price in enumerate(prices, 1)))
            original = simulate(candles, box_size=100, reversal_boxes=3, max_candles=9)
            digest = hashlib.sha256(candles.read_bytes()).hexdigest()
            output = Path(root) / "new.json"
            manifest = run(candles, expected_sha256=digest, output=output, max_candles=9)
            self.assertEqual(manifest["categories"]["CONTEXT_MATCH"], 1)
            self.assertEqual(manifest["performance"], "NOT_EVALUATED")
            self.assertEqual(json.loads(output.read_text())["manifest"], manifest)
            self.assertEqual(simulate(candles, box_size=100, reversal_boxes=3,
                                      max_candles=9), original)
            with self.assertRaisesRegex(ValueError, "existing output"):
                run(candles, expected_sha256=digest, output=output, max_candles=9)


if __name__ == "__main__":
    unittest.main()
