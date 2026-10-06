"""Offline, descriptive overlap of already simulated pole events and zones."""
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import hashlib
import json

from research_v2 import pole_harmonic_overlap as overlap
from research_v2.pole_harmonic_overlap import compare


def zone(at=100, direction="LONG", lower="100", upper="110"):
    return {"id": "z1", "type": "NONCONSECUTIVE_GARTLEY_PROJECTION",
            "direction": direction, "known_at": at, "lower": lower, "upper": upper,
            "pivot_columns": [0, 3, 6, 9]}


def report(at=200, direction="LONG"):
    return {"signal_annotations": [{"event_id": "e1", "direction": direction,
                                    "pole_signal_at": at}],
            "future_simulated_trade_outcomes_separate": [
                {"event_id": "e1", "direction": direction,
                 "signal_ts": at, "status": "SIMULATED_CLOSED",
                 "entry_simulated_open_price": "105", "gross_price_delta": "2"}]}


class OverlapTests(unittest.TestCase):
    def test_inside_and_one_box_are_descriptive(self):
        result=compare(report(),[zone()],{200:"105"})
        self.assertEqual(result["categories"]["IN_ZONE"]["signals"],1)
        self.assertEqual(result["categories"]["IN_ZONE"]["completed"],1)
        self.assertEqual(result["categories"]["IN_ZONE"]["gross_sum_bps_equal_notional_proxy"],
                         str(Decimal(2)*10000/Decimal(105)))
        self.assertEqual(compare(report(),[zone()],{200:"111"})["categories"]["NEAR_1_COARSE_BOX"]["signals"],1)

    def test_future_zone_or_wrong_direction_never_matches(self):
        self.assertEqual(compare(report(),[zone(at=200)],{200:"105"})["categories"]["NO_PRIOR_SAME_DIRECTION_ZONE"]["signals"],1)
        self.assertEqual(compare(report(),[zone(direction="SHORT")],{200:"105"})["categories"]["NO_PRIOR_SAME_DIRECTION_ZONE"]["signals"],1)

    def test_conflicting_ids_missing_prices_and_malformed_zone_fail(self):
        with self.assertRaises(ValueError):compare(report(),[zone()],{})
        bad=report();bad["signal_annotations"]*=2
        with self.assertRaises(ValueError):compare(bad,[zone()],{200:"105"})
        with self.assertRaises(ValueError):compare(report(),[zone(lower="NaN")],{200:"105"})
        with self.assertRaises(ValueError):compare(report(),[zone(at=True)],{200:"105"})

    def test_report_is_created_exclusively_after_verified_inputs(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp)
            candles=root/'candles.csv'
            candles.write_text('close_time,close\n200,105\n')
            digest=hashlib.sha256(candles.read_bytes()).hexdigest()
            prior=report()
            prior.update(schema='prz-x-break-pole-2024-exploratory-v1',
                         source_sha256=digest, execution='OFF', research_only=True)
            report_path=root/'prior.json';report_path.write_text(json.dumps(prior))
            report_hash=hashlib.sha256(report_path.read_bytes()).hexdigest()
            chart=root/'chart.html'
            chart.write_text('Κεριά SHA-256: '+digest+' · Αναφορά SHA-256: '+report_hash+
                             '<script id="dataset" type="application/json">'+
                             json.dumps({'harmonic':[zone()]})+'</script>')
            output=root/'overlap.json'
            with (patch.object(overlap,'SOURCE_SHA256',digest),
                  patch.object(overlap,'SOURCE_MINUTES',1),
                  patch.object(overlap,'FIRST_CLOSE_MS',200),
                  patch.object(overlap,'LAST_CLOSE_MS',200)):
                result=overlap.run(report_path,chart,candles,output)
                self.assertEqual(result['categories']['IN_ZONE']['signals'],1)
                original=output.read_bytes()
                with self.assertRaises(ValueError):
                    overlap.run(report_path,chart,candles,output)
                self.assertEqual(output.read_bytes(),original)
                output.unlink()
                chart.write_text(chart.read_text().replace(report_hash,'f'*64))
                with self.assertRaises(ValueError):
                    overlap.run(report_path,chart,candles,output)
                self.assertFalse(output.exists())
