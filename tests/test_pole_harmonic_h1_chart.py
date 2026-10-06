"""Targeted offline checks for close-confirmed 1m → H1 research display."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_v2 import pole_harmonic_h1_chart as chart


class H1ChartTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.candles=self.root/'candles.csv'
        first=59999
        self.candles.write_text('close_time,open,high,low,close\n'+''.join(
            f'{first+i*60000},{100+i},{102+i},{99+i},{101+i}\n'
            for i in range(120)))
        self.digest=hashlib.sha256(self.candles.read_bytes()).hexdigest()

    def test_complete_hourly_bars_and_strict_chronology(self):
        bars=chart.aggregate_h1(self.candles, expected_minutes=120, first_close=59999,
                                last_close=7199999)
        self.assertEqual(len(bars),2)
        self.assertEqual(bars[0],{'at':0,'open':'100','high':'161','low':'99','close':'160'})
        self.assertEqual(bars[1]['at'],3600000)
        self.candles.write_text(self.candles.read_text().replace('3659999,160', '3660000,160'))
        with self.assertRaises(ValueError):
            chart.aggregate_h1(self.candles,expected_minutes=120,
                               first_close=59999,last_close=7199999)

    def test_provenance_publication_and_known_time_display(self):
        zone={'id':'z1','type':'NONCONSECUTIVE_GARTLEY_PROJECTION',
              'direction':'LONG','known_at':3599999,'lower':'95','upper':'105',
              'coarse_pivot_columns':[0,3,6,9], 'pivots':[]}
        overlap={'schema':'pole-harmonic-overlap-descriptive-v1',
                 'source_sha256':self.digest,'chart_sha256':None,
                 'harmonic_zones':1,'signals':1,'execution':'OFF',
                 'observations':[{'event_id':'one','signal_ts':3659999,
                                  'direction':'LONG','signal_close':'162',
                                  'zone_known_at':3599999,'nearest_zone_id':'z1',
                                  'category':'FAR'}]}
        html=self.root/'original.html'
        html.write_text('<script id="dataset" type="application/json">'+
                        json.dumps({'harmonic':[zone]})+'</script>')
        overlap['chart_sha256']=hashlib.sha256(html.read_bytes()).hexdigest()
        prior=self.root/'overlap.json';prior.write_text(json.dumps(overlap))
        target=self.root/'h1.html'
        with (patch.object(chart,'SOURCE_SHA256',self.digest),
              patch.object(chart,'SOURCE_MINUTES',120),
              patch.object(chart,'FIRST_CLOSE_MS',59999),
              patch.object(chart,'LAST_CLOSE_MS',7199999)):
            result=chart.run(self.candles,html,prior,target)
            self.assertEqual((result['h1_bars'],result['zones']), (2,1))
            page=target.read_text()
            for token in ('known_at', 'id="pick"', '2024', 'H1', 'mixed confirmation hour'):
                self.assertIn(token,page)
            original=target.read_bytes()
            with self.assertRaises(ValueError):chart.run(self.candles,html,prior,target)
            self.assertEqual(target.read_bytes(),original)

    def test_incomplete_hour_and_invalid_ohlc_fail_closed(self):
        original=self.candles.read_text()
        self.candles.write_text('\n'.join(original.splitlines()[:-1])+'\n')
        with self.assertRaisesRegex(ValueError,'incomplete'):
            chart.aggregate_h1(self.candles,expected_minutes=120,
                               first_close=59999,last_close=7199999)
        self.candles.write_text(original.replace('59999,100,102,99,101',
                                                 '59999,100,100,99,101'))
        with self.assertRaisesRegex(ValueError,'OHLC'):
            chart.aggregate_h1(self.candles,expected_minutes=120,
                               first_close=59999,last_close=7199999)
