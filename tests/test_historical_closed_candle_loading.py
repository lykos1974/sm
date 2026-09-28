import sys
from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[1]
PNF_MVP = ROOT / "pnf_mvp"
if str(PNF_MVP) not in sys.path:
    sys.path.insert(0, str(PNF_MVP))

import app
from strategy_historical_backfill import load_all_closed_candles


class FakeStorage:
    def __init__(self, candles):
        self.candles = candles

    def load_recent_candles(self, symbol, limit):
        assert symbol == "BTCUSDT"
        assert limit is None
        return list(self.candles)


class HistoricalClosedCandleLoadingTests(TestCase):
    def test_keeps_final_candle_when_it_is_already_closed(self):
        candles = [
            {"close_time": 60_000, "close": 1.0},
            {"close_time": 120_000, "close": 2.0},
        ]

        loaded = load_all_closed_candles(
            FakeStorage(candles), "BTCUSDT", now_ms=180_000
        )

        self.assertEqual(loaded, candles)

    def test_drops_only_candles_not_closed_by_wall_clock(self):
        candles = [
            {"close_time": 60_000, "close": 1.0},
            {"close_time": 176_000, "close": 2.0},
            {"close_time": 240_000, "close": 3.0},
        ]

        loaded = load_all_closed_candles(
            FakeStorage(candles), "BTCUSDT", now_ms=180_000
        )

        self.assertEqual(loaded, candles[:1])

    def test_live_and_historical_validation_statuses_match(self):
        from strategy_historical_backfill import VALIDATION_ELIGIBLE_STATUSES as historical

        self.assertEqual(app.VALIDATION_ELIGIBLE_STATUSES, historical)
        self.assertNotIn("REJECT", app.VALIDATION_ELIGIBLE_STATUSES)
