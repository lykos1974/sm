"""Bounded offline, close-confirmed pole decision preview; never executes trades."""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

from pnf_mvp.strategies.du_plessis_poles_v1 import PoleDecisionLedger

# PnFEngine's historical absolute import is confined to this offline process.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'pnf_mvp'))
from pnf_engine import PnFEngine, PnFProfile  # noqa: E402


def preview(path: Path, *, box_size: float, reversal_boxes: int,
            mode: str, venue: str, max_candles: int = 10000) -> dict:
    if not path.is_file() or path.suffix.lower() != '.csv':
        raise ValueError('existing research candle CSV required')
    if type(max_candles) is not int or not 1 <= max_candles <= 10000:
        raise ValueError('bounded candle count required')
    engine = PnFEngine(PnFProfile('du_plessis_preview', box_size, reversal_boxes))
    ledger = PoleDecisionLedger(mode=mode, venue=venue)
    events = []
    with path.open(newline='', encoding='utf-8-sig') as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None or not {'close_time', 'close'}.issubset(reader.fieldnames):
            raise ValueError('close-confirmed candle columns required')
        last_ts = -1
        for n, row in enumerate(reader):
            if n >= max_candles:
                break
            ts = int(row['close_time'])
            if ts <= last_ts:
                raise ValueError('nonmonotonic candle timestamps')
            last_ts = ts
            engine.update_from_price(ts, float(row['close']))
            events.extend(vars(event) for event in ledger.ingest(
                engine.columns, box_size=str(box_size), reversal_boxes=reversal_boxes,
                enabled=True))
    return {'strategy_id': 'du_plessis_poles_v1', 'mode': mode, 'venue': venue,
            'candles_processed': min(n + 1 if 'n' in locals() else 0, max_candles),
            'execution': 'OFF', 'events': events}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candles', type=Path, required=True)
    parser.add_argument('--box-size', type=float, required=True)
    parser.add_argument('--reversal-boxes', type=int, required=True)
    parser.add_argument('--mode', choices=['EARLY_ENTRY', 'EXIT_ONLY'], default='EARLY_ENTRY')
    parser.add_argument('--venue', choices=['SPOT', 'FUTURES'], default='FUTURES')
    parser.add_argument('--max-candles', type=int, default=10000)
    args = parser.parse_args()
    print(json.dumps(preview(args.candles, box_size=args.box_size,
                             reversal_boxes=args.reversal_boxes, mode=args.mode,
                             venue=args.venue, max_candles=args.max_candles), indent=2))


if __name__ == '__main__':
    main()
