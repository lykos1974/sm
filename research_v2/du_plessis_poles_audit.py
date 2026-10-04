"""Bounded read-only chronology audit of one Du Plessis pole event."""
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from research_v2.du_plessis_poles_preview import PnFEngine, PnFProfile
from pnf_mvp.strategies.du_plessis_poles_v1 import PoleDecisionLedger


def _utc(milliseconds: int) -> str:
    return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).isoformat(timespec='milliseconds')


def audit(path: Path, *, pole_index: int, retrace_index: int,
          box_size: float, reversal_boxes: int, max_candles: int = 10000) -> dict:
    if (not path.is_file() or path.suffix.lower() != '.csv'
            or type(pole_index) is not int or type(retrace_index) is not int
            or retrace_index != pole_index + 1
            or type(max_candles) is not int or not 1 <= max_candles <= 10000
            or type(reversal_boxes) is not int or reversal_boxes < 1):
        raise ValueError('invalid bounded research input')
    engine = PnFEngine(PnFProfile('du_plessis_audit', box_size, reversal_boxes))
    ledger = PoleDecisionLedger()
    seen = None
    prior_close = None
    prior_ts = -1
    processed = 0
    with path.open(newline='', encoding='utf-8-sig') as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None or not {'close_time', 'close'}.issubset(reader.fieldnames):
            raise ValueError('closed-candle CSV columns required')
        for row in reader:
            if processed >= max_candles:
                break
            ts = int(row['close_time'])
            if ts <= prior_ts:
                raise ValueError('nonmonotonic candle timestamps')
            prior_ts = ts
            close = float(row['close'])
            before = prior_close
            prior_close = close
            processed += 1
            engine.update_from_price(ts, close)
            fresh = ledger.ingest(engine.columns, box_size=str(box_size),
                                  reversal_boxes=reversal_boxes, enabled=True)
            for event in fresh:
                if (event.pole_column_id, event.retracement_column_id) == (pole_index, retrace_index):
                    if seen is not None:
                        raise ValueError('duplicate pole decision')
                    cols = engine.columns
                    pole, retrace = cols[pole_index], cols[retrace_index]
                    box = Decimal(str(box_size))
                    top = int(Decimal(str(pole.top)) / box)
                    bottom = int(Decimal(str(pole.bottom)) / box)
                    rtop = int(Decimal(str(retrace.top)) / box)
                    rbottom = int(Decimal(str(retrace.bottom)) / box)
                    length = top - bottom + 1
                    retraced = rtop - rbottom + 1
                    prior = cols[pole_index - 3:pole_index]
                    excess = (top - max(int(Decimal(str(c.top)) / box) for c in prior)
                              if pole.kind == 'X' else
                              min(int(Decimal(str(c.bottom)) / box) for c in prior) - bottom)
                    if (length != event.column_length_boxes or retraced != event.retracement_boxes
                            or excess != event.breakout_excess_boxes or 2 * retraced < length
                            or event.decision_ts != ts or retrace.end_ts != ts):
                        raise ValueError('independent chronology/box check failed')
                    seen = {
                        'event_id': event.event_id,
                        'decision_utc': _utc(ts), 'decision_ts': ts,
                        'previous_candle_close': before,
                        'decision_candle_close': close,
                        'pole_kind': pole.kind, 'retrace_kind': retrace.kind,
                        'pole_start_utc': _utc(pole.start_ts),
                        'pole_last_update_utc': _utc(pole.end_ts),
                        'pole_column_id': pole.idx,
                        'retracement_column_id': retrace.idx,
                        'pole_bounds': [pole.bottom, pole.top],
                        'retracement_bounds_at_decision': [retrace.bottom, retrace.top],
                        'column_length_boxes': length,
                        'retracement_boxes': retraced,
                        'breakout_excess_boxes': excess,
                        'threshold_met': 2 * retraced >= length,
                        'structural_strict_gt_50': 2 * retraced > length,
                        'theoretical_trigger_level': event.theoretical_trigger_level,
                        'status': event.status,
                        'execution': 'OFF; no fill inferred',
                    }
            if seen is not None:
                break
    if seen is None:
        raise ValueError('event not found within bounded prefix')
    seen['candles_processed'] = processed
    return seen


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candles', required=True, type=Path)
    parser.add_argument('--pole-index', required=True, type=int)
    parser.add_argument('--retrace-index', required=True, type=int)
    parser.add_argument('--box-size', required=True, type=float)
    parser.add_argument('--reversal-boxes', required=True, type=int)
    parser.add_argument('--max-candles', default=10000, type=int)
    args = parser.parse_args()
    print(json.dumps(audit(args.candles, pole_index=args.pole_index,
                           retrace_index=args.retrace_index, box_size=args.box_size,
                           reversal_boxes=args.reversal_boxes,
                           max_candles=args.max_candles), indent=2))


if __name__ == '__main__':
    main()
