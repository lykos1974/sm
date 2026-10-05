"""Independent bounded exit chronology check for one offline simulated pole."""
from __future__ import annotations

import argparse
import csv
import json
from decimal import Decimal
from pathlib import Path

from research_v2.du_plessis_poles_preview import PnFEngine, PnFProfile
from research_v2.du_plessis_poles_forward_sim import simulate


def audit_exit(path: Path, *, event_id: str, box_size: float,
               reversal_boxes: int, max_candles: int = 10000) -> dict:
    if type(event_id) is not str or not event_id.startswith('du_plessis_poles_v1:EARLY_ENTRY:'):
        raise ValueError('explicit pole event identity required')
    result = simulate(path, box_size=box_size, reversal_boxes=reversal_boxes,
                      max_candles=max_candles)
    matches = [trade for trade in result['trades'] if trade['event_id'] == event_id]
    if len(matches) != 1 or matches[0]['status'] != 'SIMULATED_CLOSED':
        raise ValueError('one completed simulated trade required')
    trade = matches[0]
    engine = PnFEngine(PnFProfile('independent_exit_audit', box_size, reversal_boxes))
    signal_column = None
    signal_close = None
    next_open = None
    prior_ts = None
    processed = 0
    pole_index, retrace_index = (int(value) for value in event_id.rsplit(':', 2)[1:])
    expected_kind = 'X' if trade['direction'] == 'SHORT' else 'O'
    first_reversal_ts = None
    with path.open(newline='', encoding='utf-8-sig') as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None or not {'close_time', 'open', 'close'}.issubset(reader.fieldnames):
            raise ValueError('closed 1m candle CSV required')
        for row in reader:
            if processed >= max_candles:
                break
            ts = int(row['close_time'])
            if prior_ts is not None and ts != prior_ts + 60000:
                raise ValueError('noncontinuous candle chronology')
            prior_ts = ts
            processed += 1
            if signal_column is not None:
                next_open = {'timestamp': ts - 59999, 'price': Decimal(row['open'])}
                break
            engine.update_from_price(ts, float(row['close']))
            current = engine.columns[-1]
            if (first_reversal_ts is None and ts > trade['entry_simulated_open_ts']
                    and current.idx > retrace_index and current.kind == expected_kind
                    and current.start_ts == ts):
                first_reversal_ts = ts
            if ts == trade['exit_signal_ts']:
                if len(engine.columns) < retrace_index + 2:
                    raise ValueError('exit reversal column absent')
                active = engine.columns[-1]
                retrace = engine.columns[retrace_index]
                if (active.idx <= retrace_index or active.kind != expected_kind
                        or retrace.kind == expected_kind or first_reversal_ts != ts
                        or active.start_ts != ts
                        or active.end_ts != ts or ts <= trade['entry_simulated_open_ts']):
                    raise ValueError('not the next valid post-entry PnF reversal')
                signal_column = {'index': active.idx, 'kind': active.kind,
                                 'bottom': active.bottom, 'top': active.top}
                signal_close = Decimal(row['close'])
    if signal_column is None or next_open is None:
        raise ValueError('reversal or following open unavailable')
    if (next_open['timestamp'] != trade['exit_simulated_open_ts']
            or next_open['price'] != Decimal(trade['exit_simulated_open_price'])
            or next_open['timestamp'] <= trade['exit_signal_ts']):
        raise ValueError('simulated exit not at next contiguous open')
    return {'event_id': event_id, 'direction': trade['direction'],
            'pole_column_id': pole_index, 'retracement_column_id': retrace_index,
            'reversal_column': signal_column,
            'exit_signal_close_ts': trade['exit_signal_ts'],
            'exit_signal_candle_close': str(signal_close),
            'following_open_ts': next_open['timestamp'],
            'following_open_price': str(next_open['price']),
            'simulated_entry_open_price': trade['entry_simulated_open_price'],
            'gross_price_delta': trade['gross_price_delta'],
            'chronology': 'PASS', 'execution': 'OFFLINE_SIMULATION_ONLY',
            'candles_processed': processed}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candles', required=True, type=Path)
    parser.add_argument('--event-id', required=True)
    parser.add_argument('--box-size', type=float, required=True)
    parser.add_argument('--reversal-boxes', type=int, required=True)
    parser.add_argument('--max-candles', type=int, default=10000)
    args = parser.parse_args()
    print(json.dumps(audit_exit(args.candles, event_id=args.event_id,
                                box_size=args.box_size,
                                reversal_boxes=args.reversal_boxes,
                                max_candles=args.max_candles), indent=2))


if __name__ == '__main__':
    main()
