"""Bounded, offline close-confirmed next-open simulation; no orders or P&L claims."""
from __future__ import annotations

import argparse
import csv
import json
from decimal import Decimal
from pathlib import Path

from research_v2.du_plessis_poles_preview import PnFEngine, PnFProfile
from pnf_mvp.strategies.du_plessis_poles_v1 import PoleDecisionLedger


def simulate(path: Path, *, box_size: float, reversal_boxes: int,
             venue: str = 'FUTURES', max_candles: int = 10000) -> dict:
    if (not path.is_file() or path.suffix.lower() != '.csv'
            or type(max_candles) is not int or not 2 <= max_candles <= 10000
            or type(reversal_boxes) is not int or reversal_boxes < 1
            or venue not in ('FUTURES', 'SPOT')):
        raise ValueError('invalid bounded research input')
    engine = PnFEngine(PnFProfile('du_plessis_forward_sim', box_size, reversal_boxes))
    ledger = PoleDecisionLedger(venue=venue)
    trades: list[dict] = []
    skipped = []
    pending_entry = None
    pending_exit = None
    active = None
    previous_ts = None
    processed = 0
    with path.open(newline='', encoding='utf-8-sig') as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None or not {'close_time', 'open', 'close'}.issubset(reader.fieldnames):
            raise ValueError('closed-candle OHLC CSV required')
        for row in reader:
            if processed == max_candles:
                break
            ts = int(row['close_time'])
            if previous_ts is not None and ts != previous_ts + 60000:
                raise ValueError('noncontinuous 1m research candles; simulation halted')
            open_ts = ts - 59999
            open_price, close_price = Decimal(row['open']), Decimal(row['close'])
            if (not open_price.is_finite() or not close_price.is_finite()
                    or open_price <= 0 or close_price <= 0):
                raise ValueError('invalid candle price')
            if pending_exit is not None:
                if active is None or open_ts <= pending_exit.decision_ts:
                    raise ValueError('exit chronology failure')
                active['exit_signal_ts'] = pending_exit.decision_ts
                active['exit_reason'] = pending_exit.reason
                active['exit_simulated_open_ts'] = open_ts
                active['exit_simulated_open_price'] = str(open_price)
                entry = Decimal(active['entry_simulated_open_price'])
                delta = open_price - entry if active['direction'] == 'LONG' else entry - open_price
                active['gross_price_delta'] = str(delta)
                active['status'] = 'SIMULATED_CLOSED'
                trades.append(active)
                active, pending_exit = None, None
            if pending_entry is not None:
                if active is not None or open_ts <= pending_entry.decision_ts:
                    raise ValueError('entry chronology failure')
                active = {'event_id': pending_entry.event_id,
                          'pole_type': pending_entry.pole_type,
                          'direction': pending_entry.action,
                          'signal_ts': pending_entry.decision_ts,
                          'theoretical_trigger_level': pending_entry.theoretical_trigger_level,
                          'entry_simulated_open_ts': open_ts,
                          'entry_simulated_open_price': str(open_price),
                          'status': 'SIMULATED_OPEN'}
                ledger.acknowledge_simulated_fill(
                    pending_entry.event_id, direction=pending_entry.action,
                    exchange_fill_ts=open_ts, evidence_id='OFFLINE_NEXT_OPEN_ONLY')
                pending_entry = None
            engine.update_from_price(ts, float(close_price))
            for event in ledger.ingest(engine.columns, box_size=str(box_size),
                                       reversal_boxes=reversal_boxes, enabled=True):
                if event.status == 'EXIT_CANDIDATE' and active is not None:
                    if event.event_id.startswith(active['event_id'] + ':'):
                        pending_exit = event
                elif event.status == 'CANDIDATE':
                    if active is None and pending_entry is None and pending_exit is None:
                        pending_entry = event
                    else:
                        skipped.append({'event_id': event.event_id,
                                        'reason': 'POSITION_OR_PENDING_EXISTS'})
                elif event.status == 'BLOCKED_SPOT_SHORT':
                    skipped.append({'event_id': event.event_id, 'reason': event.status})
            previous_ts = ts
            processed += 1
    if active is not None:
        trades.append(active)
    return {'strategy_id': 'du_plessis_poles_v1',
            'simulation_policy': 'NEXT_CONTIGUOUS_1M_OPEN_AFTER_CLOSED_SIGNAL_V1',
            'exit_policy': 'NEXT_OPEN_AFTER_CLOSE_CONFIRMED_PNF_REVERSAL',
            'execution': 'OFFLINE_SIMULATION_ONLY',
            'limitations': 'gross price delta only; no exchange fills, fees, slippage, funding, R or capital model',
            'candles_processed': processed, 'trades': trades, 'skipped': skipped,
            'pending_entry_event_id': None if pending_entry is None else pending_entry.event_id,
            'pending_exit_event_id': None if pending_exit is None else pending_exit.event_id}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candles', type=Path, required=True)
    parser.add_argument('--box-size', type=float, required=True)
    parser.add_argument('--reversal-boxes', type=int, required=True)
    parser.add_argument('--venue', choices=('SPOT', 'FUTURES'), default='FUTURES')
    parser.add_argument('--max-candles', type=int, default=10000)
    args = parser.parse_args()
    print(json.dumps(simulate(args.candles, box_size=args.box_size,
                              reversal_boxes=args.reversal_boxes, venue=args.venue,
                              max_candles=args.max_candles), indent=2))


if __name__ == '__main__':
    main()
