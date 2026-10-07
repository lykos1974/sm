"""Research-only, frozen BTC hourly breakout. Never connects to a broker or DB."""
from __future__ import annotations

import argparse
from collections import deque
import csv
from decimal import Decimal as D
import hashlib
import json
from pathlib import Path
from datetime import datetime, timezone

COSTS = (D(0), D(2), D(5), D(10))


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1048576), b''):
            h.update(block)
    return h.hexdigest()


def candles(path):
    with path.open(newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or len(set(reader.fieldnames)) != len(reader.fieldnames) or not {'close_time', 'open', 'high', 'low', 'close'} <= set(reader.fieldnames):
            raise ValueError('Invalid candle header')
        previous = None
        for row in reader:
            ts = int(row['close_time'])
            if previous is not None and ts - previous != 60000:
                raise ValueError('Missing/duplicate minute')
            previous = ts
            if row.get('symbol') not in (None, 'BTCUSDT') or row.get('interval') not in (None, '1m'):
                raise ValueError('Wrong symbol/timeframe')
            values = (row[k] for k in ('open', 'high', 'low', 'close'))
            if any(len(v) > 80 for v in (row[k] for k in ('open', 'high', 'low', 'close'))):
                raise ValueError('Oversized candle value')
            op, hi, lo, cl = map(D, values)
            if not all(x.is_finite() and x > 0 for x in (op, hi, lo, cl)) or not lo <= min(op, cl) <= max(op, cl) <= hi:
                raise ValueError('Invalid OHLC')
            yield ts, op, hi, lo, cl


def simulate(stream, *, expected_minutes=None):
    history = deque(maxlen=21)
    active = pending = hour = None
    trades = []
    signals = count = 0
    for ts, op, hi, lo, cl in stream:
        count += 1
        start = ts - 59999
        if hour is None:
            hour = {'start': start, 'high': hi, 'low': lo, 'close': cl, 'count': 0}
        if start != hour['start'] + hour['count'] * 60000:
            raise ValueError('Incomplete hourly candle')
        if pending is not None:
            if active is not None or ts <= pending['signal_ts']:
                raise ValueError('Noncausal entry')
            risk = pending['risk']
            active = {'signal_ts': pending['signal_ts'], 'entry_ts': start, 'entry': op,
                      'stop': op - risk, 'target': op + D('2.5') * risk, 'risk': risk}
            pending = None
            if active['stop'] <= 0:
                raise ValueError('Stop below zero')
        if active is not None:
            reason = price = None
            if op <= active['stop']:
                reason, price = 'STOP_GAP', op
            elif lo <= active['stop']:
                reason, price = 'STOP_FIRST', active['stop']
            elif hi >= active['target']:
                reason, price = 'TARGET', active['target']
            elif ts - active['entry_ts'] >= 48 * 3600000 - 1:
                reason, price = 'TIMEOUT', cl
            if reason is not None:
                gross = (D(-1) if reason == 'STOP_FIRST' else
                         D('2.5') if reason == 'TARGET' else
                         (price - active['entry']) / active['risk'])
                trades.append({'signal_ts': active['signal_ts'], 'entry_ts': active['entry_ts'],
                               'exit_ts': ts, 'entry': str(active['entry']), 'exit': str(price),
                               'risk': str(active['risk']), 'reason': reason, 'gross_R': str(gross),
                               'modeled_R': {str(b): str(gross - 2*b/D(10000)*active['entry']/active['risk']) for b in COSTS}})
                active = None
        hour['high'] = max(hour['high'], hi)
        hour['low'] = min(hour['low'], lo)
        hour['close'] = cl
        hour['count'] += 1
        if hour['count'] == 60:
            if hour['start'] % 3600000 != 0 or ts != hour['start'] + 3599999:
                raise ValueError('Misaligned hour')
            if len(history) == 21 and active is None and pending is None and cl > max(h['high'] for h in list(history)[-20:]):
                pair = list(history)[-14:] + [hour]
                tr = [max(pair[i]['high']-pair[i]['low'], abs(pair[i]['high']-pair[i-1]['close']),
                          abs(pair[i]['low']-pair[i-1]['close'])) for i in range(1, 15)]
                risk = 2 * sum(tr, D(0)) / D(14)
                if risk > 0:
                    pending = {'signal_ts': ts, 'risk': risk}
                    signals += 1
            history.append(hour)
            hour = None
    if hour is not None or (expected_minutes is not None and count != expected_minutes):
        raise ValueError('Incomplete input')
    return {'minutes': count, 'signals': signals, 'trades': trades,
            'open_at_end': active is not None, 'pending_at_end': pending is not None}


def run(path: Path, expected_sha: str, output: Path, *, expected_minutes=525600):
    if output.exists() or not output.parent.is_dir() or len(expected_sha) != 64:
        raise ValueError('New output and SHA required')
    if sha(path) != expected_sha.lower():
        raise ValueError('Input SHA mismatch')
    result = simulate(candles(path), expected_minutes=expected_minutes)
    if sha(path) != expected_sha.lower():
        raise ValueError('Input changed during run')
    trades = result['trades']
    equity = peak = drawdown = D(0)
    losses = longest_losses = 0
    for trade in trades:
        outcome = D(trade['gross_R'])
        equity += outcome
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
        losses = losses + 1 if outcome < 0 else 0
        longest_losses = max(longest_losses, losses)
    quarters = {}
    for trade in trades:
        dt = datetime.fromtimestamp(trade['exit_ts'] / 1000, timezone.utc)
        key = f'{dt.year}-Q{(dt.month - 1) // 3 + 1}'
        item = quarters.setdefault(key, {'trades': 0, 'positive': 0, 'gross_R': D(0)})
        item['trades'] += 1
        item['positive'] += D(trade['gross_R']) > 0
        item['gross_R'] += D(trade['gross_R'])
    report = {'schema': 'btc-hourly-breakout-v1', 'research_only': True,
              'source_sha256': expected_sha.lower(), 'symbol': 'BTCUSDT',
              'minutes': result['minutes'], 'signals': result['signals'],
              'resolved_trades': len(trades), 'open_at_end': result['open_at_end'],
              'pending_at_end': result['pending_at_end'],
              'quarters': {k: {'trades': v['trades'], 'positive': v['positive'],
                               'gross_R': str(v['gross_R'])} for k, v in sorted(quarters.items())},
              'gross_total_R': str(sum((D(t['gross_R']) for t in trades), D(0))),
              'gross_max_drawdown_R': str(drawdown),
              'longest_losing_streak': longest_losses,
              'modeled_total_R_by_bps_per_side': {str(b): str(sum((D(t['modeled_R'][str(b)]) for t in trades), D(0))) for b in COSTS},
              'rules': 'LONG H1 close above prior 20 completed H1 highs; next 1m open; stop 2 ATR14; target 2.5R; timeout 48h; stop-first 1m',
              'limitations': 'exploratory OHLC and hypothetical costs; no fills, funding or net claim',
              'trades': trades}
    with output.open('x', encoding='utf-8') as f:
        json.dump(report, f, indent=2)
        f.write('\n')
    return {k: v for k, v in report.items() if k != 'trades'}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--candles', required=True, type=Path)
    p.add_argument('--sha256', required=True)
    p.add_argument('--output', required=True, type=Path)
    p.add_argument('--expected-minutes', type=int, default=525600)
    a = p.parse_args()
    if a.expected_minutes not in (525600, 527040):
        p.error('Only full 2024 or 2025 UTC years are supported')
    print(json.dumps(run(a.candles, a.sha256, a.output,
                         expected_minutes=a.expected_minutes), indent=2))


if __name__ == '__main__':
    main()
