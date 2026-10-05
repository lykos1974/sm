"""One pinned 2024 BTCUSDT offline annual pole replay; no optimization or orders."""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

from research_v2.du_plessis_poles_forward_sim import simulate

SOURCE_SHA256 = '8045aa135a611d4b4fc2ca0cde9a8fa68905ad4480054399f33ed77ab8f6851f'
SOURCE_MINUTES = 527040  # Leap year, 366 * 1,440 closed minutes.
FIRST_CLOSE_MS = 1704067259999
LAST_CLOSE_MS = 1735689599999


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def summarize(replay: dict) -> dict:
    completed = [t for t in replay['trades'] if t['status'] == 'SIMULATED_CLOSED']
    unresolved = [t for t in replay['trades'] if t['status'] == 'SIMULATED_OPEN']
    if len(completed) + len(unresolved) != len(replay['trades']):
        raise ValueError('unknown simulation state')
    groups = defaultdict(list)
    for trade in completed:
        entry = Decimal(trade['entry_simulated_open_price'])
        exit_ = Decimal(trade['exit_simulated_open_price'])
        delta = (exit_ - entry if trade['direction'] == 'LONG' else entry - exit_)
        if entry <= 0 or delta != Decimal(trade['gross_price_delta']):
            raise ValueError('inconsistent gross trade accounting')
        bps = delta * Decimal(10000) / entry
        groups['all'].append(bps)
        from datetime import datetime, timezone
        date = datetime.fromtimestamp(trade['signal_ts'] / 1000, timezone.utc)
        if date.year != 2024:
            raise ValueError('signal outside pinned UTC year')
        groups[f'2024-Q{(date.month - 1)//3 + 1}'].append(bps)

    def stats(values: list[Decimal]) -> dict:
        total = sum(values, Decimal(0))
        streak = maximum = 0
        for value in values:
            streak = streak + 1 if value < 0 else 0
            maximum = max(maximum, streak)
        return {'completed': len(values), 'positive': sum(x > 0 for x in values),
                'negative': sum(x < 0 for x in values), 'zero': sum(x == 0 for x in values),
                'gross_sum_bps_constant_notional_proxy': str(total),
                'gross_average_bps_per_trade': str(total / len(values)) if values else None,
                'maximum_consecutive_losses': maximum}

    return {'completed_trades': len(completed), 'open_at_end': len(unresolved),
            'pending_entry_at_end': replay['pending_entry_event_id'],
            'pending_exit_at_end': replay['pending_exit_event_id'],
            'skipped_or_blocked_signals': len(replay['skipped']),
            'annual': stats(groups['all']),
            'quarters': {f'2024-Q{q}': stats(groups[f'2024-Q{q}']) for q in range(1, 5)},
            'gross_only': True,
            'units': 'bps per filled trade relative to its entry; sum is equal-notional proxy, not capital return',
            'break_even_symmetric_bps_per_side_before_other_costs': (
                str(sum(groups['all'], Decimal(0)) / (2 * len(completed))) if completed else None)}


def run(path: Path) -> dict:
    if not path.is_file() or path.suffix.lower() != '.csv':
        raise ValueError('pinned research CSV required')
    actual = _sha256(path)
    if actual != SOURCE_SHA256:
        raise ValueError('2024 research candle SHA-256 mismatch')
    replay = simulate(path, box_size=100.0, reversal_boxes=3,
                      max_candles=SOURCE_MINUTES, full_year_verified=True)
    if (replay['candles_processed'] != SOURCE_MINUTES
            or replay['first_close_ts'] != FIRST_CLOSE_MS
            or replay['last_close_ts'] != LAST_CLOSE_MS):
        raise ValueError('incomplete pinned UTC year')
    return {'schema': 'du-plessis-poles-annual-offline-v1',
            'strategy_id': replay['strategy_id'], 'source_sha256': actual,
            'period': '2024-01-01T00:00:00Z/2025-01-01T00:00:00Z',
            'candles': SOURCE_MINUTES, 'box_size': '100', 'reversal_boxes': 3,
            'fill_assumption': replay['simulation_policy'],
            'exit_assumption': replay['exit_policy'],
            'summary': summarize(replay),
            'limitations': 'no actual fills, fee, slippage, funding, position sizing, R or portfolio capital return'}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candles', required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(run(args.candles), indent=2))


if __name__ == '__main__':
    main()
