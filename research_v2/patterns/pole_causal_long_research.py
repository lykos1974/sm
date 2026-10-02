"""Research-only causal LONG pole replay over frozen 1m candles.

Each decision is made from a prefix ending at the completed reversal column.
The later opposing-pole labels are never read or used for eligibility.
"""
from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import math
from pathlib import Path

from pnf_mvp.patterns.poles import _box_count
from research_v2.patterns.pole_core_motif_entry_timing_audit import (
    Candle, EntryTimingObservation, _candidate_observation, _load_candles,
)
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import (
    TimedColumn, _load_columns,
)
from research_v2.patterns.pole_core_motif_next_open_expectancy_audit import ENTRY_CANDIDATE
from research_v2.patterns.pole_portfolio_reality_audit import run as run_portfolio


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def causal_observations(symbol: str, columns: list[TimedColumn], box_size: float,
                        candles: list[Candle], minimum_entry_ts: int | None = None
                        ) -> list[EntryTimingObservation]:
    if not math.isfinite(box_size) or box_size <= 0:
        raise ValueError('invalid box size')
    if any(column.idx != index for index, column in enumerate(columns)):
        raise ValueError('unordered or missing PnF columns')
    if any(not math.isfinite(value) for column in columns
           for value in (column.top, column.bottom)) or any(
               column.bottom <= 0 or column.top < column.bottom for column in columns):
        raise ValueError('invalid PnF price geometry')
    times = [candle.ts for candle in candles]
    if (any(type(ts) is not int for ts in times)
            or any(right - left != 60_000 for left, right in zip(times, times[1:]))):
        raise ValueError('unordered or missing 1m candles')
    if (any(not math.isfinite(value) for candle in candles
            for value in (candle.open, candle.high, candle.low, candle.close))
            or any(candle.low <= 0 or candle.high < candle.low
                   or not candle.low <= candle.open <= candle.high
                   or not candle.low <= candle.close <= candle.high for candle in candles)):
        raise ValueError('invalid 1m OHLC geometry')
    decisions: list[EntryTimingObservation] = []
    # At the start of column j, column j-1 has just closed. No later column is
    # presented to the detector, even though the CSV contains a full year.
    for j in range(2, len(columns)):
        pole, reversal, confirmation = columns[j-2:j+1]
        if (pole.kind, reversal.kind, confirmation.kind) != ('O', 'X', 'O'):
            continue
        # Exact LOW_POLE thresholds from patterns.poles, evaluated on the
        # completed prefix only. The opposing-pole postprocessing is excluded.
        previous_o = next((columns[i] for i in range(j - 3, -1, -1)
                           if columns[i].kind == 'O'), None)
        if previous_o is None:
            continue
        pole_boxes = _box_count(pole.top, pole.bottom, box_size)
        retrace_boxes = _box_count(reversal.top, reversal.bottom, box_size)
        breakout_excess = _box_count(previous_o.bottom, pole.bottom, box_size) - 1
        if (pole_boxes <= 5 or breakout_excess < 3
                or retrace_boxes / pole_boxes <= 0.5):
            continue
        chronology = (pole.start_ts, pole.end_ts, reversal.start_ts,
                      reversal.end_ts, confirmation.start_ts)
        if (any(type(ts) is not int for ts in chronology)
                or any(left > right for left, right in zip(chronology, chronology[1:]))):
            raise ValueError('invalid causal column chronology')
        index = bisect.bisect_right(times, confirmation.start_ts)
        if index == len(times):
            continue
        eligible = times[index]
        if eligible - 59_999 <= confirmation.start_ts:
            raise ValueError('entry candle opened before the signal was known')
        if minimum_entry_ts is not None and eligible < minimum_entry_ts:
            continue
        observation = _candidate_observation(
            symbol, len(decisions) + 2, 'LONG', ENTRY_CANDIDATE,
            pole, reversal, confirmation, box_size, candles,
        )
        if observation.geometry_status != 'OBSERVABLE' or observation.observable_entry_ts != eligible:
            raise ValueError('causal observation and next-open anchor disagree')
        decisions.append(observation)
    return decisions


def run(columns_csv: Path, candles_csv: Path, output_root: Path,
        minimum_entry_ts: int, expected_columns_sha256: str,
        expected_candles_sha256: str) -> dict:
    if output_root.exists():
        raise FileExistsError('new isolated output directory required')
    if not columns_csv.is_file() or not candles_csv.is_file() or candles_csv.suffix.lower() != '.csv':
        raise ValueError('closed research CSV inputs required')
    if (_sha256(columns_csv) != expected_columns_sha256.lower()
            or _sha256(candles_csv) != expected_candles_sha256.lower()):
        raise ValueError('frozen input hash mismatch')
    if type(minimum_entry_ts) is not int or minimum_entry_ts < 10**12:
        raise ValueError('invalid warm-up boundary')
    with columns_csv.open(newline='', encoding='utf-8') as stream:
        raw_indices = [row['idx'] for row in csv.DictReader(stream)]
    if len(raw_indices) != len(set(raw_indices)):
        raise ValueError('duplicate PnF column identity')
    columns_by_idx, box_size = _load_columns(columns_csv)
    if box_size is None:
        raise ValueError('missing PnF profile')
    candles = _load_candles(candles_csv, 'BTCUSDT')
    columns = [columns_by_idx[index] for index in sorted(columns_by_idx)]
    decisions = causal_observations('BTC', columns, box_size, candles, minimum_entry_ts)
    if not decisions:
        raise ValueError('no causally eligible LONG pole decisions')

    # Use the audited portfolio simulator with its existing assumptions. The
    # injected loader changes signal generation only; the default loader stays
    # unchanged for all other callers.
    def loader(symbol_inputs, columns_inputs, candles_inputs, candle_symbols):
        if (set(symbol_inputs) != {'BTC'} or set(columns_inputs) != {'BTC'}
                or set(candles_inputs) != {'BTC'}
                or candle_symbols != {'BTC': 'BTCUSDT'}):
            raise ValueError('causal audit accepts BTCUSDT only')
        return ['BTC'], decisions, {'BTC': candles}

    run_portfolio({'BTC': columns_csv}, {'BTC': columns_csv},
                  {'BTC': candles_csv}, output_root / 'portfolio',
                  {'BTC': 'BTCUSDT'}, observation_loader=loader)
    if (_sha256(columns_csv) != expected_columns_sha256.lower()
            or _sha256(candles_csv) != expected_candles_sha256.lower()):
        raise ValueError('frozen input changed during research run')
    output_root.mkdir(parents=True, exist_ok=True)
    ledger = output_root / 'causal_decisions.csv'
    with ledger.open('x', newline='', encoding='utf-8') as stream:
        writer = csv.writer(stream)
        writer.writerow(('decision_id', 'pole_column_index', 'reversal_column_index',
                         'confirmation_column_index', 'decision_known_at_ms',
                         'entry_candle_open_ms', 'entry_candle_close_ms',
                         'direction', 'entry_price', 'stop_price'))
        for decision in decisions:
            writer.writerow((f'DEC-{decision.row_number-1:06d}', decision.pole_idx,
                             decision.reversal_idx, decision.confirmation_idx,
                             columns[decision.confirmation_idx].start_ts,
                             decision.observable_entry_ts - 59_999,
                             decision.observable_entry_ts, decision.direction,
                             decision.entry, decision.stop))
    with (output_root / 'portfolio' / 'portfolio_reality_manifest.json').open(encoding='utf-8') as stream:
        portfolio = json.load(stream)
    report = {
        'stage': 'causal_long_pole_research_v1', 'research_only': True,
        'symbol': 'BTCUSDT', 'source_columns_sha256': expected_columns_sha256.lower(),
        'source_candles_sha256': expected_candles_sha256.lower(),
        'minimum_entry_ts': minimum_entry_ts,
        'causal_decisions_sha256': _sha256(ledger),
        'decision_rule': 'LOW_POLE known at completed reversal; no opposing-pole filter',
        'decision_count': len(decisions),
        'resolved_portfolio_trades': portfolio['resolved_portfolio_trades'],
        'gross_total_R': portfolio['summary_metrics']['total_R'],
        'execution_limitations': '1m OHLC, gross R; no exchange fill, fees, slippage or funding',
    }
    with (output_root / 'causal_manifest.json').open('x', encoding='utf-8') as stream:
        json.dump(report, stream, indent=2, sort_keys=True)
        stream.write('\n')
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--columns-csv', type=Path, required=True)
    parser.add_argument('--candles-csv', type=Path, required=True)
    parser.add_argument('--output-root', type=Path, required=True)
    parser.add_argument('--minimum-entry-ts', type=int, required=True)
    parser.add_argument('--columns-sha256', required=True)
    parser.add_argument('--candles-sha256', required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.columns_csv.resolve(), args.candles_csv.resolve(),
                         args.output_root.resolve(), args.minimum_entry_ts,
                         args.columns_sha256, args.candles_sha256), indent=2))


if __name__ == '__main__':
    main()
