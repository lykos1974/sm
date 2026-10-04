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
from dataclasses import replace
from decimal import Decimal
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

TARGET_SWEEP_R = (2.5, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)
STOP_SWEEP_BOXES = (2, 3, 4, 6)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def stop_variants(decisions: list[EntryTimingObservation],
                  stop_boxes: tuple[int, ...] = STOP_SWEEP_BOXES
                  ) -> dict[int, list[EntryTimingObservation]]:
    """Change only the prospective stop; keep every causal decision and entry fixed."""
    if (not stop_boxes or len(set(stop_boxes)) != len(stop_boxes)
            or any(type(n) is not int or n < 1 or n > 10 for n in stop_boxes)):
        raise ValueError('invalid bounded stop grid')
    result = {}
    for n in stop_boxes:
        rows = []
        for decision in decisions:
            if (decision.direction != 'LONG' or decision.entry is None
                    or decision.stop is None or not math.isfinite(decision.entry)
                    or decision.entry - n * decision.box_size <= 0):
                raise ValueError('invalid stop geometry')
            rows.append(replace(decision, stop=decision.entry - n * decision.box_size))
        result[n] = rows
    return result


def _stop_sweep(output_root: Path, columns_csv: Path, candles_csv: Path,
                decisions: list[EntryTimingObservation], candles: list[Candle],
                columns_hash: str, candles_hash: str) -> None:
    """Independent portfolio replay per stop; no production execution changes."""
    sweep = output_root / 'stop_sweep'
    variants = stop_variants(decisions)
    summaries = []
    for boxes, observations in variants.items():
        def loader(symbol_inputs, columns_inputs, candles_inputs, candle_symbols):
            if (set(symbol_inputs) != {'BTC'} or set(columns_inputs) != {'BTC'}
                    or set(candles_inputs) != {'BTC'}
                    or candle_symbols != {'BTC': 'BTCUSDT'}):
                raise ValueError('stop sweep accepts BTCUSDT only')
            return ['BTC'], observations, {'BTC': candles}

        directory = output_root / 'portfolio' if boxes == 3 else sweep / f'stop_{boxes}_boxes'
        if boxes != 3:
            run_portfolio({'BTC': columns_csv}, {'BTC': columns_csv},
                          {'BTC': candles_csv}, directory, {'BTC': 'BTCUSDT'},
                          observation_loader=loader, target_r=2.5)
        manifest_path = directory / 'portfolio_reality_manifest.json'
        with manifest_path.open(encoding='utf-8') as stream:
            manifest = json.load(stream)
        with (directory / 'portfolio_reality_trade_sequence.csv').open(newline='', encoding='utf-8') as stream:
            trades = list(csv.DictReader(stream))
        with (directory / 'portfolio_reality_quarterly.csv').open(newline='', encoding='utf-8') as stream:
            quarters = list(csv.DictReader(stream))
        if (manifest['resolved_portfolio_trades'] != len(trades)
                or manifest['target_R'] != 2.5):
            raise ValueError('stop sweep portfolio evidence mismatch')
        # Opportunity IDs are assigned in sorted local-identity order by the
        # portfolio runner; only validated, actually accepted trades incur cost.
        ordered = sorted(observations, key=lambda item: (
            item.symbol, item.row_number, item.direction,
            -1 if item.observable_entry_ts is None else item.observable_entry_ts))
        by_id = {f'OPP-{i:06d}': row for i, row in enumerate(ordered, 1)}
        cost_per_bps = Decimal(0)
        for trade in trades:
            row = by_id[trade['opportunity_id']]
            entry = Decimal(str(row.entry))
            distance = entry - Decimal(str(row.stop))
            if distance <= 0:
                raise ValueError('invalid stop cost geometry')
            cost_per_bps += Decimal(2) / Decimal(10000) * entry / distance
        gross = Decimal(str(manifest['summary_metrics']['total_R']))
        summaries.append({
            'stop_boxes': boxes, 'decisions': len(decisions), 'resolved_trades': len(trades),
            'gross_total_R': str(gross),
            'gross_max_drawdown_R': manifest['summary_metrics']['max_drawdown_R'],
            'worst_quarter_gross_R': min((float(q['total_R']) for q in quarters), default=''),
            'break_even_symmetric_bps_per_side': str(gross / cost_per_bps)
                if cost_per_bps and gross > 0 else '',
            'estimated_R_at_10bps_per_side': str(gross - 10 * cost_per_bps),
            'portfolio_manifest_sha256': _sha256(manifest_path),
        })
    sweep.mkdir(parents=True, exist_ok=True)
    comparison = sweep / 'comparison.csv'
    with comparison.open('x', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    with (sweep / 'comparison_manifest.json').open('x', encoding='utf-8') as stream:
        json.dump({'schema': 'causal-pole-stop-sweep-v1', 'research_only': True,
                   'stop_boxes': list(STOP_SWEEP_BOXES), 'baseline_stop_boxes': 3,
                   'target_R': 2.5, 'break_even_trigger_R': 2.0,
                   'columns_sha256': columns_hash, 'candles_sha256': candles_hash,
                   'comparison_sha256': _sha256(comparison),
                   'interpretation': 'independent 1m OHLC portfolio replays; cost is an entry-notional approximation, no verified exchange fills'},
                  stream, indent=2, sort_keys=True)
        stream.write('\n')


def _twenty_r_check(output_root: Path, columns_csv: Path, candles_csv: Path,
                    decisions: list[EntryTimingObservation], candles: list[Candle],
                    columns_hash: str, candles_hash: str) -> None:
    """One independent 20R portfolio replay against the original 2.5R run."""
    def loader(symbol_inputs, columns_inputs, candles_inputs, candle_symbols):
        if (set(symbol_inputs) != {'BTC'} or set(columns_inputs) != {'BTC'}
                or set(candles_inputs) != {'BTC'}
                or candle_symbols != {'BTC': 'BTCUSDT'}):
            raise ValueError('20R check accepts BTCUSDT only')
        return ['BTC'], decisions, {'BTC': candles}

    directory = output_root / 'target_20' / 'portfolio'
    run_portfolio({'BTC': columns_csv}, {'BTC': columns_csv},
                  {'BTC': candles_csv}, directory, {'BTC': 'BTCUSDT'},
                  observation_loader=loader, target_r=20)
    summaries = []
    for target, location in ((2.5, output_root / 'portfolio'), (20, directory)):
        manifest_file = location / 'portfolio_reality_manifest.json'
        with manifest_file.open(encoding='utf-8') as stream:
            manifest = json.load(stream)
        with (location / 'portfolio_reality_trade_sequence.csv').open(newline='', encoding='utf-8') as stream:
            trades = list(csv.DictReader(stream))
        with (location / 'portfolio_reality_quarterly.csv').open(newline='', encoding='utf-8') as stream:
            quarters = list(csv.DictReader(stream))
        if manifest['target_R'] != target or manifest['resolved_portfolio_trades'] != len(trades):
            raise ValueError('20R portfolio evidence mismatch')
        ordered = sorted(decisions, key=lambda item: (
            item.symbol, item.row_number, item.direction,
            -1 if item.observable_entry_ts is None else item.observable_entry_ts))
        by_id = {f'OPP-{i:06d}': row for i, row in enumerate(ordered, 1)}
        cost_per_bps = Decimal(0)
        for trade in trades:
            row = by_id[trade['opportunity_id']]
            entry = Decimal(str(row.entry))
            risk = entry - Decimal(str(row.stop))
            if risk <= 0:
                raise ValueError('invalid 20R cost geometry')
            cost_per_bps += Decimal(2) * entry / (Decimal(10000) * risk)
        gross = Decimal(str(manifest['summary_metrics']['total_R']))
        summaries.append({
            'target_R': target, 'decisions': len(decisions),
            'resolved_trades': len(trades), 'gross_total_R': str(gross),
            'gross_max_drawdown_R': manifest['summary_metrics']['max_drawdown_R'],
            'worst_quarter_gross_R': min((float(q['total_R']) for q in quarters), default=''),
            'break_even_symmetric_bps_per_side': str(gross / cost_per_bps)
                if cost_per_bps and gross > 0 else '',
            'estimated_R_at_10bps_per_side': str(gross - 10 * cost_per_bps),
            'portfolio_manifest_sha256': _sha256(manifest_file),
        })
    comparison = output_root / 'target_20/comparison.csv'
    with comparison.open('x', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    with (output_root / 'target_20/comparison_manifest.json').open('x', encoding='utf-8') as stream:
        json.dump({'schema': 'causal-pole-target-20-check-v1', 'research_only': True,
                   'targets_R': [2.5, 20], 'stop_boxes': 3,
                   'break_even_trigger_R': 2, 'columns_sha256': columns_hash,
                   'candles_sha256': candles_hash,
                   'comparison_sha256': _sha256(comparison),
                   'interpretation': 'independent 1m OHLC portfolio replay, approximate entry-notional fee stress, no exchange fills'},
                  stream, indent=2, sort_keys=True)
        stream.write('\n')


def causal_observations(symbol: str, columns: list[TimedColumn], box_size: float,
                        candles: list[Candle], minimum_entry_ts: int | None = None,
                        maximum_entry_ts: int | None = None
                        ) -> list[EntryTimingObservation]:
    if not math.isfinite(box_size) or box_size <= 0:
        raise ValueError('invalid box size')
    if maximum_entry_ts is not None and (type(maximum_entry_ts) is not int
            or (minimum_entry_ts is not None and maximum_entry_ts <= minimum_entry_ts)):
        raise ValueError('invalid entry period')
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
        if maximum_entry_ts is not None and eligible >= maximum_entry_ts:
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
        expected_candles_sha256: str, maximum_entry_ts: int | None = None,
        target_sweep: bool = False, stop_sweep: bool = False,
        target_20_only: bool = False) -> dict:
    if output_root.exists():
        raise FileExistsError('new isolated output directory required')
    if not columns_csv.is_file() or not candles_csv.is_file() or candles_csv.suffix.lower() != '.csv':
        raise ValueError('closed research CSV inputs required')
    if (_sha256(columns_csv) != expected_columns_sha256.lower()
            or _sha256(candles_csv) != expected_candles_sha256.lower()):
        raise ValueError('frozen input hash mismatch')
    if type(minimum_entry_ts) is not int or minimum_entry_ts < 10**12:
        raise ValueError('invalid warm-up boundary')
    if maximum_entry_ts is not None and (type(maximum_entry_ts) is not int
                                          or maximum_entry_ts <= minimum_entry_ts):
        raise ValueError('invalid entry period')
    if (any(type(flag) is not bool for flag in (target_sweep, stop_sweep, target_20_only))
            or sum((target_sweep, stop_sweep, target_20_only)) > 1):
        raise ValueError('select at most one bounded sweep')
    with columns_csv.open(newline='', encoding='utf-8') as stream:
        raw_indices = [row['idx'] for row in csv.DictReader(stream)]
    if len(raw_indices) != len(set(raw_indices)):
        raise ValueError('duplicate PnF column identity')
    columns_by_idx, box_size = _load_columns(columns_csv)
    if box_size is None:
        raise ValueError('missing PnF profile')
    candles = _load_candles(candles_csv, 'BTCUSDT')
    columns = [columns_by_idx[index] for index in sorted(columns_by_idx)]
    decisions = causal_observations('BTC', columns, box_size, candles,
                                    minimum_entry_ts, maximum_entry_ts)

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
    if stop_sweep:
        _stop_sweep(output_root, columns_csv, candles_csv, decisions, candles,
                    expected_columns_sha256.lower(), expected_candles_sha256.lower())
    if target_20_only:
        _twenty_r_check(output_root, columns_csv, candles_csv, decisions, candles,
                        expected_columns_sha256.lower(), expected_candles_sha256.lower())
    if target_sweep:
        sweep_root = output_root / 'target_sweep'
        rows = []
        for target_r in TARGET_SWEEP_R:
            directory = (output_root / 'portfolio' if target_r == 2.5 else
                         sweep_root / f'target_{target_r:g}R')
            if target_r != 2.5:
                run_portfolio({'BTC': columns_csv}, {'BTC': columns_csv},
                              {'BTC': candles_csv}, directory, {'BTC': 'BTCUSDT'},
                              observation_loader=loader, target_r=target_r)
            manifest_path = directory / 'portfolio_reality_manifest.json'
            with manifest_path.open(encoding='utf-8') as stream:
                variant = json.load(stream)
            with (directory / 'portfolio_reality_trade_sequence.csv').open(newline='', encoding='utf-8') as stream:
                trades = list(csv.DictReader(stream))
            with (directory / 'portfolio_reality_quarterly.csv').open(newline='', encoding='utf-8') as stream:
                quarters = list(csv.DictReader(stream))
            # A stop in the fill candle is a resolved -1R trade, not an
            # unclassified observation. Keep it separate from later stops.
            counts = {name: sum(trade['classification'] == name for trade in trades)
                      for name in ('TARGET_FIRST', 'STOP_FIRST',
                                   'SAME_CANDLE_FILL_STOP_CONSERVATIVE',
                                   'BREAK_EVEN_EXIT')}
            if (variant['target_R'] != target_r or
                    sum(counts.values()) != variant['resolved_portfolio_trades']):
                raise ValueError('target comparison evidence mismatch')
            rows.append({'target_R': target_r, 'decisions': len(decisions),
                         'resolved_trades': len(trades), **counts,
                         'gross_total_R': variant['summary_metrics']['total_R'],
                         'gross_average_R': variant['summary_metrics']['average_R_per_trade'],
                         'max_drawdown_R': variant['summary_metrics']['max_drawdown_R'],
                         'quarters_with_trades': len(quarters),
                         'worst_quarter_R': min((float(q['total_R']) for q in quarters),
                                                default=''),
                         'longest_losing_streak': variant['summary_metrics']['longest_losing_streak'],
                         'portfolio_manifest_sha256': _sha256(manifest_path)})
        sweep_root.mkdir(parents=True, exist_ok=True)
        with (sweep_root / 'comparison.csv').open('x', newline='', encoding='utf-8') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        with (sweep_root / 'comparison_manifest.json').open('x', encoding='utf-8') as stream:
            json.dump({'schema': 'causal-pole-target-sweep-v1', 'research_only': True,
                       'targets_R': list(TARGET_SWEEP_R), 'baseline_target_R': 2.5,
                       'break_even_trigger_R': 2.0, 'minimum_entry_ts': minimum_entry_ts,
                       'maximum_entry_ts': maximum_entry_ts,
                       'columns_sha256': expected_columns_sha256.lower(),
                       'candles_sha256': expected_candles_sha256.lower(),
                       'comparison_sha256': _sha256(sweep_root / 'comparison.csv'),
                       'interpretation': 'gross OHLC, same causal decisions, independent portfolio replay per target; no fees, slippage, funding or exchange fills'},
                      stream, indent=2, sort_keys=True)
            stream.write('\n')
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
    if maximum_entry_ts is not None:
        report['maximum_entry_ts'] = maximum_entry_ts
        report['entry_cohort_policy'] = 'UTC start inclusive, end exclusive; later exits allowed'
    if target_sweep:
        report['target_sweep_manifest'] = 'target_sweep/comparison_manifest.json'
    if stop_sweep:
        report['stop_sweep_manifest'] = 'stop_sweep/comparison_manifest.json'
    if target_20_only:
        report['target_20_manifest'] = 'target_20/comparison_manifest.json'
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
    parser.add_argument('--stop-sweep', action='store_true', help='research-only 2/3/4/6 box stop comparison')
    parser.add_argument('--target-20-only', action='store_true', help='one independent 20R research comparison')
    args = parser.parse_args()
    print(json.dumps(run(args.columns_csv.resolve(), args.candles_csv.resolve(),
                         args.output_root.resolve(), args.minimum_entry_ts,
                         args.columns_sha256, args.candles_sha256,
                         stop_sweep=args.stop_sweep,
                         target_20_only=args.target_20_only), indent=2))


if __name__ == '__main__':
    main()
