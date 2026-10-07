"""Build isolated, hash-pinned BTC 2025 candle/PnF CSVs from verified public ZIPs.

No operational DB, network, strategy simulation, order action, or settings access.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
from pathlib import Path
import shutil
import zipfile

from research_v2.patterns.binance_um_2025_archive_preflight import SCHEMA, inspect_month
from research_v2.du_plessis_poles_preview import PnFEngine, PnFProfile

FORMAT = 'binance-um-btc-2025-frozen-inputs-v1'
COLUMNS = ('symbol', 'profile_name', 'idx', 'kind', 'top', 'bottom', 'start_ts', 'end_ts')
CANDLES = ('symbol', 'interval', 'open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume')


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def _unique(pairs):
    data = {}
    for key, value in pairs:
        if key in data:
            raise ValueError('Duplicate manifest key')
        data[key] = value
    return data


def build(source: Path, output: Path, expected_manifest_sha256: str,
          *, months: tuple[int, ...] = tuple(range(1, 13))) -> dict:
    source = source.resolve(strict=True)
    output = output.resolve()
    if not source.is_dir() or source == output or source in output.parents or output.exists():
        raise ValueError('New independent output directory required')
    manifest_path = source / 'binance_um_btc_2025_preflight.json'
    if len(expected_manifest_sha256) != 64 or _sha(manifest_path) != expected_manifest_sha256.lower():
        raise ValueError('Source preflight manifest hash mismatch')
    with manifest_path.open(encoding='utf-8') as stream:
        manifest = json.load(stream, object_pairs_hook=_unique)
    if (manifest.get('schema') != SCHEMA or manifest.get('venue') != 'BINANCE_UM'
            or manifest.get('symbol') != 'BTCUSDT' or manifest.get('interval') != '1m'
            or manifest.get('year') != 2025 or manifest.get('complete_year') is not True
            or type(manifest.get('months')) is not list or len(manifest['months']) != 12
            or manifest.get('rows') != 525600):
        raise ValueError('Invalid complete-year preflight')
    verified = [inspect_month(source, m) for m in range(1, 13)]
    if verified != manifest['months'] or sum(item['rows'] for item in verified) != manifest['rows']:
        raise ValueError('Monthly archives differ from preflight')
    profile = PnFProfile('BTCUSDT_bs100_rev3', 100.0, 3)
    engine = PnFEngine(profile)
    output.mkdir(parents=False)
    try:
        candles_path = output / 'candles_1m.csv'
        with candles_path.open('x', newline='', encoding='utf-8') as target:
            writer = csv.writer(target)
            writer.writerow(CANDLES)
            total = 0
            for m in months:
                name = f'BTCUSDT-1m-2025-{m:02}.zip'
                with zipfile.ZipFile(source / name) as z, z.open(name[:-4] + '.csv') as raw:
                    for row in csv.reader(io.TextIOWrapper(raw, encoding='utf-8-sig', newline='')):
                        if row and row[0].strip().lower() in ('open_time', 'open time'):
                            continue
                        writer.writerow(('BTCUSDT', '1m', row[0], row[6], *row[1:6]))
                        engine.update_from_price(int(row[6]), float(row[4]))
                        total += 1
        if months == tuple(range(1, 13)) and total != 525600:
            raise ValueError('Incomplete output candles')
        columns_path = output / 'columns.csv'
        with columns_path.open('x', newline='', encoding='utf-8') as target:
            writer = csv.writer(target)
            writer.writerow(COLUMNS)
            for col in engine.columns:
                writer.writerow(('BTCUSDT', profile.name, col.idx, col.kind,
                                 col.top, col.bottom, col.start_ts, col.end_ts))
        report = {'schema': FORMAT, 'research_only': True, 'source_manifest_sha256': expected_manifest_sha256.lower(),
                  'source_manifest_path': str(manifest_path), 'venue': 'BINANCE_UM', 'symbol': 'BTCUSDT',
                  'year': 2025, 'box_size': 100, 'reversal_boxes': 3,
                  'pnf_price_source': 'closed_1m_close', 'candles': total, 'columns': len(engine.columns),
                  'candles_sha256': _sha(candles_path), 'columns_sha256': _sha(columns_path),
                  'limitations': 'research inputs only; no execution, fees, funding or profitability'}
        with (output / 'frozen_inputs_manifest.json').open('x', encoding='utf-8') as target:
            json.dump(report, target, indent=2, sort_keys=True)
            target.write('\n')
        if _sha(manifest_path) != expected_manifest_sha256.lower() or any(
                inspect_month(source, m) != verified[m - 1] for m in range(1, 13)):
            raise ValueError('Source changed during conversion')
        return report
    except BaseException:
        shutil.rmtree(output)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive-dir', required=True, type=Path)
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--output-dir', required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(build(args.archive_dir, args.output_dir, args.manifest_sha256), indent=2))


if __name__ == '__main__':
    main()
