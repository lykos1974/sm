"""Offline or explicit public-download preflight for Binance USD-M BTCUSDT 2025 1m.

Research-only: files in a new directory; never opens a database or runs a strategy.
"""
from __future__ import annotations

import argparse
import calendar
import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import io
import json
from pathlib import Path
import re
from urllib.request import urlopen
import zipfile

BASE = 'https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1m/'
SCHEMA = 'binance-um-btc-2025-monthly-1m-preflight-v1'
MAX_ZIP = 128 * 1024 * 1024


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def _fetch(base: str, name: str, folder: Path) -> None:
    target = folder / name
    if target.exists():
        return
    temporary = folder / (name + '.partial')
    if temporary.exists():
        raise ValueError(f'Incomplete download: {name}; remove only its .partial file and retry')
    url = base + name
    with urlopen(url, timeout=30) as response, temporary.open('xb') as output:
        if response.geturl() != url or response.status != 200:
            raise ValueError('Archive request redirected or failed')
        count = 0
        while block := response.read(1024 * 1024):
            count += len(block)
            if count > MAX_ZIP:
                raise ValueError('Archive exceeds size bound')
            output.write(block)
    temporary.replace(target)


def _number(text: str, *, positive: bool = False) -> Decimal:
    if not re.fullmatch(r'(?:0|[1-9][0-9]*)(?:\.[0-9]+)?', text) or len(text) > 80:
        raise ValueError('Invalid kline number')
    try:
        value = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError('Invalid kline number') from exc
    if not value.is_finite() or value < 0 or (positive and value <= 0):
        raise ValueError('Invalid kline number')
    return value


def _expected_count(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1] * 1440


def inspect_month(folder: Path, month: int, *, expected_year: int = 2025) -> dict:
    if not 1 <= month <= 12:
        raise ValueError('Invalid month')
    name = f'BTCUSDT-1m-{expected_year}-{month:02}.zip'
    archive, checksum = folder / name, folder / (name + '.CHECKSUM')
    if not archive.is_file() or not checksum.is_file():
        raise ValueError(f'Missing ZIP or CHECKSUM for {expected_year}-{month:02}')
    if archive.stat().st_size > MAX_ZIP or checksum.stat().st_size > 256:
        raise ValueError('Archive/checksum exceeds size bound')
    match = re.fullmatch(r'([0-9a-fA-F]{64})\s+\*?' + re.escape(name) + r'\s*', checksum.read_text(encoding='ascii'))
    if match is None:
        raise ValueError('Invalid official checksum format')
    digest = _sha(archive)
    if digest != match.group(1).lower():
        raise ValueError(f'ZIP SHA-256 mismatch: {name}')
    start = int(datetime(expected_year, month, 1, tzinfo=timezone.utc).timestamp() * 1000)
    count_expected = _expected_count(expected_year, month)
    rows = 0
    with zipfile.ZipFile(archive) as z:
        members = z.infolist()
        if len(members) != 1 or members[0].filename != name[:-4] + '.csv' or members[0].file_size > 64 * 1024 * 1024:
            raise ValueError('Unexpected ZIP member')
        with z.open(members[0]) as raw:
            for row in csv.reader(io.TextIOWrapper(raw, encoding='utf-8-sig', newline='')):
                if rows == 0 and row and row[0].strip().lower() in ('open_time', 'open time'):
                    continue
                if len(row) != 12 or rows >= count_expected:
                    raise ValueError('Malformed or excess kline row')
                try:
                    opened, closed = int(row[0]), int(row[6])
                except ValueError as exc:
                    raise ValueError('Invalid timestamp') from exc
                if opened != start + rows * 60000 or closed != opened + 59999:
                    raise ValueError(f'Missing, duplicate, out-of-order or invalid close at row {rows}')
                op, hi, lo, cl, vol = (_number(row[i], positive=i != 5) for i in (1, 2, 3, 4, 5))
                if lo > min(op, cl) or hi < max(op, cl) or lo > hi:
                    raise ValueError('Invalid OHLC')
                rows += 1
    if rows != count_expected:
        raise ValueError(f'Incomplete month {expected_year}-{month:02}: {rows}/{count_expected}')
    return {'month': f'{expected_year}-{month:02}', 'zip': name, 'zip_sha256': digest, 'rows': rows,
            'first_open_ms': start, 'last_open_ms': start + (rows - 1) * 60000}


def run(folder: Path, *, fetch: bool = False, months: tuple[int, ...] = tuple(range(1, 13))) -> dict:
    folder = folder.resolve(strict=True)
    if not folder.is_dir():
        raise ValueError('Archive directory missing')
    report_path = folder / 'binance_um_btc_2025_preflight.json'
    if report_path.exists():
        raise ValueError('Report exists; choose a new archive directory')
    result = []
    for month in months:
        name = f'BTCUSDT-1m-2025-{month:02}.zip'
        if fetch:
            _fetch(BASE, name + '.CHECKSUM', folder)
            _fetch(BASE, name, folder)
        result.append(inspect_month(folder, month))
    report = {'schema': SCHEMA, 'venue': 'BINANCE_UM', 'symbol': 'BTCUSDT', 'interval': '1m',
              'year': 2025, 'complete_year': months == tuple(range(1, 13)),
              'rows': sum(item['rows'] for item in result), 'months': result,
              'interpretation': 'verified public OHLC archives; no tick fills, costs, backtest or net performance'}
    with report_path.open('x', encoding='utf-8') as output:
        json.dump(report, output, indent=2)
        output.write('\n')
    return report


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--archive-dir', type=Path, required=True)
    p.add_argument('--fetch', action='store_true', help='Download only missing public archives/checksums')
    args = p.parse_args()
    report = run(args.archive_dir, fetch=args.fetch)
    print(json.dumps({'status': 'PASS', 'months': len(report['months']), 'rows': report['rows'],
                      'manifest': str(args.archive_dir / 'binance_um_btc_2025_preflight.json')}))


if __name__ == '__main__':
    main()
