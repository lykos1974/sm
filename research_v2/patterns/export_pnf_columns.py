from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PNF_ROOT = REPO_ROOT / "pnf_mvp"
if str(PNF_ROOT) not in sys.path:
    sys.path.insert(0, str(PNF_ROOT))

from pnf_engine import PnFEngine, PnFProfile


def _load_candles(candles_db_path: Path, symbol: str, limit: int | None):
    conn = sqlite3.connect(str(candles_db_path))
    try:
        if limit is None:
            rows = conn.execute(
                """
                SELECT close_time, close
                FROM candles
                WHERE symbol = ?
                ORDER BY close_time ASC
                """,
                (symbol,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT close_time, close
                FROM (
                    SELECT close_time, close
                    FROM candles
                    WHERE symbol = ?
                    ORDER BY close_time DESC
                    LIMIT ?
                )
                ORDER BY close_time ASC
                """,
                (symbol, limit),
            ).fetchall()
    finally:
        conn.close()

    return [(int(ts), float(close)) for ts, close in rows]


def _export_columns(output_csv: Path, symbol: str, profile: PnFProfile, engine: PnFEngine):
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="") as f:
        fields = [
            "symbol",
            "profile_name",
            "idx",
            "kind",
            "top",
            "bottom",
            "start_ts",
            "end_ts",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for col in engine.columns:
            writer.writerow(
                {
                    "symbol": symbol,
                    "profile_name": profile.name,
                    "idx": col.idx,
                    "kind": col.kind,
                    "top": col.top,
                    "bottom": col.bottom,
                    "start_ts": col.start_ts,
                    "end_ts": col.end_ts,
                }
            )


def main():
    parser = argparse.ArgumentParser(
        description="Export PnF columns from candles DB for research diagnostics"
    )
    parser.add_argument("--candles-db-path", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--profile-box-size", required=True, type=float)
    parser.add_argument("--profile-reversal-boxes", required=True, type=int)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    candles_db_path = Path(args.candles_db_path)
    output_csv = Path(args.output_csv)
    symbol = args.symbol

    candles = _load_candles(candles_db_path=candles_db_path, symbol=symbol, limit=args.limit)

    profile = PnFProfile(
        name=f"{symbol}_bs{args.profile_box_size:g}_rev{args.profile_reversal_boxes}",
        box_size=args.profile_box_size,
        reversal_boxes=args.profile_reversal_boxes,
    )
    engine = PnFEngine(profile=profile)

    for close_ts, close_price in candles:
        engine.update_from_price(close_ts, close_price)

    _export_columns(output_csv=output_csv, symbol=symbol, profile=profile, engine=engine)

    print(
        f"Exported {len(engine.columns)} columns from {len(candles)} candles "
        f"for {symbol} -> {output_csv}"
    )


if __name__ == "__main__":
    main()
