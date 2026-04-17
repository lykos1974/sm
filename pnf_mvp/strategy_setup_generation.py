from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

from pnf_engine import PnFProfile, PnFEngine
from storage import Storage
from structure_engine import build_structure_state
from strategy_engine import evaluate_pullback_retest_long, evaluate_pullback_retest_short


DEFAULT_OUTPUT_CSV = "exports/generated_setups.csv"
FIELD_ORDER = [
    "symbol",
    "reference_ts",
    "reference_utc",
    "side",
    "status",
    "strategy",
    "reason",
    "reject_reason",
    "quality_score",
    "quality_grade",
    "trend_state",
    "trend_regime",
    "immediate_slope",
    "breakout_context",
    "market_state",
    "latest_signal_name",
    "is_extended_move",
    "active_leg_boxes",
    "zone_low",
    "zone_high",
    "ideal_entry",
    "invalidation",
    "risk",
    "tp1",
    "tp2",
    "rr1",
    "rr2",
    "raw_setup_json",
    "raw_structure_json",
]


def load_settings(settings_path: str) -> dict:
    with open(settings_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_profiles(settings: dict) -> Dict[str, PnFProfile]:
    profiles: Dict[str, PnFProfile] = {}
    for symbol in settings["symbols"]:
        p = settings["profiles"][symbol]
        profiles[symbol] = PnFProfile(
            name=symbol,
            box_size=float(p["box_size"]),
            reversal_boxes=int(p["reversal_boxes"]),
        )
    return profiles


def split_symbols(settings: dict, symbols_arg: str | None) -> List[str]:
    if not symbols_arg:
        return list(settings["symbols"])
    wanted = [s.strip() for s in symbols_arg.split(",") if s.strip()]
    return [s for s in settings["symbols"] if s in wanted]


def load_all_closed_candles(storage: Storage, symbol: str) -> List[dict]:
    candles = storage.load_recent_candles(symbol, None)
    return candles[:-1] if len(candles) > 1 else []


def evaluate_setups(symbol: str, profile: PnFProfile, engine: PnFEngine) -> Tuple[dict, List[dict]]:
    structure = build_structure_state(
        symbol=symbol,
        profile=profile,
        columns=engine.columns,
        latest_signal_name=engine.latest_signal_name(),
        market_state=engine.market_state(),
        last_price=getattr(engine, "last_price", None),
    )

    setup_long = evaluate_pullback_retest_long(
        symbol=symbol,
        profile=profile,
        columns=engine.columns,
        structure_state=structure,
    )

    setup_short = evaluate_pullback_retest_short(
        symbol=symbol,
        profile=profile,
        columns=engine.columns,
        structure_state=structure,
    )

    setups = [s for s in (setup_long, setup_short) if s]
    return structure, setups


def parse_date_to_utc_ms(value: str, *, day_end: bool = False) -> int:
    text = value.strip()
    if "T" in text:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return int(dt.timestamp() * 1000)

    dt = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if day_end:
        dt = dt + timedelta(days=1) - timedelta(milliseconds=1)
    return int(dt.timestamp() * 1000)


def ms_to_utc_text(ms_value: int | None) -> str:
    if ms_value is None:
        return "-"
    return datetime.fromtimestamp(ms_value / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _safe_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return ""


def build_output_row(*, symbol: str, reference_ts: int, setup: Dict[str, Any], structure: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "reference_ts": int(reference_ts),
        "reference_utc": ms_to_utc_text(int(reference_ts)),
        "side": setup.get("side"),
        "status": setup.get("status"),
        "strategy": setup.get("strategy"),
        "reason": setup.get("reason"),
        "reject_reason": setup.get("reject_reason"),
        "quality_score": setup.get("quality_score"),
        "quality_grade": setup.get("quality_grade"),
        "trend_state": structure.get("trend_state"),
        "trend_regime": structure.get("trend_regime"),
        "immediate_slope": structure.get("immediate_slope"),
        "breakout_context": structure.get("breakout_context"),
        "market_state": structure.get("market_state"),
        "latest_signal_name": structure.get("latest_signal_name"),
        "is_extended_move": 1 if bool(structure.get("is_extended_move")) else 0,
        "active_leg_boxes": structure.get("active_leg_boxes"),
        "zone_low": setup.get("zone_low"),
        "zone_high": setup.get("zone_high"),
        "ideal_entry": setup.get("ideal_entry"),
        "invalidation": setup.get("invalidation"),
        "risk": setup.get("risk"),
        "tp1": setup.get("tp1"),
        "tp2": setup.get("tp2"),
        "rr1": setup.get("rr1"),
        "rr2": setup.get("rr2"),
        "raw_setup_json": _safe_json(setup),
        "raw_structure_json": _safe_json(structure),
    }


def write_csv(rows: List[Dict[str, Any]], output_csv: str) -> str:
    out_path = Path(output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELD_ORDER)
        writer.writeheader()
        writer.writerows(rows)
    return str(out_path.resolve())


def str_to_bool(value: str) -> bool:
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}")


def status_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {
        "evaluated_total": len(rows),
        "rejected_total": 0,
        "watch_total": 0,
        "candidate_total": 0,
    }
    for row in rows:
        status = str(row.get("status") or "").upper()
        if status == "REJECT":
            counts["rejected_total"] += 1
        elif status == "WATCH":
            counts["watch_total"] += 1
        elif status == "CANDIDATE":
            counts["candidate_total"] += 1
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 1 research setup generation (no trade validation)")
    parser.add_argument("--settings", default="settings.research_clean.json")
    parser.add_argument("--symbols", default=None, help="Comma-separated symbols exactly as they appear in settings")
    parser.add_argument("--warmup-start", required=True, help="UTC date YYYY-MM-DD or ISO timestamp")
    parser.add_argument("--analysis-start", required=True, help="UTC date YYYY-MM-DD or ISO timestamp")
    parser.add_argument("--analysis-end", required=True, help="UTC date YYYY-MM-DD or ISO timestamp")
    parser.add_argument("--output-csv", default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--include-rejects", type=str_to_bool, default=True)
    parser.add_argument("--include-watch", type=str_to_bool, default=True)
    parser.add_argument("--include-candidates", type=str_to_bool, default=True)
    args = parser.parse_args()

    warmup_start_ms = parse_date_to_utc_ms(args.warmup_start, day_end=False)
    analysis_start_ms = parse_date_to_utc_ms(args.analysis_start, day_end=False)
    analysis_end_ms = parse_date_to_utc_ms(args.analysis_end, day_end=True)

    if warmup_start_ms > analysis_start_ms:
        raise SystemExit("--warmup-start must be <= --analysis-start")
    if analysis_start_ms > analysis_end_ms:
        raise SystemExit("--analysis-start must be <= --analysis-end")

    settings = load_settings(args.settings)
    storage = Storage(settings["database_path"])
    profiles = build_profiles(settings)
    symbols = split_symbols(settings, args.symbols)

    rows: List[Dict[str, Any]] = []
    first_analysis_ts: int | None = None
    last_analysis_ts: int | None = None

    print("PHASE 1: setup generation only")
    print(f"DB={settings['database_path']}")
    print(f"warmup_start={ms_to_utc_text(warmup_start_ms)}")
    print(f"analysis_start={ms_to_utc_text(analysis_start_ms)}")
    print(f"analysis_end={ms_to_utc_text(analysis_end_ms)}")

    for symbol in symbols:
        profile = profiles[symbol]
        engine = PnFEngine(profile)
        candles = load_all_closed_candles(storage, symbol)

        symbol_evaluated = 0
        symbol_written = 0

        for candle in candles:
            close_ts = int(candle["close_time"])
            if close_ts < warmup_start_ms:
                continue
            if close_ts > analysis_end_ms:
                break

            close_price = float(candle["close"])
            engine.update_from_price(close_ts, close_price)

            if close_ts < analysis_start_ms:
                continue

            structure, setups = evaluate_setups(symbol, profile, engine)
            symbol_evaluated += len(setups)

            if first_analysis_ts is None or close_ts < first_analysis_ts:
                first_analysis_ts = close_ts
            if last_analysis_ts is None or close_ts > last_analysis_ts:
                last_analysis_ts = close_ts

            for setup in setups:
                status = str(setup.get("status") or "").upper()
                if status == "REJECT" and not args.include_rejects:
                    continue
                if status == "WATCH" and not args.include_watch:
                    continue
                if status == "CANDIDATE" and not args.include_candidates:
                    continue

                rows.append(
                    build_output_row(
                        symbol=symbol,
                        reference_ts=close_ts,
                        setup=setup,
                        structure=structure,
                    )
                )
                symbol_written += 1

        print(f"{symbol} | evaluated_setups={symbol_evaluated} | written_rows={symbol_written}")

    output_csv = write_csv(rows, args.output_csv)
    counts = status_counts(rows)

    print(f"output_csv={output_csv}")
    print(f"first_analysis_ts={ms_to_utc_text(first_analysis_ts)}")
    print(f"last_analysis_ts={ms_to_utc_text(last_analysis_ts)}")
    print(
        " | ".join(
            [
                f"evaluated_total={counts['evaluated_total']}",
                f"rejected_total={counts['rejected_total']}",
                f"watch_total={counts['watch_total']}",
                f"candidate_total={counts['candidate_total']}",
            ]
        )
    )
    print("DONE")


if __name__ == "__main__":
    main()
