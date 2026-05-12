#!/usr/bin/env python3
"""Rank structural PnF patterns by tradeable edge.

Research/export-only utility built on top of ``pattern_outcome_analysis``. It
reuses the existing structural outcome model and does not modify strategy,
detection, execution, trader, or database behavior.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any

from pattern_outcome_analysis import (
    DEFAULT_MAX_BARS,
    DEFAULT_SETTINGS,
    OUTCOME_NO_EVENT,
    OUTCOME_STOP,
    OUTCOME_TP1,
    OUTCOME_TP2,
    SUPPORTED_PATTERNS,
    build_trade_result,
    load_settings,
    rate,
    resolve_repo_path,
    resolve_settings_relative_path,
    safe_median,
)

DEFAULT_INPUT_CSV = "pnf_mvp/exports/pattern_taxonomy_audit_v2.csv"
DEFAULT_OUTPUT_CSV = "pnf_mvp/exports/pattern_performance_ranking_v1.csv"
DEFAULT_OUTPUT_MD = "pnf_mvp/exports/pattern_performance_ranking_v1.md"
BREAKDOWN_FIELDS = ["breakout_context", "trend_regime", "pattern_quality", "pattern_width_columns"]


CSV_FIELDS = [
    "rank",
    "pattern",
    "side",
    "total_trades",
    "avg_R_multiple",
    "median_R_multiple",
    "stop_rate",
    "tp1_rate",
    "tp2_rate",
    "no_event_rate",
    "tp1_tp2_combined_rate",
    "median_bars_to_event",
    "median_bars_to_tp1",
    "median_bars_to_stop",
    "raw_edge_score",
    "stability_score",
    "execution_score",
    "strategy_fit_score",
    "final_rank_score",
    "frequency_by_symbol",
    "breakdown_by_breakout_context",
    "breakdown_by_trend_regime",
    "breakdown_by_pattern_quality",
    "breakdown_by_pattern_width_columns",
]


def _round(value: Any, digits: int = 6) -> Any:
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return ""
        return round(value, digits)
    return value


def _median_float(values: list[float]) -> float | None:
    return float(median(values)) if values else None


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _present(value: Any) -> str:
    text = str(value or "").strip()
    return text if text else "(blank)"


def group_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    counts = Counter(row["outcome"] for row in rows)
    r_values = [float(row["r_multiple"]) for row in rows]
    return {
        "trades": total,
        "avg_R_multiple": (sum(r_values) / total) if total else 0.0,
        "median_R_multiple": _median_float(r_values),
        "stop_rate": rate(counts[OUTCOME_STOP], total),
        "tp1_rate": rate(counts[OUTCOME_TP1], total),
        "tp2_rate": rate(counts[OUTCOME_TP2], total),
        "no_event_rate": rate(counts[OUTCOME_NO_EVENT], total),
        "tp1_tp2_combined_rate": rate(counts[OUTCOME_TP1] + counts[OUTCOME_TP2], total),
        "median_bars_to_event": safe_median(int(row["bars_to_event"]) for row in rows if row.get("bars_to_event") is not None),
        "median_bars_to_tp1": safe_median(int(row["bars_to_event"]) for row in rows if row["outcome"] == OUTCOME_TP1),
        "median_bars_to_stop": safe_median(int(row["bars_to_event"]) for row in rows if row["outcome"] == OUTCOME_STOP),
    }


def compact_breakdown(rows: list[dict[str, Any]], field: str) -> dict[str, dict[str, Any]]:
    if not any(field in row for row in rows):
        return {}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_present(row.get(field))].append(row)
    payload: dict[str, dict[str, Any]] = {}
    for value, group_rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        summary = group_summary(group_rows)
        payload[value] = {
            "trades": summary["trades"],
            "avg_R_multiple": _round(summary["avg_R_multiple"]),
            "stop_rate": _round(summary["stop_rate"]),
            "tp2_rate": _round(summary["tp2_rate"]),
            "median_bars_to_event": _round(summary["median_bars_to_event"]),
        }
    return payload


def symbol_frequency(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(str(row["symbol"]) for row in rows)
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def raw_edge_score(summary: dict[str, Any]) -> float:
    # Avg R is centered around zero in the current 2R/3R/1R proxy.  Convert it
    # to a bounded 0..1 component while retaining rate components directly.
    avg_r_component = _clamp((float(summary["avg_R_multiple"]) + 1.0) / 4.0)
    return _clamp(
        (0.50 * avg_r_component)
        + (0.30 * float(summary["tp2_rate"]))
        + (0.20 * (1.0 - float(summary["stop_rate"])))
    )


def stability_score(total_trades: int) -> float:
    # Smoothly approaches full credit around 100 observations while preserving
    # very small samples as explicitly speculative.
    return _clamp(math.sqrt(total_trades / 100.0)) if total_trades else 0.0


def execution_score(summary: dict[str, Any]) -> float:
    median_event = summary["median_bars_to_event"]
    if median_event is None:
        speed_component = 0.0
    else:
        speed_component = _clamp(1.0 - (float(median_event) / 1000.0))
    no_event_component = 1.0 - float(summary["no_event_rate"])
    return _clamp((0.65 * speed_component) + (0.35 * no_event_component))


def _weighted_context_score(rows: list[dict[str, Any]], field: str, value_scores: dict[str, float], default: float) -> float:
    if not rows:
        return 0.0
    total = len(rows)
    score = 0.0
    for row in rows:
        value = _present(row.get(field)).upper()
        score += value_scores.get(value, default)
    return _clamp(score / total)


def strategy_fit_score(pattern: str, rows: list[dict[str, Any]], summary: dict[str, Any]) -> float:
    breakout_score = _weighted_context_score(
        rows,
        "breakout_context",
        {
            "POST_BREAKOUT_PULLBACK": 1.0,
            "PULLBACK_RETEST": 0.95,
            "RETEST": 0.90,
            "BREAKOUT": 0.78,
            "EARLY_BREAKOUT": 0.70,
            "OVEREXTENDED": 0.20,
            "LATE_OVEREXTENSION": 0.15,
            "RANDOM": 0.10,
            "(BLANK)": 0.40,
        },
        0.55,
    )
    trend_score = _weighted_context_score(
        rows,
        "trend_regime",
        {
            "UPTREND": 0.90,
            "DOWNTREND": 0.90,
            "EARLY_TREND": 0.85,
            "TREND": 0.82,
            "RANGE": 0.45,
            "CHOP": 0.30,
            "RANDOM": 0.15,
            "UNKNOWN": 0.35,
            "(BLANK)": 0.40,
        },
        0.55,
    )
    quality_score = _weighted_context_score(
        rows,
        "pattern_quality",
        {
            "HEALTHY": 1.0,
            "STRONG": 0.95,
            "CLEAN": 0.90,
            "COMPACT": 0.85,
            "BROAD": 0.45,
            "WEAK": 0.25,
            "RANDOM": 0.15,
            "(BLANK)": 0.45,
        },
        0.55,
    )
    fit = (0.45 * breakout_score) + (0.35 * trend_score) + (0.20 * quality_score)

    extended_rows = [row for row in rows if _present(row.get("is_extended_move")).upper() in {"1", "TRUE", "YES"}]
    overextension_rows = [row for row in rows if "OVEREXT" in _present(row.get("breakout_context")).upper()]
    late_dependency = (len(extended_rows) + len(overextension_rows)) / max(1, len(rows))
    exceptional = float(summary["avg_R_multiple"]) >= 1.0 and float(summary["tp2_rate"]) >= 0.50
    if late_dependency > 0.35 and not exceptional:
        fit *= 1.0 - min(0.35, late_dependency * 0.35)

    # Triangles are not hard-coded as winners; the small shape bonus reflects
    # their pullback/retest compatibility only after outcomes are computed.
    if "triangle" in pattern:
        fit += 0.03
    return _clamp(fit)


def final_rank_score(raw: float, stability: float, execution: float, fit: float) -> float:
    return _clamp((0.40 * raw) + (0.25 * stability) + (0.20 * execution) + (0.15 * fit))


def load_all_trade_results(input_csv: Path, settings: dict[str, Any], db_path: Path, max_bars: int) -> tuple[list[dict[str, Any]], dict[str, int]]:
    with input_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or [])
        source_rows = list(reader)

    available_specs = [spec for spec in SUPPORTED_PATTERNS.values() if spec.flag_column in fieldnames]
    if not available_specs:
        raise SystemExit("input CSV does not contain any supported pattern flag columns")

    trade_results: list[dict[str, Any]] = []
    skipped_by_pattern: dict[str, int] = defaultdict(int)
    conn = sqlite3.connect(str(db_path))
    try:
        for spec in available_specs:
            for row in source_rows:
                flag = str(row.get(spec.flag_column) or "").strip().lower()
                if flag not in {"1", "1.0", "true", "yes"}:
                    continue
                trade_result = build_trade_result(row=row, spec=spec, conn=conn, settings=settings, max_bars=max_bars)
                if trade_result is None:
                    skipped_by_pattern[spec.name] += 1
                    continue
                trade_results.append(trade_result)
    finally:
        conn.close()
    return trade_results, dict(skipped_by_pattern)


def rank_patterns(trade_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in trade_results:
        grouped[(str(row["pattern"]), str(row["side"]))].append(row)

    ranked: list[dict[str, Any]] = []
    for (pattern, side), rows in grouped.items():
        summary = group_summary(rows)
        raw = raw_edge_score(summary)
        stability = stability_score(int(summary["trades"]))
        execution = execution_score(summary)
        fit = strategy_fit_score(pattern, rows, summary)
        final = final_rank_score(raw, stability, execution, fit)
        record: dict[str, Any] = {
            "pattern": pattern,
            "side": side,
            "total_trades": summary["trades"],
            "avg_R_multiple": summary["avg_R_multiple"],
            "median_R_multiple": summary["median_R_multiple"],
            "stop_rate": summary["stop_rate"],
            "tp1_rate": summary["tp1_rate"],
            "tp2_rate": summary["tp2_rate"],
            "no_event_rate": summary["no_event_rate"],
            "tp1_tp2_combined_rate": summary["tp1_tp2_combined_rate"],
            "median_bars_to_event": summary["median_bars_to_event"],
            "median_bars_to_tp1": summary["median_bars_to_tp1"],
            "median_bars_to_stop": summary["median_bars_to_stop"],
            "raw_edge_score": raw,
            "stability_score": stability,
            "execution_score": execution,
            "strategy_fit_score": fit,
            "final_rank_score": final,
            "frequency_by_symbol": symbol_frequency(rows),
        }
        for field in BREAKDOWN_FIELDS:
            record[f"breakdown_by_{field}"] = compact_breakdown(rows, field)
        ranked.append(record)

    ranked.sort(key=lambda row: (float(row["final_rank_score"]), float(row["avg_R_multiple"]), int(row["total_trades"])), reverse=True)
    for rank, row in enumerate(ranked, start=1):
        row["rank"] = rank
    return ranked


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            output = {field: _round(row.get(field)) for field in CSV_FIELDS}
            for field in ["frequency_by_symbol"] + [f"breakdown_by_{field}" for field in BREAKDOWN_FIELDS]:
                output[field] = json.dumps(row.get(field, {}), sort_keys=True)
            writer.writerow(output)


def md_table(rows: list[dict[str, Any]], limit: int = 10) -> str:
    headers = ["Rank", "Pattern", "Side", "Trades", "Avg R", "TP2", "Stop", "No event", "Med bars", "Final"]
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows[:limit]:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["rank"]),
                    str(row["pattern"]),
                    str(row["side"]),
                    str(row["total_trades"]),
                    f"{float(row['avg_R_multiple']):.3f}",
                    f"{float(row['tp2_rate']):.1%}",
                    f"{float(row['stop_rate']):.1%}",
                    f"{float(row['no_event_rate']):.1%}",
                    "" if row["median_bars_to_event"] is None else f"{float(row['median_bars_to_event']):.0f}",
                    f"{float(row['final_rank_score']):.3f}",
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def bullet_patterns(rows: list[dict[str, Any]], limit: int = 10) -> str:
    if not rows:
        return "- None."
    return "\n".join(
        f"- #{row['rank']} `{row['pattern']}` {row['side']}: trades={row['total_trades']}, "
        f"avg_R={float(row['avg_R_multiple']):.3f}, TP2={float(row['tp2_rate']):.1%}, "
        f"stop={float(row['stop_rate']):.1%}, final={float(row['final_rank_score']):.3f}"
        for row in rows[:limit]
    )


def best_matching(rows: list[dict[str, Any]], needle: str) -> dict[str, Any] | None:
    matches = [row for row in rows if needle in str(row["pattern"])]
    return max(matches, key=lambda row: float(row["final_rank_score"])) if matches else None


def comparison_note(rows: list[dict[str, Any]], needle_a: str, label_a: str, needle_b: str, label_b: str) -> str:
    best_a = best_matching(rows, needle_a)
    best_b = best_matching(rows, needle_b)
    if best_a is None or best_b is None:
        return f"Insufficient overlap to compare {label_a} with {label_b}."
    leader = label_a if float(best_a["final_rank_score"]) >= float(best_b["final_rank_score"]) else label_b
    return (
        f"Best {label_a}: `{best_a['pattern']}` {best_a['side']} final={float(best_a['final_rank_score']):.3f}, "
        f"avg_R={float(best_a['avg_R_multiple']):.3f}, trades={best_a['total_trades']}. "
        f"Best {label_b}: `{best_b['pattern']}` {best_b['side']} final={float(best_b['final_rank_score']):.3f}, "
        f"avg_R={float(best_b['avg_R_multiple']):.3f}, trades={best_b['total_trades']}. "
        f"Current ranking leader: {leader}."
    )


def triangle_strength_note(rows: list[dict[str, Any]]) -> str:
    best_triangle = best_matching(rows, "triangle")
    if best_triangle is None:
        return "No triangle rows were available in the ranked output."
    best_overall = rows[0] if rows else None
    remains_strong = best_overall is not None and best_triangle["rank"] <= min(5, len(rows))
    verdict = "Triangles remain strong" if remains_strong else "Triangles do not lead the current ranking"
    return (
        f"{verdict}: best triangle is #{best_triangle['rank']} `{best_triangle['pattern']}` {best_triangle['side']} "
        f"with final={float(best_triangle['final_rank_score']):.3f}, avg_R={float(best_triangle['avg_R_multiple']):.3f}, "
        f"TP2={float(best_triangle['tp2_rate']):.1%}, trades={best_triangle['total_trades']}. "
        f"Best overall is #{best_overall['rank']} `{best_overall['pattern']}` {best_overall['side']} "
        f"with final={float(best_overall['final_rank_score']):.3f}."
    )


def write_markdown(path: Path, rows: list[dict[str, Any]], skipped_by_pattern: dict[str, int], input_csv: Path, db_path: Path, max_bars: int) -> None:
    long_rows = [row for row in rows if row["side"] == "LONG"]
    short_rows = [row for row in rows if row["side"] == "SHORT"]
    high_edge_low_sample = [row for row in rows if float(row["avg_R_multiple"]) > 0.0 and int(row["total_trades"]) < 30]
    reject_rows = [
        row
        for row in rows
        if float(row["avg_R_multiple"]) <= 0.0
        or float(row["stop_rate"]) >= 0.60
        or (float(row["no_event_rate"]) >= 0.35 and float(row["tp2_rate"]) < 0.20)
    ]
    live_demo_rows = [
        row
        for row in rows
        if int(row["total_trades"]) >= 30
        and float(row["avg_R_multiple"]) > 0.0
        and float(row["stop_rate"]) < 0.55
        and float(row["no_event_rate"]) < 0.30
    ]

    lines = [
        "# Pattern Performance Ranking v1",
        "",
        "Research/export-only ranking of all detected structural patterns in the taxonomy audit file. Strategy logic, pattern detection, execution, and trader code are not modified.",
        "",
        "## Inputs",
        f"- Pattern taxonomy CSV: `{input_csv}`",
        f"- Candle database: `{db_path}`",
        f"- Max future bars per event: `{max_bars}`",
        "",
        "## Ranking logic",
        "- `raw_edge_score`: favors high average R, high TP2 rate, and low stop rate.",
        "- `stability_score`: smooth sample-size confidence; small samples are penalized.",
        "- `execution_score`: favors faster median bars-to-event and low no-event rate.",
        "- `strategy_fit_score`: favors post-breakout pullback/retest context, non-random trend alignment, and healthy/clean quality; late overextension is penalized unless edge is exceptionally strong.",
        "- `final_rank_score`: weighted score = 40% raw edge, 25% stability, 20% execution, 15% strategy fit.",
        "",
        "## Top 10 long patterns",
        md_table(long_rows, 10),
        "",
        "## Top 10 short patterns",
        md_table(short_rows, 10),
        "",
        "## Best overall patterns",
        md_table(rows, 10),
        "",
        "## Patterns with high edge but low sample size",
        bullet_patterns(sorted(high_edge_low_sample, key=lambda row: float(row["avg_R_multiple"]), reverse=True), 10),
        "",
        "## Patterns to reject / avoid",
        bullet_patterns(sorted(reject_rows, key=lambda row: float(row["final_rank_score"])), 10),
        "",
        "## Patterns suitable for live demo forward testing",
        bullet_patterns(live_demo_rows, 10),
        "",
        "## Triangle strength note",
        triangle_strength_note(rows) if rows else "No ranked rows were produced.",
        "",
        "## Double top/bottom vs triangles",
        comparison_note(rows, "double", "double top/bottom", "triangle", "triangles") if rows else "No ranked rows were produced.",
        "",
        "## Skipped rows",
        "Rows can be skipped when required symbol, timestamp, box-size, entry, or candle data are unavailable.",
    ]
    if skipped_by_pattern:
        lines.extend(f"- `{pattern}`: {count}" for pattern, count in sorted(skipped_by_pattern.items()))
    else:
        lines.append("- None.")
    lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Rank all detected structural PnF patterns by tradeable edge")
    parser.add_argument("--input-csv", default=DEFAULT_INPUT_CSV)
    parser.add_argument("--settings", default=DEFAULT_SETTINGS)
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--output-csv", default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--output-md", default=DEFAULT_OUTPUT_MD)
    parser.add_argument("--max-bars", type=int, default=DEFAULT_MAX_BARS)
    args = parser.parse_args()

    if args.max_bars <= 0:
        raise SystemExit("--max-bars must be positive")

    input_csv = resolve_repo_path(args.input_csv)
    settings_path = resolve_repo_path(args.settings)
    if not input_csv.exists():
        raise SystemExit(f"input CSV not found: {input_csv}")
    if not settings_path.exists():
        raise SystemExit(f"settings file not found: {settings_path}")
    settings = load_settings(settings_path)
    db_path = resolve_repo_path(args.db_path) if args.db_path else resolve_settings_relative_path(settings_path, settings["database_path"])
    if not db_path.exists():
        raise SystemExit(f"database file not found: {db_path}")

    trade_results, skipped_by_pattern = load_all_trade_results(input_csv, settings, db_path, args.max_bars)
    ranked_rows = rank_patterns(trade_results)

    output_csv = resolve_repo_path(args.output_csv)
    output_md = resolve_repo_path(args.output_md)
    write_csv(output_csv, ranked_rows)
    write_markdown(output_md, ranked_rows, skipped_by_pattern, input_csv, db_path, args.max_bars)

    print("=== Pattern Performance Ranking v1 ===")
    print(f"input_csv={input_csv}")
    print(f"db_path={db_path}")
    print(f"trade_results={len(trade_results)}")
    print(f"ranked_pattern_sides={len(ranked_rows)}")
    print(f"output_csv={output_csv}")
    print(f"output_md={output_md}")


if __name__ == "__main__":
    main()
