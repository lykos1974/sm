from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any

SPLIT_NAMES: tuple[str, ...] = ("train", "validation", "oos")
POPULATIONS: tuple[str, ...] = ("retained", "excluded", "before")


@dataclass(frozen=True)
class SplitConfig:
    train_fraction: float = 0.60
    validation_fraction: float = 0.20
    oos_fraction: float = 0.20


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _norm(value: Any) -> str:
    return str(value or "").strip().upper()


def _load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _chronological_split(rows: list[dict[str, Any]], config: SplitConfig) -> dict[str, list[dict[str, Any]]]:
    sorted_rows = sorted(rows, key=lambda r: str(r.get("reference_ts", "")))
    n = len(sorted_rows)
    train_end = int(n * config.train_fraction)
    validation_end = train_end + int(n * config.validation_fraction)
    return {
        "train": sorted_rows[:train_end],
        "validation": sorted_rows[train_end:validation_end],
        "oos": sorted_rows[validation_end:],
    }


def _metrics(rows: list[dict[str, Any]]) -> dict[str, float | int]:
    tp2_count = sum(1 for r in rows if _norm(r.get("resolution_status")) == "TP2")
    stopped_count = sum(1 for r in rows if _norm(r.get("resolution_status")) == "STOPPED")
    row_count = len(rows)
    return {
        "rows": row_count,
        "tp2_count": tp2_count,
        "stopped_count": stopped_count,
        "tp2_ratio": (tp2_count / row_count) if row_count else 0.0,
        "stopped_ratio": (stopped_count / row_count) if row_count else 0.0,
        "mean_realized_r_multiple": mean(_safe_float(r.get("realized_r_multiple"), 0.0) for r in rows) if rows else 0.0,
        "total_realized_r_multiple": sum(_safe_float(r.get("realized_r_multiple"), 0.0) for r in rows),
    }


def _by_dimension(rows: list[dict[str, Any]], split: str, population: str, fields: tuple[str, ...]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for field in fields:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            key = str(row.get(field, "") or "").strip().upper()
            grouped.setdefault(key, []).append(row)
        for value in sorted(grouped):
            out.append({"population": population, "split": split, "dimension": field, "dimension_value": value, **_metrics(grouped[value])})

    side_symbol_groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (_norm(row.get("side")), _norm(row.get("symbol")))
        side_symbol_groups.setdefault(key, []).append(row)
    for (side, symbol), members in sorted(side_symbol_groups.items()):
        out.append({
            "population": population,
            "split": split,
            "dimension": "side_symbol",
            "dimension_value": f"{side}|{symbol}",
            **_metrics(members),
        })
    return out


def validate_geometry_stability(*, retained_rows_csv: str, excluded_rows_csv: str, output_root: str, split_mode: str = "time", train_fraction: float = 0.60, validation_fraction: float = 0.20, oos_fraction: float = 0.20) -> dict[str, Any]:
    if split_mode != "time":
        raise ValueError("Only split_mode=time is supported.")
    if abs((train_fraction + validation_fraction + oos_fraction) - 1.0) > 1e-9:
        raise ValueError("train_fraction + validation_fraction + oos_fraction must equal 1.0")

    retained_rows = _load_rows(Path(retained_rows_csv).resolve())
    excluded_rows = _load_rows(Path(excluded_rows_csv).resolve())
    before_rows = sorted([*retained_rows, *excluded_rows], key=lambda r: str(r.get("reference_ts", "")))

    out_dir = Path(output_root).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    config = SplitConfig(train_fraction=train_fraction, validation_fraction=validation_fraction, oos_fraction=oos_fraction)
    split_map = {
        "retained": _chronological_split(retained_rows, config),
        "excluded": _chronological_split(excluded_rows, config),
        "before": _chronological_split(before_rows, config),
    }

    split_metrics_rows: list[dict[str, Any]] = []
    side_symbol_metrics_rows: list[dict[str, Any]] = []
    for population in POPULATIONS:
        for split in SPLIT_NAMES:
            rows = split_map[population][split]
            split_metrics_rows.append({"population": population, "split": split, **_metrics(rows)})
            side_symbol_metrics_rows.extend(_by_dimension(rows, split, population, ("side", "symbol")))

    warnings: list[dict[str, Any]] = []
    retained_train = _metrics(split_map["retained"]["train"])
    retained_val = _metrics(split_map["retained"]["validation"])
    retained_oos = _metrics(split_map["retained"]["oos"])
    excluded_all = _metrics(excluded_rows)

    if retained_train["mean_realized_r_multiple"] > 0 and (
        retained_val["mean_realized_r_multiple"] < 0 or retained_oos["mean_realized_r_multiple"] < 0
    ):
        warnings.append({"warning_code": "TRAIN_ONLY_EDGE_COLLAPSE", "severity": "high", "details": "Retained mean R is positive in train but negative in validation or OOS."})

    if retained_train["tp2_ratio"] > 0 and retained_oos["tp2_ratio"] < (retained_train["tp2_ratio"] * 0.70):
        warnings.append({"warning_code": "OOS_TP2_COLLAPSE", "severity": "high", "details": "Retained TP2 ratio collapsed in OOS vs train."})

    retained_by_symbol: dict[str, int] = {}
    for row in retained_rows:
        sym = _norm(row.get("symbol"))
        retained_by_symbol[sym] = retained_by_symbol.get(sym, 0) + 1
    if retained_rows:
        top_sym = max(retained_by_symbol, key=retained_by_symbol.get)
        top_share = retained_by_symbol[top_sym] / len(retained_rows)
        if top_share > 0.70:
            warnings.append({"warning_code": "SYMBOL_CONCENTRATION", "severity": "medium", "details": f"Retained edge may be symbol artifact: {top_sym} is {top_share:.1%} of retained rows."})

    min_required = 30
    if retained_val["rows"] < min_required or retained_oos["rows"] < min_required:
        warnings.append({"warning_code": "SMALL_OOS_OR_VALIDATION_SAMPLE", "severity": "high", "details": f"Retained validation/oos sample too small (validation={retained_val['rows']}, oos={retained_oos['rows']})."})

    retained_side = {r["dimension_value"]: r for r in side_symbol_metrics_rows if r["population"] == "retained" and r["split"] == "train" and r["dimension"] == "side"}
    long_train = retained_side.get("LONG", {"mean_realized_r_multiple": 0.0, "rows": 0})
    short_train = retained_side.get("SHORT", {"mean_realized_r_multiple": 0.0, "rows": 0})
    if long_train["rows"] > 0 and short_train["rows"] > 0:
        if abs(long_train["mean_realized_r_multiple"] - short_train["mean_realized_r_multiple"]) > 0.30:
            warnings.append({"warning_code": "LONG_SHORT_ASYMMETRY", "severity": "medium", "details": "Large LONG/SHORT mean R asymmetry in retained train split."})

    before_all = _metrics(before_rows)
    if before_all["tp2_count"] > 0:
        excluded_tp2_share = excluded_all["tp2_count"] / before_all["tp2_count"]
        if excluded_tp2_share > 0.35:
            warnings.append({"warning_code": "TP2_DESTRUCTION", "severity": "high", "details": f"Excluded rows contain {excluded_tp2_share:.1%} of all TP2 rows."})

    summary = [
        "# Geometry Stability Summary",
        "",
        "## Research answers",
        f"- Is the failure-filter improvement stable across time? {'MIXED/UNSTABLE' if warnings else 'No major stability warning triggered'}.",
        f"- Does retained population stay positive in validation/OOS? validation_mean_r={retained_val['mean_realized_r_multiple']:.4f}, oos_mean_r={retained_oos['mean_realized_r_multiple']:.4f}.",
        f"- Is improvement concentrated in one symbol? {'YES' if any(w['warning_code']=='SYMBOL_CONCENTRATION' for w in warnings) else 'No dominant >70% symbol in retained set'}.",
        f"- Is improvement mostly LONG, SHORT, or both? train_long_mean_r={long_train['mean_realized_r_multiple']:.4f}, train_short_mean_r={short_train['mean_realized_r_multiple']:.4f}.",
        f"- Are excluded rows truly toxic across splits? excluded_train_mean_r={_metrics(split_map['excluded']['train'])['mean_realized_r_multiple']:.4f}, excluded_validation_mean_r={_metrics(split_map['excluded']['validation'])['mean_realized_r_multiple']:.4f}, excluded_oos_mean_r={_metrics(split_map['excluded']['oos'])['mean_realized_r_multiple']:.4f}.",
        f"- Is the filter too destructive to TP2s? excluded_tp2={excluded_all['tp2_count']} out_of_before_tp2={before_all['tp2_count']}.",
        "",
        "## Warnings",
    ]
    summary.extend([f"- [{w['severity']}] {w['warning_code']}: {w['details']}" for w in warnings] or ["- none"])

    outputs = {
        "summary_md": out_dir / "geometry_stability_summary.md",
        "split_metrics_csv": out_dir / "split_metrics.csv",
        "side_symbol_metrics_csv": out_dir / "side_symbol_metrics.csv",
        "warnings_csv": out_dir / "stability_warnings.csv",
    }

    outputs["summary_md"].write_text("\n".join(summary) + "\n", encoding="utf-8")
    with outputs["split_metrics_csv"].open("w", encoding="utf-8", newline="") as handle:
        fields = ["population", "split", "rows", "tp2_count", "stopped_count", "tp2_ratio", "stopped_ratio", "mean_realized_r_multiple", "total_realized_r_multiple"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(split_metrics_rows)

    with outputs["side_symbol_metrics_csv"].open("w", encoding="utf-8", newline="") as handle:
        fields = ["population", "split", "dimension", "dimension_value", "rows", "tp2_count", "stopped_count", "tp2_ratio", "stopped_ratio", "mean_realized_r_multiple", "total_realized_r_multiple"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(side_symbol_metrics_rows)

    with outputs["warnings_csv"].open("w", encoding="utf-8", newline="") as handle:
        fields = ["warning_code", "severity", "details"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(warnings)

    return {k: str(v) for k, v in outputs.items()}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate geometry filter stability across time/side/symbol.")
    parser.add_argument("--retained-rows-csv", required=True)
    parser.add_argument("--excluded-rows-csv", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--split-mode", default="time", choices=["time"])
    parser.add_argument("--train-fraction", type=float, default=0.60)
    parser.add_argument("--validation-fraction", type=float, default=0.20)
    parser.add_argument("--oos-fraction", type=float, default=0.20)
    return parser


if __name__ == "__main__":
    args = build_arg_parser().parse_args()
    validate_geometry_stability(
        retained_rows_csv=args.retained_rows_csv,
        excluded_rows_csv=args.excluded_rows_csv,
        output_root=args.output_root,
        split_mode=args.split_mode,
        train_fraction=args.train_fraction,
        validation_fraction=args.validation_fraction,
        oos_fraction=args.oos_fraction,
    )
