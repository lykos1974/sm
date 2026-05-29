from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any

NA_VALUES = {"", "na", "none", "null", "nan"}
CONTINUATION_OUTCOMES = {"BULLISH_CONTINUATION", "BEARISH_CONTINUATION"}
FAILURE_OUTCOME = "FAILED_REVERSAL"

REQUIRED_LABELED_COLUMNS = {
    "timestamp",
    "symbol",
    "outcome_class",
    "max_favorable_boxes",
    "max_adverse_boxes",
    "opposing_pole_distance_columns",
    "enhanced_by_opposing_pole",
    "pole_boxes",
    "pole_boxes_bucket",
    "retrace_boxes",
    "retrace_ratio",
    "retrace_ratio_bucket",
}


@dataclass(frozen=True)
class PoleRow:
    ts: int
    symbol: str
    outcome: str
    distance: str
    enhanced: str
    pole_boxes: float
    pole_boxes_bucket: str
    retrace_boxes: float
    retrace_ratio: float
    retrace_ratio_bucket: str
    expectancy: float


def _clean(value: Any, na_token: str = "NA") -> str:
    text = str(value or "").strip()
    if text.lower() in NA_VALUES:
        return na_token
    return text


def _to_float(value: Any, default: float = 0.0) -> float:
    text = _clean(value, na_token="")
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def _to_int(value: Any) -> int:
    return int(_to_float(value, 0.0))


def _safe_div(a: float, b: float) -> float:
    return (a / b) if b else 0.0


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="") as f:
        return list(csv.DictReader(f))


def _detected_columns(rows: list[dict[str, str]]) -> list[str]:
    cols: set[str] = set()
    for row in rows:
        cols.update(row.keys())
    return sorted(c for c in cols if c)


def _missing_required_columns(columns: list[str]) -> list[str]:
    detected = set(columns)
    return sorted(REQUIRED_LABELED_COLUMNS - detected)


def _row_expectancy(outcome: str, max_fav: float, max_adv: float) -> float:
    outcome_score = 1.0 if outcome in CONTINUATION_OUTCOMES else (-1.0 if outcome == FAILURE_OUTCOME else 0.0)
    asym = _safe_div(max_fav - max_adv, max_fav + max_adv) if (max_fav + max_adv) else 0.0
    return (0.7 * outcome_score) + (0.3 * asym)


def _build_rows_from_labeled(labeled: list[dict[str, str]]) -> list[PoleRow]:
    out: list[PoleRow] = []

    for r in labeled:
        ts = _to_int(r.get("timestamp"))
        outcome = _clean(r.get("outcome_class"), na_token="")
        max_fav = _to_float(r.get("max_favorable_boxes"))
        max_adv = _to_float(r.get("max_adverse_boxes"))

        out.append(
            PoleRow(
                ts=ts,
                symbol=_clean(r.get("symbol"), na_token=""),
                outcome=outcome,
                distance=_clean(r.get("opposing_pole_distance_columns")),
                enhanced=_clean(r.get("enhanced_by_opposing_pole")),
                pole_boxes=_to_float(r.get("pole_boxes")),
                pole_boxes_bucket=_clean(r.get("pole_boxes_bucket")),
                retrace_boxes=_to_float(r.get("retrace_boxes")),
                retrace_ratio=_to_float(r.get("retrace_ratio")),
                retrace_ratio_bucket=_clean(r.get("retrace_ratio_bucket")),
                expectancy=_row_expectancy(outcome, max_fav, max_adv),
            )
        )

    return out


def _metrics(rows: list[PoleRow]) -> dict[str, float | int]:
    n = len(rows)
    cont = sum(1 for r in rows if r.outcome in CONTINUATION_OUTCOMES)
    fail = sum(1 for r in rows if r.outcome == FAILURE_OUTCOME)
    cont_pct = _safe_div(cont, n)
    fail_pct = _safe_div(fail, n)
    expect = mean([r.expectancy for r in rows]) if rows else 0.0
    asym = cont_pct - fail_pct
    return {
        "sample_size": n,
        "continuation_pct": round(cont_pct, 6),
        "failure_pct": round(fail_pct, 6),
        "expectancy_score": round(expect, 6),
        "asymmetry_score": round(asym, 6),
    }


def _write_csv(path: Path, rows: list[dict[str, str | int | float]], fields: list[str]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _verdict(
    missing_required_columns: list[str],
    rows_total: int,
    windows_total: int,
    persistence: float,
    drift_score: float,
) -> str:
    if missing_required_columns or rows_total == 0:
        return "INVALID_RUN"
    if windows_total == 0:
        return "INSUFFICIENT_INPUT"
    if persistence >= 0.65 and drift_score <= 0.2:
        return "HIGH"
    if persistence >= 0.45 and drift_score <= 0.35:
        return "MODERATE"
    return "LOW"


def main() -> None:
    ap = argparse.ArgumentParser(description="Chronological forward pole motif validation (research-only).")
    ap.add_argument("--input-labeled-outcomes-csv", required=True)
    ap.add_argument("--input-btc-columns-csv", default="")
    ap.add_argument("--input-canonical-motifs-csv", default="")
    ap.add_argument("--output-root", required=True)
    ap.add_argument("--train-ratio", type=float, default=0.5)
    ap.add_argument("--forward-ratio", type=float, default=0.2)
    ap.add_argument("--step-ratio", type=float, default=0.1)
    ap.add_argument("--min-window-sample", type=int, default=20)
    args = ap.parse_args()

    labeled_path = Path(args.input_labeled_outcomes_csv)
    btc_path = Path(args.input_btc_columns_csv) if args.input_btc_columns_csv else None

    labeled = _load_csv(labeled_path)
    btc = _load_csv(btc_path) if btc_path else []
    if args.input_canonical_motifs_csv:
        _ = _load_csv(Path(args.input_canonical_motifs_csv))

    labeled_columns = _detected_columns(labeled)
    btc_columns = _detected_columns(btc)
    missing_required_columns = _missing_required_columns(labeled_columns)

    built_rows = [] if missing_required_columns else _build_rows_from_labeled(labeled)
    rows_after_filtering = [r for r in built_rows if r.ts > 0]
    rows = sorted(rows_after_filtering, key=lambda x: x.ts)
    n = len(rows)

    train = max(int(n * args.train_ratio), args.min_window_sample)
    fwd = max(int(n * args.forward_ratio), args.min_window_sample)
    step = max(int(n * args.step_ratio), 1)
    rows_eligible_for_windows = n if n >= train + fwd else 0

    windows: list[dict[str, str | int | float]] = []
    skipped = 0
    i = 0
    while i + train + fwd <= n:
        tr = rows[i : i + train]
        fw = rows[i + train : i + train + fwd]
        if len(tr) < args.min_window_sample or len(fw) < args.min_window_sample:
            skipped += 1
            i += step
            continue
        trm = _metrics(tr)
        fwm = _metrics(fw)
        windows.append(
            {
                "window_id": len(windows) + 1,
                "train_start_ts": tr[0].ts,
                "train_end_ts": tr[-1].ts,
                "forward_start_ts": fw[0].ts,
                "forward_end_ts": fw[-1].ts,
                "train_rows": len(tr),
                "forward_rows": len(fw),
                "train_expectancy": trm["expectancy_score"],
                "forward_expectancy": fwm["expectancy_score"],
                "forward_continuation_pct": fwm["continuation_pct"],
                "forward_failure_pct": fwm["failure_pct"],
                "forward_asymmetry_score": fwm["asymmetry_score"],
                "expectancy_delta": round(float(fwm["expectancy_score"]) - float(trm["expectancy_score"]), 6),
            }
        )
        i += step

    thirds = max(n // 3, 1)
    segments = {"early": rows[:thirds], "middle": rows[thirds : (2 * thirds)], "late": rows[2 * thirds :]}

    drift_rows: list[dict[str, str | int | float]] = []
    dist_cmp: list[dict[str, str | int | float]] = []
    for seg_name, seg_rows in segments.items():
        sm = _metrics(seg_rows)
        drift_rows.append({"segment": seg_name, **sm})
        for d in ("1", "3", "NA"):
            subset = [r for r in seg_rows if r.distance == d]
            dist_cmp.append({"segment": seg_name, "distance_bucket": d, **_metrics(subset)})

    distance3 = [r for r in rows if r.distance == "3"]
    d3_enhanced = [r for r in distance3 if r.enhanced == "True"]
    d3_plain = [r for r in distance3 if r.enhanced == "False"]
    d3_small = [r for r in distance3 if r.pole_boxes <= 12]
    d3_big = [r for r in distance3 if r.pole_boxes > 12]
    d3_low_retrace = [r for r in distance3 if r.retrace_ratio <= 1.0]
    d3_high_retrace = [r for r in distance3 if r.retrace_ratio > 1.0]

    expect_curve = [float(w["forward_expectancy"]) for w in windows]
    persistence = _safe_div(sum(1 for x in expect_curve if x > 0), len(expect_curve)) if expect_curve else 0.0
    drift_score = (max(expect_curve) - min(expect_curve)) if expect_curve else 0.0
    verdict = _verdict(missing_required_columns, n, len(windows), persistence, drift_score)

    out = Path(args.output_root)
    out.mkdir(parents=True, exist_ok=True)

    _write_csv(
        out / "pole_forward_validation_windows.csv",
        windows,
        [
            "window_id",
            "train_start_ts",
            "train_end_ts",
            "forward_start_ts",
            "forward_end_ts",
            "train_rows",
            "forward_rows",
            "train_expectancy",
            "forward_expectancy",
            "forward_continuation_pct",
            "forward_failure_pct",
            "forward_asymmetry_score",
            "expectancy_delta",
        ],
    )

    _write_csv(
        out / "pole_forward_validation_drift.csv",
        drift_rows,
        ["segment", "sample_size", "continuation_pct", "failure_pct", "expectancy_score", "asymmetry_score"],
    )

    _write_csv(
        out / "pole_forward_validation_distance_comparison.csv",
        dist_cmp,
        [
            "segment",
            "distance_bucket",
            "sample_size",
            "continuation_pct",
            "failure_pct",
            "expectancy_score",
            "asymmetry_score",
        ],
    )

    with (out / "pole_forward_validation_summary.md").open("w") as f:
        f.write("# Pole Forward Structural Validation (Research-Only)\n\n")
        f.write("Strict chronological replay only. No random split, no regime labels, no execution simulation.\n")
        f.write("Forward validation rows are built only from the labeled outcomes CSV.\n")
        f.write("BTC columns CSV is accepted only for metadata diagnostics and is never used for field enrichment.\n\n")
        f.write("## Diagnostics\n")
        f.write(f"- labeled rows loaded: {len(labeled)}\n")
        f.write(f"- detected labeled columns: {', '.join(labeled_columns) if labeled_columns else 'NONE'}\n")
        f.write(
            "- missing required labeled columns: "
            f"{', '.join(missing_required_columns) if missing_required_columns else 'NONE'}\n"
        )
        f.write(f"- btc columns csv provided: {bool(btc_path)}\n")
        f.write(f"- btc rows loaded: {len(btc)}\n")
        f.write(f"- detected btc columns: {', '.join(btc_columns) if btc_columns else 'NONE'}\n")
        f.write("- btc enrichment applied: False\n")
        f.write(f"- rows after filtering: {len(rows_after_filtering)}\n")
        f.write(f"- rows after sorting: {len(rows)}\n")
        f.write(f"- rows eligible for windows: {rows_eligible_for_windows}\n")
        f.write(f"- chronological windows evaluated: {len(windows)}\n")
        f.write(f"- rows total: {n}\n")
        f.write(f"- rows per window train/forward: {train}/{fwd}\n")
        f.write(f"- windows skipped for insufficient sample: {skipped}\n")
        f.write(f"- motif persistence score (forward expectancy > 0 share): {persistence:.4f}\n")
        f.write(f"- drift score (max-min forward expectancy): {drift_score:.6f}\n\n")
        f.write("## distance=3 interaction checks\n")
        f.write(f"- enhanced=False: {_metrics(d3_plain)}\n")
        f.write(f"- enhanced=True: {_metrics(d3_enhanced)}\n")
        f.write(f"- pole size <=12: {_metrics(d3_small)}\n")
        f.write(f"- pole size >12: {_metrics(d3_big)}\n")
        f.write(f"- retrace <=1.0: {_metrics(d3_low_retrace)}\n")
        f.write(f"- retrace >1.0: {_metrics(d3_high_retrace)}\n\n")
        f.write("## Conclusions\n")
        f.write(f"- spacing law survival confidence: {verdict}\n")
        if verdict == "INVALID_RUN":
            if missing_required_columns:
                f.write("- run invalid: labeled outcomes CSV is missing required validation fields.\n")
            else:
                f.write("- run invalid: labeled outcomes produced zero usable chronological rows after filtering.\n")
        elif verdict == "INSUFFICIENT_INPUT":
            f.write("- insufficient input: usable rows exist, but no train/forward chronological window can form.\n")
        else:
            f.write(
                "- distance=3 is stable only if segment-level and forward-window expectancy remain positive and "
                "non-collapsing.\n"
            )
            f.write(
                "- enhancement/retrace are secondary unless they dominate distance=3 slices consistently in late "
                "segment.\n"
            )


if __name__ == "__main__":
    main()
