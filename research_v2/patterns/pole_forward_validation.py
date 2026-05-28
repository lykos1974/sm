from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from statistics import mean

NA_VALUES = {"", "na", "none", "null", "nan"}
CONTINUATION_OUTCOMES = {"BULLISH_CONTINUATION", "BEARISH_CONTINUATION"}
FAILURE_OUTCOME = "FAILED_REVERSAL"


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


def _clean(value: str | None, na_token: str = "NA") -> str:
    text = str(value or "").strip()
    if text.lower() in NA_VALUES:
        return na_token
    return text


def _to_float(value: str | int | float | None, default: float = 0.0) -> float:
    text = _clean(str(value) if value is not None else "", na_token="")
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def _to_int(value: str | int | float | None) -> int:
    return int(_to_float(value, 0.0))


def _safe_div(a: float, b: float) -> float:
    return (a / b) if b else 0.0


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="") as f:
        return list(csv.DictReader(f))


def _pick_column(headers: set[str], candidates: list[str]) -> str:
    for c in candidates:
        if c in headers:
            return c
    return ""


def _row_expectancy(outcome: str, max_fav: float, max_adv: float) -> float:
    outcome_score = 1.0 if outcome in CONTINUATION_OUTCOMES else (-1.0 if outcome == FAILURE_OUTCOME else 0.0)
    asym = _safe_div((max_fav - max_adv), (max_fav + max_adv)) if (max_fav + max_adv) else 0.0
    return (0.7 * outcome_score) + (0.3 * asym)


def _metrics(rows: list[PoleRow]) -> dict[str, float | int]:
    n = len(rows)
    cont = sum(1 for r in rows if r.outcome in CONTINUATION_OUTCOMES)
    fail = sum(1 for r in rows if r.outcome == FAILURE_OUTCOME)
    cont_pct = _safe_div(cont, n)
    fail_pct = _safe_div(fail, n)
    expect = mean([r.expectancy for r in rows]) if rows else 0.0
    return {
        "sample_size": n,
        "continuation_pct": round(cont_pct, 6),
        "failure_pct": round(fail_pct, 6),
        "expectancy_score": round(expect, 6),
        "asymmetry_score": round(cont_pct - fail_pct, 6),
    }


def _build_rows(labeled: list[dict[str, str]], btc: list[dict[str, str]]) -> tuple[list[PoleRow], dict[str, str | int]]:
    diag: dict[str, str | int] = {"rows_loaded_labeled": len(labeled), "rows_loaded_btc": len(btc)}
    if not labeled:
        diag["required_columns_missing"] = "timestamp|symbol|distance|outcome"
        return [], diag

    labeled_headers = set(labeled[0].keys())
    btc_headers = set(btc[0].keys()) if btc else set()
    all_headers = labeled_headers | btc_headers

    ts_col = _pick_column(labeled_headers, ["timestamp", "entry_timestamp", "ts", "open_time", "time", "datetime"])
    symbol_col = _pick_column(labeled_headers, ["symbol", "ticker", "asset"])
    distance_col = _pick_column(labeled_headers, ["opposing_pole_distance_columns", "opposing_pole_distance", "distance_columns", "distance"])
    outcome_col = _pick_column(labeled_headers, ["outcome_class", "outcome", "label"])

    # Optional backfill-only columns from BTC if absent in labeled.
    btc_ts_col = _pick_column(btc_headers, ["timestamp", "entry_timestamp", "ts", "open_time", "time", "datetime"])
    btc_symbol_col = _pick_column(btc_headers, ["symbol", "ticker", "asset"])
    btc_distance_col = _pick_column(btc_headers, ["opposing_pole_distance_columns", "opposing_pole_distance", "distance_columns", "distance"])

    diag.update(
        {
            "labeled_columns_detected": "|".join(sorted(labeled_headers)),
            "required_columns_present": "|".join([k for k, v in {"timestamp": ts_col, "symbol": symbol_col, "distance": distance_col, "outcome": outcome_col}.items() if v]),
            "required_columns_missing": "|".join([k for k, v in {"timestamp": ts_col, "symbol": symbol_col, "distance": distance_col, "outcome": outcome_col}.items() if not v]) or "<none>",
            "timestamp_column_selected": ts_col or "<none>",
            "symbol_column_selected": symbol_col or "<none>",
            "distance_column_selected": distance_col or "<none>",
            "outcome_column_selected": outcome_col or "<none>",
        }
    )

    if not ts_col or not symbol_col or not outcome_col:
        return [], diag

    # Optional BTC merge map by (symbol,timestamp) only; no pattern_name requirement.
    btc_index: dict[tuple[str, str], dict[str, str]] = {}
    if btc and btc_ts_col and btc_symbol_col:
        for r in btc:
            btc_index[(_clean(r.get(btc_symbol_col), ""), _clean(r.get(btc_ts_col), ""))] = r

    rows_after_initial_scan = len(labeled)
    rows_after_timestamp_filter = 0
    rows_after_symbol_filter = 0
    rows_with_valid_distance = 0
    rows_with_valid_outcome = 0

    out: list[PoleRow] = []
    for r in labeled:
        ts_text = _clean(r.get(ts_col), "")
        sym_text = _clean(r.get(symbol_col), "")
        if not ts_text:
            continue
        rows_after_timestamp_filter += 1
        if not sym_text:
            continue
        rows_after_symbol_filter += 1

        ref = btc_index.get((sym_text, ts_text), {})
        ts = _to_int(ts_text)
        if ts <= 0:
            continue

        distance = _clean(r.get(distance_col)) if distance_col else "NA"
        if distance == "NA" and ref and btc_distance_col:
            distance = _clean(ref.get(btc_distance_col))
        if distance != "NA":
            rows_with_valid_distance += 1

        outcome = _clean(r.get(outcome_col), "")
        if outcome:
            rows_with_valid_outcome += 1
        else:
            continue

        max_fav = _to_float(r.get("max_favorable_boxes"))
        max_adv = _to_float(r.get("max_adverse_boxes"))
        out.append(
            PoleRow(
                ts=ts,
                symbol=sym_text,
                outcome=outcome,
                distance=distance,
                enhanced=_clean(r.get("enhanced_by_opposing_pole") or ref.get("enhanced_by_opposing_pole")),
                pole_boxes=_to_float(r.get("pole_boxes") or ref.get("pole_boxes")),
                pole_boxes_bucket=_clean(r.get("pole_boxes_bucket") or ref.get("pole_boxes_bucket")),
                retrace_boxes=_to_float(r.get("retrace_boxes") or ref.get("retrace_boxes")),
                retrace_ratio=_to_float(r.get("retrace_ratio") or ref.get("retrace_ratio")),
                retrace_ratio_bucket=_clean(r.get("retrace_ratio_bucket") or ref.get("retrace_ratio_bucket")),
                expectancy=_row_expectancy(outcome, max_fav, max_adv),
            )
        )

    sorted_rows = sorted(out, key=lambda x: x.ts)
    diag.update(
        {
            "rows_after_initial_scan": rows_after_initial_scan,
            "rows_after_timestamp_filter": rows_after_timestamp_filter,
            "rows_after_symbol_filter": rows_after_symbol_filter,
            "rows_with_valid_distance": rows_with_valid_distance,
            "rows_with_valid_outcome": rows_with_valid_outcome,
            "rows_after_sorting": len(sorted_rows),
        }
    )
    return sorted_rows, diag


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

    labeled = _load_csv(Path(args.input_labeled_outcomes_csv))
    btc = _load_csv(Path(args.input_btc_columns_csv)) if args.input_btc_columns_csv else []
    if args.input_canonical_motifs_csv:
        _ = _load_csv(Path(args.input_canonical_motifs_csv))

    rows, diag = _build_rows(labeled, btc)
    n = len(rows)
    train = max(int(n * args.train_ratio), args.min_window_sample)
    fwd = max(int(n * args.forward_ratio), args.min_window_sample)
    step = max(int(n * args.step_ratio), 1)

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
        windows.append({"window_id": len(windows) + 1, "train_start_ts": tr[0].ts, "train_end_ts": tr[-1].ts, "forward_start_ts": fw[0].ts, "forward_end_ts": fw[-1].ts, "train_rows": len(tr), "forward_rows": len(fw), "train_expectancy": trm["expectancy_score"], "forward_expectancy": fwm["expectancy_score"], "forward_continuation_pct": fwm["continuation_pct"], "forward_failure_pct": fwm["failure_pct"], "forward_asymmetry_score": fwm["asymmetry_score"], "expectancy_delta": round(float(fwm["expectancy_score"]) - float(trm["expectancy_score"]), 6)})
        i += step

    thirds = max(n // 3, 1)
    segments = {"early": rows[:thirds], "middle": rows[thirds : (2 * thirds)], "late": rows[2 * thirds :]}
    drift_rows = [{"segment": seg, **_metrics(seg_rows)} for seg, seg_rows in segments.items()]
    dist_cmp = []
    for seg, seg_rows in segments.items():
        for d in ("1", "3", "NA"):
            dist_cmp.append({"segment": seg, "distance_bucket": d, **_metrics([r for r in seg_rows if r.distance == d])})

    distance3 = [r for r in rows if r.distance == "3"]
    expect_curve = [float(w["forward_expectancy"]) for w in windows]
    persistence = _safe_div(sum(1 for x in expect_curve if x > 0), len(expect_curve)) if expect_curve else 0.0
    drift_score = (max(expect_curve) - min(expect_curve)) if expect_curve else 0.0

    out = Path(args.output_root)
    out.mkdir(parents=True, exist_ok=True)

    with (out / "pole_forward_validation_windows.csv").open("w", newline="") as f:
        fields = list(windows[0].keys()) if windows else ["window_id", "train_start_ts", "train_end_ts", "forward_start_ts", "forward_end_ts", "train_rows", "forward_rows", "train_expectancy", "forward_expectancy", "forward_continuation_pct", "forward_failure_pct", "forward_asymmetry_score", "expectancy_delta"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(windows)
    with (out / "pole_forward_validation_drift.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["segment", "sample_size", "continuation_pct", "failure_pct", "expectancy_score", "asymmetry_score"])
        w.writeheader()
        w.writerows(drift_rows)
    with (out / "pole_forward_validation_distance_comparison.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["segment", "distance_bucket", "sample_size", "continuation_pct", "failure_pct", "expectancy_score", "asymmetry_score"])
        w.writeheader()
        w.writerows(dist_cmp)

    with (out / "pole_forward_validation_summary.md").open("w") as f:
        f.write("# Pole Forward Structural Validation (Research-Only)\n\n")
        f.write("Strict chronological replay only. No random split, no regime labels, no execution simulation.\n\n")
        f.write("## Input/Build Diagnostics\n")
        for k in [
            "rows_loaded_labeled",
            "labeled_columns_detected",
            "required_columns_present",
            "required_columns_missing",
            "timestamp_column_selected",
            "symbol_column_selected",
            "distance_column_selected",
            "outcome_column_selected",
            "rows_after_initial_scan",
            "rows_after_timestamp_filter",
            "rows_after_symbol_filter",
            "rows_with_valid_distance",
            "rows_with_valid_outcome",
            "rows_after_sorting",
        ]:
            f.write(f"- {k.replace('_', ' ')}: {diag.get(k, '<n/a>')}\n")
        f.write(f"- rows eligible for windows: {n}\n\n")

        f.write("## Diagnostics\n")
        f.write(f"- chronological windows evaluated: {len(windows)}\n")
        f.write(f"- rows total: {n}\n")
        f.write(f"- rows per window train/forward: {train}/{fwd}\n")
        f.write(f"- windows skipped for insufficient sample: {skipped}\n")
        f.write(f"- motif persistence score (forward expectancy > 0 share): {persistence:.4f}\n")
        f.write(f"- drift score (max-min forward expectancy): {drift_score:.6f}\n\n")

        f.write("## distance=3 interaction checks\n")
        d3_plain = [r for r in distance3 if r.enhanced == "False"]
        d3_enhanced = [r for r in distance3 if r.enhanced == "True"]
        d3_small = [r for r in distance3 if r.pole_boxes <= 12]
        d3_big = [r for r in distance3 if r.pole_boxes > 12]
        d3_low_retrace = [r for r in distance3 if r.retrace_ratio <= 1.0]
        d3_high_retrace = [r for r in distance3 if r.retrace_ratio > 1.0]
        f.write(f"- enhanced=False: {_metrics(d3_plain)}\n")
        f.write(f"- enhanced=True: {_metrics(d3_enhanced)}\n")
        f.write(f"- pole size <=12: {_metrics(d3_small)}\n")
        f.write(f"- pole size >12: {_metrics(d3_big)}\n")
        f.write(f"- retrace <=1.0: {_metrics(d3_low_retrace)}\n")
        f.write(f"- retrace >1.0: {_metrics(d3_high_retrace)}\n\n")

        if n == 0:
            verdict = "INVALID_RUN"
        elif len(windows) == 0:
            verdict = "INSUFFICIENT_INPUT"
        elif persistence >= 0.65 and drift_score <= 0.2:
            verdict = "HIGH"
        elif persistence >= 0.45 and drift_score <= 0.35:
            verdict = "MODERATE"
        else:
            verdict = "LOW"

        f.write("## Conclusions\n")
        f.write(f"- spacing law survival confidence: {verdict}\n")


if __name__ == "__main__":
    main()
