from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

NA_VALUES = {"", "na", "none", "null", "nan"}
CONTINUATION_OUTCOMES = {"BULLISH_CONTINUATION", "BEARISH_CONTINUATION"}
FAILURE_OUTCOME = "FAILED_REVERSAL"


@dataclass(frozen=True)
class OutcomeRow:
    raw: dict[str, str]
    distance: str
    enhanced: str
    retrace_ratio_bucket: str
    retrace_boxes: str
    pole_boxes_bucket: str
    pole_boxes: str
    outcome_class: str
    max_favorable: float
    max_adverse: float


def _clean_token(value: str | None, na_token: str = "NA") -> str:
    text = str(value or "").strip()
    if text.lower() in NA_VALUES:
        return na_token
    return text


def _to_float(value: str | int | float | None, default: float = 0.0) -> float:
    text = _clean_token(str(value) if value is not None else "", na_token="")
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def _distance_bucket(distance: str) -> str:
    if distance == "NA":
        return "NA"
    try:
        val = int(float(distance))
    except ValueError:
        return "NA"
    if val <= 1:
        return "distance=1"
    if val == 2:
        return "distance=2"
    if val == 3:
        return "distance=3"
    if val == 4:
        return "distance=4"
    return "distance>4"


def _safe_div(num: float, den: float) -> float:
    return (num / den) if den else 0.0


def _metrics(rows: list[OutcomeRow]) -> dict[str, float | int]:
    n = len(rows)
    continuation = sum(1 for r in rows if r.outcome_class in CONTINUATION_OUTCOMES)
    failure = sum(1 for r in rows if r.outcome_class == FAILURE_OUTCOME)
    avg_fav = _safe_div(sum(r.max_favorable for r in rows), n)
    avg_adv = _safe_div(sum(r.max_adverse for r in rows), n)
    continuation_pct = _safe_div(continuation, n)
    failure_pct = _safe_div(failure, n)
    asym = _safe_div((avg_fav - avg_adv), (avg_fav + avg_adv)) if (avg_fav + avg_adv) else 0.0
    expectancy = ((continuation_pct * 0.45) + (asym * 0.25) + (((continuation_pct * 0.60) - (failure_pct * 0.50)) * 0.30)) * min(1.0, n / 100.0)
    return {
        "sample_size": n,
        "continuation_pct": round(continuation_pct, 6),
        "failure_pct": round(failure_pct, 6),
        "avg_max_favorable": round(avg_fav, 6),
        "avg_max_adverse": round(avg_adv, 6),
        "asymmetry_score": round(asym, 6),
        "expectancy_score": round(expectancy, 6),
    }


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="") as f:
        return list(csv.DictReader(f))


def _build_rows(labeled: list[dict[str, str]], fallback_rows: list[dict[str, str]]) -> tuple[list[OutcomeRow], int, int]:
    key_fields = ["symbol", "timestamp", "pattern_name"]
    fallback_map: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in fallback_rows:
        key = tuple(_clean_token(row.get(k), na_token="") for k in key_fields)
        fallback_map[key] = row

    rows: list[OutcomeRow] = []
    missing_distance = 0
    for row in labeled:
        key = tuple(_clean_token(row.get(k), na_token="") for k in key_fields)
        ref = fallback_map.get(key, {})

        distance = _clean_token(row.get("opposing_pole_distance_columns") or ref.get("opposing_pole_distance_columns"))
        if distance == "NA":
            missing_distance += 1
        rows.append(
            OutcomeRow(
                raw=row,
                distance=distance,
                enhanced=_clean_token(row.get("enhanced_by_opposing_pole") or ref.get("enhanced_by_opposing_pole")),
                retrace_ratio_bucket=_clean_token(row.get("retrace_ratio_bucket") or ref.get("retrace_ratio_bucket")),
                retrace_boxes=_clean_token(row.get("retrace_boxes") or ref.get("retrace_boxes")),
                pole_boxes_bucket=_clean_token(row.get("pole_boxes_bucket") or ref.get("pole_boxes_bucket")),
                pole_boxes=_clean_token(row.get("pole_boxes") or ref.get("pole_boxes")),
                outcome_class=_clean_token(row.get("outcome_class"), na_token=""),
                max_favorable=_to_float(row.get("max_favorable_boxes")),
                max_adverse=_to_float(row.get("max_adverse_boxes")),
            )
        )

    dedupe_key_candidates = ["symbol", "timestamp", "pattern_name", "entry_timestamp"]
    dup_counter = Counter(tuple(_clean_token(r.raw.get(k), na_token="") for k in dedupe_key_candidates) for r in rows)
    duplicates = sum(v - 1 for v in dup_counter.values() if v > 1)
    return rows, missing_distance, duplicates


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate opposing pole spacing mechanism (research-only).")
    ap.add_argument("--input-labeled-outcomes-csv", required=True)
    ap.add_argument("--input-live-safe-rankings-csv", required=True)
    ap.add_argument("--input-canonical-motifs-csv", required=True)
    ap.add_argument("--input-btc-columns-csv", default="")
    ap.add_argument("--output-root", required=True)
    args = ap.parse_args()

    labeled_rows = _load_csv(Path(args.input_labeled_outcomes_csv))
    _ = _load_csv(Path(args.input_live_safe_rankings_csv))
    _ = _load_csv(Path(args.input_canonical_motifs_csv))
    btc_rows = _load_csv(Path(args.input_btc_columns_csv)) if args.input_btc_columns_csv else []

    rows, missing_distance, duplicate_rows = _build_rows(labeled_rows, btc_rows)
    total = len(rows)

    by_distance: defaultdict[str, list[OutcomeRow]] = defaultdict(list)
    by_interaction: defaultdict[tuple[str, str, str], list[OutcomeRow]] = defaultdict(list)

    for r in rows:
        by_distance[r.distance].append(r)
        db = _distance_bucket(r.distance)
        by_interaction[("distance+enhanced", f"{db}|{r.enhanced}", "")].append(r)
        by_interaction[("distance+retrace", f"{db}|{r.retrace_ratio_bucket}|{r.retrace_boxes}", "")].append(r)
        by_interaction[("distance+pole_size", f"{db}|{r.pole_boxes_bucket}|{r.pole_boxes}", "")].append(r)

    curve_rows: list[dict[str, str | int | float]] = []
    for distance in sorted(by_distance.keys(), key=lambda x: (x == "NA", _to_float(x, 9e9))):
        met = _metrics(by_distance[distance])
        curve_rows.append({"opposing_pole_distance_columns": distance, **met})

    interaction_rows: list[dict[str, str | int | float]] = []
    for (itype, group_key, _), bucket_rows in sorted(by_interaction.items()):
        met = _metrics(bucket_rows)
        interaction_rows.append({"interaction_type": itype, "group_key": group_key, **met})

    dist3 = len(by_distance.get("3", []))
    dist3_share = _safe_div(dist3, total)

    red_flags: list[dict[str, str]] = [
        {"check_name": "sample_size_artifact", "result": "WARN" if dist3_share > 0.45 else "OK", "details": f"distance=3 share={dist3_share:.4f}"},
        {"check_name": "duplicate_cluster_inflation", "result": "WARN" if duplicate_rows > 0 else "OK", "details": f"duplicate rows by key={duplicate_rows}"},
        {"check_name": "na_bucket_inflation", "result": "WARN" if _safe_div(missing_distance, total) > 0.1 else "OK", "details": f"missing distance rows={missing_distance}"},
        {"check_name": "enhanced_flag_imbalance", "result": "WARN" if abs(len([r for r in rows if r.enhanced == 'True']) - len([r for r in rows if r.enhanced == 'False'])) > total * 0.6 else "OK", "details": "distribution checked globally"},
        {"check_name": "retrace_bucket_imbalance", "result": "WARN" if len({r.retrace_ratio_bucket for r in rows}) <= 2 else "OK", "details": "cardinality check"},
        {"check_name": "pole_size_imbalance", "result": "WARN" if len({r.pole_boxes_bucket for r in rows}) <= 2 else "OK", "details": "cardinality check"},
    ]

    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    with (output_root / "pole_spacing_distance_curve.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["opposing_pole_distance_columns", "sample_size", "continuation_pct", "failure_pct", "avg_max_favorable", "avg_max_adverse", "asymmetry_score", "expectancy_score"])
        w.writeheader()
        w.writerows(curve_rows)

    with (output_root / "pole_spacing_interactions.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["interaction_type", "group_key", "sample_size", "continuation_pct", "failure_pct", "avg_max_favorable", "avg_max_adverse", "asymmetry_score", "expectancy_score"])
        w.writeheader()
        w.writerows(interaction_rows)

    with (output_root / "pole_spacing_red_flags.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["check_name", "result", "details"])
        w.writeheader()
        w.writerows(red_flags)

    with (output_root / "pole_spacing_mechanism_summary.md").open("w") as f:
        f.write("# Opposing Pole Spacing Mechanism Validation (Research-Only)\n\n")
        f.write("## Diagnostics\n")
        f.write(f"- rows loaded: {total}\n")
        f.write(f"- distance buckets found: {len(by_distance)}\n")
        f.write(f"- rows with missing distance: {missing_distance}\n")
        f.write(f"- duplicate rows by key if available: {duplicate_rows}\n")
        f.write(f"- distance=3 sample share: {dist3_share:.4f}\n\n")
        f.write("## Distance stability checks\n")
        for bucket in ["distance=1", "distance=2", "distance=3", "distance=4", "distance>4"]:
            matches = [r for r in rows if _distance_bucket(r.distance) == bucket]
            met = _metrics(matches)
            f.write(f"- {bucket}: n={met['sample_size']}, continuation={met['continuation_pct']}, failure={met['failure_pct']}, expectancy={met['expectancy_score']}\n")

        f.write("\n## Mechanism hypotheses (evidence-weighted)\n")
        f.write("- Continuation memory hypothesis: supported only if distance=3 retains higher continuation_pct and expectancy versus 1/2/4/>4 in distance curve output.\n")
        f.write("- Failed counter-auction hypothesis: supported only if distance=3 also shows lower failure_pct and higher asymmetry_score.\n")
        f.write("- Compression-release hypothesis: supported only if distance=3 edge persists inside retrace and pole-size interaction slices.\n")
        f.write("- Fake reversal / trap hypothesis: supported only if distance=3 remains strong when enhanced_by_opposing_pole=False.\n")


if __name__ == "__main__":
    main()
