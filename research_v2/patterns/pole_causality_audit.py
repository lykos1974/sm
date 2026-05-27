from __future__ import annotations

import argparse
import csv
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

CLASS_SIGNAL_TIME_SAFE = "SIGNAL_TIME_SAFE"
CLASS_SIGNAL_TIME_PARTIAL = "SIGNAL_TIME_PARTIAL"
CLASS_FUTURE_DERIVED = "FUTURE_DERIVED"
CLASS_FUTURE_LEAKAGE = "FUTURE_LEAKAGE"
CLASS_POST_EVENT_ONLY = "POST_EVENT_ONLY"
CLASS_DIAGNOSTIC_ONLY = "DIAGNOSTIC_ONLY"

LEAK_NONE = "NONE"
LEAK_LOW = "LOW"
LEAK_MODERATE = "MODERATE"
LEAK_HIGH = "HIGH"
LEAK_FATAL = "FATAL"


@dataclass(frozen=True)
class FeatureAudit:
    feature_name: str
    group_name: str
    category: str
    leakage_severity: str
    tradability_score: int
    fully_knowable_at_signal: bool
    requires_future_columns: bool
    depends_on_outcome_realization: bool
    min_future_columns_required: int
    live_safe_convertible: bool
    use_for_filtering: bool
    use_for_diagnostics: bool
    use_for_post_trade_only: bool
    rationale: str


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="") as f:
        return list(csv.DictReader(f))


def _norm(v: str | None) -> str:
    return "" if v is None else str(v).strip()


def _key(row: dict[str, str]) -> tuple[str, str, str]:
    return (_norm(row.get("symbol")), _norm(row.get("timestamp")), _norm(row.get("pattern_name")))


def _feature_catalog() -> list[FeatureAudit]:
    return [
        FeatureAudit("pole_boxes", "core_geometry", CLASS_SIGNAL_TIME_SAFE, LEAK_NONE, 5, True, False, False, 0, True, True, True, False, "Computed from completed pole geometry at signal."),
        FeatureAudit("pole_boxes_bucket", "core_geometry", CLASS_SIGNAL_TIME_SAFE, LEAK_NONE, 5, True, False, False, 0, True, True, True, False, "Bucketization of pole_boxes is signal-time deterministic."),
        FeatureAudit("retrace_boxes", "core_geometry", CLASS_SIGNAL_TIME_SAFE, LEAK_NONE, 5, True, False, False, 0, True, True, True, False, "Retrace depth is known once setup confirms."),
        FeatureAudit("retrace_ratio", "core_geometry", CLASS_SIGNAL_TIME_SAFE, LEAK_NONE, 5, True, False, False, 0, True, True, True, False, "Uses signal-time pole and retrace geometry only."),
        FeatureAudit("retrace_ratio_bucket", "core_geometry", CLASS_SIGNAL_TIME_SAFE, LEAK_NONE, 5, True, False, False, 0, True, True, True, False, "Bucketization of retrace_ratio remains causal."),
        FeatureAudit("breakout_excess_boxes", "core_geometry", CLASS_SIGNAL_TIME_PARTIAL, LEAK_LOW, 3, False, True, False, 1, True, True, True, False, "Requires immediate post-signal breakout extension."),
        FeatureAudit("breakout_excess_boxes_bucket", "core_geometry", CLASS_SIGNAL_TIME_PARTIAL, LEAK_LOW, 3, False, True, False, 1, True, True, True, False, "Bucket inherits one-column delay."),
        FeatureAudit("enhanced_by_opposing_pole", "core_geometry", CLASS_SIGNAL_TIME_SAFE, LEAK_NONE, 4, True, False, False, 0, True, True, True, False, "Derived from already-formed opposing pole context."),
        FeatureAudit("opposing_pole_distance_columns", "core_geometry", CLASS_SIGNAL_TIME_SAFE, LEAK_NONE, 4, True, False, False, 0, True, True, True, False, "Distance to prior opposing pole exists at signal."),
        FeatureAudit("regime_class", "regime_path", CLASS_FUTURE_LEAKAGE, LEAK_FATAL, 0, False, True, True, 3, False, False, True, True, "Uses forward path progression and encodes outcome structure."),
        FeatureAudit("continuation_after_sideways", "regime_path", CLASS_FUTURE_DERIVED, LEAK_HIGH, 1, False, True, True, 4, True, False, True, True, "Needs early sideways and later continuation confirmation."),
        FeatureAudit("volatility_compression_after_signal", "regime_path", CLASS_FUTURE_DERIVED, LEAK_HIGH, 1, False, True, False, 6, True, False, True, True, "Compares early-vs-later dispersion after signal."),
        FeatureAudit("continuation_persistence_ge_*", "regime_path", CLASS_FUTURE_DERIVED, LEAK_MODERATE, 2, False, True, False, 2, True, False, True, True, "Threshold persistence requires forward columns."),
        FeatureAudit("adverse_persistence_ge_*", "regime_path", CLASS_FUTURE_DERIVED, LEAK_MODERATE, 2, False, True, False, 2, True, False, True, True, "Adverse persistence requires forward path."),
        FeatureAudit("max_favorable_boxes", "regime_path", CLASS_POST_EVENT_ONLY, LEAK_FATAL, 0, False, True, True, 20, False, False, True, True, "MFE is realized over full horizon."),
        FeatureAudit("max_adverse_boxes", "regime_path", CLASS_POST_EVENT_ONLY, LEAK_FATAL, 0, False, True, True, 20, False, False, True, True, "MAE is realized over full horizon."),
        FeatureAudit("mfe_mae_ratio", "regime_path", CLASS_POST_EVENT_ONLY, LEAK_FATAL, 0, False, True, True, 20, False, False, True, True, "MFE/MAE is outcome-realized."),
        FeatureAudit("favorable_run", "regime_path", CLASS_FUTURE_DERIVED, LEAK_HIGH, 1, False, True, False, 2, True, False, True, True, "Consecutive favorable run requires forward bars."),
        FeatureAudit("adverse_run", "regime_path", CLASS_FUTURE_DERIVED, LEAK_HIGH, 1, False, True, False, 2, True, False, True, True, "Consecutive adverse run requires forward bars."),
        FeatureAudit("normalized_trajectory", "regime_path", CLASS_DIAGNOSTIC_ONLY, LEAK_FATAL, 0, False, True, True, 20, False, False, True, True, "Normalized path is fully future-trajectory derived."),
    ]


def _matches(name: str, pattern: str) -> bool:
    return name.startswith(pattern[:-1]) if pattern.endswith("*") else name == pattern


def _write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _to_row(a: FeatureAudit, present: bool, matched_columns: str) -> dict[str, str]:
    return {
        "feature_name": a.feature_name,
        "group_name": a.group_name,
        "classification_category": a.category,
        "leakage_severity": a.leakage_severity,
        "tradability_score": str(a.tradability_score),
        "fully_knowable_at_signal": str(a.fully_knowable_at_signal),
        "requires_future_columns": str(a.requires_future_columns),
        "depends_on_outcome_realization": str(a.depends_on_outcome_realization),
        "min_future_columns_required": str(a.min_future_columns_required),
        "live_safe_convertible": str(a.live_safe_convertible),
        "use_for_filtering": str(a.use_for_filtering),
        "use_for_diagnostics": str(a.use_for_diagnostics),
        "use_for_post_trade_only": str(a.use_for_post_trade_only),
        "present_in_inputs": str(present),
        "matched_columns": matched_columns,
        "rationale": a.rationale,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Research-only pole feature causality/tradability audit")
    ap.add_argument("--input-labeled-csv", required=True)
    ap.add_argument("--input-regimes-csv", required=True)
    ap.add_argument("--input-columns-csv")
    ap.add_argument("--output-root", required=True)
    args = ap.parse_args()

    labeled_rows = _load_csv(Path(args.input_labeled_csv))
    regime_rows = _load_csv(Path(args.input_regimes_csv))
    columns_rows = _load_csv(Path(args.input_columns_csv)) if args.input_columns_csv else []

    all_columns = set()
    for bag in (labeled_rows, regime_rows, columns_rows):
        for row in bag:
            all_columns.update(row.keys())

    labeled_keys = {_key(r) for r in labeled_rows}
    regime_keys = {_key(r) for r in regime_rows}
    matched_keys = labeled_keys & regime_keys

    audits = _feature_catalog()
    audit_rows = []
    for a in audits:
        matched = sorted(c for c in all_columns if _matches(c, a.feature_name))
        audit_rows.append(_to_row(a, bool(matched), ",".join(matched)))

    out = Path(args.output_root)
    out.mkdir(parents=True, exist_ok=True)

    fieldnames = list(audit_rows[0].keys())
    _write_csv(out / "pole_causality_audit.csv", audit_rows, fieldnames)
    _write_csv(out / "pole_feature_classification.csv", audit_rows, fieldnames)

    by_class = Counter(a.category for a in audits)
    by_leak = Counter(a.leakage_severity for a in audits)
    by_score = Counter(a.tradability_score for a in audits)

    def names(pred):
        return [a.feature_name for a in audits if pred(a)]

    with (out / "pole_causality_summary.md").open("w") as f:
        f.write("# Pole Causality / Tradability Audit\n\n")
        f.write("## Diagnostics\n")
        f.write(f"- rows loaded (labeled): {len(labeled_rows)}\n")
        f.write(f"- rows loaded (regimes): {len(regime_rows)}\n")
        f.write(f"- rows loaded (columns, optional): {len(columns_rows)}\n")
        f.write(f"- matched keys: {len(matched_keys)}\n")
        f.write(f"- features audited: {len(audits)}\n")
        f.write("- classification counts:\n")
        for k, v in sorted(by_class.items()):
            f.write(f"  - {k}: {v}\n")
        f.write("- leakage severity counts:\n")
        for k, v in sorted(by_leak.items()):
            f.write(f"  - {k}: {v}\n")
        f.write("- tradability score distribution:\n")
        for k, v in sorted(by_score.items()):
            f.write(f"  - {k}: {v}\n")

        missing = [r["feature_name"] for r in audit_rows if r["present_in_inputs"] == "False"]
        f.write("- missing audited features in provided inputs:\n")
        for m in missing:
            f.write(f"  - {m}\n")

        sections = [
            ("Fully causal features", names(lambda a: a.category == CLASS_SIGNAL_TIME_SAFE)),
            ("Features requiring future information", names(lambda a: a.requires_future_columns)),
            ("Features that accidentally encode outcome", names(lambda a: a.depends_on_outcome_realization)),
            ("Features safe for live filtering", names(lambda a: a.use_for_filtering)),
            ("Features safe only for post-trade analytics", names(lambda a: a.use_for_post_trade_only)),
            ("Most dangerous leakage variables", names(lambda a: a.leakage_severity in {LEAK_HIGH, LEAK_FATAL})),
            (
                "Recommended live-safe geometry subset",
                ["pole_boxes", "pole_boxes_bucket", "retrace_boxes", "retrace_ratio", "retrace_ratio_bucket", "enhanced_by_opposing_pole", "opposing_pole_distance_columns"],
            ),
        ]
        for title, values in sections:
            f.write(f"\n## {title}\n")
            for v in values:
                f.write(f"- {v}\n")


if __name__ == "__main__":
    main()
