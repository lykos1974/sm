from __future__ import annotations

SCHEMA_VERSION = "labeled_analysis.v3"

ANALYSIS_MODE = "labeled_outcome_scorecard_v1_continuation_diagnostics"

DEFAULT_GROUP_BY: tuple[str, ...] = (
    "side",
    "breakout_context",
    "pullback_quality",
    "entry_distance_bucket",
    "continuation_execution_class",
    "active_leg_boxes",
    "status",
    "quality_grade",
)

GROUPED_SUMMARY_COLUMNS: tuple[str, ...] = (
    "side",
    "breakout_context",
    "pullback_quality",
    "trend_regime",
    "is_extended_move",
    "entry_distance_bucket",
    "continuation_execution_class",
    "pattern_family",
    "is_baseline_profile_match",
    "active_leg_boxes",
    "status",
    "quality_grade",
    "row_count",
    "candidate_rows_registered",
    "valid_labeled_rows",
    "invalid_rows",
    "activated_count",
    "never_activated_count",
    "tp1_touch_count",
    "tp2_count",
    "resolved_rows",
    "win_rate_non_ambiguous",
    "tp1_to_tp2_conversion_rate",
    "stopped_count",
    "tp1_only_count",
    "tp1_then_be_count",
    "ambiguous_count",
    "expired_count",
    "avg_realized_r_multiple",
    "total_realized_r_multiple",
    "avg_outcome_r_proxy",
    "total_outcome_r_proxy",
)
