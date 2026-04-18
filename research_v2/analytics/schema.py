from __future__ import annotations

SCHEMA_VERSION = "labeled_analysis.v2"

ANALYSIS_MODE = "labeled_outcome_scorecard_v1"

DEFAULT_GROUP_BY: tuple[str, ...] = (
    "side",
    "breakout_context",
    "pullback_quality",
    "active_leg_boxes",
    "status",
    "quality_grade",
)

GROUPED_SUMMARY_COLUMNS: tuple[str, ...] = (
    "side",
    "breakout_context",
    "pullback_quality",
    "active_leg_boxes",
    "status",
    "quality_grade",
    "row_count",
    "valid_labeled_rows",
    "invalid_rows",
    "activated_count",
    "never_activated_count",
    "tp1_touch_count",
    "tp2_count",
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
