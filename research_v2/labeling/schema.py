from __future__ import annotations

SCHEMA_VERSION = "setup_labels.v2"

LABEL_MODE_V1_INDEPENDENT = "research_label_v1_independent"

LABEL_COLUMNS: tuple[str, ...] = (
    "symbol",
    "reference_ts",
    "side",
    "status",
    "strategy",
    "breakout_context",
    "pullback_quality",
    "active_leg_boxes",
    "quality_score",
    "quality_grade",
    "source_dataset_artifact",
    "source_manifest_path",
    "label_status",
    "activation_status",
    "resolution_status",
    "realized_r_multiple",
    "outcome_r_proxy",
    "label_mode",
    "label_notes",
    "activation_ts",
    "resolution_ts",
    "horizon_minutes",
    "source_candles_db_path",
)
