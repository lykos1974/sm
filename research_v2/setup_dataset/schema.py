from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

SCHEMA_VERSION = "setup_dataset.v2"

# Stable export order for frozen setup datasets.
EXPORT_COLUMNS: tuple[str, ...] = (
    "setup_id",
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
    "pullback_quality",
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
    "entry_distance_boxes",
    "entry_distance_bucket",
    "breakout_level",
    "continuation_quality_score",
    "extension_penalty",
    "continuation_execution_class",
    "pattern_family",
    "distance_policy_action",
    "is_baseline_profile_match",
    "raw_setup_json",
    "raw_structure_json",
)


@dataclass(frozen=True)
class SetupDatasetRecord:
    """Frozen setup row shape for research-mode datasets."""

    setup_id: str
    symbol: str
    reference_ts: int
    reference_utc: datetime
    side: str
    status: str
    strategy: str
    reason: str | None
    reject_reason: str | None
    quality_score: float | None
    quality_grade: str | None
    trend_state: str | None
    trend_regime: str | None
    immediate_slope: str | None
    breakout_context: str | None
    pullback_quality: str | None
    market_state: str | None
    latest_signal_name: str | None
    is_extended_move: int | None
    active_leg_boxes: int | None
    zone_low: float | None
    zone_high: float | None
    ideal_entry: float | None
    invalidation: float | None
    risk: float | None
    tp1: float | None
    tp2: float | None
    rr1: float | None
    rr2: float | None
    entry_distance_boxes: float | None
    entry_distance_bucket: str | None
    breakout_level: float | None
    continuation_quality_score: float | None
    extension_penalty: float | None
    continuation_execution_class: str | None
    pattern_family: str | None
    distance_policy_action: str | None
    is_baseline_profile_match: int | None
    raw_setup_json: str | None
    raw_structure_json: str | None
