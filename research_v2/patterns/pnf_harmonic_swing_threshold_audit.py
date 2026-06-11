"""Research-only audit of harmonic PnF swing extraction thresholds.

This module replays raw *completed* PnF columns through several harmonic swing
threshold sets and compares the resulting pivot/leg counts, leg lengths,
reaction ratios, and knowledge-time lag behavior. It intentionally does not
recognize harmonic patterns, generate setup candidates, calculate expectancy,
place orders, or modify production state.
"""

from __future__ import annotations

import argparse
import csv
import math
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable, Sequence

OUTPUT_SWINGS = "harmonic_swings_by_threshold.csv"
OUTPUT_REACTIONS = "harmonic_reactions_by_threshold.csv"
OUTPUT_SUMMARY = "harmonic_threshold_summary.csv"
OUTPUT_KNOWLEDGE = "harmonic_knowledge_time_summary.csv"
OUTPUT_PIVOT_COUNTS = "harmonic_pivot_counts.csv"
OUTPUT_LEG_STATS = "harmonic_leg_statistics.csv"
OUTPUT_REACTION_DISTRIBUTION = "harmonic_reaction_ratio_distribution.csv"
OUTPUT_BOX_SIZE_MANIFEST = "harmonic_box_size_manifest.csv"
OUTPUT_NAMES = (
    OUTPUT_SWINGS,
    OUTPUT_REACTIONS,
    OUTPUT_SUMMARY,
    OUTPUT_KNOWLEDGE,
    OUTPUT_PIVOT_COUNTS,
    OUTPUT_LEG_STATS,
    OUTPUT_REACTION_DISTRIBUTION,
    OUTPUT_BOX_SIZE_MANIFEST,
)

DEFAULT_THRESHOLD_SETS = (
    "FAST:2:0.146",
    "BASE:3:0.236",
    "SLOW:5:0.382",
)

SWING_FIELDS = [
    "threshold_name",
    "min_pivot_boxes",
    "min_pivot_ratio",
    "symbol",
    "harmonic_swing_id",
    "direction",
    "status",
    "start_price",
    "end_price",
    "swing_boxes",
    "swing_price_distance",
    "start_ts",
    "end_ts",
    "birth_time",
    "knowledge_time",
    "knowledge_lag",
    "knowledge_time_source",
    "start_column_id",
    "end_column_id",
    "confirming_column_id",
    "source_column_ids",
    "reaction_column_ids",
    "failed_extension_column_ids",
]
REACTION_FIELDS = [
    "threshold_name",
    "symbol",
    "active_direction",
    "candidate_direction",
    "reaction_kind",
    "candidate_boxes",
    "active_swing_boxes",
    "reaction_ratio",
    "required_boxes",
    "column_id",
    "completion_time",
    "knowledge_time",
    "knowledge_time_source",
    "active_start_ts",
    "active_end_ts",
]
SUMMARY_FIELDS = [
    "threshold_name",
    "min_pivot_boxes",
    "min_pivot_ratio",
    "symbols",
    "input_columns",
    "confirmed_pivot_count",
    "confirmed_leg_count",
    "avg_leg_boxes",
    "median_leg_boxes",
    "min_leg_boxes",
    "max_leg_boxes",
    "avg_leg_price_distance",
    "internal_reaction_count",
    "avg_internal_reaction_ratio",
    "median_internal_reaction_ratio",
    "confirming_reaction_count",
    "avg_confirming_reaction_ratio",
    "median_confirming_reaction_ratio",
]
KNOWLEDGE_FIELDS = [
    "threshold_name",
    "knowledge_rows",
    "knowledge_after_endpoint_count",
    "knowledge_equal_endpoint_count",
    "avg_knowledge_lag",
    "median_knowledge_lag",
    "max_knowledge_lag",
    "fallback_completion_time_count",
    "explicit_completion_time_count",
]
PIVOT_COUNT_FIELDS = [
    "threshold_name",
    "min_pivot_boxes",
    "min_pivot_ratio",
    "symbol",
    "confirmed_pivot_count",
    "confirmed_leg_count",
    "up_leg_count",
    "down_leg_count",
]
LEG_STATS_FIELDS = [
    "threshold_name",
    "min_pivot_boxes",
    "min_pivot_ratio",
    "symbol",
    "leg_count",
    "avg_leg_boxes",
    "median_leg_boxes",
    "p25_leg_boxes",
    "p75_leg_boxes",
    "min_leg_boxes",
    "max_leg_boxes",
    "avg_leg_price_distance",
    "median_leg_price_distance",
]
REACTION_DISTRIBUTION_FIELDS = [
    "threshold_name",
    "min_pivot_boxes",
    "min_pivot_ratio",
    "symbol",
    "reaction_kind",
    "bucket",
    "count",
    "percentage",
]
BOX_SIZE_MANIFEST_FIELDS = [
    "symbol",
    "resolved_box_size",
    "box_size_source",
    "profile_name",
    "warning_if_inferred",
    "knowledge_time_source",
    "knowledge_time_contract",
]
REACTION_RATIO_BUCKETS = (
    ("0-10%", 0.0, 0.10),
    ("10-20%", 0.10, 0.20),
    ("20-30%", 0.20, 0.30),
    ("30-40%", 0.30, 0.40),
    ("40-50%", 0.40, 0.50),
    ("50-60%", 0.50, 0.60),
    ("60-70%", 0.60, 0.70),
    ("70-80%", 0.70, 0.80),
    ("80-90%", 0.80, 0.90),
    ("90-100%", 0.90, 1.00),
    ("100%+", 1.00, math.inf),
)



def _first_value(row: dict[str, Any], aliases: Sequence[str]) -> Any:
    for alias in aliases:
        value = row.get(alias)
        if value is not None and str(value).strip() != "":
            return value
    return None


def _to_float(value: Any, *, label: str, row_number: int) -> float:
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"row {row_number}: invalid {label}: {value!r}") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"row {row_number}: non-finite {label}: {value!r}")
    return parsed


def _to_str(value: Any) -> str:
    return str(value).strip()


def _normalize_direction(raw: Any, *, row_number: int) -> str:
    text = str(raw or "").strip().upper()
    if text in {"X", "UP", "LONG", "BULL", "BULLISH"}:
        return "X"
    if text in {"O", "DOWN", "SHORT", "BEAR", "BEARISH"}:
        return "O"
    raise ValueError(f"row {row_number}: kind must be X/O or UP/DOWN, got {raw!r}")


def _format_number(value: float | int | str | None) -> str | int:
    if value is None:
        return ""
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return value
    if not math.isfinite(value):
        return ""
    return f"{value:.12g}"


def _time_value(value: str) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        pass
    iso = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(iso).timestamp()
    except ValueError:
        return None


def _time_lag(knowledge_time: str, event_time: str) -> float | None:
    knowledge = _time_value(knowledge_time)
    event = _time_value(event_time)
    if knowledge is None or event is None:
        return None
    return knowledge - event


@dataclass(frozen=True)
class ThresholdSet:
    name: str
    min_pivot_boxes: float
    min_pivot_ratio: float

    @classmethod
    def parse(cls, raw: str) -> "ThresholdSet":
        parts = [part.strip() for part in raw.split(":")]
        if len(parts) != 3 or not all(parts):
            raise ValueError(
                "threshold sets must use NAME:MIN_PIVOT_BOXES:MIN_PIVOT_RATIO"
            )
        return cls(
            name=parts[0],
            min_pivot_boxes=float(parts[1]),
            min_pivot_ratio=float(parts[2]),
        )


@dataclass(frozen=True)
class RawColumn:
    symbol: str
    column_id: str
    ordinal: int
    kind: str
    high: float
    low: float
    start_ts: str
    end_ts: str
    completion_time: str
    completion_time_source: str
    box_size: float
    knowledge_time_contract: str = ""
    profile_name: str = ""
    box_size_source: str = ""
    warning_if_inferred: str = ""

    @property
    def direction(self) -> str:
        return "UP" if self.kind == "X" else "DOWN"

    @property
    def start_price(self) -> float:
        return self.low if self.kind == "X" else self.high

    @property
    def end_price(self) -> float:
        return self.high if self.kind == "X" else self.low


@dataclass
class Candidate:
    direction: str
    start_column_id: str
    source_column_ids: list[str]
    extreme_price: float
    extreme_column_id: str
    boxes: float


@dataclass
class ActiveSwing:
    symbol: str
    direction: str
    start_price: float
    end_price: float
    start_ts: str
    end_ts: str
    start_column_id: str
    end_column_id: str
    source_column_ids: list[str]
    reaction_column_ids: list[str] = field(default_factory=list)
    failed_extension_column_ids: list[str] = field(default_factory=list)

    def boxes(self, box_size: float) -> float:
        return abs(self.end_price - self.start_price) / box_size

    def price_distance(self) -> float:
        return abs(self.end_price - self.start_price)


def _parse_box_size_from_profile(value: Any) -> float | None:
    text = str(value or "")
    match = re.search(r"(?:^|_)bs([0-9]+(?:\.[0-9]+)?)(?:_|$)", text)
    if not match:
        return None
    parsed = float(match.group(1))
    return parsed if parsed > 0 else None


def _infer_symbol_box_sizes(rows: list[dict[str, Any]]) -> dict[str, float]:
    values_by_symbol: dict[str, set[float]] = defaultdict(set)
    for row in rows:
        symbol = str(_first_value(row, ("symbol",)) or "").strip()
        for field in ("top", "bottom", "high", "low", "start_price", "end_price"):
            value = row.get(field)
            if value is None or str(value).strip() == "":
                continue
            try:
                values_by_symbol[symbol].add(float(value))
            except ValueError:
                continue

    out: dict[str, float] = {}
    for symbol, values in values_by_symbol.items():
        ordered = sorted(values)
        diffs = [b - a for a, b in zip(ordered, ordered[1:]) if b - a > 0]
        if diffs:
            out[symbol] = min(diffs)
    return out


def _parse_symbol_box_size_specs(specs: Sequence[str] | None) -> dict[str, float]:
    mapping: dict[str, float] = {}
    for raw in specs or []:
        if "=" not in raw:
            raise ValueError(f"symbol box size must use SYMBOL=BOX_SIZE, got {raw!r}")
        symbol, value = raw.split("=", 1)
        symbol = symbol.strip()
        if not symbol:
            raise ValueError(f"symbol box size has empty symbol: {raw!r}")
        try:
            parsed = float(value.strip())
        except ValueError as exc:
            raise ValueError(f"invalid box size in symbol mapping {raw!r}") from exc
        if not math.isfinite(parsed) or parsed <= 0:
            raise ValueError(f"symbol box size must be positive and finite: {raw!r}")
        mapping[symbol] = parsed
    return mapping


def _symbol_aliases(symbol: str) -> set[str]:
    text = str(symbol or "").strip()
    aliases = {text}
    if ":" in text:
        aliases.add(text.split(":", 1)[1])
    return {alias for alias in aliases if alias}


def _lookup_symbol_box_size(symbol: str, mapping: dict[str, float]) -> float | None:
    aliases = _symbol_aliases(symbol)
    for key, value in mapping.items():
        if key in aliases or _symbol_aliases(key) & aliases:
            return value
    return None


def _column_knowledge_time(row: dict[str, Any], *, row_number: int) -> tuple[str, str, str]:
    """Return the causal timestamp at which a completed PnF column is knowable.

    The input to this audit is a stream of completed PnF columns. An explicit
    ``completion_time`` is therefore accepted only under the documented source
    contract that it is the first timestamp at which the completed PnF column is
    confirmed and available to downstream research. This is not a consumer-side
    fallback: the exported ``knowledge_time`` is populated from a source field
    whose meaning is recorded in the manifest.
    """
    source_contracts = (
        (
            "knowledge_time",
            "explicit_input_knowledge_time",
            "Input row supplied an explicit knowledge_time; this is the causal availability timestamp for the completed PnF column.",
        ),
        (
            "completion_time",
            "explicit_completion_time",
            "Input contract: completion_time is the first timestamp at which the completed PnF column/reaction is confirmed and available to downstream research; exported knowledge_time is copied from that confirmed-column timestamp.",
        ),
        (
            "column_completion_time",
            "explicit_column_completion_time",
            "Input contract: column_completion_time is the first timestamp at which the completed PnF column is confirmed and available to downstream research; exported knowledge_time is copied from that confirmed-column timestamp.",
        ),
        (
            "completed_at",
            "explicit_completed_at",
            "Input contract: completed_at is the first timestamp at which the completed PnF column is confirmed and available to downstream research; exported knowledge_time is copied from that confirmed-column timestamp.",
        ),
    )
    for field_name, source_name, contract in source_contracts:
        value = row.get(field_name)
        if value is not None and str(value).strip() != "":
            return _to_str(value), source_name, contract

    end_ts = row.get("end_ts") or row.get("end_time")
    if end_ts is not None and str(end_ts).strip() != "":
        return (
            _to_str(end_ts),
            "end_ts_fallback",
            "Fallback only: source did not provide a confirmed-column knowledge timestamp, so end_ts was used for legacy harmonic-threshold diagnostics; do not treat as design_v2-validated causal input.",
        )

    raise ValueError(f"row {row_number}: missing completion_time/knowledge_time and end_ts")


def _row_box_size(
    row: dict[str, Any],
    *,
    row_number: int,
    cli_box_size: float | None,
    symbol_box_sizes: dict[str, float],
    inferred_box_sizes: dict[str, float],
    symbol: str,
    allow_infer_box_size: bool,
) -> tuple[float, str, str]:
    explicit = _first_value(row, ("box_size", "pnf_box_size", "profile_box_size"))
    if explicit is not None:
        parsed = _to_float(explicit, label="box_size", row_number=row_number)
        if parsed <= 0:
            raise ValueError(f"row {row_number}: box_size must be positive")
        return parsed, "explicit_csv", ""

    from_symbol = _lookup_symbol_box_size(symbol, symbol_box_sizes)
    if from_symbol is not None:
        return from_symbol, "symbol_box_size", ""

    from_profile = _parse_box_size_from_profile(row.get("profile_name"))
    if from_profile is not None:
        return from_profile, "profile_name", ""

    if cli_box_size is not None:
        return cli_box_size, "cli_box_size", ""

    inferred = inferred_box_sizes.get(symbol)
    if inferred is not None and inferred > 0:
        if not allow_infer_box_size:
            raise ValueError(
                f"row {row_number}: missing explicit box_size for {symbol!r}; "
                f"profile_name={row.get('profile_name')!r} does not contain parseable _bs..._; "
                "provide box_size/profile_box_size, --symbol-box-size, --box-size, "
                "or rerun with --allow-infer-box-size"
            )
        return inferred, "inferred", "WARNING: inferred from minimum observed price spacing"

    raise ValueError(
        f"row {row_number}: missing box_size; provide box_size/profile_name, "
        "--symbol-box-size, --box-size, or --allow-infer-box-size"
    )


def load_columns(
    paths: Sequence[Path],
    *,
    box_size: float | None = None,
    symbol_box_sizes: dict[str, float] | None = None,
    allow_infer_box_size: bool = False,
) -> list[RawColumn]:
    raw_rows: list[dict[str, Any]] = []
    for path in paths:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                raise ValueError(f"{path}: expected a CSV header")
            raw_rows.extend(dict(row) for row in reader)

    inferred_box_sizes = _infer_symbol_box_sizes(raw_rows)
    explicit_symbol_box_sizes = symbol_box_sizes or {}
    columns: list[RawColumn] = []
    for row_number, row in enumerate(raw_rows, start=1):
        symbol = _to_str(_first_value(row, ("symbol",)) or "")
        if not symbol:
            raise ValueError(f"row {row_number}: missing symbol")
        kind = _normalize_direction(
            _first_value(row, ("kind", "direction", "column_kind")), row_number=row_number
        )
        high_value = _first_value(row, ("high", "top", "column_high", "end_price"))
        low_value = _first_value(row, ("low", "bottom", "column_low", "start_price"))
        if high_value is None or low_value is None:
            raise ValueError(f"row {row_number}: missing high/low or top/bottom")
        high = _to_float(high_value, label="high/top", row_number=row_number)
        low = _to_float(low_value, label="low/bottom", row_number=row_number)
        if low > high:
            low, high = high, low
        column_id = _to_str(
            _first_value(row, ("column_id", "idx", "ordinal", "id")) or row_number
        )
        ordinal_value = _first_value(row, ("ordinal", "idx", "column_ordinal"))
        ordinal = int(float(ordinal_value)) if ordinal_value not in (None, "") else row_number
        start_ts = _to_str(_first_value(row, ("start_ts", "start_time")) or "")
        end_ts = _to_str(_first_value(row, ("end_ts", "end_time")) or "")
        if not start_ts or not end_ts:
            raise ValueError(f"row {row_number}: missing start_ts/end_ts")
        completion_time, completion_source, knowledge_time_contract = _column_knowledge_time(
            row, row_number=row_number
        )
        parsed_box_size, box_size_source, warning_if_inferred = _row_box_size(
            row,
            row_number=row_number,
            cli_box_size=box_size,
            symbol_box_sizes=explicit_symbol_box_sizes,
            inferred_box_sizes=inferred_box_sizes,
            symbol=symbol,
            allow_infer_box_size=allow_infer_box_size,
        )
        columns.append(
            RawColumn(
                symbol=symbol,
                column_id=column_id,
                ordinal=ordinal,
                kind=kind,
                high=high,
                low=low,
                start_ts=start_ts,
                end_ts=end_ts,
                completion_time=_to_str(completion_time),
                completion_time_source=completion_source,
                knowledge_time_contract=knowledge_time_contract,
                box_size=parsed_box_size,
                profile_name=_to_str(row.get("profile_name") or ""),
                box_size_source=box_size_source,
                warning_if_inferred=warning_if_inferred,
            )
        )
    return sorted(
        columns,
        key=lambda c: (
            c.symbol,
            _time_value(c.completion_time)
            if _time_value(c.completion_time) is not None
            else math.inf,
            c.completion_time,
            c.ordinal,
            c.column_id,
        ),
    )


def _new_candidate(active: ActiveSwing, column: RawColumn) -> Candidate:
    if active.direction == "UP":
        extreme = column.low
        boxes = abs(active.end_price - extreme) / column.box_size
        direction = "DOWN"
    else:
        extreme = column.high
        boxes = abs(extreme - active.end_price) / column.box_size
        direction = "UP"
    return Candidate(
        direction=direction,
        start_column_id=column.column_id,
        source_column_ids=[column.column_id],
        extreme_price=extreme,
        extreme_column_id=column.column_id,
        boxes=boxes,
    )


def _update_candidate(active: ActiveSwing, candidate: Candidate, column: RawColumn) -> Candidate:
    candidate.source_column_ids.append(column.column_id)
    if active.direction == "UP":
        if column.low < candidate.extreme_price:
            candidate.extreme_price = column.low
            candidate.extreme_column_id = column.column_id
        candidate.boxes = abs(active.end_price - candidate.extreme_price) / column.box_size
    else:
        if column.high > candidate.extreme_price:
            candidate.extreme_price = column.high
            candidate.extreme_column_id = column.column_id
        candidate.boxes = abs(candidate.extreme_price - active.end_price) / column.box_size
    return candidate


def _swing_row(
    *,
    threshold: ThresholdSet,
    active: ActiveSwing,
    status: str,
    box_size: float,
    birth_time: str,
    knowledge_time: str,
    knowledge_time_source: str,
    confirming_column_id: str,
    sequence: int,
) -> dict[str, Any]:
    knowledge_lag = _time_lag(knowledge_time, active.end_ts)
    return {
        "threshold_name": threshold.name,
        "min_pivot_boxes": _format_number(threshold.min_pivot_boxes),
        "min_pivot_ratio": _format_number(threshold.min_pivot_ratio),
        "symbol": active.symbol,
        "harmonic_swing_id": f"{active.symbol}:{threshold.name}:{sequence:06d}",
        "direction": active.direction,
        "status": status,
        "start_price": _format_number(active.start_price),
        "end_price": _format_number(active.end_price),
        "swing_boxes": _format_number(active.boxes(box_size)),
        "swing_price_distance": _format_number(active.price_distance()),
        "start_ts": active.start_ts,
        "end_ts": active.end_ts,
        "birth_time": birth_time,
        "knowledge_time": knowledge_time,
        "knowledge_lag": _format_number(knowledge_lag),
        "knowledge_time_source": knowledge_time_source,
        "start_column_id": active.start_column_id,
        "end_column_id": active.end_column_id,
        "confirming_column_id": confirming_column_id,
        "source_column_ids": "|".join(active.source_column_ids),
        "reaction_column_ids": "|".join(active.reaction_column_ids),
        "failed_extension_column_ids": "|".join(active.failed_extension_column_ids),
    }


def _reaction_row(
    *,
    threshold: ThresholdSet,
    active: ActiveSwing,
    candidate: Candidate,
    column: RawColumn,
    required_boxes: float,
    reaction_kind: str,
) -> dict[str, Any]:
    active_boxes = active.boxes(column.box_size)
    ratio = candidate.boxes / active_boxes if active_boxes > 0 else math.nan
    return {
        "threshold_name": threshold.name,
        "symbol": active.symbol,
        "active_direction": active.direction,
        "candidate_direction": candidate.direction,
        "reaction_kind": reaction_kind,
        "candidate_boxes": _format_number(candidate.boxes),
        "active_swing_boxes": _format_number(active_boxes),
        "reaction_ratio": _format_number(ratio),
        "required_boxes": _format_number(required_boxes),
        "column_id": column.column_id,
        "completion_time": column.completion_time,
        "knowledge_time": column.completion_time,
        "knowledge_time_source": column.completion_time_source,
        "active_start_ts": active.start_ts,
        "active_end_ts": active.end_ts,
    }


def _run_symbol_threshold(
    symbol_columns: Sequence[RawColumn], threshold: ThresholdSet
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    active: ActiveSwing | None = None
    candidate: Candidate | None = None
    swing_rows: list[dict[str, Any]] = []
    reaction_rows: list[dict[str, Any]] = []
    sequence = 0

    for column in symbol_columns:
        if active is None:
            active = ActiveSwing(
                symbol=column.symbol,
                direction=column.direction,
                start_price=column.start_price,
                end_price=column.end_price,
                start_ts=column.start_ts,
                end_ts=column.end_ts,
                start_column_id=column.column_id,
                end_column_id=column.column_id,
                source_column_ids=[column.column_id],
            )
            continue

        same_direction = active.direction == column.direction
        active.source_column_ids.append(column.column_id)

        if same_direction:
            extended = False
            if active.direction == "UP" and column.high > active.end_price:
                active.end_price = column.high
                active.end_ts = column.end_ts
                active.end_column_id = column.column_id
                candidate = None
                extended = True
            elif active.direction == "DOWN" and column.low < active.end_price:
                active.end_price = column.low
                active.end_ts = column.end_ts
                active.end_column_id = column.column_id
                candidate = None
                extended = True
            if not extended:
                active.failed_extension_column_ids.append(column.column_id)
            continue

        if candidate is None:
            candidate = _new_candidate(active, column)
        else:
            candidate = _update_candidate(active, candidate, column)

        active.reaction_column_ids.append(column.column_id)
        active_boxes = active.boxes(column.box_size)
        required_boxes = max(
            threshold.min_pivot_boxes,
            threshold.min_pivot_ratio * active_boxes,
        )
        if candidate.boxes < required_boxes:
            reaction_rows.append(
                _reaction_row(
                    threshold=threshold,
                    active=active,
                    candidate=candidate,
                    column=column,
                    required_boxes=required_boxes,
                    reaction_kind="INTERNAL",
                )
            )
            continue

        reaction_rows.append(
            _reaction_row(
                threshold=threshold,
                active=active,
                candidate=candidate,
                column=column,
                required_boxes=required_boxes,
                reaction_kind="CONFIRMING",
            )
        )
        sequence += 1
        swing_rows.append(
            _swing_row(
                threshold=threshold,
                active=active,
                status="CONFIRMED",
                box_size=column.box_size,
                birth_time=column.completion_time,
                knowledge_time=column.completion_time,
                knowledge_time_source=column.completion_time_source,
                confirming_column_id=column.column_id,
                sequence=sequence,
            )
        )
        active = ActiveSwing(
            symbol=column.symbol,
            direction=candidate.direction,
            start_price=active.end_price,
            end_price=candidate.extreme_price,
            start_ts=active.end_ts,
            end_ts=column.end_ts,
            start_column_id=candidate.start_column_id,
            end_column_id=candidate.extreme_column_id,
            source_column_ids=list(candidate.source_column_ids),
        )
        candidate = None

    return swing_rows, reaction_rows


def run_threshold_audit(
    columns: Sequence[RawColumn], threshold_sets: Sequence[ThresholdSet]
) -> dict[str, list[dict[str, Any]]]:
    by_symbol: dict[str, list[RawColumn]] = defaultdict(list)
    for column in columns:
        by_symbol[column.symbol].append(column)

    all_swings: list[dict[str, Any]] = []
    all_reactions: list[dict[str, Any]] = []
    for threshold in threshold_sets:
        for symbol in sorted(by_symbol):
            swing_rows, reaction_rows = _run_symbol_threshold(by_symbol[symbol], threshold)
            all_swings.extend(swing_rows)
            all_reactions.extend(reaction_rows)

    return {
        "swings": all_swings,
        "reactions": all_reactions,
        "summary": _summary_rows(columns, all_swings, all_reactions, threshold_sets),
        "knowledge": _knowledge_rows(all_swings, threshold_sets),
        "pivot_counts": _pivot_count_rows(columns, all_swings, threshold_sets),
        "leg_stats": _leg_stat_rows(columns, all_swings, threshold_sets),
        "reaction_distribution": _reaction_distribution_rows(
            columns, all_reactions, threshold_sets
        ),
    }


def _float_values(rows: Iterable[dict[str, Any]], field: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = row.get(field)
        if value in (None, ""):
            continue
        try:
            parsed = float(str(value))
        except ValueError:
            continue
        if math.isfinite(parsed):
            values.append(parsed)
    return values


def _avg(values: Sequence[float]) -> float:
    return mean(values) if values else math.nan


def _median(values: Sequence[float]) -> float:
    return median(values) if values else math.nan


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * (percentile / 100.0)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[int(position)]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _symbols(columns: Sequence[RawColumn]) -> list[str]:
    return sorted({column.symbol for column in columns})


def _pivot_count_rows(
    columns: Sequence[RawColumn],
    swings: Sequence[dict[str, Any]],
    threshold_sets: Sequence[ThresholdSet],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for threshold in threshold_sets:
        for symbol in _symbols(columns):
            threshold_swings = [
                row
                for row in swings
                if row["threshold_name"] == threshold.name and row["symbol"] == symbol
            ]
            rows.append(
                {
                    "threshold_name": threshold.name,
                    "min_pivot_boxes": _format_number(threshold.min_pivot_boxes),
                    "min_pivot_ratio": _format_number(threshold.min_pivot_ratio),
                    "symbol": symbol,
                    "confirmed_pivot_count": len(threshold_swings) + 1,
                    "confirmed_leg_count": len(threshold_swings),
                    "up_leg_count": sum(
                        1 for row in threshold_swings if row["direction"] == "UP"
                    ),
                    "down_leg_count": sum(
                        1 for row in threshold_swings if row["direction"] == "DOWN"
                    ),
                }
            )
    return rows


def _leg_stat_rows(
    columns: Sequence[RawColumn],
    swings: Sequence[dict[str, Any]],
    threshold_sets: Sequence[ThresholdSet],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for threshold in threshold_sets:
        for symbol in _symbols(columns):
            threshold_swings = [
                row
                for row in swings
                if row["threshold_name"] == threshold.name and row["symbol"] == symbol
            ]
            leg_boxes = _float_values(threshold_swings, "swing_boxes")
            leg_distances = _float_values(threshold_swings, "swing_price_distance")
            rows.append(
                {
                    "threshold_name": threshold.name,
                    "min_pivot_boxes": _format_number(threshold.min_pivot_boxes),
                    "min_pivot_ratio": _format_number(threshold.min_pivot_ratio),
                    "symbol": symbol,
                    "leg_count": len(threshold_swings),
                    "avg_leg_boxes": _format_number(_avg(leg_boxes)),
                    "median_leg_boxes": _format_number(_median(leg_boxes)),
                    "p25_leg_boxes": _format_number(_percentile(leg_boxes, 25)),
                    "p75_leg_boxes": _format_number(_percentile(leg_boxes, 75)),
                    "min_leg_boxes": _format_number(
                        min(leg_boxes) if leg_boxes else math.nan
                    ),
                    "max_leg_boxes": _format_number(
                        max(leg_boxes) if leg_boxes else math.nan
                    ),
                    "avg_leg_price_distance": _format_number(_avg(leg_distances)),
                    "median_leg_price_distance": _format_number(_median(leg_distances)),
                }
            )
    return rows


def _reaction_distribution_rows(
    columns: Sequence[RawColumn],
    reactions: Sequence[dict[str, Any]],
    threshold_sets: Sequence[ThresholdSet],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for threshold in threshold_sets:
        for symbol in _symbols(columns):
            for reaction_kind in ("INTERNAL", "CONFIRMING", "ALL"):
                threshold_reactions = [
                    row
                    for row in reactions
                    if row["threshold_name"] == threshold.name
                    and row["symbol"] == symbol
                    and (
                        reaction_kind == "ALL"
                        or row["reaction_kind"] == reaction_kind
                    )
                ]
                ratios = _float_values(threshold_reactions, "reaction_ratio")
                total = len(ratios)
                for bucket, lower, upper in REACTION_RATIO_BUCKETS:
                    count = sum(1 for ratio in ratios if lower <= ratio < upper)
                    rows.append(
                        {
                            "threshold_name": threshold.name,
                            "min_pivot_boxes": _format_number(
                                threshold.min_pivot_boxes
                            ),
                            "min_pivot_ratio": _format_number(
                                threshold.min_pivot_ratio
                            ),
                            "symbol": symbol,
                            "reaction_kind": reaction_kind,
                            "bucket": bucket,
                            "count": count,
                            "percentage": _format_number(
                                (count / total) * 100.0 if total else math.nan
                            ),
                        }
                    )
    return rows


def _summary_rows(
    columns: Sequence[RawColumn],
    swings: Sequence[dict[str, Any]],
    reactions: Sequence[dict[str, Any]],
    threshold_sets: Sequence[ThresholdSet],
) -> list[dict[str, Any]]:
    symbols = {column.symbol for column in columns}
    rows: list[dict[str, Any]] = []
    for threshold in threshold_sets:
        threshold_swings = [row for row in swings if row["threshold_name"] == threshold.name]
        internal = [
            row
            for row in reactions
            if row["threshold_name"] == threshold.name
            and row["reaction_kind"] == "INTERNAL"
        ]
        confirming = [
            row
            for row in reactions
            if row["threshold_name"] == threshold.name
            and row["reaction_kind"] == "CONFIRMING"
        ]
        leg_boxes = _float_values(threshold_swings, "swing_boxes")
        leg_distances = _float_values(threshold_swings, "swing_price_distance")
        internal_ratios = _float_values(internal, "reaction_ratio")
        confirming_ratios = _float_values(confirming, "reaction_ratio")
        rows.append(
            {
                "threshold_name": threshold.name,
                "min_pivot_boxes": _format_number(threshold.min_pivot_boxes),
                "min_pivot_ratio": _format_number(threshold.min_pivot_ratio),
                "symbols": len(symbols),
                "input_columns": len(columns),
                "confirmed_pivot_count": len(threshold_swings) + len(symbols),
                "confirmed_leg_count": len(threshold_swings),
                "avg_leg_boxes": _format_number(_avg(leg_boxes)),
                "median_leg_boxes": _format_number(_median(leg_boxes)),
                "min_leg_boxes": _format_number(min(leg_boxes) if leg_boxes else math.nan),
                "max_leg_boxes": _format_number(max(leg_boxes) if leg_boxes else math.nan),
                "avg_leg_price_distance": _format_number(_avg(leg_distances)),
                "internal_reaction_count": len(internal),
                "avg_internal_reaction_ratio": _format_number(_avg(internal_ratios)),
                "median_internal_reaction_ratio": _format_number(_median(internal_ratios)),
                "confirming_reaction_count": len(confirming),
                "avg_confirming_reaction_ratio": _format_number(_avg(confirming_ratios)),
                "median_confirming_reaction_ratio": _format_number(
                    _median(confirming_ratios)
                ),
            }
        )
    return rows


def _knowledge_rows(
    swings: Sequence[dict[str, Any]], threshold_sets: Sequence[ThresholdSet]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for threshold in threshold_sets:
        threshold_swings = [row for row in swings if row["threshold_name"] == threshold.name]
        lags = _float_values(threshold_swings, "knowledge_lag")
        fallback_count = sum(
            1
            for row in threshold_swings
            if row.get("knowledge_time_source") == "end_ts_fallback"
        )
        explicit_count = sum(
            1
            for row in threshold_swings
            if str(row.get("knowledge_time_source") or "").startswith("explicit_")
        )
        rows.append(
            {
                "threshold_name": threshold.name,
                "knowledge_rows": len(threshold_swings),
                "knowledge_after_endpoint_count": sum(1 for lag in lags if lag > 0),
                "knowledge_equal_endpoint_count": sum(1 for lag in lags if lag == 0),
                "avg_knowledge_lag": _format_number(_avg(lags)),
                "median_knowledge_lag": _format_number(_median(lags)),
                "max_knowledge_lag": _format_number(max(lags) if lags else math.nan),
                "fallback_completion_time_count": fallback_count,
                "explicit_completion_time_count": explicit_count,
            }
        )
    return rows


def _box_size_manifest_rows(columns: Sequence[RawColumn]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, float, str, str, str, str]] = set()
    rows: list[dict[str, Any]] = []
    for column in sorted(
        columns,
        key=lambda item: (
            item.symbol,
            item.profile_name,
            item.box_size_source,
            item.box_size,
            item.warning_if_inferred,
            item.completion_time_source,
            item.knowledge_time_contract,
        ),
    ):
        key = (
            column.symbol,
            column.profile_name,
            column.box_size,
            column.box_size_source,
            column.warning_if_inferred,
            column.completion_time_source,
            column.knowledge_time_contract,
        )
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "symbol": column.symbol,
                "resolved_box_size": _format_number(column.box_size),
                "box_size_source": column.box_size_source,
                "profile_name": column.profile_name,
                "warning_if_inferred": column.warning_if_inferred,
                "knowledge_time_source": column.completion_time_source,
                "knowledge_time_contract": column.knowledge_time_contract,
            }
        )
    return rows


def _write_csv(path: Path, fields: Sequence[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields))
        writer.writeheader()
        writer.writerows(rows)


def run_audit(
    *,
    columns_input: Sequence[str | Path],
    output_root: str | Path,
    threshold_set_specs: Sequence[str] | None = None,
    box_size: float | None = None,
    symbol_box_sizes: dict[str, float] | None = None,
    allow_infer_box_size: bool = False,
) -> dict[str, Any]:
    threshold_specs = list(threshold_set_specs or DEFAULT_THRESHOLD_SETS)
    thresholds = [ThresholdSet.parse(spec) for spec in threshold_specs]
    columns = load_columns(
        [Path(path) for path in columns_input],
        box_size=box_size,
        symbol_box_sizes=symbol_box_sizes,
        allow_infer_box_size=allow_infer_box_size,
    )
    output_path = Path(output_root)
    output_path.mkdir(parents=True, exist_ok=True)

    results = run_threshold_audit(columns, thresholds)
    _write_csv(output_path / OUTPUT_SWINGS, SWING_FIELDS, results["swings"])
    _write_csv(output_path / OUTPUT_REACTIONS, REACTION_FIELDS, results["reactions"])
    _write_csv(output_path / OUTPUT_SUMMARY, SUMMARY_FIELDS, results["summary"])
    _write_csv(output_path / OUTPUT_KNOWLEDGE, KNOWLEDGE_FIELDS, results["knowledge"])
    _write_csv(
        output_path / OUTPUT_PIVOT_COUNTS,
        PIVOT_COUNT_FIELDS,
        results["pivot_counts"],
    )
    _write_csv(output_path / OUTPUT_LEG_STATS, LEG_STATS_FIELDS, results["leg_stats"])
    _write_csv(
        output_path / OUTPUT_REACTION_DISTRIBUTION,
        REACTION_DISTRIBUTION_FIELDS,
        results["reaction_distribution"],
    )
    _write_csv(
        output_path / OUTPUT_BOX_SIZE_MANIFEST,
        BOX_SIZE_MANIFEST_FIELDS,
        _box_size_manifest_rows(columns),
    )

    return {
        "input_columns": len(columns),
        "threshold_sets": len(thresholds),
        "output_root": str(output_path),
        "output_files": [str(output_path / name) for name in OUTPUT_NAMES],
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research-only audit comparing harmonic PnF swing thresholds."
    )
    parser.add_argument(
        "--columns-input",
        action="append",
        required=True,
        help="CSV of raw completed PnF columns. Repeat for multiple files.",
    )
    parser.add_argument(
        "--output-root",
        required=True,
        help="Directory where audit CSV artifacts will be written.",
    )
    parser.add_argument(
        "--threshold-set",
        action="append",
        help=(
            "Threshold set as NAME:MIN_PIVOT_BOXES:MIN_PIVOT_RATIO. "
            "Repeat to compare multiple sets. Defaults to FAST/BASE/SLOW."
        ),
    )
    parser.add_argument(
        "--box-size",
        type=float,
        help="Fallback box size when input rows do not include box_size/profile_name.",
    )
    parser.add_argument(
        "--symbol-box-size",
        action="append",
        help="Explicit symbol-specific box size as SYMBOL=BOX_SIZE. Repeat as needed.",
    )
    parser.add_argument(
        "--allow-infer-box-size",
        action="store_true",
        help="Allow fallback inference from minimum observed price spacing when no explicit box size is available.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> dict[str, Any]:
    args = _build_parser().parse_args(argv)
    summary = run_audit(
        columns_input=args.columns_input,
        output_root=args.output_root,
        threshold_set_specs=args.threshold_set,
        box_size=args.box_size,
        symbol_box_sizes=_parse_symbol_box_size_specs(args.symbol_box_size),
        allow_infer_box_size=args.allow_infer_box_size,
    )
    print(
        "harmonic swing threshold audit complete: "
        f"input_columns={summary['input_columns']} "
        f"threshold_sets={summary['threshold_sets']} "
        f"output_root={summary['output_root']}"
    )
    return summary


if __name__ == "__main__":  # pragma: no cover
    main()
