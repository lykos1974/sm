"""Research-only audit of reaction depth across confirmed structural PnF swings.

This module measures the empirical ratio ``reaction_boxes / prior_swing_boxes``
for consecutive, already-confirmed structural swing pairs. It intentionally does
not detect harmonic patterns, calculate expectancy, generate candidates, rank
setups, or modify production strategy/scanner/live-trading behavior.
"""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

OUTPUT_REACTION_RATIOS = "structural_reaction_ratios.csv"
OUTPUT_SUMMARY = "reaction_ratio_summary.csv"
OUTPUT_DISTRIBUTION = "reaction_ratio_distribution.csv"
OUTPUT_BY_SYMBOL = "reaction_ratio_by_symbol.csv"
OUTPUT_NAMES = (
    OUTPUT_REACTION_RATIOS,
    OUTPUT_SUMMARY,
    OUTPUT_DISTRIBUTION,
    OUTPUT_BY_SYMBOL,
)

REACTION_RATIO_FIELDS = [
    "symbol",
    "prior_swing_id",
    "reaction_swing_id",
    "prior_direction",
    "reaction_direction",
    "prior_swing_boxes",
    "reaction_boxes",
    "reaction_ratio",
    "prior_start_ts",
    "prior_end_ts",
    "reaction_start_ts",
    "reaction_end_ts",
]
SUMMARY_FIELDS = [
    "count",
    "mean",
    "median",
    "p10",
    "p20",
    "p25",
    "p30",
    "p40",
    "p50",
    "p60",
    "p70",
    "p75",
    "p80",
    "p90",
    "min",
    "max",
]
DISTRIBUTION_FIELDS = ["bucket", "count", "percentage"]
BY_SYMBOL_FIELDS = ["symbol", "count", "mean", "median", "p25", "p50", "p75"]

DISTRIBUTION_BUCKETS = (
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

_TRUE_VALUES = {
    "1",
    "TRUE",
    "T",
    "YES",
    "Y",
    "CONFIRMED",
    "COMPLETE",
    "COMPLETED",
    "CLOSED",
}
_FALSE_VALUES = {
    "0",
    "FALSE",
    "F",
    "NO",
    "N",
    "PENDING",
    "UNFINISHED",
    "CURRENT",
    "OPEN",
    "FORMING",
}
_CONFIRMED_STATES = {"CONFIRMED", "COMPLETE", "COMPLETED", "CLOSED"}
_UNFINISHED_STATES = {"PENDING", "UNFINISHED", "CURRENT", "OPEN", "FORMING"}


@dataclass(frozen=True)
class ConfirmedSwing:
    """Minimal causal structural swing fields needed for this audit."""

    symbol: str
    swing_id: str
    direction: str
    boxes: float
    start_ts: str
    end_ts: str
    ordinal: int


def _first_value(row: dict[str, Any], aliases: Sequence[str]) -> Any:
    for alias in aliases:
        value = row.get(alias)
        if value is not None and str(value).strip() != "":
            return value
    return None


def _require_value(
    row: dict[str, Any], aliases: Sequence[str], row_number: int, label: str
) -> str:
    value = _first_value(row, aliases)
    if value is None:
        raise ValueError(
            f"row {row_number}: missing required {label} column; accepted aliases: {', '.join(aliases)}"
        )
    return str(value).strip()


def _to_float(value: Any, *, row_number: int, label: str) -> float:
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"row {row_number}: invalid {label}: {value!r}") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"row {row_number}: invalid non-finite {label}: {value!r}")
    return parsed


def _parse_boolish(value: Any) -> bool | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    if text == "":
        return None
    if text in _TRUE_VALUES:
        return True
    if text in _FALSE_VALUES:
        return False
    return None


def _is_confirmed_swing(row: dict[str, Any]) -> bool:
    """Return whether a row is eligible as an already-confirmed swing.

    The structural swing export format is intentionally research-facing and may
    evolve. This audit therefore accepts common confirmation/status columns while
    defaulting to inclusion when no lifecycle marker exists, because the CLI input
    contract is a file of confirmed structural swings.
    """

    for field in (
        "is_pending",
        "pending",
        "is_unfinished",
        "unfinished",
        "is_current",
        "current",
    ):
        parsed = _parse_boolish(row.get(field))
        if parsed is True:
            return False

    explicit_confirmation_seen = False
    for field in (
        "is_confirmed",
        "confirmed",
        "is_complete",
        "complete",
        "completed",
        "closed",
    ):
        if field in row:
            parsed = _parse_boolish(row.get(field))
            if parsed is False:
                return False
            if parsed is True:
                explicit_confirmation_seen = True

    lifecycle_seen = explicit_confirmation_seen
    for field in (
        "confirmation_status",
        "swing_status",
        "status",
        "state",
        "lifecycle_state",
    ):
        if field not in row:
            continue
        text = str(row.get(field) or "").strip().upper()
        if not text:
            continue
        lifecycle_seen = True
        if text in _UNFINISHED_STATES:
            return False
        if text in _CONFIRMED_STATES:
            explicit_confirmation_seen = True

    return explicit_confirmation_seen if lifecycle_seen else True


def _normalize_direction(raw: str, *, row_number: int) -> str:
    direction = raw.strip().upper()
    if direction in {"X", "UP", "LONG", "BULL", "BULLISH"}:
        return "UP"
    if direction in {"O", "DOWN", "SHORT", "BEAR", "BEARISH"}:
        return "DOWN"
    raise ValueError(
        f"row {row_number}: direction must be UP/DOWN (or X/O), got {raw!r}"
    )


def _parse_boxes(row: dict[str, Any], *, row_number: int) -> float:
    value = _first_value(
        row,
        (
            "swing_boxes",
            "boxes",
            "box_count",
            "size_boxes",
            "move_boxes",
            "structural_swing_boxes",
            "leg_boxes",
        ),
    )
    if value is not None:
        boxes = abs(_to_float(value, row_number=row_number, label="swing boxes"))
    else:
        start_price = _first_value(
            row, ("start_extreme_price", "start_price", "start", "from_price")
        )
        end_price = _first_value(
            row, ("end_extreme_price", "end_price", "end", "to_price")
        )
        box_size = _first_value(row, ("box_size", "pnf_box_size"))
        if start_price is None or end_price is None or box_size is None:
            raise ValueError(
                f"row {row_number}: missing swing boxes; provide boxes or start/end prices plus box_size"
            )
        boxes = abs(
            (
                _to_float(end_price, row_number=row_number, label="end price")
                - _to_float(start_price, row_number=row_number, label="start price")
            )
            / _to_float(box_size, row_number=row_number, label="box_size")
        )
    if boxes <= 0:
        raise ValueError(
            f"row {row_number}: swing boxes must be positive, got {boxes!r}"
        )
    return boxes


def load_confirmed_swings(path: Path) -> list[ConfirmedSwing]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("swings input must be a CSV with a header row")
        rows = list(reader)

    swings: list[ConfirmedSwing] = []
    for ordinal, row in enumerate(rows, start=1):
        if not _is_confirmed_swing(row):
            continue
        symbol = _require_value(row, ("symbol",), ordinal, "symbol")
        swing_id = _require_value(
            row, ("swing_id", "structural_swing_id", "id"), ordinal, "swing_id"
        )
        direction = _normalize_direction(
            _require_value(row, ("direction", "swing_direction"), ordinal, "direction"),
            row_number=ordinal,
        )
        start_ts = _require_value(
            row,
            ("start_ts", "start_time", "start_extreme_time", "start_timestamp"),
            ordinal,
            "start timestamp",
        )
        end_ts = _require_value(
            row,
            ("end_ts", "end_time", "end_extreme_time", "end_timestamp"),
            ordinal,
            "end timestamp",
        )
        swings.append(
            ConfirmedSwing(
                symbol=symbol.strip(),
                swing_id=swing_id,
                direction=direction,
                boxes=_parse_boxes(row, row_number=ordinal),
                start_ts=start_ts,
                end_ts=end_ts,
                ordinal=ordinal,
            )
        )
    return swings


def _format_number(value: float | int | str) -> str | int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return value
    if not math.isfinite(value):
        return ""
    return f"{value:.12g}"


def build_reaction_rows(swings: Iterable[ConfirmedSwing]) -> list[dict[str, Any]]:
    by_symbol: dict[str, list[ConfirmedSwing]] = defaultdict(list)
    for swing in swings:
        by_symbol[swing.symbol].append(swing)

    rows: list[dict[str, Any]] = []
    for symbol in sorted(by_symbol):
        ordered = sorted(
            by_symbol[symbol],
            key=lambda swing: (swing.start_ts, swing.end_ts, swing.ordinal),
        )
        for prior, reaction in zip(ordered, ordered[1:]):
            if prior.direction == reaction.direction:
                continue
            ratio = reaction.boxes / prior.boxes
            rows.append(
                {
                    "symbol": symbol,
                    "prior_swing_id": prior.swing_id,
                    "reaction_swing_id": reaction.swing_id,
                    "prior_direction": prior.direction,
                    "reaction_direction": reaction.direction,
                    "prior_swing_boxes": _format_number(prior.boxes),
                    "reaction_boxes": _format_number(reaction.boxes),
                    "reaction_ratio": _format_number(ratio),
                    "prior_start_ts": prior.start_ts,
                    "prior_end_ts": prior.end_ts,
                    "reaction_start_ts": reaction.start_ts,
                    "reaction_end_ts": reaction.end_ts,
                }
            )
    return rows


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
    lower_value = ordered[lower]
    upper_value = ordered[upper]
    return lower_value + (upper_value - lower_value) * (position - lower)


def summary_row(ratios: Sequence[float]) -> dict[str, Any]:
    count = len(ratios)
    if count == 0:
        return {field: (0 if field == "count" else "") for field in SUMMARY_FIELDS}
    return {
        "count": count,
        "mean": _format_number(sum(ratios) / count),
        "median": _format_number(_percentile(ratios, 50)),
        "p10": _format_number(_percentile(ratios, 10)),
        "p20": _format_number(_percentile(ratios, 20)),
        "p25": _format_number(_percentile(ratios, 25)),
        "p30": _format_number(_percentile(ratios, 30)),
        "p40": _format_number(_percentile(ratios, 40)),
        "p50": _format_number(_percentile(ratios, 50)),
        "p60": _format_number(_percentile(ratios, 60)),
        "p70": _format_number(_percentile(ratios, 70)),
        "p75": _format_number(_percentile(ratios, 75)),
        "p80": _format_number(_percentile(ratios, 80)),
        "p90": _format_number(_percentile(ratios, 90)),
        "min": _format_number(min(ratios)),
        "max": _format_number(max(ratios)),
    }


def distribution_rows(ratios: Sequence[float]) -> list[dict[str, Any]]:
    total = len(ratios)
    rows: list[dict[str, Any]] = []
    for label, lower, upper in DISTRIBUTION_BUCKETS:
        count = sum(1 for ratio in ratios if ratio >= lower and ratio < upper)
        rows.append(
            {
                "bucket": label,
                "count": count,
                "percentage": _format_number((count / total) * 100.0) if total else "",
            }
        )
    return rows


def by_symbol_rows(reaction_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    ratios_by_symbol: dict[str, list[float]] = defaultdict(list)
    for row in reaction_rows:
        ratios_by_symbol[str(row["symbol"])].append(float(row["reaction_ratio"]))

    rows: list[dict[str, Any]] = []
    for symbol in sorted(ratios_by_symbol):
        ratios = ratios_by_symbol[symbol]
        rows.append(
            {
                "symbol": symbol,
                "count": len(ratios),
                "mean": _format_number(sum(ratios) / len(ratios)),
                "median": _format_number(_percentile(ratios, 50)),
                "p25": _format_number(_percentile(ratios, 25)),
                "p50": _format_number(_percentile(ratios, 50)),
                "p75": _format_number(_percentile(ratios, 75)),
            }
        )
    return rows


def _write_csv(
    path: Path, fields: Sequence[str], rows: Iterable[dict[str, Any]]
) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields))
        writer.writeheader()
        writer.writerows(rows)


def run_audit(swings_input: str | Path, output_root: str | Path) -> dict[str, Any]:
    swings_path = Path(swings_input)
    output_path = Path(output_root)
    output_path.mkdir(parents=True, exist_ok=True)

    swings = load_confirmed_swings(swings_path)
    reaction_rows = build_reaction_rows(swings)
    ratios = [float(row["reaction_ratio"]) for row in reaction_rows]

    _write_csv(
        output_path / OUTPUT_REACTION_RATIOS, REACTION_RATIO_FIELDS, reaction_rows
    )
    _write_csv(output_path / OUTPUT_SUMMARY, SUMMARY_FIELDS, [summary_row(ratios)])
    _write_csv(
        output_path / OUTPUT_DISTRIBUTION,
        DISTRIBUTION_FIELDS,
        distribution_rows(ratios),
    )
    _write_csv(
        output_path / OUTPUT_BY_SYMBOL, BY_SYMBOL_FIELDS, by_symbol_rows(reaction_rows)
    )

    return {
        "confirmed_swings": len(swings),
        "reaction_observations": len(reaction_rows),
        "output_root": str(output_path),
        "output_files": [str(output_path / name) for name in OUTPUT_NAMES],
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research-only audit of structural PnF reaction ratios."
    )
    parser.add_argument(
        "--swings-input",
        required=True,
        help="CSV containing confirmed structural swings.",
    )
    parser.add_argument(
        "--output-root",
        required=True,
        help="Folder where audit CSV artifacts will be written.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> dict[str, Any]:
    args = _build_parser().parse_args(argv)
    summary = run_audit(swings_input=args.swings_input, output_root=args.output_root)
    print(
        "structural reaction ratio audit complete: "
        f"confirmed_swings={summary['confirmed_swings']} "
        f"reaction_observations={summary['reaction_observations']} "
        f"output_root={summary['output_root']}"
    )
    return summary


if __name__ == "__main__":  # pragma: no cover
    main()
