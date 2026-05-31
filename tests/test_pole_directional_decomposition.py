import csv
import sys
from pathlib import Path

from research_v2.patterns.pole_directional_decomposition import _build_rows, _infer_direction, main


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _row(index: int, pattern: str, outcome: str, fav: str, adv: str, retrace: str = "1.2") -> dict[str, str]:
    return {
        "reversal_column_index": str(index),
        "pattern_name": pattern,
        "outcome_class": outcome,
        "max_favorable_boxes": fav,
        "max_adverse_boxes": adv,
        "opposing_pole_distance_columns": "3",
        "enhanced_by_opposing_pole": "False",
        "retrace_ratio": retrace,
    }


def test_direction_prefers_explicit_pattern_direction_then_uses_pole_type() -> None:
    assert _infer_direction({"pattern_direction": "SHORT", "pattern_name": "LOW_POLE"}) == ("SHORT", "pattern_direction")
    assert _infer_direction({"pattern_name": "LOW_POLE"}) == ("LONG", "pattern_name")
    assert _infer_direction({"pattern_name": "HIGH_POLE"}) == ("SHORT", "pattern_name")


def test_build_rows_splits_actual_pole_directions() -> None:
    rows, diagnostics = _build_rows([
        _row(1, "LOW_POLE", "BULLISH_CONTINUATION", "5", "1"),
        _row(2, "HIGH_POLE", "FAILED_REVERSAL", "1", "5"),
    ])

    assert [row.direction for row in rows] == ["LONG", "SHORT"]
    assert diagnostics["pattern_name_direction_rows"] == 2


def test_main_writes_directional_audit_outputs(tmp_path: Path, monkeypatch) -> None:
    labeled = tmp_path / "pole_labeled_outcomes.csv"
    forward_root = tmp_path / "pole_forward_validation_btc_v4"
    output_root = tmp_path / "out"
    forward_root.mkdir()
    _write_csv(labeled, [
        _row(1, "LOW_POLE", "BULLISH_CONTINUATION", "6", "1"),
        _row(2, "HIGH_POLE", "FAILED_REVERSAL", "1", "6"),
        _row(3, "LOW_POLE", "BULLISH_CONTINUATION", "6", "1"),
        _row(4, "HIGH_POLE", "FAILED_REVERSAL", "1", "6"),
        _row(5, "LOW_POLE", "BULLISH_CONTINUATION", "6", "1"),
        _row(6, "HIGH_POLE", "FAILED_REVERSAL", "1", "6"),
    ])
    _write_csv(forward_root / "pole_forward_validation_windows.csv", [{
        "window_id": "1", "forward_start_ts": "1", "forward_end_ts": "6"
    }])
    monkeypatch.setattr(sys, "argv", [
        "pole_directional_decomposition.py",
        "--input-labeled-outcomes-csv", str(labeled),
        "--input-forward-validation-root", str(forward_root),
        "--output-root", str(output_root),
        "--min-sample", "2",
    ])

    main()

    assert {path.name for path in output_root.iterdir()} == {
        "pole_directional_summary.md",
        "pole_directional_breakdown.csv",
        "pole_directional_segments.csv",
        "pole_directional_stability.csv",
    }
    summary = (output_root / "pole_directional_summary.md").read_text()
    assert "- directional dominance: LONG" in summary
    assert "- side collapse: SHORT" in summary
    assert "- recommendation: LONG_ONLY" in summary
