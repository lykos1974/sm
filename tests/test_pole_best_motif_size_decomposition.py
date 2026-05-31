import csv
import sys
from pathlib import Path

from research_v2.patterns.pole_best_motif_size_decomposition import _build_rows, _pole_size_bucket, main


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _row(pattern: str, size: str, outcome: str, fav: str, adv: str, index: str, **overrides: str) -> dict[str, str]:
    row = {
        "pattern_name": pattern,
        "reversal_column_index": index,
        "opposing_pole_distance_columns": "3",
        "enhanced_by_opposing_pole": "False",
        "retrace_ratio": "1.2",
        "pole_boxes": size,
        "outcome_class": outcome,
        "max_favorable_boxes": fav,
        "max_adverse_boxes": adv,
    }
    return {**row, **overrides}


def test_pole_size_bucket_groups_required_curve_values() -> None:
    assert [_pole_size_bucket(value) for value in ("8", "9", "12", "13", "16", "17", "20", "21")] == [
        "<=8", "9-12", "9-12", "13-16", "13-16", "17-20", "17-20", ">20"
    ]


def test_build_rows_filters_best_motif_and_reuses_direction_inference() -> None:
    rows, diagnostics = _build_rows([
        _row("LOW_POLE", "8", "BULLISH_CONTINUATION", "5", "1", "2", pattern_direction="SHORT"),
        _row("HIGH_POLE", "9", "BEARISH_CONTINUATION", "5", "1", "1"),
        _row("LOW_POLE", "13", "BULLISH_CONTINUATION", "5", "1", "3", opposing_pole_distance_columns="2"),
    ])
    assert [(row.direction, row.pole_size_bucket) for row in rows] == [("SHORT", "9-12"), ("SHORT", "<=8")]
    assert diagnostics["explicit_direction_rows"] == 1
    assert diagnostics["pattern_name_direction_rows"] == 1
    assert diagnostics["rows_excluded_outside_best_motif"] == 1


def test_main_writes_summary_curve_segments_and_flags(tmp_path: Path, monkeypatch) -> None:
    labeled = tmp_path / "pole_labeled_outcomes.csv"
    distance_root = tmp_path / "distance"
    output = tmp_path / "sizes"
    distance_root.mkdir()
    _write_csv(distance_root / "pole_directional_distance_curve.csv", [{"scope": "enhanced=False", "direction": "LONG"}])
    _write_csv(distance_root / "pole_directional_distance_curve_flags.csv", [{"check_name": "distance_3_unique", "result": "YES"}])
    rows = []
    for index in range(1, 7):
        rows.append(_row("LOW_POLE", "10", "BULLISH_CONTINUATION", "6", "1", str(index)))
    for index in range(7, 13):
        rows.append(_row("HIGH_POLE", "22", "FAILED_REVERSAL", "1", "6", str(index)))
    _write_csv(labeled, rows)
    monkeypatch.setattr(sys, "argv", [
        "pole_best_motif_size_decomposition.py",
        "--input-labeled-outcomes-csv", str(labeled),
        "--input-directional-distance-curve-root", str(distance_root),
        "--output-root", str(output),
        "--min-sample", "2",
    ])

    main()

    assert {path.name for path in output.iterdir()} == {
        "pole_best_motif_size_summary.md",
        "pole_best_motif_size_curve.csv",
        "pole_best_motif_size_segments.csv",
        "pole_best_motif_size_flags.csv",
    }
    summary = (output / "pole_best_motif_size_summary.md").read_text()
    assert "- best_pole_size_bucket: 9-12" in summary
    assert "- weakest_pole_size_bucket: >20" in summary
    assert "- large_poles_exhaustion_risk: YES" in summary
    assert "- small_medium_poles_dominate: YES" in summary
