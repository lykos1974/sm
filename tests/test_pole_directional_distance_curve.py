import csv
import sys
from pathlib import Path

from research_v2.patterns.pole_directional_distance_curve import _build_rows, _distance_bucket, main


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _row(pattern: str, distance: str, enhanced: str, retrace: str, outcome: str, fav: str, adv: str) -> dict[str, str]:
    return {
        "pattern_name": pattern,
        "opposing_pole_distance_columns": distance,
        "enhanced_by_opposing_pole": enhanced,
        "retrace_ratio": retrace,
        "outcome_class": outcome,
        "max_favorable_boxes": fav,
        "max_adverse_boxes": adv,
    }


def test_distance_bucket_groups_required_curve_values() -> None:
    assert [_distance_bucket(value) for value in ("1", "2", "3", "4", "5", "", "garbage")] == [
        "distance=1", "distance=2", "distance=3", "distance=4", "distance>4", "NA", "NA"
    ]


def test_build_rows_reuses_directional_decomposition_inference() -> None:
    rows, diagnostics = _build_rows([
        {**_row("LOW_POLE", "3", "False", "1.2", "BULLISH_CONTINUATION", "5", "1"), "pattern_direction": "SHORT"},
        _row("HIGH_POLE", "3", "False", "1.2", "BEARISH_CONTINUATION", "5", "1"),
    ])

    assert [row.direction for row in rows] == ["SHORT", "SHORT"]
    assert diagnostics == {"rows_missing_direction": 0, "explicit_direction_rows": 1, "pattern_name_direction_rows": 1}


def test_main_writes_curve_summary_and_flags(tmp_path: Path, monkeypatch) -> None:
    labeled = tmp_path / "pole_labeled_outcomes.csv"
    decomposition = tmp_path / "decomposition"
    output = tmp_path / "curve"
    decomposition.mkdir()
    _write_csv(decomposition / "pole_directional_breakdown.csv", [{"scope": "all_direction_observations"}])
    rows = []
    for pattern, continuation in (("LOW_POLE", "BULLISH_CONTINUATION"), ("HIGH_POLE", "BEARISH_CONTINUATION")):
        rows.extend([
            _row(pattern, "3", "False", "1.2", continuation, "6", "1"),
            _row(pattern, "3", "False", "1.3", continuation, "6", "1"),
            _row(pattern, "2", "False", "0.8", "FAILED_REVERSAL", "1", "6"),
            _row(pattern, "2", "False", "0.9", "FAILED_REVERSAL", "1", "6"),
            _row(pattern, "3", "True", "1.2", "FAILED_REVERSAL", "1", "6"),
            _row(pattern, "3", "True", "1.3", "FAILED_REVERSAL", "1", "6"),
        ])
    _write_csv(labeled, rows)
    monkeypatch.setattr(sys, "argv", [
        "pole_directional_distance_curve.py",
        "--input-labeled-outcomes-csv", str(labeled),
        "--input-directional-decomposition-root", str(decomposition),
        "--output-root", str(output),
        "--min-sample", "2",
    ])

    main()

    assert {path.name for path in output.iterdir()} == {
        "pole_directional_distance_curve_summary.md",
        "pole_directional_distance_curve.csv",
        "pole_directional_distance_curve_flags.csv",
    }
    summary = (output / "pole_directional_distance_curve_summary.md").read_text()
    assert "- distance_3_unique: YES" in summary
    assert "- long_distance_3_dominant: YES" in summary
    assert "- short_distance_3_dominant: YES" in summary
    assert "- enhanced_false_required: YES" in summary
