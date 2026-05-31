import csv
import sys
from pathlib import Path

from research_v2.patterns.pole_forward_validation import (
    _bucket_pole_size,
    _bucket_retrace_ratio,
    _build_rows_from_labeled,
    _missing_required_columns,
    _select_chronological_index_source,
    main,
)


BASE_ROW = {
    "outcome_class": "BULLISH_CONTINUATION",
    "max_favorable_boxes": "5",
    "max_adverse_boxes": "1",
    "opposing_pole_distance_columns": "3",
    "enhanced_by_opposing_pole": "False",
    "pole_boxes": "12",
    "retrace_boxes": "8",
    "retrace_ratio": "0.8",
}


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_required_columns_exclude_metadata_and_derived_buckets() -> None:
    columns = [*BASE_ROW, "pole_column_index"]

    assert _missing_required_columns(columns) == []
    assert _select_chronological_index_source(columns) == "pole_column_index"


def test_reversal_column_index_is_preferred_for_chronological_ordering() -> None:
    columns = [*BASE_ROW, "pole_column_index", "reversal_column_index"]

    assert _select_chronological_index_source(columns) == "reversal_column_index"


def test_missing_required_columns_reports_chronological_index_alternatives() -> None:
    assert _missing_required_columns(list(BASE_ROW)) == ["reversal_column_index or pole_column_index"]


def test_buckets_are_computed_from_continuous_geometry_fields() -> None:
    rows = _build_rows_from_labeled([{**BASE_ROW, "pole_column_index": "0"}], "pole_column_index")

    assert len(rows) == 1
    assert rows[0].ts == 0
    assert rows[0].pole_boxes_bucket == "9-12"
    assert rows[0].retrace_ratio_bucket == "0.75-1.00"
    assert [_bucket_pole_size(value) for value in (8, 12, 20, 21)] == ["<=8", "9-12", "13-20", ">20"]
    assert [_bucket_retrace_ratio(value) for value in (0.5, 0.75, 1.0, 1.5, 1.6)] == [
        "0.50-0.75",
        "0.75-1.00",
        "1.00-1.50",
        "1.00-1.50",
        ">1.50",
    ]


def test_main_accepts_metadata_only_btc_csv_and_reports_computed_fields(tmp_path: Path, monkeypatch) -> None:
    labeled_csv = tmp_path / "labeled.csv"
    btc_csv = tmp_path / "btc.csv"
    output_root = tmp_path / "output"
    _write_csv(
        labeled_csv,
        [
            {**BASE_ROW, "reversal_column_index": "2", "pole_column_index": "1"},
            {**BASE_ROW, "reversal_column_index": "1", "pole_column_index": "0"},
        ],
    )
    _write_csv(btc_csv, [{"metadata": "btc-only"}])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pole_forward_validation.py",
            "--input-labeled-outcomes-csv",
            str(labeled_csv),
            "--input-btc-columns-csv",
            str(btc_csv),
            "--output-root",
            str(output_root),
            "--min-window-sample",
            "1",
        ],
    )

    main()

    summary = (output_root / "pole_forward_validation_summary.md").read_text()
    assert "- missing required labeled columns: NONE" in summary
    assert "- chronological index source selected: reversal_column_index" in summary
    assert "- computed pole_boxes_bucket: True" in summary
    assert "- computed retrace_ratio_bucket: True" in summary
    assert "- btc enrichment applied: False" in summary
    assert "- rows after sorting: 2" in summary
