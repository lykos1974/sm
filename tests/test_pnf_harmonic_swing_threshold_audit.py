import csv
from pathlib import Path

import pytest

from research_v2.patterns.pnf_harmonic_swing_threshold_audit import (
    BOX_SIZE_MANIFEST_FIELDS,
    OUTPUT_BOX_SIZE_MANIFEST,
    load_columns,
    run_audit,
)


def _write_columns(path: Path, rows: list[dict[str, object]]) -> None:
    fields = sorted({field for row in rows for field in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _base_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "symbol": "BTCUSDT",
        "profile_name": "BTCUSDT_bs100_rev3",
        "idx": 1,
        "kind": "X",
        "top": 10100,
        "bottom": 10000,
        "start_ts": 1,
        "end_ts": 2,
    }
    row.update(overrides)
    return row


def test_profile_name_box_size_resolves_to_100(tmp_path: Path) -> None:
    columns_csv = tmp_path / "columns.csv"
    _write_columns(columns_csv, [_base_row(profile_name="BTCUSDT_bs100_rev3")])

    columns = load_columns([columns_csv])

    assert columns[0].box_size == 100
    assert columns[0].box_size_source == "profile_name"
    assert columns[0].profile_name == "BTCUSDT_bs100_rev3"
    assert columns[0].warning_if_inferred == ""


def test_explicit_csv_box_size_resolves_correctly(tmp_path: Path) -> None:
    columns_csv = tmp_path / "columns.csv"
    _write_columns(
        columns_csv,
        [_base_row(profile_name="unparseable", box_size="25")],
    )

    columns = load_columns([columns_csv])

    assert columns[0].box_size == 25
    assert columns[0].box_size_source == "explicit_csv"


def test_symbol_box_size_overrides_inference(tmp_path: Path) -> None:
    columns_csv = tmp_path / "columns.csv"
    _write_columns(
        columns_csv,
        [
            _base_row(profile_name="btc_style_v1", top="100.005", bottom="100.000"),
            _base_row(profile_name="btc_style_v1", idx=2, kind="O", top="100.000", bottom="99.995", start_ts=3, end_ts=4),
        ],
    )

    columns = load_columns([columns_csv], symbol_box_sizes={"BINANCE_FUT:BTCUSDT": 100})

    assert {column.box_size for column in columns} == {100}
    assert {column.box_size_source for column in columns} == {"symbol_box_size"}


def test_missing_box_size_raises_unless_allow_infer_box_size(tmp_path: Path) -> None:
    columns_csv = tmp_path / "columns.csv"
    _write_columns(
        columns_csv,
        [
            _base_row(profile_name="btc_style_v1", top="100.005", bottom="100.000"),
            _base_row(profile_name="btc_style_v1", idx=2, kind="O", top="100.000", bottom="99.995", start_ts=3, end_ts=4),
        ],
    )

    with pytest.raises(ValueError, match="allow-infer-box-size"):
        load_columns([columns_csv])

    columns = load_columns([columns_csv], allow_infer_box_size=True)

    assert [column.box_size for column in columns] == [pytest.approx(0.005), pytest.approx(0.005)]
    assert {column.box_size_source for column in columns} == {"inferred"}
    assert all(column.warning_if_inferred for column in columns)


def test_inferred_0005_cannot_silently_pass_for_major_symbols(tmp_path: Path) -> None:
    columns_csv = tmp_path / "columns.csv"
    _write_columns(
        columns_csv,
        [
            _base_row(symbol="BTCUSDT", profile_name="btc_style_v1", top="100.005", bottom="100.000"),
            _base_row(symbol="ETHUSDT", profile_name="btc_style_v1", idx=2, kind="O", top="10.005", bottom="10.000", start_ts=3, end_ts=4),
            _base_row(symbol="SOLUSDT", profile_name="btc_style_v1", idx=3, top="1.005", bottom="1.000", start_ts=5, end_ts=6),
        ],
    )

    with pytest.raises(ValueError, match="missing explicit box_size"):
        load_columns([columns_csv])


def test_run_audit_writes_box_size_manifest(tmp_path: Path) -> None:
    columns_csv = tmp_path / "columns.csv"
    output_root = tmp_path / "audit"
    _write_columns(
        columns_csv,
        [
            _base_row(profile_name="BTCUSDT_bs100_rev3", idx=1, kind="X", top=10100, bottom=10000, start_ts=1, end_ts=2),
            _base_row(profile_name="BTCUSDT_bs100_rev3", idx=2, kind="O", top=10000, bottom=9900, start_ts=3, end_ts=4),
        ],
    )

    result = run_audit(columns_input=[columns_csv], output_root=output_root)

    manifest_path = output_root / OUTPUT_BOX_SIZE_MANIFEST
    assert manifest_path.exists()
    with manifest_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == BOX_SIZE_MANIFEST_FIELDS
        rows = list(reader)
    assert rows == [
        {
            "symbol": "BTCUSDT",
            "resolved_box_size": "100",
            "box_size_source": "profile_name",
            "profile_name": "BTCUSDT_bs100_rev3",
            "warning_if_inferred": "",
        }
    ]
    assert str(manifest_path) in result["output_files"]
