import csv
from pathlib import Path

import pytest

from research_v2.patterns.pnf_abcd_population_audit import (
    load_validated_pivots,
    run_audit as run_abcd_population_audit,
)
from research_v2.patterns.pnf_harmonic_swing_threshold_audit import (
    BOX_SIZE_MANIFEST_FIELDS,
    OUTPUT_BOX_SIZE_MANIFEST,
    OUTPUT_REACTIONS,
    OUTPUT_SWINGS,
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
            "knowledge_time_source": "end_ts_fallback",
            "knowledge_time_contract": "Fallback only: source did not provide a confirmed-column knowledge timestamp, so end_ts was used for legacy harmonic-threshold diagnostics; do not treat as design_v2-validated causal input.",
        }
    ]
    assert str(manifest_path) in result["output_files"]


def _alternating_confirmed_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for idx, kind in enumerate(["X", "O", "X", "O", "X", "O"], start=1):
        rows.append(
            _base_row(
                idx=idx,
                kind=kind,
                top=110,
                bottom=100,
                start_ts=1_704_067_200 + idx * 100,
                end_ts=1_704_067_250 + idx * 100,
                completion_time=1_704_067_260 + idx * 100,
                profile_name="BTCUSDT_bs1_rev3",
            )
        )
    return rows


def test_run_audit_writes_causal_knowledge_time_to_swings_and_reactions(tmp_path: Path) -> None:
    columns_csv = tmp_path / "columns.csv"
    output_root = tmp_path / "audit"
    _write_columns(columns_csv, _alternating_confirmed_rows())

    run_audit(columns_input=[columns_csv], output_root=output_root)

    for filename in (OUTPUT_SWINGS, OUTPUT_REACTIONS):
        with (output_root / filename).open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            assert "knowledge_time" in (reader.fieldnames or [])
            assert "knowledge_time_source" in (reader.fieldnames or [])
            rows = list(reader)
        assert rows
        assert {row["knowledge_time_source"] for row in rows} == {"explicit_completion_time"}
        assert all(row["knowledge_time"] for row in rows)

    with (output_root / OUTPUT_BOX_SIZE_MANIFEST).open(newline="", encoding="utf-8") as handle:
        manifest_rows = list(csv.DictReader(handle))
    assert manifest_rows[0]["knowledge_time_source"] == "explicit_completion_time"
    assert "first timestamp" in manifest_rows[0]["knowledge_time_contract"]


def test_abcd_population_audit_accepts_regenerated_harmonic_artifact(tmp_path: Path) -> None:
    columns_csv = tmp_path / "columns.csv"
    harmonic_root = tmp_path / "VALIDATED_harmonic_swing_threshold_local_v3"
    abcd_root = tmp_path / "abcd"
    _write_columns(columns_csv, _alternating_confirmed_rows())

    run_audit(columns_input=[columns_csv], output_root=harmonic_root)

    assert run_abcd_population_audit(input_root=harmonic_root, output_root=abcd_root) is True
    pivots, rejects, _ = load_validated_pivots(harmonic_root)
    assert pivots
    assert rejects["missing_or_invalid_knowledge_time"] == 0
    report = (abcd_root / "abcd_population_report.md").read_text(encoding="utf-8")
    assert "Explicit `knowledge_time` required; no `completion_time` fallback is allowed or used." in report


def test_abcd_population_audit_still_rejects_missing_knowledge_time(tmp_path: Path) -> None:
    input_root = tmp_path / "artifact"
    output_root = tmp_path / "abcd"
    input_root.mkdir()
    reactions_path = input_root / OUTPUT_REACTIONS
    with reactions_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "threshold_name",
                "symbol",
                "candidate_direction",
                "reaction_kind",
                "candidate_boxes",
                "completion_time",
                "column_id",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "threshold_name": "SLOW",
                "symbol": "BTCUSDT",
                "candidate_direction": "UP",
                "reaction_kind": "CONFIRMING",
                "candidate_boxes": "10",
                "completion_time": "1704067200",
                "column_id": "1",
            }
        )

    assert run_abcd_population_audit(input_root=input_root, output_root=output_root) is False
    report = (output_root / "abcd_population_report.md").read_text(encoding="utf-8")
    assert "missing required knowledge_time column; completion_time fallback is forbidden" in report
