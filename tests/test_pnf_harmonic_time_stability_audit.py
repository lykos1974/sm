import csv
from datetime import datetime, timezone
from pathlib import Path

from research_v2.patterns.pnf_harmonic_swing_threshold_audit import RawColumn
from research_v2.patterns.pnf_harmonic_time_stability_audit import (
    CROSS_PERIOD_CONSISTENCY_FIELDS,
    LEVEL_RANKING_FIELDS,
    LEVEL_SURVIVAL_FIELDS,
    OUTPUT_CROSS_PERIOD_CONSISTENCY,
    OUTPUT_LEVEL_RANKINGS,
    OUTPUT_LEVEL_SURVIVAL,
    OUTPUT_PERIOD_COMPARISON,
    OUTPUT_REPORT,
    OUTPUT_TIME_SUMMARY,
    PERIOD_COMPARISON_FIELDS,
    TIME_SUMMARY_FIELDS,
    _columns_for_period,
    _period_specs,
    _timestamp_seconds,
    run_audit,
)


def _utc_year(raw: int) -> int:
    parsed = _timestamp_seconds(raw)
    assert parsed is not None
    return datetime.fromtimestamp(parsed, timezone.utc).year


def _column(symbol: str, column_id: str, end_ms: int) -> RawColumn:
    return RawColumn(
        symbol=symbol,
        column_id=column_id,
        ordinal=int(column_id.removeprefix("c") or "0"),
        kind="X",
        high=120.0,
        low=100.0,
        start_ts=str(end_ms - 60_000),
        end_ts=str(end_ms),
        completion_time=str(end_ms),
        completion_time_source="explicit",
        box_size=1.0,
    )


def _write_columns(path: Path) -> None:
    fields = [
        "symbol",
        "column_id",
        "ordinal",
        "direction",
        "high",
        "low",
        "start_ts",
        "end_ts",
        "box_size",
    ]
    rows = []
    periods = [
        ("2024", 1704067259999),
        ("2025", 1735689600000),
        ("2026", 1767225600000),
    ]
    ordinal = 1
    for _year, base_ms in periods:
        rows.extend(
            [
                {
                    "symbol": "BINANCE_FUT:BTCUSDT",
                    "column_id": f"c{ordinal}",
                    "ordinal": ordinal,
                    "direction": "X",
                    "high": 120,
                    "low": 100,
                    "start_ts": base_ms,
                    "end_ts": base_ms + 60_000,
                    "box_size": 1,
                },
                {
                    "symbol": "BINANCE_FUT:BTCUSDT",
                    "column_id": f"c{ordinal + 1}",
                    "ordinal": ordinal + 1,
                    "direction": "O",
                    "high": 119,
                    "low": 116,
                    "start_ts": base_ms + 60_000,
                    "end_ts": base_ms + 120_000,
                    "box_size": 1,
                },
            ]
        )
        ordinal += 2
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _header(path: Path) -> list[str]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        return next(reader)


def test_unix_millisecond_timestamps_convert_to_utc_years() -> None:
    assert _utc_year(1704067259999) == 2024
    assert _utc_year(1735689600000) == 2025
    assert _utc_year(1767225600000) == 2026


def test_period_slicing_includes_unix_millisecond_rows_for_each_year() -> None:
    columns = [
        _column("BINANCE_FUT:BTCUSDT", "c1", 1704067259999),
        _column("BINANCE_FUT:BTCUSDT", "c2", 1735689600000),
        _column("BINANCE_FUT:BTCUSDT", "c3", 1767225600000),
    ]
    periods = {period.name: period for period in _period_specs()}

    assert [
        column.column_id for column in _columns_for_period(columns, periods["2024"])
    ] == ["c1"]
    assert [
        column.column_id for column in _columns_for_period(columns, periods["2025"])
    ] == ["c2"]
    assert [
        column.column_id for column in _columns_for_period(columns, periods["2026"])
    ] == ["c3"]


def test_synthetic_columns_dataset_produces_reactions_in_expected_periods(
    tmp_path: Path,
) -> None:
    columns_csv = tmp_path / "columns.csv"
    output_root = tmp_path / "audit"
    _write_columns(columns_csv)

    run_audit(columns_input=[columns_csv], output_root=output_root)

    rows = _read_csv(output_root / OUTPUT_TIME_SUMMARY)
    btc_by_period = {
        row["period"]: int(row["total_reactions"])
        for row in rows
        if row["symbol"] == "BTC" and row["period"] in {"2024", "2025", "2026"}
    }

    assert btc_by_period == {"2024": 1, "2025": 1, "2026": 1}


def test_output_schemas_remain_unchanged(tmp_path: Path) -> None:
    columns_csv = tmp_path / "columns.csv"
    output_root = tmp_path / "audit"
    _write_columns(columns_csv)

    result = run_audit(columns_input=[columns_csv], output_root=output_root)

    assert _header(output_root / OUTPUT_TIME_SUMMARY) == TIME_SUMMARY_FIELDS
    assert _header(output_root / OUTPUT_LEVEL_RANKINGS) == LEVEL_RANKING_FIELDS
    assert _header(output_root / OUTPUT_LEVEL_SURVIVAL) == LEVEL_SURVIVAL_FIELDS
    assert (
        _header(output_root / OUTPUT_CROSS_PERIOD_CONSISTENCY)
        == CROSS_PERIOD_CONSISTENCY_FIELDS
    )
    assert _header(output_root / OUTPUT_PERIOD_COMPARISON) == PERIOD_COMPARISON_FIELDS
    assert (output_root / OUTPUT_REPORT).exists()
    assert [Path(path).name for path in result["output_files"]] == [
        OUTPUT_TIME_SUMMARY,
        OUTPUT_LEVEL_RANKINGS,
        OUTPUT_LEVEL_SURVIVAL,
        OUTPUT_CROSS_PERIOD_CONSISTENCY,
        OUTPUT_PERIOD_COMPARISON,
        OUTPUT_REPORT,
    ]
