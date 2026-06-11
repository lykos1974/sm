import csv
from pathlib import Path

from research_v2.patterns.pnf_ratio_predictive_audit import (
    DEFAULT_SYMBOLS,
    OUTPUT_BY_YEAR,
    OUTPUT_STABILITY_REPORT,
    build_symbol_rows,
    build_year_rows,
    measure_next_confirmed_swings,
    normalize_symbol,
    run_audit,
    write_stability_report,
)


def _reaction(
    *,
    symbol: str = "BINANCE_FUT:BTCUSDT",
    ratio: float = 0.45,
    reaction_kind: str = "INTERNAL",
    completion_time: float = 1.0,
    active_direction: str = "UP",
    candidate_direction: str = "DOWN",
    candidate_boxes: float = 10.0,
    column_id: int = 1,
) -> dict[str, object]:
    return {
        "threshold_name": "SLOW",
        "symbol": symbol,
        "active_direction": active_direction,
        "candidate_direction": candidate_direction,
        "reaction_kind": reaction_kind,
        "candidate_boxes": candidate_boxes,
        "active_swing_boxes": 100.0,
        "reaction_ratio": ratio,
        "column_id": column_id,
        "completion_time": completion_time,
        "active_start_ts": "",
        "active_end_ts": "",
    }


def _measured_rows_for(symbol: str, *, start_time: float = 1.0, pairs: int = 2):
    reactions = []
    for offset in range(pairs):
        completion_time = start_time + offset * 10
        reactions.append(
            _reaction(
                symbol=symbol,
                ratio=0.45,
                reaction_kind="INTERNAL",
                completion_time=completion_time,
                column_id=offset * 2 + 1,
            )
        )
        reactions.append(
            _reaction(
                symbol=symbol,
                ratio=3.0,
                reaction_kind="CONFIRMING",
                completion_time=completion_time + 1,
                candidate_direction="UP",
                column_id=offset * 2 + 2,
            )
        )
    return measure_next_confirmed_swings([_to_reaction(row) for row in reactions])


def _to_reaction(row: dict[str, object]):
    from research_v2.patterns.pnf_ratio_predictive_audit import Reaction

    return Reaction(
        threshold_name=str(row["threshold_name"]),
        symbol=str(row["symbol"]),
        active_direction=str(row["active_direction"]),
        candidate_direction=str(row["candidate_direction"]),
        reaction_kind=str(row["reaction_kind"]),
        candidate_boxes=float(row["candidate_boxes"]),
        active_swing_boxes=float(row["active_swing_boxes"]),
        reaction_ratio=float(row["reaction_ratio"]),
        column_id=str(row["column_id"]),
        completion_time=float(row["completion_time"]),
        active_start_ts=str(row["active_start_ts"]),
        active_end_ts=str(row["active_end_ts"]),
    )


def _write_reactions_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fieldnames = list(rows[0])
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _stability_reaction_rows(symbol: str, *, start_time: float) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    column_id = int(start_time)
    # Build robust sub-0.40 reversal and 0.40+ continuation samples. Confirming
    # rows use an out-of-scope ratio so bucket definitions remain untouched.
    for offset in range(30):
        completion_time = start_time + offset * 10
        rows.append(
            _reaction(
                symbol=symbol,
                ratio=0.35,
                reaction_kind="INTERNAL",
                completion_time=completion_time,
                active_direction="UP",
                candidate_direction="DOWN",
                column_id=column_id,
            )
        )
        rows.append(
            _reaction(
                symbol=symbol,
                ratio=3.0,
                reaction_kind="CONFIRMING",
                completion_time=completion_time + 1,
                active_direction="UP",
                candidate_direction="DOWN",
                column_id=column_id + 1,
            )
        )
        column_id += 2
    for offset in range(30):
        completion_time = start_time + 500 + offset * 10
        rows.append(
            _reaction(
                symbol=symbol,
                ratio=0.45,
                reaction_kind="INTERNAL",
                completion_time=completion_time,
                active_direction="UP",
                candidate_direction="DOWN",
                column_id=column_id,
            )
        )
        rows.append(
            _reaction(
                symbol=symbol,
                ratio=3.0,
                reaction_kind="CONFIRMING",
                completion_time=completion_time + 1,
                active_direction="UP",
                candidate_direction="UP",
                column_id=column_id + 1,
            )
        )
        column_id += 2
    return rows


def test_exchange_qualified_symbols_aggregate_under_bare_symbol() -> None:
    rows = _measured_rows_for("BINANCE_FUT:BTCUSDT", pairs=2)

    symbol_rows = build_symbol_rows(rows, symbols=("BTCUSDT",))

    assert normalize_symbol("BINANCE_FUT:BTCUSDT") == "BTCUSDT"
    target = next(row for row in symbol_rows if row["bucket"] == "0.40-0.50")
    assert target["symbol"] == "BTCUSDT"
    assert target["raw_reactions"] == 2
    assert target["measured_count"] == 2


def test_by_symbol_counts_are_non_zero_for_exchange_qualified_input() -> None:
    rows = _measured_rows_for("BINANCE_FUT:BTCUSDT", pairs=1)

    symbol_rows = build_symbol_rows(rows, symbols=DEFAULT_SYMBOLS)

    btc_rows = [row for row in symbol_rows if row["symbol"] == "BTCUSDT"]
    other_rows = [row for row in symbol_rows if row["symbol"] != "BTCUSDT"]
    assert sum(int(row["raw_reactions"]) for row in btc_rows) > 0
    assert sum(int(row["measured_count"]) for row in btc_rows) > 0
    assert sum(int(row["raw_reactions"]) for row in other_rows) == 0


def test_by_year_output_is_unchanged_by_symbol_normalization(tmp_path: Path) -> None:
    qualified_input = tmp_path / "qualified.csv"
    bare_input = tmp_path / "bare.csv"
    qualified_output = tmp_path / "qualified_out"
    bare_output = tmp_path / "bare_out"
    base_rows = [
        _reaction(
            symbol="BINANCE_FUT:BTCUSDT",
            ratio=0.45,
            reaction_kind="INTERNAL",
            completion_time=1714521600,
            column_id=1,
        ),
        _reaction(
            symbol="BINANCE_FUT:BTCUSDT",
            ratio=3.0,
            reaction_kind="CONFIRMING",
            completion_time=1714521660,
            candidate_direction="UP",
            column_id=2,
        ),
    ]
    _write_reactions_csv(qualified_input, base_rows)
    _write_reactions_csv(
        bare_input,
        [{**row, "symbol": "BTCUSDT"} for row in base_rows],
    )

    run_audit(reactions_input=qualified_input, output_root=qualified_output)
    run_audit(reactions_input=bare_input, output_root=bare_output)

    assert _read_csv(qualified_output / OUTPUT_BY_YEAR) == _read_csv(
        bare_output / OUTPUT_BY_YEAR
    )


def test_stability_report_uses_normalized_non_zero_symbol_rows(tmp_path: Path) -> None:
    reactions = []
    for index, symbol in enumerate(DEFAULT_SYMBOLS):
        reactions.extend(
            _to_reaction(row)
            for row in _stability_reaction_rows(
                f"BINANCE_FUT:{symbol}", start_time=1_700_000_000 + index * 10_000
            )
        )
    measured = measure_next_confirmed_swings(reactions)
    summary_rows = [
        {
            "bucket": row["bucket"],
            "count": row["measured_count"],
            "continuation_frequency": row["continuation_frequency"],
            "reversal_frequency": row["reversal_frequency"],
        }
        for row in build_symbol_rows(measured, symbols=("BTCUSDT",))
    ]
    symbol_rows = build_symbol_rows(measured)
    year_rows = build_year_rows(measured, years=(2024, 2025, 2026))
    report_path = tmp_path / OUTPUT_STABILITY_REPORT

    write_stability_report(
        report_path,
        summary_rows=summary_rows,
        symbol_rows=symbol_rows,
        year_rows=year_rows,
    )

    report = report_path.read_text(encoding="utf-8")
    assert "Cross-symbol in the requested symbols." in report
    assert "Not proven cross-symbol" not in report
    assert all(
        sum(
            int(row["raw_reactions"])
            for row in symbol_rows
            if row["symbol"] == symbol
        )
        > 0
        for symbol in DEFAULT_SYMBOLS
    )
