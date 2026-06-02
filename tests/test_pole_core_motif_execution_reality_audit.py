from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_core_motif_entry_timing_audit import Candle, EntryTimingObservation
from research_v2.patterns.pole_core_motif_execution_reality_audit import (
    ALLOWED_VERDICTS,
    COMBINED,
    OUTPUT_NAMES,
    PRIMARY_TARGET_R,
    _build_opportunities,
    _classifications_for_observations,
    _concentration_stats,
    _edge_verdict,
    _opportunity_expectancy_rows,
    _overlap_rows,
)


def _obs(symbol: str, row_number: int, ts: int, entry: float = 100.0, stop: float = 97.0) -> EntryTimingObservation:
    return EntryTimingObservation(
        symbol=symbol,
        row_number=row_number,
        direction="LONG",
        entry_candidate="NEXT_COLUMN_OPEN_ENTRY",
        pole_idx=0,
        reversal_idx=1,
        confirmation_idx=2,
        box_size=1.0,
        entry=entry,
        stop=stop,
        observable_entry_ts=ts,
        replay_includes_anchor=True,
        candles_in_replay=1,
        geometry_status="OBSERVABLE",
        geometry_details="synthetic",
    )


def _lookup(observations: list[EntryTimingObservation], high: float = 108.0, low: float = 99.0):
    candles = {}
    for symbol in {row.symbol for row in observations}:
        first = next(row for row in observations if row.symbol == symbol)
        candles[symbol] = [Candle(ts=first.observable_entry_ts or 0, open=first.entry or 0, high=high, low=low, close=first.entry or 0)]
    return _classifications_for_observations(observations, candles)


def test_independent_opportunities_remain_one_to_one() -> None:
    observations = [_obs("BTC", 2, 100), _obs("BTC", 3, 200, entry=101, stop=98)]
    opportunities = _build_opportunities(observations)
    assert len(opportunities) == 2
    assert [len(opp.observations) for opp in opportunities] == [1, 1]


def test_clustered_observations_collapse_to_one_opportunity() -> None:
    observations = [_obs("BTC", 2, 100), _obs("BTC", 3, 100)]
    opportunities = _build_opportunities(observations)
    assert len(opportunities) == 1
    assert opportunities[0].representative.row_number == 2
    assert [row.row_number for row in opportunities[0].observations] == [2, 3]


def test_overlap_detection_reports_same_timestamp_across_symbols() -> None:
    opportunities = _build_opportunities([_obs("BTC", 2, 100), _obs("ETH", 2, 100), _obs("SOL", 2, 200)])
    rows = _overlap_rows(opportunities)
    assert rows == [{
        "observable_entry_ts": 100,
        "observable_entry_time_utc": "1970-01-01T00:01:40+00:00",
        "symbol_count": 2,
        "opportunity_count": 2,
        "symbols": "BTC;ETH",
        "opportunity_ids": "OPP-000001;OPP-000002",
    }]


def test_opportunity_level_expectancy_uses_one_representative_per_cluster() -> None:
    observations = [_obs("BTC", 2, 100), _obs("BTC", 3, 100), _obs("ETH", 2, 200)]
    opportunities = _build_opportunities(observations)
    target_lookup = _lookup(observations, high=108.0, low=99.0)
    rows = _opportunity_expectancy_rows("COMBINED", COMBINED, opportunities, target_lookup)
    two_r = next(row for row in rows if row["r_target"] == 2.0)
    assert two_r["opportunities"] == 2
    assert two_r["resolved"] == 2
    assert two_r["win_rate"] == 1.0
    assert two_r["expected_R"] == 2.0


def test_concentration_risk_calculates_top_decile_and_quintile_share() -> None:
    observations = [_obs("BTC", 2, 100), _obs("ETH", 2, 200), _obs("SOL", 2, 300)]
    opportunities = _build_opportunities(observations)
    candles = {
        "BTC": [Candle(100, 100, 108.0, 99.0, 100)],
        "ETH": [Candle(200, 100, 108.0, 99.0, 100)],
        "SOL": [Candle(300, 100, 101.0, 96.0, 100)],
    }
    target_lookup = _classifications_for_observations(observations, candles)
    stats = _concentration_stats(opportunities, target_lookup, PRIMARY_TARGET_R)
    assert stats["resolved_opportunities"] == 3
    assert stats["total_realized_R"] == 4.0
    assert stats["top_10_share"] == 0.625
    assert stats["top_20_share"] == 0.625


def test_edge_survival_verdicts_are_allowed_and_never_promote() -> None:
    assert "PROMOTE" not in ALLOWED_VERDICTS
    assert _edge_verdict(1.65, 1.60, 20)[0] == "EDGE_SURVIVES"
    assert _edge_verdict(1.65, 1.00, 20)[0] == "EDGE_WEAKENS"
    assert _edge_verdict(1.65, 0.0, 20)[0] == "EDGE_COLLAPSES"
    assert _edge_verdict(1.65, 1.60, 2)[0] == "INSUFFICIENT_DATA"


def _write_fixture(tmp_path: Path, symbol: str, labels: str, columns: str, candles: str) -> tuple[Path, Path, Path]:
    root = tmp_path / symbol
    root.mkdir()
    label_path = root / "labels.csv"
    column_path = root / "columns.csv"
    candle_path = root / "candles.csv"
    label_path.write_text(labels)
    column_path.write_text(columns)
    candle_path.write_text(candles)
    return label_path, column_path, candle_path


def test_cli_outputs_execution_reality_files_and_preserves_production_isolation(tmp_path: Path) -> None:
    labels = "pattern_name,pole_column_index,reversal_column_index,opposing_pole_distance_columns,enhanced_by_opposing_pole\nLOW_POLE,0,1,3,false\nLOW_POLE,0,1,3,false\n"
    columns = "idx,kind,top,bottom,start_ts,end_ts,profile_name\n0,O,100,95,1,10,TEST_bs1_rev3\n1,X,100,96,11,20,TEST_bs1_rev3\n2,O,99,97,21,30,TEST_bs1_rev3\n"
    candles = "close_time,open,high,low,close\n22,100,108,99,100\n"
    fixture = _write_fixture(tmp_path, "BTC", labels, columns, candles)
    output = tmp_path / "output"
    args = ["--symbol-input", f"BTC={fixture[0]}", "--columns-input", f"BTC={fixture[1]}", "--candles-input", f"BTC={fixture[2]}", "--output-root", str(output)]

    subprocess.run([sys.executable, "-m", "research_v2.patterns.pole_core_motif_execution_reality_audit", *args], check=True)

    assert {path.name for path in output.iterdir()} == set(OUTPUT_NAMES)
    opportunities = list(csv.DictReader((output / "execution_reality_opportunity_breakdown.csv").open()))
    assert len(opportunities) == 1
    assert opportunities[0]["cluster_size"] == "2"
    flags = list(csv.DictReader((output / "execution_reality_flags.csv").open()))
    assert any(row["flag"] == "PRODUCTION_ISOLATION" for row in flags)
    manifest = json.loads((output / "execution_reality_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["execution_model_construction"] is False
    assert manifest["optimization_performed"] is False
    assert manifest["allowed_verdicts"] == list(ALLOWED_VERDICTS)
    assert "PROMOTE" not in manifest["allowed_verdicts"]
    module_text = Path("research_v2/patterns/pole_core_motif_execution_reality_audit.py").read_text()
    assert "evaluate_pullback_retest" not in module_text
