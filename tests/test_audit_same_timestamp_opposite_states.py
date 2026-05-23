import csv
from pathlib import Path

from research_v2.optimizers.audit_same_timestamp_opposite_states import audit_same_timestamp_opposite_states


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_same_symbol_grouping_and_opposite_side_detection(tmp_path: Path) -> None:
    rows = [
        {"row_id":"1","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"WATCH","resolution_status":"OPEN","pullback_quality":"DEEP","breakout_context":"POST_BREAKOUT_PULLBACK","continuation_execution_class":"A","strategy":"S1"},
        {"row_id":"2","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"SHORT","status":"WATCH","resolution_status":"OPEN","pullback_quality":"DEEP","breakout_context":"POST_BREAKOUT_PULLBACK","continuation_execution_class":"A","strategy":"S1"},
        {"row_id":"3","symbol":"BTC","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"WATCH","resolution_status":"OPEN","pullback_quality":"DEEP","breakout_context":"POST_BREAKOUT_PULLBACK","continuation_execution_class":"A","strategy":"S1"},
    ]
    data = tmp_path / "labeled.csv"
    out = tmp_path / "out"
    _write_csv(data, rows)

    result = audit_same_timestamp_opposite_states(labeled_dataset_path=str(data), output_root=str(out))
    assert result["opposite_groups"] == 1

    with (out / "same_timestamp_opposite_state_groups.csv").open("r", encoding="utf-8") as h:
        groups = list(csv.DictReader(h))
    assert len(groups) == 1
    assert groups[0]["symbol"] == "ETH"


def test_classification_logic_variants(tmp_path: Path) -> None:
    rows = [
        {"row_id":"1","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"WATCH","resolution_status":"OPEN","pullback_quality":"DEEP","breakout_context":"CTX","continuation_execution_class":"A","strategy":"S"},
        {"row_id":"2","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"SHORT","status":"WATCH","resolution_status":"OPEN","pullback_quality":"DEEP","breakout_context":"CTX","continuation_execution_class":"A","strategy":"S"},
        {"row_id":"3","symbol":"BTC","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"WATCH","resolution_status":"OPEN","pullback_quality":"DEEP","breakout_context":"CTX","continuation_execution_class":"A","strategy":"S"},
        {"row_id":"4","symbol":"BTC","reference_ts":"2024-01-01T00:00:00Z","side":"SHORT","status":"CANDIDATE","resolution_status":"OPEN","pullback_quality":"HEALTHY","breakout_context":"CTX2","continuation_execution_class":"B","strategy":"S2"},
        {"row_id":"5","symbol":"SOL","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"WATCH","resolution_status":"TP2","pullback_quality":"X","breakout_context":"B1","continuation_execution_class":"C","strategy":"S"},
        {"row_id":"6","symbol":"SOL","reference_ts":"2024-01-01T00:00:00Z","side":"SHORT","status":"WATCH","resolution_status":"TP2","pullback_quality":"X","breakout_context":"B1","continuation_execution_class":"C","strategy":"S"},
        {"row_id":"7","symbol":"ADA","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"WATCH","resolution_status":"OPEN","pullback_quality":"X","breakout_context":"B1","continuation_execution_class":"C","strategy":"S"},
        {"row_id":"8","symbol":"ADA","reference_ts":"2024-01-01T00:00:00Z","side":"SHORT","status":"WATCH","resolution_status":"OPEN","pullback_quality":"X","breakout_context":"B1","continuation_execution_class":"C","strategy":"S"},
        {"row_id":"9","symbol":"ADA","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"WATCH","resolution_status":"OPEN","pullback_quality":"X","breakout_context":"B1","continuation_execution_class":"C","strategy":"S"},
    ]
    data = tmp_path / "labeled.csv"
    out = tmp_path / "out"
    _write_csv(data, rows)

    audit_same_timestamp_opposite_states(labeled_dataset_path=str(data), output_root=str(out))

    with (out / "same_timestamp_opposite_state_classification.csv").open("r", encoding="utf-8") as h:
        classified = {(r["symbol"], r["reference_ts"]): r["classification"] for r in csv.DictReader(h)}

    assert classified[("ETH", "2024-01-01T00:00:00Z")] == "PURE_SIDE_FLIP"
    assert classified[("BTC", "2024-01-01T00:00:00Z")] == "SIDE_PLUS_STATUS_TRANSITION"
    assert classified[("SOL", "2024-01-01T00:00:00Z")] == "STRUCTURAL_POLARITY_TRANSITION"
    assert classified[("ADA", "2024-01-01T00:00:00Z")] == "POSSIBLE_DUPLICATE_LABELING"


def test_no_cross_symbol_contamination(tmp_path: Path) -> None:
    rows = [
        {"row_id":"1","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"WATCH","resolution_status":"STOPPED","pullback_quality":"DEEP","breakout_context":"CTX","continuation_execution_class":"A","strategy":"S"},
        {"row_id":"2","symbol":"BTC","reference_ts":"2024-01-01T00:00:00Z","side":"SHORT","status":"WATCH","resolution_status":"TP2","pullback_quality":"DEEP","breakout_context":"CTX","continuation_execution_class":"A","strategy":"S"},
    ]
    data = tmp_path / "labeled.csv"
    out = tmp_path / "out"
    _write_csv(data, rows)

    result = audit_same_timestamp_opposite_states(labeled_dataset_path=str(data), output_root=str(out))
    assert result["opposite_groups"] == 0
