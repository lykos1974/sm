import csv
from pathlib import Path

from research_v2.optimizers.analyze_structural_state_transitions import analyze_structural_state_transitions


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_same_symbol_only_and_future_only_and_first_occurrence(tmp_path: Path) -> None:
    rows = [
        {"row_id":"s0","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"CANDIDATE","resolution_status":"STOPPED","pullback_quality":"DEEP","strategy":"S"},
        # same timestamp, opposite side (warning check), and counts as future structural row via later index
        {"row_id":"s1","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"SHORT","status":"WATCH","resolution_status":"OPEN","pullback_quality":"N/A","strategy":"S"},
        {"row_id":"s2","symbol":"ETH","reference_ts":"2024-01-01T00:00:10Z","side":"SHORT","status":"WATCH","resolution_status":"OPEN","pullback_quality":"N/A","strategy":"S"},
        {"row_id":"s3","symbol":"ETH","reference_ts":"2024-01-01T00:00:20Z","side":"SHORT","status":"CANDIDATE","resolution_status":"OPEN","pullback_quality":"N/A","strategy":"S"},
        {"row_id":"s4","symbol":"ETH","reference_ts":"2024-01-01T00:00:30Z","side":"SHORT","status":"CANDIDATE","resolution_status":"TP2","pullback_quality":"N/A","strategy":"S"},
        {"row_id":"s5","symbol":"ETH","reference_ts":"2024-01-01T00:00:40Z","side":"SHORT","status":"WATCH","resolution_status":"STOPPED","pullback_quality":"N/A","strategy":"S"},
        {"row_id":"s6","symbol":"ETH","reference_ts":"2024-01-01T00:00:50Z","side":"LONG","status":"WATCH","resolution_status":"OPEN","pullback_quality":"N/A","strategy":"S"},
        {"row_id":"s7","symbol":"ETH","reference_ts":"2024-01-01T00:01:00Z","side":"LONG","status":"CANDIDATE","resolution_status":"TP2","pullback_quality":"N/A","strategy":"S"},
        # other symbol should not be considered
        {"row_id":"x1","symbol":"BTC","reference_ts":"2024-01-01T00:00:05Z","side":"SHORT","status":"CANDIDATE","resolution_status":"TP2","pullback_quality":"N/A","strategy":"S"},
    ]
    data = tmp_path / "labeled.csv"
    out = tmp_path / "out"
    _write_csv(data, rows)

    analyze_structural_state_transitions(labeled_dataset_path=str(data), output_root=str(out), forward_structural_window=20)

    with (out / "structural_state_transition_rows.csv").open("r", encoding="utf-8") as h:
        got = list(csv.DictReader(h))

    assert len(got) == 1
    seed = got[0]
    assert seed["opposite_watch_distance"] == "1"  # same-timestamp later row index counts as structural future row
    assert seed["opposite_candidate_distance"] == "3"
    assert seed["opposite_tp2_distance"] == "4"
    assert seed["opposite_stopped_distance"] == "5"
    assert seed["same_side_watch_distance"] == "6"
    assert seed["same_side_tp2_distance"] == "7"


def test_future_only_no_same_row_and_independent_transition_detection(tmp_path: Path) -> None:
    rows = [
        {"row_id":"a0","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"WATCH","resolution_status":"STOPPED","pullback_quality":"DEEP","strategy":"S"},
        {"row_id":"a1","symbol":"ETH","reference_ts":"2024-01-01T00:00:10Z","side":"SHORT","status":"WATCH","resolution_status":"TP2","pullback_quality":"X","strategy":"S"},
    ]
    data = tmp_path / "labeled.csv"
    out = tmp_path / "out"
    _write_csv(data, rows)

    analyze_structural_state_transitions(labeled_dataset_path=str(data), output_root=str(out), forward_structural_window=5)

    with (out / "structural_state_transition_rows.csv").open("r", encoding="utf-8") as h:
        seed = list(csv.DictReader(h))[0]
    # should not count seed row itself even though it's WATCH
    assert seed["same_side_watch_found"] == "0"
    assert seed["opposite_watch_found"] == "1"
    assert seed["opposite_tp2_found"] == "1"

    with (out / "structural_state_transition_matrix.csv").open("r", encoding="utf-8") as h:
        matrix = {r["to_state"]: float(r["probability"]) for r in csv.DictReader(h)}
    # independent first-occurrence events within window: can exceed 1 when summed
    assert matrix["opposite_watch"] == 1.0
    assert matrix["opposite_tp2"] == 1.0
    assert sum(matrix.values()) > 1.0


def test_same_timestamp_warning_and_split_metrics_exist(tmp_path: Path) -> None:
    rows = []
    for i in range(12):
        rows.append({"row_id":f"l{i}","symbol":"ETH","reference_ts":f"2024-01-01T00:{i:02d}:00Z","side":"LONG","status":"CANDIDATE","resolution_status":"STOPPED","pullback_quality":"DEEP","strategy":"S"})
        rows.append({"row_id":f"s{i}","symbol":"ETH","reference_ts":f"2024-01-01T00:{i:02d}:00Z","side":"SHORT","status":"WATCH","resolution_status":"OPEN","pullback_quality":"X","strategy":"S"})

    data = tmp_path / "labeled.csv"
    out = tmp_path / "out"
    _write_csv(data, rows)

    analyze_structural_state_transitions(labeled_dataset_path=str(data), output_root=str(out), forward_structural_window=1)

    with (out / "structural_state_transition_warnings.csv").open("r", encoding="utf-8") as h:
        warnings = [r["warning_code"] for r in csv.DictReader(h)]
    assert "SAME_TIMESTAMP_OPPOSITE_STATE" in warnings

    with (out / "structural_state_transition_split_metrics.csv").open("r", encoding="utf-8") as h:
        splits = [r["split"] for r in csv.DictReader(h)]
    assert set(splits) == {"train", "validation", "oos"}


def test_exclude_same_timestamp_opposite_toggle(tmp_path: Path) -> None:
    rows = [
        {"row_id":"e0","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"WATCH","resolution_status":"STOPPED","pullback_quality":"DEEP","strategy":"S"},
        {"row_id":"e1","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"SHORT","status":"WATCH","resolution_status":"OPEN","pullback_quality":"X","strategy":"S"},
        {"row_id":"e2","symbol":"ETH","reference_ts":"2024-01-01T00:00:10Z","side":"SHORT","status":"WATCH","resolution_status":"OPEN","pullback_quality":"X","strategy":"S"},
    ]
    data = tmp_path / "labeled.csv"
    out_default = tmp_path / "out_default"
    out_excluded = tmp_path / "out_excluded"
    _write_csv(data, rows)

    analyze_structural_state_transitions(labeled_dataset_path=str(data), output_root=str(out_default), forward_structural_window=5)
    analyze_structural_state_transitions(labeled_dataset_path=str(data), output_root=str(out_excluded), forward_structural_window=5, exclude_same_timestamp_opposite=True)

    default_seed = list(csv.DictReader((out_default / "structural_state_transition_rows.csv").open("r", encoding="utf-8")))[0]
    excluded_seed = list(csv.DictReader((out_excluded / "structural_state_transition_rows.csv").open("r", encoding="utf-8")))[0]
    assert default_seed["opposite_watch_distance"] == "1"
    assert excluded_seed["opposite_watch_distance"] == "2"
