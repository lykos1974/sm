import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from research_v2.patterns.pnf_abcd_candle_reality_sim_v1 import (
    STRUCTURAL_BOX_MODE,
    UNKNOWN_ORDERING,
    _classify_structural,
    _summarize,
)


def test_structural_mode_target_first_uses_box_threshold_without_prices():
    risk_boxes = 0.382 * 13
    row = {"continuation_boxes_after_retrace": str(risk_boxes)}

    classification, first_event, detail = _classify_structural(row, risk_boxes, 1)

    assert classification == "TARGET_FIRST"
    assert first_event == ""
    assert "no candle-ordering claim" in detail


def test_structural_mode_unknown_ordering_when_adverse_unavailable():
    risk_boxes = 0.382 * 13
    row = {"continuation_boxes_after_retrace": "1"}

    classification, first_event, detail = _classify_structural(row, risk_boxes, 1)

    assert classification == UNKNOWN_ORDERING
    assert first_event == ""
    assert "adverse_after_entry_boxes unavailable" in detail


def test_structural_summary_counts_measured_and_unknown_ordering_separately():
    rows = [
        {
            "simulation_mode": STRUCTURAL_BOX_MODE,
            "target_1R_classification": "TARGET_FIRST",
            "target_2R_classification": UNKNOWN_ORDERING,
            "target_3R_classification": UNKNOWN_ORDERING,
        }
    ]

    summary = _summarize(rows)

    assert summary["simulation_mode"] == STRUCTURAL_BOX_MODE
    assert summary["measured_candidates"] == 1
    assert summary["unknown_ordering_count"] == 1
    assert summary["unknown_missing_entry_context_count"] == 0
