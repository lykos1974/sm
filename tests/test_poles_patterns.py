from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
PNF_DIR = ROOT / "pnf_mvp"
if str(PNF_DIR) not in sys.path:
    sys.path.insert(0, str(PNF_DIR))

from pnf_engine import PnFColumn, PnFEngine, PnFProfile
from patterns.poles import detect_pole_patterns


def test_box_count_uses_actual_pnf_boxes_not_price_distance():
    cols = [
        PnFColumn(0, "X", 100, 96, 0, 1),
        PnFColumn(1, "O", 99, 97, 2, 3),
        PnFColumn(2, "X", 106, 100, 4, 5),
        PnFColumn(3, "O", 105, 102, 6, 7),
    ]
    out = detect_pole_patterns(cols, box_size=1)
    assert out[0]["pole_boxes"] == 7
    assert out[0]["retrace_boxes"] == 4


def test_detects_high_pole_early_50_retrace():
    cols = [
        PnFColumn(0, "X", 100, 96, 0, 1),
        PnFColumn(1, "O", 99, 97, 2, 3),
        PnFColumn(2, "X", 106, 100, 4, 5),
        PnFColumn(3, "O", 105, 102, 6, 7),
    ]
    out = detect_pole_patterns(cols, box_size=1)
    assert len(out) == 1
    assert out[0]["pattern_name"] == "HIGH_POLE"
    assert out[0]["status"] == "EARLY_50_RETRACE"


def test_detects_high_pole_overretrace_classification():
    cols = [
        PnFColumn(0, "X", 100, 96, 0, 1),
        PnFColumn(1, "O", 99, 97, 2, 3),
        PnFColumn(2, "X", 106, 100, 4, 5),
        PnFColumn(3, "O", 106, 98, 6, 7),
    ]
    out = detect_pole_patterns(cols, box_size=1)
    assert out[0]["status"] == "OVERRETRACE_POLE"


def test_detects_low_pole_early_50_retrace():
    cols = [
        PnFColumn(0, "O", 100, 96, 0, 1),
        PnFColumn(1, "X", 99, 97, 2, 3),
        PnFColumn(2, "O", 95, 89, 4, 5),
        PnFColumn(3, "X", 93, 90, 6, 7),
    ]
    out = detect_pole_patterns(cols, box_size=1)
    assert len(out) == 1
    assert out[0]["pattern_name"] == "LOW_POLE"
    assert out[0]["status"] == "EARLY_50_RETRACE"


def test_detects_low_pole_overretrace_classification():
    cols = [
        PnFColumn(0, "O", 100, 96, 0, 1),
        PnFColumn(1, "X", 99, 97, 2, 3),
        PnFColumn(2, "O", 95, 89, 4, 5),
        PnFColumn(3, "X", 98, 89, 6, 7),
    ]
    out = detect_pole_patterns(cols, box_size=1)
    assert out[0]["status"] == "OVERRETRACE_POLE"


def test_rejects_pole_height_less_or_equal_5_boxes():
    cols = [
        PnFColumn(0, "X", 100, 96, 0, 1),
        PnFColumn(1, "O", 99, 97, 2, 3),
        PnFColumn(2, "X", 106, 102, 4, 5),
        PnFColumn(3, "O", 105, 102, 6, 7),
    ]
    assert detect_pole_patterns(cols, box_size=1) == []


def test_rejects_breakout_excess_under_3_boxes():
    cols = [
        PnFColumn(0, "X", 100, 96, 0, 1),
        PnFColumn(1, "O", 99, 97, 2, 3),
        PnFColumn(2, "X", 102, 96, 4, 5),
        PnFColumn(3, "O", 101, 98, 6, 7),
    ]
    assert detect_pole_patterns(cols, box_size=1) == []


def test_rejects_non_adjacent_reversal_column():
    cols = [
        PnFColumn(0, "X", 100, 96, 0, 1),
        PnFColumn(1, "O", 99, 97, 2, 3),
        PnFColumn(2, "X", 106, 100, 4, 5),
        PnFColumn(4, "O", 105, 102, 6, 7),
    ]
    assert detect_pole_patterns(cols, box_size=1) == []


def test_detects_high_to_low_opposing_poles_with_enhancement():
    cols = [
        PnFColumn(0, "X", 100, 96, 0, 1),
        PnFColumn(1, "O", 99, 97, 2, 3),
        PnFColumn(2, "X", 106, 100, 4, 5),
        PnFColumn(3, "O", 105, 102, 6, 7),
        PnFColumn(4, "X", 104, 101, 8, 9),
        PnFColumn(5, "O", 100, 93, 10, 11),
        PnFColumn(6, "X", 98, 94, 12, 13),
    ]
    out = detect_pole_patterns(cols, box_size=1, max_opposing_distance_columns=4)
    assert len(out) == 2
    assert out[0]["pattern_name"] == "HIGH_POLE"
    assert out[0]["opposing_pole_role"] == "FIRST_POLE"
    assert out[1]["pattern_name"] == "LOW_POLE"
    assert out[1]["opposing_pole_role"] == "SECOND_POLE"
    assert out[1]["enhanced_by_opposing_pole"] is True


def test_detects_low_to_high_opposing_poles_with_enhancement():
    cols = [
        PnFColumn(0, "O", 100, 96, 0, 1),
        PnFColumn(1, "X", 99, 97, 2, 3),
        PnFColumn(2, "O", 95, 89, 4, 5),
        PnFColumn(3, "X", 93, 90, 6, 7),
        PnFColumn(4, "O", 92, 90, 8, 9),
        PnFColumn(5, "X", 99, 93, 10, 11),
        PnFColumn(6, "O", 98, 95, 12, 13),
    ]
    out = detect_pole_patterns(cols, box_size=1, max_opposing_distance_columns=4)
    assert len(out) == 2
    assert out[0]["pattern_name"] == "LOW_POLE"
    assert out[1]["pattern_name"] == "HIGH_POLE"
    assert out[1]["opposing_pole_role"] == "SECOND_POLE"
    assert out[1]["enhanced_by_opposing_pole"] is True


def test_does_not_change_existing_double_top_bottom_behavior():
    profile = PnFProfile(name="t", box_size=1, reversal_boxes=3)
    engine = PnFEngine(profile)
    engine.columns = [
        PnFColumn(0, "X", 100, 98, 0, 1),
        PnFColumn(1, "O", 99, 97, 2, 3),
        PnFColumn(2, "X", 101, 98, 4, 5),
    ]
    assert engine.latest_signal_name() == "BUY"


def test_audit_load_columns_has_friendly_missing_csv_error():
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.patterns.audit_poles",
            "--input-columns-csv",
            "/tmp/does-not-exist-columns.csv",
            "--output-root",
            "/tmp",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert (
        "Input columns CSV not found. Generate it first with research_v2.patterns.export_pnf_columns."
        in result.stderr
    )
