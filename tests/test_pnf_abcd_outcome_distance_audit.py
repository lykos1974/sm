from types import SimpleNamespace

from research_v2.patterns.pnf_abcd_outcome_distance_audit import summarize


def test_distance_summary_percentages_and_quantiles():
    rows = [
        SimpleNamespace(cohort="OTHER", column_distance=1, time_distance_ms=1000),
        SimpleNamespace(cohort="OTHER", column_distance=2, time_distance_ms=2000),
        SimpleNamespace(cohort="OTHER", column_distance=4, time_distance_ms=4000),
        SimpleNamespace(cohort="SYM_0_90_1_10", column_distance=1, time_distance_ms=3000),
    ]

    summary = summarize(rows, "OTHER", "all")

    assert summary["count"] == 3
    assert summary["median_column_distance"] == "2"
    assert summary["avg_column_distance"] == "2.3333333333"
    assert summary["pct_column_distance_1"] == "0.3333333333"
    assert summary["pct_column_distance_lte_2"] == "0.6666666667"
    assert summary["pct_column_distance_lte_3"] == "0.6666666667"
    assert summary["median_time_distance_ms"] == "2000"
