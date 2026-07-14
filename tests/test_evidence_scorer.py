from dataclasses import FrozenInstanceError

import pytest

from research_core import Evidence, EvidenceScoreResult, EvidenceScorer


def ev(evidence_id, confidence=0.5, source_id="source_a"):
    evidence = Evidence(
        evidence_id,
        (f"obs_{evidence_id}",),
        confidence,
        "medium",
        "repeatable",
    )
    object.__setattr__(evidence, "source_id", source_id)
    return evidence


def test_empty_input_returns_zero_score_result():
    result = EvidenceScorer().score([])

    assert result == EvidenceScoreResult((), 0, 0.0, 0.0, 0.0, 0.0, {})
    assert dict(result.confidence_by_source_id) == {}


def test_single_evidence_scores_confidence_and_preserves_identity():
    evidence = ev("ev_1", 0.75, "source_a")

    result = EvidenceScorer().score([evidence])

    assert result.evidence_count == 1
    assert result.confidence_sum == 0.75
    assert result.confidence_average == 0.75
    assert result.confidence_maximum == 0.75
    assert result.confidence_minimum == 0.75
    assert dict(result.confidence_by_source_id) == {"source_a": 0.75}
    assert result.evidence == (evidence,)
    assert result.evidence[0] is evidence


def test_multiple_evidence_scores_unweighted_confidence_summary():
    first = ev("ev_1", 0.25, "source_a")
    second = ev("ev_2", 0.75, "source_b")
    third = ev("ev_3", 1.0, "source_c")

    result = EvidenceScorer().score([first, second, third])

    assert result.evidence_count == 3
    assert result.confidence_sum == 2.0
    assert result.confidence_average == pytest.approx(2.0 / 3.0)
    assert result.confidence_maximum == 1.0
    assert result.confidence_minimum == 0.25


def test_confidence_is_grouped_by_source_id_in_first_seen_order():
    first = ev("ev_1", 0.25, "source_b")
    second = ev("ev_2", 0.5, "source_a")
    third = ev("ev_3", 0.75, "source_b")

    result = EvidenceScorer().score([first, second, third])

    assert dict(result.confidence_by_source_id) == {
        "source_b": 1.0,
        "source_a": 0.5,
    }
    assert tuple(result.confidence_by_source_id) == ("source_b", "source_a")


def test_deterministic_ordering_preserves_input_evidence_order():
    first = ev("ev_2", 0.2, "source_b")
    second = ev("ev_1", 0.1, "source_a")
    third = ev("ev_3", 0.3, "source_b")

    result = EvidenceScorer().score([first, second, third])

    assert result.evidence == (first, second, third)
    assert tuple(result.confidence_by_source_id) == ("source_b", "source_a")


def test_generator_input_is_consumed_deterministically():
    first = ev("ev_1", 0.4, "source_a")
    second = ev("ev_2", 0.6, "source_b")

    result = EvidenceScorer().score(item for item in [first, second])

    assert result.evidence == (first, second)
    assert result.evidence_count == 2
    assert result.confidence_sum == 1.0


def test_result_is_immutable():
    result = EvidenceScorer().score([ev("ev_1", 0.5, "source_a")])

    with pytest.raises(FrozenInstanceError):
        result.evidence_count = 99
    with pytest.raises(TypeError):
        result.evidence[0] = ev("ev_2")
    with pytest.raises(TypeError):
        result.confidence_by_source_id["source_a"] = 99.0


def test_package_exports_public_scorer_types():
    from research_core import (  # noqa: PLC0415
        EvidenceScoreResult as ImportedResult,
        EvidenceScorer as ImportedScorer,
    )

    assert ImportedResult is EvidenceScoreResult
    assert ImportedScorer is EvidenceScorer
