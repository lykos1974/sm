from dataclasses import FrozenInstanceError

import pytest

from research_core import (
    ConflictingEvidenceError,
    Evidence,
    EvidenceDeduplicationResult,
    EvidenceDeduplicator,
)


def ev(evidence_id, observation_id=None, confidence=0.5):
    return Evidence(
        evidence_id,
        (observation_id or f"obs_{evidence_id}",),
        confidence,
        "medium",
        "repeatable",
    )


def test_no_duplicates_returns_all_evidence_with_counts():
    first = ev("ev_1")
    second = ev("ev_2")

    result = EvidenceDeduplicator().deduplicate([first, second])

    assert result == EvidenceDeduplicationResult((first, second), 0, (), 2)


def test_repeated_equal_evidence_keeps_first_and_records_duplicate_count():
    first = ev("ev_1")
    duplicate = ev("ev_1")

    result = EvidenceDeduplicator().deduplicate([first, duplicate, duplicate])

    assert result.unique_evidence == (first,)
    assert result.duplicate_count == 2
    assert result.duplicate_ids == ("ev_1", "ev_1")
    assert result.input_count == 3


def test_first_seen_order_is_preserved():
    first = ev("ev_2")
    second = ev("ev_1")
    duplicate_first = ev("ev_2")
    third = ev("ev_3")

    result = EvidenceDeduplicator().deduplicate([first, second, duplicate_first, third])

    assert result.unique_evidence == (first, second, third)


def test_original_evidence_object_identity_is_preserved():
    first = ev("ev_1")
    duplicate = ev("ev_1")

    result = EvidenceDeduplicator().deduplicate([first, duplicate])

    assert result.unique_evidence[0] is first
    assert result.unique_evidence[0] is not duplicate


def test_conflicting_same_id_evidence_fails_closed():
    first = ev("ev_1", confidence=0.5)
    conflicting = ev("ev_1", confidence=0.7)

    with pytest.raises(ConflictingEvidenceError):
        EvidenceDeduplicator().deduplicate([first, conflicting])


def test_generator_input_is_consumed_deterministically():
    first = ev("ev_1")
    second = ev("ev_2")

    result = EvidenceDeduplicator().deduplicate(item for item in [first, second, ev("ev_1")])

    assert result.unique_evidence == (first, second)
    assert result.duplicate_count == 1
    assert result.duplicate_ids == ("ev_1",)
    assert result.input_count == 3


def test_empty_input_returns_empty_result():
    result = EvidenceDeduplicator().deduplicate([])

    assert result == EvidenceDeduplicationResult((), 0, (), 0)


def test_result_is_immutable():
    result = EvidenceDeduplicator().deduplicate([ev("ev_1")])

    with pytest.raises(FrozenInstanceError):
        result.duplicate_count = 99
    with pytest.raises(TypeError):
        result.unique_evidence[0] = ev("ev_2")


def test_package_imports_public_deduplicator_types():
    from research_core import (  # noqa: PLC0415
        ConflictingEvidenceError as ImportedError,
        EvidenceDeduplicationResult as ImportedResult,
        EvidenceDeduplicator as ImportedDeduplicator,
    )

    assert ImportedError is ConflictingEvidenceError
    assert ImportedResult is EvidenceDeduplicationResult
    assert ImportedDeduplicator is EvidenceDeduplicator
