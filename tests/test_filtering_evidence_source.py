from dataclasses import FrozenInstanceError

import pytest

from research_core import Evidence, EvidenceSource, FilteringEvidenceSource, RequiredFieldError, StaticEvidenceSource


class ContextRecordingSource:
    def __init__(self, source_id, evidence):
        self._source_id = source_id
        self._evidence = tuple(evidence)
        self.calls = []

    @property
    def source_id(self):
        return self._source_id

    def produce_evidence(self, context):
        self.calls.append(context)
        return self._evidence


def make_evidence():
    first = Evidence("ev_filter_1", ("obs_1",), 0.6, "medium", "repeatable")
    second = Evidence("ev_filter_2", ("obs_2",), 0.8, "high", "repeatable")
    third = Evidence("ev_filter_3", ("obs_3",), 0.4, "low", "repeatable")
    return first, second, third


def test_filtering_evidence_source_imports_from_package():
    from research_core import FilteringEvidenceSource as ImportedFilteringEvidenceSource

    assert ImportedFilteringEvidenceSource is FilteringEvidenceSource


def test_filtering_evidence_source_implements_evidence_source_protocol():
    source = StaticEvidenceSource("static_source", [])
    filtering_source = FilteringEvidenceSource("filtering_source", source, lambda evidence: True)

    assert isinstance(filtering_source, EvidenceSource)
    assert filtering_source.source_id == "filtering_source"


def test_filtering_evidence_source_filters_in_original_order():
    first, second, third = make_evidence()
    wrapped = StaticEvidenceSource("static_source", [first, second, third])
    source = FilteringEvidenceSource(
        "filtering_source",
        wrapped,
        lambda evidence: evidence.confidence >= 0.5,
    )

    produced = tuple(source.produce_evidence({"ignored": "context"}))

    assert produced == (first, second)


def test_filtering_evidence_source_forwards_exact_context_to_wrapped_source():
    first, second, _ = make_evidence()
    wrapped = ContextRecordingSource("recording_source", [first, second])
    context = {"observation_ids": ("obs_1",)}
    source = FilteringEvidenceSource("filtering_source", wrapped, lambda evidence: True)

    produced = tuple(source.produce_evidence(context))

    assert wrapped.calls == [context]
    assert wrapped.calls[0] is context
    assert produced == (first, second)


def test_filtering_evidence_source_preserves_evidence_object_identity():
    first, second, third = make_evidence()
    wrapped = StaticEvidenceSource("static_source", [first, second, third])
    source = FilteringEvidenceSource("filtering_source", wrapped, lambda evidence: evidence is not second)

    produced = tuple(source.produce_evidence({}))

    assert produced == (first, third)
    assert produced[0] is first
    assert produced[1] is third


def test_filtering_evidence_source_returns_empty_result_when_nothing_matches():
    first, second, third = make_evidence()
    wrapped = StaticEvidenceSource("static_source", [first, second, third])
    source = FilteringEvidenceSource("filtering_source", wrapped, lambda evidence: False)

    assert tuple(source.produce_evidence({})) == ()


def test_filtering_evidence_source_propagates_predicate_exceptions():
    first, second, _ = make_evidence()
    wrapped = StaticEvidenceSource("static_source", [first, second])

    def predicate(evidence):
        if evidence is second:
            raise RuntimeError("predicate failed")
        return True

    source = FilteringEvidenceSource("filtering_source", wrapped, predicate)

    with pytest.raises(RuntimeError, match="predicate failed"):
        source.produce_evidence({})


def test_filtering_evidence_source_rejects_invalid_constructor_values():
    wrapped = StaticEvidenceSource("static_source", [])

    with pytest.raises(RequiredFieldError):
        FilteringEvidenceSource("", wrapped, lambda evidence: True)
    with pytest.raises(RequiredFieldError):
        FilteringEvidenceSource("filtering_source", object(), lambda evidence: True)
    with pytest.raises(RequiredFieldError):
        FilteringEvidenceSource("filtering_source", wrapped, None)


def test_filtering_evidence_source_is_immutable():
    wrapped = StaticEvidenceSource("static_source", [])
    source = FilteringEvidenceSource("filtering_source", wrapped, lambda evidence: True)

    with pytest.raises(FrozenInstanceError):
        source.source_id = "changed_source"
