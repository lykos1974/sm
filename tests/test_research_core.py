from dataclasses import FrozenInstanceError
import json

import pytest

from research_core import (
    Decision,
    DecisionType,
    Evidence,
    EvidenceSource,
    Hypothesis,
    Knowledge,
    Observation,
    RequiredFieldError,
    StaticEvidenceSource,
    Validation,
    ValidationOutcome,
    deterministic_id,
)


def test_construction():
    observation = Observation("obs_1", "2026-07-09T00:00:00Z", "unit", "BTCUSDT", "1h", {"close": 100})
    evidence = Evidence("ev_1", (observation.id,), 0.75, "high", "repeatable")
    hypothesis = Hypothesis("hyp_1", "Pullbacks can continue", (evidence.id,))
    validation = Validation("val_1", hypothesis.id, "walk_forward", ValidationOutcome.PASS, {"trades": 10})
    decision = Decision("dec_1", DecisionType.ACCEPT, "Promote only after review", (validation.id,))
    knowledge = Knowledge("know_1", "candidate", (evidence.id,), (0.25, 0.75))

    assert observation.payload["close"] == 100
    assert evidence.observation_ids == ("obs_1",)
    assert validation.outcome is ValidationOutcome.PASS
    assert decision.decision_type is DecisionType.ACCEPT
    assert knowledge.confidence_history == (0.25, 0.75)


def test_immutability():
    observation = Observation("obs_1", "2026-07-09T00:00:00Z", "unit", "BTCUSDT", "1h", {"close": 100})

    with pytest.raises(FrozenInstanceError):
        observation.symbol = "ETHUSDT"
    with pytest.raises(TypeError):
        observation.payload["close"] = 101


def test_serialization_roundtrip():
    objects = [
        Observation("obs_1", "2026-07-09T00:00:00Z", "unit", "BTCUSDT", "1h", {"levels": [1, 2]}),
        Evidence("ev_1", ("obs_1",), 0.75, "high", "repeatable"),
        Hypothesis("hyp_1", "Statement", ("ev_1",)),
        Validation("val_1", "hyp_1", "method", ValidationOutcome.INCONCLUSIVE, {"nested": {"a": 1}}),
        Decision("dec_1", DecisionType.DEFER, "Wait", ("val_1",)),
        Knowledge("know_1", "learning", ("ev_1",), (0.1, 0.2)),
    ]

    for obj in objects:
        payload = json.loads(json.dumps(obj.to_dict(), sort_keys=True))
        assert type(obj).from_dict(payload) == obj


def test_equality_and_hashability():
    left = Observation("obs_1", "2026-07-09T00:00:00Z", "unit", "BTCUSDT", "1h", {"a": [1, 2]})
    right = Observation("obs_1", "2026-07-09T00:00:00Z", "unit", "BTCUSDT", "1h", {"a": [1, 2]})
    evidence = Evidence("ev_1", ["obs_1"], 0.5, "medium", "repeatable")

    assert left == right
    assert hash(left) == hash(right)
    assert {left, right} == {left}
    assert evidence in {evidence}


def test_required_field_validation():
    with pytest.raises(RequiredFieldError):
        Observation("", "2026-07-09T00:00:00Z", "unit", "BTCUSDT", "1h", {})
    with pytest.raises(RequiredFieldError):
        Evidence("ev_1", (), 0.5, "medium", "repeatable")
    with pytest.raises(RequiredFieldError):
        Hypothesis("hyp_1", "", ("ev_1",))
    with pytest.raises(RequiredFieldError):
        Validation("val_1", "", "method", ValidationOutcome.FAIL, {})
    with pytest.raises(RequiredFieldError):
        Decision("dec_1", DecisionType.REJECT, "", ("val_1",))
    with pytest.raises(RequiredFieldError):
        Knowledge("know_1", "learning", ("ev_1",), ())


def test_deterministic_id_helper():
    first = deterministic_id("obs", {"b": 2, "a": [1, 2]})
    second = deterministic_id("obs", {"a": [1, 2], "b": 2})
    different = deterministic_id("obs", {"a": [1, 3], "b": 2})

    assert first == second
    assert first.startswith("obs_")
    assert first != different


def test_evidence_source_protocol_accepts_structural_producers():
    class SyntheticEvidenceSource:
        @property
        def source_id(self):
            return "synthetic_source"

        def produce_evidence(self, context):
            return [Evidence("ev_protocol", tuple(context["observation_ids"]), 0.9, "high", "repeatable")]

    source = SyntheticEvidenceSource()
    produced = tuple(source.produce_evidence({"observation_ids": ("obs_1",)}))

    assert isinstance(source, EvidenceSource)
    assert source.source_id == "synthetic_source"
    assert produced == (Evidence("ev_protocol", ("obs_1",), 0.9, "high", "repeatable"),)


def test_evidence_source_protocol_rejects_missing_producer_method():
    class MissingProducerMethod:
        @property
        def source_id(self):
            return "incomplete_source"

    assert not isinstance(MissingProducerMethod(), EvidenceSource)


def test_static_evidence_source_returns_predefined_evidence_unchanged():
    first = Evidence("ev_static_1", ("obs_1",), 0.6, "medium", "repeatable")
    second = Evidence("ev_static_2", ("obs_2",), 0.7, "high", "repeatable")
    source = StaticEvidenceSource("static_source", [first, second])

    produced = tuple(source.produce_evidence({"ignored": "context"}))

    assert isinstance(source, EvidenceSource)
    assert source.source_id == "static_source"
    assert produced == (first, second)
    assert produced[0] is first
    assert produced[1] is second


def test_static_evidence_source_is_deterministic_for_iterators_and_contexts():
    evidence = [Evidence("ev_static_1", ("obs_1",), 0.6, "medium", "repeatable")]
    source = StaticEvidenceSource("static_source", iter(evidence))

    first_call = tuple(source.produce_evidence({"observation_ids": ("obs_1",)}))
    second_call = tuple(source.produce_evidence({"observation_ids": ("different",)}))

    assert first_call == second_call == tuple(evidence)


def test_static_evidence_source_rejects_missing_source_id():
    evidence = [Evidence("ev_static_1", ("obs_1",), 0.6, "medium", "repeatable")]

    with pytest.raises(RequiredFieldError):
        StaticEvidenceSource("", evidence)
