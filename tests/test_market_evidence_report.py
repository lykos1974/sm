from __future__ import annotations

import json
from dataclasses import FrozenInstanceError

import pytest

from research_core.evidence import Evidence
from research_core.evidence_deduplicator import ConflictingEvidenceError
from research_core.evidence_pipeline import (
    EvidencePipelineStatus,
    EvidenceSourceExecutionStatus,
)
from research_core.experiments import market_evidence_report as report_module
from research_core.experiments.market_evidence_report import (
    build_market_evidence_report,
)


class FundingProvider:
    def __init__(self, observations):
        self.observations = observations
        self.contexts = []

    def fetch_funding_rates(self, context):
        self.contexts.append(context)
        return self.observations


class OpenInterestProvider:
    def __init__(self, observations):
        self.observations = observations

    def fetch_open_interest(self, context):
        return self.observations


class LiquidationProvider:
    def __init__(self, observations):
        self.observations = observations

    def fetch_liquidations(self, context):
        return self.observations


class FailingOpenInterestProvider:
    def fetch_open_interest(self, context):
        raise RuntimeError("provider unavailable")


class DummySource:
    def __init__(self, source_id, evidence=(), exc=None):
        self._source_id = source_id
        self._evidence = tuple(evidence)
        self._exc = exc

    @property
    def source_id(self):
        return self._source_id

    def produce_evidence(self, context):
        if self._exc is not None:
            raise self._exc
        return self._evidence


def funding_obs(observation_id="fr-1", rate="0.005"):
    return {"observation_id": observation_id, "funding_rate": rate}


def open_interest_obs(observation_id="oi-1", change="0.05"):
    return {"observation_id": observation_id, "open_interest_change": change}


def liquidation_obs(observation_id="liq-1", size="50000"):
    return {
        "observation_id": observation_id,
        "side": "long",
        "size": size,
        "price": "100",
        "timestamp": "2026-07-14T00:00:00Z",
        "symbol": "BTCUSDT",
        "exchange": "fixture",
    }


def build_report(funding, open_interest, liquidation):
    return build_market_evidence_report(
        funding_rate_provider=FundingProvider(funding),
        open_interest_provider=OpenInterestProvider(open_interest),
        liquidation_provider=LiquidationProvider(liquidation),
        context={"symbol": "BTCUSDT"},
    )


def test_all_three_sources_produce_evidence():
    report = build_report([funding_obs()], [open_interest_obs()], [liquidation_obs()])

    assert report.status == EvidencePipelineStatus.SUCCESS
    assert [snapshot.source_id for snapshot in report.source_statuses] == [
        "funding_rate",
        "open_interest",
        "liquidation",
    ]
    assert [snapshot.status for snapshot in report.source_statuses] == [
        EvidenceSourceExecutionStatus.SUCCESS,
        EvidenceSourceExecutionStatus.SUCCESS,
        EvidenceSourceExecutionStatus.SUCCESS,
    ]
    assert report.raw_evidence_count == 3
    assert report.unique_evidence_count == 3
    assert report.duplicate_count == 0
    assert set(report.confidence_by_source_id) == {
        "funding_rate",
        "open_interest",
        "liquidation",
    }
    assert tuple(e.observation_ids[0] for e in report.evidence) == (
        "fr-1",
        "oi-1",
        "liq-1",
    )


def test_one_source_returns_no_evidence():
    report = build_report([funding_obs()], [], [liquidation_obs()])

    assert report.status == EvidencePipelineStatus.SUCCESS
    assert [snapshot.evidence_count for snapshot in report.source_statuses] == [1, 0, 1]
    assert report.raw_evidence_count == 2
    assert report.unique_evidence_count == 2
    assert "open_interest" not in report.confidence_by_source_id


def test_equal_duplicate_evidence_is_deduplicated(monkeypatch):
    duplicate = Evidence("same-id", ("obs-1",), 0.25, "fixture", "snapshot")
    monkeypatch.setattr(
        report_module,
        "FundingRateEvidenceSource",
        lambda source_id, provider: DummySource(source_id, [duplicate]),
    )
    monkeypatch.setattr(
        report_module,
        "OpenInterestEvidenceSource",
        lambda source_id, provider: DummySource(source_id, [duplicate]),
    )
    monkeypatch.setattr(
        report_module,
        "LiquidationEvidenceSource",
        lambda source_id, provider: DummySource(source_id, []),
    )

    report = build_market_evidence_report(
        funding_rate_provider=object(),
        open_interest_provider=object(),
        liquidation_provider=object(),
        context={},
    )

    assert report.raw_evidence_count == 2
    assert report.unique_evidence_count == 1
    assert report.duplicate_count == 1
    assert report.duplicate_ids == ("same-id",)
    assert report.evidence == (duplicate,)
    assert report.confidence_by_source_id == {"funding_rate": 0.25}


def test_conflicting_duplicate_ids_fail_closed(monkeypatch):
    first = Evidence("same-id", ("obs-1",), 0.25, "fixture", "snapshot")
    second = Evidence("same-id", ("obs-2",), 0.25, "fixture", "snapshot")
    monkeypatch.setattr(
        report_module,
        "FundingRateEvidenceSource",
        lambda source_id, provider: DummySource(source_id, [first]),
    )
    monkeypatch.setattr(
        report_module,
        "OpenInterestEvidenceSource",
        lambda source_id, provider: DummySource(source_id, [second]),
    )
    monkeypatch.setattr(
        report_module,
        "LiquidationEvidenceSource",
        lambda source_id, provider: DummySource(source_id, []),
    )

    with pytest.raises(ConflictingEvidenceError):
        build_market_evidence_report(
            funding_rate_provider=object(),
            open_interest_provider=object(),
            liquidation_provider=object(),
            context={},
        )


def test_provider_failure_is_represented_like_pipeline_behavior():
    report = build_market_evidence_report(
        funding_rate_provider=FundingProvider([funding_obs()]),
        open_interest_provider=FailingOpenInterestProvider(),
        liquidation_provider=LiquidationProvider([liquidation_obs()]),
        context={},
    )

    assert report.status == EvidencePipelineStatus.PARTIAL_SUCCESS
    assert [snapshot.status for snapshot in report.source_statuses] == [
        EvidenceSourceExecutionStatus.SUCCESS,
        EvidenceSourceExecutionStatus.FAILED,
        EvidenceSourceExecutionStatus.SUCCESS,
    ]
    failure = report.source_statuses[1]
    assert failure.evidence_count == 0
    assert failure.error_type == "RuntimeError"
    assert failure.error_message == "provider unavailable"
    assert report.raw_evidence_count == 2


def test_source_order_and_evidence_identity_are_preserved(monkeypatch):
    first = Evidence("first", ("obs-1",), 0.1, "fixture", "snapshot")
    second = Evidence("second", ("obs-2",), 0.2, "fixture", "snapshot")
    third = Evidence("third", ("obs-3",), 0.3, "fixture", "snapshot")
    monkeypatch.setattr(
        report_module,
        "FundingRateEvidenceSource",
        lambda source_id, provider: DummySource(source_id, [first]),
    )
    monkeypatch.setattr(
        report_module,
        "OpenInterestEvidenceSource",
        lambda source_id, provider: DummySource(source_id, [second]),
    )
    monkeypatch.setattr(
        report_module,
        "LiquidationEvidenceSource",
        lambda source_id, provider: DummySource(source_id, [third]),
    )

    report = build_market_evidence_report(
        funding_rate_provider=object(),
        open_interest_provider=object(),
        liquidation_provider=object(),
        context={},
    )

    assert report.evidence == (first, second, third)
    assert report.evidence[0] is first
    assert report.evidence[1] is second
    assert report.evidence[2] is third
    with pytest.raises(FrozenInstanceError):
        report.raw_evidence_count = 99


def test_deterministic_json_output():
    first = build_report([funding_obs()], [open_interest_obs()], [liquidation_obs()])
    second = build_report([funding_obs()], [open_interest_obs()], [liquidation_obs()])

    assert first.to_json() == second.to_json()
    decoded = json.loads(first.to_json())
    assert decoded["raw_evidence_count"] == 3
    assert decoded["source_statuses"][0] == {
        "source_id": "funding_rate",
        "status": "success",
        "evidence_count": 1,
        "error_type": None,
        "error_message": None,
    }


def test_empty_combined_result():
    report = build_report([], [], [])

    assert report.status == EvidencePipelineStatus.SUCCESS
    assert report.raw_evidence_count == 0
    assert report.unique_evidence_count == 0
    assert report.duplicate_count == 0
    assert report.duplicate_ids == ()
    assert report.confidence_summary.evidence_count == 0
    assert report.confidence_by_source_id == {}
    assert report.evidence == ()
