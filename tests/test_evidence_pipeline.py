from dataclasses import FrozenInstanceError

import pytest

from research_core import (
    Evidence,
    EvidencePipeline,
    EvidencePipelineStatus,
    EvidenceSourceExecutionStatus,
)


class RecordingSource:
    def __init__(self, source_id, evidence, calls):
        self._source_id = source_id
        self._evidence = tuple(evidence)
        self._calls = calls

    @property
    def source_id(self):
        return self._source_id

    def produce_evidence(self, context):
        self._calls.append((self.source_id, context))
        return self._evidence


class FailingSource:
    def __init__(self, source_id, calls):
        self._source_id = source_id
        self._calls = calls

    @property
    def source_id(self):
        return self._source_id

    def produce_evidence(self, context):
        self._calls.append((self.source_id, context))
        raise RuntimeError(f"{self.source_id} failed")


def ev(evidence_id):
    return Evidence(evidence_id, (f"obs_{evidence_id}",), 0.5, "medium", "repeatable")


def test_pipeline_executes_sources_sequentially_and_preserves_evidence_order():
    calls = []
    context = {"research_run": "unit"}
    first = ev("first")
    second = ev("second")
    third = ev("third")
    pipeline = EvidencePipeline(
        [
            RecordingSource("source_a", [first, second], calls),
            RecordingSource("source_b", [third], calls),
        ]
    )

    result = pipeline.run(context)

    assert calls == [("source_a", context), ("source_b", context)]
    assert result.evidence == (first, second, third)
    assert [snapshot.source_id for snapshot in result.snapshots] == [
        "source_a",
        "source_b",
    ]
    assert [snapshot.evidence_count for snapshot in result.snapshots] == [2, 1]
    assert all(
        snapshot.status is EvidenceSourceExecutionStatus.SUCCESS
        for snapshot in result.snapshots
    )
    assert result.status is EvidencePipelineStatus.SUCCESS
    assert result.total_duration_seconds >= 0


def test_pipeline_snapshots_source_iterable_at_construction_time():
    calls = []
    first = RecordingSource("source_a", [ev("first")], calls)
    source_iter = iter([first])
    pipeline = EvidencePipeline(source_iter)

    assert pipeline.sources == (first,)
    assert pipeline.run({}).evidence == (ev("first"),)


def test_pipeline_result_and_snapshots_are_immutable():
    pipeline = EvidencePipeline([RecordingSource("source_a", [ev("first")], [])])
    result = pipeline.run({})

    with pytest.raises(FrozenInstanceError):
        result.status = EvidencePipelineStatus.FAILED
    with pytest.raises(FrozenInstanceError):
        result.snapshots[0].evidence_count = 99


def test_pipeline_continues_after_failure_when_configured():
    calls = []
    first = ev("first")
    second = ev("second")
    pipeline = EvidencePipeline(
        [
            RecordingSource("source_a", [first], calls),
            FailingSource("source_b", calls),
            RecordingSource("source_c", [second], calls),
        ],
        continue_on_error=True,
    )

    result = pipeline.run({"mode": "continue"})

    assert calls == [
        ("source_a", {"mode": "continue"}),
        ("source_b", {"mode": "continue"}),
        ("source_c", {"mode": "continue"}),
    ]
    assert result.evidence == (first, second)
    assert [snapshot.source_id for snapshot in result.snapshots] == [
        "source_a",
        "source_b",
        "source_c",
    ]
    assert [snapshot.status for snapshot in result.snapshots] == [
        EvidenceSourceExecutionStatus.SUCCESS,
        EvidenceSourceExecutionStatus.FAILED,
        EvidenceSourceExecutionStatus.SUCCESS,
    ]
    assert result.snapshots[1].evidence_count == 0
    assert result.snapshots[1].error_type == "RuntimeError"
    assert result.snapshots[1].error_message == "source_b failed"
    assert result.status is EvidencePipelineStatus.PARTIAL_SUCCESS


def test_pipeline_stops_immediately_after_failure_by_default():
    calls = []
    pipeline = EvidencePipeline(
        [
            RecordingSource("source_a", [ev("first")], calls),
            FailingSource("source_b", calls),
            RecordingSource("source_c", [ev("second")], calls),
        ]
    )

    result = pipeline.run({})

    assert calls == [("source_a", {}), ("source_b", {})]
    assert result.evidence == (ev("first"),)
    assert [snapshot.source_id for snapshot in result.snapshots] == [
        "source_a",
        "source_b",
    ]
    assert result.snapshots[1].status is EvidenceSourceExecutionStatus.FAILED
    assert result.status is EvidencePipelineStatus.FAILED


def test_pipeline_failure_with_no_success_is_failed_even_when_continuing():
    result = EvidencePipeline([FailingSource("source_a", [])], continue_on_error=True).run({})

    assert result.evidence == ()
    assert result.status is EvidencePipelineStatus.FAILED


def test_pipeline_success_with_zero_evidence_then_failure_is_partial_success():
    result = EvidencePipeline(
        [RecordingSource("source_a", [], []), FailingSource("source_b", [])],
        continue_on_error=True,
    ).run({})

    assert result.evidence == ()
    assert result.status is EvidencePipelineStatus.PARTIAL_SUCCESS
