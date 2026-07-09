from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from time import perf_counter
from typing import Any, Iterable, Mapping

from .evidence import Evidence
from .evidence_source import EvidenceSource
from .exceptions import RequiredFieldError


class EvidencePipelineStatus(str, Enum):
    """Terminal status for an EvidencePipeline execution."""

    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"


class EvidenceSourceExecutionStatus(str, Enum):
    """Terminal status for one EvidenceSource execution."""

    SUCCESS = "success"
    FAILED = "failed"


@dataclass(frozen=True)
class EvidenceSourceSnapshot:
    """Immutable audit record for one evidence source execution."""

    source_id: str
    status: EvidenceSourceExecutionStatus
    evidence_count: int
    duration_seconds: float
    error_type: str | None = None
    error_message: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.source_id, str) or not self.source_id:
            raise RequiredFieldError("source_id is required")
        object.__setattr__(self, "status", EvidenceSourceExecutionStatus(self.status))
        if not isinstance(self.evidence_count, int) or self.evidence_count < 0:
            raise RequiredFieldError("evidence_count is required")
        if (
            not isinstance(self.duration_seconds, (int, float))
            or self.duration_seconds < 0
        ):
            raise RequiredFieldError("duration_seconds is required")


@dataclass(frozen=True)
class EvidencePipelineResult:
    """Immutable result returned by one pipeline execution."""

    evidence: tuple[Evidence, ...]
    snapshots: tuple[EvidenceSourceSnapshot, ...]
    status: EvidencePipelineStatus
    total_duration_seconds: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence", tuple(self.evidence))
        object.__setattr__(self, "snapshots", tuple(self.snapshots))
        object.__setattr__(self, "status", EvidencePipelineStatus(self.status))
        if (
            not isinstance(self.total_duration_seconds, (int, float))
            or self.total_duration_seconds < 0
        ):
            raise RequiredFieldError("total_duration_seconds is required")


@dataclass(frozen=True)
class EvidencePipeline:
    """Sequential, deterministic coordinator for EvidenceSource objects.

    The pipeline snapshots its ordered source iterable at construction time,
    executes sources sequentially, and concatenates produced Evidence without
    deduplication or trading-specific interpretation.
    """

    _sources: tuple[EvidenceSource, ...]
    continue_on_error: bool = False

    def __init__(
        self, sources: Iterable[EvidenceSource], continue_on_error: bool = False
    ) -> None:
        object.__setattr__(self, "_sources", tuple(sources))
        object.__setattr__(self, "continue_on_error", bool(continue_on_error))

    @property
    def sources(self) -> tuple[EvidenceSource, ...]:
        """Evidence sources in deterministic execution order."""
        return self._sources

    def run(self, context: Mapping[str, Any] | None = None) -> EvidencePipelineResult:
        """Execute all configured sources and return a single immutable result."""
        execution_context: Mapping[str, Any] = {} if context is None else context
        pipeline_started_at = perf_counter()
        evidence: list[Evidence] = []
        snapshots: list[EvidenceSourceSnapshot] = []
        failure_count = 0
        success_count = 0

        for source in self._sources:
            source_started_at = perf_counter()
            try:
                source_evidence = tuple(source.produce_evidence(execution_context))
            except Exception as exc:
                failure_count += 1
                snapshots.append(
                    EvidenceSourceSnapshot(
                        source_id=source.source_id,
                        status=EvidenceSourceExecutionStatus.FAILED,
                        evidence_count=0,
                        duration_seconds=perf_counter() - source_started_at,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                )
                if not self.continue_on_error:
                    break
                continue

            success_count += 1
            evidence.extend(source_evidence)
            snapshots.append(
                EvidenceSourceSnapshot(
                    source_id=source.source_id,
                    status=EvidenceSourceExecutionStatus.SUCCESS,
                    evidence_count=len(source_evidence),
                    duration_seconds=perf_counter() - source_started_at,
                )
            )

        if failure_count == 0:
            status = EvidencePipelineStatus.SUCCESS
        elif success_count > 0 and self.continue_on_error:
            status = EvidencePipelineStatus.PARTIAL_SUCCESS
        else:
            status = EvidencePipelineStatus.FAILED

        return EvidencePipelineResult(
            evidence=tuple(evidence),
            snapshots=tuple(snapshots),
            status=status,
            total_duration_seconds=perf_counter() - pipeline_started_at,
        )
