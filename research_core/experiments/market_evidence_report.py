from __future__ import annotations

import json
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from research_core.evidence import Evidence
from research_core.evidence_deduplicator import EvidenceDeduplicator
from research_core.evidence_pipeline import (
    EvidencePipeline,
    EvidencePipelineStatus,
    EvidenceSourceExecutionStatus,
    EvidenceSourceSnapshot,
)
from research_core.evidence_scorer import EvidenceScorer, EvidenceScoreResult
from research_core.funding_rate_evidence_source import (
    FundingRateEvidenceSource,
    FundingRateProvider,
)
from research_core.liquidation_evidence_source import (
    LiquidationEvidenceSource,
    LiquidationProvider,
)
from research_core.open_interest_evidence_source import (
    OpenInterestEvidenceSource,
    OpenInterestProvider,
)


@dataclass(frozen=True)
class MarketEvidenceReport:
    """Immutable read-only report for one market-evidence workflow run."""

    status: EvidencePipelineStatus
    source_statuses: tuple[EvidenceSourceSnapshot, ...]
    raw_evidence_count: int
    unique_evidence_count: int
    duplicate_count: int
    duplicate_ids: tuple[str, ...]
    confidence_summary: EvidenceScoreResult
    confidence_by_source_id: Mapping[str, float]
    evidence: tuple[Evidence, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", EvidencePipelineStatus(self.status))
        object.__setattr__(self, "source_statuses", tuple(self.source_statuses))
        object.__setattr__(self, "duplicate_ids", tuple(self.duplicate_ids))
        object.__setattr__(self, "evidence", tuple(self.evidence))
        object.__setattr__(
            self,
            "confidence_by_source_id",
            MappingProxyType(dict(self.confidence_by_source_id)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "source_statuses": [
                {
                    "source_id": snapshot.source_id,
                    "status": snapshot.status.value,
                    "evidence_count": snapshot.evidence_count,
                    "error_type": snapshot.error_type,
                    "error_message": snapshot.error_message,
                }
                for snapshot in self.source_statuses
            ],
            "raw_evidence_count": self.raw_evidence_count,
            "unique_evidence_count": self.unique_evidence_count,
            "duplicate_count": self.duplicate_count,
            "duplicate_ids": list(self.duplicate_ids),
            "confidence_summary": {
                "evidence_count": self.confidence_summary.evidence_count,
                "confidence_sum": self.confidence_summary.confidence_sum,
                "confidence_average": self.confidence_summary.confidence_average,
                "confidence_maximum": self.confidence_summary.confidence_maximum,
                "confidence_minimum": self.confidence_summary.confidence_minimum,
            },
            "confidence_by_source_id": dict(self.confidence_by_source_id),
            "evidence": [item.to_dict() for item in self.evidence],
        }

    def to_json(self) -> str:
        """Return deterministic JSON without timing fields."""
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))


def build_market_evidence_report(
    *,
    funding_rate_provider: FundingRateProvider,
    open_interest_provider: OpenInterestProvider,
    liquidation_provider: LiquidationProvider,
    context: Mapping[str, Any],
) -> MarketEvidenceReport:
    """Run the existing market EvidenceSources and summarize their evidence.

    This is read-only: injected providers are queried for market observations,
    evidence is deduplicated and scored, and no trading decisions or exchange
    order calls are made.
    """

    sources = (
        FundingRateEvidenceSource("funding_rate", funding_rate_provider),
        OpenInterestEvidenceSource("open_interest", open_interest_provider),
        LiquidationEvidenceSource("liquidation", liquidation_provider),
    )
    pipeline_result = EvidencePipeline(sources, continue_on_error=True).run(context)
    deduplication = EvidenceDeduplicator().deduplicate(pipeline_result.evidence)
    score = EvidenceScorer().score(deduplication.unique_evidence)
    confidence_by_source_id = _group_confidence_by_source(
        pipeline_result.evidence,
        pipeline_result.snapshots,
        deduplication.unique_evidence,
    )

    return MarketEvidenceReport(
        status=pipeline_result.status,
        source_statuses=pipeline_result.snapshots,
        raw_evidence_count=deduplication.input_count,
        unique_evidence_count=len(deduplication.unique_evidence),
        duplicate_count=deduplication.duplicate_count,
        duplicate_ids=deduplication.duplicate_ids,
        confidence_summary=score,
        confidence_by_source_id=confidence_by_source_id,
        evidence=deduplication.unique_evidence,
    )


def _group_confidence_by_source(
    raw_evidence: tuple[Evidence, ...],
    snapshots: tuple[EvidenceSourceSnapshot, ...],
    unique_evidence: tuple[Evidence, ...],
) -> dict[str, float]:
    source_by_evidence_id: dict[str, str] = {}
    offset = 0
    for snapshot in snapshots:
        if snapshot.status != EvidenceSourceExecutionStatus.SUCCESS:
            continue
        for item in raw_evidence[offset : offset + snapshot.evidence_count]:
            source_by_evidence_id.setdefault(item.id, snapshot.source_id)
        offset += snapshot.evidence_count

    grouped: dict[str, float] = {}
    for item in unique_evidence:
        source_id = source_by_evidence_id[item.id]
        grouped[source_id] = grouped.get(source_id, 0.0) + float(item.confidence)
    return grouped
