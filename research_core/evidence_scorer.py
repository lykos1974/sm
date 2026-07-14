from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Iterable, Mapping

from .evidence import Evidence


@dataclass(frozen=True)
class EvidenceScoreResult:
    """Immutable deterministic confidence summary for collected Evidence."""

    evidence: tuple[Evidence, ...]
    evidence_count: int
    confidence_sum: float
    confidence_average: float
    confidence_maximum: float
    confidence_minimum: float
    confidence_by_source_id: Mapping[str, float]

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence", tuple(self.evidence))
        object.__setattr__(
            self,
            "confidence_by_source_id",
            MappingProxyType(dict(self.confidence_by_source_id)),
        )


class EvidenceScorer:
    """Calculate unweighted confidence scores without decision logic."""

    def score(self, evidence: Iterable[Evidence]) -> EvidenceScoreResult:
        items = tuple(evidence)
        confidences = tuple(float(item.confidence) for item in items)
        evidence_count = len(items)

        if evidence_count == 0:
            return EvidenceScoreResult(
                evidence=(),
                evidence_count=0,
                confidence_sum=0.0,
                confidence_average=0.0,
                confidence_maximum=0.0,
                confidence_minimum=0.0,
                confidence_by_source_id={},
            )

        confidence_sum = sum(confidences)
        grouped: dict[str, float] = {}
        for item in items:
            source_id = str(getattr(item, "source_id", item.source_quality))
            grouped[source_id] = grouped.get(source_id, 0.0) + float(item.confidence)

        return EvidenceScoreResult(
            evidence=items,
            evidence_count=evidence_count,
            confidence_sum=confidence_sum,
            confidence_average=confidence_sum / evidence_count,
            confidence_maximum=max(confidences),
            confidence_minimum=min(confidences),
            confidence_by_source_id=grouped,
        )
