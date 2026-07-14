from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .evidence import Evidence
from .exceptions import ResearchCoreError


class ConflictingEvidenceError(ResearchCoreError):
    """Raised when duplicate evidence ids refer to different evidence content."""


@dataclass(frozen=True)
class EvidenceDeduplicationResult:
    """Immutable summary of deterministic evidence deduplication."""

    unique_evidence: tuple[Evidence, ...]
    duplicate_count: int
    duplicate_ids: tuple[str, ...]
    input_count: int


class EvidenceDeduplicator:
    """Deduplicate evidence by id while preserving first-seen object identity."""

    def deduplicate(self, evidence: Iterable[Evidence]) -> EvidenceDeduplicationResult:
        seen: dict[str, Evidence] = {}
        unique: list[Evidence] = []
        duplicate_ids: list[str] = []
        duplicate_count = 0
        input_count = 0

        for item in evidence:
            input_count += 1
            existing = seen.get(item.id)
            if existing is None:
                seen[item.id] = item
                unique.append(item)
                continue

            if existing != item:
                raise ConflictingEvidenceError(
                    f"conflicting evidence content for duplicate id {item.id!r}"
                )

            duplicate_count += 1
            duplicate_ids.append(item.id)

        return EvidenceDeduplicationResult(
            unique_evidence=tuple(unique),
            duplicate_count=duplicate_count,
            duplicate_ids=tuple(duplicate_ids),
            input_count=input_count,
        )
