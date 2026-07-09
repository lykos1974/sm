from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .evidence import Evidence
from .evidence_source import EvidenceSource
from .exceptions import RequiredFieldError


@dataclass(frozen=True)
class CompositeEvidenceSource(EvidenceSource):
    """Deterministic EvidenceSource that concatenates child source evidence.

    The composite snapshots the provided child sources at construction time and
    queries them in that exact order. Evidence is yielded unchanged and is not
    deduplicated, preserving child order and any repeated Evidence objects for a
    later explicit deduplication stage.
    """

    _source_id: str
    _sources: tuple[EvidenceSource, ...]

    def __init__(self, source_id: str, sources: Iterable[EvidenceSource]) -> None:
        if not isinstance(source_id, str) or not source_id:
            raise RequiredFieldError("source_id is required")

        object.__setattr__(self, "_source_id", source_id)
        object.__setattr__(self, "_sources", tuple(sources))

    @property
    def source_id(self) -> str:
        """Stable identifier for this composite evidence producer."""
        return self._source_id

    def produce_evidence(self, context: Mapping[str, Any]) -> Iterable[Evidence]:
        """Return child Evidence objects in source order without deduplication."""
        return tuple(
            evidence
            for source in self._sources
            for evidence in source.produce_evidence(context)
        )
