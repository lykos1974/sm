from __future__ import annotations

from typing import Any, Iterable, Mapping, Protocol, runtime_checkable

from .evidence import Evidence


@runtime_checkable
class EvidenceSource(Protocol):
    """Structural interface for objects that produce research evidence.

    Evidence sources are intentionally generic: implementations may use any
    upstream data or process, but they must expose a stable source identifier
    and return :class:`Evidence` domain objects.
    """

    @property
    def source_id(self) -> str:
        """Stable identifier for the producer of emitted evidence."""
        ...

    def produce_evidence(self, context: Mapping[str, Any]) -> Iterable[Evidence]:
        """Produce zero or more Evidence domain objects for the given context."""
        ...
