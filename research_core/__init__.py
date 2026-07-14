from .decision import Decision
from .composite_evidence_source import CompositeEvidenceSource
from .enums import DecisionType, ValidationOutcome
from .evidence import Evidence
from .evidence_deduplicator import (
    ConflictingEvidenceError,
    EvidenceDeduplicationResult,
    EvidenceDeduplicator,
)
from .evidence_source import EvidenceSource
from .evidence_pipeline import (
    EvidencePipeline,
    EvidencePipelineResult,
    EvidencePipelineStatus,
    EvidenceSourceExecutionStatus,
    EvidenceSourceSnapshot,
)
from .funding_rate_evidence_source import FundingRateEvidenceSource, FundingRateProvider
from .liquidation_evidence_source import LiquidationEvidenceSource, LiquidationProvider
from .open_interest_evidence_source import OpenInterestEvidenceSource, OpenInterestProvider
from .static_evidence_source import StaticEvidenceSource
from .exceptions import RequiredFieldError, ResearchCoreError
from .hypothesis import Hypothesis
from .ids import deterministic_id
from .knowledge import Knowledge
from .observation import Observation
from .validation import Validation

__all__ = (
    "Decision",
    "DecisionType",
    "CompositeEvidenceSource",
    "ConflictingEvidenceError",
    "Evidence",
    "EvidenceSource",
    "EvidenceDeduplicationResult",
    "EvidenceDeduplicator",
    "EvidencePipeline",
    "EvidencePipelineResult",
    "EvidencePipelineStatus",
    "EvidenceSourceExecutionStatus",
    "EvidenceSourceSnapshot",
    "FilteringEvidenceSource",
    "FundingRateEvidenceSource",
    "FundingRateProvider",
    "Hypothesis",
    "Knowledge",
    "LiquidationEvidenceSource",
    "LiquidationProvider",
    "OpenInterestEvidenceSource",
    "OpenInterestProvider",
    "Observation",
    "RequiredFieldError",
    "ResearchCoreError",
    "StaticEvidenceSource",
    "Validation",
    "ValidationOutcome",
    "deterministic_id",
)


def __getattr__(name):
    if name == "FilteringEvidenceSource":
        from .filtering_evidence_source import FilteringEvidenceSource

        return FilteringEvidenceSource
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
