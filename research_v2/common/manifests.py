from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class DatasetArtifact:
    """Single artifact produced by a research_v2 stage."""

    stage: str
    artifact_type: str
    relative_path: str
    row_count: int | None = None
    notes: str = ""


@dataclass
class DatasetManifest:
    """Versioned manifest describing one isolated research_v2 run."""

    run_id: str
    created_at_utc: str
    source_context: dict[str, Any] = field(default_factory=dict)
    artifacts: list[DatasetArtifact] = field(default_factory=list)


def new_manifest(run_id: str, source_context: dict[str, Any] | None = None) -> DatasetManifest:
    """Build a new manifest object with explicit UTC creation timestamp."""
    return DatasetManifest(
        run_id=run_id,
        created_at_utc=datetime.now(timezone.utc).isoformat(),
        source_context=source_context or {},
    )


def write_manifest(path: Path, manifest: DatasetManifest) -> None:
    """Write manifest JSON deterministically and create parent dirs if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = asdict(manifest)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
