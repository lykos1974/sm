from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ResearchPaths:
    """Resolved filesystem paths used by the isolated research_v2 workflow."""

    repo_root: Path
    data_root: Path
    setups_root: Path
    labels_root: Path
    analysis_root: Path
    manifests_root: Path


def resolve_research_paths(repo_root: Path | None = None) -> ResearchPaths:
    """Resolve canonical research directories with no side effects."""
    resolved_repo_root = repo_root or Path(__file__).resolve().parents[2]
    data_root = resolved_repo_root / "data" / "research"

    return ResearchPaths(
        repo_root=resolved_repo_root,
        data_root=data_root,
        setups_root=data_root / "setups",
        labels_root=data_root / "labels",
        analysis_root=data_root / "analysis",
        manifests_root=data_root / "manifests",
    )


def ensure_research_directories(paths: ResearchPaths) -> None:
    """Create research directories if they do not exist."""
    for directory in (
        paths.data_root,
        paths.setups_root,
        paths.labels_root,
        paths.analysis_root,
        paths.manifests_root,
    ):
        directory.mkdir(parents=True, exist_ok=True)
