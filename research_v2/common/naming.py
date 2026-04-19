from __future__ import annotations

from datetime import datetime, timezone


def utc_timestamp_label(ts: datetime | None = None) -> str:
    """Return sortable UTC timestamp label for versioned outputs."""
    dt = ts or datetime.now(timezone.utc)
    return dt.strftime("%Y%m%dT%H%M%SZ")


def versioned_dataset_name(dataset_kind: str, run_id: str, extension: str) -> str:
    """Create overwrite-safe dataset names.

    Example output: setups__run_20260418T101530Z__v001.parquet
    """
    normalized_ext = extension.lstrip(".")
    return f"{dataset_kind}__{run_id}__v001.{normalized_ext}"


def manifest_name(run_id: str) -> str:
    """Create a manifest filename bound to a specific run ID."""
    return f"manifest__{run_id}.json"
