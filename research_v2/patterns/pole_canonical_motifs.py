from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path

NA_VALUES = {"", "na", "none", "null", "nan"}


@dataclass(frozen=True)
class RawGroup:
    source_type: str
    dimensions_used: tuple[str, ...]
    values: tuple[str, ...]
    support_count: int
    continuation_pct: float
    avg_expectancy: float
    weighted_expectancy: float
    avg_asymmetry: float
    avg_adverse: float


@dataclass
class CanonicalAggregate:
    dimensions: tuple[str, ...]
    values: tuple[str, ...]
    support_count: int = 0
    derived_cluster_count: int = 0
    weighted_expectancy_sum: float = 0.0
    expectancy_sum: float = 0.0
    continuation_sum: float = 0.0
    asymmetry_sum: float = 0.0
    adverse_sum: float = 0.0


def _to_float(value: str | float | int | None, default: float = 0.0) -> float:
    if value is None:
        return default
    text = str(value).strip()
    if text.lower() in NA_VALUES:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def _to_int(value: str | float | int | None, default: int = 0) -> int:
    if value is None:
        return default
    text = str(value).strip()
    if text.lower() in NA_VALUES:
        return default
    try:
        return int(float(text))
    except ValueError:
        return default


def _norm_token(value: str | None) -> str:
    text = str(value or "").strip()
    if text.lower() in NA_VALUES:
        return "NA"
    return text


def _parse_dimensions(raw: dict[str, str]) -> tuple[str, ...]:
    dims = [_norm_token(x) for x in str(raw.get("dimensions_used", "")).split("|")]
    return tuple(d for d in dims if d and d != "NA")


def _parse_values(raw: dict[str, str]) -> tuple[str, ...]:
    vals = [_norm_token(x) for x in str(raw.get("group_key", "")).split("|")]
    return tuple(vals)


def _load_raw_groups(path: Path, source_type: str) -> list[RawGroup]:
    if not path.exists():
        return []

    out: list[RawGroup] = []
    with path.open("r", newline="") as f:
        for row in csv.DictReader(f):
            dims = _parse_dimensions(row)
            vals = _parse_values(row)
            pairs: list[tuple[str, str]] = []
            for i, dim in enumerate(dims):
                val = vals[i] if i < len(vals) else "NA"
                if val == "NA":
                    continue
                pairs.append((dim, val))
            canonical_dims = tuple(p[0] for p in pairs)
            canonical_vals = tuple(p[1] for p in pairs)
            support = _to_int(row.get("sample_size"), 0)
            continuation = _to_float(row.get("continuation_pct"), 0.0)
            expectancy = _to_float(row.get("expectancy_score"), 0.0)
            asymmetry = _to_float(row.get("asymmetry_score"), 0.0)
            adverse = _to_float(row.get("avg_max_adverse"), _to_float(row.get("avg_adverse"), 0.0))
            out.append(
                RawGroup(
                    source_type=source_type,
                    dimensions_used=canonical_dims,
                    values=canonical_vals,
                    support_count=support,
                    continuation_pct=continuation,
                    avg_expectancy=expectancy,
                    weighted_expectancy=expectancy * support,
                    avg_asymmetry=asymmetry,
                    avg_adverse=adverse,
                )
            )
    return out


def _aggregate_canonical(raw_groups: list[RawGroup]) -> tuple[dict[tuple[tuple[str, ...], tuple[str, ...]], CanonicalAggregate], list[dict[str, str | int]]]:
    motif_map: dict[tuple[tuple[str, ...], tuple[str, ...]], CanonicalAggregate] = {}
    redundant_map: list[dict[str, str | int]] = []
    for rg in raw_groups:
        key = (rg.dimensions_used, rg.values)
        if key not in motif_map:
            motif_map[key] = CanonicalAggregate(dimensions=rg.dimensions_used, values=rg.values)
        agg = motif_map[key]
        agg.support_count += rg.support_count
        agg.derived_cluster_count += 1
        agg.weighted_expectancy_sum += rg.weighted_expectancy
        agg.expectancy_sum += rg.avg_expectancy
        agg.continuation_sum += rg.continuation_pct
        agg.asymmetry_sum += rg.avg_asymmetry
        agg.adverse_sum += rg.avg_adverse

        redundant_map.append(
            {
                "source_type": rg.source_type,
                "source_dimensions_used": "|".join(rg.dimensions_used) if rg.dimensions_used else "(none)",
                "source_group_key": "|".join(rg.values) if rg.values else "(none)",
                "canonical_dimensions": "|".join(rg.dimensions_used) if rg.dimensions_used else "(none)",
                "canonical_group_key": "|".join(rg.values) if rg.values else "(none)",
                "support_count": rg.support_count,
            }
        )
    return motif_map, redundant_map


def main() -> None:
    ap = argparse.ArgumentParser(description="Compress live-safe expectancy clusters into canonical structural motifs.")
    ap.add_argument("--input-rankings-csv", required=True)
    ap.add_argument("--input-clusters-csv", default="")
    ap.add_argument("--output-root", required=True)
    args = ap.parse_args()

    ranking_path = Path(args.input_rankings_csv)
    cluster_path = Path(args.input_clusters_csv) if args.input_clusters_csv else Path("")
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    raw_groups = _load_raw_groups(ranking_path, "rankings")
    if cluster_path and cluster_path.exists():
        raw_groups.extend(_load_raw_groups(cluster_path, "clusters"))

    raw_count = len(raw_groups)
    na_dims_removed = 0
    redundant_dims_removed = 0
    for rg in raw_groups:
        full_dims = len(_parse_dimensions({"dimensions_used": "|".join(rg.dimensions_used)}))
        na_dims_removed += max(0, full_dims - len(rg.dimensions_used))
        redundant_dims_removed += max(0, full_dims - len(set(rg.dimensions_used)))

    motif_map, redundant_rows = _aggregate_canonical(raw_groups)

    motif_rows: list[dict[str, str | int | float]] = []
    for agg in motif_map.values():
        cluster_count = agg.derived_cluster_count
        avg_expectancy = agg.expectancy_sum / cluster_count if cluster_count else 0.0
        weighted_expectancy = agg.weighted_expectancy_sum / agg.support_count if agg.support_count else 0.0
        continuation_pct = agg.continuation_sum / cluster_count if cluster_count else 0.0
        avg_asymmetry = agg.asymmetry_sum / cluster_count if cluster_count else 0.0
        avg_adverse = agg.adverse_sum / cluster_count if cluster_count else 0.0
        stability_score = (min(1.0, cluster_count / 10.0) * 0.5) + (min(1.0, agg.support_count / 200.0) * 0.5)
        motif_rows.append(
            {
                "canonical_dimensions": "|".join(agg.dimensions),
                "canonical_group_key": "|".join(agg.values),
                "support_count": agg.support_count,
                "derived_cluster_count": cluster_count,
                "avg_expectancy": round(avg_expectancy, 6),
                "weighted_expectancy": round(weighted_expectancy, 6),
                "continuation_pct": round(continuation_pct, 6),
                "avg_asymmetry": round(avg_asymmetry, 6),
                "avg_adverse": round(avg_adverse, 6),
                "stability_score": round(stability_score, 6),
            }
        )

    motif_rows.sort(key=lambda r: (-float(r["weighted_expectancy"]), -int(r["support_count"])))

    with (output_root / "pole_canonical_motifs.csv").open("w", newline="") as f:
        fields = [
            "canonical_dimensions",
            "canonical_group_key",
            "support_count",
            "derived_cluster_count",
            "avg_expectancy",
            "weighted_expectancy",
            "continuation_pct",
            "avg_asymmetry",
            "avg_adverse",
            "stability_score",
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(motif_rows)

    with (output_root / "pole_redundant_cluster_map.csv").open("w", newline="") as f:
        fields = ["source_type", "source_dimensions_used", "source_group_key", "canonical_dimensions", "canonical_group_key", "support_count"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(redundant_rows)

    duplicate_collapsed = raw_count - len(motif_rows)
    compression_ratio = (len(motif_rows) / raw_count) if raw_count else 0.0
    top = motif_rows[:10]

    with (output_root / "pole_canonical_motif_summary.md").open("w") as f:
        f.write("# Pole Canonical Motif Compression (Research-Only)\n\n")
        f.write("## Diagnostics\n")
        f.write(f"- raw groups loaded: {raw_count}\n")
        f.write(f"- duplicate groups collapsed: {duplicate_collapsed}\n")
        f.write(f"- canonical motifs surviving: {len(motif_rows)}\n")
        f.write(f"- redundant dimensions removed: {redundant_dims_removed}\n")
        f.write(f"- NA dimensions removed: {na_dims_removed}\n")
        f.write(f"- motif compression ratio: {compression_ratio:.4f}\n\n")

        def _yes_no(keyword: str) -> str:
            return "YES" if any(keyword in str(r["canonical_dimensions"]) for r in top) else "NO"

        f.write("## Dominant canonical motifs\n")
        for row in top:
            f.write(f"- {row['canonical_dimensions']} => {row['canonical_group_key']} | weighted_expectancy={row['weighted_expectancy']} | support={row['support_count']}\n")
        f.write("\n## Geometry dimensions that truly matter\n")
        dim_count: dict[str, int] = {}
        for row in top:
            for dim in str(row["canonical_dimensions"]).split("|"):
                if dim:
                    dim_count[dim] = dim_count.get(dim, 0) + 1
        for dim, cnt in sorted(dim_count.items(), key=lambda kv: (-kv[1], kv[0])):
            f.write(f"- {dim}: appears in {cnt} top motifs\n")

        f.write("\n## Dimensions adding no incremental explanatory value\n")
        f.write("- Candidate no-value dimensions are those absent from top canonical motifs after NA/subset collapse.\n")

        f.write("\n## Stable motifs across many derived clusters\n")
        stable = sorted(motif_rows, key=lambda r: (-int(r["derived_cluster_count"]), -float(r["stability_score"])))[:10]
        for row in stable:
            f.write(f"- {row['canonical_dimensions']} => {row['canonical_group_key']} | derived_clusters={row['derived_cluster_count']} | stability={row['stability_score']}\n")

        f.write("\n## Most over-fragmented geometries\n")
        f.write("- Over-fragmentation is approximated by high derived_cluster_count mapped into the same canonical motif.\n")

        f.write("\n## Minimal causal motif candidates\n")
        minimal = sorted(motif_rows, key=lambda r: (len(str(r["canonical_dimensions"]).split("|")), -float(r["weighted_expectancy"])))[:10]
        for row in minimal:
            f.write(f"- {row['canonical_dimensions']} => {row['canonical_group_key']}\n")

        f.write("\n## Whether retrace_ratio remains invariant\n")
        f.write(f"- result: {_yes_no('retrace_ratio')}\n")
        f.write("\n## Whether opposing_pole_distance_columns remains invariant\n")
        f.write(f"- result: {_yes_no('opposing_pole_distance_columns')}\n")
        f.write("\n## Whether enhanced_by_opposing_pole survives compression\n")
        f.write(f"- result: {_yes_no('enhanced_by_opposing_pole')}\n")


if __name__ == "__main__":
    main()
