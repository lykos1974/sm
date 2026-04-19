# research_v2 (Scaffolding Step)

## Why this isolated path is needed

The current workflow mixes:
- live/operational app state,
- evolving research extraction,
- ad-hoc analytics,
- and execution-style simulation logic.

That coupling makes it harder to reproduce research outputs, compare experiments, and safely evolve simulation behavior without risking baseline regressions.

## Direction for the next architecture phase

`research_v2` introduces a data-first separation:

1. **Operational/app state** (SQLite)
   - fast mutable state for app/runtime behavior.
2. **Frozen research datasets** (Parquet)
   - immutable, versioned snapshots for reproducibility.
3. **Analytics** (DuckDB/Polars)
   - fast analysis against frozen datasets.
4. **Execution simulation**
   - separate from research labeling mode, so research metrics and realistic fill behavior evolve independently.

## What this step includes

- New isolated module tree:
  - `research_v2/setup_dataset/`
  - `research_v2/labeling/`
  - `research_v2/analytics/`
  - `research_v2/execution/`
  - `research_v2/common/` helpers for paths, naming, and manifests.
- New data roots:
  - `data/research/setups/`
  - `data/research/labels/`
  - `data/research/analysis/`
  - `data/research/manifests/`
- Minimal schema + helpers for versioned output naming and run manifests.

## What this step explicitly does NOT include

- No changes to existing strategy rules.
- No changes to trade validation or break-even behavior.
- No changes to simulator fill/activation behavior.
- No rewrite of current simulation path.
- No performance optimization work.
- No Rust integration.

This is only a low-risk skeleton to enable future isolated development.
