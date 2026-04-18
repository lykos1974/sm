# analytics

Outcome-focused research analytics for deterministic labeled datasets.

## Dependency note (DuckDB path)

The DuckDB analytics CLI requires the `duckdb` Python package.
If `duckdb` is not installed, the CLI exits with a clear error message.
The plain Python analytics CLI remains available as a fallback.

## Python engine (existing fallback)

```bash
python -m research_v2.analytics.analyze_labeled_dataset \
  --input-labeled-dataset-path data/research/labels/labels__run_YYYYMMDDTHHMMSSZ__v001.parquet
```

## DuckDB engine

```bash
python -m research_v2.analytics.analyze_labeled_dataset_duckdb \
  --input-labeled-dataset-path data/research/labels/labels__run_YYYYMMDDTHHMMSSZ__v001.parquet
```

Minimal local CSV example:

```bash
python -m research_v2.analytics.analyze_labeled_dataset_duckdb \
  --input-labeled-dataset-path /tmp/research_labels.csv \
  --output-root /tmp/research_out \
  --dry-run
```

Optional ad-hoc SQL mode:

```bash
python -m research_v2.analytics.analyze_labeled_dataset_duckdb \
  --input-labeled-dataset-path data/research/labels/labels__run_YYYYMMDDTHHMMSSZ__v001.parquet \
  --sql-query "SELECT symbol, COUNT(*) AS rows FROM labeled GROUP BY symbol ORDER BY rows DESC"
```

Behavior:
- reads frozen labeled dataset (`.parquet` preferred, `.csv` supported)
- computes overall outcome scorecard metrics
- computes grouped breakdown summaries by research dimensions
- optional ad-hoc SQL against DuckDB view `labeled`
- writes versioned artifacts to `data/research/analysis/`
- writes matching manifest to `data/research/manifests/`
