# labeling

Deterministic research labeling engine (independent per-setup mode).

CLI example:

```bash
python -m research_v2.labeling.label_setup_dataset \
  --input-dataset-path data/research/setups/setups__run_YYYYMMDDTHHMMSSZ__v001.parquet \
  --settings-path pnf_mvp/settings.research_clean.json \
  --horizon-minutes 240
```

Behavior:
- reads frozen setup dataset (`.parquet` preferred, `.csv` supported)
- loads future candles from storage DB in read-only mode
- evaluates each setup independently (no global pending state)
- writes versioned labels artifact to `data/research/labels/`
- writes matching run manifest to `data/research/manifests/`

Resolution states in v1:
- `STOPPED`, `TP1_ONLY`, `TP1_THEN_BE`, `TP2`, `AMBIGUOUS`, `EXPIRED`
