# setup_dataset

Minimal frozen setup dataset export path.

CLI example:

```bash
python -m research_v2.setup_dataset.export_setup_dataset \
  --settings-path pnf_mvp/settings.research_clean.json
```

This stage reads `strategy_setups` in read-only mode and writes:
- versioned dataset artifact in `data/research/setups/`
- matching run manifest in `data/research/manifests/`
