# filters

Research-only filter stage for frozen setup datasets.

Current mode:
- `profitable_family_v1`
  - side = LONG
  - breakout_context = POST_BREAKOUT_PULLBACK
  - active_leg_boxes = 2
  - status = CANDIDATE
  - quality_grade = A

CLI example:

```bash
python -m research_v2.filters.filter_setup_dataset \
  --input-setup-dataset-path data/research/setups/setups__run_YYYYMMDDTHHMMSSZ__v001.parquet
```
