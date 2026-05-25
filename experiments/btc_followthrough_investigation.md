# BTC Follow-through Investigation (No logic redesign)

## 1) Why only ~5 rows appear
The follow-through job consumed `/tmp/btc_validation/labeled/pole_labeled_outcomes.csv` with **4 rows** and exported **4 rows**. The small output is upstream-driven:

- `export_pnf_columns` on `pnf_mvp/pnf_mvp.db` + `BTCUSDT` produced **14** PnF columns.
- `audit_poles` on those columns produced **4** pole rows.
- `label_poles` produced **4** labeled rows.
- `pole_followthrough` consumed those **4** rows and exported **4** regime rows.

So the pipeline run was not using a full BTC labeled dataset (~2291 rows); it was operating on a tiny locally generated pole sample.

## 2) Debug/sample truncation check
No explicit truncation was found in `research_v2/patterns/pole_followthrough.py`:
- no `limit` argument,
- no slicing (`[:N]`) on loaded rows,
- full iteration over all loaded rows.

## 3) Whether classifier receives full labeled BTC input
It does **not** in this rerun.
The classifier input file had only 4 rows.
Additionally, those labeled rows do not contain `fav_col_i` / `adv_col_i` series columns, so classifier path arrays default to zeros in `_classify_regime`.

## 4) Exact row counts by stage
From the actual rerun:

- CSV load (follow-through input): **4**
- Preprocessing rows with fav/adv series:
  - rows_with_any_fav_series: **0**
  - rows_with_any_adv_series: **0**
  - rows_with_both_series: **0**
- Regime classification rows: **4**
- Final export rows: **4**

Upstream context counts:

- PnF columns CSV rows: **14**
- Pole patterns rows: **4**
- Pole labeled outcomes rows: **4**

## 5) Normalized trajectory clipping/binarization check
Observed trajectories are single-token strings (`length=1`) with unique values `{-1.0, 0.0, 1.0}` in this run.
This is caused by fallback behavior in `_compute_followthrough`:
- when `fav_col_i`/`adv_col_i` are absent, it uses only `[max_favorable_boxes]` and `[max_adverse_boxes]`,
- denominator is `max(max_fav, max_adv, 1.0)`,
- therefore normalized value often collapses to -1, 0, or +1.

No separate explicit clipping to `{-1,0,1}` exists.

## 6) Volatility compression denominator/range collapse check
Compression collapsed to `{0.0, 1.0}` in this run.
Cause:
- with no `fav_col_i`/`adv_col_i` series, fallback arrays are length 1,
- early/later windows reduce to minimal values, producing highly quantized outcomes.

No sample-size truncation inside compression logic was found; collapse is a consequence of missing path series + 1-point fallback.

## 7) Future path series truncation before classification
Yes, effectively absent for this input.
`pole_followthrough` expects `fav_col_i` / `adv_col_i` columns for path classification; `label_poles` currently does not output those series.
So classification receives zero-filled synthetic paths (`fav_path`, `adv_path`) and fallback single-point trajectories in metrics.

## Diagnostic command outputs
Ran:

`python -m research_v2.patterns.pole_followthrough --input-labeled-csv /tmp/btc_validation/labeled/pole_labeled_outcomes.csv --output-root /tmp/btc_validation/follow_diag --future-columns 20 --diagnostics`

Output:
- `DIAG csv_load_rows=4`
- `DIAG rows_with_any_fav_series=0`
- `DIAG rows_with_any_adv_series=0`
- `DIAG rows_with_both_series=0`
- `DIAG regime_classification_rows=4`
- `DIAG final_export_rows=4`
