# Causal Pole Genetic Hypothesis Miner Design

Date: 2026-06-04
Status: Phase 0 design only
Scope: research-only hypothesis mining for pole setups

## 1. Objective and Guardrails

Design a causal hypothesis miner for the validated `CAUSAL_P2_POLE_REVERSAL_CONFIRMATION` universe.
The miner searches for rule-based sub-hypotheses that may concentrate edge without using information unavailable at entry.

Known validated universe anchor:

| Metric | Value |
|---|---:|
| Trades | 4,023 |
| Expectancy | +0.062516R/trade |
| Total R | +251.5R |
| Universe status | `UNIVERSE_MATCH` confirmed |

Prior research constraints:

- Original non-causal core motif: rejected.
- P+4 causal true-birth motif: rejected.
- Generic P+2 causal motif: survives, but weak.
- Edge concentration hints: `ENA`/`HYPE`/`TAO`, `SHORT`, deep retrace `>0.618`, and `NEAR_RECENT_AVG` pole size.

Hard guardrails:

- No live-trader changes.
- No production strategy changes.
- No promotion from this design.
- No database schema changes.
- No exported metric renames.
- No changes to protected strategy interfaces.
- Short-side logic remains experimental unless explicitly promoted later.
- The current long-only stable rollback profile remains untouched.

## 2. Causal Boundary

A mined rule may only reference features that are fully known at or before the candidate entry timestamp.
For the P+2 universe, entry is the first executable candle open after the P+2 confirmation column start timestamp, unless an explicitly causal entry-timing variant is selected.

### 2.1 Forbidden Features

The miner must reject any chromosome, query, dataset column, or derived feature that uses:

- `opposing_pole_distance_columns`.
- `enhanced_by_opposing_pole`.
- Future outcome labels during candidate selection.
- Target/stop result labels during feature construction.
- Maximum favorable excursion after entry.
- Maximum adverse excursion after entry.
- Any future bar, candle, column, swing, or market state after the selected entry timestamp.
- Any field whose value is finalized only after entry.

Outcome labels are allowed only inside fitness evaluation after a rule has selected trades using causal features.
They must not be used as genes or rule predicates.

### 2.2 Allowed Feature Families

The chromosome may use only these families when the implementation can prove timestamp causality:

- Symbol and market family.
- Direction.
- Pole boxes.
- Pole duration.
- Pole velocity.
- Relative pole size versus recent average.
- Reversal boxes.
- Retrace ratio.
- Retrace quality.
- Trend/range context known before entry.
- Recent column alternation/choppiness known before entry.
- Causal entry timing variants.
- Stop boxes.
- Target R.
- Break-even trigger R.

## 3. Chromosome Structure

A chromosome represents one executable rule. It has four layers:

1. Universe filters: select which causal P+2 trades are eligible.
2. Geometry filters: constrain pole/reversal/retrace structure.
3. Context filters: constrain pre-entry market regime and recent P&F behavior.
4. Execution parameters: define entry timing, stop, target, and break-even management.

Recommended serialized shape:

```yaml
chromosome_id: string
version: pole_genetic_hypothesis_miner_v1
universe:
  symbols: [SYMBOL_OR_FAMILY]
  directions: [LONG_OR_SHORT]
geometry:
  pole_boxes: interval_or_disabled
  pole_duration_columns: interval_or_disabled
  pole_velocity_boxes_per_column: interval_or_disabled
  relative_pole_size_bucket: enum_set_or_disabled
  reversal_boxes: interval_or_disabled
  retrace_ratio: interval_or_disabled
  retrace_quality: enum_set_or_disabled
context:
  trend_range_context: enum_set_or_disabled
  recent_alternation_count: interval_or_disabled
  recent_choppiness_bucket: enum_set_or_disabled
execution:
  entry_timing: enum
  stop_boxes: integer
  target_r: decimal
  break_even_trigger_r: decimal_or_disabled
constraints:
  max_rule_terms: integer
  min_trades: integer
metadata:
  random_seed: integer
  generation: integer
  parent_ids: [string]
```

A disabled filter contributes no predicate and no feature degrees of freedom other than the decision to disable it.
This allows the miner to compare simple broad rules against narrower multi-condition rules.

## 4. Gene Ranges and Allowed Values

All ranges should be represented as closed intervals unless stated otherwise.
Continuous ranges should be snapped to coarse bins to reduce overfit risk.

### 4.1 Universe Genes

| Gene | Allowed values | Notes |
|---|---|---|
| `symbols` | single symbol, symbol subset, or `ALL` | Allowed symbols initially inherit the confirmed universe. Tiny one-symbol rules are heavily penalized and may be rejected. |
| `market_family` | `MAJOR`, `HIGH_BETA`, `MEME_OR_NARRATIVE`, `ALL`, implementation-defined causal families | A symbol may be selected directly or through a predeclared family, not through performance-derived families created after seeing test results. |
| `direction` | `LONG`, `SHORT`, `BOTH` | `SHORT` is experimental; no promotion implied. |

Symbol-family examples must be defined before mining and versioned in config.
If prior hints are used, they must be represented as an explicitly labeled seed chromosome or prior, not silently baked into validation.

### 4.2 Geometry Genes

| Gene | Allowed values | Suggested initial bins |
|---|---|---|
| `pole_boxes_min/max` | integers, disabled allowed | `[3, 4]`, `[5, 7]`, `[8, 12]`, `[13, +inf]` or dataset-derived quantile bins computed on train only. |
| `pole_duration_columns_min/max` | integers, disabled allowed | `[1, 1]`, `[2, 2]`, `[3, 4]`, `[5, +inf]`. |
| `pole_velocity_min/max` | boxes per column, disabled allowed | `SLOW`, `NORMAL`, `FAST`, `EXTREME`; bucket edges learned from train only. |
| `relative_pole_size_bucket` | enum set, disabled allowed | `SMALL_VS_RECENT_AVG`, `NEAR_RECENT_AVG`, `LARGE_VS_RECENT_AVG`, `EXTREME_VS_RECENT_AVG`. |
| `reversal_boxes_min/max` | integers, disabled allowed | `[1, 2]`, `[3, 4]`, `[5, +inf]`. |
| `retrace_ratio_min/max` | decimal bins, disabled allowed | `[0.000, 0.382)`, `[0.382, 0.500)`, `[0.500, 0.618]`, `(0.618, 0.786]`, `(0.786, +inf)`. |
| `retrace_quality` | enum set, disabled allowed | `SHALLOW`, `ORDERLY`, `DEEP`, `FAILED`, `V_SHAPED`, implementation-defined causal buckets. |

The `DEEP` and `>0.618` retrace hints may be included as seeds, but the final verdict must come from out-of-sample validation.

### 4.3 Context Genes

| Gene | Allowed values | Notes |
|---|---|---|
| `trend_range_context` | `UPTREND`, `DOWNTREND`, `RANGE`, `TRANSITION`, `UNKNOWN`, disabled | Must be computed from columns/candles ending at or before entry. |
| `recent_alternation_count_min/max` | integers, disabled allowed | Count direction flips in a fixed pre-entry column window. |
| `recent_choppiness_bucket` | `LOW`, `MEDIUM`, `HIGH`, `EXTREME`, disabled | Bucket edges computed on train only. |
| `lookback_columns` | `5`, `10`, `20`, `40` | Causal pre-entry window for alternation/choppiness. |

Lookback windows are parameters of feature computation and therefore part of the chromosome complexity budget.

### 4.4 Execution Genes

| Gene | Allowed values | Notes |
|---|---|---|
| `entry_timing` | `P2_NEXT_OPEN`, `P2_CONFIRMATION_CLOSE_NEXT_OPEN`, `P2_PLUS_ONE_COLUMN_START_NEXT_OPEN`, `LIMIT_AT_P2_MID_RETRACE_IF_TOUCHED_AFTER_SIGNAL` | Each variant must be executable using only information known when the order would be placed. |
| `stop_boxes` | `2`, `3`, `4`, `5`, `6` | Stop distance in P&F boxes. |
| `target_r` | `1.5`, `2.0`, `2.5`, `3.0`, `4.0` | Fixed R target. |
| `break_even_trigger_r` | disabled, `1.0`, `1.5`, `2.0`, `2.5` | Trigger must occur after entry; it is management logic, not a selection feature. |

The baseline P+2 universe currently uses fixed execution assumptions when auditing the generic motif.
Changing execution genes creates a new research experiment, not a strategy promotion.

## 5. Causal Feature Validation Rules

The implementation must include a dedicated causal validator before any mining run is accepted.
The validator should be conservative and fail closed.

Required validation rules:

1. Every feature has a `known_at` timestamp.
2. For a selected trade, every filter feature must satisfy `known_at <= decision_timestamp`.
3. The `decision_timestamp` is the timestamp when the entry order could first be decided.
4. For market/limit entry variants, order parameters must be computable at `decision_timestamp`.
5. Stop, target, and break-even settings may be chosen by the chromosome before entry, but realized stop/target/BE outcomes must be hidden until fitness scoring.
6. Feature transforms using rolling averages must use windows ending strictly before or at `decision_timestamp`.
7. Train-derived bin edges must be stored and reused unchanged on validation/test.
8. Any null, late, or ambiguous timestamp must cause the feature to be unavailable for that trade, not imputed from the future.
9. Forbidden feature names must be blocked by explicit denylist and lineage checks.
10. Rule predicates must be reconstructed from the serialized chromosome and audited independently of any cached selection mask.

Recommended feature-lineage metadata:

| Field | Purpose |
|---|---|
| `feature_name` | Stable feature identifier. |
| `source_columns` | Raw inputs used to compute the feature. |
| `lookback_window` | Number of pre-entry candles/columns used. |
| `known_at_rule` | Deterministic timestamp rule. |
| `uses_future_data` | Must be false for mining features. |
| `allowed_for_selection` | Must be true for chromosome genes. |

## 6. Fitness Function

The miner optimizes out-of-sample expectancy first and then total R, while penalizing complexity and fragility.
Fitness should be computed on validation data during search and on test data only once for final candidates.

### 6.1 Metrics

For each split, compute:

- `trades`.
- `wins`.
- `losses`.
- `break_even_exits` where applicable.
- `win_rate`.
- `expectancy_r` = `total_R / trades`.
- `total_R`.
- `max_drawdown_R`.
- `profit_factor` if meaningful.
- `median_R`.
- `p25_R` and `p75_R`.
- `symbol_concentration`.
- `time_concentration`.
- Yearly and quarterly expectancy/total_R.
- Rule complexity score.

### 6.2 Minimum Trades Threshold

Default gates:

| Scope | Minimum |
|---|---:|
| Full candidate all-sample selected trades | 200 |
| Train split selected trades | 100 |
| Validation split selected trades | 50 |
| Test split selected trades | 50 |
| Per walk-forward validation fold | 30 |

If the 4,023-trade universe cannot support these thresholds for a candidate, the verdict should be `INSUFFICIENT_DATA` or `OVERFIT_LIKELY`, not promoted.
Thresholds may be increased for complex chromosomes.

### 6.3 Complexity Penalty

Complexity score:

```text
complexity =
  active_filter_count
  + 0.5 * interval_bound_count
  + 0.5 * enum_value_count
  + 1.0 * custom_lookback_count
  + 1.0 * non-default_execution_parameter_count
```

Fitness penalty:

```text
complexity_penalty = complexity_lambda * max(0, complexity - simple_rule_budget)
```

Suggested defaults:

- `simple_rule_budget = 6`.
- `complexity_lambda = 0.01R` per excess complexity point.
- Hard reject if `active_filter_count > 8` unless explicitly running a diagnostic stress test.

### 6.4 Symbol Concentration Penalty

Use Herfindahl-Hirschman Index over selected trades by symbol:

```text
symbol_hhi = sum(symbol_trade_share^2)
symbol_penalty = symbol_lambda * max(0, symbol_hhi - symbol_hhi_limit)
```

Suggested defaults:

- `symbol_hhi_limit = 0.45`.
- `symbol_lambda = 0.10R`.
- Hard reject if one symbol contributes more than `65%` of selected trades, unless the rule is explicitly labeled as a single-symbol exploratory candidate and never receives `ROBUST_CAUSAL_CANDIDATE`.

### 6.5 Time Concentration Penalty

Compute concentration over calendar quarters or fixed chronological buckets:

```text
time_hhi = sum(bucket_trade_share^2)
time_penalty = time_lambda * max(0, time_hhi - time_hhi_limit)
```

Suggested defaults:

- `time_hhi_limit = 0.35`.
- `time_lambda = 0.10R`.
- Hard reject if more than `50%` of total_R comes from one quarter or one contiguous 10% time slice.

### 6.6 Stability Penalty

Compute yearly and quarterly metrics for all buckets with enough selected trades.
Penalize candidates that have positive aggregate expectancy but unstable distribution.

Stability components:

- Fraction of profitable years.
- Fraction of profitable quarters.
- Worst-year expectancy.
- Worst-quarter expectancy.
- Standard deviation of quarterly expectancy.
- Drawdown depth and duration.
- Validation/test degradation versus train.

Suggested penalty:

```text
stability_penalty =
  yearly_lambda * max(0, min_profitable_year_fraction - profitable_year_fraction)
  + quarterly_lambda * max(0, min_profitable_quarter_fraction - profitable_quarter_fraction)
  + degradation_lambda * max(0, train_expectancy - validation_expectancy - degradation_tolerance)
```

Suggested defaults:

- `min_profitable_year_fraction = 0.60`.
- `min_profitable_quarter_fraction = 0.50`.
- `degradation_tolerance = 0.10R`.
- Reject if validation expectancy is negative.
- Reject if test expectancy is negative.

### 6.7 Final Fitness Formula

Validation search fitness:

```text
fitness =
  validation_expectancy_r
  + secondary_total_r_weight * normalized_validation_total_R
  - complexity_penalty
  - symbol_penalty
  - time_penalty
  - stability_penalty
```

Suggested `secondary_total_r_weight = 0.10` after normalizing total_R by expected universe-scale opportunity count.

Primary ranking must remain out-of-sample expectancy.
A high total_R candidate with weak expectancy should not outrank a broad stable candidate with stronger out-of-sample expectancy unless the expectancy difference is immaterial and all stability gates pass.

## 7. Train / Validation / Test Split

Use chronological splits to prevent future leakage.
Repeated random splits are allowed only as additional robustness checks after the chronological split.

Default chronological split:

| Split | Share | Purpose |
|---|---:|---|
| Train | 50% | Fit bins, seed population, and estimate rough rule viability. |
| Validation | 25% | Genetic selection and hyperparameter choice. |
| Test | 25% | Final untouched evaluation. |

Rules:

- Split by entry timestamp, not row order if row order could be unstable.
- Compute rolling/binning parameters on train only.
- Validation and test must reuse train-fitted feature definitions.
- Do not tune thresholds after test results.
- If data spans distinct market regimes, report regime boundaries and trade counts per split.

Alternative split for smaller windows:

- Train: first 60%.
- Validation: next 20%.
- Test: final 20%.

Use the 50/25/25 default unless the implementation documents why an alternative is necessary.

## 8. Walk-Forward Validation

Walk-forward validation verifies that a candidate survives repeated causal retraining and forward evaluation.

Recommended procedure:

1. Sort all universe trades by entry timestamp.
2. Create rolling or expanding windows.
3. For each fold:
   - Fit feature bins and mine candidates on the in-sample window.
   - Select only candidates that pass train/internal-validation gates.
   - Freeze the selected chromosome and bin edges.
   - Evaluate on the next unseen forward window.
4. Aggregate forward-window metrics across folds.
5. Compare the aggregate walk-forward result against the generic P+2 universe baseline.

Default fold design:

| Fold setting | Default |
|---|---:|
| In-sample window | 50% of currently available history or 18 months, whichever has enough trades |
| Internal validation | last 25% of in-sample window |
| Forward window | next quarter or next 10%-15% chronological slice |
| Minimum forward trades | 30 per fold |
| Minimum valid folds | 5 |

A candidate should not be considered robust unless it has positive aggregate walk-forward expectancy and does not depend on one successful fold.

## 9. Anti-Overfit Protections

Required protections:

1. Minimum selected trades at all split levels.
2. No tiny segments.
3. Explicit complexity penalty.
4. Symbol concentration penalty and hard rejection thresholds.
5. Time concentration penalty and hard rejection thresholds.
6. Repeated random split checks.
7. Chronological holdout test that remains untouched until final evaluation.
8. Walk-forward validation.
9. Seed sensitivity checks.
10. Rule-family de-duplication to avoid reporting many equivalent variants.
11. Train-only bin estimation.
12. Causal feature lineage validation.
13. No target leakage from execution outcomes into selection features.
14. Multiple-comparisons awareness through validation/test/walk-forward gates rather than single-split cherry-picking.

### 9.1 Tiny Segment Rejection

Reject or downgrade candidates when:

- Full selected trades `< 200`.
- Validation trades `< 50`.
- Test trades `< 50`.
- Any required symbol/time bucket has too few trades for its reported stability statistic.
- A candidate adds filters that reduce trade count without improving validation expectancy by a meaningful margin.

### 9.2 Repeated Random Splits

After a candidate passes chronological validation, run repeated grouped random splits as a stress test.
These splits must not replace chronological validation.

Suggested protocol:

- 100 random seeds.
- Group by month or quarter to preserve local serial correlation.
- 70/30 train/evaluation split over time buckets.
- Report median, p10, and p90 evaluation expectancy.
- Reject if p10 evaluation expectancy is negative and the rule is highly complex or concentrated.

### 9.3 One Symbol / One Quarter Rejection

A candidate is `OVERFIT_LIKELY` if:

- One symbol supplies more than `65%` of selected trades.
- One symbol supplies more than `75%` of total_R.
- One quarter supplies more than `50%` of selected trades.
- One quarter supplies more than `50%` of total_R.
- Removing the best symbol turns test expectancy negative.
- Removing the best quarter turns test expectancy negative.

Single-symbol candidates may be retained as exploratory artifacts with verdict `PROMISING_BUT_UNSTABLE` at best unless the user explicitly requests a single-market research program.

## 10. Miner Outputs

Each run should write a versioned artifact directory and a compact summary table.
No production strategy file should consume these artifacts automatically.

Required outputs:

| Artifact | Contents |
|---|---|
| `candidate_rules.csv` | One row per candidate chromosome with serialized predicates and verdict. |
| `candidate_rules.jsonl` | Full chromosome, fitted bin edges, causality metadata, and fitness components. |
| `candidate_trades.parquet` or `.csv` | Selected trades for each candidate with causal features, entry timestamp, and realized R. |
| `split_metrics.csv` | Train/validation/test metrics for each candidate. |
| `walk_forward_metrics.csv` | Fold-level forward metrics. |
| `stability_metrics.csv` | Yearly, quarterly, symbol, and time concentration metrics. |
| `miner_run_manifest.json` | Input data hashes, code version, random seeds, split definitions, and denylist version. |
| `verdict_summary.md` | Human-readable summary and recommended next action. |

Required candidate-level fields:

- `candidate_id`.
- `chromosome_version`.
- `rule_text`.
- `active_filters`.
- `execution_parameters`.
- `trades`.
- `expectancy_r`.
- `total_R`.
- `win_rate`.
- `max_drawdown_R`.
- `profit_factor` where meaningful.
- `train_expectancy_r`.
- `validation_expectancy_r`.
- `test_expectancy_r`.
- `walk_forward_expectancy_r`.
- `profitable_year_fraction`.
- `profitable_quarter_fraction`.
- `symbol_concentration_hhi`.
- `top_symbol_trade_share`.
- `top_symbol_total_R_share`.
- `time_concentration_hhi`.
- `top_quarter_trade_share`.
- `top_quarter_total_R_share`.
- `complexity_score`.
- `fitness_score`.
- `causal_validation_status`.
- `verdict`.

## 11. Verdict Rules

Allowed verdicts only:

- `ROBUST_CAUSAL_CANDIDATE`.
- `PROMISING_BUT_UNSTABLE`.
- `OVERFIT_LIKELY`.
- `NO_EDGE_FOUND`.
- `INSUFFICIENT_DATA`.

### 11.1 `ROBUST_CAUSAL_CANDIDATE`

Use only when all are true:

- Causal validator passes.
- Minimum trade thresholds pass.
- Chronological validation expectancy is positive and materially above the generic P+2 baseline.
- Test expectancy is positive and not materially degraded from validation.
- Aggregate walk-forward expectancy is positive.
- Complexity is within budget or justified by materially stronger out-of-sample results.
- Symbol and time concentration thresholds pass.
- Best-symbol and best-quarter removal tests do not destroy the edge.
- Repeated random split p10 expectancy is non-negative or only mildly negative with strong median performance.

This verdict is still a research verdict, not production promotion.

### 11.2 `PROMISING_BUT_UNSTABLE`

Use when:

- Causality passes.
- Expectancy is positive in validation or test.
- At least one stability, concentration, or walk-forward gate is weak.
- The result is useful for further research but not robust enough for promotion.

### 11.3 `OVERFIT_LIKELY`

Use when:

- The rule is too narrow.
- Performance depends on one symbol, one quarter, or one lucky fold.
- Train performance is strong but validation/test degrades materially.
- Complexity is high relative to trade count.
- Repeated split performance is unstable.

### 11.4 `NO_EDGE_FOUND`

Use when:

- Adequate data exists.
- Causal mining runs successfully.
- No candidate passes minimum positive validation/test expectancy and stability gates.

### 11.5 `INSUFFICIENT_DATA`

Use when:

- Candidate trade count is below required thresholds.
- Split/fold coverage is too sparse.
- Required causal features are unavailable for too many rows.
- The universe cannot support the requested mining scope.

## 12. Implementation Phases

### Phase 0: Design Only

Deliver this design document.
No code, no strategy changes, no live-trader changes, and no promotion.

### Phase 1: Deterministic Exhaustive / Random Sampler Baseline

Purpose: establish a transparent baseline before adding genetic search.

Tasks:

- Build causal feature registry and denylist.
- Build chronological split utility.
- Build simple rule evaluator.
- Enumerate coarse single-filter and two-filter rules.
- Add deterministic random sampler for multi-filter rules.
- Emit the required artifacts and verdicts.

Exit criteria:

- Reproduces the 4,023-trade universe count.
- Confirms `UNIVERSE_MATCH`.
- Produces stable artifacts with fixed seeds.
- Demonstrates the causal validator blocks forbidden features.

### Phase 2: Genetic Miner

Purpose: search larger causal rule spaces while preserving anti-overfit controls.

Tasks:

- Add chromosome serializer/deserializer.
- Add population initialization from broad random rules plus prior-hint seeds.
- Add mutation operators:
  - enable/disable filter;
  - widen/narrow interval;
  - shift interval bin;
  - add/remove enum value;
  - switch entry timing;
  - adjust stop/target/BE gene.
- Add crossover operators that preserve valid causal chromosomes.
- Add elitism with diversity constraints.
- Rank by validation fitness only.
- Record seeds, parents, generations, and all fitness components.

Exit criteria:

- Deterministic with fixed seed.
- Does not evaluate forbidden selection features.
- Produces candidate rules that can be independently replayed from serialized chromosomes.

### Phase 3: Walk-Forward Validation

Purpose: determine whether mined candidates survive forward retraining/evaluation.

Tasks:

- Implement rolling or expanding walk-forward folds.
- Freeze per-fold rules before forward evaluation.
- Aggregate fold-level metrics.
- Add fold concentration and best-fold removal checks.

Exit criteria:

- At least five valid forward folds when data permits.
- Forward aggregate metrics emitted for all finalists.
- Verdict rules use walk-forward results.

### Phase 4: Candidate Export

Purpose: produce research artifacts suitable for review and possible later shadow validation.

Tasks:

- Export candidate rules, trades, split metrics, stability metrics, and manifest.
- Include human-readable rule text.
- Include causal validation proof summary.
- Include comparison to the generic P+2 universe baseline.

Exit criteria:

- Candidate artifacts are self-contained and reproducible.
- No production strategy imports candidate artifacts.

### Phase 5: Shadow Validation Only

Purpose: observe finalists without live trading or production promotion.

Tasks:

- Convert selected research candidates into shadow-only monitors.
- Log hypothetical entries/exits and realized R.
- Compare shadow outcomes against historical expectations.
- Keep all results outside live execution.

Exit criteria:

- Shadow monitor cannot place orders.
- Shadow metrics include the same scorecard used in historical mining.
- Any promotion requires a separate explicit approval and separate implementation task.

## 13. Required Research Scorecard

Any serious experiment produced by this miner must report the repository scorecard where applicable:

- `candidate_rows_registered`.
- `resolved_rows`.
- `win_rate_non_ambiguous`.
- `avg_realized_r_multiple`.
- `total_realized_r_multiple`.
- `TP1 -> TP2 conversion`.

For the P+2 miner's execution-model outputs, also report:

- `trades`.
- `expectancy_r`.
- `total_R`.
- `win_rate`.
- `max_drawdown_R`.
- `symbol_concentration`.
- `time_concentration`.
- `train/validation/test metrics`.
- `walk-forward metrics`.
- `verdict`.

## 14. Non-Goals

This design intentionally does not:

- Implement the miner.
- Modify live trading code.
- Modify production strategy code.
- Promote short-side logic.
- Promote any pole candidate.
- Change schemas or exported metric names.
- Use non-causal opposing-pole enhancement fields.

## 15. Recommended Default Run Configuration

Initial research run defaults once implementation begins:

```yaml
universe: CAUSAL_P2_POLE_REVERSAL_CONFIRMATION
expected_universe_trades: 4023
require_universe_match: true
split: chronological_50_25_25
walk_forward_min_folds: 5
full_min_trades: 200
validation_min_trades: 50
test_min_trades: 50
per_fold_min_trades: 30
simple_rule_budget: 6
max_active_filters: 8
symbol_hhi_limit: 0.45
single_symbol_trade_share_reject: 0.65
time_hhi_limit: 0.35
single_quarter_trade_share_reject: 0.50
single_quarter_total_r_reject: 0.50
random_split_repeats: 100
forbidden_features:
  - opposing_pole_distance_columns
  - enhanced_by_opposing_pole
  - future_outcome_labels
primary_metric: out_of_sample_expectancy_r
secondary_metric: total_R
allowed_verdicts:
  - ROBUST_CAUSAL_CANDIDATE
  - PROMISING_BUT_UNSTABLE
  - OVERFIT_LIKELY
  - NO_EDGE_FOUND
  - INSUFFICIENT_DATA
```

## 16. Review Checklist Before Any Future Implementation

Before Phase 1 starts, confirm:

- The source dataset still reproduces `trades = 4023`, `expectancy = 0.062516R`, and `total_R = 251.5R`.
- The feature registry can distinguish selection features from outcome fields.
- All allowed features have deterministic `known_at` timestamps.
- Forbidden fields fail the validator if included in a chromosome.
- The stable long-only rollback profile is not imported, mutated, or overwritten.
- Output artifacts are research-only and cannot affect live execution.
