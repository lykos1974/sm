# PnF-Native Harmonic Swing Framework Design

Date: 2026-06-09
Status: Phase 0 design only
Scope: research-only PnF structural swing geometry

## 1. Research Objective

Design a deterministic research framework to determine whether Point-and-Figure (PnF) structural swing ratios have predictive expectancy.
This is a research design document only; it does not implement harmonic detection, trading logic, strategy changes, live trading behavior, or demo trading behavior.

The first research target is deliberately narrow: AB=CD / AB≈CD style geometry measured on PnF structural swings.
No named harmonic pattern is promoted at this stage.
Classical Gartley, Bat, Butterfly, Crab, or other named harmonic templates must not be copied directly into this framework.

The core research conclusion is:

> Columns are raw material. Structural swings are the research unit.

This framework is intended to extend validated pole research discipline without modifying the frozen forward-validated P2 survivor, `P2_SURVIVOR_V1` / `CAND-000053`.
The survivor and all existing pole research, strategy, detector, live trader, and demo trader code remain untouched by this design.

## 2. Definitions

### 2.1 Structural Swing

A **structural swing** is a causally confirmed directional price movement assembled from one or more PnF columns that together represent a coherent move from a confirmed structural extreme to the next confirmed structural extreme.
It is not required to equal a single X column or a single O column.

A structural swing must have:

- `swing_id`.
- `symbol`.
- `box_size` and reversal settings used to construct the underlying PnF columns.
- `direction`: `UP` or `DOWN`.
- `start_extreme_price` and `end_extreme_price`.
- `start_extreme_time` and `end_extreme_time`.
- `birth_time`: when the swing can be known as a completed structural swing.
- `knowledge_time`: timestamp at which downstream research labels may consume the swing.
- The list or range of source columns used to form the swing.

### 2.2 Swing High / Swing Low

A **swing high** is a causally confirmed local structural high formed after an upward structural swing and confirmed only after sufficient subsequent reversal evidence exists.

A **swing low** is a causally confirmed local structural low formed after a downward structural swing and confirmed only after sufficient subsequent reversal evidence exists.

Swing highs and swing lows are not known at the raw endpoint timestamp unless the structure engine can prove confirmation occurred at that same timestamp.
The default assumption is that confirmation occurs later than the endpoint.

### 2.3 Peak / Valley

A **peak** is the price extreme at a confirmed swing high.
A **valley** is the price extreme at a confirmed swing low.

For bullish AB=CD-style research, the candidate sequence is:

```text
low A -> peak B -> valley C -> peak D
```

For bearish AB=CD-style research, the candidate sequence is:

```text
peak A -> valley B -> peak C -> valley D
```

### 2.4 AB, BC, and CD Legs

An **AB leg** is the first structural swing in the candidate sequence.
For bullish candidates, AB moves upward from low A to peak B.
For bearish candidates, AB moves downward from peak A to valley B.

A **BC leg** is the corrective structural swing after AB.
For bullish candidates, BC moves downward from peak B to valley C.
For bearish candidates, BC moves upward from valley B to peak C.

A **CD leg** is the structural swing after BC.
For bullish candidates, CD moves upward from valley C to peak D.
For bearish candidates, CD moves downward from peak C to valley D.

### 2.5 D Confirmation

**D confirmation** is the first timestamp at which the structure engine can causally declare the D point complete.
Outcomes may only be measured after D confirmation.
The D endpoint price may occur before D confirmation, but research labels must not use D as a completed point until confirmation is available.

### 2.6 Observable Time

**Observable time** is the timestamp attached to a raw market artifact such as a candle, PnF column start, PnF column end, or column extreme.
Observable time records when price activity occurred, not necessarily when a completed pattern was knowable.

### 2.7 Causal Birth Time

**Causal birth time** is the earliest timestamp when a label, swing, or pattern completion is fully knowable by the research system without future information.
For harmonic swing labels, the relevant causal birth time is normally the D confirmation timestamp.
All downstream datasets must store both `birth_time` and `knowledge_time` so audits can distinguish price occurrence from research availability.

## 3. PnF Translation

### 3.1 Why Raw X/O Columns Are Not Stable Across Box Sizes

Raw X/O columns are a useful PnF representation, but they are not a stable harmonic research unit across box sizes and reversal settings.
Changing box size can split, merge, delay, or remove columns even when the higher-level structural move is visually similar.
A one-column measurement at one box size may become a multi-column movement at another box size.
A direct Gartley/Bat/Butterfly detector on raw columns would therefore risk learning artifacts of the PnF construction parameters rather than durable market structure.

### 3.2 Multiple Columns Can Belong to One Structural Swing

A structural swing may include multiple source columns when the move retains the same structural purpose.
Examples include:

- A strong directional move interrupted by shallow countercolumns that do not form a confirmed structural reversal.
- A stair-step advance where local pauses occur but the prior structural low remains intact.
- A decline with minor X-column reactions that fail to create a confirmed swing high.

The framework should treat such columns as raw material feeding a structure engine, not as separate harmonic legs by default.

### 3.3 Aggregating Columns Into Swings

A future structure engine should aggregate columns into swings using deterministic, parameterized rules.
Candidate aggregation inputs may include:

- Column direction and box count.
- Reversal-box settings.
- Breaks of prior structural highs/lows.
- Minimum swing magnitude in boxes.
- Minimum or maximum intervening column count.
- Nested swing hierarchy, if supported, with explicit level identifiers.
- Trend/range state known at the time of aggregation.

Every aggregation decision must be reproducible from raw PnF column inputs and stored configuration.
No subjective drawing, manual endpoint adjustment, chart-only validation, or manual pattern selection is allowed.

## 4. Measurements

Each completed AB=CD-style structural swing candidate should record the following measurements.
All measurements must be computed from confirmed structural swings and must respect the D confirmation boundary.

| Measurement | Description |
|---|---|
| `ab_boxes` | Absolute box distance from A to B. |
| `bc_boxes` | Absolute box distance from B to C. |
| `cd_boxes` | Absolute box distance from C to D. |
| `bc_ab_ratio` | `bc_boxes / ab_boxes`; null only if AB is invalid or zero. |
| `cd_ab_ratio` | `cd_boxes / ab_boxes`; primary AB=CD symmetry ratio. |
| `cd_bc_ratio` | `cd_boxes / bc_boxes`; expansion or contraction versus the corrective leg. |
| `swing_direction` | `BULLISH` for low-peak-valley-peak sequences; `BEARISH` for peak-valley-peak-valley sequences. |
| `column_count` | Total count of source PnF columns contributing to AB, BC, and CD. |
| `duration_columns` | Number of columns from A source-column start through D confirmation source-column boundary. |
| `trend_regime` | Deterministic trend/range context known at or before D confirmation. |
| `support_resistance_proximity` | Deterministic distance or bucket to known support/resistance levels at or before D confirmation. |

Additional metadata should include symbol, box configuration, source column identifiers, structural swing identifiers, observable endpoint times, `birth_time`, `knowledge_time`, configuration version, and code version when implementation eventually exists.

## 5. First Research Target: PnF Swing AB=CD v1

`PnF Swing AB=CD v1` is the initial framework target.
It is a neutral geometry label, not a strategy and not a named harmonic pattern promotion.

### 5.1 Bullish Sequence

A bullish candidate is a four-point structural sequence:

```text
low A -> peak B -> valley C -> peak D
```

Interpretation:

1. AB is an upward structural swing.
2. BC is a downward corrective structural swing.
3. CD is an upward structural swing completing at D.
4. The candidate becomes observable to research only after D confirmation.

### 5.2 Bearish Sequence

A bearish candidate is a four-point structural sequence:

```text
peak A -> valley B -> peak C -> valley D
```

Interpretation:

1. AB is a downward structural swing.
2. BC is an upward corrective structural swing.
3. CD is a downward structural swing completing at D.
4. The candidate becomes observable to research only after D confirmation.

### 5.3 No Named Harmonic Promotion

The v1 target only asks whether structural swing ratios around AB≈CD have predictive expectancy.
It must not label rows as Gartley, Bat, Butterfly, Crab, Shark, Cypher, or any other named harmonic pattern.
Named-pattern research, if ever attempted, requires a separate design and approval step after the PnF-native swing layer has been validated.

## 6. Buckets

### 6.1 CD/AB Buckets

The primary symmetry bucket is `cd_ab_ratio`:

| Bucket | Range |
|---|---|
| `LT_0_50` | `< 0.50` |
| `0_50_TO_0_75` | `0.50 <= cd_ab_ratio < 0.75` |
| `0_75_TO_0_90` | `0.75 <= cd_ab_ratio < 0.90` |
| `0_90_TO_1_10` | `0.90 <= cd_ab_ratio <= 1.10` |
| `1_10_TO_1_25` | `1.10 < cd_ab_ratio <= 1.25` |
| `1_25_TO_1_50` | `1.25 < cd_ab_ratio <= 1.50` |
| `GT_1_50` | `> 1.50` |

The `0_90_TO_1_10` bucket is the initial AB=CD / AB≈CD symmetry zone.
It is a research bucket only and does not imply trade eligibility.

### 6.2 BC/AB Buckets

The corrective-depth bucket is `bc_ab_ratio`:

| Bucket | Suggested definition | Notes |
|---|---|---|
| `SHALLOW` | `bc_ab_ratio < 0.382` | Initial deterministic default; may be revised before implementation. |
| `NORMAL` | `0.382 <= bc_ab_ratio <= 0.786` | Broad middle zone for non-extreme corrections. |
| `DEEP` | `bc_ab_ratio > 0.786` | Deep correction bucket; no automatic bearish or bullish implication. |

The exact BC/AB thresholds must be versioned in configuration when implementation occurs.
Any threshold changes create a new experiment version.

## 7. Outcomes

Outcomes are measured only after D confirmation.
The framework must evaluate both reversal and continuation behavior after completion.

Required outcome metrics:

| Outcome | Description |
|---|---|
| `reversal_success` | Whether price moves in the reversal direction by a configured threshold before invalidation. |
| `continuation_failure` | Whether expected reversal fails and price continues beyond D-side invalidation or continuation threshold. |
| `plus_1r_first` | Whether +1R is reached before -1R after D confirmation. |
| `plus_2r_first` | Whether +2R is reached before -1R after D confirmation. |
| `plus_3r_first` | Whether +3R is reached before -1R after D confirmation. |
| `minus_1r_first` | Whether -1R is reached before the configured positive threshold. |
| `expectancy` | Average realized R under the specified research outcome model. |
| `total_r` | Sum of realized R across candidates. |
| `win_rate` | Winning candidates divided by non-ambiguous candidates. |
| `sample_size` | Count of candidates in the evaluated group or bucket. |

Outcome definitions must specify direction, entry proxy, stop proxy, target proxy, maximum holding horizon, ambiguous-tie handling, and whether candle or PnF data drives path resolution.
Those settings are part of the experiment configuration and must be recorded in the manifest.

## 8. Anti-Lookahead Rules

The framework must fail closed on causality.

Hard anti-lookahead rules:

1. D is only known after structural swing confirmation.
2. No future swing endpoint may be used before confirmation.
3. All labels must store `birth_time` and `knowledge_time`.
4. A candidate row may not be emitted before its D confirmation timestamp.
5. Structural swing endpoints may store observable endpoint times, but outcome measurement and label availability must use causal confirmation times.
6. Trend regime and support/resistance proximity must be computed using data known at or before D confirmation.
7. Any feature with missing, ambiguous, or late causal metadata must be excluded or marked invalid; it must not be imputed from future data.
8. Train/test splits, bucket statistics, and summaries must never use future outcomes to alter label construction.
9. If nested swing hierarchy is introduced later, each hierarchy level must have independent confirmation metadata.
10. Any implementation must include an audit path from output row back to source columns and structural swing confirmations.

## 9. Failure Research

Harmonic failure may be more valuable than harmonic reversal.
This framework must explicitly measure both reversal and continuation after completion.

The AB≈CD symmetry zone may identify exhaustion, but it may also identify acceleration points where continuation through a completed structure is more predictive than reversal.
For that reason, v1 research must not assume that harmonic completion is a reversal signal.

Required failure analyses include:

- Continuation after D beyond a configured threshold.
- Failed reversal after initial favorable movement.
- Bucket comparison of reversal expectancy versus continuation expectancy.
- Whether failure behavior is stronger in shallow, normal, or deep BC/AB buckets.
- Whether failure behavior changes by trend regime or support/resistance proximity.

Failure labels must be first-class outputs, not incidental error cases.

## 10. Relationship to Existing Architecture

The proposed future architecture is layered:

```text
PnF engine
-> structure engine
-> harmonic_geometry_engine
-> research labeling
-> strategy engine later
```

Responsibilities:

1. **PnF engine**: produces deterministic X/O columns from market data and PnF configuration.
2. **Structure engine**: converts raw columns into confirmed structural swings with causal metadata.
3. **harmonic_geometry_engine**: measures AB, BC, and CD geometry across confirmed structural swings.
4. **Research labeling**: writes labels, bucket stats, summaries, and manifests for offline analysis.
5. **Strategy engine later**: consumes only explicitly promoted research outputs in a separate future change.

This design intentionally does not modify existing pole architecture, `P2_SURVIVOR_V1`, `CAND-000053`, detectors, strategies, live trader, demo trader, or production code.

## 11. What NOT To Do

This phase explicitly forbids:

- No Gartley detector.
- No Bat detector.
- No Butterfly detector.
- No Crab detector.
- No named harmonic detector of any kind.
- No live trading.
- No demo trading.
- No optimization.
- No candidate promotion.
- No changes to `CAND-000053`.
- No changes to `P2_SURVIVOR_V1`.
- No strategy changes.
- No production changes.
- No pole research rewrites.
- No detector rewrites.
- No database schema changes.
- No exported metric renames.
- No subjective chart annotation.
- No manual pattern selection.

## 12. Proposed Future CLI (Design Only)

A future audit command may look like this:

```bash
python -m research_v2.patterns.pnf_harmonic_swing_audit \
  --columns-input SYMBOL=CSV \
  --output-root OUTPUT_ROOT
```

Design-only expectations:

- `--columns-input` maps one or more symbols to deterministic PnF column CSV files.
- `--output-root` selects a new output directory for generated research artifacts.
- The CLI must be deterministic for identical inputs and configuration.
- The CLI must write a manifest with input checksums, configuration, code version, and run timestamp.
- The CLI must not place orders, call live trader modules, call demo trader modules, or mutate production state.

This command is only a proposed future interface.
It is not implemented in this change.

## 13. Required Future Artifacts

A future implementation must produce these artifacts under the selected output root:

| Artifact | Purpose |
|---|---|
| `pnf_harmonic_swing_labels.csv` | Row-level structural swing geometry labels with causal timestamps and outcome fields. |
| `pnf_harmonic_swing_summary.md` | Human-readable run summary, scorecard, caveats, and discard/promote-neutral conclusions. |
| `pnf_harmonic_swing_bucket_stats.csv` | Aggregated statistics by CD/AB bucket, BC/AB bucket, direction, regime, and optional support/resistance proximity. |
| `pnf_harmonic_swing_manifest.json` | Reproducibility manifest with config, inputs, checksums, code version, run metadata, and anti-lookahead validation status. |

The row-level label file should include enough source identifiers to reproduce every candidate from raw columns and confirmed structural swings.
The summary must report sample size and expectancy by bucket and must clearly separate reversal findings from continuation/failure findings.

## 14. Guardrails

This framework is research only.
It must be deterministic, reproducible, and auditable.

Core guardrails:

- Research only.
- Deterministic inputs, rules, labels, buckets, and outputs.
- Reproducible from raw PnF columns plus versioned configuration.
- No subjective drawing.
- No chart-only validation.
- No manual pattern selection.
- No live trading.
- No demo trading.
- No optimization in the design phase.
- No candidate promotion in the design phase.
- No changes to `CAND-000053` or `P2_SURVIVOR_V1`.
- No strategy changes.
- No production changes.
- No detector changes.
- No changes to protected interfaces.
- No database schema changes.
- Preserve stable rollback behavior.

If future empirical results degrade, the experiment must be marked `DISCARD` and baseline behavior must remain preserved.
If results are promising, promotion still requires a separate review, separate change set, and explicit approval.

## 15. Future Research Scorecard

When this framework is eventually implemented as a serious experiment, it should report the repository-standard scorecard alongside harmonic-specific metrics:

- `candidate_rows_registered`.
- `resolved_rows`.
- `win_rate_non_ambiguous`.
- `avg_realized_r_multiple`.
- `total_realized_r_multiple`.
- `TP1 -> TP2 conversion`, if the selected outcome model includes TP1 and TP2 stages.

Harmonic-specific additions should include:

- Candidate count by `cd_ab_ratio` bucket.
- Candidate count by `bc_ab_ratio` bucket.
- Reversal expectancy by bucket.
- Continuation/failure expectancy by bucket.
- Direction split: bullish versus bearish.
- Trend-regime split.
- Support/resistance proximity split.

The scorecard must state whether each run is `RESEARCH_ONLY`, `DISCARD`, or `REVIEW_REQUIRED`.
It must not declare promotion automatically.
