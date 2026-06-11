# PnF AB=CD / Swing Symmetry Audit Design

## Status and Scope

This is a **research design document only**. It defines a future descriptive audit for PnF-native AB=CD / swing-symmetry geometry and does not implement a detector, setup, strategy, scanner, signal, expectancy model, live/demo trader behavior, or production-code change.

The future audit must use the validated local harmonic research artifacts as the trusted baseline:

- `research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v2`
- `research_v2/patterns/VALIDATED_ratio_predictive_local_v4`
- `research_v2/patterns/VALIDATED_raw_ratio_density_local_v2`
- `research_v2/patterns/VALIDATED_harmonic_time_stability_local_v2`

The future audit must **not** use the broken boundary-regime artifacts or any outputs derived from them.

## Trusted Baseline Facts

The design assumes the following validated facts are fixed inputs to the next research phase:

1. Explicit PnF box sizes are validated:
   - `BINANCE_FUT:BTCUSDT = 100`
   - `BINANCE_FUT:ETHUSDT = 5`
   - `BINANCE_FUT:SOLUSDT = 0.25`
2. No inferred `0.005` pseudo-boxes are allowed.
3. Validated SLOW reactions count: `30,491`.
4. Validated SLOW confirming reactions count: `7,832`.
5. Validated ratio context:
   - `reaction_ratio < 0.40` is reversal dominated.
   - `reaction_ratio >= 0.40` is continuation dominated.
6. The ratio context is validated cross-symbol across `BTCUSDT`, `ETHUSDT`, and `SOLUSDT`.
7. The ratio context is validated cross-year across `2024`, `2025`, and `2026`.

## Research Question

The next research question is descriptive:

> Do PnF-native SLOW confirming swing sequences exhibit stable AB=CD / swing-symmetry geometry across symbols and years?

This question is about **geometry and structural repetition only**. It must not be framed as a trading edge, entry model, exit model, setup, or signal.

## PnF-Native Definitions

### Pivot

A pivot is a SLOW `CONFIRMING` reaction that finalizes a directional swing transition. The pivot point is represented by the confirmed reaction's causal completion state:

- `symbol`
- `completion_time`
- `knowledge_time`
- `column_id`
- `candidate_direction`
- `candidate_boxes`
- `active_direction`
- `active_swing_boxes`
- `reaction_ratio`

The future audit should treat the confirming reaction as the causal moment at which the pivot becomes known.

### AB Leg

`AB` is the first completed PnF-native harmonic swing in a four-pivot sequence `A-B-C-D`.

In PnF-native terms:

- `A` is the prior SLOW confirmed pivot.
- `B` is the next SLOW confirmed pivot in the opposite direction.
- `AB` direction is the confirmed movement from `A` to `B`.
- `AB` size is measured in validated PnF boxes, not raw price units.
- `AB` completion is known only at pivot `B`'s `knowledge_time`.

### BC Leg

`BC` is the corrective leg immediately after `AB`.

In PnF-native terms:

- `B` is the confirmed terminal pivot of `AB`.
- `C` is the next SLOW confirmed pivot in the opposite direction from `AB`.
- `BC` direction opposes `AB`.
- `BC` size is measured in validated PnF boxes.
- `BC/AB` describes the depth of the correction relative to `AB`.
- `BC` completion is known only at pivot `C`'s `knowledge_time`.

### CD Leg

`CD` is the post-correction leg immediately after `BC`.

In PnF-native terms:

- `C` is the confirmed terminal pivot of `BC`.
- `D` is the next SLOW confirmed pivot in the opposite direction from `BC` and in the same direction as `AB`.
- `CD` direction should match `AB` direction for a valid AB=CD geometry candidate.
- `CD` size is measured in validated PnF boxes.
- `CD/AB` measures symmetry or extension relative to the original `AB` leg.
- `CD` completion is known only at pivot `D`'s `knowledge_time`.

## Pivot Selection From SLOW Confirming Reactions

The future audit should construct pivot chains using only SLOW `CONFIRMING` reactions from the trusted validated local harmonic reactions artifact.

Required pivot-selection rules:

1. Filter to `threshold_name == SLOW`.
2. Filter to `reaction_kind == CONFIRMING` for pivot construction.
3. Normalize symbols only to stable market identifiers while retaining enough provenance to verify exchange-qualified source rows.
4. Sort pivots per symbol by causal order:
   1. `completion_time`
   2. `column_id` as a deterministic tie-breaker if needed
5. Construct rolling four-pivot windows: `A-B-C-D`.
6. Require alternating directions:
   - `AB` and `BC` must oppose each other.
   - `BC` and `CD` must oppose each other.
   - `AB` and `CD` must share direction.
7. Reject windows with duplicate or non-increasing causal timestamps.
8. Reject windows with missing, non-finite, or non-positive leg sizes.

The audit should preserve rejected windows in `abcd_rejection_reasons.csv` rather than silently dropping them.

## Internal Reactions Inside a Leg

Internal SLOW reactions are allowed **inside** a leg as descriptive substructure, but they must not define `A`, `B`, `C`, or `D` pivots.

Rules:

- Pivot endpoints are selected only from SLOW `CONFIRMING` reactions.
- SLOW `INTERNAL` reactions between two confirming pivots may be counted as internal leg complexity.
- Internal reactions may be reported as context fields, such as `ab_internal_count`, `bc_internal_count`, and `cd_internal_count`.
- Internal reactions must not shorten, extend, replace, or relabel the confirmed leg endpoints.
- Internal reactions must not introduce lookahead. They are only known when their own `knowledge_time` has occurred.

This keeps the audit focused on harmonic swing geometry rather than mixing endpoint definitions.

## Ratio Measurements

### CD/AB

`CD/AB` measures symmetry or extension of the final leg relative to the initial leg:

```text
cd_ab_ratio = cd_boxes / ab_boxes
```

Measurement requirements:

- Use validated box-normalized leg sizes.
- Require `ab_boxes > 0` and `cd_boxes > 0`.
- Use absolute leg magnitude in boxes, while preserving directional fields separately.
- Do not use raw price differences unless only as an auxiliary diagnostic after box-size validation.

### BC/AB

`BC/AB` measures corrective depth of the middle leg relative to the initial leg:

```text
bc_ab_ratio = bc_boxes / ab_boxes
```

Measurement requirements:

- Use validated box-normalized leg sizes.
- Require `ab_boxes > 0` and `bc_boxes > 0`.
- Use absolute leg magnitude in boxes.
- Preserve `BC` direction separately to verify it opposes `AB`.

## Symmetry Zones

The future audit should classify `CD/AB` into descriptive symmetry zones:

| Zone name | Inclusive lower | Exclusive upper | Interpretation |
|---|---:|---:|---|
| `near_equal` | `0.90` | `1.10` | CD approximately matches AB. |
| `moderate_extension` | `1.20` | `1.35` | CD extends AB by a moderate harmonic amount. |
| `large_extension` | `1.55` | `1.70` | CD extends AB by a larger harmonic amount. |

Rules:

- These zones are descriptive buckets only.
- Boundaries should be lower-inclusive and upper-exclusive: `lower <= cd_ab_ratio < upper`.
- Rows outside these zones remain in the candidate geometry output with `symmetry_zone = outside_defined_zones` unless rejected for structural reasons.
- Do not optimize the zones in this phase.
- Do not convert zones into setup or trading rules.

## Use of the Validated 0.40 Ratio Regime Split

The validated `0.40` split should be used only as contextual structure metadata.

Allowed uses:

- Annotate each pivot reaction with `reaction_ratio_regime`:
  - `reversal_context` when `reaction_ratio < 0.40`
  - `continuation_context` when `reaction_ratio >= 0.40`
- Summarize AB=CD candidate geometry by pivot-context combinations.
- Check whether symmetry-zone prevalence differs descriptively by context.

Forbidden uses:

- Do not create a trading rule from the `0.40` split.
- Do not filter candidates solely because they are below or above `0.40`.
- Do not compute expectancy by regime.
- Do not label the regime as entry/exit confirmation.
- Do not use broken boundary-regime artifacts to redefine or retest the threshold.

The `0.40` split is a validated descriptive context, not a signal.

## knowledge_time Assignment

Every future AB=CD candidate row should have a single `knowledge_time` equal to the time at which the full `A-B-C-D` geometry first becomes knowable.

Required rule:

```text
candidate_knowledge_time = D.knowledge_time
```

If the source row lacks an explicit `knowledge_time`, use the confirming reaction's causal `completion_time` as the fallback and record the fallback in a diagnostic field such as `knowledge_time_source`.

Intermediate fields should also be retained:

- `a_knowledge_time`
- `b_knowledge_time`
- `c_knowledge_time`
- `d_knowledge_time`
- `candidate_knowledge_time`

The candidate must not be available before `D` is confirmed.

## Lookahead Avoidance

The future implementation must obey strict causal ordering:

1. A pivot may only be used after its own `knowledge_time`.
2. An `A-B-C-D` candidate may only be emitted at `D.knowledge_time`.
3. No fields from pivots after `D` may be used in candidate geometry.
4. No future reactions may be used to decide whether the candidate is valid.
5. Rejection reasons must be based only on data available up to `D.knowledge_time`.
6. Year partitioning should use `candidate_knowledge_time`, not any future outcome timestamp.
7. Internal-reaction context inside `AB`, `BC`, or `CD` may only include internal reactions whose own knowledge time is within that leg and not after `D.knowledge_time`.

## Required Future Outputs

The future audit should produce exactly the following research outputs:

### `abcd_candidate_geometry.csv`

One row per accepted `A-B-C-D` geometry candidate.

Minimum fields:

- `candidate_id`
- `symbol`
- `a_time`, `b_time`, `c_time`, `d_time`
- `a_column_id`, `b_column_id`, `c_column_id`, `d_column_id`
- `ab_direction`, `bc_direction`, `cd_direction`
- `ab_boxes`, `bc_boxes`, `cd_boxes`
- `bc_ab_ratio`
- `cd_ab_ratio`
- `symmetry_zone`
- `a_reaction_ratio`, `b_reaction_ratio`, `c_reaction_ratio`, `d_reaction_ratio`
- `a_ratio_regime`, `b_ratio_regime`, `c_ratio_regime`, `d_ratio_regime`
- `ab_internal_count`, `bc_internal_count`, `cd_internal_count`
- `candidate_knowledge_time`
- `knowledge_time_source`

### `abcd_ratio_summary.csv`

Aggregate descriptive geometry counts by symmetry zone.

Minimum fields:

- `symmetry_zone`
- `raw_candidate_count`
- `median_cd_ab_ratio`
- `avg_cd_ab_ratio`
- `median_bc_ab_ratio`
- `avg_bc_ab_ratio`
- `median_ab_boxes`
- `median_bc_boxes`
- `median_cd_boxes`

### `abcd_by_symbol.csv`

Descriptive symbol-level geometry counts.

Minimum fields:

- `symbol`
- `symmetry_zone`
- `raw_candidate_count`
- `median_cd_ab_ratio`
- `avg_cd_ab_ratio`
- `median_bc_ab_ratio`
- `avg_bc_ab_ratio`

### `abcd_by_year.csv`

Descriptive year-level geometry counts using `candidate_knowledge_time`.

Minimum fields:

- `year`
- `symmetry_zone`
- `raw_candidate_count`
- `median_cd_ab_ratio`
- `avg_cd_ab_ratio`
- `median_bc_ab_ratio`
- `avg_bc_ab_ratio`

### `abcd_rejection_reasons.csv`

One row per rejected four-pivot window.

Minimum fields:

- `candidate_window_id`
- `symbol`
- `a_time`, `b_time`, `c_time`, `d_time`
- `rejection_reason`
- `rejection_detail`
- `candidate_knowledge_time`

Expected rejection reasons include:

- `non_alternating_direction`
- `ab_cd_direction_mismatch`
- `non_increasing_time`
- `missing_leg_size`
- `non_positive_leg_size`
- `missing_knowledge_time`
- `untrusted_symbol_or_box_size`

### `abcd_design_report.md`

Human-readable descriptive report.

Required sections:

- Scope and restrictions
- Trusted artifacts used
- Dataset coverage
- Pivot-construction rules
- Internal-reaction handling
- Ratio-measurement definitions
- Symmetry-zone summary
- Symbol validation summary
- Year validation summary
- Rejection-reason summary
- Lookahead controls
- Risks and limitations
- Research-only conclusion

## Minimum Sample-Size Rules

Minimum sample-size rules are descriptive robustness gates, not trading gates.

Required minimums:

1. Overall geometry table:
   - At least `1,000` accepted candidates before making broad descriptive claims.
2. Per symmetry zone:
   - At least `100` accepted candidates per zone for zone-level descriptive comparison.
3. Per symbol and symmetry zone:
   - At least `30` accepted candidates per symbol-zone cell.
4. Per year and symmetry zone:
   - At least `30` accepted candidates per year-zone cell.
5. Rejection diagnostics:
   - Every rejected window must have exactly one primary rejection reason.

If a cell fails its minimum, the future report must mark it as `SPARSE` and avoid descriptive stability claims for that cell.

## Cross-Symbol Validation Requirements

The audit must validate geometry across:

- `BTCUSDT`
- `ETHUSDT`
- `SOLUSDT`

Requirements:

- Use the validated explicit box size for each symbol.
- Confirm no rows are assigned to an untrusted symbol without an explicit rejection reason.
- Report candidate counts by symbol and symmetry zone.
- Require each symbol to meet minimum sample-size rules before claiming cross-symbol stability.
- Do not let one high-count symbol dominate the conclusion without separately reporting all three symbols.

## Cross-Year Validation Requirements

The audit must validate geometry across:

- `2024`
- `2025`
- `2026`

Requirements:

- Partition by `candidate_knowledge_time` year.
- Report candidate counts by year and symmetry zone.
- Require each year to meet minimum sample-size rules before claiming cross-year stability.
- Flag any missing or malformed timestamps in rejection diagnostics.
- Avoid comparing years if extraction coverage differs materially without documenting the coverage difference.

## Risks and Controls

### Overfitting

Risk: choosing symmetry zones or post-filters because they look favorable after inspecting the data.

Control: use the pre-declared zones only and do not optimize boundaries in this phase.

### Named-Pattern Bias

Risk: assuming `AB=CD` has predictive value because it is a named harmonic pattern.

Control: treat `AB=CD` only as a geometric label. The audit must describe frequency and stability, not profitability or signal quality.

### Hidden Lookahead

Risk: using information after pivot `D` to validate or label the candidate.

Control: emit candidates only at `D.knowledge_time`; reject or accept using only information known by that time.

### Using Untrusted Boundary Artifacts

Risk: importing conclusions from the broken boundary-regime outputs.

Control: only use the trusted validated local artifacts listed in this document. Do not reference or join to broken boundary-regime outputs.

### Mixing Structural Swings With Harmonic Swings

Risk: combining structural swing logic and harmonic SLOW confirming reactions into a single inconsistent pivot chain.

Control: define AB, BC, and CD only from SLOW confirming harmonic reactions for this audit. Structural swings may be discussed in future research only if separately designed and explicitly isolated.

## Non-Goals

This design does not authorize:

- Detector implementation
- Setup generation
- Strategy logic
- Entry rules
- Exit rules
- Stop-loss or take-profit logic
- Expectancy calculations
- Backtests
- Scanners
- Live/demo trader changes
- Production-code changes
- Boundary-regime re-optimization

## Research-Only Conclusion

The next phase should be a descriptive PnF-native geometry audit that asks whether SLOW confirming pivot sequences form stable AB=CD / swing-symmetry structures across validated symbols and years. The validated `0.40` reaction-ratio split may be carried as contextual metadata, but it must not become a trading rule or optimization target.
