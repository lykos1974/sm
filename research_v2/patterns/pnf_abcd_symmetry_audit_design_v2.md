# PnF AB=CD / Swing Symmetry Audit Design v2

## Status and Scope

This is a **research design document only**. It revises the AB=CD / swing-symmetry audit design to close mandatory design gaps before any implementation work is considered.

This document does **not** authorize or implement:

- AB=CD detector logic
- setup generation
- strategy logic
- entries, exits, stops, targets, or position management
- expectancy calculations
- backtests
- scanners
- live/demo trader changes
- production-code changes

The future audit remains descriptive structure research only.

## Trusted Baseline Artifacts

The future audit must use only the validated local harmonic research artifacts as the trusted baseline:

- `research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v2`
- `research_v2/patterns/VALIDATED_ratio_predictive_local_v4`
- `research_v2/patterns/VALIDATED_raw_ratio_density_local_v2`
- `research_v2/patterns/VALIDATED_harmonic_time_stability_local_v2`

The future audit must **not** use broken boundary-regime artifacts or any outputs derived from them.

## Trusted Baseline Facts

The future audit must preserve these validated facts:

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

This is a geometry audit only. It must not be framed as a trading edge, entry model, exit model, setup, signal, or expectancy study.

## Canonical PnF Pivot Model

### Pivot Source

A pivot is a SLOW `CONFIRMING` reaction from the trusted harmonic reactions artifact.

Pivot construction must filter to:

```text
threshold_name == SLOW
reaction_kind == CONFIRMING
```

SLOW `INTERNAL` reactions may be counted as descriptive leg substructure, but must not define pivots.

### Canonical Direction Source

The canonical pivot/leg direction source is:

```text
pivot_direction = candidate_direction
```

Rationale:

- A SLOW confirming reaction confirms movement in the reaction/candidate direction.
- `candidate_direction` is therefore the direction of the newly confirmed leg ending at that pivot.
- `active_direction` is retained only as context for the prior active swing and must not be used as the primary leg-direction field.

Required direction validation:

1. `candidate_direction` must be present.
2. `candidate_direction` must be one of the expected PnF directions, e.g. `UP` or `DOWN`.
3. The future audit must reject rows with missing, malformed, or unsupported `candidate_direction` using `invalid_candidate_direction`.
4. If `active_direction` conflicts with expected alternating behavior, that conflict may be logged as a diagnostic field, but it must not override `candidate_direction`.

### Canonical Pivot Ordering

Pivots must be ordered per normalized symbol by:

1. `knowledge_time`
2. `completion_time`
3. `column_id`
4. stable source-row ordinal from the validated artifact

`knowledge_time` is the primary ordering key because it represents when the pivot is observable. `completion_time` and `column_id` are deterministic tie-breakers only.

Tie-handling rules:

- Equal `knowledge_time` is allowed only if later tie-breakers produce a strict deterministic order.
- If all ordering keys are identical for two source rows, they are duplicate source rows and must be handled by duplicate endpoint/source-row rules, not silently sorted.
- Equal `completion_time` alone is not a rejection reason if `knowledge_time` and deterministic tie-breakers produce a strict order.
- Non-increasing ordered pivot identity after all tie-breakers is a rejection reason: `non_increasing_pivot_order`.

## PnF-Native AB, BC, and CD Definitions

### AB Leg

`AB` is the first completed PnF-native harmonic swing in a candidate chain.

Required definition:

- `A` is a SLOW confirming pivot.
- `B` is the next accepted SLOW confirming pivot for the same symbol in canonical order.
- `AB` direction is `B.candidate_direction`.
- `AB` size is the validated box-normalized magnitude of the confirmed swing ending at `B`.
- `AB` becomes known at `B.knowledge_time`.

### BC Leg

`BC` is the corrective leg after `AB`.

Required definition:

- `C` is the next accepted SLOW confirming pivot for the same symbol after `B`.
- `BC` direction is `C.candidate_direction`.
- `BC` must oppose `AB`.
- `BC` size is the validated box-normalized magnitude of the confirmed swing ending at `C`.
- `BC` becomes known at `C.knowledge_time`.

### CD Leg

`CD` is the post-correction leg after `BC`.

Required definition:

- `D` is the next accepted SLOW confirming pivot for the same symbol after `C`.
- `CD` direction is `D.candidate_direction`.
- `CD` must oppose `BC`.
- `CD` must match `AB` direction.
- `CD` size is the validated box-normalized magnitude of the confirmed swing ending at `D`.
- `CD` becomes known at `D.knowledge_time`.

## Alternating Direction Rules

Using the canonical direction source above:

```text
AB direction = B.candidate_direction
BC direction = C.candidate_direction
CD direction = D.candidate_direction
```

A completed candidate is directionally valid only when:

```text
B.candidate_direction != C.candidate_direction
C.candidate_direction != D.candidate_direction
B.candidate_direction == D.candidate_direction
```

Rejection reasons:

- `non_alternating_ab_bc`
- `non_alternating_bc_cd`
- `ab_cd_direction_mismatch`
- `invalid_candidate_direction`

## Internal Reactions Inside a Leg

Internal SLOW reactions are allowed inside a leg as descriptive substructure only.

Rules:

- Pivot endpoints are selected only from SLOW `CONFIRMING` reactions.
- SLOW `INTERNAL` reactions between two confirming pivots may be counted as internal complexity.
- Internal reactions must not shorten, extend, replace, or relabel the confirmed leg endpoints.
- Internal reactions are counted only if their own `knowledge_time` is within the leg window and no later than the candidate's current observable state.
- Internal reaction context must never use reactions after `D.knowledge_time` for completed candidates or after the current state knowledge time for unresolved candidates.

Suggested context fields:

- `ab_internal_count`
- `bc_internal_count`
- `cd_internal_count`
- `ab_internal_max_ratio`
- `bc_internal_max_ratio`
- `cd_internal_max_ratio`

## Unresolved State Preservation

The future audit must preserve partial and unresolved states. Completed `A-B-C-D` candidates alone are not sufficient because completed-D-only output creates survivorship bias for any question about how often earlier structures complete.

Required unresolved states:

### `AB_WAITING_FOR_C`

Created when `A-B` is known but no valid `C` has yet been observed.

State timestamp:

```text
state_knowledge_time = B.knowledge_time
```

Required fields:

- `state_id`
- `symbol`
- `state = AB_WAITING_FOR_C`
- `a_time`, `b_time`
- `a_column_id`, `b_column_id`
- `ab_direction`
- `ab_boxes`
- `state_knowledge_time`

### `ABC_WAITING_FOR_D`

Created when `A-B-C` is known and directionally valid, but no valid `D` has yet been observed.

State timestamp:

```text
state_knowledge_time = C.knowledge_time
```

Required fields:

- `state_id`
- `symbol`
- `state = ABC_WAITING_FOR_D`
- `a_time`, `b_time`, `c_time`
- `a_column_id`, `b_column_id`, `c_column_id`
- `ab_direction`, `bc_direction`
- `ab_boxes`, `bc_boxes`
- `bc_ab_ratio`
- `state_knowledge_time`

### `INVALIDATED_ABC`

Created when an `A-B-C` state becomes structurally invalid before a valid `D` is formed.

Invalidation examples:

- next confirming pivot after `C` does not oppose `BC`
- next confirming pivot after `C` does not match `AB` direction
- non-positive or missing `D` leg size
- malformed direction or timestamp on the next pivot
- duplicate source row prevents deterministic candidate formation

State timestamp:

```text
state_knowledge_time = invalidating_pivot.knowledge_time
```

Required fields:

- `state_id`
- `symbol`
- `state = INVALIDATED_ABC`
- `a_time`, `b_time`, `c_time`
- `invalidating_time`
- `invalidation_reason`
- `invalidation_detail`
- `state_knowledge_time`

### `END_OF_SAMPLE_ABC`

Created for any still-open `A-B-C` state at the end of the trusted artifact with no observed valid or invalidating `D`.

State timestamp:

```text
state_knowledge_time = artifact_end_knowledge_time
```

Required fields:

- `state_id`
- `symbol`
- `state = END_OF_SAMPLE_ABC`
- `a_time`, `b_time`, `c_time`
- `ab_direction`, `bc_direction`
- `ab_boxes`, `bc_boxes`
- `bc_ab_ratio`
- `state_knowledge_time`
- `artifact_end_knowledge_time`

### Required Future Output for Unresolved States

Add a required future output:

```text
abcd_unresolved_states.csv
```

One row per unresolved or invalidated partial state.

Minimum fields:

- `state_id`
- `symbol`
- `state`
- `a_time`, `b_time`, `c_time`
- `a_column_id`, `b_column_id`, `c_column_id`
- `ab_direction`, `bc_direction`
- `ab_boxes`, `bc_boxes`
- `bc_ab_ratio`
- `invalidation_reason`
- `invalidation_detail`
- `state_knowledge_time`
- `artifact_end_knowledge_time`

The report must separately summarize completed candidates and unresolved states.

## Overlap and Reuse Semantics

### Policy

Overlapping structures are **allowed** because rolling four-pivot windows are a descriptive way to audit the full SLOW confirming pivot chain.

However, overlapping candidates are **not independent observations**. The future audit must report reuse explicitly and must not imply independent samples where candidates share pivots or legs.

### Allowed Reuse

A pivot may appear as:

- `D` in one completed candidate
- `C` in the next overlapping candidate
- `B` in another overlapping candidate
- `A` in a later overlapping candidate

A leg may appear as:

- `CD` in one candidate
- `BC` in the next candidate
- `AB` in a later candidate

### Required Reuse Reporting Fields

`abcd_candidate_geometry.csv` must include:

- `overlap_policy = allowed_rolling_windows`
- `a_pivot_reuse_count`
- `b_pivot_reuse_count`
- `c_pivot_reuse_count`
- `d_pivot_reuse_count`
- `ab_leg_reuse_count`
- `bc_leg_reuse_count`
- `cd_leg_reuse_count`
- `shares_pivot_with_prior_candidate`
- `shares_leg_with_prior_candidate`
- `candidate_independence = overlapping_not_independent`

Aggregate reports must include:

- total accepted candidates
- unique pivots used
- unique legs used
- average candidates per unique pivot
- average candidates per unique leg
- maximum pivot reuse count
- maximum leg reuse count

## Duplicate Endpoint Handling

Duplicate handling must be deterministic and explicit.

### Duplicate Source Rows

Rows are duplicate source rows if they share all of:

- normalized `symbol`
- `threshold_name`
- `reaction_kind`
- `knowledge_time`
- `completion_time`
- `column_id`
- `candidate_direction`
- validated leg size
- `reaction_ratio`

Policy:

- Preserve one canonical row.
- Record duplicate source rows in rejection diagnostics with `duplicate_source_row`.
- Include `duplicate_source_row_count` in dataset coverage.

### Duplicate Pivot Identity

Rows are duplicate pivot identities if they share:

- normalized `symbol`
- `knowledge_time`
- `completion_time`
- `column_id`

but differ in direction, size, or ratio fields.

Policy:

- Reject the affected pivot identity as ambiguous.
- Reject any candidate window requiring that ambiguous pivot.
- Use rejection reason `ambiguous_duplicate_pivot_identity`.

### Duplicate Candidate Endpoints

Candidate endpoints are duplicate if an accepted or rejected candidate has the same:

```text
symbol + a_pivot_id + b_pivot_id + c_pivot_id + d_pivot_id
```

Policy:

- Emit exactly one canonical candidate row.
- Record additional duplicates in `abcd_rejection_reasons.csv` using `duplicate_candidate_endpoints`.
- The canonical candidate ID must be deterministic from endpoint pivot IDs.

## Ratio Measurements

### CD/AB

```text
cd_ab_ratio = cd_boxes / ab_boxes
```

Requirements:

- Use validated box-normalized leg sizes.
- Require `ab_boxes > 0` and `cd_boxes > 0`.
- Use absolute leg magnitude in boxes.
- Preserve leg directions separately.

### BC/AB

```text
bc_ab_ratio = bc_boxes / ab_boxes
```

Requirements:

- Use validated box-normalized leg sizes.
- Require `ab_boxes > 0` and `bc_boxes > 0`.
- Use absolute leg magnitude in boxes.
- Preserve `BC` direction separately.

## Symmetry Zones

The future audit should classify completed candidates by `CD/AB` into descriptive zones:

| Zone name | Inclusive lower | Exclusive upper | Interpretation |
|---|---:|---:|---|
| `near_equal` | `0.90` | `1.10` | CD approximately matches AB. |
| `moderate_extension` | `1.20` | `1.35` | CD extends AB by a moderate harmonic amount. |
| `large_extension` | `1.55` | `1.70` | CD extends AB by a larger harmonic amount. |

Rules:

- Boundaries are lower-inclusive and upper-exclusive: `lower <= cd_ab_ratio < upper`.
- Rows outside these zones remain in candidate output as `outside_defined_zones` unless structurally rejected.
- Do not optimize zones in this phase.
- Do not convert zones into setup or trading rules.

## Use of the Validated 0.40 Ratio Split

The validated `0.40` split is contextual metadata only.

Allowed uses:

- annotate pivot reactions with `reaction_ratio_regime`
- summarize geometry counts by pivot-context combinations
- describe whether symmetry-zone prevalence differs by context

Forbidden uses:

- no trading rule
- no candidate filtering solely by the `0.40` split
- no expectancy by regime
- no entry/exit confirmation label
- no re-optimization using broken boundary artifacts

## knowledge_time Assignment

### No Unsafe Fallbacks

The future audit must not silently fall back from missing `knowledge_time` to `completion_time`.

Policy:

- Explicit `knowledge_time` is required for all pivots used in accepted candidates.
- If `knowledge_time` is missing, malformed, non-finite, or not causally ordered, reject the affected row or candidate with `missing_or_invalid_knowledge_time`.
- `completion_time` may be retained as a descriptive source timestamp, but it must not replace `knowledge_time` for candidate availability.
- Any future proposal to use `completion_time` as a fallback requires a separate validation proving that `completion_time` is the earliest live-observable timestamp.

### Completed Candidate knowledge_time

For accepted completed candidates:

```text
candidate_knowledge_time = D.knowledge_time
```

The candidate must not be emitted before `D.knowledge_time`.

### Unresolved State knowledge_time

For unresolved states:

```text
AB_WAITING_FOR_C.state_knowledge_time = B.knowledge_time
ABC_WAITING_FOR_D.state_knowledge_time = C.knowledge_time
INVALIDATED_ABC.state_knowledge_time = invalidating_pivot.knowledge_time
END_OF_SAMPLE_ABC.state_knowledge_time = artifact_end_knowledge_time
```

## Lookahead Avoidance

The future implementation must obey strict causal ordering:

1. A pivot may be used only after its own `knowledge_time`.
2. `AB_WAITING_FOR_C` may be emitted only at `B.knowledge_time`.
3. `ABC_WAITING_FOR_D` may be emitted only at `C.knowledge_time`.
4. A completed `A-B-C-D` candidate may be emitted only at `D.knowledge_time`.
5. No fields from pivots after the current state timestamp may be used in the current state row.
6. No future reactions may be used to decide whether the current state is valid.
7. Rejection and invalidation reasons must be based only on data available at the rejection/invalidation timestamp.
8. Year partitioning must use `candidate_knowledge_time` for completed candidates and `state_knowledge_time` for unresolved states.

## Batch vs Incremental Equivalence Requirement

Before implementation results can be trusted, the future audit must pass a batch-vs-incremental equivalence validation.

Required validation:

1. **Batch pass:** compute candidates from the full trusted artifact while respecting `knowledge_time` rules.
2. **Incremental replay pass:** replay each symbol in strict `knowledge_time` order and emit states/candidates only as they become knowable.
3. The two passes must produce identical rows for:
   - `abcd_candidate_geometry.csv`
   - `abcd_unresolved_states.csv`
   - `abcd_rejection_reasons.csv`
4. Equality must be checked on deterministic IDs and all causal fields, not only aggregate counts.
5. Any mismatch must block use of the audit outputs until explained and fixed.

Required report fields:

- `batch_candidate_count`
- `incremental_candidate_count`
- `batch_unresolved_state_count`
- `incremental_unresolved_state_count`
- `batch_rejection_count`
- `incremental_rejection_count`
- `batch_incremental_match`
- `batch_incremental_mismatch_detail`

## Required Future Outputs

The future audit should produce these research outputs:

### `abcd_candidate_geometry.csv`

One row per accepted completed `A-B-C-D` candidate.

Minimum fields:

- `candidate_id`
- `symbol`
- `a_pivot_id`, `b_pivot_id`, `c_pivot_id`, `d_pivot_id`
- `a_time`, `b_time`, `c_time`, `d_time`
- `a_knowledge_time`, `b_knowledge_time`, `c_knowledge_time`, `d_knowledge_time`
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
- `overlap_policy`
- `a_pivot_reuse_count`, `b_pivot_reuse_count`, `c_pivot_reuse_count`, `d_pivot_reuse_count`
- `ab_leg_reuse_count`, `bc_leg_reuse_count`, `cd_leg_reuse_count`
- `shares_pivot_with_prior_candidate`
- `shares_leg_with_prior_candidate`
- `candidate_independence`

### `abcd_unresolved_states.csv`

One row per unresolved or invalidated partial state.

Minimum fields:

- `state_id`
- `symbol`
- `state`
- `a_pivot_id`, `b_pivot_id`, `c_pivot_id`
- `a_time`, `b_time`, `c_time`
- `a_knowledge_time`, `b_knowledge_time`, `c_knowledge_time`
- `a_column_id`, `b_column_id`, `c_column_id`
- `ab_direction`, `bc_direction`
- `ab_boxes`, `bc_boxes`
- `bc_ab_ratio`
- `invalidation_reason`
- `invalidation_detail`
- `state_knowledge_time`
- `artifact_end_knowledge_time`

### `abcd_ratio_summary.csv`

Aggregate descriptive geometry counts by symmetry zone.

Minimum fields:

- `symmetry_zone`
- `raw_candidate_count`
- `unique_pivot_count`
- `unique_leg_count`
- `median_cd_ab_ratio`
- `avg_cd_ab_ratio`
- `median_bc_ab_ratio`
- `avg_bc_ab_ratio`
- `median_ab_boxes`
- `median_bc_boxes`
- `median_cd_boxes`
- `max_pivot_reuse_count`
- `max_leg_reuse_count`

### `abcd_by_symbol.csv`

Descriptive symbol-level geometry counts.

Minimum fields:

- `symbol`
- `symmetry_zone`
- `raw_candidate_count`
- `unique_pivot_count`
- `unique_leg_count`
- `median_cd_ab_ratio`
- `avg_cd_ab_ratio`
- `median_bc_ab_ratio`
- `avg_bc_ab_ratio`
- `max_pivot_reuse_count`
- `max_leg_reuse_count`

### `abcd_by_year.csv`

Descriptive year-level geometry counts using `candidate_knowledge_time`.

Minimum fields:

- `year`
- `symmetry_zone`
- `raw_candidate_count`
- `unique_pivot_count`
- `unique_leg_count`
- `median_cd_ab_ratio`
- `avg_cd_ab_ratio`
- `median_bc_ab_ratio`
- `avg_bc_ab_ratio`
- `max_pivot_reuse_count`
- `max_leg_reuse_count`

### `abcd_rejection_reasons.csv`

One row per rejected four-pivot window or duplicate/ambiguous source condition.

Minimum fields:

- `candidate_window_id`
- `symbol`
- `a_pivot_id`, `b_pivot_id`, `c_pivot_id`, `d_pivot_id`
- `a_time`, `b_time`, `c_time`, `d_time`
- `rejection_reason`
- `rejection_detail`
- `candidate_knowledge_time`

Expected rejection reasons include:

- `invalid_candidate_direction`
- `non_alternating_ab_bc`
- `non_alternating_bc_cd`
- `ab_cd_direction_mismatch`
- `non_increasing_pivot_order`
- `missing_leg_size`
- `non_positive_leg_size`
- `missing_or_invalid_knowledge_time`
- `untrusted_symbol_or_box_size`
- `duplicate_source_row`
- `ambiguous_duplicate_pivot_identity`
- `duplicate_candidate_endpoints`

### `abcd_design_report.md`

Human-readable descriptive report.

Required sections:

- Scope and restrictions
- Trusted artifacts used
- Dataset coverage
- Duplicate handling summary
- Pivot-construction rules
- Direction-source validation
- Internal-reaction handling
- Unresolved-state summary
- Ratio-measurement definitions
- Symmetry-zone summary
- Overlap and reuse summary
- Symbol validation summary
- Year validation summary
- Rejection-reason summary
- Batch-vs-incremental equivalence validation
- Lookahead controls
- Risks and limitations
- Research-only conclusion

## Minimum Sample-Size Rules

Minimum sample-size rules are descriptive robustness gates, not trading gates.

Required minimums:

1. Overall completed geometry table:
   - At least `1,000` accepted completed candidates before making broad descriptive claims.
2. Unresolved-state accounting:
   - All `AB_WAITING_FOR_C`, `ABC_WAITING_FOR_D`, `INVALIDATED_ABC`, and `END_OF_SAMPLE_ABC` states must be counted before making completion-rate or coverage statements.
3. Per symmetry zone:
   - At least `100` accepted completed candidates per zone for zone-level descriptive comparison.
4. Per symbol and symmetry zone:
   - At least `30` accepted completed candidates per symbol-zone cell.
5. Per year and symmetry zone:
   - At least `30` accepted completed candidates per year-zone cell.
6. Rejection diagnostics:
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
- Report candidate counts, unresolved states, unique pivots, unique legs, and reuse counts by symbol.
- Require each symbol to meet minimum sample-size rules before claiming cross-symbol stability.
- Do not let one high-count symbol dominate the conclusion without separately reporting all three symbols.

## Cross-Year Validation Requirements

The audit must validate geometry across:

- `2024`
- `2025`
- `2026`

Requirements:

- Partition completed candidates by `candidate_knowledge_time` year.
- Partition unresolved states by `state_knowledge_time` year.
- Report candidate counts, unresolved states, unique pivots, unique legs, and reuse counts by year.
- Require each year to meet minimum sample-size rules before claiming cross-year stability.
- Flag missing or malformed timestamps in rejection diagnostics.
- Avoid comparing years if extraction coverage differs materially without documenting the coverage difference.

## Risks and Controls

### Completed-D Survivorship Bias

Risk: completed `A-B-C-D` rows overrepresent structures that survived long enough to form `D`.

Control: preserve `AB_WAITING_FOR_C`, `ABC_WAITING_FOR_D`, `INVALIDATED_ABC`, and `END_OF_SAMPLE_ABC` states and summarize them separately from completed candidates.

### Overlap and Non-Independence

Risk: rolling windows reuse pivots and legs, inflating candidate counts.

Control: allow overlap explicitly, report pivot/leg reuse fields, and label candidate rows as `overlapping_not_independent`.

### Direction Ambiguity

Risk: implementation could inconsistently use `candidate_direction`, `active_direction`, or derived price movement.

Control: define `candidate_direction` as the canonical direction source and reject malformed direction rows.

### Duplicate Endpoint Ambiguity

Risk: duplicate source rows or endpoint tuples create double-counted candidates.

Control: define duplicate source-row, duplicate pivot-identity, and duplicate candidate-endpoint policies.

### Hidden Lookahead

Risk: batch processing could use future pivots to label states before they are observable.

Control: require batch-vs-incremental equivalence on deterministic row IDs and causal fields.

### Unsafe Timestamp Fallback

Risk: `completion_time` may not be the earliest observable timestamp.

Control: require explicit valid `knowledge_time`; reject missing or invalid knowledge time instead of falling back.

### Named-Pattern Bias

Risk: assuming `AB=CD` has predictive value because it is a named pattern.

Control: treat `AB=CD` as geometry only; no signal, setup, strategy, or expectancy conclusions.

### Using Untrusted Boundary Artifacts

Risk: importing conclusions from broken boundary-regime outputs.

Control: only use the trusted validated local artifacts listed in this document.

### Mixing Structural Swings With Harmonic Swings

Risk: combining structural swing logic and harmonic SLOW confirming reactions into one inconsistent pivot chain.

Control: define AB, BC, and CD only from SLOW confirming harmonic reactions in this audit.

## Non-Goals

This design does not authorize:

- implementation of AB=CD
- detector implementation
- setup generation
- strategy logic
- entry rules
- exit rules
- stop-loss or take-profit logic
- expectancy calculations
- backtests
- scanners
- live/demo trader changes
- production-code changes
- boundary-regime re-optimization

## Research-Only Conclusion

This v2 design closes the mandatory pre-implementation gaps by preserving unresolved states, defining `candidate_direction` as the canonical direction source, allowing overlap with explicit reuse reporting, defining duplicate endpoint handling, removing unsafe `knowledge_time` fallbacks, and requiring batch-vs-incremental equivalence validation. The future audit remains descriptive geometry research only.
