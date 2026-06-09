# PnF Harmonic Swing Extraction Design

Date: 2026-06-09
Status: Phase 0 design only
Scope: research-only extraction of harmonic-grade swings from raw completed PnF columns

## Executive Summary

A harmonic swing is a causally confirmed geometric leg extracted directly from raw completed Point-and-Figure (PnF) columns. It is not the same object as a trend-oriented structural swing. A structural swing asks whether the market's dominant structure has reversed. A harmonic swing asks whether a sequence of completed columns has created a clean, measurable pivot-to-pivot leg suitable for future geometry research.

The extraction rule should be deterministic:

> A harmonic swing starts at a confirmed harmonic pivot, extends when same-direction completed columns make a new harmonic extreme, records opposite columns as reactions while they remain below the harmonic reversal threshold, and confirms reversal into a new harmonic swing when an opposite completed-column sequence reaches the configured harmonic pivot threshold before same-direction continuation invalidates it.

This document defines the exact research specification for future implementation. It does not implement code, create a detector, recognize named harmonic patterns, generate candidates, calculate expectancy, change strategy behavior, modify production systems, or change database schema.

## Non-Goals and Restrictions

This design explicitly does not:

- Implement a harmonic swing extractor.
- Implement Gartley, Bat, Butterfly, Crab, Shark, Cypher, AB=CD, or any named harmonic detector.
- Create setup candidates or promote candidates.
- Define entries, stops, targets, position sizing, strategy logic, expectancy, or outcomes.
- Change live trader or demo trader behavior.
- Change production detectors, scanners, or strategy validation interfaces.
- Change database schema or exported production metrics.
- Modify the trend-oriented structural swing aggregation design.

## Input Contract

The extractor consumes raw completed PnF columns sorted by causal availability. A raw column is eligible only after it is complete under the upstream PnF construction rule.

Required input fields per column:

| Field | Required meaning |
|---|---|
| `symbol` | Instrument identifier. |
| `column_id` | Stable source identifier or deterministic ordinal. |
| `ordinal` | Monotonic source order within symbol, used for deterministic tie-breaking. |
| `kind` | `X` for an upward column, `O` for a downward column. |
| `start_ts` | Observable time of the column start. |
| `end_ts` | Observable time of the column final extreme. |
| `completion_time` | First time the downstream research system may know the column is complete. |
| `start_price` | Column start-side price. |
| `end_price` | Column direction-side extreme price. |
| `high` | Column high/top price. |
| `low` | Column low/bottom price. |
| `boxes` | Positive completed-column height in boxes. |
| `box_size` | PnF box size used to construct the column. |
| `reversal_boxes` | PnF reversal setting used to construct the column, if available. |

Ordering rule:

```text
sort_key = (symbol, completion_time, ordinal, column_id)
```

Each symbol is replayed independently. No cross-symbol state is permitted.

## Output Contract

A harmonic swing output row represents one pivot-to-pivot geometric leg at one configured harmonic level.

Required output fields:

| Field | Required meaning |
|---|---|
| `symbol` | Instrument identifier. |
| `harmonic_swing_id` | Stable deterministic ID. |
| `harmonic_level` | Versioned swing granularity, such as `H1`. |
| `direction` | `UP` for X-led harmonic advance; `DOWN` for O-led harmonic decline. |
| `status` | `CONFIRMED`, `PENDING`, or `WITHDRAWN`. Confirmed-only downstream geometry must use only `CONFIRMED`. |
| `start_pivot_id` | Pivot ID at the swing origin. |
| `end_pivot_id` | Pivot ID at the swing endpoint, if confirmed or currently pending. |
| `start_column_id` | First source column in the swing. |
| `end_column_id` | Source column that set the current swing extreme. |
| `confirming_column_id` | Opposite column that confirms the next swing and therefore confirms this swing's endpoint. |
| `start_price` | Pivot price at swing origin. |
| `end_price` | Direction-side pivot price at swing endpoint. |
| `swing_boxes` | Absolute endpoint distance in boxes; not sum of all contained columns. |
| `source_column_ids` | Ordered source column IDs included in the swing interval. |
| `reaction_column_ids` | Opposite columns contained inside the swing but not promoted to pivots at this level. |
| `failed_extension_column_ids` | Same-direction columns that did not make a new harmonic extreme. |
| `start_ts` | Observable time of the origin pivot. |
| `end_ts` | Observable time of the endpoint pivot. |
| `birth_time` | First timestamp the completed swing endpoint is knowable. |
| `knowledge_time` | Timestamp downstream harmonic research may consume this swing. Equal to `birth_time` unless a later audit delay is explicitly configured. |
| `config_version` | Harmonic extraction configuration version. |
| `parent_structural_swing_id` | Optional structural context ID; must not be required for extraction. |

## Core Definitions

### Harmonic pivot

A harmonic pivot is a causally confirmed local PnF extreme at the configured harmonic level. A pivot is confirmed only after an opposite completed-column sequence satisfies the harmonic reversal threshold. The endpoint price may occur before confirmation, but downstream research may not consume the pivot until `knowledge_time`.

### Harmonic swing

A harmonic swing is the price movement from one confirmed harmonic pivot to the next confirmed harmonic pivot of opposite type. An `UP` harmonic swing runs from a harmonic low to a harmonic high. A `DOWN` harmonic swing runs from a harmonic high to a harmonic low.

### Harmonic reversal candidate

A harmonic reversal candidate is one or more opposite completed columns being evaluated as a possible new pivot and next swing. It is not a confirmed pivot until it satisfies the configured harmonic threshold.

### Internal harmonic reaction

An internal harmonic reaction is an opposite column or opposite-column sequence that is visible and recorded but does not satisfy the harmonic reversal threshold at the current level.

### Failed extension

A failed extension is a same-direction completed column after an internal reaction that fails to exceed the active swing's current direction-side extreme. Failed extensions are not ignored; they are recorded as leg-quality metadata and may become lower-level harmonic pivots in a separate hierarchy level.

## Required Configuration

Every future extraction run must predeclare and persist these fields before replay begins:

| Config field | Required default for first research version | Meaning |
|---|---:|---|
| `config_version` | `PNF_HARMONIC_SWING_H1_V0` | Immutable label for the rule set. |
| `harmonic_level` | `H1` | First research harmonic level. |
| `min_pivot_boxes` | `3` | Minimum opposite movement, in boxes, required to confirm a harmonic pivot. |
| `min_pivot_ratio` | `0.236` | Minimum opposite movement relative to active swing boxes. |
| `pivot_threshold_mode` | `MAX_OF_ABSOLUTE_AND_RELATIVE` | Confirmation requires `opposite_boxes >= max(min_pivot_boxes, min_pivot_ratio * active_swing_boxes)`. |
| `candidate_memory` | `RESET_ON_NEW_EXTREME` | Same-direction new extreme invalidates the current opposite candidate. |
| `allow_cumulative_opposite_boxes` | `false` | First version does not accumulate separated opposite columns across same-direction columns. |
| `record_failed_extensions` | `true` | Preserve non-extreme same-direction columns as metadata. |
| `emit_pending_rows` | `false` | Confirmed research exports should be confirmed-only by default. |
| `use_structural_context` | `OPTIONAL_METADATA_ONLY` | Parent structural swings may annotate output but must not define pivots. |

The numeric defaults are research starting points, not promoted strategy values. Changing any value creates a new configuration version.

## Exact Extraction State Machine

The extractor is a per-symbol causal replay state machine.

### State fields

For each symbol and harmonic level, maintain:

- `active_swing`: current harmonic swing under construction.
- `last_confirmed_pivot`: most recent confirmed harmonic pivot.
- `opposite_candidate`: current possible reversal candidate, if any.
- `confirmed_swings`: append-only confirmed harmonic swings.
- `withdrawn_candidates`: optional research-only log of invalidated candidates.

### State 0: no active swing

When the first completed column for a symbol is processed:

1. Create a bootstrap pivot at the column's start-side extreme.
2. Create an active harmonic swing in the column direction.
3. Set `start_column_id` and `end_column_id` to the first column.
4. Set `start_price` to the column start-side price.
5. Set `end_price` to the column direction-side extreme.
6. Set `start_ts` to the column `start_ts`.
7. Set `end_ts` to the column `end_ts`.
8. Set `birth_time` and `knowledge_time` to the column `completion_time` for the pending active swing only.
9. Do not emit a confirmed swing, because no opposite pivot has confirmed the endpoint.

### State 1: active UP swing receives an X column

When the active swing direction is `UP` and a completed `X` column arrives:

1. Add the column ID to `source_column_ids`.
2. If `column.high > active_swing.end_price`, extend the active swing:
   - Set `end_price = column.high`.
   - Set `end_ts = column.end_ts`.
   - Set `end_column_id = column.column_id`.
   - Recompute `swing_boxes = abs(end_price - start_price) / box_size`.
   - If `candidate_memory = RESET_ON_NEW_EXTREME`, mark any `opposite_candidate` as `WITHDRAWN` and clear it.
3. If `column.high <= active_swing.end_price`, record the column as a failed extension:
   - Add it to `failed_extension_column_ids`.
   - Do not change `end_price`, `end_ts`, or `swing_boxes`.
   - Do not confirm or withdraw an opposite candidate solely because of this non-extreme X column.

### State 2: active DOWN swing receives an O column

When the active swing direction is `DOWN` and a completed `O` column arrives:

1. Add the column ID to `source_column_ids`.
2. If `column.low < active_swing.end_price`, extend the active swing:
   - Set `end_price = column.low`.
   - Set `end_ts = column.end_ts`.
   - Set `end_column_id = column.column_id`.
   - Recompute `swing_boxes = abs(end_price - start_price) / box_size`.
   - If `candidate_memory = RESET_ON_NEW_EXTREME`, mark any `opposite_candidate` as `WITHDRAWN` and clear it.
3. If `column.low >= active_swing.end_price`, record the column as a failed extension:
   - Add it to `failed_extension_column_ids`.
   - Do not change `end_price`, `end_ts`, or `swing_boxes`.
   - Do not confirm or withdraw an opposite candidate solely because of this non-extreme O column.

### State 3: active UP swing receives an O column

When the active swing direction is `UP` and a completed `O` column arrives:

1. Add the column ID to `source_column_ids`.
2. Start or update a `DOWN` opposite candidate:
   - If no candidate exists, set candidate start to this O column.
   - Candidate extreme is the lowest low reached by candidate O columns.
   - Candidate boxes are `abs(active_swing.end_price - candidate.low) / box_size`.
3. Compute the required threshold:

```text
required_boxes = max(min_pivot_boxes, min_pivot_ratio * active_swing.swing_boxes)
```

4. If `candidate_boxes < required_boxes`, record the column as an internal harmonic reaction and keep the active UP swing pending.
5. If `candidate_boxes >= required_boxes`, confirm reversal:
   - Confirm the active UP swing endpoint at its current `end_price`.
   - Set the confirmed UP swing `birth_time` and `knowledge_time` to the O column's `completion_time`.
   - Set `confirming_column_id` to the O column ID.
   - Append the confirmed UP swing.
   - Create a confirmed harmonic high pivot at the UP swing endpoint.
   - Create a new active DOWN swing that starts at that high pivot.
   - The new DOWN swing's first source column is the first O column in the opposite candidate.
   - Set the new DOWN swing `end_price` to the candidate low and `end_column_id` to the column that made that low.
   - Set the new DOWN swing pending `birth_time` and `knowledge_time` to the confirming O column's `completion_time`.
   - Clear the opposite candidate.

### State 4: active DOWN swing receives an X column

When the active swing direction is `DOWN` and a completed `X` column arrives:

1. Add the column ID to `source_column_ids`.
2. Start or update an `UP` opposite candidate:
   - If no candidate exists, set candidate start to this X column.
   - Candidate extreme is the highest high reached by candidate X columns.
   - Candidate boxes are `abs(candidate.high - active_swing.end_price) / box_size`.
3. Compute the required threshold:

```text
required_boxes = max(min_pivot_boxes, min_pivot_ratio * active_swing.swing_boxes)
```

4. If `candidate_boxes < required_boxes`, record the column as an internal harmonic reaction and keep the active DOWN swing pending.
5. If `candidate_boxes >= required_boxes`, confirm reversal:
   - Confirm the active DOWN swing endpoint at its current `end_price`.
   - Set the confirmed DOWN swing `birth_time` and `knowledge_time` to the X column's `completion_time`.
   - Set `confirming_column_id` to the X column ID.
   - Append the confirmed DOWN swing.
   - Create a confirmed harmonic low pivot at the DOWN swing endpoint.
   - Create a new active UP swing that starts at that low pivot.
   - The new UP swing's first source column is the first X column in the opposite candidate.
   - Set the new UP swing `end_price` to the candidate high and `end_column_id` to the column that made that high.
   - Set the new UP swing pending `birth_time` and `knowledge_time` to the confirming X column's `completion_time`.
   - Clear the opposite candidate.

## Pivot Price Rules

For an `UP` harmonic swing:

- Origin pivot is the prior confirmed low.
- Endpoint pivot is the highest high reached before an opposite O candidate confirms reversal.
- Endpoint price is not revised after confirmation.

For a `DOWN` harmonic swing:

- Origin pivot is the prior confirmed high.
- Endpoint pivot is the lowest low reached before an opposite X candidate confirms reversal.
- Endpoint price is not revised after confirmation.

If multiple columns set the same endpoint price, choose the earliest column by `(completion_time, ordinal, column_id)` as the endpoint owner and record later equal-price columns as non-extreme continuation metadata.

## Knowledge-Time Rules

The extractor must store both observable event time and causal knowledge time.

1. A raw completed column is knowable at `completion_time`.
2. A bootstrap pending swing is knowable at the first column's `completion_time`, but it is not confirmed.
3. An active swing extension is knowable at the extending column's `completion_time`.
4. An internal reaction is knowable at the reaction column's `completion_time`.
5. A harmonic pivot is confirmed only when the opposite candidate reaches the harmonic threshold.
6. The confirmed swing ending at that pivot has `birth_time = knowledge_time = confirming_column.completion_time`.
7. The new opposite active swing may have a start pivot at a past event time, but it may only be consumed after its own future confirmation.
8. Downstream harmonic geometry must use only confirmed swings whose `knowledge_time` is at or before the research row's allowed time.

## Exact Handling of Common Column Sequences

### `X O X`

- If the `O` does not reach the harmonic reversal threshold and the final `X` makes a new high, the sequence remains one pending or later confirmed `UP` harmonic swing with the `O` recorded as an internal reaction.
- If the `O` reaches the threshold before the final `X`, the initial `UP` swing is confirmed and the final `X` is evaluated against the new active `DOWN` swing as either an internal reaction or a new reversal candidate.
- If the final `X` does not make a new high, record it as a failed extension and keep the unresolved `O` candidate active unless a configured variant says otherwise.

### `O X O`

- If the `X` does not reach the harmonic reversal threshold and the final `O` makes a new low, the sequence remains one pending or later confirmed `DOWN` harmonic swing with the `X` recorded as an internal reaction.
- If the `X` reaches the threshold before the final `O`, the initial `DOWN` swing is confirmed and the final `O` is evaluated against the new active `UP` swing as either an internal reaction or a new reversal candidate.
- If the final `O` does not make a new low, record it as a failed extension and keep the unresolved `X` candidate active unless a configured variant says otherwise.

### `X O X O X`

- If neither `O` reaches the harmonic threshold, the full sequence is one `UP` harmonic swing candidate with two internal reactions, provided the X columns preserve or extend the UP leg according to the extension rules.
- If the first `O` reaches the threshold, the first `UP` swing is confirmed at the first UP endpoint; subsequent `X O X` columns are replayed under the new active state and cannot retroactively erase that confirmation.
- If the first `O` fails, a later new-high `X` withdraws the first O candidate under `RESET_ON_NEW_EXTREME`; the second `O` starts a fresh DOWN candidate.
- If an intervening `X` does not make a new high, it is a failed extension; the current O candidate remains available for confirmation in the first version only if no later new high has invalidated it.

### `O X O X O`

- If neither `X` reaches the harmonic threshold, the full sequence is one `DOWN` harmonic swing candidate with two internal reactions, provided the O columns preserve or extend the DOWN leg according to the extension rules.
- If the first `X` reaches the threshold, the first `DOWN` swing is confirmed at the first DOWN endpoint; subsequent `O X O` columns are replayed under the new active state and cannot retroactively erase that confirmation.
- If the first `X` fails, a later new-low `O` withdraws the first X candidate under `RESET_ON_NEW_EXTREME`; the second `X` starts a fresh UP candidate.
- If an intervening `O` does not make a new low, it is a failed extension; the current X candidate remains available for confirmation in the first version only if no later new low has invalidated it.

## Relationship to Trend-Oriented Structural Swings

Harmonic swing extraction should run from raw completed PnF columns, not from already-aggregated structural swings. Structural swings may be joined later as context using event-time/knowledge-time-safe mapping.

Mapping rule:

```text
parent_structural_swing_id = structural swing whose event interval contains the harmonic swing endpoint or whose confirmed context is known at harmonic knowledge_time
```

The exact mapping variant must be versioned. A harmonic pivot is allowed to occur inside a structural swing and does not need to coincide with a structural reversal.

## Causal Validity Rules

The extractor must fail closed on causality:

1. Never use forming columns.
2. Never use columns ordered after the current column's `completion_time`.
3. Never revise a confirmed harmonic swing endpoint.
4. Never confirm a pivot using future ratio fit, future harmonic pattern completion, future outcome, or future structural state.
5. Never choose a pivot because it makes a later AB/BC/CD ratio cleaner.
6. Never change thresholds inside a replay.
7. Never infer missing `completion_time`; exclude or quarantine rows with missing causal metadata.
8. Never mix symbols during replay.
9. Always record source column IDs for starts, extensions, reactions, failed extensions, and confirmations.
10. Emit confirmed-only files by default; optional pending/withdrawn research logs must be separate artifacts.

## Recommended Future Research Artifacts

A future implementation should write research-only artifacts under an experiment output directory, not production tables:

| Artifact | Contents |
|---|---|
| `harmonic_swings_confirmed.csv` | Confirmed harmonic swings only. |
| `harmonic_pivots_confirmed.csv` | Confirmed harmonic pivot points and confirmation metadata. |
| `harmonic_internal_reactions.csv` | Internal reaction sequences by parent harmonic swing. |
| `harmonic_failed_extensions.csv` | Non-extreme same-direction continuation attempts. |
| `harmonic_withdrawn_candidates.csv` | Invalidated opposite candidates if enabled. |
| `manifest.json` | Input checksums, config version, thresholds, code version, and run timestamp. |

## Open Research Questions

The first implementation should not optimize these settings. It should run one versioned extraction rule and audit the resulting swing distribution. Later isolated experiments may test:

- Whether `min_pivot_boxes = 3` is too small or too large for each symbol/box size.
- Whether `min_pivot_ratio = 0.236` creates stable pivots or over-fragments geometry.
- Whether cumulative opposite boxes improve or degrade harmonic leg quality.
- Whether failed extensions should invalidate unresolved candidates in additional variants.
- Whether hierarchy levels `H2`, `H3`, etc. should be derived by increasing thresholds or by aggregating confirmed lower-level harmonic swings.
- Whether parent structural context improves later harmonic geometry audits without changing harmonic pivot extraction.

## Recommended Next Step

Implement nothing yet. The next research step should be a small manual fixture design document or CSV sketch showing 8-12 raw PnF column sequences and the expected harmonic swing state transitions under `PNF_HARMONIC_SWING_H1_V0`. That fixture should be approved before writing an extractor.
