# PnF Structural Swing Aggregation Design

## Status and Scope

This is a research-only design for causally aggregating raw completed Point-and-Figure (PnF) columns into larger structural swings.

The design is intentionally limited to structural swing construction. It does not define harmonics, expectancy, strategy rules, live/demo trader behavior, detectors, setup candidates, or candidate promotion. It is meant to create an auditable intermediate structural series that later research scripts can consume without changing production behavior or the current stable baseline.

## Inputs and Terminology

### Raw completed PnF column

A raw completed PnF column is the smallest causal input unit. It is available only after the next opposite column has started far enough to satisfy the PnF reversal rule used by the upstream column builder.

Each completed column should be treated as immutable and should expose at least:

- `symbol`
- `column_id` or chronological ordinal
- `kind`: `X` for rising columns, `O` for falling columns
- `start_ts`
- `end_ts`
- `start_price`
- `end_price`
- `high` and `low` or equivalent top/bottom prices
- `boxes`: positive completed-column height in boxes
- `box_size`
- `completion_time`: the first timestamp at which the column became completed and could be used by downstream research

### Structural swing

A structural swing is a causal aggregation of one or more completed PnF columns in the same dominant structural direction, while allowing smaller opposite columns to remain inside the swing as internal reactions until an opposite move is large enough to confirm reversal.

A structural swing should expose at least:

- `swing_id`
- `symbol`
- `direction`: `UP` for an X-led structural advance, `DOWN` for an O-led structural decline
- `start_column_id`
- `end_column_id`
- `start_ts`
- `end_ts`
- `start_extreme_price`
- `end_extreme_price`
- `swing_boxes`
- `included_column_ids`
- `reaction_column_ids`
- `confirmation_status`: `PENDING` or `CONFIRMED`
- `knowledge_time`
- `confirmed_by_column_id` for confirmed swings
- `reversal_threshold_variant` used by the research run

### Active swing versus pending reversal candidate

At any point in a causal replay there is at most one active structural swing per symbol. An opposite column that does not yet confirm reversal is stored as a pending reversal candidate and/or an internal reaction attached to the active swing. It must not be emitted as a confirmed structural swing until the reversal condition is satisfied.

## 1. When Does a Structural Swing Start?

A structural swing starts when a completed PnF column is the first usable evidence of a new structural direction.

There are two start cases:

1. **Bootstrap start**: the first completed column for a symbol starts the first active structural swing.
   - If the column is `X`, the swing direction is `UP`.
   - If the column is `O`, the swing direction is `DOWN`.
   - The swing start extreme is the column's start-side extreme, and the provisional end extreme is the column's direction-side extreme.
   - The swing is `PENDING` because no opposite structural swing has confirmed against it yet.

2. **Confirmed reversal start**: when an opposite move confirms reversal against the active swing, a new structural swing starts at the first opposite completed column that participated in that reversal candidate, not at the later confirmation column.
   - For an active `UP` swing, the new `DOWN` swing starts at the first `O` column in the reversal candidate sequence.
   - For an active `DOWN` swing, the new `UP` swing starts at the first `X` column in the reversal candidate sequence.
   - This preserves the true structural pivot while assigning `knowledge_time` only when the reversal becomes knowable.

The prior active swing is finalized at the last same-direction extension extreme before the confirmed opposite reversal began. The new swing may have a historical `start_ts`, but it must not be visible to downstream consumers until its `knowledge_time`.

## 2. When Does a Structural Swing Extend?

A structural swing extends when a newly completed column in the same structural direction makes a new direction-side extreme beyond the active swing's current extreme.

For an `UP` swing:

- A completed `X` column extends the swing if its top/high is greater than the active swing's current end extreme.
- The active swing's end extreme, end timestamp, `end_column_id`, and `swing_boxes` update to that new high.
- Any unresolved opposite reaction candidates are retained as internal reaction history but are no longer reversal candidates unless the research variant explicitly keeps multi-leg reaction context.

For a `DOWN` swing:

- A completed `O` column extends the swing if its bottom/low is lower than the active swing's current end extreme.
- The active swing's end extreme, end timestamp, `end_column_id`, and `swing_boxes` update to that new low.
- Any unresolved opposite reaction candidates are retained as internal reaction history but are no longer reversal candidates unless the research variant explicitly keeps multi-leg reaction context.

Same-direction columns that do not make a new extreme are not structural extensions. They may still be recorded as contained columns for auditability, but they should not change the structural end extreme or structural box count.

## 3. When Is an Opposite Column Only an Internal Reaction?

An opposite column is only an internal reaction when it is complete and visible, but it has not satisfied the active research variant's reversal confirmation rule.

For an active `UP` swing, an `O` column is an internal reaction if one or more of the following remain true:

- Its decline is smaller than the minimum absolute reaction boxes required by the variant.
- Its decline is smaller than the minimum reaction ratio required relative to the active `UP` swing's current `swing_boxes`.
- It does not break the active swing's relevant prior reaction low, pivot low, or last structural support threshold if that variant requires such a break.
- It is followed by an `X` column that extends the active `UP` swing before the reversal rule confirms.

For an active `DOWN` swing, an `X` column is an internal reaction under the symmetric conditions:

- Its advance is smaller than the minimum absolute reaction boxes required by the variant.
- Its advance is smaller than the minimum reaction ratio required relative to the active `DOWN` swing's current `swing_boxes`.
- It does not break the active swing's relevant prior reaction high, pivot high, or last structural resistance threshold if that variant requires such a break.
- It is followed by an `O` column that extends the active `DOWN` swing before the reversal rule confirms.

Internal reactions are not discarded. They should be retained inside the active swing as reaction observations with their own completed-column timestamps, size, and maximum adverse/favorable depth. This lets later research test whether small reactions tend to precede continuation or reversal without rewriting the structural swing series.

## 4. When Does an Opposite Move Confirm Reversal?

An opposite move confirms reversal only when the causal evidence available at a completed-column boundary satisfies a predeclared reversal threshold. The design should support multiple research variants, but each run must choose one variant before replay begins and must record it in the output.

The recommended first variant is deliberately simple:

- **Absolute minimum**: the opposite candidate must include at least `min_reaction_boxes` boxes.
- **Relative minimum**: the opposite candidate must reach at least `min_reaction_ratio * active_swing_boxes`.
- **Directional extreme requirement**: the opposite candidate must make a new candidate extreme in its own direction compared with the first opposite column in the candidate.

A reversal is confirmed at the completion time of the column that first satisfies all selected conditions. At that moment:

1. The existing active swing becomes `CONFIRMED`.
2. The new opposite structural swing is created from the first opposite candidate column.
3. The new swing's `knowledge_time` is set to the confirming column's completion time.
4. Columns between the candidate start and confirmation are included in the new swing if they belong to the reversal candidate, while non-confirming counter-columns inside that candidate are marked as internal reactions of the new swing.

A reversal must never be confirmed using columns that are not completed yet, future same-direction extensions, future reaction ratios, future highs/lows, or future outcome information.

## 5. How Are X O X O X Sequences Handled?

An `X O X O X` sequence is handled as a causal contest between an active or emerging `UP` swing and intervening `O` reactions.

### If the first `X` bootstraps or continues an `UP` swing

1. The first `X` starts or extends the `UP` swing.
2. The first `O` becomes an internal reaction unless it confirms a `DOWN` reversal under the selected threshold.
3. The second `X` extends the `UP` swing only if it makes a new high beyond the active swing extreme.
   - If it extends, the first `O` is locked in as an internal reaction.
   - If it does not extend, it is a same-direction non-extension contained in the active structure.
4. The second `O` is evaluated as a fresh or continued `DOWN` reversal candidate depending on the variant's reaction-memory rule.
5. The final `X` extends the `UP` swing if it makes a new high before any `DOWN` reversal has confirmed.

### Structural interpretation

- If neither `O` confirms reversal, the whole sequence remains one `UP` structural swing with two internal reactions.
- If either `O` confirms reversal before a later `X` makes a new `UP` structural high, the prior `UP` swing is confirmed and a new `DOWN` swing starts at the confirming candidate's first `O`.
- If a later `X` makes a new high after a `DOWN` reversal was already confirmed, that `X` is evaluated against the new active `DOWN` swing as a possible internal reaction or `UP` reversal candidate. It cannot retroactively erase the confirmed `DOWN` swing.

## 6. How Are O X O X O Sequences Handled?

An `O X O X O` sequence is the exact downside-symmetric case.

### If the first `O` bootstraps or continues a `DOWN` swing

1. The first `O` starts or extends the `DOWN` swing.
2. The first `X` becomes an internal reaction unless it confirms an `UP` reversal under the selected threshold.
3. The second `O` extends the `DOWN` swing only if it makes a new low beyond the active swing extreme.
   - If it extends, the first `X` is locked in as an internal reaction.
   - If it does not extend, it is a same-direction non-extension contained in the active structure.
4. The second `X` is evaluated as a fresh or continued `UP` reversal candidate depending on the variant's reaction-memory rule.
5. The final `O` extends the `DOWN` swing if it makes a new low before any `UP` reversal has confirmed.

### Structural interpretation

- If neither `X` confirms reversal, the whole sequence remains one `DOWN` structural swing with two internal reactions.
- If either `X` confirms reversal before a later `O` makes a new `DOWN` structural low, the prior `DOWN` swing is confirmed and a new `UP` swing starts at the confirming candidate's first `X`.
- If a later `O` makes a new low after an `UP` reversal was already confirmed, that `O` is evaluated against the new active `UP` swing as a possible internal reaction or `DOWN` reversal candidate. It cannot retroactively erase the confirmed `UP` swing.

## 7. How Is `knowledge_time` Assigned?

`knowledge_time` is the earliest timestamp at which a structural swing state is knowable using only completed PnF columns.

Assignment rules:

- For the bootstrap active swing, `knowledge_time` is the first completed column's `completion_time`.
- For a same-direction extension, the updated active swing extreme is knowable at the extending column's `completion_time`.
- For an internal reaction, the reaction observation is knowable at that opposite column's `completion_time`, but it does not create a new structural swing.
- For a confirmed reversal, the new swing's `start_ts` is the first opposite candidate column's start time, but its `knowledge_time` is the confirming column's `completion_time`.
- For the prior swing finalized by the reversal, its final version also receives or updates `knowledge_time` to the confirming column's `completion_time`, because only then do we know that no later same-direction column inside the pending contest will extend it before reversal confirmation.

The output should distinguish event time from knowledge time. `start_ts` and `end_ts` describe where the swing occurred on the chart. `knowledge_time` describes when the research process was allowed to know and use the swing.

## 8. How Do We Avoid Lookahead?

Lookahead is avoided by replaying completed columns strictly in chronological availability order and by making all state transitions at completed-column boundaries.

Required guardrails:

1. **Use only completed columns**: no forming PnF column may start, extend, or confirm a structural swing.
2. **Use availability ordering**: sort by `completion_time`, then a deterministic ordinal for ties.
3. **Predeclare thresholds**: minimum boxes, ratios, and break requirements must be fixed before replay starts.
4. **Do not relabel emitted history retroactively for consumers**: a swing can have an event-time start in the past, but it becomes available only at `knowledge_time`.
5. **Record lifecycle status**: pending active swings and confirmed swings must be distinguishable so audits can choose confirmed-only inputs.
6. **No outcome coupling**: no trade result, expectancy measure, future return, candidate label, or later strategy filter may affect structural aggregation.
7. **No cross-symbol leakage**: each symbol's structural state is replayed independently.
8. **Deterministic tie handling**: if multiple columns have the same `completion_time`, process by source ordinal and record that ordinal.
9. **Immutable raw columns**: once a completed column is consumed, its fields are never revised by later structural logic.
10. **Auditable provenance**: each structural swing records the raw column IDs that created, extended, reacted inside, and confirmed it.

## 9. What Minimum Reaction Concepts Should Be Tested Later?

The aggregation design should remain neutral and let later research compare alternative minimum reaction definitions. The first concepts to test should be structural, not strategic:

1. **Absolute reaction boxes**: require at least `N` boxes in the opposite direction before reversal confirmation.
2. **Relative reaction ratio**: require `opposite_candidate_boxes / active_swing_boxes >= R`.
3. **Prior reaction break**: require the opposite candidate to break the most recent internal reaction extreme.
4. **Swing-origin break**: require the opposite candidate to violate the active swing's origin-side extreme or a nearby structural pivot.
5. **Two-column confirmation**: require at least two opposite columns, or an opposite-same-opposite sequence, before confirming reversal.
6. **Continuation invalidation**: cancel an opposite candidate if the active swing extends to a new extreme before confirmation.
7. **Cumulative reaction depth**: allow multiple opposite columns separated by non-extending same-direction columns to accumulate into one reversal candidate.
8. **Single-column reaction depth**: require one completed opposite column to satisfy the full reversal threshold by itself.
9. **Box-size-normalized thresholds**: test whether fixed box counts need different values by symbol, volatility regime, or PnF box size.
10. **Time-persistence filter**: require the reaction candidate to persist across a minimum number of completed columns, while still avoiding candle or trade outcome information.

These concepts are experiment candidates only. They should be evaluated in isolated research runs and should not imply candidate promotion or production detector behavior.

## 10. How Will This Feed `structural_reaction_ratio_audit.py`?

`structural_reaction_ratio_audit.py` expects a CSV of confirmed structural swings and computes ratios between consecutive opposite-direction confirmed swings per symbol. This design should feed that audit by exporting confirmed swings with field names already accepted by the audit script.

Recommended export columns:

- `symbol`
- `swing_id`
- `direction`: `UP` or `DOWN`
- `swing_boxes`
- `start_ts`
- `end_ts`
- `knowledge_time`
- `confirmation_status`: `CONFIRMED`
- `is_confirmed`: `true`
- `start_extreme_price`
- `end_extreme_price`
- `box_size`
- `start_column_id`
- `end_column_id`
- `confirmed_by_column_id`
- `included_column_ids`
- `reaction_column_ids`
- `reversal_threshold_variant`

The audit then computes:

```text
reaction_ratio = reaction_swing_boxes / prior_swing_boxes
```

where `prior_swing` and `reaction_swing` are consecutive confirmed swings of opposite direction for the same symbol.

Important integration rules:

- Export only confirmed structural swings to the confirmed-swings CSV used by the audit, or include lifecycle fields that cause pending rows to be excluded.
- Preserve `UP`/`DOWN` direction values or accepted `X`/`O` aliases.
- Ensure `swing_boxes` is positive and represents the structural extreme-to-extreme distance, not the sum of all contained column heights.
- Sort deterministically by `symbol`, `knowledge_time`, `start_ts`, `end_ts`, and source ordinal before export.
- Keep `knowledge_time` available for future causal audits even though the current reaction-ratio audit orders by swing event timestamps.
- Do not add expectancy, strategy labels, candidate IDs, trade outcomes, or live/demo execution fields to this structural audit feed.

## Open Research Decisions

The following decisions should be made by isolated future research experiments, not by this design document:

- Default values for `min_reaction_boxes` and `min_reaction_ratio`.
- Whether reversal candidates reset immediately on a same-direction non-extension.
- Whether cumulative reaction depth should span multiple opposite columns separated by non-extending same-direction columns.
- Whether `knowledge_time` ordering should replace event-time ordering in future versions of reaction-ratio audits.
- Whether confirmed structural swing exports should include both event-time and knowledge-time sequence numbers.

## Non-Goals

This design does not:

- Detect harmonic patterns.
- Calculate or optimize expectancy.
- Define entry, exit, stop, target, or risk rules.
- Promote candidates or setups.
- Modify live/demo trader behavior.
- Change production scanners or detectors.
- Change database schema.
- Change protected strategy validation interfaces.
