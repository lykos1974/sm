# AB=CD Phase 3 Outcome Direction Sanity Audit (Research Only)

## Scope and guardrails

- Research-only sanity audit of the Phase 3 outcome direction labels.
- No production code was changed.
- No AB=CD logic, geometry logic, harmonic rules, or outcome definitions were modified.
- No detector, strategy, expectancy, PnL, entry, exit, or trade model was created.

## Local reproducibility status

The exact nonzero validated output described in the request is not present in this workspace. The checked file `research_v2/patterns/abcd_outcome_local_v1/abcd_outcome_report.md` is currently a blocked report, and the code's default trusted input root `research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v3` is absent. Therefore the requested 25 BTC + 25 ETH + 25 SOL non-fabricated completed-candidate examples cannot be reproduced from the current workspace artifacts.

The audit below is therefore a code-path sanity audit plus a validation of the locally present blocked summary. It identifies the mechanism that would force 0% continuation / 100% reversal whenever resolved rows are produced from the current AB=CD outcome implementation and the confirmed-swing stream alternates direction.

## 1. How continuation is currently defined

### AB=CD Phase 3 implementation

- Source object: `Outcome.same_direction_as_cd`.
- Source file: `research_v2/patterns/pnf_abcd_outcome_audit.py`.
- D direction variable used: `d.candidate_direction` in rolling-pivot mode; `cd_direction` read from the geometry candidate file in geometry-file mode.
- Next swing direction variable used: `next_pivot.candidate_direction`.
- Comparison logic:
  - rolling-pivot mode: `same = next_direction == d.candidate_direction`.
  - geometry-file mode: `same = next_direction == cd_direction`.
- Summary logic: continuation count is `sum(1 for row in resolved if row.same_direction_as_cd is True)`.

Result: in the current AB=CD outcome audit, **continuation means the next confirmed swing has the same direction as CD/D**.

### Prior ratio-predictive definition for comparison

The ratio-predictive audit defines continuation differently: it compares `next_confirmed.candidate_direction` to the audited reaction's `active_direction`, and defines reversal by comparison to the audited reaction's `candidate_direction`.

For a D pivot in an AB=CD sequence, the reaction/candidate direction is CD/D, while the active direction before D is BC/C. Therefore the AB=CD Phase 3 implementation is directionally opposite to the earlier ratio-predictive continuation/reversal semantics.

## 2. How reversal is currently defined

### AB=CD Phase 3 implementation

- Source object: `Outcome.opposite_direction_to_cd`.
- Source file: `research_v2/patterns/pnf_abcd_outcome_audit.py`.
- D direction variable used: `d.candidate_direction` in rolling-pivot mode; `cd_direction` from geometry candidate rows in geometry-file mode.
- Next swing direction variable used: `next_pivot.candidate_direction`.
- Comparison logic:
  - rolling-pivot mode: `opposite = next_direction != d.candidate_direction`.
  - geometry-file mode: `opposite = next_direction != cd_direction`.
- Summary logic: reversal count is `sum(1 for row in resolved if row.opposite_direction_to_cd is True)`.

Result: in the current AB=CD outcome audit, **reversal means the next confirmed swing has any direction other than CD/D**. Because the accepted direction universe is only `UP`/`DOWN`, this is equivalent to the next confirmed swing being the opposite of CD/D for resolved rows.

## 3. Direction trace sample status

The requested sample table was created at `outcome_direction_trace_sample.csv`, but it contains explicit blocked-status rows instead of fabricated examples. The reason is that the current workspace lacks the trusted Phase 3 validated inputs and the local `abcd_outcome_local_v1` report is blocked/zero-row.

## 4. Mathematical validation

The validation table was created at `outcome_classification_validation.csv`.

For resolved rows under the current implementation:

```text
same_direction_as_cd = (next_direction == cd_direction)
opposite_direction_to_cd = (next_direction != cd_direction)
```

Therefore, for every resolved row, exactly one of the two booleans is true. Consequently:

```text
continuation_count + reversal_count = total_measured_rows
```

This identity is guaranteed by the implementation for every cohort with resolved rows, independent of market data.

## 5. Can continuation ever occur under the current definitions?

Continuation can occur only if the next confirmed pivot after D has the same `candidate_direction` as D/CD.

However, the harmonic threshold engine records a confirming reaction and then promotes the candidate direction into the new active swing. The next confirmed reaction after that active swing is, by construction, a reaction in the opposite direction. In a normal alternating confirmed-swing stream, this means:

```text
D/CD candidate_direction = X
next confirmed swing candidate_direction = opposite(X)
```

Under the current AB=CD outcome definition, that row is always counted as reversal, not continuation. Thus a 0% continuation / 100% reversal result is expected whenever:

1. the next-confirmed-swing matcher selects the immediately next later same-symbol confirmed swing, and
2. the confirmed-swing stream alternates direction without duplicate same-direction confirmed pivots.

A continuation row would require a same-direction confirmed pivot immediately after D, which would violate the normal confirmed-swing alternation implied by the active/candidate promotion flow.

## 6. Direction inversion audit

| Field | Source | Meaning in source flow | AB=CD outcome use | Finding |
|---|---|---|---|---|
| `active_direction` | harmonic reaction rows | Direction of the active swing before the candidate reaction confirms | Not carried into `Outcome` | Dropped before Phase 3 classification. |
| `candidate_direction` | harmonic reaction rows | Direction of the candidate reaction pivot | Loaded into `Pivot.candidate_direction` | Used as the D/CD direction. |
| `cd_direction` | geometry candidate rows | Direction of D/CD leg | Used as the classification anchor | Treated as continuation anchor. |
| `next_confirmed_direction` | next later same-symbol confirmed pivot | Direction of the next confirmed reaction after D | Compared to CD/D | Opposite-CD rows are labeled reversal. |

The likely integrity issue is not geometry and not harmonic-rule logic. The issue is that Phase 3 AB=CD outcome labels use D/CD as the continuation anchor, while the earlier ratio-predictive outcome semantics define continuation as a return to the active-swing direction and reversal as a move in the reaction/candidate direction.

## 7. Final classification

The observed 0% continuation / 100% reversal result is best classified as a **DIRECTION-INVERSION BUG** relative to the prior ratio-predictive continuation/reversal semantics, and as a **DEFINITIONAL ARTIFACT** under the literal current AB=CD Phase 3 implementation.

Final required sentence: The observed 0% continuation / 100% reversal outcome is DIRECTION-INVERSION BUG because the Phase 3 AB=CD outcome audit anchors continuation to the D/CD candidate direction instead of the pre-D active/BC direction, so a normally alternating next confirmed swing is mechanically labeled reversal.
