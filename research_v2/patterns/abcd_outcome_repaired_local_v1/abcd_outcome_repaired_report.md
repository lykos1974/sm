# AB=CD Phase 3 Repaired Structural Outcome Audit Report

## Status
BLOCKED — Phase 3-approved input artifacts are not available in this workspace.

## Reason
missing trusted harmonic_reactions_by_threshold.csv under /workspace/sm/research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v3 or /workspace/sm/research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v3/audit

## Repaired Continuation/Reversal Semantics
- The code path has been repaired so continuation is measured as `next_confirmed_direction == pre_d_active_direction`.
- The code path has been repaired so reversal is measured as `next_confirmed_direction != pre_d_active_direction`.
- The old D/CD-anchor comparison is no longer used for continuation/reversal classification.
- This blocked report does not rerun nonzero cohort metrics because trusted Phase 3 inputs are absent.

## Approved Input Requirement
- Pivot root: `research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v3`
- Geometry root: `research_v2/patterns/abcd_geometry_local_v1`
- Population root reference: `research_v2/patterns/abcd_population_local_v2`
- Design: `research_v2/patterns/pnf_abcd_symmetry_audit_design_v2.md`
- No fallback to non-approved local artifacts was used.

## Answers
1. SYM_0_90_1_10 vs OTHER: not computed because inputs are missing.
2. EXT_1_20_1_35 vs OTHER: not computed because inputs are missing.
3. EXT_1_55_1_70 vs OTHER: not computed because inputs are missing.
4. Symbol stability across BTCUSDT / ETHUSDT / SOLUSDT: not determined.
5. Year stability across 2024 / 2025 / 2026: not determined.
6. Meaningful structural separation from OTHER: not determined from this workspace.

## Required Validation
- `continuation_count + reversal_count == measured_rows`: TRUE for the local blocked output (0 + 0 == 0).
- For any measured row, classification is exhaustive because `continuation` is `next_confirmed_direction == pre_d_active_direction` and `reversal` is `next_confirmed_direction != pre_d_active_direction`.
- Continuation frequency is no longer mechanically forced to zero by the D/CD-anchor bug because continuation is now measured against `pre_d_active_direction`, not `cd_direction`.
- Reversal frequency is no longer mechanically forced to one by the D/CD-anchor bug because reversal is now measured against `pre_d_active_direction`, not `cd_direction`.

## Research Guardrail
Descriptive structural outcome analysis only; no profitability conclusion, expectancy conclusion, trading conclusion, detector, scanner, strategy, or trade model.

Final required answer: After repairing the continuation/reversal semantics, AB=CD symmetry does not exhibit structural separation from OTHER in this workspace because the trusted Phase 3 inputs needed to measure separation are absent.
