# ABCD Strategy State

## Executive summary

This report synthesizes the ABCD conclusions already committed under `research_v2/patterns/abcd_*` and the adjacent ABCD design/code artifacts. It does not add new research, rerun historical analysis, or create datasets.

The committed evidence does **not** support an executable ABCD trading edge today. The strongest completed finding is negative: the original Phase 3 continuation/reversal result is a direction-inversion / definitional artifact, not a tradable market behavior. The only nonzero completed metric set in the committed ABCD outputs is the outcome-distance audit, which is useful for methodology but does not establish expectancy, profitability, or an entry/exit rule.

If forced to build an ABCD production strategy today using only existing evidence, the exact rule set would be a **no-trade production guardrail strategy**:

1. Detect or annotate ABCD context only if a future detector follows the documented v2 pivot model.
2. Do not enter trades from ABCD symmetry, extension, BAMM, PRZ, retest, or next-pivot labels.
3. Do not use the original Phase 3 continuation/reversal labels.
4. Do not promote any ABCD rule above the existing stable baseline until a later committed study demonstrates positive expectancy and stability.

In short: **ABCD is not production-tradable on the current committed evidence.**

## Existing evidence

| Artifact / study | Hypothesis tested | Metrics produced | Conclusion reached | Result | Confidence | Production rule usable? |
|---|---|---|---|---|---|---|
| Symmetry audit design v1 | PnF-native SLOW confirming swing sequences may exhibit stable AB=CD / swing-symmetry geometry across symbols and years. | No empirical metrics; design-only definitions for pivots, AB/BC/CD legs, ratio zones, and guardrails. | Descriptive geometry research was scoped, but not converted into a signal, setup, entry, exit, or expectancy model. | Inconclusive | High confidence as design governance; no statistical confidence as edge evidence. | No. Design rules only. |
| Symmetry audit design v2 | The ABCD audit design needed tighter canonical rules for direction, ordering, unresolved states, and survivorship-bias prevention. | No empirical metrics; canonical pivot model, direction source, ordering rules, unresolved-state requirements, rejection reasons. | Future ABCD research should use SLOW `CONFIRMING` pivots, `candidate_direction`, `knowledge_time` ordering, alternating-leg checks, and unresolved-state preservation. | Positive for research hygiene; inconclusive for edge. | High for methodology. | No trading rule; yes as implementation guardrail only. |
| Geometry input diagnostic | Determine whether the geometry-audit issue was caused by ABCD geometry logic. | Script/path comparison and rejection-reason diagnostic. | The artifact reached no ABCD market, geometry, or edge conclusion; it is operational only. | Inconclusive for edge. | High for diagnostic scope; none for trading evidence. | No. |
| Population audit local v1 | ABCD population size may be sufficient and stable enough across symbols/years to justify later symmetry research. | Empty/zero population summary tables. | No population-size, rarity/abundance, symbol-stability, year-stability, or Phase 2 sufficiency conclusion was reached. | Inconclusive | Low for edge; no measured population evidence. | No. |
| Original Phase 3 structural outcome audit local v1 | Symmetry and extension zones may differ from OTHER in future swing size and continuation/reversal frequency, with symbol/year stability. | Cohort summary rows for `SYM_0_90_1_10`, `EXT_1_20_1_35`, `EXT_1_55_1_70`, and `OTHER`, all with count `0`. | No symmetry-zone or extension-zone structural separation was established. | Inconclusive / no edge shown. | Low for edge; zero measured cohort evidence in the committed summary. | No. |
| Outcome direction sanity audit local v1 | The observed 0% continuation / 100% reversal result may be real market behavior or a definitional artifact. | Direction-flow code-path audit and classification identity validation. | The original 0% continuation / 100% reversal result is a **DIRECTION-INVERSION BUG** and **DEFINITIONAL ARTIFACT** because continuation was anchored to D/CD instead of the pre-D active direction. | Negative | High. The finding follows from the implemented classification identity and normal alternating confirmed-swing flow. | Yes, as a negative rule: never use the original labels for production. |
| Repaired Phase 3 structural outcome audit local v1 | After fixing semantics, symmetry/extension cohorts may show structural separation from OTHER. | Cohort summary rows for `SYM_0_90_1_10`, `EXT_1_20_1_35`, `EXT_1_55_1_70`, and `OTHER`, all with count/measured rows `0`; continuation/reversal counts `0`. | Semantics were repaired, but no cohort separation was measured in the committed summary. | Inconclusive / no edge shown. | Low for edge; high that repaired semantics are the correct direction if the audit is rerun later. | No. |
| Outcome distance audit local v1 | The first-next-confirmed-pivot outcome metric may be tautological if it is almost always the immediate next structural pivot. | 7,820 measured rows; median column distance `1`; average `6.7537`; p90 `11`; pct distance `=1` is `0.5425831202`; pct distance `<=3` is `0.7514066496`; cohort counts: `SYM_0_90_1_10=854`, `EXT_1_20_1_35=548`, `EXT_1_55_1_70=328`, `OTHER=6090`. | The metric is not proven fully tautological by a one-column test, but it remains structurally constrained to the first confirmed pivot; later-horizon research is needed. | Positive for methodology; negative for production edge. | Medium-high for this metric’s descriptive conclusion because sample size is 7,820, but low for trading because no PnL/expectancy was tested. | No trading rule. Use only as an outcome-design warning. |


### ABCD code artifacts without committed result metrics

These files are ABCD research procedures, not completed evidence reports in the committed `abcd_*` output set. They are included here for scope completeness; because they do not themselves contain completed metric tables and final empirical conclusions, none is production-usable as a trading rule.

| Artifact | Hypothesis/procedure encoded | Metrics in committed report? | Conclusion reached in committed evidence | Result | Confidence | Production rule usable? |
|---|---|---|---|---|---|---|
| `pnf_abcd_population_audit.py` | Build ABCD population states. | No completed nonzero metrics in committed output. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_geometry_audit.py` | Build descriptive ABCD geometry candidates and zones. | No committed geometry report metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_outcome_audit.py` | Measure structural outcomes after D. | Original committed summary has zero cohort counts and later sanity audit rejects original labels. | Original labels are not production-usable. | Negative / inconclusive | High for rejection; low for edge. | No. |
| `pnf_abcd_outcome_distance_audit.py` | Measure distance from D to next confirmed pivot. | Yes: 7,820 measured rows in distance output. | Useful horizon diagnostic; no trading edge. | Methodology-positive, edge-negative | Medium-high for diagnostic. | No. |
| `pnf_abcd_bamm_trigger_population_audit.py` | Identify BAMM trigger population from existing geometry. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_bamm_chronology_audit.py` | Measure B-break versus D chronology. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_bamm_window_audit.py` | Measure BAMM window from B-break to D. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_bamm_window_quality_audit.py` | Classify structural quality inside valid BAMM windows. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_bamm_value_audit.py` | Compare D-completion frequency for BAMM versus non-BAMM. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_prz_convergence_audit.py` | Measure PRZ convergence around projected D. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_prz_confirmation_confluence_audit.py` | Combine PRZ validity with D-confirmation context. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_prz_overshoot_audit.py` | Measure overshoot beyond PRZ / D. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_confirmation_threshold_audit.py` | Evaluate post-D confirmation thresholds. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_d_confirmation_audit.py` | Audit D confirmation behavior. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_d_reaction_audit.py` | Audit post-D reaction behavior. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_d_mfe_audit.py` | Audit maximum favorable excursion after D. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_retest_feasibility_audit.py` | Test structural retest feasibility after PRZ confirmation. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_retest_entry_level_audit.py` | Compare retest-depth entry levels. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_entry_timing_audit.py` | Evaluate entry timing alternatives. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_structural_invalidation_audit.py` | Measure structural adverse pivots / stop feasibility. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_execution_context_v1.py` | Build execution context from prior research artifacts. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_candle_reality_sim_v1.py` | Simulate candle-level reality for retest candidates. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_price_mode_reality_sim_v1.py` | Simulate structural/price-mode target-stop ordering. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_price_mode_trade_diagnostics_v1.py` | Diagnose price-mode trade classifications. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_reaction_size_threshold_curve_v1.py` | Relate post-D reaction-size thresholds to existing target-first rates. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |
| `pnf_abcd_pnl_prototype_audit.py` | Prototype PnL audit from existing artifacts. | No committed result metrics. | No edge conclusion. | Inconclusive | Low for edge. | No. |

## Strong findings

1. **Original Phase 3 continuation/reversal labels are rejected.** The sanity audit concludes that the 0% continuation / 100% reversal result is a direction-inversion bug and definitional artifact. This is the strongest committed ABCD conclusion.
2. **First-next-pivot outcome is insufficient as a production edge metric.** The distance audit shows the first next pivot is not always immediate, but the horizon is still structurally constrained and does not define profit, loss, entry, stop, target, or expectancy.
3. **The v2 geometry rules are the best available governance for any future ABCD detector.** The committed design supports `candidate_direction`, SLOW `CONFIRMING` pivots, `knowledge_time` ordering, alternating-leg checks, and unresolved-state preservation.

## Weak findings

1. **ABCD population sufficiency is unproven.** The committed population summary provides no measured population evidence.
2. **ABCD symmetry and extension zones have no committed positive edge.** Original and repaired outcome summaries do not show nonzero cohort separation.
3. **BAMM, PRZ, retest, candle/price-mode, structural invalidation, and PnL scripts are not completed evidence by themselves.** They define research procedures and guardrails, but the committed `abcd_*` evidence set does not contain matching completed metric reports for production synthesis.

## Contradictions

1. **Original outcome labels conflict with prior continuation semantics.** The sanity audit resolves this by rejecting the original labels as direction-inverted.
2. **ABCD geometry design is descriptive, while strategy language appears in later script names.** The committed design and code guardrails repeatedly restrict the work to research-only use unless completed evidence later supports promotion.
3. **Outcome distance has nonzero descriptive metrics, while outcome edge remains unproven.** This is not a contradiction in result quality: distance metrics describe horizon mechanics, not trade expectancy.

## Missing pieces

The existing evidence does not provide:

1. A nonzero population sufficiency conclusion.
2. A nonzero repaired outcome-separation conclusion.
3. Any positive expectancy, PnL, win rate, drawdown, or risk/reward metric for an ABCD trading model.
4. Symbol/year stability for a production ABCD rule.
5. Validated stop, target, entry, position sizing, or invalidation rules.
6. A comparison showing ABCD improves on the current stable rollback baseline.

## Rejected findings

1. **“ABCD next pivot is 100% reversal”** — rejected as a direction-inversion / definitional artifact.
2. **“Original Phase 3 continuation/reversal supports a strategy”** — rejected.
3. **“Symmetry or extension zones produce edge”** — not established; no production rule is supported.
4. **“First-next-pivot behavior is enough for production”** — rejected; later-horizon and trade-level metrics are required.
5. **“ABCD scripts imply production readiness”** — rejected; scripts are not evidence without completed metrics and conclusions.

## Inconclusive findings

1. Whether completed ABCD structures are abundant enough for trading research.
2. Whether `SYM_0_90_1_10`, `EXT_1_20_1_35`, or `EXT_1_55_1_70` outperform OTHER after repaired semantics.
3. Whether ABCD behavior is stable across symbols and years.
4. Whether BAMM/PRZ/retest conditions create edge under any subset.
5. Whether a price-mode or candle-reality trade implementation would be profitable.

## Proposed production strategy

### Strategy name

`ABCD_NO_TRADE_GUARDRAIL_V1`

### Exact production rules

#### Universe

- All existing production symbols may remain in scope.
- ABCD must not remove symbols from the existing baseline universe.

#### Signal generation

- Generate **no executable long signal** from ABCD.
- Generate **no executable short signal** from ABCD.
- Generate **no position-sizing adjustment** from ABCD.
- Generate **no target, stop, breakeven, or exit adjustment** from ABCD.

#### Allowed annotation only

A non-trading annotation may be emitted only as:

```text
ABCD_CONTEXT_ONLY
```

This annotation may be attached when a future detector can identify an ABCD structure using the v2 design guardrails:

1. SLOW `CONFIRMING` pivots only.
2. `candidate_direction` as canonical pivot/leg direction.
3. `knowledge_time` ordering.
4. Alternating AB/BC/CD direction checks.
5. Unresolved-state preservation.

The annotation must not affect orders, alerts that imply actionability, ranking, sizing, stops, or targets.

#### Explicitly forbidden rules

Do **not** implement any of the following as production trading rules:

1. Buy or sell because `CD/AB` is in `0.90–1.10`.
2. Buy or sell because `CD/AB` is in `1.20–1.35`.
3. Buy or sell because `CD/AB` is in `1.55–1.70`.
4. Buy or sell because the next confirmed pivot after D has a particular direction.
5. Use the original Phase 3 continuation/reversal labels.
6. Use BAMM, PRZ, retest, candle-reality, price-mode, or PnL-prototype script names as production evidence.

#### Decision output

For every ABCD context encountered today, the production decision must be:

```text
NO_TRADE_ABCD_EVIDENCE_INSUFFICIENT
```

#### Promotion requirement

ABCD may only become tradable after a later committed study demonstrates all of the following:

1. Nonzero candidate population.
2. Repaired, nonzero outcome separation.
3. Positive expectancy/PnL after realistic entry, stop, target, and ambiguity handling.
4. Symbol/year stability.
5. Improvement over the current stable rollback baseline.

### Final answer

If I had to build an ABCD production strategy today using only existing evidence, I would implement **no ABCD trades**. The only production-safe behavior is a disabled/guardrail strategy that can optionally annotate `ABCD_CONTEXT_ONLY` while always returning `NO_TRADE_ABCD_EVIDENCE_INSUFFICIENT`.

## Top 5 production candidate strategies for future validation

These are **candidate designs only**. They are ranked from most promising to least promising using only the conclusions already summarized above. None is approved for live trading because the current committed evidence shows no validated ABCD edge, no positive expectancy, and no production-ready stop/target model.

### Ranking summary

| Rank | Candidate | Core idea | Confidence | Estimated probability of becoming profitable after validation |
|---:|---|---|---|---:|
| 1 | `ABCD_NO_TRADE_GUARDRAIL_V1` | Production-safe context annotation only. | High | 100% chance of avoiding ABCD-driven losses; 0% chance of producing standalone ABCD profit. |
| 2 | `ABCD_REPAIRED_SEMANTICS_REVERSAL_CANDIDATE` | Trade only after repaired continuation/reversal semantics can observe a post-D reversal away from CD. | Low | 20% |
| 3 | `ABCD_LATER_HORIZON_BC_BREAK_CANDIDATE` | Use the distance-audit warning by requiring a later structural break, not the first next pivot. | Low | 18% |
| 4 | `ABCD_SYMMETRY_ZONE_CONFIRMATION_CANDIDATE` | Test near-equal `CD/AB` symmetry only with an additional post-D confirmation. | Low | 15% |
| 5 | `ABCD_EXTENSION_EXHAUSTION_CANDIDATE` | Test extended `CD/AB` zones as possible exhaustion only after confirmation. | Low | 12% |

### 1. `ABCD_NO_TRADE_GUARDRAIL_V1`

**Rank:** 1 — most promising for immediate production because it is the only candidate fully supported by the current evidence.

**Exact entry**

- No trade entry is permitted.
- If ABCD context is detected, emit annotation only:

```text
ABCD_CONTEXT_ONLY
```

**Exact stop**

- No stop is placed because no position is opened.

**Exact target**

- No target is placed because no position is opened.

**Filters**

- ABCD context may be annotated only if it follows the v2 design guardrails:
  1. SLOW `CONFIRMING` pivots only.
  2. `candidate_direction` as canonical pivot/leg direction.
  3. `knowledge_time` ordering.
  4. Alternating AB/BC/CD direction checks.
  5. Unresolved-state preservation.
- Original Phase 3 continuation/reversal labels are forbidden.
- First-next-pivot behavior is not allowed as a trading edge.

**Why it follows from the existing evidence**

- The current synthesis concludes that no committed ABCD evidence supports executable entries, stops, targets, or positive expectancy.
- The strongest finding is a negative one: original Phase 3 continuation/reversal labels must not be used.
- The outcome-distance audit supports only an outcome-design warning, not a trade rule.

**Confidence**

High for production safety; zero confidence as a profit-generating strategy.

**Expected weaknesses**

- Produces no standalone ABCD profit.
- Cannot capture any real ABCD edge if one exists but remains unvalidated.

**Estimated probability of becoming profitable after validation**

- As a no-trade strategy: 0% standalone profit probability.
- As a production guardrail that prevents invalid ABCD trades: 100% probability of avoiding ABCD-driven losses from unvalidated rules.

### 2. `ABCD_REPAIRED_SEMANTICS_REVERSAL_CANDIDATE`

**Rank:** 2 — most promising *tradable* candidate because it directly incorporates the strongest negative finding: the original direction labels were wrong, so any tradable candidate must use repaired semantics.

**Exact entry**

- Candidate is disabled by default.
- If later validation enables it, enter only after a completed ABCD candidate and a post-D confirmed pivot classified with repaired semantics.
- Long entry:
  - `CD` direction is `DOWN`.
  - The first accepted post-D confirmed pivot direction is `UP` under repaired semantics.
  - Enter long at the close/knowledge time of that confirming pivot.
- Short entry:
  - `CD` direction is `UP`.
  - The first accepted post-D confirmed pivot direction is `DOWN` under repaired semantics.
  - Enter short at the close/knowledge time of that confirming pivot.

**Exact stop**

- Long stop: one validated PnF box below the D extreme.
- Short stop: one validated PnF box above the D extreme.

**Exact target**

- Primary target: `2R` from entry.
- If structural levels are available, cap the target at the C level if C is reached before `2R`.

**Filters**

- Use only v2 geometry rules.
- Require repaired continuation/reversal semantics; original Phase 3 labels are forbidden.
- Exclude candidates where the first accepted post-D confirmation is also the same event used to form D.
- Exclude candidates unless stop distance is positive and finite.

**Why it follows from the existing evidence**

- It follows from the sanity audit by avoiding the rejected D/CD-anchored label bug.
- It does not claim existing edge; it is a candidate created from the repaired-definition requirement.

**Confidence**

Low. The repaired semantics are methodologically supported, but the committed repaired outcome summary does not show positive cohort evidence.

**Expected weaknesses**

- May simply trade normal alternating pivots without edge.
- Stop and target are not validated by committed ABCD evidence.
- Entry may be late because it waits for a confirmed post-D pivot.

**Estimated probability of becoming profitable after validation**

20%.

### 3. `ABCD_LATER_HORIZON_BC_BREAK_CANDIDATE`

**Rank:** 3 — uses the outcome-distance conclusion that first-next-pivot behavior is too constrained and later horizons should be considered.

**Exact entry**

- Candidate is disabled by default.
- Long entry:
  - Completed ABCD has `CD` direction `DOWN`.
  - Within the next 3 confirmed pivots after D, price/structure breaks the B or C structural level upward, whichever is closer to entry and available.
  - Enter long at the knowledge time of that structural break.
- Short entry:
  - Completed ABCD has `CD` direction `UP`.
  - Within the next 3 confirmed pivots after D, price/structure breaks the B or C structural level downward, whichever is closer to entry and available.
  - Enter short at the knowledge time of that structural break.

**Exact stop**

- Long stop: one validated PnF box below D.
- Short stop: one validated PnF box above D.

**Exact target**

- Target 1: `1R`.
- Target 2: `2R` only if Target 1 is reached before the stop.
- No target may rely on the original next-pivot continuation/reversal labels.

**Filters**

- Use only v2 geometry rules.
- Require the break to occur after D and within the next 3 confirmed pivots.
- Reject if the first-next-pivot label is the only supporting evidence.
- Reject if B/C structural levels are unavailable.

**Why it follows from the existing evidence**

- The distance audit explicitly concludes that first-next-pivot outcome is structurally constrained and future work should consider later horizons and first breaks of B/C structural levels.

**Confidence**

Low. The horizon-design logic is supported, but profitability is untested.

**Expected weaknesses**

- Structural break may be too late.
- B/C break selection is not validated as a profitable trigger.
- The next-3-pivot horizon is suggested by methodology, not proven as an edge.

**Estimated probability of becoming profitable after validation**

18%.

### 4. `ABCD_SYMMETRY_ZONE_CONFIRMATION_CANDIDATE`

**Rank:** 4 — tests the most intuitive AB=CD geometry bucket, but the committed evidence does not show edge for it.

**Exact entry**

- Candidate is disabled by default.
- Eligible geometry: `0.90 <= CD/AB < 1.10`.
- Long entry:
  - `CD` direction is `DOWN`.
  - After D, require a repaired-semantics post-D confirmed pivot in the `UP` direction.
  - Enter long at that pivot’s knowledge time.
- Short entry:
  - `CD` direction is `UP`.
  - After D, require a repaired-semantics post-D confirmed pivot in the `DOWN` direction.
  - Enter short at that pivot’s knowledge time.

**Exact stop**

- Long stop: one validated PnF box below D.
- Short stop: one validated PnF box above D.

**Exact target**

- Target: `1R` only.
- No runner and no automatic TP2 because no committed ABCD evidence validates a larger target.

**Filters**

- Use only v2 geometry rules.
- Use only the near-equal symmetry zone `0.90–1.10`.
- Require repaired post-D confirmation; do not trade merely because the zone exists.
- Exclude original Phase 3 continuation/reversal labels.

**Why it follows from the existing evidence**

- The design documents define the near-equal symmetry zone as a descriptive bucket.
- The current synthesis rejects trading the zone directly, so this candidate requires additional repaired confirmation and remains validation-only.

**Confidence**

Low. The zone exists as a design concept, but committed outcome evidence does not prove edge.

**Expected weaknesses**

- Symmetry may be visually appealing but statistically empty.
- A `1R` target may be too small or too large; no committed ABCD study validates it.
- Confirmation may reduce sample size materially.

**Estimated probability of becoming profitable after validation**

15%.

### 5. `ABCD_EXTENSION_EXHAUSTION_CANDIDATE`

**Rank:** 5 — least promising because extension zones are defined, but no committed evidence shows they outperform OTHER.

**Exact entry**

- Candidate is disabled by default.
- Eligible geometry must be one of:
  - `1.20 <= CD/AB < 1.35`, or
  - `1.55 <= CD/AB < 1.70`.
- Long entry:
  - `CD` direction is `DOWN`.
  - After D, require a repaired-semantics post-D confirmed pivot in the `UP` direction.
  - Enter long at that pivot’s knowledge time.
- Short entry:
  - `CD` direction is `UP`.
  - After D, require a repaired-semantics post-D confirmed pivot in the `DOWN` direction.
  - Enter short at that pivot’s knowledge time.

**Exact stop**

- Long stop: one validated PnF box below the post-D extreme.
- Short stop: one validated PnF box above the post-D extreme.

**Exact target**

- Target: `1R`.
- If both extension zones validate later, the larger extension bucket `1.55–1.70` should be tested separately from `1.20–1.35`; do not merge them for production until validated.

**Filters**

- Use only v2 geometry rules.
- Require repaired post-D confirmation.
- Do not use first-next-pivot labels as proof of edge.
- Do not trade extension zones without separate validation by zone.

**Why it follows from the existing evidence**

- The design documents define moderate and large extension buckets.
- The committed synthesis states that these zones have no proven edge, so the only valid production-candidate use is a disabled validation candidate with strict confirmation.

**Confidence**

Low.

**Expected weaknesses**

- Extension may indicate continuation rather than exhaustion.
- Stop placement beyond an extended D can create poor reward/risk.
- Zone-specific sample size may be too small after confirmation filters.

**Estimated probability of becoming profitable after validation**

12%.
