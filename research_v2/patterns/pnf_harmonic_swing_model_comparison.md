# PnF Harmonic Swing Model Comparison

Date: 2026-06-09
Status: Phase 0 design-only research study
Scope: compare swing definitions for future PnF-native harmonic geometry versus market-structure/trend analysis

## Executive Summary

The proposed PnF structural swing aggregation design is well suited for market-structure and trend analysis because it intentionally suppresses minor opposite columns until a causal reversal threshold is met. That behavior produces stable, auditable trend swings with clear lifecycle state, `knowledge_time`, and no lookahead.

However, harmonic geometry has a different objective. Harmonic-grade swings need accurate geometric pivots and proportional leg measurement, including smaller reactions, failed extensions, and nested or sub-structural turns that may be meaningful even when they do not qualify as major market-structure reversals.

The central conclusion is:

> Harmonic research should not assume that one harmonic leg equals one trend-oriented structural swing.

The current aggregation design should remain the trend-oriented structural baseline, but future harmonic work should begin with a **separate harmonic swing framework** that can reference the structural swings as context. The recommended next step is **Option C: a separate harmonic swing framework**, with a shared causal input layer and explicit mapping back to structural swings.

This is a design-only study. It does not implement an aggregation engine, detector, harmonic pattern recognizer, Gartley/Bat/Butterfly logic, strategy logic, expectancy analysis, candidate generation, production change, or database change.

## Background and Research Question

Completed PnF columns are raw material. The current structural aggregation design proposes combining those columns into larger structural swings before later research consumes them. The earlier PnF harmonic framework design also states that structural swings are the intended research unit for initial PnF geometry work.

Before implementing any aggregation engine, this document asks:

> Should harmonic detection use the same structural swings produced by the proposed aggregation engine, or does harmonic research require a different swing model?

The answer depends on whether the assumptions that improve trend-state stability also preserve the pivot granularity required for harmonic geometry.

## Current Aggregation Assumptions That Define a Structural Swing

The current design defines structural swings through the following assumptions:

1. **Completed-column-only input**: only completed PnF columns are eligible for swing decisions.
2. **One active swing per symbol**: causal replay maintains at most one active structural swing and pending opposite reversal context.
3. **Bootstrap start**: the first completed column starts the first active swing.
4. **Confirmed-reversal start**: a new opposite swing starts at the first opposite candidate column, but is only knowable at the confirming column's completion time.
5. **Same-direction extension requires a new extreme**: an `UP` swing extends only on a new X high; a `DOWN` swing extends only on a new O low.
6. **Same-direction non-extremes are contained, not extensions**: non-extreme continuation columns can be retained for provenance but do not change the structural endpoint.
7. **Opposite columns begin as internal reactions**: an opposite column is not a new swing unless reversal confirmation criteria are met.
8. **Reversal confirmation thresholds are predeclared**: a candidate reversal must satisfy selected rules such as minimum absolute boxes, minimum relative reaction ratio, and candidate directional extreme requirements.
9. **Reaction ratio logic is structural**: opposite move size may be evaluated relative to the active swing's box size.
10. **Continuation invalidation may cancel reversal candidates**: a same-direction new extreme can lock prior opposite columns as internal reactions.
11. **Cumulative reaction logic is a variant**: a design variant may allow multiple opposite columns separated by non-extreme continuation columns to accumulate into one reversal candidate.
12. **Confirmed versus pending lifecycle**: downstream confirmed-only audits should exclude pending swings.
13. **Event time is separated from knowledge time**: endpoints may occur before the swing or reversal is causally knowable.
14. **No outcome coupling**: no future trade result, strategy outcome, expectancy, or candidate label may influence swing construction.
15. **Per-symbol independence and deterministic ordering**: no cross-symbol leakage; ties are resolved deterministically.
16. **Provenance is mandatory**: source columns that start, extend, react inside, and confirm a swing must be traceable.

## Assumption-by-Assumption Harmonic Evaluation

| Aggregation assumption | Effect on trend / market structure | Harmonic construction impact | Beneficial / Neutral / Harmful for harmonics | Reasoning |
|---|---|---|---|---|
| Completed-column-only input | Essential for causal structure state. | Also essential for causal harmonic research. | Beneficial | Harmonic labels must not use forming columns. Completed-column boundaries are a safe causal unit. |
| One active swing per symbol | Simplifies trend state and avoids overlapping major swing narratives. | Can hide nested harmonic geometry that exists inside a larger trend swing. | Partially beneficial | Good as a top-level context layer, but harmonic research may need multiple concurrent swing levels. |
| Bootstrap start from first completed column | Provides deterministic initialization. | Mostly irrelevant after warmup; early pivots may be unstable. | Neutral | Harmonic research can discard warmup sequences or require minimum prior context. |
| New swing starts at first opposite candidate column but is knowable at confirmation | Preserves structural pivot while enforcing causal availability. | Useful for pivot geometry if the first opposite column is the true harmonic pivot. | Beneficial | Separating pivot event time from `knowledge_time` is required for causal harmonic measurement. |
| Extension requires new high/new low | Keeps trend swings focused on directional progress. | May merge smaller harmonic legs into one structural swing when reactions do not become structural reversals. | Partially harmful | Harmonic ratios may need pivots formed by meaningful reactions even if no major trend reversal occurs. |
| Same-direction non-extremes are contained | Avoids endpoint drift from weak continuation. | May obscure failed extensions or non-extreme retests that are geometrically important. | Partially harmful | Harmonic construction may need to know when a same-direction attempt failed, not only whether a new extreme printed. |
| Opposite columns are internal reactions until confirmation | Prevents over-fragmented trend swings. | Risks deleting the very pivots harmonic geometry would measure. | Harmful if used alone | A minor internal reaction can be a valid harmonic BC or sub-leg even when it is not a trend reversal. |
| Absolute reversal thresholds | Stabilize major swing definitions. | Can filter noise but may remove small valid harmonic pivots. | Partially beneficial | Thresholds should exist, but harmonic thresholds may need to be lower or hierarchy-specific. |
| Relative reaction ratio versus active swing | Normalizes reversals by prior structural size. | Directly resembles harmonic proportionality concepts, but using trend-swing size can be too coarse. | Partially beneficial | Useful as a feature, not necessarily as the sole swing boundary rule. |
| Directional extreme requirement | Confirms real opposite movement. | Supports clean pivot measurement. | Beneficial | Harmonic pivots should have a measurable directional extreme; exact threshold may differ. |
| Continuation invalidation on new structural extreme | Makes trend interpretation clean. | Can erase an attempted harmonic leg that failed but remains geometrically informative. | Partially harmful | Harmonic failure or nested geometry may require preserving canceled candidates as lower-level legs. |
| Cumulative reaction candidate variants | Captures multi-leg reactions in trend context. | Can merge multiple harmonic legs into one reaction candidate. | Partially harmful | Harmonic geometry often needs individual alternating pivots, not only cumulative depth. |
| Confirmed-only lifecycle for audit feed | Ensures stable structural audit inputs. | May delay or omit emerging harmonic pivots. | Partially beneficial | Confirmed lifecycle is causal, but harmonic research may need provisional-withdrawn states for study, clearly separated from confirmed labels. |
| Event time versus `knowledge_time` | Prevents lookahead while preserving chart geometry. | Essential for harmonic D-point causality. | Beneficial | Harmonic geometry needs event-time distances and causal availability times. |
| No outcome coupling | Protects research validity. | Essential. | Beneficial | Harmonic swing construction must be independent of any later outcome. |
| Per-symbol deterministic replay | Protects reproducibility. | Essential. | Beneficial | Harmonic research should not inherit cross-symbol or nondeterministic leakage. |
| Provenance tracking | Supports auditability. | Essential for nested harmonic debugging. | Beneficial | A harmonic-grade model must trace legs back to raw columns and structural context. |

## Detailed Comparison

Trend-oriented structural swings answer: **What is the current dominant structural move, and when has that move been causally reversed?**

Harmonic-oriented swings answer: **Which causally observable pivots create stable geometric legs that can be measured for proportional relationships?**

Those are related questions, but not identical. Trend swings prioritize stability and suppress microstructure. Harmonic swings prioritize measurable turns, symmetry, nesting, and the preservation of local pivots.

### Trend / Market Structure Requirements

A market-structure swing model should:

- Reduce noise and avoid excessive fragmentation.
- Require convincing reversal evidence.
- Support trend/range state classification.
- Treat shallow opposite columns as reactions inside a larger move.
- Prefer fewer, more stable swings.
- Confirm completed swings before downstream structural audits use them.
- Preserve rollback safety by remaining research-only until promoted.

### Harmonic Geometry Requirements

A harmonic-grade swing model should:

- Preserve enough alternating pivots to measure AB, BC, CD, and possible future multi-leg geometry.
- Retain smaller reactions if they produce meaningful proportional structure.
- Represent nested swings explicitly instead of forcing one global swing layer.
- Track failed extensions and non-extreme continuation attempts because these may affect pivot quality.
- Separate geometric pivot occurrence from causal confirmation.
- Allow harmonic legs to be smaller than major trend swings when the geometry is local.
- Avoid over-merging multiple alternating turns into one monolithic structural swing.

## Compatibility Matrix

| Trend / market structure requirements | Harmonic geometry requirements | Compatible? | Reasoning |
|---|---|---|---|
| Suppress minor opposite columns as internal reactions. | Preserve local pivots that may define BC or CD turns. | PARTIALLY | Suppression helps trend clarity but can remove harmonic-grade turning points. |
| Require new highs/new lows to extend structural swings. | Measure failed retests and non-extreme continuation sequences when they form geometry. | PARTIALLY | New extremes are clean endpoints, but failed extensions may still be relevant harmonic pivots. |
| Confirm reversals only after minimum absolute/relative movement. | Allow smaller proportional pivots if they are geometrically meaningful. | PARTIALLY | Thresholds are useful, but harmonic thresholds may differ by level and should not simply inherit trend thresholds. |
| Maintain one active swing per symbol. | Support nested or overlapping local swing levels. | NO | Harmonic research may need a major trend swing plus lower-level harmonic legs inside it. |
| Use confirmed-only structural swings for audits. | Study provisional and invalidated pivot candidates as geometry-quality evidence. | PARTIALLY | Confirmed-only labels are safer, but research may need clearly flagged provisional/withdrawn geometry states. |
| Treat continuation invalidation as canceling pending reversal. | Preserve failed reversal attempts as possible harmonic sub-legs or failure evidence. | PARTIALLY | Cancellation is valid for trend state, but harmonic research should retain the failed turn as geometry metadata. |
| Allow cumulative reaction depth across multiple opposite columns. | Keep individual alternating pivots available for leg-by-leg ratio measurement. | PARTIALLY | Cumulative depth can describe trend reversal strength but may collapse multiple harmonic legs. |
| Use event time plus `knowledge_time`. | Measure geometry at event pivots while enforcing causal pattern availability. | YES | This is a shared requirement and should be preserved in every model. |
| Sort by completion-time availability. | Avoid D-point and pivot lookahead. | YES | Harmonic research must use the same causal replay principle. |
| Keep source-column provenance. | Audit whether a harmonic leg is single-column, multi-column, nested, or structurally internal. | YES | Provenance is mandatory for both structural and harmonic-quality audits. |
| Favor stable major swings. | Favor accurate proportional geometry. | PARTIALLY | Stability and geometry quality can conflict; a hierarchy can reconcile them. |
| Use structural reaction ratios to characterize major reversals. | Use leg ratios to characterize geometric symmetry and correction depth. | PARTIALLY | Both use ratios, but the numerator/denominator semantics differ. |

## Explicit Harmonic Leg Containment Analysis

### May a harmonic leg contain multiple completed PnF columns?

Yes. A harmonic leg may contain multiple completed PnF columns when they collectively express one directional geometric move. This is already consistent with the earlier harmonic framework principle that raw columns are not stable across box sizes and that a structural swing may contain multiple columns.

However, the aggregation rule that all small opposite columns become internal reactions may be too coarse for harmonic work. A harmonic leg can be multi-column, but the model must still preserve the internal column sequence so researchers can distinguish a smooth leg from a choppy leg.

### May a harmonic leg contain multiple structural reactions?

Sometimes. A harmonic leg can contain multiple small reactions if the leg remains directionally coherent at the harmonic hierarchy level being studied. For example, an AB advance may include shallow O-column reactions that do not create harmonic pivots at the selected level.

But if those reactions produce meaningful alternating pivots relative to the harmonic leg scale, they should be represented as lower-level harmonic legs rather than being hidden inside one major structural swing. Therefore, the answer depends on hierarchy level and minimum harmonic-pivot thresholds.

### May a harmonic leg contain failed extensions?

Yes. Failed extensions may be important harmonic-quality metadata. A failed extension might indicate that the endpoint selected by a trend-oriented structural swing is not the best geometric pivot, or that the leg contains internal distribution/absorption before reversal.

A trend model can treat a failed extension as contained non-extension. A harmonic model should preserve it as a possible sub-pivot, failed-continuation feature, or reason to downgrade geometric cleanliness.

### May a harmonic leg contain non-extreme continuation sequences?

Yes, but with caution. Non-extreme continuation sequences can remain inside one harmonic leg if they do not create a valid opposite pivot at the selected harmonic scale. They may also describe leg texture: smooth, stair-stepped, overlapping, or choppy.

If non-extreme continuation attempts alternate with meaningful reactions, they may imply that the harmonic-grade swing model should split the region into nested legs rather than force a single major structural leg.

## Truth Analysis of Key Statements

| Statement | Classification | Analysis |
|---|---|---|
| A. One harmonic leg = one structural swing. | Sometimes true | This is true when the structural swing's endpoints coincide with the intended harmonic pivots and the trend threshold matches the harmonic scale. It is false when harmonic geometry occurs inside a larger structural swing or when a structural swing merges several smaller alternating turns. |
| B. One structural swing can contain multiple harmonic legs. | Sometimes true | A broad trend-oriented swing can contain nested harmonic geometry, especially if internal reactions are too small for trend reversal but large enough for harmonic measurement. It is not always true because some structural swings may be clean single-leg moves. |
| C. A harmonic pivot must always coincide with a structural reversal. | False | A harmonic pivot must be a causal geometric turn at the selected harmonic scale, but it does not always have to be a major market-structure reversal. It may be an internal reaction, failed extension, or lower-level pivot inside a larger trend swing. |
| D. A structural swing extension always improves harmonic measurement quality. | False | A new high or low can improve trend endpoint accuracy, but harmonic geometry may degrade if the extension creates an overextended leg, distorts symmetry, or hides a prior cleaner pivot. Structural extension is trend-useful, not automatically harmonic-useful. |

## Candidate Swing Models for Future Research

### Model A: Trend-Oriented Structural Swing

**Definition**

Use the current aggregation design unchanged as the harmonic input: completed columns are aggregated into major structural swings using new-extreme extension, internal reaction handling, predeclared reversal thresholds, confirmed/pending lifecycle, and `knowledge_time`.

**Advantages**

- Strong causal validity.
- Clean market-structure interpretation.
- Lower noise and fewer fragmented pivots.
- Direct compatibility with `structural_reaction_ratio_audit.py` confirmed-swing exports.
- Easier to audit because one active swing per symbol simplifies state.

**Disadvantages**

- Likely over-merges smaller harmonic turns.
- Can hide harmonic-grade BC legs as internal reactions.
- May discard failed reversals that are geometrically meaningful.
- A single threshold set may be too blunt across symbols, box sizes, and hierarchy levels.

**Expected harmonic quality**

Medium to low for local harmonic geometry, but potentially useful for large, slow AB=CD-style structures.

**Expected structural quality**

High. This is the model's primary purpose.

**Causal validity**

High, provided all thresholds are predeclared and only completed columns are used.

### Model B: Harmonic-Oriented Swing

**Definition**

Create a separate harmonic swing model that still uses completed PnF columns and causal confirmation, but uses harmonic-grade pivot rules instead of trend reversal rules. It may allow lower thresholds, nested pivot levels, explicit failed-extension recording, and preservation of internal reactions as possible harmonic pivots.

**Advantages**

- Better geometric fidelity.
- Preserves local pivots needed for AB/BC/CD measurement.
- Can evaluate whether internal reactions are meaningful rather than suppressing them by default.
- Can support multiple hierarchy levels without redefining trend state.
- Avoids forcing trend assumptions onto harmonic research.

**Disadvantages**

- Higher risk of over-fragmentation.
- More configuration complexity.
- More ambiguous pivot selection.
- Requires stronger validation discipline to avoid subjective or overfit geometry.
- Less directly compatible with existing structural reaction ratio exports unless mapping fields are added.

**Expected harmonic quality**

High, assuming pivot thresholds are versioned and tested with strict causal audits.

**Expected structural quality**

Medium to low as a trend-state representation, because it intentionally preserves local turns that a trend model should suppress.

**Causal validity**

High if it keeps completed-column-only input, event-time versus `knowledge_time`, deterministic replay, and explicit lifecycle metadata. Causal validity is not reduced by using a separate model; it is reduced only if the model uses future confirmation improperly.

### Model C: Hybrid Swing

**Definition**

Maintain the trend-oriented structural swing as the top-level context and create harmonic sub-swings within each structural swing. The output records both a structural swing ID and harmonic swing IDs. Harmonic pivots may be internal to structural swings but must remain causally confirmed at their own hierarchy level.

**Advantages**

- Preserves trend context while allowing local harmonic geometry.
- Lets researchers test whether harmonic quality improves inside specific structural regimes.
- Avoids choosing a single swing definition for all purposes.
- Supports mapping from harmonic legs to parent structural swings.
- Can keep `structural_reaction_ratio_audit.py` unchanged while adding separate harmonic-grade research artifacts later.

**Disadvantages**

- More complex outputs and provenance requirements.
- Requires clear hierarchy rules to prevent subjective nesting.
- Needs careful lifecycle semantics for parent structural swings versus child harmonic swings.
- Can still overfit if many hierarchy variants are tried without isolated experiment discipline.

**Expected harmonic quality**

Medium to high. It should outperform pure trend swings for local geometry while retaining useful context.

**Expected structural quality**

High at the parent layer and medium at the child layer. The parent model remains structurally stable; child swings are not intended to define dominant trend state.

**Causal validity**

High if each layer has independent `knowledge_time`, confirmation rules, and raw-column provenance.

## Risks

1. **Over-merging risk**: using trend structural swings unchanged may merge several harmonic-grade pivots into one leg and produce misleading ratio measurements.
2. **Over-fragmentation risk**: a separate harmonic swing model may create too many small pivots, producing noisy geometry that does not reflect durable PnF structure.
3. **Hierarchy ambiguity risk**: hybrid models can become subjective unless every level has deterministic thresholds and explicit IDs.
4. **Confirmation-delay risk**: harmonic endpoints may occur long before their `knowledge_time`, so any future study must carefully distinguish geometric measurement from label availability.
5. **Threshold proliferation risk**: many alternative minimum reaction and pivot thresholds can create hidden optimization. Each threshold set must be an isolated, versioned research experiment.
6. **Terminology collision risk**: the word `swing` can mean trend swing, harmonic leg, or raw column movement. Future artifacts should name fields explicitly, such as `structural_swing_id`, `harmonic_swing_id`, and `parent_structural_swing_id`.
7. **Audit compatibility risk**: `structural_reaction_ratio_audit.py` consumes confirmed structural swings. Harmonic-grade swings should not be silently fed into that audit unless the audit is explicitly versioned for harmonic input semantics.
8. **False precision risk**: preserving more pivots can make ratios look exact while the pivot-selection rule remains unstable. Ratio precision should not be confused with model quality.
9. **Production contamination risk**: harmonic swing research must not alter current detectors, strategies, live/demo trader behavior, database schema, or protected interfaces.

## Recommendation

Future harmonic research should start from:

> **(C) a separate harmonic swing framework**

The current aggregation design should remain the trend-oriented structural model and should be used as context, not as the only harmonic input. A separate harmonic swing framework should share the same completed-column causal input layer, `knowledge_time` discipline, deterministic replay, lifecycle metadata, and provenance requirements. It should not inherit the trend model's major-reversal thresholds as mandatory harmonic pivot thresholds.

A modified aggregation design alone is not recommended as the first step because it risks weakening the market-structure model while still not fully solving harmonic pivot granularity. A pure unchanged structural model is also not recommended because it is likely too coarse for local harmonic geometry.

The preferred future architecture is:

```text
completed PnF columns
-> trend-oriented structural swing aggregation
-> harmonic-oriented swing extraction using structural context
-> design-only harmonic geometry audits later
```

The structural swing layer answers market-structure questions. The harmonic swing layer answers geometric pivot questions. They should be linked but not treated as identical.

## Recommended Next Research Step

Create a design-only specification for **harmonic-grade PnF swing extraction** before any engine implementation.

That design should define:

- Required harmonic swing fields and provenance.
- Minimum harmonic pivot thresholds by boxes and ratios.
- Whether harmonic swings are single-level or hierarchical.
- How failed extensions and non-extreme continuation attempts are recorded.
- How harmonic `knowledge_time` differs from parent structural `knowledge_time`.
- How harmonic swings map to parent `structural_swing_id` values.
- Which artifacts remain confirmed-only versus provisional/withdrawn research rows.
- How to prevent harmonic swing outputs from being confused with `structural_reaction_ratio_audit.py` inputs.

No harmonic detection engine, named harmonic recognizer, strategy logic, expectancy study, candidate generation, production change, or database change should be attempted until that harmonic-grade swing extraction design is complete.
