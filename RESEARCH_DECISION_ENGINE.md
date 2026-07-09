# Research Decision Engine

## Scope

This document defines how the Research Operating System (ROS) decides what research should be done next. It is architecture and operating design only.

It does not write code, create folders, run research, backtest, optimize, change production behavior, or modify existing Pole, ABCD, Harmonic, MEXC, or other detector logic.

## Purpose

The Research Decision Engine is the prioritization layer above the ROS. Its job is to convert accumulated project knowledge into the smallest useful next research action.

It should prevent the project from drifting into disconnected detector creation by requiring that every next action be justified by existing registries, known failures, available features, validation history, research debt, and the protected Pole baseline.

The engine does not decide whether an edge is profitable. It decides what should be investigated, migrated, blocked, rejected, or deferred next.

## 1. Input Sources

The engine consumes existing ROS records. It should not rely on informal memory when a registry or manifest can represent the information.

| Input source | What it contributes to the decision |
| --- | --- |
| `Edge Registry` | Validated edges, known weaknesses, dependencies, stability, robustness, live eligibility, and promotion state. |
| `Failure Registry` | Rejected ideas, repeated failure reasons, do-not-repeat notes, revisit conditions, and kill criteria evidence. |
| `Hypothesis Registry` | Candidate ideas, expected mechanisms, required features, expected populations, priority scores, status, and next actions. |
| `Feature Store` | Existing stable features, experimental features, missing features, dataset compatibility, incremental support, and recomputation policy. |
| `Research Debt` | Open proof gaps, blockers, estimated cost, dependencies, owners, and blocking level. |
| `Validation History` | Prior runs, metrics, policies, data splits, ambiguity handling, reports, and evidence quality. |
| `Live/Forward Observations` | Forward validation behavior, live-shadow observations, execution mismatches, and confidence changes. |
| `Existing Pole Baseline` | Protected rollback point and comparison standard for serious candidates. |

Input requirements:

- Each candidate should link to at least one `Hypothesis`, `ResearchDebtItem`, `Failure`, `Edge`, or migration task.
- If no ROS object exists for a candidate, the first action is usually `MIGRATE_EXISTING_KNOWLEDGE` or `NEEDS_MORE_INFO`, not validation.
- Any serious candidate expected to compete with trading logic must be compared against the existing Pole baseline when validation eventually occurs.

## 2. Research Candidate Scoring

The engine ranks candidate research tasks. The score is a prioritization aid, not an approval to validate, promote, or deploy.

### Scoring Dimensions

| Dimension | Interpretation | Higher score means |
| --- | --- | --- |
| `expected_information_gain` | How much the task reduces uncertainty or clarifies future research direction. | The result will meaningfully guide many future decisions. |
| `probability_of_edge` | Prior belief that a real edge may exist within the stated scope. | Mechanism and prior evidence are plausible. |
| `implementation_cost` | Work required to define or express the idea once implementation is allowed. | Lower cost is better. |
| `validation_cost` | Compute, review, and reporting effort required to answer the question. | Lower cost is better. |
| `dataset_readiness` | Whether frozen datasets already support the question. | Required data already exists and is compatible. |
| `feature_availability` | Whether required features already exist and are stable. | Required features are already available or nearly available. |
| `overlap_with_existing_work` | Reuse of existing features, labels, validations, reports, or failures. | The task can leverage prior artifacts. |
| `time_to_answer` | Expected time to reach a useful decision. | Shorter time is better. |
| `risk_of_overfitting` | Risk that the task encourages parameter chasing or narrow-sample conclusions. | Lower risk is better. |
| `risk_to_protected_baseline` | Risk of destabilizing or confusing the current profitable Pole rollback baseline. | Lower risk is better. |

### Practical Ranking Model

Use qualitative or numeric scoring, but require the same fields for every candidate in the same decision cycle.

Suggested directionality:

```text
Higher priority when:
  expected_information_gain is high
  probability_of_edge is high
  dataset_readiness is high
  feature_availability is high
  overlap_with_existing_work is high
  implementation_cost is low
  validation_cost is low
  time_to_answer is short
  risk_of_overfitting is low
  risk_to_protected_baseline is low
```

The engine should also record why each score was assigned. A score without rationale is not a decision record.

### Tie-Breakers

If two candidates are close, prefer the one that:

1. Closes existing research debt.
2. Migrates valuable historical knowledge into the ROS.
3. Reuses stable features without recomputation.
4. Has clear kill criteria.
5. Can be answered with the smallest validation scope.
6. Improves confidence in or around the protected Pole baseline without changing it.

## 3. Decision Categories

Every proposed next action must be classified into exactly one decision category.

| Category | Meaning | Typical trigger |
| --- | --- | --- |
| `DO_NOW` | The task is ready, useful, bounded, and has the required inputs. | High information gain, available data/features, low waste, clear next action. |
| `DEFER` | The task is valid but not currently the best use of effort. | Lower priority than other ready tasks, or depends on later roadmap timing. |
| `BLOCKED` | The task cannot proceed because required inputs are missing. | Missing dataset, missing feature, unresolved policy, open debt blocker. |
| `REJECT` | The task should not proceed under current evidence. | Duplicates known failure, violates kill criteria, high overfit risk, or no plausible mechanism. |
| `NEEDS_MORE_INFO` | The task may be valuable but is not yet defined well enough to score or execute. | Unclear mechanism, unclear population, missing owner/status, ambiguous expected output. |
| `MIGRATE_EXISTING_KNOWLEDGE` | The next best action is to encode known historical information into ROS records. | Existing knowledge exists but is not represented in registries/manifests. |
| `CLOSE_RESEARCH_DEBT` | The next best action is to close a known evidence or methodology gap. | Debt blocks promotion, validation, comparison, or confidence updates. |

Decision records should include:

- Candidate name.
- Decision category.
- Score summary.
- Primary reason.
- Required ROS objects.
- Missing inputs or blockers.
- Smallest useful next action.
- Expected output artifact.
- Whether the Pole baseline is relevant for eventual comparison.

## 4. Operating Loop

The Research Decision Engine runs as a recurring loop.

```text
Inventory
  ↓
Score candidates
  ↓
Pick smallest useful next action
  ↓
Run only needed validation
  ↓
Update registries
  ↓
Update confidence
  ↓
Update failure/debt records
  ↓
Pick next action
```

### Step Definitions

1. **Inventory**
   - Read active hypotheses, validated edges, failures, open research debt, feature availability, validation history, forward/live observations, and baseline state.
   - Identify candidates that are ready, blocked, stale, duplicated, or missing registry records.

2. **Score candidates**
   - Score every candidate using the same dimensions for the decision cycle.
   - Record rationale for each score.
   - Flag candidates that cannot be scored because required knowledge is missing.

3. **Pick smallest useful next action**
   - Prefer the smallest action that changes a decision state.
   - Examples: migrate one historical report, define one hypothesis, close one debt item, validate one missing split, or reject one duplicate idea.

4. **Run only needed validation**
   - If validation is approved later, run only the validation needed to answer the current question.
   - Do not run broad recomputation or broad backtests unless dependencies require it.

5. **Update registries**
   - Update hypothesis, edge, failure, validation, feature, report, promotion, and debt records as appropriate.
   - No output should exist without a manifest or registry reference.

6. **Update confidence**
   - Record confidence changes as history, with reason and evidence.
   - Confidence updates should stay scoped to the tested population.

7. **Update failure/debt records**
   - Preserve negative results.
   - Close debt only when the stated close condition is met.
   - Add new debt when validation exposes missing evidence or assumptions.

8. **Pick next action**
   - Re-run the loop from the updated state.
   - The next action should reflect new evidence, not the previous plan by inertia.

## 5. Anti-Waste Rules

The engine must enforce the following rules:

- No broad recomputation unless a dependency was invalidated.
- No new detector before the hypothesis and required features exist.
- No production promotion without validation.
- Preserve negative results.
- Prefer incremental checks.
- Always compare serious candidates against the existing Pole baseline when they are intended to compete with or alter trading decisions.
- Stop research early when kill criteria are met.
- Do not retest a failed idea unless revisit conditions are satisfied.
- Do not create a new feature if an existing stable feature already answers the question.
- Do not treat a report as evidence unless it links to dataset, feature, policy, and validation context.
- Do not let live observations override validation records without a documented confidence update and promotion review.
- Do not change the protected baseline as part of prioritization.

## 6. First Practical Use Case: Ranking Candidate Tracks

This section defines how the engine would rank possible next research tracks. It does not validate them, score them with real values, or select a trading strategy.

### Candidate Track Evaluation Template

Each candidate should be scored using this template:

| Field | Required answer |
| --- | --- |
| `candidate_track` | Name of the research track. |
| `linked_ros_objects` | Existing hypotheses, failures, features, edges, reports, or debt items. |
| `expected_information_gain` | Low / Medium / High with rationale. |
| `probability_of_edge` | Low / Medium / High with rationale. |
| `implementation_cost` | Low / Medium / High with rationale. |
| `validation_cost` | Low / Medium / High with rationale. |
| `dataset_readiness` | Low / Medium / High with rationale. |
| `feature_availability` | Low / Medium / High with rationale. |
| `overlap_with_existing_work` | Low / Medium / High with rationale. |
| `time_to_answer` | Short / Medium / Long. |
| `risk_of_overfitting` | Low / Medium / High. |
| `risk_to_protected_baseline` | Low / Medium / High. |
| `decision_category` | One of the seven decision categories. |
| `smallest_useful_next_action` | The next action that changes project state. |

### Track-Specific Selection Logic

| Candidate track | How it should be scored and selected |
| --- | --- |
| `Continuation Failure` | Score highly if existing Pole or structure records already contain failed continuation examples, required features exist, and the task can explain known losses or failure reports. Select only if it can reuse validation history and has clear kill criteria. |
| `Failed Breakout` | Score highly if breakout labels, retest labels, and baseline Pole context features already exist. Penalize if it duplicates known failure records or requires broad relabeling. Useful if it may explain false-positive baseline candidates without modifying the baseline. |
| `Compression → Expansion` | Score based on availability of compression, volatility, range, and expansion features. If those features are missing, classify as `BLOCKED` or `CLOSE_RESEARCH_DEBT` rather than starting detector work. |
| `Exhaustion` | Score based on whether exhaustion can be expressed with existing momentum, volatility, range extension, or reaction features. Penalize high overfitting risk if the concept is vague or parameter-heavy. |
| `Structural Trap` | Score highly if prior failure reports indicate trapped participants or false structural breaks and existing structural labels can represent the idea. Classify as `NEEDS_MORE_INFO` if the mechanism cannot be stated without ad-hoc detector logic. |
| `Trend Aging` | Score highly if trend duration, leg count, slope, volatility decay, or regime features already exist. It may be a good context layer if time-to-answer is short and it can be compared against known Pole outcomes. |
| `Sentiment / Funding / OI context layer` | Score high for information gain but low for dataset readiness unless clean frozen external datasets already exist. Likely `BLOCKED` or `CLOSE_RESEARCH_DEBT` until data manifests and compatibility rules exist. |
| `ABCD validation` | Score highly if ABCD historical knowledge, labels, and reports already exist but are not migrated. First action may be `MIGRATE_EXISTING_KNOWLEDGE`; validation should wait until hypotheses and feature dependencies are registered. |
| `Harmonic continuation` | Score based on existing harmonic features, failure records, and validation history. If prior harmonic work is mostly undocumented, classify as `MIGRATE_EXISTING_KNOWLEDGE` before any new validation. |

### Practical Initial Ranking Behavior

Before scoring real values, the engine should apply these ordering rules:

1. Prefer tracks that reuse existing Pole, structure, breakout, pullback, and validation artifacts.
2. Prefer tracks that explain existing failures or close known debt.
3. Defer tracks requiring new external datasets until dataset manifests exist.
4. Reject or defer tracks that require vague detector invention before hypothesis definition.
5. Use `MIGRATE_EXISTING_KNOWLEDGE` for ABCD or Harmonic work if their historical artifacts are not yet represented.
6. Use `CLOSE_RESEARCH_DEBT` for Compression → Expansion or Sentiment/Funding/OI if required features or datasets are missing.
7. Use `DO_NOW` only for a track whose smallest next action is bounded, registry-backed, and likely to change a decision state quickly.

The engine should not choose the most exciting detector. It should choose the next action that most efficiently improves the research system's knowledge state.

## 7. Final Recommendation

The project should now stop architecture expansion and begin `ROS-0` / real migration.

Reasoning:

- The Blueprint defines the core operating contracts.
- The Advanced document defines long-term extensibility.
- The Knowledge Migration Review confirms the current ROS can represent existing historical knowledge with only minor operational gaps.
- This Decision Engine defines how to choose the next research action without creating another detector or running unnecessary validation.

The next best project action is therefore not more architecture. It is `ROS-0`: inventory existing research artifacts, map them into the current ROS concepts, identify missing manifests or registry records, and preserve the existing Pole baseline as the protected rollback reference.
