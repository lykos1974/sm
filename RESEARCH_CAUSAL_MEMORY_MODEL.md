# Research Causal Memory Model (ROS-2)

## Purpose

ROS-2 defines how the Research Operating System remembers **why research knowledge changed**.

ROS-1 stores facts, records, and research lineage. ROS-2 adds the conceptual memory layer that preserves the reasoning behind changes in belief, confidence, decisions, dormancy, rejection, promotion, rollback, and reactivation.

This document is architecture and knowledge design only. It does not define implementation, schemas, formulas, scoring algorithms, registry files, detector logic, strategy logic, validation logic, data pipelines, backtests, optimizers, machine learning, or execution systems.

## Core Principle

The ROS must never remember only outcomes.

A research system that stores only `PASSED`, `FAILED`, or `SUCCESS` cannot explain whether an idea was wrong, unproven, under-sampled, contradicted by better evidence, dependent on missing data, or temporarily dormant.

The required memory chain is:

```text
Observation
↓
Belief
↓
Hypothesis
↓
Experiment
↓
Evidence
↓
Decision
↓
Knowledge
↓
Reason why the belief changed
↓
Conditions under which it should be reconsidered
```

ROS-2 exists to preserve the lower half of that chain: the causal explanation for belief evolution.

## Failed vs. Failed Because

`FAILED` and `FAILED BECAUSE` are different concepts.

`FAILED` is only an outcome label. It says that a research attempt did not meet a required standard.

`FAILED BECAUSE` is causal memory. It records the reason the system should change, weaken, suspend, reject, or revisit a belief.

Examples:

- `FAILED` does not say whether the idea was invalid or whether the detector was too noisy.
- `FAILED` does not say whether sample size was too small or whether the edge disappeared in a specific market regime.
- `FAILED` does not say whether contradictory evidence was strong, weak, temporary, or dependent on missing data.
- `FAILED` does not say when the idea should be reconsidered.

A scientific research system must preserve `FAILED BECAUSE` so future researchers do not repeat old work blindly or discard ideas that only failed under incomplete conditions.

## Causal Memory Concepts

### Belief

A belief is a research claim the system currently considers plausible, active, weakened, dormant, rejected, superseded, or promoted.

A belief should be remembered as an evolving object, not a final sentence. It must preserve:

- the initial belief;
- updates to the belief;
- the current belief;
- why each change occurred;
- what evidence supported it;
- what evidence contradicted it;
- what assumptions it depended on;
- when it became weaker, stronger, dormant, rejected, or reactivated.

### Confidence

Confidence describes how strongly the system trusts a belief at a point in time.

ROS-2 does not define formulas or scoring algorithms. It only requires that confidence changes be explainable.

Confidence memory should preserve:

- the confidence state at the time of an initial belief;
- later confidence changes;
- the reason for each change;
- the evidence that caused confidence to increase or decrease;
- uncertainty that remained after each change.

### Supporting Evidence

Supporting evidence is evidence that strengthens a belief, hypothesis, decision, or knowledge statement.

Supporting evidence should be connected to the reasoning it supports. It should not merely be counted as positive. The system should remember what part of the belief it supports and under what conditions the support applies.

### Contradictory Evidence

Contradictory evidence weakens, challenges, narrows, rejects, or supersedes a belief.

Contradiction should not automatically mean permanent rejection. It may mean:

- the belief was too broad;
- the sample was weak;
- the detector was flawed;
- the market regime changed;
- the evidence source improved;
- the hypothesis needs narrower conditions;
- the idea should become dormant until better inputs exist.

### Assumptions

Assumptions are unstated or stated conditions that must be true for a belief to remain valid.

Examples include assumptions about data quality, detector reliability, market regime stability, liquidity, timeframe relevance, source completeness, and survivorship bias.

ROS-2 should make assumptions visible so that future evidence can challenge them directly.

### Dependencies

Dependencies are external or upstream requirements that a belief, hypothesis, validation, or decision relies on.

Examples include:

- a detector version;
- a data source;
- a validation method;
- an execution assumption;
- a timeframe;
- market coverage;
- a minimum sample size;
- a stable baseline definition.

When a dependency changes, related beliefs may need review.

### Known Weaknesses

Known weaknesses are reasons the current belief may be fragile even if it is still active.

Weaknesses should remain attached to the belief so that promotion, reuse, and future validation do not overstate certainty.

### Failure Reason

A failure reason explains why a validation, hypothesis, or belief did not satisfy the required research standard.

A failure reason should distinguish between:

- invalid idea;
- insufficient evidence;
- poor data quality;
- detector weakness;
- narrow regime dependency;
- poor sample size;
- contradictory evidence;
- execution mismatch;
- inconclusive validation;
- baseline underperformance;
- missing source artifacts.

### Superseded By

A belief or knowledge statement is superseded when a newer claim replaces it while preserving the historical record.

Supersession must not erase the older belief. The older belief remains valuable because it explains the path of reasoning that led to the newer state.

### Replaced Because

`Replaced Because` records the reason one belief, decision, or knowledge statement replaced another.

Examples:

- better evidence appeared;
- validation quality improved;
- a detector was corrected;
- a narrower formulation explained the edge better;
- an earlier assumption was invalidated;
- the baseline changed after explicit promotion.

### Evidence Quality

Evidence quality describes the trustworthiness of evidence as research support or contradiction.

ROS-2 does not define a scoring algorithm. Conceptually, evidence quality may consider whether evidence is complete, reproducible, source-traceable, representative, timely, independent, and aligned with the hypothesis being evaluated.

### Confidence History

Confidence history preserves how trust changed over time.

The system should remember confidence as a timeline of changes with reasons, rather than only storing current confidence.

### Decision History

Decision history records what decisions were made, why they were made, what alternatives existed, what evidence was available, and what uncertainty remained.

Decision history protects the research process from hindsight bias.

### Validation History

Validation history records how a hypothesis was tested over time and how those tests affected belief state.

A hypothesis may have multiple validations with different outcomes, evidence quality, sample sizes, regimes, or dependencies. ROS-2 should preserve the progression rather than collapsing it into a final pass/fail result.

### Promotion History

Promotion history records why knowledge was or was not promoted toward a stable baseline.

Promotion is not merely a success state. It must remember the evidence threshold, unresolved weaknesses, rollback implications, and the reason the promotion was accepted, blocked, deferred, or rejected.

### Research Debt

Research debt records unresolved work that prevents stronger conclusions.

Research debt is part of causal memory because it explains why a belief is uncertain, dormant, blocked, or not promoted.

### Rollback Reason

Rollback reason records why the system returned to a prior baseline or rejected a promoted change.

A rollback should preserve:

- what changed;
- what degraded;
- what evidence triggered rollback;
- what baseline was restored;
- whether the rolled-back idea is rejected or dormant;
- what future evidence could reopen it.

### Dormant Reason

Dormant reason records why an idea is not active but also not permanently rejected.

Dormancy prevents the system from losing ideas that are plausible but currently blocked by missing data, poor evidence quality, detector limitations, insufficient sample size, or unsuitable market conditions.

### Reactivation Conditions

Reactivation conditions define what future change would make a dormant, rejected, or superseded idea worth reconsidering.

Examples include:

- a better detector;
- a larger sample;
- a new data source;
- a higher timeframe dataset;
- a changed market regime;
- improved evidence quality;
- a new baseline;
- a corrected assumption;
- a new source artifact.

### Unknowns

Unknowns are known gaps in the research memory.

Unknowns should be preserved explicitly so future researchers can distinguish between missing information and negative evidence.

### Missing Evidence

Missing evidence is evidence that would be required to strengthen, reject, revive, or promote a belief but is not currently available.

Missing evidence is not the same as contradictory evidence. A hypothesis can be unproven because evidence is missing, not because it has been disproven.

### Future Evidence Needed

Future evidence needed records the specific kind of evidence that would change the research state.

This is a bridge between current research memory and future research planning.

## Belief Evolution

ROS-2 must preserve belief evolution rather than overwriting history.

The system should conceptually remember:

```text
Initial Belief
↓
Updated Belief
↓
Current Belief
```

Each transition should preserve:

- what changed;
- why it changed;
- which evidence caused the change;
- which assumptions were revised;
- whether confidence increased or decreased;
- whether the belief became active, weaker, dormant, rejected, superseded, or promoted.

The latest belief is not enough. The path matters because future researchers need to understand why the current state exists.

## Confidence Evolution

Confidence changes over time as evidence accumulates, weakens, contradicts, or becomes obsolete.

ROS-2 should preserve confidence as conceptual history, not as a single mutable field.

Confidence evolution should answer:

- What was confidence when the belief was first recorded?
- What evidence increased confidence?
- What evidence reduced confidence?
- What uncertainty remained after validation?
- Did confidence decline because of contradiction or because of missing evidence?
- Is confidence low because the idea is weak or because required data does not yet exist?

No formula is required. The architectural requirement is traceability.

## Decision Traceability

Every important research decision should be able to answer:

- What did we believe?
- Why did we believe it?
- What evidence existed?
- What evidence contradicted it?
- Why did we change our mind?
- What evidence was missing?
- What assumptions were active?
- What dependencies mattered?
- What weaknesses were known?
- Would we make the same decision today?

A decision that cannot answer these questions is not sufficiently traceable for ROS-2.

## Dormant Knowledge

The ROS must not permanently reject every idea that is currently unsupported.

Some research claims should become dormant rather than rejected. Dormant knowledge is inactive knowledge that remains available for future reconsideration under defined conditions.

Dormant states may include:

- `WAITING_FOR_BETTER_DATA`
- `WAITING_FOR_NEW_EVIDENCE`
- `WAITING_FOR_NEW_DETECTOR`
- `WAITING_FOR_LARGER_SAMPLE`
- `WAITING_FOR_HIGHER_TIMEFRAME_DATA`
- `WAITING_FOR_MARKET_REGIME_CHANGE`

Dormant knowledge should preserve:

- why it became dormant;
- what evidence was insufficient;
- what evidence contradicted it, if any;
- what dependencies blocked it;
- what future condition would reactivate it;
- whether prior evidence remains useful.

Dormancy is an architectural state for scientific patience. It prevents premature deletion of ideas that may become useful when research inputs improve.

## Reactivation Rules

Future evidence can make an old hypothesis interesting again.

A dormant, rejected, or superseded idea may become a research candidate again when the reason for dormancy or rejection is resolved.

Example:

```text
Funding edge
↓
Rejected
↓
Two years later
↓
Higher quality funding source appears
↓
Hypothesis becomes research candidate again
```

Reactivation should require causal linkage between the old reason for rejection and the new evidence.

Examples:

- If a hypothesis failed because funding data was unreliable, a higher quality funding source can justify reconsideration.
- If a hypothesis was dormant because sample size was too small, a larger sample can justify reconsideration.
- If a hypothesis was blocked by detector weakness, a new detector version can justify reconsideration.
- If a hypothesis failed in one market regime, a regime change can justify a new scoped test.

Reactivation is not automatic. The system should remember why the old state existed and why the new condition is relevant.

## Future Recommendation Memory

ROS-2 should remember recommendations for future research.

Examples include:

- revisit after detector v2;
- revisit when liquidation data exists;
- revisit after OI feed;
- revisit after volatility model;
- revisit after higher timeframe data exists;
- revisit after larger cross-symbol sample;
- revisit after market regime change;
- revisit when source artifact is restored.

Future recommendation memory converts abandoned context into actionable research memory. It helps future researchers understand which old ideas are worth revisiting and which condition must change first.

## Lifecycle Examples

### Lifecycle: Supported, Dormant, Reactivated

```text
Belief
↓
Supported
↓
Validated
↓
Weakening Evidence
↓
Dormant
↓
Reactivated
↓
Validated Again
```

This lifecycle represents an idea that was once useful, later weakened, but not permanently rejected. It becomes active again when new evidence addresses the reason it became dormant.

### Lifecycle: Validated, Contradicted, Superseded

```text
Belief
↓
Validated
↓
Contradicted
↓
Rejected
↓
Superseded
```

This lifecycle represents an idea that once had support but was later contradicted strongly enough to reject the old formulation. The record remains preserved and points to the replacement belief.

### Lifecycle: Unproven, Dormant, Future Candidate

```text
Belief
↓
Hypothesis
↓
Insufficient Evidence
↓
Dormant
↓
Waiting For Larger Sample
↓
Future Research Candidate
```

This lifecycle distinguishes an unproven idea from a disproven idea.

## Anti-Patterns

### Outcome-Only Memory

Storing only `PASS`, `FAIL`, or `SUCCESS` is insufficient because it hides the reason behind the outcome.

An outcome-only system cannot tell whether a hypothesis failed because the idea was wrong, data was missing, the detector was immature, the sample was too small, the regime changed, or contradictory evidence was decisive.

### Overwriting Current Belief

Replacing the old belief with the current belief erases the reasoning path.

A scientific system must preserve the initial belief, updated beliefs, and current belief so future researchers can audit how knowledge evolved.

### Permanent Rejection Without Reactivation Conditions

Rejecting ideas without recording reactivation conditions can cause the system to forget ideas that may become useful after better data, better detectors, or new regimes appear.

### Promotion Without Causal Justification

Promoting a result only because it performed well is unsafe. Promotion memory must include why the system trusted the evidence, what weaknesses remained, and what rollback condition protects the baseline.

### Missing Evidence Treated As Negative Evidence

Missing evidence should not be treated as proof that a hypothesis is false. The ROS must distinguish between contradicted, unproven, dormant, and rejected states.

## Relationship to ROS-1

ROS-1 stores knowledge records and research lineage.

ROS-2 stores the evolution of knowledge and the reasoning that caused research state changes.

The two layers complement each other:

- ROS-1 records what exists.
- ROS-2 explains why it changed.
- ROS-1 preserves the chain from observation to knowledge.
- ROS-2 preserves the causal memory behind belief changes, confidence changes, dormancy, rejection, reactivation, promotion, and rollback.

ROS-2 does not replace ROS-1. It defines the conceptual architecture needed for the Research Operating System to remember not only what it knows, but why it knows it, why it stopped believing it, and under which conditions it should reconsider it.
