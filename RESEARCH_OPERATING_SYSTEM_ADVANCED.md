# Research Operating System Advanced Architecture

## Scope

This document extends `RESEARCH_OPERATING_SYSTEM_BLUEPRINT.md`. It does not replace the Blueprint, implement any system, change any detector, create datasets, run analysis, backtest, optimize, or make profitability claims.

The purpose is to define long-term research capabilities that can sit on top of the ROS once the foundational registries, manifests, validation records, and promotion gates exist.

## 1. Knowledge Graph

The long-term ROS should evolve from isolated registries into a linkable research knowledge graph. Registries remain the source of truth for object records, while the graph describes relationships between those records.

### Node Types

Every node must have a stable ID, version, lifecycle status, owner, creation date, update date, and source manifest or registry reference.

| Node type | Purpose |
| --- | --- |
| `Dataset` | Frozen data source or derived dataset manifest. |
| `Feature` | Versioned reusable computed attribute. |
| `Hypothesis` | Testable research claim. |
| `Experiment` | Universal experiment instance linking inputs, evaluation, and outputs. |
| `ValidationRun` | Reproducible evaluation event. |
| `Edge` | Validated source of conditional evidence. |
| `Failure` | Rejected or abandoned research path. |
| `Report` | Human-readable summary or comparison artifact. |
| `PromotionDecision` | Gate decision, approval, rejection, or rollback decision. |
| `ProductionCandidate` | Edge or composed decision package eligible for live-shadow or later stages. |
| `ResearchDebtItem` | Known unresolved research limitation. |
| `LiveDeployment` | Shadow, micro-live, or production deployment record. |

### Relationship Types

Relationships must be explicit, directional, versioned where relevant, and auditable.

| Relationship | Meaning |
| --- | --- |
| `DEPENDS_ON` | Object requires another object to be valid or reproducible. |
| `GENERATED_FROM` | Object was created from a dataset, feature, run, or report. |
| `GENERATED_FEATURE` | Dataset or upstream feature produced a feature. |
| `TESTS` | Experiment evaluates a hypothesis. |
| `VALIDATES` | Validation run supports an edge or promotion gate. |
| `REJECTS` | Failure record rejects a hypothesis, experiment, or edge candidate. |
| `PROMOTES` | Promotion decision advances an edge or candidate. |
| `BLOCKS` | Research debt item prevents promotion or further validation. |
| `REFERENCES` | Report, decision, or note references another object. |
| `SUPERSEDES` | New version replaces an older version without deleting it. |
| `CONFLICTS_WITH` | Evidence or policy contradicts another record. |
| `COMPOSES_WITH` | Edge can contribute evidence alongside another edge in a decision engine. |

Example relationship patterns:

```text
Feature DEPENDS_ON Dataset
Edge DEPENDS_ON Feature
ValidationRun VALIDATES Edge
Failure REJECTS Hypothesis
PromotionDecision PROMOTES Edge
Dataset GENERATED_FEATURE Feature
Experiment TESTS Hypothesis
Report REFERENCES ValidationRun
ResearchDebtItem BLOCKS PromotionDecision
LiveDeployment GENERATED_FROM PromotionDecision
```

### Required Metadata

Every graph object should include:

- `node_id`
- `node_type`
- `version`
- `status`
- `registry_reference`
- `created_at`
- `updated_at`
- `owner`
- `source_manifest`
- `evidence_links`
- `supersedes`
- `notes`

Every relationship should include:

- `relationship_id`
- `relationship_type`
- `from_node_id`
- `to_node_id`
- `from_version`
- `to_version`
- `created_at`
- `created_by`
- `valid_from`
- `valid_until`
- `confidence_context`
- `evidence_reference`

### Versioning

The graph must never silently mutate historical meaning. New evidence should create new versions or new relationships instead of rewriting old decisions.

Versioning rules:

- Node IDs are stable; versions capture changes in definition, data, policy, or evidence.
- Relationships may be version-specific when a feature, dataset, validation run, or edge changes.
- Superseded nodes remain queryable.
- Deleted research objects should be represented as deprecated, discarded, or superseded rather than removed.
- Historical validation must remain reproducible from the graph state that existed at the time of the run.

### Graph Traversal Examples

Useful long-term questions:

- Start from an `Edge` and traverse to all `Feature` and `Dataset` dependencies to determine whether it can be reproduced.
- Start from a `Feature` and find all validated edges, failed hypotheses, and research debt items that depend on it.
- Start from a `Failure` and find related hypotheses that should not be repeated without new evidence.
- Start from a `Dataset` and list all generated features, validations, reports, and promotion decisions affected by a dataset revision.
- Start from a `ProductionCandidate` and retrieve its hypothesis, experiment ID, validation runs, debt items, promotion decisions, reports, and rollback record.

## 2. Confidence Evolution

Research confidence should be treated as history, not a single final label. A hypothesis can move from low confidence to high confidence through accumulated evidence, or fall back when new evidence contradicts prior assumptions.

### Confidence History Model

Each confidence update should be recorded as an event:

| Field | Purpose |
| --- | --- |
| `object_id` | Hypothesis, edge, production candidate, or composed decision package being updated. |
| `object_type` | Type of object receiving the confidence update. |
| `confidence` | Current confidence estimate recorded at this point in time. |
| `date` | Date of confidence update. |
| `reason` | Human-readable explanation for the change. |
| `evidence` | Report, validation run, forward observation, failure, or review note. |
| `validation_id` | Validation run ID when applicable. |
| `experiment_id` | Universal experiment ID when applicable. |
| `previous_confidence` | Prior recorded confidence value. |
| `changed_by` | Owner or reviewer who recorded the update. |

Example evolution:

```text
0.20
  ↓ initial mechanism is plausible but untested
0.35
  ↓ required features exist and population appears sufficient
0.60
  ↓ first frozen-dataset validation supports the hypothesis
0.82
  ↓ walk-forward and symbol splits remain stable
0.93
  ↓ forward validation and live-shadow behavior match expectations
```

Preserving confidence history is useful because it shows why belief changed, prevents hindsight bias, separates evidence from opinion, and makes reversals explainable. It also allows future researchers to inspect whether confidence rose because of diverse evidence or because of repeated tests on similar data.

## 3. Research Debt

Research Debt is the backlog of unresolved evidence, coverage, data, or methodology issues that limit trust in an edge or hypothesis. It is equivalent in importance to Technical Debt, but it is about incomplete research proof rather than incomplete implementation quality.

Examples of Research Debt:

- Edge requires walk-forward validation.
- Edge requires cross-exchange validation.
- Edge requires larger population.
- Edge requires a new feature.
- Edge requires a new dataset.
- Edge requires a better execution model.
- Edge requires stronger ambiguity analysis.
- Edge requires regime-specific review.

Each debt item must include:

| Field | Purpose |
| --- | --- |
| `debt_id` | Stable research debt identifier. |
| `linked_object_id` | Hypothesis, edge, validation run, promotion gate, or production candidate affected. |
| `description` | What is missing or unresolved. |
| `priority` | Relative urgency. |
| `estimated_cost` | Expected research, compute, data, or review effort. |
| `blocking_level` | `NONE`, `RESEARCH_ONLY`, `PROMOTION_BLOCKER`, `LIVE_BLOCKER`, or `PRODUCTION_BLOCKER`. |
| `dependencies` | Features, datasets, policies, or reviews required to close it. |
| `owner` | Responsible person or role. |
| `status` | `OPEN`, `IN_PROGRESS`, `BLOCKED`, `CLOSED`, `ACCEPTED_RISK`, or `SUPERSEDED`. |
| `created_at` | Date opened. |
| `target_resolution` | Intended close condition. |

Research Debt differs from implementation debt because it can block confidence even when code is clean. Implementation debt asks whether the system is maintainable; Research Debt asks whether the evidence is sufficient to trust a claim under the intended operating conditions.

## 4. Universal Experiment IDs

The ROS should assign a universal experiment identifier to every serious research experiment.

Example:

```text
EXP-000145
```

An experiment ID should connect:

- Hypothesis
- Dataset
- Features
- Validation run
- Edge candidate
- Failure record
- Reports
- Promotion decisions
- Live-shadow or live deployment records

### Experiment Record Contract

| Field | Purpose |
| --- | --- |
| `experiment_id` | Stable identifier such as `EXP-000145`. |
| `hypothesis_id` | Hypothesis being tested. |
| `dataset_manifest_ids` | Frozen datasets used. |
| `feature_versions` | Feature IDs and versions used. |
| `validation_run_ids` | Runs produced by the experiment. |
| `edge_ids` | Edges generated or updated. |
| `failure_ids` | Failures generated or updated. |
| `report_ids` | Reports produced. |
| `promotion_decision_ids` | Gate decisions related to the experiment. |
| `live_deployment_ids` | Shadow, micro-live, or production records if applicable. |
| `policy_versions` | Cost, slippage, ambiguity, split, and walk-forward policies. |
| `artifact_manifest_ids` | Outputs generated by the experiment. |
| `final_status` | `PASS`, `FAIL`, `INCONCLUSIVE`, `PROMOTED`, `DISCARD`, or `SUPERSEDED`. |

Years later, a researcher should be able to start from `EXP-000145` and reconstruct exactly what was believed, what was tested, what data and features were used, what outputs were produced, and why the experiment did or did not advance.

## 5. Bayesian Research Model

The current operating model can keep `PASS`, `FAIL`, and `INCONCLUSIVE` semantics, but the advanced ROS should also support evidence-based belief updates. This is an architecture for belief tracking, not a request to implement Bayesian equations.

### Belief Update Architecture

| Concept | Role |
| --- | --- |
| `prior_belief` | Starting confidence before new evidence. |
| `new_evidence` | Validation result, failure, forward observation, live-shadow behavior, or review finding. |
| `updated_belief` | Revised confidence after incorporating evidence. |
| `confidence_history` | Time-ordered record of belief changes and reasons. |
| `evidence_quality` | Qualitative rating of how relevant, independent, broad, and reliable the evidence is. |
| `scope_of_belief` | Population, symbols, regimes, date ranges, and assumptions where the belief applies. |

Evidence should update confidence within a defined scope. A validation result on one symbol group should not automatically imply high confidence across all symbols, exchanges, regimes, or execution contexts.

This model complements gate decisions:

- `PASS` can raise confidence within its tested scope.
- `FAIL` can reduce confidence or narrow the applicable scope.
- `INCONCLUSIVE` can preserve uncertainty while identifying the next evidence needed.

## 6. Edge Composition

The final objective is not a collection of independent detectors. The final objective is a Decision Engine where many validated micro-edges contribute evidence to a trading decision.

### Architectural Objective

A detector identifies a pattern. An edge contributes conditional evidence. A Decision Engine combines evidence from many validated micro-edges into a single decision context.

Potential micro-edge families:

- Pole
- ABCD
- Compression
- Trend
- Liquidity
- Volatility
- Structure
- Momentum
- Reaction

Each edge should be able to answer:

- What condition does it observe?
- What evidence does it contribute?
- What scope is the evidence valid under?
- What features and datasets does it depend on?
- What validation supports it?
- What known weaknesses limit it?
- What confidence history is attached to it?

### Decision Engine Architecture

The Decision Engine should be a consumer of validated evidence, not a replacement for validation.

Conceptual layers:

1. **Observation layer**: Shared features and structural labels describe current market state.
2. **Edge evidence layer**: Validated micro-edges emit evidence records within their validated scope.
3. **Conflict layer**: Contradictory evidence is surfaced rather than hidden.
4. **Context layer**: Symbol, regime, volatility, liquidity, time, and structural state define applicability.
5. **Decision context layer**: All evidence relevant to a potential trade is assembled into a traceable decision packet.
6. **Execution eligibility layer**: Promotion status, live eligibility, debt blockers, and risk controls determine whether a decision can be acted upon.

No weighting, machine learning, optimization, or detector logic is defined here. The architecture only requires that evidence from multiple edges remains traceable to its source hypothesis, validation, confidence history, dependencies, and promotion status.

### Edge Composition Requirements

- Edges must be composable without changing their original validation records.
- Composed evidence must preserve individual edge provenance.
- Conflicting edges must be recorded explicitly.
- Composition must not imply production eligibility unless promotion gates allow it.
- A composed decision must be reproducible from graph links, experiment IDs, features, datasets, and validation records.
- The existing Pole baseline remains a comparison and rollback reference, not an automatically replaced component.

## 7. Meta-Research

Meta-research is the layer that studies the research process itself. Its goal is to improve future research efficiency, not to create trading signals directly.

Questions this layer should answer:

- Which features appear most often in successful edges?
- Which failure reasons repeat?
- Which symbols validate hypotheses fastest?
- Which validation gates eliminate the most bad ideas?
- Which datasets are most reused?
- Which feature dependencies create the most blockers?
- Which research debt categories most often prevent promotion?
- Which hypothesis families produce repeated inconclusive results?
- Which reports or scorecards are most useful for decisions?

Meta-research outputs should feed prioritization, roadmap planning, and debt reduction. For example, if many hypotheses fail because of missing liquidity features, the next best investment may be a shared liquidity feature rather than another detector.

## 8. Self-Improving Research System

The ROS can eventually recommend the next hypothesis or research action automatically from structured records. This is not an AI design and does not require autonomous strategy generation. It is an architecture for recommendation based on existing research state.

Inputs:

- Existing failures
- Validated edges
- Missing features
- Open research debt
- Knowledge gaps
- Expected information gain
- Dataset readiness
- Feature reuse potential
- Promotion blockers
- Repeated failure reasons
- Confidence histories

Recommendation outputs:

- Next hypothesis to define.
- Next missing feature to prioritize.
- Next validation split or dataset to add.
- Next research debt item to close.
- Next failed idea worth revisiting because conditions changed.
- Next edge ready for forward validation or live-shadow review.

The recommendation layer should explain why an action is suggested by citing graph relationships, missing dependencies, repeated blockers, expected information gain, and time-to-answer. Recommendations should remain advisory; governance and promotion gates still control what gets validated or promoted.

## 9. Long-Term Vision

The intended evolution is:

```text
Today
  individual detector research
    ↓
Shared validation
    ↓
Shared features
    ↓
Edge registry
    ↓
Decision Engine
    ↓
Evidence-Based Trading Platform
```

This architecture is expected to scale better than continuously creating independent detectors because it separates reusable evidence from one-off pattern logic. Independent detectors tend to duplicate features, recompute labels, repeat failures, and produce incomparable results. A shared ROS makes datasets, features, validations, failures, debt, confidence, and promotion decisions reusable.

The long-term platform should make every future research question faster to answer because prior work remains linkable. A new hypothesis can reuse frozen datasets, stable features, known failures, existing edge evidence, confidence histories, research debt, and promotion rules instead of starting from zero.

The destination is an evidence-based trading platform where decisions are built from traceable, validated, composable research evidence rather than isolated detector outputs.
